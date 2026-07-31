// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::conf::JournalConf;
use crate::proto::raft::*;
use crate::raft::raft_error::RaftError;
use crate::raft::{LibRaftMessage, NodeId, RaftCode, RaftGroup, RaftResult};
use crate::utils::SerdeUtils;
use orpc::client::{ClientConf, ClusterConnector, SyncClient};
use orpc::io::net::{InetAddr, NodeAddr};
use orpc::message::{Builder, Message, RefMessage};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use prost::Message as PMessage;
use raft::eraftpb::{ConfChange, ConfChangeType};
use std::sync::Arc;

/// Raft message processing client.
/// Key points of core functions:
/// 1. Send a message.
/// 2. Select leader and switch leader.
/// 3. Automatically retry the exception and try to switch the leader or re-establish the connection.
#[derive(Clone)]
pub struct RaftClient {
    rt: Arc<Runtime>,
    connector: Arc<ClusterConnector>,
}

impl RaftClient {
    pub fn new(rt: Arc<Runtime>, group: &RaftGroup, conf: ClientConf) -> Self {
        let connector = ClusterConnector::with_rt(conf, rt.clone());
        for node in group.peers.values() {
            let node_addr = NodeAddr::new(node.id, &node.hostname, node.port);

            connector.add_node(node_addr).unwrap();
        }

        Self {
            rt,
            connector: Arc::new(connector),
        }
    }

    pub fn from_conf(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        let group = RaftGroup::from_conf(conf);
        Self::new(rt, &group, conf.new_client_conf())
    }

    pub fn add_node(&self, id: u64, addr: &InetAddr) -> RaftResult<()> {
        let node_addr = NodeAddr::new(id, &addr.hostname, addr.port);
        self.connector.add_node(node_addr)?;
        Ok(())
    }

    // Cluster configuration modification, usually used to join the cluster with new nodes.
    pub async fn conf_change(&self, req: ConfChange) -> RaftResult<()> {
        let _: ConfChangeResponse = self.leader_rpc(RaftCode::ConfChange, req).await?;
        Ok(())
    }

    // Send raft internal message.
    pub async fn send_raft(&self, req: LibRaftMessage) -> CommonResult<()> {
        let _: RaftResponse = self.leader_rpc(RaftCode::Raft, req).await?;
        Ok(())
    }

    // Send application layer messages.
    pub async fn send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        let req = ProposeRequest { data };
        let _: ProposeResponse = self.leader_rpc(RaftCode::Propose, req).await?;
        Ok(())
    }

    pub fn block_on_send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        self.rt.block_on(self.send_propose(data))
    }

    // Join the cluster.
    pub async fn join_cluster(&self, id: NodeId, addr: &InetAddr) -> RaftResult<()> {
        let change = ConfChange {
            change_type: ConfChangeType::AddNode.into(),
            node_id: id,
            context: SerdeUtils::serialize(addr)?,
            id,
        };
        let header = ConfChangeRequest { change };
        let _: ConfChangeResponse = self.leader_rpc(RaftCode::ConfChange, header).await?;
        Ok(())
    }

    pub async fn ping(&self, id: NodeId) -> RaftResult<PingResponse> {
        let header = PingRequest::default();
        let req = Builder::new_rpc(RaftCode::Ping)
            .proto_header(header)
            .build();

        self.retry_rpc::<PingResponse>(id, req).await
    }

    // Send a request to the leader.
    pub async fn leader_rpc<T, R>(&self, code: RaftCode, header: T) -> RaftResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        self.connector
            .proto_rpc::<T, R, RaftError>(code, header)
            .await
    }

    pub async fn timeout_rpc<R>(
        &self,
        id: u64,
        msg: impl RefMessage,
    ) -> Result<R, (bool, RaftError)>
    where
        R: PMessage + Default,
    {
        match self.connector.timeout_rpc::<RaftError>(id, msg).await {
            Ok(rep) => match rep.parse_header::<R>() {
                Err(e) => Err((false, e.into())),
                Ok(v) => Ok(v),
            },

            Err(e) => Err(e),
        }
    }

    pub async fn retry_rpc<R>(&self, id: u64, msg: Message) -> RaftResult<R>
    where
        R: PMessage + Default,
    {
        let rep = self.connector.retry_rpc::<RaftError>(id, msg).await?;
        let rep_header: R = rep.parse_header()?;
        Ok(rep_header)
    }

    pub fn create_snapshot_client(&self, id: u64) -> RaftResult<SyncClient> {
        let client = self.connector.create_sync_with_id(id)?;
        Ok(client)
    }
}
