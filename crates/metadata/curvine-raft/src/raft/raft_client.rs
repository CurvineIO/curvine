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
use curvine_core_error::CommonResult;
use curvine_net::net::InetAddr;
use curvine_rpc::client::ClientConf;
use curvine_rpc::message::{Builder, Message, RefMessage};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::{stream::FuturesUnordered, StreamExt};
use prost::Message as PMessage;
use raft::eraftpb::ConfChange;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

/// Raft message processing client.
/// Key points of core functions:
/// 1. Send a message.
/// 2. Select leader and switch leader.
/// 3. Automatically retry the exception and try to switch the leader or re-establish the connection.
#[derive(Clone)]
pub struct RaftClient {
    rt: Arc<Runtime>,
    conf: ClientConf,
    addresses: Arc<RwLock<HashMap<u64, InetAddr>>>,
    leader: Arc<AtomicU64>,
    peer_ids: Arc<RwLock<Vec<u64>>>,
    grpc_channels: Arc<RwLock<HashMap<u64, tonic::transport::Channel>>>,
}

impl RaftClient {
    pub fn new(rt: Arc<Runtime>, group: &RaftGroup, conf: ClientConf) -> Self {
        assert!(!group.peers.contains_key(&0), "Raft node id cannot be zero");
        Self {
            rt,
            conf,
            addresses: Arc::new(RwLock::new(
                group
                    .peers
                    .iter()
                    .map(|(&id, peer)| (id, peer.to_addr()))
                    .collect(),
            )),
            leader: Arc::new(AtomicU64::new(0)),
            peer_ids: Arc::new(RwLock::new(group.peers.keys().copied().collect())),
            grpc_channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_conf(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        let group = RaftGroup::from_conf(conf);
        Self::new(rt, &group, conf.new_client_conf())
    }

    pub fn add_node(&self, id: u64, addr: &InetAddr) -> RaftResult<()> {
        if id == 0 {
            return Err(RaftError::other("Raft node id cannot be zero".into()));
        }
        self.addresses
            .write()
            .expect("Raft address map poisoned")
            .insert(id, addr.clone());
        self.grpc_channels
            .write()
            .expect("Raft channel map poisoned")
            .remove(&id);
        let mut ids = self.peer_ids.write().expect("Raft peer list poisoned");
        if !ids.contains(&id) {
            ids.push(id);
        }
        Ok(())
    }

    // Cluster configuration modification, usually used to join the cluster with new nodes.
    pub async fn conf_change(&self, req: ConfChange) -> RaftResult<()> {
        let _: ConfChangeResponse = self
            .leader_rpc(
                RaftCode::ConfChange,
                ConfChangeRequest { change: req.into() },
            )
            .await?;
        Ok(())
    }

    // Send raft internal message.
    pub async fn send_raft(&self, req: LibRaftMessage) -> CommonResult<()> {
        let _: RaftResponse = self
            .leader_rpc(
                RaftCode::Raft,
                RaftRequest {
                    message: req.into(),
                    sender_session: None,
                    receiver_session: None,
                    leader_commit: None,
                },
            )
            .await?;
        Ok(())
    }

    // Send application layer messages.
    pub async fn send_propose_response(&self, data: Vec<u8>) -> RaftResult<ProposeResponse> {
        let req = ProposeRequest { data };
        let response: ProposeResponse = self.leader_rpc(RaftCode::Propose, req).await?;
        Ok(response)
    }

    // Send application layer messages.
    pub async fn send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        self.send_propose_response(data).await?;
        Ok(())
    }

    pub fn block_on_send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        self.rt.block_on(self.send_propose(data))
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
        let msg = Builder::new_rpc(code).proto_header(header).build();
        self.grpc_leader_rpc(msg)
            .await?
            .parse_header()
            .map_err(Into::into)
    }

    pub async fn timeout_rpc<R>(
        &self,
        id: u64,
        msg: impl RefMessage,
    ) -> Result<R, (bool, RaftError)>
    where
        R: PMessage + Default,
    {
        match self.grpc_rpc(id, msg.as_ref()).await {
            Ok(rep) => rep.parse_header::<R>().map_err(|e| (false, e.into())),
            Err(e) => Err(e),
        }
    }

    pub async fn retry_rpc<R>(&self, id: u64, msg: Message) -> RaftResult<R>
    where
        R: PMessage + Default,
    {
        let rep = self.grpc_retry_rpc(id, &msg).await?;
        let rep_header: R = rep.parse_header()?;
        Ok(rep_header)
    }

    async fn grpc_rpc(&self, id: u64, message: &Message) -> Result<Message, (bool, RaftError)> {
        let channel = self.grpc_channel(id).map_err(|e| (false, e))?;
        let mut client = crate::proto::raft::raft_service_client::RaftServiceClient::new(channel)
            .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
        macro_rules! invoke {
            ($header:ty, $method:ident) => {{
                let header: $header = message
                    .parse_header()
                    .map_err(|e| (false, RaftError::from(e)))?;
                let mut request = tonic::Request::new(header);
                curvine_rpc::grpc::set_request_id(&mut request, message.req_id());
                match client.$method(request).await {
                    Ok(response) => Ok(Builder::success(message)
                        .proto_header(response.into_inner())
                        .build()),
                    Err(status) => {
                        let transport_retry = matches!(
                            status.code(),
                            tonic::Code::Unavailable
                                | tonic::Code::DeadlineExceeded
                                | tonic::Code::Cancelled
                        );
                        let error = crate::raft::grpc_error::from_status(status);
                        let retry = transport_retry || error.retry_leader();
                        Err((retry, error))
                    }
                }
            }};
        }
        match RaftCode::from(message.code()) {
            RaftCode::Ping => invoke!(PingRequest, ping),
            RaftCode::ConfChange => invoke!(ConfChangeRequest, conf_change),
            RaftCode::Propose => invoke!(ProposeRequest, propose),
            RaftCode::Raft => invoke!(RaftRequest, send_raft),
            RaftCode::ReportSnapshot => invoke!(ReportSnapshotRequest, report_snapshot),
            code => Err((
                false,
                RaftError::other(format!("unsupported Raft gRPC operation {code:?}").into()),
            )),
        }
    }

    fn grpc_channel(&self, id: u64) -> RaftResult<tonic::transport::Channel> {
        if let Some(channel) = self
            .grpc_channels
            .read()
            .expect("Raft channel map poisoned")
            .get(&id)
        {
            return Ok(channel.clone());
        }
        let mut channels = self
            .grpc_channels
            .write()
            .expect("Raft channel map poisoned");
        if let Some(channel) = channels.get(&id) {
            return Ok(channel.clone());
        }
        let addr = self.address(id)?;
        let port = addr.port;
        let host = &addr.hostname;
        let tls = self.conf.grpc.client_tls(host).map_err(RaftError::from)?;
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: std::time::Duration::from_millis(self.conf.conn_timeout_ms),
            request_timeout: Some(std::time::Duration::from_millis(self.conf.rpc_timeout_ms)),
            ..Default::default()
        };
        let endpoint = transport
            .endpoint(self.conf.grpc.uri(host, port), tls.as_ref())
            .map_err(|e| RaftError::other(e.to_string().into()))?;
        let channel = endpoint.connect_lazy();
        Ok(channels.entry(id).or_insert(channel).clone())
    }

    async fn grpc_retry_rpc(&self, id: u64, message: &Message) -> RaftResult<Message> {
        let mut policy = self.conf.io_retry_policy();
        let mut last_error = None;
        while policy.attempt().await {
            match self.grpc_rpc(id, message).await {
                Ok(reply) => return Ok(reply),
                Err((false, error)) => return Err(error),
                Err((true, error)) => last_error = Some(error),
            }
        }
        Err(RaftError::with_opt_msg(
            last_error,
            "Raft gRPC retry deadline expired",
        ))
    }

    async fn grpc_leader_rpc(&self, message: Message) -> RaftResult<Message> {
        let mut policy = self.conf.io_retry_policy();
        let mut last_error = None;
        let leader = self.leader.load(Ordering::SeqCst);
        if leader != 0 {
            match self.grpc_rpc(leader, &message).await {
                Ok(reply) => return Ok(reply),
                Err((false, error)) => return Err(error),
                Err((true, error)) => last_error = Some(error),
            }
            self.leader.store(0, Ordering::SeqCst);
        }
        while policy.attempt().await {
            let ids = self
                .peer_ids
                .read()
                .expect("Raft peer list poisoned")
                .clone();
            if ids.is_empty() {
                return Err(RaftError::other("no Raft peers configured".into()));
            }
            let requests = FuturesUnordered::new();
            for id in ids {
                let request = &message;
                requests.push(async move { (id, self.grpc_rpc(id, request).await) });
            }
            tokio::pin!(requests);
            while let Some((id, result)) = requests.next().await {
                match result {
                    Ok(reply) => {
                        self.leader.store(id, Ordering::SeqCst);
                        return Ok(reply);
                    }
                    Err((false, error)) => return Err(error),
                    Err((true, error)) => {
                        last_error = Some(error);
                    }
                }
            }
        }
        Err(RaftError::with_opt_msg(
            last_error,
            "Raft gRPC leader retry deadline expired",
        ))
    }

    pub async fn snapshot_client(
        &self,
        id: u64,
        dir: String,
        file: SnapshotFileInfo,
    ) -> RaftResult<crate::raft::snapshot::SnapshotClient> {
        let addr = self.address(id)?;
        let port = addr.port;
        let host = &addr.hostname;
        let tls = self.conf.grpc.client_tls(host).map_err(RaftError::from)?;
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: std::time::Duration::from_millis(self.conf.conn_timeout_ms),
            request_timeout: None,
            ..Default::default()
        };
        let channel = transport
            .endpoint(self.conf.grpc.uri(host, port), tls.as_ref())
            .map_err(|e| RaftError::other(e.to_string().into()))?
            .connect()
            .await
            .map_err(|e| RaftError::other(e.to_string().into()))?;
        Ok(crate::raft::snapshot::SnapshotClient::new_grpc(
            dir,
            file,
            InetAddr::new(host.clone(), port),
            channel,
            std::time::Duration::from_millis(self.conf.data_timeout_ms),
        ))
    }

    pub fn report_snapshot(&self, snapshot: SnapshotData) -> RaftResult<bool> {
        let fut = async move {
            let req = ReportSnapshotRequest { snapshot };
            let rep: ReportSnapshotResponse =
                self.leader_rpc(RaftCode::ReportSnapshot, req).await?;
            Ok(rep.accepted)
        };
        self.rt.block_on(fut)
    }

    pub fn get_addr_string(&self, id: u64) -> String {
        self.address(id)
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "None".to_string())
    }

    fn address(&self, id: u64) -> RaftResult<InetAddr> {
        self.addresses
            .read()
            .expect("Raft address map poisoned")
            .get(&id)
            .cloned()
            .ok_or_else(|| RaftError::other(format!("Raft node {id} does not exist").into()))
    }
}
