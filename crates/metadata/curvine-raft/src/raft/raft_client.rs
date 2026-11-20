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

use crate::conf::{JournalConf, JournalConfExt};
use crate::proto::raft::*;
use crate::raft::raft_error::RaftError;
use crate::raft::{LibRaftMessage, NodeId, RaftCode, RaftGroup, RaftResult};
use curvine_core_error::CommonResult;
use curvine_net::net::{InetAddr, NodeAddr};
use curvine_rpc::client::{ClientConf, ClusterConnector, RpcClient};
use curvine_rpc::message::{Builder, Message, RefMessage};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::warn;
use prost::Message as PMessage;
use raft::eraftpb::ConfChange;
use std::sync::Arc;
use std::time::Duration;

// Pacing between retry rounds of `send_propose_retry`. Each round already
// spends the connector's bounded retry budget before failing, so this only
// prevents a hot loop when nodes answer quickly with retryable errors.
const PROPOSE_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Raft message processing client.
/// Key points of core functions:
/// 1. Send a message.
/// 2. Select leader and switch leader.
/// 3. Automatically retry the exception and try to switch the leader or re-establish the connection.
#[derive(Clone)]
pub struct RaftClient {
    rt: Arc<Runtime>,
    connector: Arc<ClusterConnector>,

    /// Id of this node in the raft group. Proposes carry it in the header so
    /// the driver can tell which incarnation already mutated the namespace
    /// before proposing: only that node may skip the replay of the entry.
    node_id: NodeId,
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
            node_id: 0,
        }
    }

    pub fn from_conf(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        let group = RaftGroup::from_conf(conf);
        let mut client = Self::new(rt, &group, conf.new_client_conf());
        client.node_id = conf.node_id().unwrap_or(0);
        client
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
    pub async fn send_propose_response(&self, data: Vec<u8>) -> RaftResult<ProposeResponse> {
        let req = ProposeRequest {
            data,
            applied_node: Some(self.node_id),
        };
        let response: ProposeResponse = self.leader_rpc(RaftCode::Propose, req).await?;
        Ok(response)
    }

    // Send application layer messages.
    pub async fn send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        self.send_propose_response(data).await?;
        Ok(())
    }

    /// Send a propose and keep retrying transient failures with the same req id.
    ///
    /// Leader elections and loss of quorum routinely outlive the connector's
    /// bounded retry budget, and the journal writer must not lose its batch to
    /// such a transient failure: it used to abort the whole master, turning a
    /// recoverable election into a full-cluster outage. Every round carries
    /// the same req id, which the server dedups, so the batch can never be
    /// committed twice. Only non-transient errors (raft-internal failures and
    /// the catch-all `Other`, which includes journal apply failures) are
    /// returned to the caller.
    pub async fn send_propose_retry(&self, data: Vec<u8>) -> RaftResult<()> {
        let req_id = Utils::req_id();
        let req = ProposeRequest {
            data,
            // The retry may land on a new leader whose namespace never ran
            // this batch: the header tells every node that only this
            // incarnation pre-applied the mutation, so the new leader must
            // replay it instead of treating it as already applied.
            applied_node: Some(self.node_id),
        };

        loop {
            let msg = Builder::new_rpc(RaftCode::Propose)
                .req_id(req_id)
                .proto_header(req.clone())
                .build();

            let rep = match self.connector.rpc::<RaftError>(msg).await {
                Ok(rep) => rep,
                Err(e) if e.retry_propose() => {
                    warn!(
                        "propose({}) failed, retrying with the same req id: {}",
                        req_id, e
                    );
                    tokio::time::sleep(PROPOSE_RETRY_INTERVAL).await;
                    continue;
                }
                Err(e) => return Err(e),
            };

            let _: ProposeResponse = rep.parse_header()?;
            return Ok(());
        }
    }

    pub fn block_on_send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        self.rt.block_on(self.send_propose_retry(data))
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

    pub async fn create_snapshot_client(&self, id: u64) -> RaftResult<RpcClient> {
        let client = self.connector.create_client_with_id(id, false).await?;
        Ok(client)
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
        self.connector.get_addr_string(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::RaftPeer;
    use curvine_net::net::NetUtils;

    // A propose to an unreachable cluster must stay in the retry loop instead
    // of failing after the connector's bounded retry budget: the journal
    // writer relies on this to survive a transient loss of quorum without
    // aborting the master.
    #[test]
    fn send_propose_retry_keeps_retrying_unreachable_cluster() {
        let rt = Arc::new(Runtime::new("propose-retry-test", 2, 2));
        let mut conf = JournalConf::with_test();
        // An available (and therefore unbound) port: nothing is listening.
        let port = NetUtils::get_available_port();
        conf.journal_addrs = vec![RaftPeer::new(1, &conf.hostname, port)];
        // Shrink the connector's bounded retry budget so the plain rpc fails
        // within the test: this proves the budget really expires here, and
        // that the retry loop below survives only because it re-arms the
        // budget on every round instead of returning the failure.
        conf.rpc_retry_max_duration_ms = 500;
        conf.rpc_retry_min_sleep_ms = 50;
        conf.rpc_retry_max_sleep_ms = 100;

        // The bounded form must fail once the budget is exhausted.
        let bounded = RaftClient::from_conf(rt.clone(), &conf);
        let bounded_result = rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), bounded.send_propose(vec![1])).await
        });
        let bounded_result = bounded_result
            .expect("bounded send_propose must fail within its retry budget, not hang");
        assert!(
            bounded_result.is_err(),
            "a propose to an unreachable cluster fails after the connector budget is spent"
        );

        // The retry form re-arms the budget with the same req id on every
        // round, so it must still be pending across several retry intervals.
        let retrying = RaftClient::from_conf(rt.clone(), &conf);
        let mut task = rt.spawn(async move { retrying.send_propose_retry(vec![1]).await });

        let still_retrying = rt.block_on(async {
            tokio::time::timeout(PROPOSE_RETRY_INTERVAL * 3, &mut task)
                .await
                .is_err()
        });
        assert!(
            still_retrying,
            "a propose to an unreachable cluster must keep retrying across connector budgets, not fail fast"
        );

        // Clean up so the runtime does not wait for the infinite retry loop.
        task.abort();
        let _ = rt.block_on(task);
    }
}
