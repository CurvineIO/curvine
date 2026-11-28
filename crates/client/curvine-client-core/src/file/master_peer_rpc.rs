// Copyright 2026 OPPO.
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

use super::FsClient;
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_net::net::NodeAddr;
use curvine_rpc::message::{MessageBuilder, WORKER_MIRROR_SEQ_ID};
use prost::Message;

impl FsClient {
    /// Snapshot candidate master identities without selecting a leader.
    pub fn master_peer_snapshot(&self) -> Vec<NodeAddr> {
        self.context().connector.peer_snapshot()
    }

    /// Return the hint learned by the normal leader-routed RPC path.
    pub fn master_peer_leader_id(&self) -> Option<u64> {
        self.context().connector.leader_id()
    }

    /// Send a typed request to one master without changing the leader hint.
    ///
    /// This intentionally does not run a handshake against the selected peer.
    /// Worker mirror callers first complete their normal leader-routed request
    /// and must discard commands in mirror responses. Heartbeat and block
    /// report peer calls carry the explicit worker-mirror marker.
    pub async fn rpc_to_master_peer<T, R>(
        &self,
        peer_id: u64,
        code: RpcCode,
        request: T,
    ) -> FsResult<R>
    where
        T: Message + Default,
        R: Message + Default,
    {
        let mut builder = MessageBuilder::new_rpc(code);
        if matches!(code, RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport) {
            builder = builder.seq_id(WORKER_MIRROR_SEQ_ID);
        }
        let message = builder.proto_header(request).build();
        let response = self
            .context()
            .grpc_master
            .rpc_to_peer(peer_id, message)
            .await?;
        Ok(response.parse_header()?)
    }
}
