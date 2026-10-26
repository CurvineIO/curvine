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
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_net::net::NodeAddr;
use curvine_rpc::message::MessageBuilder;
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
    /// and must discard commands in mirror responses.
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
        let message = MessageBuilder::new_rpc(code).proto_header(request).build();
        let response = self
            .context()
            .connector
            .rpc_to_peer::<FsError>(peer_id, message)
            .await?;
        Ok(response.parse_header()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FsContext;
    use curvine_config::ClusterConf;
    use curvine_core_error::{CommonErrorExt, CommonResultExt};
    use curvine_net::net::{ConnState, InetAddr, NetUtils};
    use curvine_proto::WorkerHeartbeatRequest;
    use curvine_rpc::handler::{HandlerService, MessageHandler};
    use curvine_rpc::message::{Message as RpcMessage, MessageBuilder};
    use curvine_rpc::server::{RpcServer, ServerConf};
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
    use std::sync::Arc;

    struct EchoHeartbeat;

    impl HandlerService for EchoHeartbeat {
        type Item = EchoHeartbeat;

        fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
            EchoHeartbeat
        }
    }

    impl MessageHandler for EchoHeartbeat {
        type Error = CommonErrorExt;

        fn handle(&self, request: &RpcMessage) -> CommonResultExt<RpcMessage> {
            // Any automatic GetFilesystemInfo handshake would fail this test.
            assert_eq!(request.code(), i8::from(RpcCode::WorkerHeartbeat));
            let header: WorkerHeartbeatRequest = request.parse_header()?;
            Ok(MessageBuilder::success_with_header(request, header).build())
        }
    }

    #[test]
    fn typed_peer_rpc_does_not_handshake_or_replace_cached_leader() {
        let rt = Arc::new(AsyncRuntime::single());
        let port = NetUtils::hold_available_port();
        let mut rpc_conf = ServerConf::with_hostname("127.0.0.1", port);
        rpc_conf.io_threads = 1;
        rpc_conf.worker_threads = 1;
        let server = RpcServer::with_rt(rt.clone(), rpc_conf, EchoHeartbeat);
        let mut status = server.start();
        rt.block_on(status.wait_running()).unwrap();

        let mut conf = ClusterConf::default();
        conf.client.hostname = "127.0.0.1".to_string();
        conf.client.master_addrs = vec![
            InetAddr::new("127.0.0.1", 1),
            InetAddr::new("127.0.0.1", port),
        ];
        let context = Arc::new(FsContext::with_rt(conf, rt.clone()).unwrap());
        context.connector.change_leader(100);
        let client = FsClient::new(context);
        let peers = client.master_peer_snapshot();
        assert_eq!(
            peers.iter().map(|peer| peer.id).collect::<Vec<_>>(),
            [100, 101]
        );

        let request = WorkerHeartbeatRequest {
            worker_id: 42,
            worker_session_id: Some("typed-mirror-test".to_string()),
            ..Default::default()
        };
        let response: WorkerHeartbeatRequest = rt
            .block_on(client.rpc_to_master_peer(101, RpcCode::WorkerHeartbeat, request))
            .unwrap();
        assert_eq!(response.worker_id, 42);
        assert_eq!(
            response.worker_session_id.as_deref(),
            Some("typed-mirror-test")
        );
        assert_eq!(client.master_peer_leader_id(), Some(100));
        assert!(!client.context().handshake_started());
    }
}
