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

use bytes::BytesMut;
use curvine_client_core::file::{FsClient, FsContext};
use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, RpcCode, MASTER_CAPABILITY_GET_CONTENT_SUMMARY_SNAPSHOT};
use curvine_model::{ContentSummary, ContentSummarySource, ProtoUtils};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::{
    CompatibilityModeProto, ComponentInfoProto, GetContentSummaryRequest,
    GetContentSummaryResponse, GetFilesystemInfoResponse, ServerCompatibilityInfoProto,
};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::Message;
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::sync::FastRwLock;
use prost::Message as ProstMessage;
use std::sync::Arc;

struct MockState {
    advertise_capability: bool,
    rpc_calls: usize,
}

struct MockService(Arc<FastRwLock<MockState>>);

struct MockHandler(Arc<FastRwLock<MockState>>);

impl HandlerService for MockService {
    type Item = MockHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        MockHandler(self.0.clone())
    }
}

impl MessageHandler for MockHandler {
    type Error = FsError;

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        let header = match RpcCode::from(msg.code()) {
            RpcCode::GetFilesystemInfo => {
                let capabilities = if self.0.read().advertise_capability {
                    vec![MASTER_CAPABILITY_GET_CONTENT_SUMMARY_SNAPSHOT.to_string()]
                } else {
                    Vec::new()
                };
                let response = GetFilesystemInfoResponse {
                    compatibility: Some(ServerCompatibilityInfoProto {
                        server: ComponentInfoProto {
                            capabilities,
                            ..Default::default()
                        },
                        compatibility_mode: CompatibilityModeProto::Diagnose as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                response.encode_to_vec()
            }
            RpcCode::GetContentSummary => {
                let _request: GetContentSummaryRequest = msg.parse_header()?;
                self.0.write().rpc_calls += 1;
                let summary = ContentSummary::live_file(128);
                let response = GetContentSummaryResponse {
                    summary: Some(ProtoUtils::content_summary_to_pb(summary)),
                };
                response.encode_to_vec()
            }
            _ => return Ok(msg.error_ext(&FsError::common("Unsupported operation"))),
        };
        Ok(msg.success_with_header(BytesMut::from(header.as_slice())))
    }
}

fn new_client(port: u16) -> FsClient {
    let mut conf = ClusterConf::default();
    conf.master.hostname = "127.0.0.1".to_string();
    conf.master.rpc_port = port;
    FsClient::new(Arc::new(FsContext::new(conf).unwrap()))
}

#[test]
fn content_summary_without_capability_is_unsupported() {
    let state = Arc::new(FastRwLock::new(MockState {
        advertise_capability: false,
        rpc_calls: 0,
    }));
    let port = NetUtils::hold_available_port();
    let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
    server_conf.io_threads = 1;
    server_conf.worker_threads = 1;
    let mut server = RpcServer::new(server_conf, MockService(state.clone())).start();
    let client = new_client(port);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        server.wait_startup().await.unwrap();
        let err = client
            .get_content_summary(&Path::from_str("/a").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, FsError::Unsupported(_)));
        assert_eq!(state.read().rpc_calls, 0);
    });
}

#[test]
fn content_summary_uses_rpc_when_capability_present() {
    let state = Arc::new(FastRwLock::new(MockState {
        advertise_capability: true,
        rpc_calls: 0,
    }));
    let port = NetUtils::hold_available_port();
    let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
    server_conf.io_threads = 1;
    server_conf.worker_threads = 1;
    let mut server = RpcServer::new(server_conf, MockService(state.clone())).start();
    let client = new_client(port);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        server.wait_startup().await.unwrap();
        let summary = client
            .get_content_summary(&Path::from_str("/a").unwrap())
            .await
            .unwrap();
        assert_eq!(summary.logical_bytes, 128);
        assert_eq!(summary.source, Some(ContentSummarySource::LiveFile));
        assert_eq!(state.read().rpc_calls, 1);
    });
}
