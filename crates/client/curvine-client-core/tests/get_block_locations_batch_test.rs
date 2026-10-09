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
use curvine_core_error::ErrorExt;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{
    Path, RpcCode, GET_BLOCK_LOCATIONS_BATCH_MAX_ENCODED_PATH_BYTES,
    GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS,
};
use curvine_model::{FileBlocks, FileStatus};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::{
    CompatibilityModeProto, ComponentInfoProto, GetBlockLocationsBatchItem,
    GetBlockLocationsBatchRequest, GetBlockLocationsBatchResponse, GetFilesystemInfoResponse,
    ServerCompatibilityInfoProto,
};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::Message;
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::sync::FastRwLock;
use prost::Message as ProstMessage;
use std::sync::Arc;

#[derive(Clone, Copy)]
enum BatchMode {
    Normal,
    TooLarge,
    Unsupported,
}

struct MockState {
    advertise_batch: bool,
    mode: BatchMode,
    batch_calls: usize,
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
                let capabilities = if self.0.read().advertise_batch {
                    vec!["get-block-locations-batch".to_string()]
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
            RpcCode::GetBlockLocationsBatch => {
                let request: GetBlockLocationsBatchRequest = msg.parse_header()?;
                self.0.write().batch_calls += 1;
                let state = self.0.read();
                if matches!(state.mode, BatchMode::Unsupported) {
                    return Ok(msg.error_ext(&FsError::common("Unsupported operation")));
                }
                let response = match state.mode {
                    BatchMode::TooLarge => GetBlockLocationsBatchResponse {
                        too_large: Some(true),
                        ..Default::default()
                    },
                    BatchMode::Normal => GetBlockLocationsBatchResponse {
                        results: request
                            .paths
                            .into_iter()
                            .map(|path| {
                                if path == "/missing" {
                                    GetBlockLocationsBatchItem {
                                        path: path.clone(),
                                        blocks: None,
                                        error: Some(
                                            FsError::file_not_found(&path).encode().to_vec(),
                                        ),
                                    }
                                } else {
                                    let blocks = FileBlocks {
                                        status: FileStatus {
                                            path: path.clone(),
                                            is_complete: true,
                                            ..Default::default()
                                        },
                                        block_locs: Vec::new(),
                                    };
                                    GetBlockLocationsBatchItem {
                                        path,
                                        blocks: Some(curvine_model::ProtoUtils::file_blocks_to_pb(
                                            blocks,
                                        )),
                                        error: None,
                                    }
                                }
                            })
                            .collect(),
                        too_large: None,
                    },
                    BatchMode::Unsupported => unreachable!(),
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

fn paths(values: &[&str]) -> Vec<Path> {
    values
        .iter()
        .map(|value| Path::from_str(value).unwrap())
        .collect()
}

#[test]
fn batch_rpc_negotiates_capability_and_preserves_fallback_and_item_errors() {
    let state = Arc::new(FastRwLock::new(MockState {
        advertise_batch: true,
        mode: BatchMode::Normal,
        batch_calls: 0,
    }));
    let port = NetUtils::hold_available_port();
    let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
    server_conf.io_threads = 1;
    server_conf.worker_threads = 1;
    let mut server = RpcServer::new(server_conf, MockService(state.clone())).start();
    let client = new_client(port);
    let legacy_client = new_client(port);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        server.wait_startup().await.unwrap();

        let results = client
            .get_block_locations_batch(&paths(&["/first", "/missing", "/last"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().status.path, "/first");
        assert!(matches!(results[1], Err(FsError::FileNotFound(_))));
        assert_eq!(results[2].as_ref().unwrap().status.path, "/last");
        assert_eq!(state.read().batch_calls, 1);

        state.write().mode = BatchMode::TooLarge;
        assert!(client
            .get_block_locations_batch(&paths(&["/large-layout"]))
            .await
            .unwrap()
            .is_none());
        assert_eq!(state.read().batch_calls, 2);

        state.write().mode = BatchMode::Normal;
        let long_path = Path::from_str(format!(
            "/{}",
            "x".repeat(GET_BLOCK_LOCATIONS_BATCH_MAX_ENCODED_PATH_BYTES)
        ))
        .unwrap();
        assert!(client
            .get_block_locations_batch(&[long_path])
            .await
            .unwrap()
            .is_none());
        assert_eq!(state.read().batch_calls, 2);

        state.write().mode = BatchMode::Unsupported;
        assert!(client
            .get_block_locations_batch(&paths(&["/legacy-route"]))
            .await
            .unwrap()
            .is_none());
        assert_eq!(state.read().batch_calls, 3);

        state.write().mode = BatchMode::Normal;
        state.write().advertise_batch = false;
        assert!(legacy_client
            .get_block_locations_batch(&paths(&["/legacy-master"]))
            .await
            .unwrap()
            .is_none());
        assert_eq!(state.read().batch_calls, 3);

        let over_limit =
            vec![Path::from_str("/x").unwrap(); GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS + 1];
        assert!(matches!(
            client.get_block_locations_batch(&over_limit).await,
            Err(FsError::InvalidArgument(_))
        ));
    });

    drop(client);
    drop(legacy_client);
    drop(runtime);
}
