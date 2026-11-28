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

use curvine_client_core::block::BlockWriterRemote;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, StorageType, WorkerAddress};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::{BlockWriteRequest, BlockWriteResponse};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use prost::Message as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type Observed = Arc<Mutex<Vec<(usize, RequestStatus)>>>;

struct RemoteService {
    next_connection: AtomicUsize,
    malformed_open: Arc<AtomicBool>,
    observed: Observed,
}

impl HandlerService for RemoteService {
    type Item = RemoteHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        RemoteHandler {
            connection: self.next_connection.fetch_add(1, Ordering::SeqCst),
            malformed_open: self.malformed_open.clone(),
            observed: self.observed.clone(),
        }
    }
}

struct RemoteHandler {
    connection: usize,
    malformed_open: Arc<AtomicBool>,
    observed: Observed,
}

impl MessageHandler for RemoteHandler {
    type Error = curvine_core_error::CommonErrorExt;

    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        if RpcCode::from(msg.code()) == RpcCode::WriteBlock {
            let status = msg.request_status();
            self.observed
                .lock()
                .unwrap()
                .push((self.connection, status));
            if status == RequestStatus::Open {
                let request: BlockWriteRequest = msg.parse_header()?;
                // A valid RPC envelope with a truncated protobuf header:
                // the RPC has no outstanding reply when Open decoding fails.
                let header = if self.malformed_open.swap(false, Ordering::SeqCst) {
                    vec![0x80]
                } else {
                    BlockWriteResponse {
                        id: request.block.id,
                        path: None,
                        off: request.off,
                        block_size: request.block_size,
                        storage_type: StorageType::Disk.into(),
                        pipeline_status: None,
                    }
                    .encode_to_vec()
                };
                return Ok(msg.success_with_data(
                    Some(bytes::BytesMut::from(header.as_slice())),
                    DataSlice::empty(),
                ));
            }
        }
        Ok(msg.success_with_data(None, DataSlice::empty()))
    }
}

struct TestEnv {
    context: Arc<FsContext>,
    worker: WorkerAddress,
    observed: Observed,
}

impl TestEnv {
    fn new(malformed_open: bool) -> Self {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let port = NetUtils::hold_available_port();
        let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
        server_conf.io_threads = 1;
        server_conf.worker_threads = 1;
        let server = RpcServer::new(
            server_conf,
            RemoteService {
                next_connection: AtomicUsize::new(0),
                malformed_open: Arc::new(AtomicBool::new(malformed_open)),
                observed: observed.clone(),
            },
        );
        std::thread::spawn(move || server.block_on_start());
        let mut conf = ClusterConf::default();
        conf.client.conn_timeout_ms = 500;
        conf.client.conn_retry_max_duration_ms = 2000;
        conf.client.conn_retry_min_sleep_ms = 10;
        conf.client.conn_retry_max_sleep_ms = 50;
        Self {
            context: Arc::new(
                FsContext::with_rt(conf, Arc::new(Runtime::new("remote-write-lifecycle", 1, 1)))
                    .unwrap(),
            ),
            worker: WorkerAddress {
                worker_id: 1,
                hostname: "127.0.0.1".into(),
                ip_addr: "127.0.0.1".into(),
                rpc_port: u32::from(port),
                web_port: 1,
            },
            observed,
        }
    }

    async fn writer(&self) -> curvine_error::FsResult<BlockWriterRemote> {
        BlockWriterRemote::new(
            &self.context,
            ExtendedBlock::new(1, 0, StorageType::Disk, FileType::File),
            self.worker.clone(),
            0,
            64,
        )
        .await
    }

    fn assert_open_connections(&self, expected: &[usize]) {
        let observed = self.observed.lock().unwrap();
        let connections: Vec<_> = observed
            .iter()
            .filter(|(_, status)| *status == RequestStatus::Open)
            .map(|(connection, _)| *connection)
            .collect();
        assert_eq!(connections, expected, "observed requests: {observed:?}");
    }
}

#[test]
fn dropped_unfinished_remote_writer_retires_idle_rpc_connection() {
    let env = TestEnv::new(false);
    env.context.clone_runtime().block_on(async {
        let writer = env.writer().await.unwrap();
        // Open succeeded and its reply has been fully consumed. Only the
        // writer's Drop guard knows that this idle RPC still owns a write lease.
        drop(writer);
        let mut next = env.writer().await.unwrap();
        env.assert_open_connections(&[0, 1]);
        next.cancel().await.unwrap();
    });
}

#[test]
fn malformed_remote_open_retires_fully_received_rpc_connection() {
    let env = TestEnv::new(true);
    env.context.clone_runtime().block_on(async {
        assert!(env.writer().await.is_err());
        let mut next = env.writer().await.unwrap();
        env.assert_open_connections(&[0, 1]);
        // A malformed reply must not trigger a speculative block deletion.
        assert_eq!(env.observed.lock().unwrap().len(), 2);
        next.cancel().await.unwrap();
    });
}

fn acknowledged_remote_terminal_rpc_preserves_reuse(cancel: bool) {
    let env = TestEnv::new(false);
    env.context.clone_runtime().block_on(async {
        let mut writer = env.writer().await.unwrap();
        if cancel {
            writer.cancel().await.unwrap();
        } else {
            writer.complete().await.unwrap();
        }
        drop(writer);
        let mut next = env.writer().await.unwrap();
        env.assert_open_connections(&[0, 0]);
        next.cancel().await.unwrap();
        drop(next);
        let mut third = env.writer().await.unwrap();
        env.assert_open_connections(&[0, 0, 0]);
        third.cancel().await.unwrap();
    });
}

#[test]
fn successful_remote_complete_keeps_connection_poolable() {
    acknowledged_remote_terminal_rpc_preserves_reuse(false);
}

#[test]
fn successful_remote_cancel_keeps_connection_poolable() {
    acknowledged_remote_terminal_rpc_preserves_reuse(true);
}
