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

//! Integration wiring test for `BlockWriter::new` (issue #1648).
//!
//! Each test owns an isolated RPC server whose handler accepts the
//! `WriteBlock` Open of the first replica and records every `WriteBlock`
//! request status afterwards. The second replica points at an unreachable
//! address so its open fails after the first replica opened fully. One test
//! pins that a fresh-block open failure sends exactly one cancel commit;
//! the other pins that a committed-block reopen failure sends none (the
//! rollback gate in `BlockWriter::new`).

use curvine_client_core::block::BlockWriter;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, LocatedBlock, StorageType, WorkerAddress};
use curvine_net::net::ConnState;
use curvine_proto::BlockWriteResponse;
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct RecordingService {
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
}

impl HandlerService for RecordingService {
    type Item = RecordingHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        RecordingHandler {
            observed: self.observed.clone(),
        }
    }
}

struct RecordingHandler {
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
}

impl MessageHandler for RecordingHandler {
    type Error = curvine_core_error::CommonErrorExt;

    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        let code = RpcCode::from(msg.code());
        let status = msg.request_status();
        if code == RpcCode::WriteBlock {
            self.observed.lock().unwrap().push((msg.req_id(), status));
        }

        if code == RpcCode::WriteBlock && status == RequestStatus::Open {
            // Respond with a writable block context so the first replica
            // opens fully.
            let rep = BlockWriteResponse {
                id: 1,
                path: Some("/tmp/curvine-test-block".to_string()),
                off: 0,
                block_size: 64,
                storage_type: StorageType::Disk.into(),
                pipeline_status: None,
            };
            use prost::Message as _;
            let header = bytes::BytesMut::from(&rep.encode_to_vec()[..]);
            return Ok(msg.success_with_data(Some(header), DataSlice::empty()));
        }

        Ok(msg.success_with_data(None, DataSlice::empty()))
    }
}

/// One test = one server + one observation buffer, so parallel tests never
/// observe each other's requests.
struct TestEnv {
    context: Arc<FsContext>,
    worker: WorkerAddress,
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
}

impl TestEnv {
    fn new() -> Self {
        let observed: Arc<Mutex<Vec<(i64, RequestStatus)>>> = Arc::new(Mutex::new(Vec::new()));
        let port = curvine_net::net::NetUtils::hold_available_port();
        let conf = ServerConf::with_hostname("127.0.0.1", port);
        let service = RecordingService {
            observed: observed.clone(),
        };
        let server = RpcServer::new(conf, service);
        let rt = server.clone_rt();
        let port = server.bind_addr().port;
        std::thread::spawn(move || {
            server.block_on_start();
        });
        let worker = WorkerAddress {
            worker_id: 1,
            hostname: "127.0.0.1".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: port as u32,
            web_port: 8000,
        };

        let mut client_conf = ClusterConf::default();
        client_conf.client.short_circuit = false;
        // Fail fast on the unreachable second replica instead of retrying
        // for the default 2 minutes.
        client_conf.client.conn_retry_max_duration_ms = 500;
        client_conf.client.conn_retry_min_sleep_ms = 50;
        client_conf.client.conn_retry_max_sleep_ms = 100;
        client_conf.client.conn_timeout_ms = 500;
        client_conf.client.rpc_retry_max_duration_ms = 500;
        client_conf.client.rpc_retry_min_sleep_ms = 50;
        client_conf.client.rpc_retry_max_sleep_ms = 100;
        client_conf.client.rpc_timeout_ms = 500;
        let context = Arc::new(FsContext::with_rt(client_conf, rt).unwrap());

        Self {
            context,
            worker,
            observed,
        }
    }

    fn unreachable_second_replica(&self) -> WorkerAddress {
        WorkerAddress {
            worker_id: 2,
            hostname: "127.0.0.1".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 1, // nothing listens here
            web_port: 1,
        }
    }

    fn located_block(&self, block: ExtendedBlock) -> LocatedBlock {
        LocatedBlock::new(
            block,
            vec![self.worker.clone(), self.unreachable_second_replica()],
        )
    }

    async fn wait_ready(&self) {
        for _ in 0..50 {
            if self.context.block_client(&self.worker).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("test server failed to start within 5 seconds");
    }

    fn observed(&self) -> Vec<(i64, RequestStatus)> {
        self.observed.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn new_block_failure_cancels_opened_replicas_exactly_once() {
    let env = TestEnv::new();
    env.wait_ready().await;

    // len = 0: a freshly allocated block, exactly what the new-block path of
    // `FsWriterBase::get_writer` hands to `BlockWriter::new`. Its open
    // failure must roll back the already opened replicas with cancel commits.
    let block = ExtendedBlock::new(1, 0, StorageType::Ssd, FileType::File);

    let result = BlockWriter::new(env.context.clone(), env.located_block(block), 0, 64).await;
    let err = match result {
        Ok(_) => panic!("second replica open must fail"),
        Err(e) => e,
    };

    // The original open error of the failing replica must be the primary
    // error, not a cancellation error.
    let err_msg = err.to_string();
    assert!(
        !err_msg.contains("failed to cancel"),
        "primary error must be the original open error, got: {err_msg}"
    );

    let cancels = env
        .observed()
        .into_iter()
        .filter(|(_, status)| *status == RequestStatus::Cancel)
        .count();
    assert_eq!(
        cancels,
        1,
        "the opened replica must receive exactly one cancel commit, observed: {:?}",
        env.observed()
    );
}

#[tokio::test]
async fn committed_reopen_failure_does_not_cancel_opened_replicas() {
    let env = TestEnv::new();
    env.wait_ready().await;

    // A committed block (len > 0) reopened for append/overwrite through
    // `FsWriterBase::get_writer` must NOT be rolled back with cancel commits
    // on a later open failure: the already opened replica may hold committed
    // data and a cancel would abort it on the worker.
    let block = ExtendedBlock::new(1, 64, StorageType::Ssd, FileType::File);

    let result = BlockWriter::new(env.context.clone(), env.located_block(block), 0, 64).await;
    assert!(
        result.is_err(),
        "second replica open must fail for a committed reopen too"
    );

    let cancels = env
        .observed()
        .into_iter()
        .filter(|(_, status)| *status == RequestStatus::Cancel)
        .count();
    assert_eq!(
        cancels,
        0,
        "committed reopen must not send cancel commits, observed: {:?}",
        env.observed()
    );
}
