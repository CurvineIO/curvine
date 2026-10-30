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
//! A custom in-process RPC handler accepts the `WriteBlock` Open of the first
//! replica and records every `WriteBlock` request status afterwards. The
//! second replica points at an unreachable address so its open fails after
//! the first replica opened fully. The test pins the real call site: if
//! `BlockWriter::new` reverted to the old `?` loop, no cancel commit would
//! reach the first worker and the assertion below would fail.

use curvine_client_core::block::BlockWriter;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, LocatedBlock, StorageType, WorkerAddress};
use curvine_net::net::ConnState;
use curvine_proto::BlockWriteResponse;
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::RequestStatus;
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::Runtime;
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// WriteBlock requests observed by the test handler, as (req_id, status).
static OBSERVED_REQUESTS: Lazy<Mutex<Vec<(i64, RequestStatus)>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

struct RecordingHandler;

impl MessageHandler for RecordingHandler {
    type Error = curvine_core_error::CommonErrorExt;

    fn handle(
        &self,
        msg: &curvine_rpc::message::Message,
    ) -> Result<curvine_rpc::message::Message, Self::Error> {
        let code = RpcCode::from(msg.code());
        let status = msg.request_status();
        if code == RpcCode::WriteBlock {
            OBSERVED_REQUESTS
                .lock()
                .unwrap()
                .push((msg.req_id(), status));
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

struct RecordingService;

impl HandlerService for RecordingService {
    type Item = RecordingHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        RecordingHandler
    }
}

static TEST_SERVER: Lazy<(WorkerAddress, Arc<Runtime>)> = Lazy::new(|| {
    let port = curvine_net::net::NetUtils::hold_available_port();
    let conf = ServerConf::with_hostname("127.0.0.1", port);
    let server = RpcServer::new(conf, RecordingService);
    let rt = server.clone_rt();
    let addr_port = server.bind_addr().port;
    std::thread::spawn(move || {
        server.block_on_start();
    });
    (
        WorkerAddress {
            worker_id: 1,
            hostname: "127.0.0.1".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: addr_port as u32,
            web_port: 8000,
        },
        rt,
    )
});

fn create_test_context() -> Arc<FsContext> {
    let mut conf = ClusterConf::default();
    conf.client.short_circuit = false;
    // Fail fast on the unreachable second replica instead of retrying for
    // the default 2 minutes.
    conf.client.conn_retry_max_duration_ms = 500;
    conf.client.conn_retry_min_sleep_ms = 50;
    conf.client.conn_retry_max_sleep_ms = 100;
    conf.client.conn_timeout_ms = 500;
    conf.client.rpc_retry_max_duration_ms = 500;
    conf.client.rpc_retry_min_sleep_ms = 50;
    conf.client.rpc_retry_max_sleep_ms = 100;
    conf.client.rpc_timeout_ms = 500;
    Arc::new(FsContext::with_rt(conf, TEST_SERVER.1.clone()).unwrap())
}

fn located_block(second_addr: WorkerAddress) -> LocatedBlock {
    // len = 0: a freshly allocated block, exactly what the new-block path of
    // `FsWriterBase::get_writer` hands to `BlockWriter::new`. Only this case
    // must roll back with cancel commits on partial open failure.
    let block = ExtendedBlock::new(1, 0, StorageType::Ssd, FileType::File);
    LocatedBlock::new(block, vec![TEST_SERVER.0.clone(), second_addr])
}

#[tokio::test]
async fn committed_reopen_failure_does_not_cancel_opened_replicas() {
    let context = create_test_context();

    let mut ready = false;
    for _ in 0..50 {
        if context.block_client(&TEST_SERVER.0).await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "test server failed to start within 5 seconds");

    // A committed reopen (len > 0, e.g. append/overwrite through
    // `FsWriterBase::get_writer`) must NOT cancel the replicas opened before
    // a later failure: the first replica may already hold committed data and
    // a cancel commit would abort it on the worker (Copilot review on
    // PR #1918).
    let committed = ExtendedBlock::new(1, 64, StorageType::Ssd, FileType::File);
    let unreachable = WorkerAddress {
        worker_id: 2,
        hostname: "127.0.0.1".to_string(),
        ip_addr: "127.0.0.1".to_string(),
        rpc_port: 1,
        web_port: 1,
    };
    let lb = LocatedBlock::new(committed, vec![TEST_SERVER.0.clone(), unreachable]);

    let result = BlockWriter::new(context, lb, 0, 64).await;
    assert!(
        result.is_err(),
        "second replica open must fail for a committed reopen too"
    );

    let observed = OBSERVED_REQUESTS.lock().unwrap().clone();
    let cancels = observed
        .iter()
        .filter(|(_, status)| *status == RequestStatus::Cancel)
        .count();
    assert_eq!(
        cancels, 0,
        "committed reopen must not send cancel commits, observed: {:?}",
        observed
    );
}

#[tokio::test]
async fn new_cancels_opened_replicas_when_a_later_replica_fails_to_open() {
    let context = create_test_context();

    // Wait until the test server accepts connections.
    let mut ready = false;
    for _ in 0..50 {
        if context.block_client(&TEST_SERVER.0).await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "test server failed to start within 5 seconds");

    // The first replica is the reachable test server; the second one is an
    // unreachable address so its open fails after the first opened fully.
    let unreachable = WorkerAddress {
        worker_id: 2,
        hostname: "127.0.0.1".to_string(),
        ip_addr: "127.0.0.1".to_string(),
        rpc_port: 1, // nothing listens here
        web_port: 1,
    };

    let result = BlockWriter::new(context, located_block(unreachable), 0, 64).await;
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

    // The first replica opened fully against the test server; its writer must
    // have received a cancel commit before `BlockWriter::new` returned.
    let observed = OBSERVED_REQUESTS.lock().unwrap().clone();
    let cancels: Vec<_> = observed
        .iter()
        .filter(|(_, status)| *status == RequestStatus::Cancel)
        .collect();
    assert_eq!(
        cancels.len(),
        1,
        "the opened replica must receive exactly one cancel commit, observed: {:?}",
        observed
    );
}
