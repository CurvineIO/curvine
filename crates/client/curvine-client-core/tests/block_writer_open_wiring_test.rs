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
//! Each test owns an isolated gRPC server whose handler accepts the
//! first replica's open and records subsequent write lifecycle operations. The second replica points at an unreachable
//! address so its open fails after the first replica opened fully. One test
//! pins that a fresh-block open failure sends exactly one cancel commit;
//! the other pins that a committed-block reopen failure sends none (the
//! rollback gate in `BlockWriter::new`).

use curvine_client_core::block::{BlockWriter, BlockWriterLocal};
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_model::{ExtendedBlock, FileType, LocatedBlock, StorageType, WorkerAddress};
use curvine_proto::block_data_request::Operation;
use curvine_proto::block_data_response::Result as OperationResult;
use curvine_proto::block_data_service_server::{BlockDataService, BlockDataServiceServer};
use curvine_proto::{BlockDataRequest, BlockDataResponse, BlockWriteResponse};
use curvine_rpc::message::RequestStatus;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct RecordingService {
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
    echo_mismatched_block_size: bool,
}

#[tonic::async_trait]
impl BlockDataService for RecordingService {
    type ExchangeStream =
        Pin<Box<dyn futures::Stream<Item = Result<BlockDataResponse, tonic::Status>> + Send>>;

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<BlockDataRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        let mut input = request.into_inner();
        let observed = self.observed.clone();
        let mismatch = self.echo_mismatched_block_size;
        let output = async_stream::try_stream! {
            let mut next_seq = 0;
            while let Some(frame) = input.message().await? {
                if frame.seq_id != next_seq {
                    Err(tonic::Status::invalid_argument("unexpected write sequence"))?;
                }
                next_seq += 1;
                let (status, result) = match frame.operation {
                    Some(Operation::OpenWrite(open)) => (
                        RequestStatus::Open,
                        OperationResult::OpenedWrite(BlockWriteResponse {
                            id: open.block.id,
                            // An empty local path deliberately fails LocalFile::open.
                            path: Some(String::new()),
                            off: open.off,
                            block_size: if mismatch { open.block_size - 1 } else { open.block_size },
                            storage_type: StorageType::Disk.into(),
                            pipeline_status: None,
                        }),
                    ),
                    Some(Operation::CancelWrite(_)) => (RequestStatus::Cancel, OperationResult::Ack(true)),
                    Some(Operation::CompleteWrite(_)) => (RequestStatus::Complete, OperationResult::Ack(true)),
                    _ => Err(tonic::Status::invalid_argument("unexpected write operation"))?,
                };
                observed.lock().unwrap().push((frame.req_id, status));
                yield BlockDataResponse {
                    req_id: frame.req_id,
                    seq_id: frame.seq_id,
                    result: Some(result),
                };
            }
        };
        Ok(tonic::Response::new(Box::pin(output)))
    }
}

/// One test = one server + one observation buffer, so parallel tests never
/// observe each other's requests.
struct TestEnv {
    context: Arc<FsContext>,
    worker: WorkerAddress,
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl TestEnv {
    fn new() -> Self {
        Self::with_echo_mismatch(false)
    }

    /// A mismatched response drives the constructor's gated abort branch.
    fn with_echo_mismatch(echo_mismatched_block_size: bool) -> Self {
        let observed: Arc<Mutex<Vec<(i64, RequestStatus)>>> = Arc::new(Mutex::new(Vec::new()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let service = RecordingService {
            observed: observed.clone(),
            echo_mismatched_block_size,
        };
        let rt = Arc::new(Runtime::single());
        let server = rt.spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
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
            server,
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

#[tokio::test]
async fn remote_constructor_mismatch_aborts_fresh_block() {
    // The handler's Open reply reports a block_size differing from the
    // request, so `BlockWriterRemote::new` fails in its block-size mismatch
    // branch after `write_block` succeeded. For a freshly allocated block
    // (len == 0 && pos == 0) the constructor must send the abort commit.
    let env = TestEnv::with_echo_mismatch(true);
    env.wait_ready().await;

    let block = ExtendedBlock::new(1, 0, StorageType::Ssd, FileType::File);

    let result = BlockWriter::new(env.context.clone(), env.located_block(block), 0, 65).await;
    assert!(
        result.is_err(),
        "block-size mismatch must fail the open on the first replica"
    );

    let cancels = env
        .observed()
        .into_iter()
        .filter(|(_, status)| *status == RequestStatus::Cancel)
        .count();
    assert_eq!(
        cancels,
        1,
        "the mismatch abort must fire exactly once for a fresh block, observed: {:?}",
        env.observed()
    );
}

#[tokio::test]
async fn remote_constructor_mismatch_spares_committed_block() {
    // Same mismatch, but the block is a committed reopen (len == 64): the
    // gate must suppress the abort so the committed SPDK extent is not
    // freed on the worker.
    let env = TestEnv::with_echo_mismatch(true);
    env.wait_ready().await;

    let block = ExtendedBlock::new(1, 64, StorageType::Ssd, FileType::File);

    let result = BlockWriter::new(env.context.clone(), env.located_block(block), 0, 65).await;
    assert!(
        result.is_err(),
        "block-size mismatch must fail the open on the first replica"
    );

    let cancels = env
        .observed()
        .into_iter()
        .filter(|(_, status)| *status == RequestStatus::Cancel)
        .count();
    assert_eq!(
        cancels,
        0,
        "the mismatch abort must not fire for a committed reopen, observed: {:?}",
        env.observed()
    );
}

#[tokio::test]
async fn local_constructor_file_open_failure_aborts_in_sequence() {
    let env = TestEnv::new();
    env.wait_ready().await;
    let block = ExtendedBlock::new(1, 0, StorageType::Ssd, FileType::File);
    let result = BlockWriterLocal::new(env.context.clone(), block, env.worker.clone(), 0, 64).await;
    assert!(result.is_err(), "an empty local path must fail to open");
    let statuses: Vec<_> = env
        .observed()
        .into_iter()
        .map(|(_, status)| status)
        .collect();
    assert_eq!(statuses, [RequestStatus::Open, RequestStatus::Cancel]);
}
