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

//! Commit gate test for `FsWriterBase::commit_all_pending_writers` (issue #2000).
//!
//! One RPC server stands in for the worker. It accepts every block open and
//! every complete except for one configured block id, whose complete fails.
//! The test drives `commit_all_pending_writers` with two cached writers and
//! pins that the block finalized before the failure is recorded in
//! `durable_blocks` (and therefore completes, not cancels, during `cancel()`).

use super::*;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, StorageType, WorkerAddress};
use curvine_net::net::ConnState;
use curvine_proto::{BlockWriteRequest, BlockWriteResponse};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BLOCK_SIZE: i64 = 64;

struct CommitGateService {
    fail_block_id: i64,
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
}

impl HandlerService for CommitGateService {
    type Item = CommitGateHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        CommitGateHandler {
            fail_block_id: self.fail_block_id,
            observed: self.observed.clone(),
        }
    }
}

struct CommitGateHandler {
    fail_block_id: i64,
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
}

impl MessageHandler for CommitGateHandler {
    type Error = curvine_core_error::CommonErrorExt;

    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        let code = RpcCode::from(msg.code());
        let status = msg.request_status();
        eprintln!("[fake-worker] code={code:?} status={status:?}");
        if code != RpcCode::WriteBlock {
            return Ok(msg.success());
        }

        let req: BlockWriteRequest = msg.parse_header()?;
        match status {
            RequestStatus::Open => {
                use prost::Message as _;
                let rep = BlockWriteResponse {
                    id: req.block.id,
                    path: Some("/tmp/curvine-test-block".to_string()),
                    off: req.off,
                    block_size: req.block_size,
                    storage_type: StorageType::Disk.into(),
                    pipeline_status: None,
                };
                let header = bytes::BytesMut::from(&rep.encode_to_vec()[..]);
                Ok(msg.success_with_data(Some(header), DataSlice::empty()))
            }
            RequestStatus::Complete | RequestStatus::Cancel => {
                self.observed.lock().unwrap().push((req.block.id, status));
                if status == RequestStatus::Complete && req.block.id == self.fail_block_id {
                    // Encode as FsError so the client-side check_error_ext
                    // (which decodes the kind-prefixed FsError format) can
                    // surface the failure instead of choking on raw text.
                    let err = FsError::common(format!(
                        "simulated worker failure for block {}",
                        req.block.id
                    ));
                    return Ok(msg.error_ext(&err));
                }
                Ok(msg.success())
            }
            _ => Ok(msg.success()),
        }
    }
}

struct TestEnv {
    context: Arc<FsContext>,
    worker: WorkerAddress,
    observed: Arc<Mutex<Vec<(i64, RequestStatus)>>>,
}

impl TestEnv {
    fn new(fail_block_id: i64) -> Self {
        let observed: Arc<Mutex<Vec<(i64, RequestStatus)>>> = Arc::new(Mutex::new(Vec::new()));
        let port = curvine_net::net::NetUtils::hold_available_port();
        let conf = ServerConf::with_hostname("127.0.0.1", port);
        let service = CommitGateService {
            fail_block_id,
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
        client_conf.client.rpc_timeout_ms = 500;
        client_conf.client.rpc_retry_max_duration_ms = 500;
        client_conf.client.rpc_retry_min_sleep_ms = 50;
        client_conf.client.rpc_retry_max_sleep_ms = 100;
        let context = Arc::new(FsContext::with_rt(client_conf, rt).unwrap());

        Self {
            context,
            worker,
            observed,
        }
    }

    async fn wait_ready(&self) {
        eprintln!("wait_ready: polling block_client");
        for _ in 0..50 {
            if self.context.block_client(&self.worker).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        eprintln!("wait_ready: ok");
    }

    fn observed(&self) -> Vec<(i64, RequestStatus)> {
        self.observed.lock().unwrap().clone()
    }

    fn located_block(&self, block: ExtendedBlock) -> LocatedBlock {
        LocatedBlock::new(block, vec![self.worker.clone()])
    }

    async fn writer(&self, block_id: i64, pos: i64) -> BlockWriter {
        let block = ExtendedBlock::new(block_id, 0, StorageType::Ssd, FileType::File);
        BlockWriter::new(
            self.context.clone(),
            self.located_block(block),
            pos,
            BLOCK_SIZE,
        )
        .await
        .unwrap()
    }

    fn writer_base(&self) -> FsWriterBase {
        eprintln!("writer_base: building");
        let status = FileBlocks::new(
            FileStatus {
                len: 0,
                block_size: BLOCK_SIZE,
                ..Default::default()
            },
            vec![
                self.located_block(ExtendedBlock::new(1, 0, StorageType::Ssd, FileType::File)),
                self.located_block(ExtendedBlock::new(2, 0, StorageType::Ssd, FileType::File)),
            ],
        );
        eprintln!("writer_base: built");
        FsWriterBase::new(
            self.context.clone(),
            Path::from_str("/test").unwrap(),
            status,
            0,
        )
    }
}

#[tokio::test]
async fn failed_commit_keeps_earlier_durable_blocks() {
    // Block 1 completes on the worker, block 2's complete fails. The commit
    // of block 1 must be recorded before the failure so cleanup never aborts
    // data already finalized on the worker (issue #2000).
    let env = TestEnv::new(2);
    env.wait_ready().await;

    let mut writer = env.writer_base();
    writer.cache_writers.insert(1, env.writer(1, 0).await);
    writer
        .cache_writers
        .insert(2, env.writer(2, BLOCK_SIZE).await);

    let result = writer.commit_all_pending_writers().await;
    assert!(result.is_err(), "block 2's complete must fail");

    assert!(
        writer.durable_blocks.contains(&1),
        "the block finalized before the failure must be durable"
    );
    assert!(
        !writer.durable_blocks.contains(&2),
        "the failed block must not be marked durable"
    );
}

#[tokio::test]
async fn cancel_after_failed_commit_completes_durable_block() {
    // After the failure above, `cancel()` must finalize the durable block
    // (sending Complete, never Cancel) and only cancel the failed block.
    let env = TestEnv::new(2);
    env.wait_ready().await;

    let mut writer = env.writer_base();
    writer.cache_writers.insert(1, env.writer(1, 0).await);
    writer
        .cache_writers
        .insert(2, env.writer(2, BLOCK_SIZE).await);

    assert!(writer.commit_all_pending_writers().await.is_err());
    let _ = writer.cancel().await;

    let observed = env.observed();
    let complete_1 = observed
        .iter()
        .filter(|(id, s)| *id == 1 && *s == RequestStatus::Complete)
        .count();
    let cancel_1 = observed
        .iter()
        .filter(|(id, s)| *id == 1 && *s == RequestStatus::Cancel)
        .count();
    assert!(
        complete_1 >= 1,
        "the durable block must be finalized during cleanup, observed: {:?}",
        observed
    );
    assert_eq!(
        cancel_1, 0,
        "the durable block must never be aborted, observed: {:?}",
        observed
    );
}
