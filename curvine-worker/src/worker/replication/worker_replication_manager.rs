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

use crate::worker::block::{BlockState, BlockStore, MasterClient};
use crate::worker::replication::replication_job::ReplicationJob;
use curvine_client_core::block::BlockWriterRemote;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::{ExtendedBlock, FileType};
use curvine_proto::{ReportBlockReplicationRequest, ReportBlockReplicationResponse};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use log::{error, info, warn};
use once_cell::sync::OnceCell;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;

async fn finish_replication_with_cleanup<C, F>(
    result: CommonResult<()>,
    cancel: C,
    block_id: i64,
    target_worker_id: u32,
) -> CommonResult<()>
where
    C: FnOnce() -> F,
    F: Future<Output = FsResult<()>>,
{
    match result {
        Ok(()) => Ok(()),
        Err(replication_error) => {
            if let Err(cancel_error) = cancel().await {
                warn!(
                    "Failed to cancel remote writer for block {} on worker {} after replication error '{}': {}",
                    block_id, target_worker_id, replication_error, cancel_error
                );
            }
            Err(replication_error)
        }
    }
}

#[derive(Clone)]
pub struct WorkerReplicationManager {
    block_store: BlockStore,
    replication_semaphore: Arc<Semaphore>,
    jobs_queue_sender: Arc<Sender<ReplicationJob>>,
    fs_client_context: Arc<FsContext>,
    master_client: OnceCell<MasterClient>,
    replicate_chunk_size: usize,
    // todo: add more metrics to track
}

impl WorkerReplicationManager {
    pub fn new(
        block_store: &BlockStore,
        async_runtime: &Arc<AsyncRuntime>,
        conf: &ClusterConf,
        fs_client_context: &Arc<FsContext>,
    ) -> Arc<Self> {
        let (send, recv) = tokio::sync::mpsc::channel(Semaphore::MAX_PERMITS);
        let handler = Self {
            block_store: block_store.clone(),
            replication_semaphore: Arc::new(Semaphore::new(
                conf.worker.block_replication_concurrency_limit,
            )),
            jobs_queue_sender: Arc::new(send),
            fs_client_context: fs_client_context.clone(),
            master_client: Default::default(),
            replicate_chunk_size: conf.worker.block_replication_chunk_size,
        };
        let handler = Arc::new(handler);
        Self::handle(&handler, async_runtime.clone(), recv);

        info!("Worker replication manager is initialized");
        handler
    }

    fn handle(
        me: &Arc<Self>,
        async_runtime: Arc<AsyncRuntime>,
        mut recv: Receiver<ReplicationJob>,
    ) {
        let manager = me.clone();
        async_runtime.spawn(async move {
            while let Some(mut job) = recv.recv().await {
                let msg = match manager.replicate_block(&mut job).await {
                    Ok(_) => None,
                    Err(e) => {
                        error!("Errors on replicating block: {}. err: {}", job.block_id, e);
                        Some(e.to_string())
                    }
                };
                if let Err(e) = manager.report_job(&job, msg).await {
                    error!("Errors on reporting block: {}. err: {}", job.block_id, e);
                }
            }
        });
    }

    async fn report_job(
        &self,
        job: &ReplicationJob,
        err_msg: Option<String>,
    ) -> CommonResult<ReportBlockReplicationResponse> {
        let Some(storage_type) = job.storage_type else {
            return err_box!(
                "missing storage type when reporting replication result for block {}",
                job.block_id
            );
        };
        let request = ReportBlockReplicationRequest {
            block_id: job.block_id,
            storage_type: storage_type.into(),
            success: err_msg.is_none(),
            message: err_msg,
        };

        let Some(master_client) = self.master_client.get() else {
            return err_box!("master client is not initialized for worker replication reporting");
        };

        let response: ReportBlockReplicationResponse = master_client
            .fs_client
            .rpc(RpcCode::ReportBlockReplicationResult, request)
            .await?;
        Ok(response)
    }

    async fn replicate_block(&self, job: &mut ReplicationJob) -> CommonResult<()> {
        let _permit = match self.replication_semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(e) => return err_box!("replication semaphore closed: {}", e),
        };
        let (block_meta, mut reader) = self
            .block_store
            .open_reader_by_id_at_stored_len(job.block_id, 0)?;
        if block_meta.state != BlockState::Finalized {
            return err_box!("Block: {} is not finalized", job.block_id);
        }
        // update the storage type for the replication job.
        job.with_storage_type(block_meta.storage_type());
        let extend_block =
            ExtendedBlock::new(block_meta.id, 0, block_meta.storage_type(), FileType::File);
        let target_capacity = block_meta.replication_capacity();
        info!(
            "Replicating block_id: {} from {} to {} (copy_bytes={}, target_capacity={})",
            job.block_id,
            self.block_store.worker_id()?,
            job.target_worker_addr.worker_id,
            block_meta.len,
            target_capacity
        );
        let mut writer = BlockWriterRemote::new(
            &self.fs_client_context,
            extend_block,
            job.target_worker_addr.clone(),
            0,
            target_capacity,
        )
        .await?;
        let replication_result: CommonResult<()> = async {
            let mut remaining = block_meta.len;
            while remaining > 0 {
                let size = remaining.min(self.replicate_chunk_size as i64);
                let slice = reader.read_region(true, size as i32)?;
                let read_len = slice.len() as i64;
                writer.write(slice).await?;
                remaining -= read_len;
            }
            writer.flush().await?;
            writer.complete().await?;
            Ok(())
        }
        .await;

        finish_replication_with_cleanup(
            replication_result,
            || writer.cancel(),
            job.block_id,
            job.target_worker_addr.worker_id,
        )
        .await
    }

    pub fn accept_job(&self, job: ReplicationJob) -> CommonResult<()> {
        if let Err(e) = self.jobs_queue_sender.try_send(job) {
            return err_box!("Failed to queue replication job: {}", e);
        }
        Ok(())
    }

    pub fn with_master_client(&self, master_client: MasterClient) {
        let _ = self.master_client.set(master_client);
    }
}

#[cfg(test)]
mod tests {
    use super::finish_replication_with_cleanup;
    use curvine_core_error::{err_box, CommonResult};
    use curvine_error::FsError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn successful_replication_skips_cancel() -> CommonResult<()> {
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let calls = cancel_calls.clone();

        finish_replication_with_cleanup(
            Ok(()),
            move || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<(), FsError>(())
            },
            1,
            2,
        )
        .await?;

        assert_eq!(cancel_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn failed_replication_cancels_writer_and_preserves_error() {
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let calls = cancel_calls.clone();
        let replication_result: CommonResult<()> = err_box!("source read failed");

        let error = finish_replication_with_cleanup(
            replication_result,
            move || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<(), FsError>(())
            },
            1,
            2,
        )
        .await
        .expect_err("replication failure must be returned");

        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert!(error.to_string().contains("source read failed"));
    }

    #[tokio::test]
    async fn cancel_failure_does_not_replace_replication_error() {
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let calls = cancel_calls.clone();
        let replication_result: CommonResult<()> = err_box!("remote write failed");

        let error = finish_replication_with_cleanup(
            replication_result,
            move || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(FsError::common("cancel failed"))
            },
            1,
            2,
        )
        .await
        .expect_err("replication failure must remain primary");

        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert!(error.to_string().contains("remote write failed"));
        assert!(!error.to_string().contains("cancel failed"));
    }
}
