use crate::worker::block::{BlockState, BlockStore, MasterClient};
use crate::worker::replication::replication_job::ReplicationJob;
use chrono::try_opt;
use curvine_client::block::BlockWriterRemote;
use curvine_client::file::FsContext;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    ReportBlockReplicationRequest, ReportBlockReplicationResponse, SumbitBlockReplicationRequest,
};
use curvine_common::state::{ExtendedBlock, FileType};
use futures::future::ok;
use log::error;
use orpc::client::ClientFactory;
use orpc::message::{Builder, RequestStatus};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::{err_box, try_option, CommonResult};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct WorkerReplicationManager {
    block_store: BlockStore,
    replication_semaphore: Arc<Semaphore>,
    jobs_queue_sender: Arc<Sender<ReplicationJob>>,

    runtime: Arc<AsyncRuntime>,
    fs_client_context: Arc<FsContext>,

    master_client: MasterClient,

    replicate_chunk_size: usize,
    // todo: add more metrics to track
}

impl WorkerReplicationManager {
    pub fn new(
        block_store: BlockStore,
        async_runtime: Arc<AsyncRuntime>,
        conf: &ClusterConf,
        fs_client_context: &Arc<FsContext>,
        master_client: &MasterClient,
    ) -> Arc<Self> {
        let (send, recv) = tokio::sync::mpsc::channel(10000);
        let handler = Self {
            block_store,
            replication_semaphore: Arc::new(Semaphore::new(10)),
            jobs_queue_sender: Arc::new(send),
            runtime: async_runtime.clone(),
            fs_client_context: fs_client_context.clone(),
            master_client: master_client.clone(),
            replicate_chunk_size: 1024 * 1024,
        };
        let handler = Arc::new(handler);
        Self::handle(&handler, async_runtime, recv);
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
                if let Err(e) = manager.replicate_block(&mut job).await {
                    error!("Errors on replicating block: {}. err: {}", job.block_id, e);
                } else {
                    if let Err(e) = manager.report_job(&job).await {
                        error!("Errors on reporting block: {}. err: {}", job.block_id, e);
                    }
                }
            }
        });
    }

    async fn report_job(
        &self,
        job: &ReplicationJob,
    ) -> CommonResult<ReportBlockReplicationResponse> {
        let storage_type = try_option!(job.storage_type);
        let request = ReportBlockReplicationRequest {
            block_id: job.block_id,
            storage_type: storage_type.into(),
            success: false,
            message: None,
        };
        let response: ReportBlockReplicationResponse = self
            .master_client
            .fs_client
            .rpc(RpcCode::ReportBlockReplicationResult, request)
            .await?;
        Ok(response)
    }

    async fn replicate_block(&self, job: &mut ReplicationJob) -> CommonResult<()> {
        let permit = self
            .replication_semaphore
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let block_meta = self.block_store.get_block(job.block_id)?;
        if block_meta.state != BlockState::Finalized {
            return err_box!("");
        }
        // update the storage type for the replication job.
        job.with_storage_type(block_meta.storage_type());
        let extend_block = ExtendedBlock::new(
            block_meta.id,
            block_meta.len,
            block_meta.storage_type(),
            FileType::File,
        );
        let mut writer = BlockWriterRemote::new(
            &self.fs_client_context,
            extend_block,
            job.target_worker_addr.clone(),
        )
        .await?;
        let mut reader = block_meta.create_reader(0)?;
        let mut remaining = block_meta.len;
        while remaining > 0 {
            let size = remaining.min(self.replicate_chunk_size as i64);
            let slice = reader.read_region(true, size as i32)?;
            writer.write(slice).await?;
            remaining -= size;
        }
        writer.flush().await?;
        writer.complete().await?;
        Ok(())
    }

    pub fn accept_job(&self, job: ReplicationJob) -> CommonResult<()> {
        // step1: check the block_id existence (todo)
        // step2: push into the queue
        self.runtime
            .block_on(async move { self.jobs_queue_sender.send(job).await })?;
        Ok(())
    }
}
