use curvine_client::block::BlockWriterRemote;
use curvine_client::file::FsContext;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use std::ops::{Deref, DerefMut};

pub struct WritePipeline {
    remote_worker_client: BlockWriterRemote,
}

impl WritePipeline {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
    ) -> FsResult<Self> {
        let client = BlockWriterRemote::new(fs_context, block, worker_address).await?;
        let handler = Self {
            remote_worker_client: client,
        };
        Ok(handler)
    }
}

impl Deref for WritePipeline {
    type Target = BlockWriterRemote;

    fn deref(&self) -> &Self::Target {
        &self.remote_worker_client
    }
}

impl DerefMut for WritePipeline {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.remote_worker_client
    }
}
