use crate::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::proto::{ContainerMetadataProto, SmallFileMetaProto};
use curvine_common::state::{ContainerStatus, ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::RawPtr;
use orpc::try_option;
use std::sync::Arc;

pub struct ContainerBlockWriterLocal {
    rt: Arc<Runtime>,
    fs_context: Arc<FsContext>,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    file: RawPtr<LocalFile>,
    block_size: i64,
    pos: i64,
    container_meta: Option<ContainerMetadataProto>,
}

impl ContainerBlockWriterLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        pos: i64,
        container_status: ContainerStatus,
        small_files_metadata: Vec<SmallFileMetaProto>,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;

        // SINGLE RPC call to setup multiple blocks
        let write_context = client
            .write_container_block(
                &block,
                0,
                block_size,
                req_id,
                0_i32,
                fs_context.write_chunk_size() as i32,
                true,
                container_status,
                small_files_metadata,
            )
            .await?;

        let container_meta = write_context.container_meta.clone();

        // create local file for storing large of small files
        let path = try_option!(write_context
            .contexts
            .first()
            .and_then(|ctx| ctx.path.as_ref()));
        let file = RawPtr::from_owned(LocalFile::with_write_offset(path, false, pos)?);

        Ok(Self {
            rt: fs_context.clone_runtime(),
            fs_context,
            block,
            worker_address,
            file,
            block_size,
            pos: 0,
            container_meta,
        })
    }

    // SINGLE RPC call to complete all blocks
    pub async fn complete(&mut self) -> FsResult<()> {
        // flush before commit
        self.flush().await?;
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
            .write_commit_container(
                &self.block,
                self.pos,
                self.block_size,
                Utils::req_id(),
                0,
                false,
                self.container_meta.clone(),
            )
            .await
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let file_clone = self.file.clone();
        self.rt
            .spawn_blocking(move || {
                file_clone.as_mut().flush()?;
                Ok::<(), FsError>(())
            })
            .await??;
        Ok(())
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        for (_, content) in files.iter() {
            let file_clone = self.file.clone();
            let content_owned = content.to_string();
            let handle = self.rt.spawn_blocking(move || {
                let bytes_content = bytes::Bytes::copy_from_slice(content_owned.as_bytes());
                file_clone.as_mut().write_all(&bytes_content)?;
                Ok::<(), FsError>(())
            });
            handle.await??;

            // Update position after each write
            self.pos += content.len() as i64;
        }

        // Update block length to reflect total written data
        if self.pos > self.block.len {
            self.block.len = self.pos;
        }

        Ok(())
    }
}
