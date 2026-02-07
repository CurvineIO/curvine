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
    files: Vec<RawPtr<LocalFile>>,
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
            .write_blocks_batch(
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

        // Create multiple files, one for each block context
        let mut files = Vec::new();
        for context in &write_context.contexts {
            let path = try_option!(&context.path);
            let file = LocalFile::with_write_offset(path, false, pos)?;
            files.push(RawPtr::from_owned(file));
        }

        Ok(Self {
            rt: fs_context.clone_runtime(),
            fs_context,
            block,
            worker_address,
            files,
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
            .write_commit_batch(
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
        for file in &mut self.files {
            let file_clone = file.clone();
            self.rt
                .spawn_blocking(move || {
                    file_clone.as_mut().flush()?;
                    Ok::<(), FsError>(())
                })
                .await??;
        }
        Ok(())
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        for (index, file) in files.iter().enumerate() {
            let local_file = self.files[index].clone();
            let current_pos = file.1.len() as i64;
            let content_owned = file.1.to_string();
            let handle = self.rt.spawn_blocking(move || {
                let bytes_content = bytes::Bytes::copy_from_slice(content_owned.as_bytes());
                local_file.as_mut().write_all(&bytes_content)?;
                Ok::<(), FsError>(())
            });
            handle.await??;

            // update length of block
            if current_pos > self.block.len {
                self.block.len = current_pos;
            }
        }

        Ok(())
    }
}
