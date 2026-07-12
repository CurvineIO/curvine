use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::RawPtr;
use std::sync::Arc;

pub struct BatchBlockWriterLocal {
    rt: Arc<Runtime>,
    blocks: Vec<ExtendedBlock>,
    worker_address: WorkerAddress,
    client: BlockClient,
    files: Vec<Option<RawPtr<LocalFile>>>,
    block_size: i64,
    pos: i64,
    req_id: i64,
}

impl BatchBlockWriterLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        blocks: Vec<ExtendedBlock>,
        worker_address: WorkerAddress,
        pos: i64,
    ) -> FsResult<(Self, Vec<bool>)> {
        let req_id = Utils::req_id();
        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;

        // SINGLE RPC call to setup multiple blocks
        let write_context = match client
            .write_blocks_batch(
                &blocks,
                0,
                block_size,
                req_id,
                0_i32,
                fs_context.write_chunk_size() as i32,
                true,
            )
            .await
        {
            Ok(context) => context,
            Err(error) => {
                let cancels = vec![true; blocks.len()];
                if let Err(cancel_error) = client
                    .write_commit_batch(&blocks, 0, block_size, req_id, 1, &cancels)
                    .await
                {
                    log::warn!(
                        "failed to cancel local batch blocks after open error: {}",
                        cancel_error
                    );
                }
                return Err(error);
            }
        };

        let mut files = Vec::with_capacity(write_context.len());
        let mut results = Vec::with_capacity(write_context.len());
        for result in write_context {
            let file = match result {
                Ok(context) => match context.path.as_deref() {
                    Some(path) => match LocalFile::with_write_offset(path, false, pos) {
                        Ok(file) => Some(RawPtr::from_owned(file)),
                        Err(error) => {
                            log::warn!("failed to open local batch file {}: {}", path, error);
                            None
                        }
                    },
                    None => {
                        log::warn!("local batch open response is missing a file path");
                        None
                    }
                },
                Err(error) => {
                    log::warn!("failed to open local batch block: {}", error);
                    None
                }
            };
            results.push(file.is_some());
            files.push(file);
        }

        Ok((
            Self {
                rt: fs_context.clone_runtime(),
                blocks,
                worker_address,
                client,
                files,
                block_size,
                pos: 0,
                req_id,
            },
            results,
        ))
    }

    // SINGLE RPC call to complete all blocks
    pub async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        for (file, cancel) in self.files.iter_mut().zip(cancels.iter()) {
            if *cancel {
                file.take();
            }
        }
        self.client
            .write_commit_batch(
                &self.blocks,
                self.pos,
                self.block_size,
                self.req_id,
                0,
                cancels,
            )
            .await
    }

    /// Flush only still-active short-circuit files before mixed commit/cancel.
    /// Inactive or already-failed items are reported as `false` without flushing.
    pub async fn flush_active(&mut self, active: &[bool]) -> FsResult<Vec<bool>> {
        if active.len() != self.files.len() {
            return orpc::err_box!(
                "batch local flush count mismatch, active={}, files={}",
                active.len(),
                self.files.len()
            );
        }
        let mut results = Vec::with_capacity(self.files.len());
        for (file, active) in self.files.iter_mut().zip(active.iter()) {
            let Some(file) = file else {
                results.push(false);
                continue;
            };
            if !active {
                results.push(false);
                continue;
            }
            let file_clone = file.clone();
            let result = self
                .rt
                .spawn_blocking(move || {
                    file_clone.as_mut().flush()?;
                    Ok::<(), FsError>(())
                })
                .await?;
            results.push(result.is_ok());
        }
        Ok(results)
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<Vec<bool>> {
        if files.len() != self.files.len() {
            return orpc::err_box!(
                "batch local write count mismatch, request={}, files={}",
                files.len(),
                self.files.len()
            );
        }
        let mut results = Vec::with_capacity(files.len());
        for (index, file) in files.iter().enumerate() {
            let Some(local_file) = self.files[index].clone() else {
                results.push(false);
                continue;
            };
            let current_pos = file.1.len() as i64;
            let content_owned = file.1.to_string();
            let handle = self.rt.spawn_blocking(move || {
                let bytes_content = bytes::Bytes::copy_from_slice(content_owned.as_bytes());
                local_file.as_mut().write_all(&bytes_content)?;
                Ok::<(), FsError>(())
            });
            let result = handle.await?;
            if result.is_ok() && current_pos > self.blocks[index].len {
                self.blocks[index].len = current_pos;
            }
            results.push(result.is_ok());
        }

        Ok(results)
    }
}
