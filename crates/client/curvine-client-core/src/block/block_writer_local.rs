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

use crate::block::block_writer::rollback_on_failure;
use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_io::LocalFile;
use curvine_model::{ExtendedBlock, StorageType, WorkerAddress};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;

// Own the already opened RPC session while the local file open is queued or
// running. Cancelling the constructor must not return that session to the pool.
struct LocalOpenSession(Option<BlockClient>);

impl LocalOpenSession {
    fn client(&self) -> &BlockClient {
        self.0.as_ref().expect("local open session owns its client")
    }

    fn into_client(mut self) -> BlockClient {
        self.0.take().expect("local open session owns its client")
    }
}

impl Drop for LocalOpenSession {
    fn drop(&mut self) {
        if let Some(client) = &mut self.0 {
            client.clear_pool();
        }
    }
}

// Keep the RPC write lease and file together. A detached blocking operation
// must own both until it exits, rather than lending a pointer from its writer.
struct LocalWriteSession {
    client: BlockClient,
    file: LocalFile,
    finished: bool,
}

impl Drop for LocalWriteSession {
    fn drop(&mut self) {
        if !self.finished {
            // An Open with no acknowledged Complete/Cancel is not an idle
            // session, even if its RPC outstanding-response count is zero.
            // Dropping the unpooled client closes the connection only after
            // the blocking task has stopped using the file.
            self.client.clear_pool();
        }
    }
}

pub struct BlockWriterLocal {
    rt: Arc<Runtime>,
    session: Option<LocalWriteSession>,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    // Last acknowledged position: never read file state from a live task.
    pos: i64,
    block_size: i64,
    seq_id: i32,
    req_id: i64,
    actual_storage_type: StorageType,
}

impl BlockWriterLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        pos: i64,
        block_size: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;
        let opening = LocalOpenSession(Some(fs_context.acquire_write(&worker_address).await?));
        let write_context = opening
            .client()
            .write_block(
                &block,
                pos,
                block_size,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                true,
                Vec::new(),
            )
            .await?;

        let path = match write_context.path {
            Some(p) => p,
            None => {
                // No local path available (e.g. SPDK bdev without filesystem path).
                // Only a fresh allocation may be aborted; a local-to-remote
                // fallback during reopen must preserve committed replicas.
                if rollback_on_failure(&block, pos) {
                    let _ = opening
                        .client()
                        .write_commit(&block, pos, block_size, req_id, 0, true)
                        .await;
                }
                return Err(FsError::no_local_path(format!(
                    "no local path for block {} (storage type {:?})",
                    block.id, block.storage_type
                )));
            }
        };
        let actual_storage_type = write_context.storage_type;
        let rt = fs_context.clone_runtime();
        let (opening, file) = rt
            .spawn_blocking(move || {
                let file = LocalFile::with_write_offset(path, false, pos);
                (opening, file)
            })
            .await?;
        let file = match file {
            Ok(file) => file,
            Err(e) => {
                // Preserve the same fresh-allocation rollback contract as
                // BlockWriter::new. Disconnect, never Cancel, a failed reopen.
                if rollback_on_failure(&block, pos) {
                    let _ = opening
                        .client()
                        .write_commit(&block, pos, block_size, req_id, 0, true)
                        .await;
                }
                return Err(e.into());
            }
        };

        let writer = Self {
            rt,
            session: Some(LocalWriteSession {
                client: opening.into_client(),
                file,
                finished: false,
            }),
            block,
            worker_address,
            pos,
            block_size,
            seq_id,
            req_id,
            actual_storage_type,
        };

        Ok(writer)
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    fn session_mut(&mut self) -> FsResult<&mut LocalWriteSession> {
        self.session.as_mut().ok_or_else(|| {
            FsError::common("Local writer is unavailable after a cancelled or failed operation")
        })
    }

    async fn with_file<T, F>(&mut self, operation: F) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut LocalFile) -> FsResult<T> + Send + 'static,
    {
        let mut session = self.session.take().ok_or_else(|| {
            FsError::common("Local writer is unavailable after a cancelled or failed operation")
        })?;
        session.finished = false;
        let (session, result) = self
            .rt
            .spawn_blocking(move || {
                let result = operation(&mut session.file);
                (session, result)
            })
            .await?;
        // On cancellation or JoinError the task owns (and ultimately drops)
        // the session; on I/O error drop it here. Neither path permits reuse.
        let output = result?;
        self.pos = session.file.pos();
        self.session = Some(session);
        Ok(output)
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        // Bytes/Buffer stay zero-copy; borrowed MemSlice is copied before the
        // first await, so a cancelled caller can release its input memory.
        let chunk = chunk.to_bytes();
        self.with_file(move |file| {
            file.write_all(&chunk)?;
            Ok(())
        })
        .await?;

        if self.pos() > self.block.len {
            self.block.len = self.pos();
        }
        Ok(())
    }

    pub fn blocking_write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let mut session = self.session.take().ok_or_else(|| {
            FsError::common("Local writer is unavailable after a cancelled or failed operation")
        })?;
        session.finished = false;
        session.file.write_all(chunk.as_slice())?;
        self.pos = session.file.pos();
        self.session = Some(session);
        if self.pos() > self.block.len {
            self.block.len = self.pos();
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        self.with_file(|file| {
            file.flush()?;
            Ok(())
        })
        .await
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;
        let next_seq_id = self.next_seq_id();
        let session = self.session.as_mut().expect("flush restored the session");
        session
            .client
            .write_commit(
                &self.block,
                self.pos,
                self.block_size,
                self.req_id,
                next_seq_id,
                false,
            )
            .await?;
        session.finished = true;
        Ok(())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        // Do not send Cancel while a detached local task may still be writing.
        // Its session owns the lease until the task exits and closes it.
        self.session_mut()?;
        let next_seq_id = self.next_seq_id();
        let session = self.session.as_mut().expect("session checked above");
        session
            .client
            .write_commit(
                &self.block,
                self.pos,
                self.block_size,
                self.req_id,
                next_seq_id,
                true,
            )
            .await?;
        session.finished = true;
        Ok(())
    }

    pub fn remaining(&self) -> i64 {
        self.block_size - self.pos()
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.block.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub fn actual_storage_type(&self) -> StorageType {
        self.actual_storage_type
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative position: {}", pos);
        } else if pos > self.block_size {
            return err_box!(
                "Seek position {} exceeds block capacity {}",
                pos,
                self.block_size
            );
        }

        self.with_file(move |file| {
            file.seek(pos)?;
            Ok(())
        })
        .await
    }
}
