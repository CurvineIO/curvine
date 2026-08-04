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

use crate::block::{BlockClient, BlockReaderRemote};
use crate::file::FsContext;
use bytes::BytesMut;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_io::LocalFile;
use curvine_io::{CacheManager, DataSlice, ReadAheadTask};
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_sys::RawPtr;
use std::sync::Arc;

pub struct BlockReaderLocal {
    rt: Arc<Runtime>,
    client: BlockClient,
    os_cache: CacheManager,
    last_task: Option<ReadAheadTask>,
    block: ExtendedBlock,
    file: RawPtr<LocalFile>,
    worker_address: WorkerAddress,
    len: i64,
    req_id: i64,
    seq_id: i32,
    chunk: BytesMut,
    chunk_size: usize,
}

pub(crate) enum LocalReaderOpen {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
}

enum ReadOpenMode {
    Local(String),
    Remote,
}

impl ReadOpenMode {
    fn from_path(path: Option<String>) -> Self {
        match path {
            Some(path) => Self::Local(path),
            None => Self::Remote,
        }
    }
}

impl BlockReaderLocal {
    pub(crate) async fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        addr: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<LocalReaderOpen> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let chunk_size = fs_context.read_chunk_size();
        let client = fs_context.acquire_read(&addr).await?;
        let read_context = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                true,
            )
            .await?;

        let path = match ReadOpenMode::from_path(read_context.path) {
            ReadOpenMode::Local(path) => path,
            ReadOpenMode::Remote => {
                // The worker can reject short-circuit mode when it must synthesize a
                // sparse logical tail. Reuse the already-open remote read session.
                return Ok(LocalReaderOpen::Remote(BlockReaderRemote::from_opened(
                    client, block, addr, off, len, req_id, seq_id,
                )));
            }
        };
        let file = match LocalFile::with_read(&path, off as u64) {
            Ok(file) => file,
            Err(e) => {
                // Do not return a connection with an active read session to the pool.
                let mut client = client;
                client.clear_pool();
                return Err(e.into());
            }
        };

        let reader = Self {
            rt: fs_context.clone_runtime(),
            client,
            os_cache: fs_context.clone_os_cache(),
            last_task: None,
            block,
            file: RawPtr::from_owned(file),
            worker_address: addr.clone(),
            len,
            req_id,
            seq_id,
            chunk: BytesMut::with_capacity(chunk_size),
            chunk_size,
        };

        Ok(LocalReaderOpen::Local(reader))
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.file.pos()
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.file.pos()
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        Ok(self.file.as_mut().seek(pos)?)
    }

    fn get_chunk(&mut self) -> FsResult<BytesMut> {
        let read_size = self.chunk_size.min(self.remaining() as usize);
        if read_size == 0 {
            return err_box!("No readable data");
        }

        self.chunk.reserve(read_size);
        unsafe {
            self.chunk.set_len(read_size);
        }
        Ok(self.chunk.split())
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        let mut chunk = self.get_chunk()?;
        let file = self.file.clone();

        // Perform read-out.
        self.last_task = file
            .as_mut()
            .read_ahead(&self.os_cache, self.last_task.take());

        let chunk = self
            .rt
            .spawn_blocking(move || {
                file.as_mut().read_all(&mut chunk)?;
                Ok::<BytesMut, FsError>(chunk)
            })
            .await??;
        Ok(DataSlice::buffer(chunk))
    }

    pub fn blocking_read(&mut self) -> FsResult<DataSlice> {
        let mut chunk = self.get_chunk()?;
        self.last_task = self
            .file
            .as_mut()
            .read_ahead(&self.os_cache, self.last_task.take());
        self.file.as_mut().read_all(&mut chunk)?;
        Ok(DataSlice::buffer(chunk))
    }

    // Reading is completed and the server needs to be notified.
    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await?;
        Ok(())
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}

#[cfg(test)]
mod tests {
    use super::ReadOpenMode;

    #[test]
    fn missing_short_circuit_path_selects_remote_mode() {
        assert!(matches!(
            ReadOpenMode::from_path(None),
            ReadOpenMode::Remote
        ));
    }

    #[test]
    fn returned_short_circuit_path_selects_local_mode() {
        assert!(matches!(
            ReadOpenMode::from_path(Some("/data/block".to_string())),
            ReadOpenMode::Local(path) if path == "/data/block"
        ));
    }
}
