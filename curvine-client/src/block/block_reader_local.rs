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

use crate::file::FsContext;
use bytes::BytesMut;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{CacheManager, DataSlice, RawPtr, ReadAheadTask};
use orpc::{err_box, try_option};
use std::sync::Arc;
use curvine_common::error::FsError;

pub struct BlockReaderLocal {
    rt: Arc<Runtime>,
    fs_context: Arc<FsContext>,
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

impl BlockReaderLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        addr: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let chunk_size = fs_context.read_chunk_size();
        let client = fs_context.block_client(&addr).await?;
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

        let path = try_option!(read_context.path);
        let file = LocalFile::with_read(&path, off as u64)?;
        if file.len() < read_context.len {
            return err_box!(
                "File data is lost. block len: {}, actual file length: {}",
                read_context.len,
                file.len()
            );
        }

        let reader = Self {
            rt: fs_context.clone_runtime(),
            fs_context: fs_context.clone(),
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

        Ok(reader)
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

        let chunk = self.rt
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
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
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
