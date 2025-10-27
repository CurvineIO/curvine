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

use crate::fs::Path;
use crate::state::FileStatus;
use crate::FsResult;
use orpc::err_box;
use orpc::runtime::RpcRuntime;
use orpc::{runtime::Runtime, sys::DataSlice};
use std::future::Future;
use tokio_util::bytes::{BufMut, BytesMut};

pub trait Writer {
    fn status(&self) -> &FileStatus;

    fn path(&self) -> &Path;

    fn pos(&self) -> i64;

    fn pos_mut(&mut self) -> &mut i64;

    fn chunk_mut(&mut self) -> &mut BytesMut;

    fn chunk_size(&self) -> usize;

    fn flush_chunk(&mut self) -> impl Future<Output = FsResult<i64>> {
        async move {
            if !self.chunk_mut().is_empty() {
                let chunk = DataSlice::Bytes(self.chunk_mut().split().freeze());
                self.write_chunk(chunk).await
            } else {
                Ok(0)
            }
        }
    }

    // Write a block of data and return the number of bytes written. Writing does not go through the buffer;
    // Set the core methods that Writer needs to implement.
    fn write_chunk(&mut self, chunk: DataSlice) -> impl Future<Output = FsResult<i64>>;

    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = FsResult<()>> {
        async move {
            if chunk.is_empty() {
                return Ok(());
            }

            let mut remaining = chunk.len();
            let mut off: usize = 0;
            while remaining > 0 {
                if self.chunk_mut().len() >= self.chunk_size() {
                    self.flush_chunk().await?;
                }

                let write_len = (self.chunk_size() - self.chunk_mut().len()).min(remaining);
                self.chunk_mut().put_slice(&chunk[off..off + write_len]);

                remaining -= write_len;
                off += write_len;
                *self.pos_mut() += write_len as i64;
            }

            Ok(())
        }
    }

    fn async_write(&mut self, chunk: DataSlice) -> impl Future<Output = FsResult<()>> {
        async move { self.fuse_write(chunk).await }
    }

    fn blocking_write(&mut self, rt: &Runtime, chunk: DataSlice) -> FsResult<()> {
        let len = rt.block_on(async {
            self.flush_chunk().await?;
            self.write_chunk(chunk).await
        })?;
        *self.pos_mut() += len;
        Ok(())
    }

    fn fuse_write(&mut self, chunk: DataSlice) -> impl Future<Output = FsResult<()>> {
        async move {
            self.flush_chunk().await?;
            let len = self.write_chunk(chunk).await?;
            *self.pos_mut() += len;

            Ok(())
        }
    }

    fn flush(&mut self) -> impl Future<Output = FsResult<()>>;

    fn complete(&mut self) -> impl Future<Output = FsResult<()>>;

    fn cancel(&mut self) -> impl Future<Output = FsResult<()>>;

    // Random write seek support
    fn seek(&mut self, _pos: i64) -> impl Future<Output = FsResult<()>> {
        async move { err_box!("Not support") }
    }
}
