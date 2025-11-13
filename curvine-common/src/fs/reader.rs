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
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use std::future::Future;
use tokio_util::bytes::BytesMut;

pub trait Reader {
    fn status(&self) -> &FileStatus;

    fn path(&self) -> &Path;

    fn len(&self) -> i64;

    fn chunk_mut(&mut self) -> &mut DataSlice;

    fn chunk_size(&self) -> usize;

    fn pos(&self) -> i64;

    fn pos_mut(&mut self) -> &mut i64;

    fn remaining(&self) -> i64 {
        self.len() - self.pos()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    fn read_chunk0(&mut self) -> impl Future<Output = FsResult<DataSlice>>;

    fn read_chunk(&mut self, len: Option<usize>) -> impl Future<Output = FsResult<DataSlice>> {
        async move {
            if !self.has_remaining() {
                return Ok(DataSlice::Empty);
            }

            if self.chunk_mut().is_empty() {
                *self.chunk_mut() = self.read_chunk0().await?;
            }

            let read_len = match len {
                None => self.chunk_mut().len(),
                Some(v) => v.min(self.chunk_mut().len()),
            };
            if read_len == 0 {
                return err_box!("Abnormal status: chunk is empty");
            }

            let chunk = self.chunk_mut().split_to(read_len);
            Ok(chunk)
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = FsResult<usize>> {
        async move {
            let mut chunk = self.read_chunk(Some(buf.len())).await?;
            let read_len = chunk.len();
            if read_len > 0 {
                chunk.copy_to_slice(&mut buf[0..read_len]);
            }
            *self.pos_mut() += read_len as i64;
            Ok(read_len)
        }
    }

    // Blocking to read data, Java abi will call the modified method.
    fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        let chunk = rt.block_on(self.read_chunk(None))?;
        *self.pos_mut() += chunk.len() as i64;
        Ok(chunk)
    }

    fn async_read(&mut self, len: Option<usize>) -> impl Future<Output = FsResult<DataSlice>> {
        async move {
            let chunk = self.read_chunk(len).await?;
            *self.pos_mut() += chunk.len() as i64;
            Ok(chunk)
        }
    }

    // Read data directly from the chunk, fuse will call the modified method.
    // The fuse call tries to return the same number of bytes as expected, otherwise the delay may be increased.
    // This is more important in fio testing.
    fn fuse_read(
        &mut self,
        pos: i64,
        len: usize,
    ) -> impl Future<Output = FsResult<Vec<DataSlice>>> {
        async move {
            self.seek(pos).await?;

            let mut vec = Vec::with_capacity(len / self.chunk_size() + 1);
            let mut remaining = len;
            while remaining > 0 {
                let chunk = self.read_chunk(Some(remaining)).await?;
                let read_len = chunk.len();
                if read_len == 0 {
                    break;
                }
                vec.push(chunk);
                remaining -= read_len;
                *self.pos_mut() += read_len as i64;
            }

            Ok(vec)
        }
    }

    fn read_full(&mut self, buf: &mut [u8]) -> impl Future<Output = FsResult<usize>> {
        async move {
            let mut remaining = buf.len();
            let mut off = 0;
            while remaining > 0 {
                let read_size = self.read(&mut buf[off..]).await?;
                if read_size == 0 {
                    break;
                }
                remaining -= read_size;
                off += read_size;
            }

            Ok(buf.len() - remaining)
        }
    }

    fn read_as_string(&mut self) -> impl Future<Output = FsResult<String>> {
        async move {
            let len = self.len() as usize;
            let mut buf = BytesMut::zeroed(len);

            let len = self.read_full(&mut buf).await?;
            Ok(String::from_utf8_lossy(&buf[..len]).to_string())
        }
    }

    fn seek(&mut self, pos: i64) -> impl Future<Output = FsResult<()>>;

    fn complete(&mut self) -> impl Future<Output = FsResult<()>>;
}
