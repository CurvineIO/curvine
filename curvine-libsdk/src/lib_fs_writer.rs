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

use bytes::BytesMut;
use curvine_client::unified::UnifiedWriter;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use curvine_core::err_box;
use curvine_core::handler::FrameBuf;
use curvine_core::runtime::{RpcRuntime, Runtime};
use curvine_core::sys::DataSlice;
use std::sync::Arc;

pub struct LibFsWriter {
    pub(crate) rt: Arc<Runtime>,
    pub(crate) inner: UnifiedWriter,
    pub(crate) buf: FrameBuf,
    pub(crate) chunk_size: usize,
    pub(crate) cur_chunk: Option<BytesMut>,
}

impl LibFsWriter {
    pub fn new(rt: Arc<Runtime>, writer: UnifiedWriter, conf: &ClusterConf) -> Self {
        Self {
            rt,
            inner: writer,
            buf: FrameBuf::new(conf.client.write_chunk_size * conf.client.write_chunk_num),
            chunk_size: conf.client.write_chunk_size,
            cur_chunk: None,
        }
    }

    pub fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        self.inner.blocking_write(&self.rt, buf)
    }

    pub fn alloc_chunk(&mut self) -> FsResult<&[u8]> {
        if self.cur_chunk.is_some() {
            return err_box!("chunk already allocated");
        }
        let chunk = self.buf.take_exact(self.chunk_size);
        Ok(self.cur_chunk.insert(chunk))
    }

    pub fn write_v2(&mut self, addr: i64, len: i32) -> FsResult<&[u8]> {
        let Some(mut chunk) = self.cur_chunk.take() else {
            return err_box!("no chunk allocated; call alloc_chunk first");
        };

        if chunk.as_ptr() as i64 != addr {
            self.cur_chunk = Some(chunk);
            return err_box!("write address does not match allocated chunk");
        }

        if len < 0 || len as usize > chunk.capacity() {
            self.cur_chunk.replace(chunk);
            return err_box!("invalid write length: {}", len);
        }
        unsafe {
            chunk.set_len(len as usize);
        }
        self.inner
            .blocking_write(&self.rt, DataSlice::buffer(chunk))?;

        self.alloc_chunk()
    }

    pub fn flush(&mut self) -> FsResult<()> {
        self.rt.block_on(self.inner.flush())
    }

    pub fn complete(&mut self) -> FsResult<()> {
        let _ = self.cur_chunk.take();
        self.rt.block_on(self.inner.complete())
    }

    pub fn pos(&self) -> i64 {
        self.inner.pos()
    }

    pub fn status(&self) -> &FileStatus {
        self.inner.status()
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn block_size(&self) -> i64 {
        self.inner.status().block_size
    }
}
