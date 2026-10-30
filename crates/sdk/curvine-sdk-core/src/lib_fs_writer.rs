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
use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_fs_api::{Path, Writer};
use curvine_io::DataSlice;
use curvine_model::FileStatus;
use curvine_rpc::handler::FrameBuf;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::warn;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

pub struct LibFsWriter {
    pub(crate) rt: Arc<Runtime>,
    pub(crate) inner: UnifiedWriter,
    pub(crate) buf: FrameBuf,
    pub(crate) chunk_size: usize,
    pub(crate) cur_chunk: Option<BytesMut>,
    pub(crate) timeout_secs: u64,
}

impl LibFsWriter {
    pub fn new(rt: Arc<Runtime>, writer: UnifiedWriter, conf: &ClusterConf) -> Self {
        Self {
            rt,
            inner: writer,
            buf: FrameBuf::new(conf.client.write_chunk_size * conf.client.write_chunk_num),
            chunk_size: conf.client.write_chunk_size,
            cur_chunk: None,
            timeout_secs: conf.client.close_timeout_secs,
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

    /// Upper bound for a blocking flush/complete from the JNI layer: without
    /// it, a wedged write-buffer background task blocks the caller (e.g. Java
    /// SDK closeWriter) forever (see https://github.com/CurvineIO/curvine/issues/1908).
    /// Uses close_timeout_secs so the blocking wait is capped at roughly the
    /// same budget as the rest of the close path.
    fn block_on_with_deadline<F, T>(rt: &Runtime, timeout_secs: u64, task: F) -> FsResult<T>
    where
        F: Future<Output = FsResult<T>>,
    {
        let timeout = Duration::from_secs(timeout_secs);
        match rt.block_on(time::timeout(timeout, task)) {
            Ok(res) => res,
            Err(_) => {
                let msg = format!(
                    "writer flush/complete timed out after {:?}; \
                     the write buffer background task may be stuck",
                    timeout
                );
                warn!("{}", msg);
                err_box!(msg)
            }
        }
    }

    pub fn flush(&mut self) -> FsResult<()> {
        let task = self.inner.flush();
        Self::block_on_with_deadline(&self.rt, self.timeout_secs, task)
    }

    pub fn complete(&mut self) -> FsResult<()> {
        let _ = self.cur_chunk.take();
        let task = self.inner.complete();
        Self::block_on_with_deadline(&self.rt, self.timeout_secs, task)
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
