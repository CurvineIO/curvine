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

use curvine_client::unified::UnifiedReader;
use curvine_common::fs::{Path, Reader};
use curvine_common::FsResult;
use curvine_core::runtime::{RpcRuntime, Runtime};
use curvine_core::sys::DataSlice;
use std::sync::Arc;

pub struct LibFsReader {
    rt: Arc<Runtime>,
    inner: UnifiedReader,
    cur_chunk: Option<DataSlice>,
}

impl LibFsReader {
    pub fn new(rt: Arc<Runtime>, reader: UnifiedReader) -> Self {
        Self {
            rt,
            inner: reader,
            cur_chunk: None,
        }
    }

    pub fn remaining(&self) -> i64 {
        self.inner.remaining()
    }

    pub fn pos(&self) -> i64 {
        self.inner.pos()
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.rt.block_on(self.inner.seek(pos))
    }

    pub fn read(&mut self) -> FsResult<&[u8]> {
        let chunk = self.inner.blocking_read(&self.rt)?;
        let r = self.cur_chunk.insert(chunk);
        Ok(r.as_slice())
    }

    pub fn complete(&mut self) -> FsResult<()> {
        let _ = self.cur_chunk.take();
        self.rt.block_on(self.inner.complete())
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn len(&self) -> i64 {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
