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
use orpc::err_box;
use orpc::sys::DataSlice;
use std::sync::Arc;

pub struct BlockReaderHole {
    block: ExtendedBlock,
    chunk_size: usize,
    pos: i64,
    len: i64,
    address: WorkerAddress,
}

impl BlockReaderHole {
    pub fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        pos: i64,
        len: i64,
    ) -> FsResult<Self> {
        let reader = Self {
            block,
            chunk_size: fs_context.read_chunk_size(),
            pos,
            len,
            address: WorkerAddress::default(),
        };
        Ok(reader)
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        self.pos = pos;
        Ok(pos)
    }

    pub fn read(&mut self) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        let len = self.chunk_size.min(self.remaining() as usize);
        let bytes = BytesMut::zeroed(len);
        self.pos += bytes.len() as i64;

        Ok(DataSlice::buffer(bytes))
    }

    pub fn complete(&mut self) -> FsResult<()> {
        Ok(())
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.address
    }
}
