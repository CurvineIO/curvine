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

use curvine_codec::{CodecResult, Decode, Encode, Reader, Writer};
use curvine_model::{BlockLocation, CommitBlock, FileAllocOpts, WorkerAddress};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockMeta {
    pub(crate) id: i64,
    pub(crate) len: u32,
    pub(crate) replicas: u8,
    // The pre-assigned worker id is required when deleting.
    pub(crate) locs: Option<Vec<BlockLocation>>,
    pub(crate) alloc_opts: Option<FileAllocOpts>,
}

impl BlockMeta {
    pub fn new(id: i64, len: i64) -> Self {
        Self {
            id,
            len: len as u32,
            replicas: 1,
            locs: None,
            alloc_opts: None,
        }
    }

    // Pre-allocated worker block
    pub fn with_pre(id: i64, workers: &[WorkerAddress]) -> Self {
        let locs = workers
            .iter()
            .map(|x| BlockLocation::with_id(x.worker_id))
            .collect();
        Self {
            id,
            len: 0,
            replicas: 1,
            locs: Some(locs),
            alloc_opts: None,
        }
    }

    pub fn with_alloc(id: i64, alloc_opts: FileAllocOpts) -> Self {
        Self {
            id,
            len: alloc_opts.len as u32,
            replicas: 0,
            locs: None,
            alloc_opts: Some(alloc_opts),
        }
    }

    pub fn len(&self) -> i64 {
        self.len as i64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn commit(&mut self, commit: &CommitBlock) {
        self.len = (self.len as i64).max(commit.block_len) as u32;
        let _ = self.locs.take();
        let _ = self.alloc_opts.take();
    }

    pub fn assign_worker(&mut self, workers: &[WorkerAddress]) -> bool {
        if self.alloc_opts.is_none() || self.locs.is_some() {
            false
        } else {
            let locs = workers
                .iter()
                .map(|x| BlockLocation::with_id(x.worker_id))
                .collect();
            self.locs = Some(locs);
            true
        }
    }
    pub fn matching_block(a: Option<&BlockMeta>, b: Option<&CommitBlock>) -> bool {
        let id_a = a.map(|x| x.id);
        let id_b = b.map(|x| x.block_id);
        id_a == id_b
    }
}

impl Encode for BlockMeta {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i64(self.id)?;
        writer.put_u32(self.len)?;
        writer.put_u8(self.replicas)?;
        self.locs.encode(writer)?;
        self.alloc_opts.encode(writer)
    }
}

impl Decode for BlockMeta {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let id = reader.get_i64()?;
        let len = reader.get_u32()?;
        let replicas = reader.get_u8()?;
        let locs = Option::<Vec<BlockLocation>>::decode(reader)?;
        let alloc_opts = Option::<FileAllocOpts>::decode(reader)?;
        Ok(Self {
            id,
            len,
            replicas,
            locs,
            alloc_opts,
        })
    }
}
