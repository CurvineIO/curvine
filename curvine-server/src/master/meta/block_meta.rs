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

use curvine_common::state::{BlockLocation, CommitBlock, WorkerAddress};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
#[repr(i8)]
pub enum BlockState {
    Complete = 0,
    Committed = 1,
    Writing = 2,
    Recovering = 3,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockMeta {
    pub(crate) id: i64,
    pub(crate) len: i64,
    pub(crate) replicas: u16,
    // The pre-assigned worker id is required when deleting.
    pub(crate) locs: Option<Vec<BlockLocation>>,
}

impl BlockMeta {
    pub fn new(id: i64, len: i64) -> Self {
        Self {
            id,
            len,
            replicas: 1,
            locs: None,
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
        }
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn commit(&mut self, commit: &CommitBlock) {
        self.len = self.len.max(commit.block_len);
        let _ = self.locs.take();
    }

    pub fn matching_block(a: Option<&BlockMeta>, b: Option<&CommitBlock>) -> bool {
        let id_a = a.map(|x| x.id);
        let id_b = b.map(|x| x.block_id);
        id_a == id_b
    }
}
