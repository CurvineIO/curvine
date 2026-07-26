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

use curvine_common::state::BlockLocation;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DeleteResult {
    // Number of inodes removed
    pub(crate) inodes: u64,
    // Which blocks need to be deleted
    pub(crate) blocks: HashMap<i64, Vec<BlockLocation>>,
}

impl Default for DeleteResult {
    fn default() -> Self {
        Self::new()
    }
}

impl DeleteResult {
    pub fn new() -> Self {
        Self {
            inodes: 0,
            blocks: Default::default(),
        }
    }
}
