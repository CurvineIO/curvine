//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::state::BlockLocation;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct FreeResult {
    pub inodes: i64,
    pub bytes: i64,
    pub blocks: HashMap<i64, Vec<BlockLocation>>,
}

impl FreeResult {
    pub fn add(&mut self, file_len: i64, blocks: HashMap<i64, Vec<BlockLocation>>) {
        self.inodes += 1;
        self.bytes += file_len;
        self.blocks.extend(blocks);
    }
}
