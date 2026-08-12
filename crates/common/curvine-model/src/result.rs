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
    pub fn add(&mut self, bytes: i64, blocks: HashMap<i64, Vec<BlockLocation>>) {
        self.inodes += 1;
        self.bytes += bytes;
        self.blocks.extend(blocks);
    }
}

#[derive(Debug, Default)]
pub struct DeleteResult {
    // Number of file inodes removed. Directories are excluded so the value is
    // comparable with FreeResult stats produced by cache-mode free.
    pub inodes: u64,
    pub bytes: u64,
    pub blocks: HashMap<i64, Vec<BlockLocation>>,
}

impl DeleteResult {
    pub fn new() -> Self {
        Self {
            inodes: 0,
            bytes: 0,
            blocks: Default::default(),
        }
    }
}

impl From<DeleteResult> for FreeResult {
    fn from(value: DeleteResult) -> Self {
        Self {
            inodes: i64::try_from(value.inodes).unwrap_or(i64::MAX),
            bytes: i64::try_from(value.bytes).unwrap_or(i64::MAX),
            blocks: value.blocks,
        }
    }
}

impl From<FreeResult> for DeleteResult {
    fn from(value: FreeResult) -> Self {
        Self {
            inodes: u64::try_from(value.inodes).unwrap_or(0),
            bytes: u64::try_from(value.bytes).unwrap_or(0),
            blocks: value.blocks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_result_to_free_result_clamps_large_counts() {
        let delete_result = DeleteResult {
            inodes: i64::MAX as u64 + 1,
            bytes: u64::MAX,
            blocks: HashMap::new(),
        };

        let free_result: FreeResult = delete_result.into();

        assert_eq!(free_result.inodes, i64::MAX);
        assert_eq!(free_result.bytes, i64::MAX);
    }

    #[test]
    fn free_result_to_delete_result_ignores_negative_counts() {
        let free_result = FreeResult {
            inodes: -1,
            bytes: -1,
            blocks: HashMap::new(),
        };

        let delete_result: DeleteResult = free_result.into();

        assert_eq!(delete_result.inodes, 0);
        assert_eq!(delete_result.bytes, 0);
    }
}
