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

use crate::worker::storage::VfsDir;
use curvine_common::state::StorageType;
use indexmap::IndexMap;
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::sync::Arc;

pub enum ChoosingPolicy {
    Robin(RobinChoosingPolicy),
}

impl ChoosingPolicy {
    pub fn choose_dir<'a>(
        &mut self,
        dirs: &'a IndexMap<u32, Arc<VfsDir>>,
        storage_type: StorageType,
        required_bytes: i64,
    ) -> CommonResult<&'a Arc<VfsDir>> {
        match self {
            ChoosingPolicy::Robin(c) => c.choose_dir(dirs, storage_type, required_bytes),
        }
    }
}

pub struct RobinChoosingPolicy {
    cur_dirs: HashMap<StorageType, usize>,
}

impl Default for RobinChoosingPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinChoosingPolicy {
    pub fn new() -> Self {
        Self {
            cur_dirs: HashMap::new(),
        }
    }

    fn choose_dir<'a>(
        &mut self,
        dirs: &'a IndexMap<u32, Arc<VfsDir>>,
        storage_type: StorageType,
        required_bytes: i64,
    ) -> CommonResult<&'a Arc<VfsDir>> {
        if dirs.is_empty() {
            return err_box!("No physical storage directories are configured");
        }

        let mut res = self.get_next_dir(dirs, storage_type, required_bytes);
        if res.is_none() && storage_type != StorageType::Disk {
            res = self.get_next_dir(dirs, StorageType::Disk, required_bytes)
        }

        if let Some(v) = res {
            Ok(v)
        } else {
            err_box!(
                "Not enough {:?} storage capacity for {} bytes",
                storage_type,
                required_bytes
            )
        }
    }

    fn get_next_dir<'a>(
        &mut self,
        dirs: &'a IndexMap<u32, Arc<VfsDir>>,
        stg_type: StorageType,
        block_size: i64,
    ) -> Option<&'a Arc<VfsDir>> {
        let mut index = *self.cur_dirs.get(&stg_type).unwrap_or(&0);

        let start_index = index;
        loop {
            index = (index + 1) % dirs.len();
            let dir = &dirs[index];
            if dir.can_allocate(stg_type, block_size) {
                self.cur_dirs.insert(stg_type, index);
                return Some(dir);
            }

            if index == start_index {
                return None;
            }
        }
    }
}
