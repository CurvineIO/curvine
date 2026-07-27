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

use crate::state::WorkerInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MasterInfo {
    pub active_master: String,
    pub journal_nodes: Vec<String>,

    pub inode_dir_num: i64,
    pub inode_file_num: i64,
    pub block_num: i64,
    pub capacity: i64,
    pub available: i64,
    pub fs_used: i64,
    pub non_fs_used: i64,
    pub reserved_bytes: i64,

    pub live_workers: Vec<WorkerInfo>,
    pub blacklist_workers: Vec<WorkerInfo>,
    pub decommission_workers: Vec<WorkerInfo>,
    pub lost_workers: Vec<WorkerInfo>,
}

impl MasterInfo {
    pub fn journal_nodes_count(&self) -> usize {
        self.journal_nodes.len()
    }

    pub fn get_journal_nodes(&self, index: usize) -> Option<&String> {
        self.journal_nodes.get(index)
    }

    pub fn get_live_worker(&self, index: usize) -> Option<&WorkerInfo> {
        self.live_workers.get(index)
    }

    pub fn get_lost_worker(&self, index: usize) -> Option<&WorkerInfo> {
        self.lost_workers.get(index)
    }
}
