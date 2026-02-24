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

use curvine_common::state::FileStatus;
use moka::sync::Cache;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MetaGate {
    negative_open_notfound: Cache<String, ()>,
    positive_ufs_status: Cache<String, FileStatus>,
}

impl MetaGate {
    pub fn new(
        negative_capacity: u64,
        negative_ttl_ms: u64,
        positive_capacity: u64,
        positive_ttl_ms: u64,
    ) -> Self {
        let negative_ttl = Duration::from_millis(negative_ttl_ms.max(1));
        let positive_ttl = Duration::from_millis(positive_ttl_ms.max(1));
        let negative_open_notfound = Cache::builder()
            .max_capacity(negative_capacity.max(1))
            .time_to_live(negative_ttl)
            .build();
        let positive_ufs_status = Cache::builder()
            .max_capacity(positive_capacity.max(1))
            .time_to_live(positive_ttl)
            .build();

        Self {
            negative_open_notfound,
            positive_ufs_status,
        }
    }

    pub fn should_skip_open_probe(&self, key: &str) -> bool {
        self.negative_open_notfound.get(key).is_some()
    }

    pub fn mark_open_notfound(&self, key: String) {
        self.negative_open_notfound.insert(key, ());
    }

    pub fn clear_open_notfound(&self, key: &str) {
        self.negative_open_notfound.invalidate(key);
    }

    pub fn get_ufs_status(&self, key: &str) -> Option<FileStatus> {
        self.positive_ufs_status.get(key)
    }

    pub fn put_ufs_status(&self, key: String, status: FileStatus) {
        self.positive_ufs_status.insert(key, status);
    }

    pub fn clear_ufs_status(&self, key: &str) {
        self.positive_ufs_status.invalidate(key);
    }
}
