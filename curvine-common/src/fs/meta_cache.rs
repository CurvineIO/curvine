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

use crate::fs::Path;
use crate::state::{FileBlocks, FileStatus};
use orpc::sync::FastSyncCache;
use std::time::Duration;

pub struct MetaCache {
    list_cache: FastSyncCache<String, Vec<FileStatus>>,
    status_cache: FastSyncCache<String, FileStatus>,
    open_cache: FastSyncCache<String, FileBlocks>,
}

impl MetaCache {
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self {
            list_cache: FastSyncCache::new(capacity, ttl),
            status_cache: FastSyncCache::new(capacity, ttl),
            open_cache: FastSyncCache::new(capacity, ttl),
        }
    }

    pub fn get_list(&self, path: &Path) -> Option<Vec<FileStatus>> {
        self.list_cache.get(path.full_path())
    }

    pub fn get_status(&self, path: &Path) -> Option<FileStatus> {
        self.status_cache.get(path.full_path())
    }

    pub fn get_blocks(&self, path: &Path) -> Option<FileBlocks> {
        self.open_cache.get(path.full_path())
    }

    pub fn get_open(&self, path: &Path) -> Option<FileBlocks> {
        self.open_cache.get(path.full_path())
    }

    pub fn put_list(&self, path: &Path, list: Vec<FileStatus>) {
        self.list_cache.insert(path.clone_uri(), list);
    }

    pub fn put_status(&self, path: &Path, status: FileStatus) {
        self.status_cache.insert(path.clone_uri(), status);
    }

    pub fn put_open(&self, path: &Path, blocks: FileBlocks) {
        self.open_cache.insert(path.clone_uri(), blocks);
    }

    pub fn invalidate(&self, path: &Path) {
        self.list_cache.invalidate(path.full_path());
        self.status_cache.invalidate(path.full_path());
        self.open_cache.invalidate(path.full_path());
    }

    pub fn invalidate_list(&self, path: &Path) {
        self.list_cache.invalidate(path.full_path());
    }

    pub fn invalidate_status(&self, path: &Path) {
        self.status_cache.invalidate(path.full_path());
    }

    pub fn invalidate_open(&self, path: &Path) {
        self.open_cache.invalidate(path.full_path());
    }

    pub fn clear(&self) {
        self.list_cache.invalidate_all();
        self.status_cache.invalidate_all();
        self.open_cache.invalidate_all();
    }
}
