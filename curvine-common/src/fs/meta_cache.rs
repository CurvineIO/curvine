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
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct CacheValue {
    list: Option<Arc<Vec<FileStatus>>>,
    status: Option<Arc<FileStatus>>,
    blocks: Option<Arc<FileBlocks>>,
}

pub struct MetaCache {
    inner: FastSyncCache<String, Arc<CacheValue>>,
}

impl MetaCache {
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self {
            inner: FastSyncCache::new(capacity, ttl),
        }
    }

    pub fn get_list(&self, path: &Path) -> Option<Vec<FileStatus>> {
        self.inner
            .get(path.full_path())
            .and_then(|v| v.list.as_ref().map(|a| (**a).clone()))
    }

    pub fn get_status(&self, path: &Path) -> Option<FileStatus> {
        self.inner
            .get(path.full_path())
            .and_then(|v| v.status.as_ref().map(|a| (**a).clone()))
    }

    pub fn get_blocks(&self, path: &Path) -> Option<FileBlocks> {
        self.inner
            .get(path.full_path())
            .and_then(|v| v.blocks.as_ref().map(|a| (**a).clone()))
    }

    pub fn get_open(&self, path: &Path) -> Option<FileBlocks> {
        self.inner
            .get(path.full_path())
            .and_then(|v| v.blocks.as_ref().map(|a| (**a).clone()))
    }

    pub fn put_list(&self, path: &Path, list: Vec<FileStatus>) {
        self.inner
            .entry(path.clone_uri())
            .and_upsert_with(|maybe_entry| {
                let prev = maybe_entry.map(|e| e.into_value());
                Arc::new(CacheValue {
                    list: Some(Arc::new(list)),
                    status: prev.as_ref().and_then(|v| v.status.clone()),
                    blocks: prev.as_ref().and_then(|v| v.blocks.clone()),
                })
            });
    }

    pub fn put_status(&self, path: &Path, status: FileStatus) {
        self.inner
            .entry(path.clone_uri())
            .and_upsert_with(|maybe_entry| {
                let prev = maybe_entry.map(|e| e.into_value());
                Arc::new(CacheValue {
                    list: prev.as_ref().and_then(|v| v.list.clone()),
                    status: Some(Arc::new(status)),
                    blocks: prev.as_ref().and_then(|v| v.blocks.clone()),
                })
            });
    }

    pub fn put_open(&self, path: &Path, blocks: FileBlocks) {
        self.inner
            .entry(path.clone_uri())
            .and_upsert_with(|maybe_entry| {
                let prev = maybe_entry.map(|e| e.into_value());
                Arc::new(CacheValue {
                    list: prev.as_ref().and_then(|v| v.list.clone()),
                    status: prev.as_ref().and_then(|v| v.status.clone()),
                    blocks: Some(Arc::new(blocks)),
                })
            });
    }

    pub fn invalidate(&self, path: &Path) {
        self.inner.invalidate(path.full_path());
    }

    pub fn invalidate_list(&self, path: &Path) {
        self.inner
            .entry(path.clone_uri())
            .and_upsert_with(|maybe_entry| {
                let prev = maybe_entry.map(|e| e.into_value());
                Arc::new(CacheValue {
                    list: None,
                    status: prev.as_ref().and_then(|v| v.status.clone()),
                    blocks: prev.as_ref().and_then(|v| v.blocks.clone()),
                })
            });
    }

    pub fn invalidate_status(&self, path: &Path) {
        self.inner
            .entry(path.clone_uri())
            .and_upsert_with(|maybe_entry| {
                let prev = maybe_entry.map(|e| e.into_value());
                Arc::new(CacheValue {
                    list: prev.as_ref().and_then(|v| v.list.clone()),
                    status: None,
                    blocks: prev.as_ref().and_then(|v| v.blocks.clone()),
                })
            });
    }

    pub fn invalidate_open(&self, path: &Path) {
        self.inner
            .entry(path.clone_uri())
            .and_upsert_with(|maybe_entry| {
                let prev = maybe_entry.map(|e| e.into_value());
                Arc::new(CacheValue {
                    list: prev.as_ref().and_then(|v| v.list.clone()),
                    status: prev.as_ref().and_then(|v| v.status.clone()),
                    blocks: None,
                })
            });
    }

    pub fn clear(&self) {
        self.inner.invalidate_all();
    }
}
