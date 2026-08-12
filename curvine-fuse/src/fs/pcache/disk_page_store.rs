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

use crate::fs::pcache::{Page, PageStore};
use crate::macros::err_fuse;
use crate::FuseResult;
use curvine_config::FuseConf;
use curvine_core_error::err_box;
use curvine_io::LocalFile;
use curvine_runtime::common::{FileUtils, Utils};
use curvine_runtime::sync::AtomicCounter;
use curvine_sys::FsStats;
use std::hash::Hasher;

pub struct DiskPageStore {
    page_size: u64,
    capacity: u64,
    dirs: Vec<FsStats>,
    used: AtomicCounter,
}

impl DiskPageStore {
    pub fn new(page_size: u64, capacity: u64, dirs: Vec<FsStats>) -> Self {
        Self {
            page_size,
            capacity,
            dirs,
            used: AtomicCounter::new(0),
        }
    }

    pub fn from_conf(conf: &FuseConf) -> FuseResult<Self> {
        if conf.write_back_disk_dirs.is_empty() {
            return err_box!(
                "write_back_disk_dirs must not be empty when write_back_cache_type=disk"
            );
        }

        let mut dirs = vec![];
        let id = Utils::unique_id();
        for item in &conf.write_back_disk_dirs {
            let dir = format!("{}/curvine-cache/{}", item, id);
            let fs_stats = FsStats::new(dir);
            FileUtils::create_dir(fs_stats.path(), true)?;

            dirs.push(fs_stats);
        }
        let store = Self::new(
            conf.write_back_page_size.as_byte(),
            conf.write_back_cache_size.as_byte(),
            dirs,
        );
        Ok(store)
    }

    fn try_reserve(&self, amount: u64) -> FuseResult<()> {
        let current = self.used.get();
        let Some(new) = current.checked_add(amount) else {
            return err_fuse!(libc::ENOSPC, "disk page store reservation overflow");
        };
        if new > self.capacity {
            return err_fuse!(libc::ENOSPC, "disk page store is full");
        }
        Ok(())
    }

    fn allocate_dir(&self, ino: u64, page_id: u64) -> FuseResult<usize> {
        let n = self.dirs.len();
        if n == 0 {
            return err_fuse!(libc::ENOSPC, "no write-back directories configured.");
        }

        let mut hasher = fxhash::FxHasher::default();
        hasher.write_u64(ino);
        hasher.write_u64(page_id);
        let start = (hasher.finish() as usize) % n;
        for i in 0..n {
            let idx = (start + i) % n;

            if self.dirs[idx].available_space() >= self.page_size {
                return Ok(idx);
            }
        }

        err_fuse!(
            libc::ENOSPC,
            "no write-back directories have available space."
        )
    }

    pub fn page_path(dir: &FsStats, ino: u64, page_id: u64) -> String {
        format!("{}/{}_{}", dir.path().display(), ino, page_id)
    }

    pub fn allocate(&self, ino: u64, page_id: u64) -> FuseResult<Page> {
        self.try_reserve(self.page_size)?;

        let idx = self.allocate_dir(ino, page_id)?;
        let Some(dir) = self.dirs.get(idx) else {
            return err_box!("invalid directory index: {}", idx);
        };

        let path = Self::page_path(dir, ino, page_id);
        let file = LocalFile::with_write(path, true)?;
        Ok(Page::with_disk(page_id, file))
    }

    pub fn add_usage(&self, len: u64) {
        self.used.add_and_get(len);
    }

    pub fn release_usage(&self, len: u64) {
        loop {
            let current = self.used.get();
            let new = current.saturating_sub(len);
            if self.used.compare_and_set(current, new) {
                break;
            }
        }
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub fn used(&self) -> u64 {
        self.used.get()
    }

    pub fn free_space(&self) -> u64 {
        self.dirs.iter().map(|dir| dir.free_space()).sum::<u64>()
    }

    pub fn free_ratio(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }

        let quota_ratio =
            (self.capacity.saturating_sub(self.used()) as f64 / self.capacity as f64).min(1.0);
        let disk_ratio = (self.free_space() as f64 / self.capacity as f64).min(1.0);

        quota_ratio.min(disk_ratio)
    }
}

impl PageStore for DiskPageStore {
    fn page_size(&self) -> u64 {
        self.page_size
    }

    fn allocate(&self, ino: u64, page_id: u64) -> FuseResult<Page> {
        DiskPageStore::allocate(self, ino, page_id)
    }

    fn add_usage(&self, len: u64) {
        DiskPageStore::add_usage(self, len)
    }

    fn release_usage(&self, len: u64) {
        DiskPageStore::release_usage(self, len)
    }

    fn capacity(&self) -> u64 {
        self.capacity
    }

    fn used(&self) -> u64 {
        self.used.get()
    }

    fn free_ratio(&self) -> f64 {
        DiskPageStore::free_ratio(self)
    }
}
