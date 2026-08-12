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
use bytes::BytesMut;
use curvine_config::FuseConf;
use curvine_runtime::sync::AtomicCounter;

pub struct MemoryPageStore {
    page_size: u64,
    capacity: u64,
    used: AtomicCounter,
}

impl MemoryPageStore {
    pub fn new(page_size: u64, capacity: u64) -> Self {
        Self {
            page_size,
            capacity,
            used: AtomicCounter::new(0),
        }
    }

    pub fn from_conf(conf: &FuseConf) -> Self {
        Self::new(
            conf.write_back_page_size.as_byte(),
            conf.write_back_cache_size.as_byte(),
        )
    }

    fn try_reserve(&self, amount: u64) -> FuseResult<()> {
        let current = self.used.get();
        let Some(new) = current.checked_add(amount) else {
            return err_fuse!(libc::ENOSPC, "memory page store reservation overflow");
        };
        if new > self.capacity {
            return err_fuse!(libc::ENOSPC, "memory page store is full");
        }
        Ok(())
    }
}

impl PageStore for MemoryPageStore {
    fn page_size(&self) -> u64 {
        self.page_size
    }

    fn allocate(&self, _ino: u64, page_id: u64) -> FuseResult<Page> {
        self.try_reserve(self.page_size)?;
        Ok(Page::with_memory(page_id, BytesMut::new()))
    }

    fn add_usage(&self, len: u64) {
        self.used.add_and_get(len);
    }

    fn release_usage(&self, len: u64) {
        loop {
            let current = self.used.get();
            let new = current.saturating_sub(len);
            if self.used.compare_and_set(current, new) {
                break;
            }
        }
    }

    fn capacity(&self) -> u64 {
        self.capacity
    }

    fn used(&self) -> u64 {
        self.used.get()
    }

    fn free_ratio(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        (self.capacity.saturating_sub(self.used.get()) as f64 / self.capacity as f64).min(1.0)
    }
}
