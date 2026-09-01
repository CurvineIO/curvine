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

use crate::fs::pcache::{Page, PageStore, PageStoreQuota};
use crate::FuseResult;
use bytes::BytesMut;
use curvine_config::FuseConf;

pub struct MemoryPageStore {
    quota: PageStoreQuota,
}

impl MemoryPageStore {
    pub fn new(page_size: u64, capacity: u64) -> Self {
        Self {
            quota: PageStoreQuota::new(page_size, capacity),
        }
    }

    pub fn from_conf(conf: &FuseConf) -> Self {
        Self::new(
            conf.write_back_page_size.as_byte(),
            conf.write_back_cache_size.as_byte(),
        )
    }
}

impl PageStore for MemoryPageStore {
    fn quota(&self) -> &PageStoreQuota {
        &self.quota
    }

    fn allocate(&self, _ino: u64, page_id: u64) -> FuseResult<Page> {
        self.add_usage(self.page_size())?;
        Ok(Page::with_memory(page_id, BytesMut::new()))
    }
}
