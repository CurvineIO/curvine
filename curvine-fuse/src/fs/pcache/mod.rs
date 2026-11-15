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

mod inode_cache;

pub use self::inode_cache::*;

mod cache_manager;
pub use self::cache_manager::CacheManager;

mod write_back_work;
pub use self::write_back_work::*;

mod page;
pub use self::page::*;

mod disk_page_store;
pub use disk_page_store::*;

mod memory_page_store;
pub use memory_page_store::*;

mod page_store;
pub use page_store::*;

mod dirty_list;
pub use dirty_list::*;

#[cfg(test)]
mod write_back_integration;

#[derive(Default, Debug)]
pub struct EvictInode {
    pub ino: u64,
    pub page_len: u64,
    pub file_len: u64,
}

impl EvictInode {
    pub fn new(ino: u64, page_len: u64, file_len: u64) -> Self {
        EvictInode {
            ino,
            page_len,
            file_len,
        }
    }
}

#[cfg(test)]
#[path = "tests/truncate_aba_test.rs"]
mod truncate_aba_test;

#[cfg(test)]
#[path = "tests/flush_cancel_test.rs"]
mod flush_cancel_test;
