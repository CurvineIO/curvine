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

use crate::fs::pcache::{DiskPageStore, MemoryPageStore, Page};
use crate::FuseResult;
use curvine_config::FuseConf;
use curvine_core_error::err_box;

pub trait PageStore: Send + Sync {
    fn page_size(&self) -> u64;
    fn allocate(&self, ino: u64, page_id: u64) -> FuseResult<Page>;
    fn add_usage(&self, len: u64);
    fn release_usage(&self, len: u64);
    fn capacity(&self) -> u64;
    fn used(&self) -> u64;
    fn free_ratio(&self) -> f64;
}

pub enum PageStoreVariant {
    Disk(DiskPageStore),
    Memory(MemoryPageStore),
}

impl PageStoreVariant {
    pub fn from_conf(conf: &FuseConf) -> FuseResult<Self> {
        let store = match conf.write_back_cache_type.as_str() {
            "disk" => PageStoreVariant::Disk(DiskPageStore::from_conf(conf)?),
            "memory" => PageStoreVariant::Memory(MemoryPageStore::from_conf(conf)),
            other => {
                return err_box!(
                    "unsupported write_back_cache_type '{}', expected 'memory' or 'disk'",
                    other
                );
            }
        };

        Ok(store)
    }
}

impl PageStore for PageStoreVariant {
    fn page_size(&self) -> u64 {
        match self {
            PageStoreVariant::Disk(s) => s.page_size(),
            PageStoreVariant::Memory(s) => s.page_size(),
        }
    }

    fn allocate(&self, ino: u64, page_id: u64) -> FuseResult<Page> {
        match self {
            PageStoreVariant::Disk(s) => DiskPageStore::allocate(s, ino, page_id),
            PageStoreVariant::Memory(s) => s.allocate(ino, page_id),
        }
    }

    fn add_usage(&self, len: u64) {
        match self {
            PageStoreVariant::Disk(s) => DiskPageStore::add_usage(s, len),
            PageStoreVariant::Memory(s) => s.add_usage(len),
        }
    }

    fn release_usage(&self, len: u64) {
        match self {
            PageStoreVariant::Disk(s) => DiskPageStore::release_usage(s, len),
            PageStoreVariant::Memory(s) => s.release_usage(len),
        }
    }

    fn capacity(&self) -> u64 {
        match self {
            PageStoreVariant::Disk(s) => s.capacity(),
            PageStoreVariant::Memory(s) => s.capacity(),
        }
    }

    fn used(&self) -> u64 {
        match self {
            PageStoreVariant::Disk(s) => s.used(),
            PageStoreVariant::Memory(s) => s.used(),
        }
    }

    fn free_ratio(&self) -> f64 {
        match self {
            PageStoreVariant::Disk(s) => DiskPageStore::free_ratio(s),
            PageStoreVariant::Memory(s) => s.free_ratio(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::pcache::{DiskPageStore, MemoryPageStore};
    use curvine_sys::FsStats;
    use std::path::{Path, PathBuf};

    const PAGE_SIZE: u64 = 128 * 1024;
    const CAPACITY: u64 = 1024 * 1024;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("curvine_pcache_{}_{}", tag, std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn memory_store(page_size: u64, capacity: u64) -> PageStoreVariant {
        PageStoreVariant::Memory(MemoryPageStore::new(page_size, capacity))
    }

    fn disk_store(page_size: u64, capacity: u64, dir: &TempDir) -> PageStoreVariant {
        let stats = FsStats::new(dir.path().to_str().unwrap());
        PageStoreVariant::Disk(DiskPageStore::new(page_size, capacity, vec![stats]))
    }

    #[test]
    fn memory_page_size_returns_config() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        assert_eq!(store.page_size(), PAGE_SIZE);
    }

    #[test]
    fn disk_page_size_returns_config() {
        let dir = TempDir::new("page_size");
        let store = disk_store(PAGE_SIZE, CAPACITY, &dir);
        assert_eq!(store.page_size(), PAGE_SIZE);
    }

    #[test]
    fn capacity_returns_config() {
        let mem = memory_store(PAGE_SIZE, CAPACITY);
        assert_eq!(mem.capacity(), CAPACITY);

        let dir = TempDir::new("capacity");
        let disk = disk_store(PAGE_SIZE, CAPACITY, &dir);
        assert_eq!(disk.capacity(), CAPACITY);
    }

    #[test]
    fn used_starts_zero() {
        let mem = memory_store(PAGE_SIZE, CAPACITY);
        assert_eq!(mem.used(), 0);

        let dir = TempDir::new("used_zero");
        let disk = disk_store(PAGE_SIZE, CAPACITY, &dir);
        assert_eq!(disk.used(), 0);
    }

    #[test]
    fn add_usage_accumulates() {
        let mem = memory_store(PAGE_SIZE, CAPACITY);
        mem.add_usage(100);
        mem.add_usage(50);
        assert_eq!(mem.used(), 150);

        let dir = TempDir::new("add_usage");
        let disk = disk_store(PAGE_SIZE, CAPACITY, &dir);
        disk.add_usage(100);
        disk.add_usage(50);
        assert_eq!(disk.used(), 150);
    }

    #[test]
    fn release_usage_decrements() {
        let mem = memory_store(PAGE_SIZE, CAPACITY);
        mem.add_usage(100);
        mem.release_usage(40);
        assert_eq!(mem.used(), 60);

        let dir = TempDir::new("release_usage");
        let disk = disk_store(PAGE_SIZE, CAPACITY, &dir);
        disk.add_usage(100);
        disk.release_usage(40);
        assert_eq!(disk.used(), 60);
    }

    #[test]
    fn release_usage_saturates_at_zero() {
        let mem = memory_store(PAGE_SIZE, CAPACITY);
        mem.add_usage(10);
        mem.release_usage(100);
        assert_eq!(mem.used(), 0);

        let dir = TempDir::new("release_saturate");
        let disk = disk_store(PAGE_SIZE, CAPACITY, &dir);
        disk.add_usage(10);
        disk.release_usage(100);
        assert_eq!(disk.used(), 0);
    }

    #[test]
    fn memory_allocate_returns_memory_page() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        let page = store.allocate(1, PAGE_SIZE).unwrap();
        assert!(!page.is_disk());
        assert!(page.path().is_none());
        assert_eq!(page.page_id, PAGE_SIZE);
        assert_eq!(page.start(), 0);
        assert_eq!(page.len(), 0);
    }

    #[test]
    fn disk_allocate_returns_disk_page() {
        let dir = TempDir::new("disk_allocate");
        let store = disk_store(PAGE_SIZE, CAPACITY, &dir);
        let page = store.allocate(1, PAGE_SIZE).unwrap();
        assert!(page.is_disk());
        assert!(page.path().is_some());
        assert_eq!(page.page_id, PAGE_SIZE);
        assert_eq!(page.start(), 0);
        assert_eq!(page.len(), 0);

        let path = page.path().unwrap();
        assert!(Path::new(path).exists());
    }

    #[test]
    fn disk_allocate_no_dirs_fails() {
        let store = PageStoreVariant::Disk(DiskPageStore::new(PAGE_SIZE, CAPACITY, vec![]));
        let err = store.allocate(1, 0).err().unwrap();
        assert_eq!(err.errno, libc::ENOSPC);
    }

    #[test]
    fn memory_allocate_exceeds_capacity_fails() {
        let store = memory_store(PAGE_SIZE, PAGE_SIZE);
        store.add_usage(PAGE_SIZE);
        let err = store.allocate(1, 0).err().unwrap();
        assert_eq!(err.errno, libc::ENOSPC);
    }

    #[test]
    fn memory_allocate_at_capacity_boundary_succeeds() {
        let store = memory_store(PAGE_SIZE, PAGE_SIZE * 2);
        store.add_usage(PAGE_SIZE);
        let page = store.allocate(1, 0).unwrap();
        assert_eq!(page.page_id, 0);
        assert_eq!(page.start(), 0);
        assert_eq!(page.len(), 0);
    }

    #[test]
    fn memory_free_ratio_full() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        assert_eq!(store.free_ratio(), 1.0);
    }

    #[test]
    fn memory_free_ratio_after_usage() {
        let store = memory_store(100, 1000);
        store.add_usage(250);
        assert!((store.free_ratio() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn memory_free_ratio_zero_capacity() {
        let store = memory_store(PAGE_SIZE, 0);
        assert_eq!(store.free_ratio(), 0.0);
    }

    #[test]
    fn memory_free_ratio_used_exceeds_capacity() {
        let store = memory_store(100, 1000);
        store.add_usage(2000);
        assert_eq!(store.free_ratio(), 0.0);
    }

    #[test]
    fn disk_free_ratio_within_bounds() {
        let dir = TempDir::new("disk_free_ratio");
        let store = disk_store(PAGE_SIZE, CAPACITY, &dir);
        let ratio = store.free_ratio();
        assert!((0.0..=1.0).contains(&ratio));
    }

    #[test]
    fn memory_page_write_read_roundtrip() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        let mut page = store.allocate(1, 0).unwrap();
        page.write(0, b"hello").unwrap();
        assert_eq!(page.len(), 5);
        assert_eq!(&page.read(0, 5).unwrap()[..], b"hello");
    }

    #[test]
    fn disk_page_write_read_roundtrip() {
        let dir = TempDir::new("disk_roundtrip");
        let store = disk_store(PAGE_SIZE, CAPACITY, &dir);
        let mut page = store.allocate(1, 0).unwrap();
        page.write(0, b"hello").unwrap();
        assert_eq!(page.len(), 5);
        assert_eq!(&page.read(0, 5).unwrap()[..], b"hello");
    }

    #[test]
    fn memory_page_write_at_offset_then_read_back() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        let mut page = store.allocate(1, 0).unwrap();
        page.write(100, b"abc").unwrap();
        assert_eq!(page.start(), 100);
        assert_eq!(page.len(), 3);
        assert_eq!(page.end(), 103);
        assert_eq!(&page.read(100, 3).unwrap()[..], b"abc");
    }

    #[test]
    fn memory_page_read_out_of_range_fails() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        let mut page = store.allocate(1, 0).unwrap();
        page.write(0, b"hi").unwrap();
        assert!(page.read(0, 3).is_err());
    }

    #[test]
    fn disk_page_clear_removes_file() {
        let dir = TempDir::new("disk_clear");
        let store = disk_store(PAGE_SIZE, CAPACITY, &dir);
        let mut page = store.allocate(1, 0).unwrap();
        page.write(0, b"data").unwrap();
        let path = PathBuf::from(page.path().unwrap());
        assert!(path.exists());

        let len = page.clear();
        assert_eq!(len, 4);
        assert!(!path.exists());
    }

    #[test]
    fn disk_page_path_contains_ino_and_page_id() {
        let dir = TempDir::new("page_path");
        let store = disk_store(PAGE_SIZE, CAPACITY, &dir);
        let page = store.allocate(7, PAGE_SIZE).unwrap();
        let path = page.path().unwrap();
        assert_eq!(path, format!("{}/7_{}", dir.path().display(), PAGE_SIZE));
    }

    #[test]
    fn memory_allocate_any_ino_and_page_id() {
        let store = memory_store(PAGE_SIZE, CAPACITY);
        let a = store.allocate(1, 0).unwrap();
        let b = store.allocate(2, PAGE_SIZE * 2).unwrap();
        assert_eq!(a.page_id, 0);
        assert_eq!(a.start(), 0);
        assert_eq!(b.page_id, PAGE_SIZE * 2);
        assert_eq!(b.start(), 0);
    }

    #[test]
    fn from_conf_defaults_to_memory() {
        let conf = FuseConf::default();
        let store = PageStoreVariant::from_conf(&conf).unwrap();
        assert!(matches!(store, PageStoreVariant::Memory(_)));
    }

    #[test]
    fn from_conf_disk_when_configured() {
        let dir = TempDir::new("from_conf_disk");
        let conf = FuseConf {
            write_back_cache_type: "disk".to_string(),
            write_back_disk_dirs: vec![dir.path().to_str().unwrap().to_string()],
            ..Default::default()
        };
        let store = PageStoreVariant::from_conf(&conf).unwrap();
        assert!(matches!(store, PageStoreVariant::Disk(_)));
    }

    #[test]
    fn from_conf_unknown_type_errors() {
        let conf = FuseConf {
            write_back_cache_type: "unknown".to_string(),
            ..Default::default()
        };
        let err = PageStoreVariant::from_conf(&conf).err().unwrap();
        assert!(err
            .to_string()
            .contains("unsupported write_back_cache_type"));
    }

    #[test]
    fn from_conf_disk_empty_dirs_errors() {
        let conf = FuseConf {
            write_back_cache_type: "disk".to_string(),
            write_back_disk_dirs: vec![],
            ..Default::default()
        };
        let err = PageStoreVariant::from_conf(&conf).err().unwrap();
        assert!(err.to_string().contains("write_back_disk_dirs"));
    }
}
