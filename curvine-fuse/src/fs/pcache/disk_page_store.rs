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
use crate::macros::err_fuse;
use crate::FuseResult;
use curvine_config::FuseConf;
use curvine_core_error::err_box;
use curvine_io::LocalFile;
use curvine_runtime::common::FileUtils;
use curvine_sys::FsStats;
use std::fs::File;
use std::hash::Hasher;

pub struct DiskPageStore {
    quota: PageStoreQuota,
    dirs: Vec<FsStats>,
    // Held for the store lifetime so a second process cannot wipe this cache.
    _locks: Vec<File>,
}

impl DiskPageStore {
    pub fn new(page_size: u64, capacity: u64, dirs: Vec<FsStats>) -> Self {
        Self {
            quota: PageStoreQuota::new(page_size, capacity),
            dirs,
            _locks: Vec::new(),
        }
    }

    pub fn from_conf(conf: &FuseConf) -> FuseResult<Self> {
        if conf.write_back_disk_dirs.is_empty() {
            return err_box!(
                "write_back_disk_dirs must not be empty when write_back_cache_type=disk"
            );
        }

        let cache_name = Self::cache_dir_name(&conf.mnt_path)?;

        // Phase 1: acquire every dir lock before touching any directory. A
        // lock taken one dir at a time interleaved with wiping would let a
        // second process delete a live instance's pages on dir[0] and only
        // then fail on dir[1].
        let mut locks = vec![];
        for item in &conf.write_back_disk_dirs {
            let lock = Self::try_lock_cache(item, &cache_name)?;
            locks.push(lock);
        }

        // Phase 2: all locks held; leftover pages are not recoverable after
        // restart, wipe before reuse.
        let mut dirs = vec![];
        for item in &conf.write_back_disk_dirs {
            let dir = format!("{}/curvine-cache/{}", item, cache_name);
            let fs_stats = FsStats::new(dir);
            FileUtils::delete_path(fs_stats.path(), true)?;
            FileUtils::create_dir(fs_stats.path(), true)?;
            dirs.push(fs_stats);
        }

        Ok(Self {
            quota: PageStoreQuota::new(
                conf.write_back_page_size.as_byte(),
                conf.write_back_cache_size.as_byte(),
            ),
            dirs,
            _locks: locks,
        })
    }

    pub(crate) fn cache_dir_name(mnt_path: &str) -> FuseResult<String> {
        let prefix = mnt_path.replace('/', "-");
        if prefix.is_empty() || prefix == "." || prefix == ".." {
            return err_box!(
                "write-back cache dir name derived from mnt_path {:?} is invalid",
                mnt_path
            );
        }

        let mut hasher = fxhash::FxHasher::default();
        hasher.write(mnt_path.as_bytes());
        Ok(format!("{}_{:x}", prefix, hasher.finish()))
    }

    fn try_lock_cache(disk_dir: &str, cache_name: &str) -> FuseResult<File> {
        let parent = format!("{}/curvine-cache", disk_dir);
        FileUtils::create_dir(&parent, true)?;
        let lock_path = format!("{}/{}.lock", parent, cache_name);
        let file = File::options()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        // Fail fast: a held lock means another live process owns this cache
        // (or the parent is still shutting down on the reload path). Locks are
        // released automatically when that process exits, so retrying here
        // would only turn a real double-start into a silent long wait.
        if let Err(e) = file.try_lock() {
            return err_box!(
                "write-back cache dir is in use by another process ({}): {}",
                lock_path,
                e
            );
        }
        Ok(file)
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

            if self.dirs[idx].available_space() >= self.page_size() {
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
        self.add_usage(self.page_size())?;

        let allocated = (|| {
            let idx = self.allocate_dir(ino, page_id)?;
            let Some(dir) = self.dirs.get(idx) else {
                return err_box!("invalid directory index: {}", idx);
            };
            let path = Self::page_path(dir, ino, page_id);
            let file = LocalFile::with_write(path, true)?;
            Ok(Page::with_disk(page_id, file))
        })();

        if allocated.is_err() {
            self.release_usage(self.page_size());
        }
        allocated
    }

    pub fn free_space(&self) -> u64 {
        self.dirs.iter().map(|dir| dir.free_space()).sum::<u64>()
    }
}

impl PageStore for DiskPageStore {
    fn quota(&self) -> &PageStoreQuota {
        &self.quota
    }

    fn allocate(&self, ino: u64, page_id: u64) -> FuseResult<Page> {
        DiskPageStore::allocate(self, ino, page_id)
    }

    fn free_ratio(&self) -> f64 {
        let quota_ratio = self.quota.free_ratio();
        if self.capacity() == 0 {
            return quota_ratio;
        }
        let disk_ratio = (self.free_space() as f64 / self.capacity() as f64).min(1.0);
        quota_ratio.min(disk_ratio)
    }
}

#[cfg(test)]
mod tests {
    use super::DiskPageStore;
    use curvine_config::FuseConf;
    use std::fs::File;
    use std::path::{Path, PathBuf};

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

    #[test]
    fn cache_dir_name_uses_prefix_and_hash() {
        let name = DiskPageStore::cache_dir_name("/curvine-fuse").unwrap();
        assert!(name.starts_with("-curvine-fuse_"));
        let name = DiskPageStore::cache_dir_name("/data/mnt/fuse").unwrap();
        assert!(name.starts_with("-data-mnt-fuse_"));
        let name = DiskPageStore::cache_dir_name("/").unwrap();
        assert!(name.starts_with("-_"));
    }

    #[test]
    fn cache_dir_name_is_injective_for_slash_collisions() {
        let a = DiskPageStore::cache_dir_name("/curvine-fuse").unwrap();
        let b = DiskPageStore::cache_dir_name("/curvine/fuse").unwrap();
        let c = DiskPageStore::cache_dir_name("/data/mnt1").unwrap();
        let d = DiskPageStore::cache_dir_name("/data-mnt1").unwrap();
        assert_ne!(a, b);
        assert_ne!(c, d);
    }

    #[test]
    fn cache_dir_name_rejects_empty_dot_and_dotdot() {
        assert!(DiskPageStore::cache_dir_name("").is_err());
        assert!(DiskPageStore::cache_dir_name(".").is_err());
        assert!(DiskPageStore::cache_dir_name("..").is_err());
    }

    fn disk_conf(dir: &TempDir) -> FuseConf {
        FuseConf {
            write_back_cache_type: "disk".to_string(),
            write_back_disk_dirs: vec![dir.path().to_str().unwrap().to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn from_conf_creates_sibling_lock_file() {
        let dir = TempDir::new("cache_lock");
        let conf = disk_conf(&dir);
        let _store = DiskPageStore::from_conf(&conf).unwrap();
        let cache_name = DiskPageStore::cache_dir_name("/curvine-fuse").unwrap();
        let lock_path = dir
            .path()
            .join("curvine-cache")
            .join(format!("{cache_name}.lock"));
        assert!(lock_path.is_file(), "expected lock {}", lock_path.display());
    }

    /// A second `from_conf` on the same cache dirs must fail while the first
    /// store is still alive: the held `File` locks make `try_lock` return
    /// `WouldBlock`, and the fail-fast path (no reload env) refuses before any
    /// directory is wiped.
    #[test]
    fn from_conf_fails_while_first_store_alive() {
        let dir = TempDir::new("cache_double");
        let conf = disk_conf(&dir);
        let _first = DiskPageStore::from_conf(&conf).unwrap();

        // `DiskPageStore` is not `Debug`; match instead of `unwrap_err`.
        let err = match DiskPageStore::from_conf(&conf) {
            Ok(_) => panic!("second from_conf must fail while the first store is alive"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("in use by another process"),
            "unexpected error: {}",
            err
        );
    }

    /// With two dirs, locking dir[0] then failing on dir[1] must not wipe
    /// dir[0]: the pages left there by the running instance survive the
    /// failed start.
    #[test]
    fn from_conf_does_not_wipe_dir0_when_dir1_locked() {
        let dir0 = TempDir::new("cache_stripe0");
        let dir1 = TempDir::new("cache_stripe1");

        let cache_name = DiskPageStore::cache_dir_name("/curvine-fuse").unwrap();

        // Pre-lock dir1 as if another live process owns it.
        let parent1 = dir1.path().join("curvine-cache");
        std::fs::create_dir_all(&parent1).unwrap();
        let held = File::options()
            .create(true)
            .write(true)
            .truncate(false)
            .open(parent1.join(format!("{cache_name}.lock")))
            .unwrap();
        held.try_lock().unwrap();

        // A live page on dir0 that must survive the failed start.
        let cache0 = dir0.path().join("curvine-cache").join(&cache_name);
        std::fs::create_dir_all(&cache0).unwrap();
        let live_page = cache0.join("1_0");
        std::fs::write(&live_page, b"dirty").unwrap();

        let conf = FuseConf {
            write_back_cache_type: "disk".to_string(),
            write_back_disk_dirs: vec![
                dir0.path().to_str().unwrap().to_string(),
                dir1.path().to_str().unwrap().to_string(),
            ],
            ..Default::default()
        };
        let err = match DiskPageStore::from_conf(&conf) {
            Ok(_) => panic!("from_conf must fail while dir1 is locked"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("in use by another process"),
            "unexpected error: {}",
            err
        );
        assert!(live_page.is_file(), "live page on dir0 was wiped");
    }
}
