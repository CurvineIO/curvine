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

/*!
# Mount Cache System

High-performance caching layer for filesystem mount information with bidirectional path mapping.

## Data Structure Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                        MountCache                               │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ update_interval │  │   last_update   │  │     mounts      │  │
│  │     (u64)       │  │(AtomicCounter)  │  │  RwLock<Map>    │  │
│  │ TTL in millis   │  │ lock-free time  │  │ thread-safe     │  │
│  └─────────────────┘  └─────────────────┘  └─────────┬───────┘  │
│                                                       │          │
└───────────────────────────────────────────────────────┼──────────┘
                                                        │
        ┌───────────────────────────────────────────────▼──────────┐
        │                    InnerMap                              │
        │           (Bidirectional Path Index)                    │
        │                                                         │
        │  ┌─────────────────────────┐  ┌─────────────────────────┐│
        │  │        cv_map           │  │       ufs_map           ││
        │  │FastHashMap<String, Arc> │  │FastHashMap<String, Arc> ││
        │  │                         │  │                         ││
        │  │Key: CV Path             │  │Key: UFS Path            ││
        │  │"/data/ml/model.bin"     │  │"s3://bucket/model.bin"  ││
        │  │"/data/ml/"              │  │"s3://bucket/"           ││
        │  │"/data/"                 │  │"hdfs://cluster/data/"   ││
        │  │                         │  │                         ││
        │  │Val: Arc<MountValue> ────┼──┼──▶ Same Instance       ││
        │  └─────────────────────────┘  └─────────────────────────┘│
        └─────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │          MountValue          │
                    │                              │
                    │  ┌─────────┐ ┌─────────────┐ │
                    │  │  info   │ │     ufs     │ │
                    │  │MountInfo│ │UfsFileSystem│ │
                    │  │metadata │ │ I/O handler │ │
                    │  └─────────┘ └─────────────┘ │
                    │           mount_id           │
                    │          (String)           │
                    └─────────────────────────────┘
```
*/

use crate::{UfsFileSystem, UnifiedFileSystem};
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::MountInfo;
use curvine_runtime::common::{FastHashMap, LocalTime};
use curvine_runtime::runtime::RpcRuntime;
use curvine_runtime::sync::AtomicCounter;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// Represents a single mount point with its filesystem handler.
/// Contains mount metadata, UFS handler, and path conversion utilities.
pub struct MountValue {
    pub info: MountInfo,
    ufs: OnceCell<UfsFileSystem>,
    pub mount_id: String,
}

impl MountValue {
    pub fn new(info: MountInfo) -> FsResult<Self> {
        let mount_id = format!("{}", info.mount_id);

        Ok(Self {
            info,
            ufs: OnceCell::new(),
            mount_id,
        })
    }

    pub fn ufs(&self) -> FsResult<UfsFileSystem> {
        self.ufs
            .get_or_try_init(|| {
                let ufs_path = Path::from_str(&self.info.ufs_path)?;
                UfsFileSystem::new(&ufs_path, self.info.properties.clone(), self.info.provider)
            })
            .cloned()
    }

    /// Converts CV path to UFS path
    /// Example: cv://cluster/data/file.txt -> s3://bucket/data/file.txt
    pub fn get_ufs_path(&self, cv_path: &Path) -> CommonResult<Path> {
        self.info.get_ufs_path(cv_path)
    }

    /// Converts UFS path to CV path
    /// Example: s3://bucket/data/file.txt -> cv://cluster/data/file.txt
    pub fn get_cv_path(&self, ufs_path: &Path) -> CommonResult<Path> {
        self.info.get_cv_path(ufs_path)
    }

    pub fn toggle_path(&self, path: &Path) -> CommonResult<Path> {
        self.info.toggle_path(path)
    }

    pub fn mount_id(&self) -> &str {
        &self.mount_id
    }
}

#[derive(Default)]
struct InnerMap {
    ufs_map: FastHashMap<String, Arc<MountValue>>,
    cv_map: FastHashMap<String, Arc<MountValue>>,
}

impl InnerMap {
    pub fn insert(&mut self, info: MountInfo) -> CommonResult<()> {
        let value = Arc::new(MountValue::new(info)?);
        self.cv_map
            .insert(value.info.cv_path.clone(), value.clone());
        self.ufs_map.insert(value.info.ufs_path.clone(), value);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.cv_map.clear();
        self.ufs_map.clear();
    }

    pub fn remove(&mut self, path: &Path) {
        if path.is_cv() {
            if let Some(info) = self.cv_map.remove(path.path()) {
                let _ = self.ufs_map.remove(&info.info.ufs_path);
            }
        } else if let Some(info) = self.ufs_map.remove(path.full_path()) {
            let _ = self.cv_map.remove(&info.info.cv_path);
        }
    }

    pub fn get(&self, is_cv: bool, path: &str) -> Option<Arc<MountValue>> {
        if is_cv {
            self.cv_map.get(path).cloned()
        } else {
            self.ufs_map.get(path).cloned()
        }
    }

    pub fn len(&self) -> usize {
        self.cv_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// RAII guard that clears the `refreshing` flag on drop. This guarantees the
/// flag is released on *every* exit path of the background task — normal
/// completion, early return, and panic-unwind alike.
struct RefreshingGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for RefreshingGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

pub struct MountCache {
    mounts: RwLock<InnerMap>,
    update_interval: u64,
    last_update: AtomicCounter,
    /// Mount table version held by this cache, valid when `version_known`.
    /// Lets the periodic refresh become a tens-of-bytes probe instead of a
    /// full table pull when the master reports no change.
    last_version: AtomicU64,
    version_known: AtomicBool,
    /// Single-flight lock: only one task performs full refresh when TTL expires.
    refresh_lock: Mutex<()>,
    /// True while a background refresh has been scheduled but not yet finished.
    /// Used to avoid spawning more than one concurrent background refresh task.
    refreshing: AtomicBool,
}

impl MountCache {
    pub fn new(update_interval: u64) -> Self {
        Self {
            mounts: RwLock::new(InnerMap::default()),
            update_interval,
            last_update: AtomicCounter::new(0),
            last_version: AtomicU64::new(0),
            version_known: AtomicBool::new(false),
            refresh_lock: Mutex::new(()),
            refreshing: AtomicBool::new(false),
        }
    }

    fn need_update(&self) -> bool {
        LocalTime::mills() > self.update_interval + self.last_update.get()
    }

    /// Whether the cache has ever been successfully populated. `last_update` is
    /// 0 until the first successful refresh sets it to a real wall-clock millis.
    fn is_initialized(&self) -> bool {
        self.last_update.get() != 0
    }

    /// Pulls the mount table from the master, negotiating by version, and
    /// applies the response to the cache. Guarded by `refresh_lock` so only one
    /// refresh runs at a time.
    async fn do_refresh(&self, fs: &UnifiedFileSystem) -> FsResult<()> {
        let known = if self.version_known.load(Ordering::Acquire) {
            Some(self.last_version.load(Ordering::Acquire))
        } else {
            None
        };
        let (mounts, version) = fs.get_mount_table_versioned(known).await?;
        self.apply_refresh(known, mounts, version)
    }

    /// Applies a refresh response to the cache:
    ///
    /// - Master reports "no change" (same version, empty table) → the cache is
    ///   kept as-is (including lazily-initialized UFS handles) and only the
    ///   TTL is renewed.
    /// - Anything else (first pull, change, legacy master without a version)
    ///   → full replace, and the reported version is recorded. A legacy
    ///   master's response carries no version, so every refresh stays a full
    ///   pull — identical to the pre-negotiation behavior.
    fn apply_refresh(
        &self,
        known: Option<u64>,
        mounts: Vec<MountInfo>,
        version: Option<u64>,
    ) -> FsResult<()> {
        let mut state = self.mounts.write().unwrap();

        if version.is_some() && version == known && mounts.is_empty() {
            debug!(
                "mount table unchanged (version {:?}), skip rebuild",
                version
            );
        } else {
            state.clear();
            for item in mounts {
                state.insert(item)?;
            }
            match version {
                Some(v) => {
                    self.last_version.store(v, Ordering::Release);
                    self.version_known.store(true, Ordering::Release);
                }
                None => self.version_known.store(false, Ordering::Release),
            }
        }

        debug!("update mounts {:?}", state.len());
        self.last_update.set(LocalTime::mills());
        Ok(())
    }

    /// Synchronous refresh: blocks until the mount table is up to date.
    /// Used when the caller must observe the latest state immediately
    /// (e.g. right after a `mount` call). `force` bypasses the TTL check.
    pub async fn check_update(&self, fs: &UnifiedFileSystem, force: bool) -> FsResult<()> {
        if !self.need_update() && !force {
            return Ok(());
        }

        let _guard = self.refresh_lock.lock().await;
        if !self.need_update() && !force {
            return Ok(());
        }

        self.do_refresh(fs).await
    }

    /// Non-blocking refresh trigger: if the cache is stale, spawn a background
    /// task to refresh it and return immediately, letting the caller keep using
    /// the current (possibly stale) snapshot.
    ///
    /// At most one background refresh runs at a time: the `refreshing` flag is
    /// claimed via compare_exchange, and a duplicate trigger is a no-op until the
    /// in-flight task clears it. The task takes ownership of cloned handles
    /// (`Arc<MountCache>` and `UnifiedFileSystem`), so it can outlive the current
    /// request.
    fn trigger_async_update(self: &Arc<Self>, fs: &UnifiedFileSystem) {
        if !self.need_update() {
            return;
        }

        // Ensure at most one background refresh is in flight. compare_exchange
        // returning Err means another task already set the flag and will publish
        // a fresh snapshot soon, so this call becomes a no-op.
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let cache = self.clone();
        let fs = fs.clone();
        let rt = fs.clone_runtime();
        rt.spawn(async move {
            // Take ownership of the claimed `refreshing` flag via an RAII guard
            // so it is cleared on every exit path — including a panic in
            // `do_refresh` (e.g. a poisoned RwLock) — not just on the normal
            // tail. Without this, a panic here would leak the flag and wedge all
            // future refreshes. Created before the lock so it covers the whole
            // task body.
            let _refreshing = RefreshingGuard {
                flag: &cache.refreshing,
            };
            // Hold the single-flight lock for the whole refresh so the semantics
            // match the synchronous path and double-refresh is impossible.
            let _guard = cache.refresh_lock.lock().await;
            if cache.need_update() {
                if let Err(e) = cache.do_refresh(&fs).await {
                    warn!("background mount cache refresh failed: {:?}", e);
                }
            }
        });
    }

    /// Finds mount point for a path using hierarchical lookup.
    /// Returns the most specific mount that contains the given path.
    ///
    /// Refresh policy:
    /// - On cold start (cache never populated) the first call refreshes
    ///   synchronously so a freshly-created client observes the correct mount
    ///   table instead of an empty one.
    /// - Once populated, a stale cache triggers a background refresh that is NOT
    ///   awaited: the lookup proceeds against the current snapshot so the caller
    ///   is never blocked on a master round-trip. The refreshed table becomes
    ///   visible to subsequent calls.
    pub async fn get_mount(
        self: &Arc<Self>,
        fs: &UnifiedFileSystem,
        path: &Path,
    ) -> FsResult<Option<Arc<MountValue>>> {
        if self.is_initialized() {
            self.trigger_async_update(fs);
        } else {
            // First access: block once to populate the cache. check_update is
            // single-flight, so concurrent first callers share one refresh.
            self.check_update(fs, false).await?;
        }

        let state = self.mounts.read().unwrap();
        if state.is_empty() {
            return Ok(None);
        }

        for mount_path in path.get_possible_mounts() {
            if let Some(mount) = state.get(path.is_cv(), &mount_path) {
                return Ok(Some(mount));
            }
        }

        Ok(None)
    }

    pub fn remove(&self, path: &Path) {
        let mut state = self.mounts.write().unwrap();
        state.remove(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{AccessMode, MountInfo, Provider, TtlAction, TtlType, WriteType};
    use std::collections::HashMap;

    fn opendal_oss_mount() -> MountInfo {
        MountInfo {
            cv_path: "/oss-mount/data".to_string(),
            ufs_path: "oss://example-bucket/data".to_string(),
            mount_id: 1,
            properties: HashMap::new(),
            ttl_ms: 7 * 24 * 60 * 60 * 1000,
            ttl_action: TtlAction::Delete,
            read_verify_ufs: false,
            storage_type: None,
            block_size: None,
            replicas: None,
            write_type: WriteType::CacheMode,
            provider: Some(Provider::Opendal),
            auto_cache: true,
            access_mode: AccessMode::ReadOnly,
            write_cache: false,
            ttl_type: TtlType::Mtime,
        }
    }

    fn oss_mount(cv_path: &str, mount_id: u32) -> MountInfo {
        let mut info = opendal_oss_mount();
        info.cv_path = cv_path.to_string();
        info.ufs_path = format!("oss://example-bucket{}", cv_path);
        info.mount_id = mount_id;
        info
    }

    fn cached_mount(cache: &MountCache, cv_path: &str) -> Option<Arc<MountValue>> {
        cache.mounts.read().unwrap().get(true, cv_path)
    }

    #[test]
    fn cache_insert_is_metadata_only_for_unsupported_provider() {
        let mut mounts = InnerMap::default();

        mounts
            .insert(opendal_oss_mount())
            .expect("mount cache refresh should not initialize UFS providers");

        assert!(mounts.get(true, "/flink/checkpoints").is_none());
        let mount = mounts
            .get(true, "/oss-mount/data")
            .expect("mounted path should be cached");
        assert!(mount.ufs.get().is_none());
    }

    #[test]
    fn unsupported_provider_error_is_lazy_until_mount_use() {
        let mut mounts = InnerMap::default();
        mounts
            .insert(opendal_oss_mount())
            .expect("mount cache refresh should not initialize UFS providers");

        let mount = mounts
            .get(true, "/oss-mount/data")
            .expect("mounted path should be cached");

        assert!(mount.ufs().is_err());
    }

    #[test]
    fn first_pull_records_version_for_negotiation() {
        let cache = MountCache::new(60_000);
        assert!(!cache.version_known.load(Ordering::Acquire));

        cache
            .apply_refresh(None, vec![oss_mount("/a", 1)], Some(5))
            .unwrap();

        assert!(cache.version_known.load(Ordering::Acquire));
        assert_eq!(cache.last_version.load(Ordering::Acquire), 5);
        assert!(cached_mount(&cache, "/a").is_some());
    }

    #[test]
    fn negotiated_matching_version_skips_rebuild_without_jitter() {
        let cache = MountCache::new(60_000);
        cache
            .apply_refresh(None, vec![oss_mount("/a", 1)], Some(3))
            .unwrap();
        let before = cached_mount(&cache, "/a").unwrap();

        // Same version + empty payload = "no change": the cached MountValue
        // (and any lazily-initialized UFS handle) must survive untouched.
        cache.apply_refresh(Some(3), vec![], Some(3)).unwrap();

        let after = cached_mount(&cache, "/a").unwrap();
        assert!(Arc::ptr_eq(&before, &after));
        assert_eq!(cache.last_version.load(Ordering::Acquire), 3);
    }

    #[test]
    fn bumped_version_with_empty_payload_rebuilds() {
        let cache = MountCache::new(60_000);
        cache
            .apply_refresh(None, vec![oss_mount("/a", 1)], Some(3))
            .unwrap();

        // A version bump with an empty table is a real change (last mount
        // removed) — the stale cache must be cleared.
        cache.apply_refresh(Some(3), vec![], Some(4)).unwrap();

        assert!(cached_mount(&cache, "/a").is_none());
        assert_eq!(cache.last_version.load(Ordering::Acquire), 4);
    }

    #[test]
    fn same_version_with_payload_still_rebuilds() {
        let cache = MountCache::new(60_000);
        cache
            .apply_refresh(None, vec![oss_mount("/a", 1)], Some(3))
            .unwrap();

        // Defensive: a payload must never be dropped, even on a version match.
        cache
            .apply_refresh(Some(3), vec![oss_mount("/b", 2)], Some(3))
            .unwrap();

        assert!(cached_mount(&cache, "/a").is_none());
        assert!(cached_mount(&cache, "/b").is_some());
    }

    #[test]
    fn version_regression_forces_full_rebuild() {
        let cache = MountCache::new(60_000);
        cache
            .apply_refresh(None, vec![oss_mount("/a", 1)], Some(42))
            .unwrap();

        // Restart / HA failover can yield a lower version; a mismatch must
        // rebuild instead of wrongly reporting "no change".
        cache
            .apply_refresh(Some(42), vec![oss_mount("/b", 2)], Some(3))
            .unwrap();

        assert!(cached_mount(&cache, "/a").is_none());
        assert!(cached_mount(&cache, "/b").is_some());
        assert_eq!(cache.last_version.load(Ordering::Acquire), 3);
    }

    #[test]
    fn legacy_master_response_keeps_full_pull_mode() {
        let cache = MountCache::new(60_000);

        // A master without version negotiation returns the full table and no
        // version — the cache must stay version-unknown (full pull per refresh).
        cache
            .apply_refresh(None, vec![oss_mount("/a", 1)], None)
            .unwrap();

        assert!(!cache.version_known.load(Ordering::Acquire));
        assert!(cached_mount(&cache, "/a").is_some());

        cache
            .apply_refresh(None, vec![oss_mount("/a", 1), oss_mount("/b", 2)], None)
            .unwrap();

        assert!(!cache.version_known.load(Ordering::Acquire));
        assert_eq!(cache.mounts.read().unwrap().len(), 2);
    }
}
