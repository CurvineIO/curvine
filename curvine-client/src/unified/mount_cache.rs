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

use crate::unified::{UfsFileSystem, UnifiedFileSystem};
use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::FsResult;
use log::{debug, warn};
use orpc::common::{FastHashMap, LocalTime};
use orpc::runtime::RpcRuntime;
use orpc::sync::AtomicCounter;
use orpc::CommonResult;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// Represents a single mount point with its filesystem handler.
/// Contains mount metadata, UFS handler, and path conversion utilities.
pub struct MountValue {
    pub info: MountInfo,
    pub ufs: UfsFileSystem,
    pub mount_id: String,
}

impl MountValue {
    pub fn new(info: MountInfo) -> FsResult<Self> {
        let ufs_path = Path::from_str(&info.ufs_path)?;
        let ufs = UfsFileSystem::new(&ufs_path, info.properties.clone(), info.provider)?;
        let mount_id = format!("{}", info.mount_id);

        Ok(Self {
            info,
            ufs,
            mount_id,
        })
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

    /// Performs the actual full refresh of the mount table from the master.
    /// Guarded by `refresh_lock` so only one refresh runs at a time.
    async fn do_refresh(&self, fs: &UnifiedFileSystem) -> FsResult<()> {
        let mounts = fs.get_mount_table().await?;
        let mut state = self.mounts.write().unwrap();

        state.clear();
        for item in mounts {
            state.insert(item)?;
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
