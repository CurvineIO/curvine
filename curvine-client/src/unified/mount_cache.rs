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

use crate::unified::pdpc::RouteIndexSnapshot;
use crate::unified::{UfsFileSystem, UnifiedFileSystem};
use arc_swap::ArcSwap;
use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::FsResult;
use log::debug;
use orpc::common::{FastHashMap, LocalTime};
use orpc::sync::AtomicCounter;
use orpc::CommonResult;
use std::sync::{Arc, RwLock};

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

    pub fn len(&self) -> usize {
        self.cv_map.len()
    }
}

pub struct MountCache {
    mounts: RwLock<InnerMap>,
    route_index: ArcSwap<RouteIndexSnapshot<MountValue>>,
    update_interval: u64,
    last_update: AtomicCounter,
}

impl MountCache {
    pub fn new(update_interval: u64) -> Self {
        Self {
            mounts: RwLock::new(InnerMap::default()),
            route_index: ArcSwap::from_pointee(RouteIndexSnapshot::default()),
            update_interval,
            last_update: AtomicCounter::new(0),
        }
    }

    fn need_update(&self) -> bool {
        LocalTime::mills() > self.update_interval + self.last_update.get()
    }

    pub async fn check_update(&self, fs: &UnifiedFileSystem, force: bool) -> FsResult<()> {
        if self.need_update() || force {
            let mounts = fs.get_mount_table().await?;

            let mut state = self.mounts.write().unwrap();
            state.clear();

            for item in mounts {
                state.insert(item)?;
            }

            let snapshot = Arc::new(RouteIndexSnapshot::from_maps(&state.cv_map, &state.ufs_map));
            self.route_index.store(snapshot);

            debug!("update mounts {:?}", state.len());
            self.last_update.set(LocalTime::mills());
        }

        Ok(())
    }

    /// Finds mount point for a path using hierarchical lookup.
    /// Returns the most specific mount that contains the given path.
    pub async fn get_mount(
        &self,
        fs: &UnifiedFileSystem,
        path: &Path,
    ) -> FsResult<Option<Arc<MountValue>>> {
        self.check_update(fs, false).await?;

        let snapshot = self.route_index.load();
        if snapshot.is_empty() {
            return Ok(None);
        }

        Ok(snapshot.lookup(path))
    }

    pub fn remove(&self, path: &Path) {
        let mut state = self.mounts.write().unwrap();
        state.remove(path);
        let snapshot = Arc::new(RouteIndexSnapshot::from_maps(&state.cv_map, &state.ufs_map));
        self.route_index.store(snapshot);
    }
}
