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

use crate::master::SyncFsDir;
use curvine_config::{UfsConf, UfsConfBuilder};
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{MountInfo, MountOptions};
use log::{info, warn};
use rand::Rng;
use std::collections::HashMap;
use std::convert::Into;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct MountTableInner {
    ufs2mountid: HashMap<String, u32>,
    mountid2entry: HashMap<u32, MountInfo>,
    mountpath2id: HashMap<String, u32>,
}

/// Immutable point-in-time view of the mount table served to `GetMountTable`.
///
/// Rebuilt (O(N)) only inside the write choke points; readers share the `Arc`
/// clones so a pull costs one atomic increment instead of a full deep copy
/// under the table lock.
#[derive(Clone)]
pub struct TableSnapshot {
    pub version: u64,
    pub entries: Vec<Arc<MountInfo>>,
}

pub struct MountTable {
    inner: RwLock<MountTableInner>,
    snapshot: RwLock<Arc<TableSnapshot>>,
    fs_dir: SyncFsDir,
}

impl MountTable {
    pub fn new(fs_dir: SyncFsDir) -> Self {
        MountTable {
            inner: RwLock::new(MountTableInner {
                ufs2mountid: HashMap::new(),
                mountid2entry: HashMap::new(),
                mountpath2id: HashMap::new(),
            }),
            snapshot: RwLock::new(Arc::new(TableSnapshot {
                version: 0,
                entries: Vec::new(),
            })),
            fs_dir,
        }
    }

    fn read_inner(&self) -> FsResult<RwLockReadGuard<'_, MountTableInner>> {
        match self.inner.read() {
            Ok(inner) => Ok(inner),
            Err(e) => err_box!("mount table read lock poisoned: {}", e),
        }
    }

    fn write_inner(&self) -> FsResult<RwLockWriteGuard<'_, MountTableInner>> {
        match self.inner.write() {
            Ok(inner) => Ok(inner),
            Err(e) => err_box!("mount table write lock poisoned: {}", e),
        }
    }

    fn read_snapshot(&self) -> FsResult<Arc<TableSnapshot>> {
        match self.snapshot.read() {
            Ok(snap) => Ok(snap.clone()),
            Err(e) => err_box!("mount table snapshot read lock poisoned: {}", e),
        }
    }

    fn write_snapshot(&self) -> FsResult<RwLockWriteGuard<'_, Arc<TableSnapshot>>> {
        match self.snapshot.write() {
            Ok(snap) => Ok(snap),
            Err(e) => err_box!("mount table snapshot write lock poisoned: {}", e),
        }
    }

    /// Publishes a fresh snapshot of `inner` with version bumped by one.
    ///
    /// Caller must hold `write_inner()` on the same `inner`; the snapshot lock
    /// is a different lock so no re-entrancy occurs. Persisting the new version
    /// to the store is left to the caller (after the inner lock is released).
    fn publish_snapshot(&self, inner: &MountTableInner) -> FsResult<u64> {
        let mut snap = self.write_snapshot()?;
        let version = snap.version + 1;
        let entries = inner
            .mountid2entry
            .values()
            .map(|m| Arc::new(m.clone()))
            .collect();
        *snap = Arc::new(TableSnapshot { version, entries });
        Ok(version)
    }

    //for new master node
    pub fn restore(&self) -> FsResult<()> {
        let fs_dir = self.fs_dir.read();
        let mounts = fs_dir.get_mount_table()?;
        let version = fs_dir.get_mount_table_version()?;
        self.replace_with_mounts(mounts, version)
    }

    pub fn restore_best_effort(&self) {
        if let Err(e) = self.restore() {
            warn!("failed to restore mount table: {}", e);
        }
    }

    /// Replaces the in-memory table and snapshot with `mounts`. Pure
    /// in-memory: the caller holds the `fs_dir` read guard and `SyncFsDir`
    /// forbids re-entrancy.
    fn replace_with_mounts(&self, mounts: Vec<MountInfo>, version: u64) -> FsResult<()> {
        let mut restored = MountTableInner {
            ufs2mountid: HashMap::new(),
            mountid2entry: HashMap::new(),
            mountpath2id: HashMap::new(),
        };

        for mnt in mounts {
            if restored
                .ufs2mountid
                .insert(mnt.ufs_path.to_string(), mnt.mount_id)
                .is_some()
            {
                return err_box!("duplicate restored UFS mount path {}", mnt.ufs_path);
            }
            if restored
                .mountpath2id
                .insert(mnt.cv_path.to_string(), mnt.mount_id)
                .is_some()
            {
                return err_box!("duplicate restored CV mount path {}", mnt.cv_path);
            }
            if restored.mountid2entry.insert(mnt.mount_id, mnt).is_some() {
                return err_box!("duplicate restored mount id");
            }
        }

        let entries = restored
            .mountid2entry
            .values()
            .map(|m| Arc::new(m.clone()))
            .collect();
        let mut inner = self.write_inner()?;
        let mut snapshot = self.write_snapshot()?;
        *inner = restored;
        *snapshot = Arc::new(TableSnapshot { version, entries });
        Ok(())
    }

    // ufs maybe mounted already or has prefix overlap with existing mounts
    pub fn exists(&self, ufs_path: &str) -> FsResult<bool> {
        let inner = self.read_inner()?;

        // full match check
        if inner.ufs2mountid.contains_key(ufs_path) {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn check_conflict(&self, cv_path: &str, ufs_path: &str) -> FsResult<()> {
        let inner = self.read_inner()?;
        for info in inner.mountid2entry.values() {
            if Path::has_prefix(cv_path, &info.cv_path) {
                return err_box!("mount point {} is a prefix of {}", info.cv_path, cv_path);
            }

            if Path::has_prefix(&info.cv_path, cv_path) {
                return err_box!("mount point {} is a prefix of {}", cv_path, info.cv_path);
            }

            if Path::has_prefix(ufs_path, &info.ufs_path) {
                return err_box!("mount point {} is a prefix of {}", info.ufs_path, ufs_path);
            }
            if Path::has_prefix(&info.ufs_path, ufs_path) {
                return err_box!("mount point {} is a prefix of {}", ufs_path, info.ufs_path);
            }
        }

        Ok(())
    }

    // mountid maybe occupied
    pub fn has_mounted(&self, mount_id: u32) -> FsResult<bool> {
        let inner = self.read_inner()?;
        Ok(inner.mountid2entry.contains_key(&mount_id))
    }

    // mount_path maybe mounted by other ufs
    fn mount_point_inuse(&self, cv_path: &str) -> FsResult<bool> {
        let inner = self.read_inner()?;
        Ok(inner.mountpath2id.contains_key(cv_path))
    }

    /// In-memory add + snapshot publish. Returns the new version; persisting
    /// it is the caller's job — this method must not touch `fs_dir`, since
    /// every caller (journal replay, add/update) already holds or immediately
    /// takes the `fs_dir` write lock and `SyncFsDir` forbids re-entrancy.
    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<u64> {
        info!("add mount: {:?}", info);

        let mut inner = self.write_inner()?;
        inner
            .ufs2mountid
            .insert(info.ufs_path.to_string(), info.mount_id);
        inner
            .mountpath2id
            .insert(info.cv_path.to_string(), info.mount_id);
        inner.mountid2entry.insert(info.mount_id, info);
        self.publish_snapshot(&inner)
    }

    pub fn add_mount(
        &self,
        mount_id: u32,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        if self.exists(ufs_path)? {
            return err_box!("{} already exists in mount table", ufs_path);
        }

        if self.mount_point_inuse(cv_path)? {
            return err_box!("{} already exists in mount table", cv_path);
        }

        if self.has_mounted(mount_id)? {
            return err_box!("mount id {} already exists in mount table", mount_id);
        }

        self.check_conflict(cv_path, ufs_path)?;

        let info = mnt_opt.clone().to_info(mount_id, cv_path, ufs_path);
        let version = self.unprotected_add_mount(info.clone())?;

        fs_dir.store_mount(info, true)?;
        fs_dir.save_mount_table_version(version)
    }

    pub fn update_mount(&self, info: MountInfo) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let old = self.get_mount_info_by_id(info.mount_id)?;
        if old.cv_path != info.cv_path || old.ufs_path != info.ufs_path {
            return err_box!("cannot change mount path");
        }

        let version = self.unprotected_add_mount(info.clone())?;

        fs_dir.store_mount(info, true)?;
        fs_dir.save_mount_table_version(version)
    }

    pub fn assign_mount_id(&self) -> FsResult<u32> {
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let new_id = rng.gen::<u32>();
            if !self.has_mounted(new_id)? {
                return Ok(new_id);
            }
        }

        err_box!("failed assign mount id")
    }

    pub fn umount(&self, mount_path: &str) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let mount_id = {
            let inner = self.read_inner()?;
            match inner.mountpath2id.get(mount_path) {
                Some(&id) => id,
                None => return err_box!("failed found {} to umount", mount_path),
            }
        };

        let version = self.unprotected_umount_by_id(mount_id)?;

        fs_dir.unmount(mount_id)?;
        fs_dir.save_mount_table_version(version)
    }

    /// use ufs_uri to find ufs config
    pub fn get_ufs_conf(&self, ufs_path: &String) -> FsResult<UfsConf> {
        let inner = self.read_inner()?;

        let mount_id = match inner.ufs2mountid.get(ufs_path) {
            Some(&id) => id,
            None => return err_box!("failed found {}", ufs_path),
        };

        let properties = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => &entry.properties,
            None => return err_box!("failed found {} properties", mount_id),
        };

        let mut ufs_conf_builder = UfsConfBuilder::default();
        for (k, v) in properties {
            ufs_conf_builder.add_config(k, v);
        }
        let ufs_conf = ufs_conf_builder.build();
        Ok(ufs_conf)
    }

    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        let list = path.get_possible_mounts();
        let is_cv = path.is_cv();
        let inner = self.read_inner()?;

        for mnt in list {
            let option_id = if is_cv {
                inner.mountpath2id.get(&mnt)
            } else {
                inner.ufs2mountid.get(&mnt)
            };
            if let Some(id) = option_id {
                let Some(entry) = inner.mountid2entry.get(id) else {
                    return err_box!(
                        "mount table corrupt: path {} references missing mount id {}",
                        mnt,
                        id
                    );
                };
                return Ok(Some(entry.clone()));
            }
        }

        Ok(None)
    }

    pub fn get_mount_info_by_id(&self, mount_id: u32) -> FsResult<MountInfo> {
        let inner = self.read_inner()?;
        let entry = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => entry.clone(),
            None => return err_box!("failed found {} entry", mount_id),
        };
        Ok(entry)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let snap = self.read_snapshot()?;
        Ok(snap.entries.iter().map(|e| e.as_ref().clone()).collect())
    }

    /// O(1) point-in-time view of the table for `GetMountTable` serving.
    pub fn snapshot(&self) -> FsResult<Arc<TableSnapshot>> {
        self.read_snapshot()
    }

    /// In-memory remove + snapshot publish. Returns the new version; the
    /// caller persists it under its own `fs_dir` write lock (see the
    /// re-entrancy note on [`MountTable::unprotected_add_mount`]).
    pub fn unprotected_umount_by_id(&self, mount_id: u32) -> FsResult<u64> {
        let mut inner = self.write_inner()?;

        let info = match inner.mountid2entry.remove(&mount_id) {
            Some(entry) => entry,
            None => return err_box!("failed found {} entry", mount_id),
        };

        inner.ufs2mountid.remove(&info.ufs_path);
        inner.mountpath2id.remove(&info.cv_path);
        self.publish_snapshot(&inner)
    }
}
