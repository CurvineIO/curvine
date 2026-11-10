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

use crate::master::meta::FsDir;
use curvine_config::{UfsConf, UfsConfBuilder};
use curvine_core_error::{err_box, err_msg};
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{MountInfo, MountOptions};
use log::warn;
use rand::Rng;
use std::collections::HashMap;
use std::convert::Into;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

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
}

/// Online RPC mutation: emits a journal entry through `FsDir`.
const ONLINE: bool = true;
/// Journal replay mutation: persists the row without echoing a journal entry.
const REPLAY: bool = false;

impl MountTable {
    pub fn new() -> Self {
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
    /// is a different lock so no re-entrancy occurs.
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
    pub fn restore(&self, fs_dir: &FsDir) -> FsResult<()> {
        let mounts = fs_dir.get_mount_table()?;
        let version = fs_dir.get_mount_table_version()?;
        self.replace_with_mounts(mounts, version)
    }

    pub fn restore_best_effort(&self, fs_dir: &FsDir) {
        if let Err(e) = self.restore(fs_dir) {
            warn!("failed to restore mount table: {}", e);
        }
    }

    /// Replaces the in-memory table and snapshot with `mounts`.
    ///
    /// Release points are separate from the online mutate path: this runs
    /// under the namespace read guard (the caller's), reads rows and the
    /// persisted version from the store, then swaps the live table and the
    /// snapshot in one write-locked window. It is NOT the mount business
    /// write guard of `put_mount`/`remove_mount` — restore has no row or
    /// version writes of its own.
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

    /// Prefix-overlap check against every live entry. The caller holds the
    /// write guard, so this runs on `&mut Inner`.
    fn check_conflict_locked(
        inner: &MountTableInner,
        cv_path: &str,
        ufs_path: &str,
    ) -> FsResult<()> {
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

    /// Insert-or-replace one entry and persist the row + the same version
    /// while the business write guard is held.
    ///
    /// `online` distinguishes the RPC path (journal echo, full duplicate and
    /// prefix-conflict checks) from journal replay (record application with
    /// no conflict re-check and no journal echo), matching the pre-existing
    /// semantics of the old unprotected helper.
    fn put_mount(&self, fs_dir: &FsDir, info: MountInfo, online: bool) -> FsResult<()> {
        let mut inner = self.write_inner()?;
        if online {
            if inner.ufs2mountid.contains_key(&info.ufs_path) {
                return err_box!("{} already exists in mount table", info.ufs_path);
            }
            if inner.mountpath2id.contains_key(&info.cv_path) {
                return err_box!("{} already exists in mount table", info.cv_path);
            }
            if inner.mountid2entry.contains_key(&info.mount_id) {
                return err_box!("mount id {} already exists in mount table", info.mount_id);
            }
            Self::check_conflict_locked(&inner, &info.cv_path, &info.ufs_path)?;
        } else {
            // Replay applies a recorded row exactly like the old
            // unprotected helper: an unconditional overwrite of the id's
            // entry. If a mixed-version journal reuses an id with different
            // paths, stale ufs2mountid/mountpath2id keys can linger; this is
            // the pre-existing replay contract and is intentionally kept
            // (a rebuild via restore() clears it).
        }

        inner
            .ufs2mountid
            .insert(info.ufs_path.to_string(), info.mount_id);
        inner
            .mountpath2id
            .insert(info.cv_path.to_string(), info.mount_id);
        inner.mountid2entry.insert(info.mount_id, info.clone());
        let version = self.publish_snapshot(&inner)?;

        fs_dir.store_mount(info, online)?;
        fs_dir.save_mount_table_version(version)
    }

    /// Removes by mount path (online RPC): path lookup, table removal and
    /// persistence all run under the single write guard, so no concurrent
    /// mount mutation can slip between the check and the delete.
    fn remove_mount_by_path(&self, fs_dir: &FsDir, mount_path: &str) -> FsResult<()> {
        let mut inner = self.write_inner()?;
        let mount_id = match inner.mountpath2id.get(mount_path) {
            Some(&id) => id,
            None => return err_box!("failed found {} to umount", mount_path),
        };
        let info = match inner.mountid2entry.remove(&mount_id) {
            Some(entry) => entry,
            None => return err_box!("failed found {} entry", mount_id),
        };

        inner.ufs2mountid.remove(&info.ufs_path);
        inner.mountpath2id.remove(&info.cv_path);
        let version = self.publish_snapshot(&inner)?;

        fs_dir.unmount_with_log(info.mount_id, ONLINE)?;
        fs_dir.save_mount_table_version(version)
    }

    /// Removes by mount id (journal replay): the same single write guard;
    /// an unknown id is an idempotent no-op that still acks.
    fn remove_mount_by_id(&self, fs_dir: &FsDir, mount_id: u32) -> FsResult<()> {
        let mut inner = self.write_inner()?;
        let info = match inner.mountid2entry.remove(&mount_id) {
            Some(entry) => entry,
            None => return Ok(()),
        };

        inner.ufs2mountid.remove(&info.ufs_path);
        inner.mountpath2id.remove(&info.cv_path);
        let version = self.publish_snapshot(&inner)?;

        fs_dir.unmount_with_log(info.mount_id, REPLAY)?;
        fs_dir.save_mount_table_version(version)
    }

    pub fn add_mount(
        &self,
        fs_dir: &FsDir,
        mount_id: u32,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        let info = mnt_opt.clone().to_info(mount_id, cv_path, ufs_path);
        self.put_mount(fs_dir, info, ONLINE)
    }

    pub fn update_mount(&self, fs_dir: &FsDir, info: MountInfo) -> FsResult<()> {
        self.put_mount_replace(fs_dir, info)
    }

    /// Update variant: the entry must exist, paths stay unchanged, and the
    /// row is persisted with the online journal echo.
    fn put_mount_replace(&self, fs_dir: &FsDir, info: MountInfo) -> FsResult<()> {
        let mut inner = self.write_inner()?;
        let old = inner
            .mountid2entry
            .get(&info.mount_id)
            .ok_or_else(|| err_msg!("failed found {} entry", info.mount_id))?;
        if old.cv_path != info.cv_path || old.ufs_path != info.ufs_path {
            return err_box!("cannot change mount path");
        }

        inner
            .ufs2mountid
            .insert(info.ufs_path.to_string(), info.mount_id);
        inner
            .mountpath2id
            .insert(info.cv_path.to_string(), info.mount_id);
        inner.mountid2entry.insert(info.mount_id, info.clone());
        let version = self.publish_snapshot(&inner)?;

        fs_dir.store_mount(info, ONLINE)?;
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

    pub fn umount(&self, fs_dir: &FsDir, mount_path: &str) -> FsResult<()> {
        self.remove_mount_by_path(fs_dir, mount_path)
    }

    /// Replay entry point for a Mount journal entry: applies the recorded row
    /// through the same business guard without echoing a journal entry.
    pub fn replay_mount(&self, fs_dir: &FsDir, info: MountInfo) -> FsResult<()> {
        self.put_mount(fs_dir, info, REPLAY)
    }

    /// Replay entry point for an UnMount journal entry: idempotent when the
    /// table no longer knows the id.
    pub fn replay_unmount(&self, fs_dir: &FsDir, mount_id: u32) -> FsResult<()> {
        self.remove_mount_by_id(fs_dir, mount_id)
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

    /// Test-only probe distinguishing a free lock (`Ok`) from contention
    /// (`Err(TryLockError::WouldBlock)`) or poisoning
    /// (`Err(TryLockError::Poisoned)`). Lives on this type so the test child
    /// module can reach the private `inner` without a production accessor.
    #[cfg(test)]
    pub(crate) fn try_write_inner(
        &self,
    ) -> Result<(), TryLockError<RwLockWriteGuard<'_, MountTableInner>>> {
        self.inner.try_write().map(|_| ())
    }
}

#[cfg(test)]
#[path = "tests/mount_txn_test.rs"]
mod mount_txn_test;
