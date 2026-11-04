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

use crate::fs::dcache::{CachedStatus, DirEntry, Lifecycle, SharedStr};
use crate::raw::fuse_abi::fuse_attr;
use crate::{err_fuse, FuseResult, FuseUtils, FUSE_PATH_SEPARATOR, FUSE_ROOT_ID};
use curvine_config::FuseConf;
use curvine_core_error::try_option_mut;
use curvine_model::{
    FileStatus, LocatedBlock, SetAttrOpts, SetAttrOptsBuilder, INTERNAL_CTIME_XATTR,
};
use curvine_runtime::common::LocalTime;
use serde::{Deserialize, Deserializer, Serialize};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};

/// Deserialize a field that used to be `Option<i64>`.
///
/// The compact layout stores "absent" as 0 (master ids are always positive), so
/// state files written before the change still load whether the field is
/// `null`, missing, or already a plain number.
fn deserialize_master_id<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<i64>::deserialize(deserializer)?.unwrap_or(0))
}

/// Kernel lookup count. Atomic so a dcache-hit LOOKUP can bump it under the read lock.
/// Relaxed suffices: read-lock readers (e.g. `DirTree::evict_candidates`) only pre-filter,
/// and the eviction decision is taken under the write lock, where `DirTree::evict` rechecks it.
#[derive(Default, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct LookupCount(AtomicU64);

impl LookupCount {
    fn new(v: u64) -> Self {
        Self(AtomicU64::new(v))
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    fn add(&self, v: u64) -> u64 {
        let prev = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(v))
            })
            .unwrap_or_else(|n| n);
        prev.saturating_add(v)
    }

    fn sub(&mut self, v: u64) -> u64 {
        let n = self.0.get_mut();
        *n = n.saturating_sub(v);
        *n
    }
}

impl Clone for LookupCount {
    fn clone(&self) -> Self {
        Self::new(self.get())
    }
}

#[derive(Default, Deserialize, Serialize, Clone, Debug)]
pub struct Inode {
    pub ino: u64,
    pub parent: u64,
    /// Shared with the parent directory's children map key (one heap copy).
    pub name: SharedStr,

    pub status: CachedStatus,
    pub locs: Option<Box<Vec<LocatedBlock>>>,

    pub lifecycle: Lifecycle,

    n_lookup: LookupCount,
    pub ref_ctr: u64,
    pub last_access: u64,

    pub dir: Option<Box<DirEntry>>,

    pub mark_delete: bool,

    /// Curvine-namespace path recorded at unlink time for a deferred delete.
    ///
    /// When unlink defers the backend delete to last-close (release), the
    /// dcache ancestors may be evicted by then, so the plain-name path cannot
    /// be reconstructed. Recording the exact path taken at unlink (which is
    /// protected by `lock_path`) gives release a stable, correct deletion
    /// target even after renames or evictions (#1395).
    ///
    /// `None` (the common case) for every inode that is not pending a
    /// deferred delete; `Box<str>` drops the `String` capacity field.
    #[serde(default)]
    pub deferred_delete_path: Option<Box<str>>,

    /// Master file id captured at unlink time alongside
    /// `deferred_delete_path`; 0 when absent.
    ///
    /// `new_handle` overwrites `status.id` with the FUSE ino, so the master
    /// id must be captured explicitly. Release compares it against a fresh
    /// `stat` of the recorded path before falling back to a delete: a path
    /// can be reused after an ancestor rename, and deleting by path alone
    /// could remove an unrelated file (#1395).
    #[serde(default, deserialize_with = "deserialize_master_id")]
    pub deferred_delete_master_id: i64,

    /// Master-side file id captured when this inode first entered the dcache,
    /// before `status.id` is overwritten with the local FUSE ino; 0 when
    /// unknown.
    ///
    /// Persisted with the dcache so the release-time deferred-delete identity
    /// check can compare a fresh `stat` of the recorded path against the
    /// originally unlinked file even after a FUSE restart (#1395).
    #[serde(default, deserialize_with = "deserialize_master_id")]
    pub master_id: i64,
}

impl Inode {
    pub fn new_root() -> Self {
        // Match master `AclFeature::DEFAULT_MODE` (0o777). A mode-0 placeholder would
        // fail-closed on every non-root path traverse that walks the mount root.
        let root_st = CachedStatus {
            is_dir: true,
            nlink: 2,
            mode: 0o777,
            ..Default::default()
        };
        let dir = Some(Box::new(DirEntry::new()));
        Inode {
            ino: FUSE_ROOT_ID,
            parent: 0,
            name: SharedStr::from(FUSE_PATH_SEPARATOR),
            status: root_st,
            locs: None,
            lifecycle: Lifecycle::Invalid,
            n_lookup: LookupCount::new(0),
            ref_ctr: 0,
            last_access: LocalTime::mills(),
            dir,
            mark_delete: false,
            deferred_delete_path: None,
            deferred_delete_master_id: 0,
            master_id: 0,
        }
    }

    // `n_lookup` is the initial kernel lookup count: 1 for real LOOKUP /
    // READDIRPLUS (kernel caches the child and will FORGET it), 0 for plain
    // READDIR (no kernel ref). `ref_ctr` is always 1 for the local dcache dentry.
    pub fn with_status(
        ino: u64,
        parent: u64,
        name: SharedStr,
        status: FileStatus,
        n_lookup: u64,
    ) -> Self {
        let dir = if status.is_dir {
            Some(Box::new(DirEntry::new()))
        } else {
            None
        };

        // Capture the master-side id before it is replaced by the FUSE ino in
        // the cached copy.
        let master_id = status.id;
        Inode {
            ino,
            parent,
            name,
            status: CachedStatus::from_status(status),
            locs: None,
            lifecycle: Lifecycle::Cached,
            n_lookup: LookupCount::new(n_lookup),
            ref_ctr: 1,
            last_access: LocalTime::mills(),
            dir,
            mark_delete: false,
            deferred_delete_path: None,
            deferred_delete_master_id: 0,
            master_id: master_id.max(0),
        }
    }

    /// Capture the master-side id before `status.id` is overwritten with the
    /// FUSE ino. Called from `update_status` when a backend status is applied
    /// (#1395).
    fn record_master_id(&mut self, backend_id: i64) {
        if self.master_id <= 0 && backend_id > 0 {
            self.master_id = backend_id;
        }
    }

    pub fn clone_status(&self) -> FileStatus {
        self.status.to_file_status(self.ino, &self.name)
    }

    pub fn update_dirty_len(&mut self, len: i64) -> FuseResult<()> {
        if self.is_dirty() && !self.is_dir {
            self.len = len;
            self.mtime = LocalTime::mills() as i64;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.mark_delete && self.nlink <= 1
    }

    pub fn update_status(&mut self, status: FileStatus) {
        if self.is_dirty() {
            return;
        }

        if status.is_dir {
            let needs_fresh_dir = self.dir.is_none() || !self.status.is_dir;
            if needs_fresh_dir {
                let _ = self.dir.replace(Box::new(DirEntry::new()));
            }
        } else {
            let _ = self.dir.take();
        }

        // The incoming status still carries the master-side id (callers pass
        // backend statuses; the local FUSE ino is re-stamped in the cached
        // copy). Capture it once for the release-time deferred-delete identity
        // check (#1395).
        self.record_master_id(status.id);
        self.status = CachedStatus::from_status(status);

        self.lifecycle = Lifecycle::Cached;
        self.last_access = LocalTime::mills();
    }

    pub fn invalid_cache(&mut self) {
        if matches!(self.lifecycle, Lifecycle::Cached) {
            self.lifecycle = Lifecycle::Invalid;
        }
    }

    pub fn is_root(&self) -> bool {
        self.ino == FUSE_ROOT_ID
    }

    pub fn n_lookup(&self) -> u64 {
        self.n_lookup.get()
    }

    pub fn add_lookup(&self, v: u64) -> u64 {
        self.n_lookup.add(v)
    }

    pub fn sub_lookup(&mut self, v: u64) -> u64 {
        self.n_lookup.sub(v)
    }

    pub fn add_ref(&mut self, v: u64) -> u64 {
        self.ref_ctr = self.ref_ctr.saturating_add(v);
        self.ref_ctr
    }

    pub fn sub_ref(&mut self, v: u64) -> u64 {
        self.ref_ctr = self.ref_ctr.saturating_sub(v);
        self.ref_ctr
    }

    pub fn should_unref(&self) -> bool {
        self.n_lookup() == 0 && self.ref_ctr == 0 && !self.is_root()
    }

    pub fn is_unlinked(&self) -> bool {
        self.ref_ctr == 0 && !self.is_root()
    }

    pub fn sub_link(&mut self, v: u32) {
        self.nlink = self.nlink.saturating_sub(v);
    }

    pub fn add_link(&mut self, v: u32) {
        self.nlink = self.nlink.saturating_add(v);
    }

    pub fn ensure_dir_empty(&self) -> FuseResult<()> {
        if !self.is_dir {
            return Ok(());
        }
        let Some(dir) = self.dir.as_ref() else {
            return err_fuse!(libc::EIO, "directory inode {} missing DirEntry", self.ino);
        };
        if !dir.children.is_empty() {
            return err_fuse!(
                libc::ENOTEMPTY,
                "directory inode {} still has cached children",
                self.ino
            );
        }
        Ok(())
    }

    pub fn can_evict(&self, ttl: u64) -> bool {
        // The kernel may continue issuing requests by nodeid until it balances
        // every LOOKUP/READDIRPLUS reference with FORGET.
        !self.is_root()
            // A failed deferred delete must remain observable until cleanup
            // succeeds or reports that the backend entry is already absent.
            && !self.mark_delete
            && self.n_lookup() == 0
            && self.last_access + ttl < LocalTime::mills()
            && self.dir.as_ref().is_none_or(|d| d.children.is_empty())
    }

    pub fn remove_child(&mut self, name: &str) {
        if let Some(dir) = &mut self.dir {
            dir.remove_child(name);
        }
    }

    pub fn is_dirty(&self) -> bool {
        matches!(self.lifecycle, Lifecycle::Dirty)
    }

    pub fn cache_valid(&self, ttl: u64) -> bool {
        match self.lifecycle {
            Lifecycle::Cached => self.last_access + ttl >= LocalTime::mills(),
            Lifecycle::Dirty => true,
            Lifecycle::Invalid => false,
        }
    }

    pub fn dir_scan_valid(&self, ttl: u64) -> bool {
        let Some(dir) = &self.dir else { return false };
        dir.scan_complete && self.last_access + ttl >= LocalTime::mills()
    }

    pub fn mark_dirty(&mut self) -> FuseResult<()> {
        if self.is_dir {
            let dir = try_option_mut!(self.dir);
            dir.scan_complete = true;
        } else {
            self.lifecycle = Lifecycle::Dirty;
        }
        Ok(())
    }

    pub fn dirty_set_attr(&mut self, opts: SetAttrOpts) -> FuseResult<FileStatus> {
        if !self.is_dirty() {
            return err_fuse!(libc::EIO, "inode {} is not dirty", self.ino);
        }

        if let Some(mtime) = opts.mtime {
            self.mtime = mtime;
        }

        if let Some(atime) = opts.atime {
            self.atime = atime;
        }

        if let Some(mode) = opts.mode {
            self.mode = mode;
        }

        if let Some(owner) = opts.owner {
            self.owner = SharedStr::from(owner);
        }

        if let Some(group) = opts.group {
            self.group = SharedStr::from(group);
        }

        self.status
            .apply_xattr_changes(opts.add_x_attr, &opts.remove_x_attr);

        Ok(self.clone_status())
    }

    pub fn to_attr(&self, conf: &FuseConf) -> FuseResult<fuse_attr> {
        FuseUtils::cached_status_to_attr(conf, &self.status, self.ino, &self.name)
    }

    pub fn to_set_opts(&self) -> SetAttrOpts {
        let mut builder = SetAttrOptsBuilder::new()
            .mode(self.mode)
            .owner(self.owner.to_string())
            .group(self.group.to_string())
            .mtime(self.mtime)
            .atime(self.atime);

        if self.status.ctime != 0 {
            builder = builder.add_x_attr(
                INTERNAL_CTIME_XATTR.to_owned(),
                self.status.ctime.to_le_bytes().to_vec(),
            );
        }

        if let Some(x_attr) = self.status.x_attr.as_deref() {
            for (k, v) in x_attr.iter() {
                builder = builder.add_x_attr(k.to_owned(), v.to_owned());
            }
        }

        builder.build()
    }

    /// Migrate a legacy state-file entry whose ctime lived in `x_attr`.
    pub fn normalize_after_restore(&mut self) {
        self.status.normalize_after_restore();
    }
}

impl Deref for Inode {
    type Target = CachedStatus;

    fn deref(&self) -> &Self::Target {
        &self.status
    }
}

impl DerefMut for Inode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.status
    }
}

#[cfg(test)]
mod tests {
    use super::Inode;
    use crate::fs::dcache::SharedStr;
    use curvine_model::FileStatus;

    #[test]
    fn n_lookup_serializes_as_plain_number() {
        let inode = Inode::with_status(7, 1, SharedStr::from("f"), FileStatus::default(), 3);
        inode.add_lookup(2);

        let json = serde_json::to_value(&inode).unwrap();
        assert_eq!(json["n_lookup"], serde_json::json!(5));

        let restored: Inode = serde_json::from_value(json).unwrap();
        assert_eq!(restored.n_lookup(), 5);
        assert_eq!(restored.clone().n_lookup(), 5);
    }

    #[test]
    fn legacy_inode_state_loads_into_compact_fields() {
        // State written before the compact layout: flat deferred-delete
        // fields and `master_id` as an `Option` (`null` when unknown).
        let legacy = serde_json::json!({
            "ino": 7,
            "parent": 1,
            "name": "f",
            "status": {},
            "locs": null,
            "lifecycle": "Cached",
            "n_lookup": 1,
            "ref_ctr": 1,
            "last_access": 0,
            "dir": null,
            "mark_delete": true,
            "deferred_delete_path": "/a/f",
            "deferred_delete_master_id": 603,
            "master_id": null
        });

        let inode: Inode = serde_json::from_value(legacy).unwrap();
        assert_eq!(inode.deferred_delete_path.as_deref(), Some("/a/f"));
        assert_eq!(inode.deferred_delete_master_id, 603);
        assert_eq!(inode.master_id, 0);
        assert_eq!(inode.name, "f");
    }
}
