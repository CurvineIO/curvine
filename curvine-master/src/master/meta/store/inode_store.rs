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

use crate::master::fs::DeleteResult;
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::{
    ChildAddResult, Inode, InodeDir, InodeFile, InodePath, InodePtr, InodeView, ROOT_INODE_ID,
};
use crate::master::meta::store::{InodeWriteBatch, RocksInodeStore};
use crate::master::meta::{FileSystemStats, FsDir, LockMeta};
use crate::master::space::scan_lease::{
    MetadataRestoreGuard, MetadataScanControl, MetadataScanLease,
};
use crate::master::MetadataCodec;
use curvine_core_error::{err_box, try_err, try_option, CommonResult};
use curvine_model::{
    BlockLocation, CommitBlock, DirectoryAttributeDelta, DirectoryAttributes, FileLock, FileStatus,
    ListOptions, MountInfo,
};
use curvine_raft::proto::raft::AppliedIndex;
use curvine_rocksdb::{DBConf, DbReadSnapshot as DBReadSnapshot, RocksIterator, RocksUtils};
use curvine_runtime::common::{FileUtils, Utils};
use log::info;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, LinkedList, VecDeque};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Private helper structs for bulk-load snapshot restore (Issue #964)
// ---------------------------------------------------------------------------

/// Bulk-loaded metadata from RocksDB, used as input for in-memory tree assembly.
struct SnapshotData {
    /// parent_id -> list of (child_name, child_id) edges
    edges: HashMap<i64, Vec<(String, i64)>>,
    /// inode_id -> deserialized InodeView
    inodes: HashMap<i64, InodeView>,
    /// inode_id -> directory attributes; absent when the record does not exist.
    dir_attrs: HashMap<i64, DirectoryAttributes>,
}

/// Lightweight phase-timing / counter collector for create_tree restore.
struct RestoreTimer {
    phases: Vec<(&'static str, u64)>,
    edge_count: usize,
    inode_count: usize,
    orphaned_count: usize,
    repair_count: usize,
    file_count: i64,
    dir_count: i64,
    last_mark: std::time::Instant,
}

impl RestoreTimer {
    fn new() -> Self {
        Self {
            phases: Vec::new(),
            edge_count: 0,
            inode_count: 0,
            orphaned_count: 0,
            repair_count: 0,
            file_count: 0,
            dir_count: 0,
            last_mark: std::time::Instant::now(),
        }
    }

    fn mark(&mut self, name: &'static str) {
        let elapsed = self.last_mark.elapsed().as_millis() as u64;
        self.phases.push((name, elapsed));
        self.last_mark = std::time::Instant::now();
    }

    fn log_summary(&self) {
        let phase_str: Vec<String> = self
            .phases
            .iter()
            .map(|(name, ms)| format!("{}={}ms", name, ms))
            .collect();
        info!(
            "create_tree: edges={}, inodes={}, orphaned={}, repairs={}, files={}, dirs={}, phases=[{}]",
            self.edge_count,
            self.inode_count,
            self.orphaned_count,
            self.repair_count,
            self.file_count,
            self.dir_count,
            phase_str.join(", "),
        );
    }
}

/// One namespace entry removed by [`InodeStore::apply_purge`].
pub struct PurgeItem {
    /// Parent directory metadata with the batch's mtime/nlink updates applied.
    /// Directory edges live in their own column family, so children are absent.
    pub parent: InodeView,
    /// The removed dentry: a directory's metadata, or the id of the file inode
    /// released by a file or `FileEntry` dentry.
    pub target: PurgeTarget,
    pub name: String,
}

pub enum PurgeTarget {
    Dir(InodeView),
    File(i64),
}

// Currently, only RockSDB is supported.
// Note: InodeStore is intentionally NOT Clone.
// Cloning InodeStore increases Arc<RocksInodeStore> refcount, which prevents
// the RocksDB lock from being released during Raft snapshot restore.
// If you need to share InodeStore, use Arc<InodeStore> or access it via FsDir.
pub struct InodeStore {
    pub(crate) store: Arc<RocksInodeStore>,
    fs_stats: Arc<FileSystemStats>,
    ttl_bucket_list: Arc<TtlBucketList>,
    scan_control: Arc<MetadataScanControl>,
    /// Replaceable handle for the journal applied cursor row. Cloning this
    /// handle never pins a RocksInodeStore: restore clears it before swapping
    /// the database instance and republishes the new one afterwards, so the
    /// journal applier can persist the cursor without taking the namespace
    /// guard and without blocking a restore.
    cursor_store: Arc<RwLock<Option<Arc<RocksInodeStore>>>>,
    /// Availability published by restore; see `store_available`.
    available: std::sync::atomic::AtomicBool,
}

const DIRECTORY_ATTRIBUTE_MIGRATION_BATCH_SIZE: usize = 4096;

/// Cloneable handle to the journal applied cursor row. It follows store
/// replacement during restore without pinning any RocksInodeStore.
#[derive(Clone)]
pub struct JournalCursorStore {
    inner: Arc<RwLock<Option<Arc<RocksInodeStore>>>>,
}

impl JournalCursorStore {
    pub fn put(&self, key: &str, value: &[u8]) -> CommonResult<()> {
        crate::fault_point! {
            sync,
            name: "master.journal.cursor.before_write",
            description: "Before persisting the journal applied cursor",
            context: { "key" => key },
            return_error: |fault| Err(fault.message.into()),
        }
        self.with_store(|store| store.put_common_kv(key, value))
    }

    /// Borrows the current store for the whole synchronous operation, so
    /// restore must wait before detaching or replacing it.
    fn with_store<T>(
        &self,
        op: impl FnOnce(&RocksInodeStore) -> CommonResult<T>,
    ) -> CommonResult<T> {
        let store = self.inner.read();
        match store.as_ref() {
            Some(store) => op(store),
            None => err_box!("cursor store unavailable during metadata restore"),
        }
    }

    pub(crate) fn get_applied_req(&self, req_id: i64) -> CommonResult<Option<AppliedIndex>> {
        self.with_store(|store| store.get_applied_req(req_id))
    }

    pub(crate) fn put_applied_req(&self, req_id: i64, index: u64, term: u64) -> CommonResult<()> {
        self.with_store(|store| store.put_applied_req(req_id, index, term))
    }

    pub(crate) fn prune_applied_req_below(&self, floor: u64) -> CommonResult<()> {
        self.with_store(|store| store.prune_applied_req_below(floor))
    }
}

impl InodeStore {
    fn set_available(&self, available: bool) {
        self.available
            .store(available, std::sync::atomic::Ordering::Release);
    }

    /// One-load availability check. A stale `true` is impossible: restore
    /// clears the flag before detaching the store and republishes only after
    /// the cursor store is valid again.
    fn store_available(&self) -> bool {
        self.available.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Merges a caller-computed delta. The delta's base decision belongs to
    /// the live node (fs_dir side); the store never derives it from a
    /// projected view.
    fn merge_directory_attributes(
        batch: &mut InodeWriteBatch<'_>,
        parent_id: i64,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        batch.merge_directory_attributes(parent_id, delta)
    }

    pub fn new(store: RocksInodeStore, ttl_bucket_list: Arc<TtlBucketList>) -> CommonResult<Self> {
        let store = Arc::new(store);
        Ok(InodeStore {
            cursor_store: Arc::new(RwLock::new(Some(store.clone()))),
            available: std::sync::atomic::AtomicBool::new(true),
            store,
            fs_stats: Arc::new(FileSystemStats::new()?),
            ttl_bucket_list,
            scan_control: Arc::new(MetadataScanControl::new()),
        })
    }

    /// Snapshot handle for the journal cursor row. It stays valid across a
    /// restore (which republishes the new store into the same handle) and
    /// never pins the current RocksInodeStore itself.
    pub fn cursor_store(&self) -> JournalCursorStore {
        JournalCursorStore {
            inner: self.cursor_store.clone(),
        }
    }

    /// Adds directory-attribute records absent from stores created before the
    /// separate directory-attributes column family existed. Existing records
    /// are preserved because they may already contain merged updates.
    pub fn migrate_directory_attributes(&self) -> CommonResult<()> {
        if self.store.directory_attributes_migrated()? {
            return Ok(());
        }

        let existing: HashSet<i64> = self.store.directory_attribute_ids()?.into_iter().collect();
        let mut batch = self.store.new_batch();
        let mut pending = 0;

        for entry in self.store.bulk_scan_inodes()? {
            let (key, bytes) = entry?;
            let id = RocksUtils::i64_from_bytes(&key)?;
            let inode = RocksInodeStore::decode_inode(id, &bytes)?;
            let Ok(directory) = inode.as_dir_ref() else {
                continue;
            };
            if existing.contains(&directory.id()) {
                continue;
            }

            let mut attributes =
                DirectoryAttributes::new(directory.mtime(), directory.ctime(), directory.nlink());
            attributes.children = None;
            batch.write_legacy_directory_attributes(directory.id(), attributes)?;
            pending += 1;
            if pending == DIRECTORY_ATTRIBUTE_MIGRATION_BATCH_SIZE {
                batch.commit()?;
                batch = self.store.new_batch();
                pending = 0;
            }
        }

        if pending > 0 {
            batch.commit()?;
        }

        // A crash before this marker is durable repeats only idempotent
        // missing-record writes on the next startup.
        self.store.mark_directory_attributes_migrated()
    }

    pub fn initialize_root_directory_attributes(&self) -> CommonResult<()> {
        self.store.initialize_root_directory_attributes()
    }

    /// Acquire a metadata snapshot scan lease.
    ///
    /// The lease temporarily holds an `Arc<RocksInodeStore>` clone; scanners
    /// must drop the lease before the store can be restored. Returns an error
    /// while a restore is pending or in progress.
    pub fn begin_snapshot_scan(&self) -> CommonResult<MetadataScanLease> {
        self.ensure_available()?;
        let ticket = self.scan_control.try_acquire_lease()?;
        Ok(MetadataScanLease::new(
            self.scan_control.clone(),
            self.store.clone(),
            ticket,
        ))
    }

    /// Begin a Raft snapshot restore.
    ///
    /// Cancels all active scan leases and waits (bounded by `wait_timeout`)
    /// until they are dropped. On success the returned guard keeps new leases
    /// out until the restore completes; on timeout the store is untouched.
    pub fn begin_restore(
        &self,
        wait_timeout: std::time::Duration,
    ) -> CommonResult<MetadataRestoreGuard> {
        self.scan_control.begin_restore(wait_timeout)?;
        Ok(MetadataRestoreGuard::new(self.scan_control.clone()))
    }

    pub fn scan_control(&self) -> &Arc<MetadataScanControl> {
        &self.scan_control
    }

    pub fn get_ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.ttl_bucket_list.clone()
    }

    pub fn apply_add(
        &self,
        parent: &InodeView,
        child: &InodeView,
        write_parent: bool,
        parent_delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(child)?;
        if write_parent {
            batch.write_inode(parent)?;
        }
        Self::merge_directory_attributes(&mut batch, parent.id(), parent_delta)?;
        if child.is_dir() {
            batch.write_fresh_directory_attributes(child.id(), child.directory_attributes())?;
        }
        batch.add_child(parent.id(), child.name(), child.id())?;

        crate::fault_point! {
            sync,
            name: "master.metadata.add.before_commit",
            description: "Before committing the add metadata batch",
            context: { "child_name" => child.name() },
            return_error: |fault| Err(fault.message.into()),
        }

        batch.commit()?;

        parent.mark_directory_attributes_persisted();
        child.mark_directory_attributes_persisted();

        self.ttl_bucket_list.add(child);

        match child {
            InodeView::File(_) => self.fs_stats.increment_file_count(),
            InodeView::Dir(d) => {
                // Don't count root directory
                if d.id != ROOT_INODE_ID {
                    self.fs_stats.increment_dir_count();
                }
            }
            InodeView::FileEntry(..) => self.fs_stats.increment_file_count(),
        }

        Ok(())
    }

    /// Removes a bounded batch of namespace entries with a single store commit.
    ///
    /// Directories are removed as empty inodes only: descendants are never
    /// walked, so the caller must ensure every listed directory is empty once
    /// the earlier paths of the batch are gone. In-memory indexes (TTL buckets
    /// and file counts) and the returned block lists are published only after
    /// the batch commits, so a failed commit applies nothing instead of leaving
    /// paths durable without their reclamation results.
    pub fn apply_purge(
        &self,
        items: Vec<PurgeItem>,
        ctime: i64,
        parent_deltas: HashMap<i64, DirectoryAttributeDelta>,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();
        // Multiple names (hard links) of one inode may appear in a batch; the
        // cache makes each decrement see the staged value instead of re-reading
        // the pre-batch nlink from the store. `None` marks an inode deleted by
        // an earlier item.
        let mut staged: HashMap<i64, Option<InodeView>> = HashMap::new();
        let mut ttl_removals = Vec::new();
        let mut del_res = DeleteResult::new();
        let mut deleted_files = 0i64;
        let mut deleted_dirs = 0i64;

        for item in items {
            let PurgeItem {
                parent,
                target,
                name,
            } = item;

            batch.write_inode(&parent)?;
            // The caller (fs_dir) staged these deltas from the live nodes.
            let delta = parent_deltas
                .get(&parent.id())
                .cloned()
                .unwrap_or_else(|| DirectoryAttributeDelta::new(parent.mtime(), parent.ctime(), 0));
            Self::merge_directory_attributes(&mut batch, parent.id(), delta)?;
            batch.delete_child(parent.id(), &name)?;

            match target {
                PurgeTarget::Dir(dir) => {
                    batch.delete_inode(dir.id())?;
                    batch.delete_directory_attributes(dir.id())?;
                    if dir.id() != ROOT_INODE_ID {
                        deleted_dirs += 1;
                    }
                    ttl_removals.push(dir);
                }
                PurgeTarget::File(inode_id) => {
                    let res = self.stage_decrement_nlink(
                        inode_id,
                        1,
                        ctime,
                        &mut batch,
                        Some(&mut staged),
                        &mut ttl_removals,
                    )?;
                    del_res.inodes += res.inodes;
                    del_res.bytes += res.bytes;
                    del_res.blocks.extend(res.blocks);
                    deleted_files += 1;
                }
            }
        }

        batch.commit()?;

        for inode in &ttl_removals {
            self.ttl_bucket_list.remove(inode);
        }
        if deleted_files > 0 {
            self.fs_stats.add_file_count(-deleted_files);
        }
        if deleted_dirs > 0 {
            self.fs_stats.add_dir_count(-deleted_dirs);
        }

        Ok(del_res)
    }

    pub fn apply_delete(
        &self,
        parent: &InodeView,
        del: &InodeView,
        del_name: &str,
        ctime: i64,
        write_parent: bool,
        parent_delta: DirectoryAttributeDelta,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();
        if write_parent {
            batch.write_inode(parent)?;
        }
        Self::merge_directory_attributes(&mut batch, parent.id(), parent_delta)?;

        let mut del_res = DeleteResult::new();
        let mut deleted_files = 0i64;
        let mut deleted_dirs = 0i64;
        let mut ttl_removals = Vec::new();

        if !del.is_dir() {
            batch.delete_child(parent.id(), del_name)?;
            // The caller holds the namespace write path, so an in-memory file
            // inode already matches the store row; skip the point lookup and
            // clone the view instead. A FileEntry edge (no full inode in
            // memory) must still go through the store lookup, which skips
            // missing rows.
            del_res = if let InodeView::File(_) = del {
                let mut staged: HashMap<i64, Option<InodeView>> = HashMap::new();
                staged.insert(del.id(), Some(del.clone()));
                self.stage_decrement_nlink(
                    del.id(),
                    1,
                    ctime,
                    &mut batch,
                    Some(&mut staged),
                    &mut ttl_removals,
                )?
            } else {
                self.decrement_inode_nlink(del.id(), 1, ctime, &mut batch, &mut ttl_removals)?
            };
            deleted_files = 1;
        } else {
            let mut stack = LinkedList::new();
            // The caller keeps the namespace stable until this batch commits.
            stack.push_back((parent.id(), del_name.to_string(), InodePtr::from_ref(del)));
            let mut file_link_counts: HashMap<i64, u32> = HashMap::new();
            while let Some((parent_id, edge_name, item)) = stack.pop_front() {
                batch.delete_child(parent_id, &edge_name)?;
                let inode_id = item.id();
                match item.as_ref() {
                    InodeView::Dir(dir) => {
                        batch.delete_inode(inode_id)?;
                        batch.delete_directory_attributes(inode_id)?;
                        ttl_removals.push(item.as_ref().clone_metadata());
                        if dir.id != ROOT_INODE_ID {
                            deleted_dirs += 1;
                        }
                        for child in dir.children_vec() {
                            stack.push_back((
                                inode_id,
                                child.name().to_string(),
                                InodePtr::from_owned(child),
                            ));
                        }
                    }
                    _ => {
                        deleted_files += 1;
                        let links = file_link_counts.entry(inode_id).or_default();
                        *links = links.saturating_add(1);
                    }
                }
            }

            // Multiple edges may reference the same inode.
            for (inode_id, links) in file_link_counts {
                let res = self.decrement_inode_nlink(
                    inode_id,
                    links,
                    ctime,
                    &mut batch,
                    &mut ttl_removals,
                )?;
                del_res.inodes += res.inodes;
                del_res.bytes += res.bytes;
                del_res.blocks.extend(res.blocks);
            }
        }

        crate::fault_point! {
            sync,
            name: "master.metadata.delete.before_commit",
            description: "Before committing delete metadata",
            context: {},
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;

        for inode in &ttl_removals {
            self.ttl_bucket_list.remove(inode);
        }

        if deleted_files > 0 {
            self.fs_stats.add_file_count(-deleted_files);
        }
        if deleted_dirs > 0 {
            self.fs_stats.add_dir_count(-deleted_dirs);
        }

        Ok(del_res)
    }

    pub fn apply_free(&self, inodes: Vec<InodeView>) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        for inode in inodes {
            batch.write_inode(&inode)?;
            if inode.is_dir() {
                batch.write_directory_attributes(inode.id(), inode.directory_attributes())?;
            }
        }
        crate::fault_point! {
            sync,
            name: "master.metadata.free.before_commit",
            description: "Before committing free metadata",
            context: {},
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;
        Ok(())
    }

    pub fn apply_cache_invalidations(&self, inodes: Vec<InodeView>) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        for inode in inodes {
            batch.write_inode(&inode)?;
        }
        batch.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // rename delta matrix; refactor with RenameDeltas struct later
    pub fn apply_rename(
        &self,
        src_parent: &InodeView,
        src_inode: &InodeView,
        src_name: &str,
        dst_parent: &InodeView,
        dst_inode: &InodeView,
        parents_equal: bool,
        src_delta: DirectoryAttributeDelta,
        dst_delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        if src_inode.id() != dst_inode.id() {
            return err_box!(
                "rename inode id mismatch: source id {}, destination id {}",
                src_inode.id(),
                dst_inode.id()
            );
        }

        let mut batch = self.store.new_batch();

        // The edge name is the namespace key. The inode name is duplicated metadata
        // and may lag behind after older bugs or checkpoint restore.
        batch.delete_child(src_parent.id(), src_name)?;

        // Add new node.
        batch.write_inode(dst_inode)?;
        batch.add_child(dst_parent.id(), dst_inode.name(), dst_inode.id())?;
        Self::merge_directory_attributes(&mut batch, src_parent.id(), src_delta)?;
        if src_parent.id() != dst_parent.id() {
            Self::merge_directory_attributes(&mut batch, dst_parent.id(), dst_delta)?;
        }
        // Update the modification time of the previous node. A plain
        // same-directory rename stages two equal parent views; one row write
        // carries their shared value.
        batch.write_inode(src_parent)?;
        if !parents_equal {
            batch.write_inode(dst_parent)?;
        }

        crate::fault_point! {
            sync,
            name: "master.metadata.rename.before_commit",
            description: "Before committing the rename metadata batch",
            context: { "source_name" => src_name },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;

        src_parent.mark_directory_attributes_persisted();
        dst_parent.mark_directory_attributes_persisted();

        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // rename delta matrix; refactor with RenameDeltas struct later
    pub fn apply_replace_rename(
        &self,
        src_parent: &InodeView,
        src_inode: &InodeView,
        src_name: &str,
        dst_parent: &InodeView,
        dst_inode: &InodeView,
        replaced: &InodeView,
        ctime: i64,
        src_delta: DirectoryAttributeDelta,
        dst_delta: DirectoryAttributeDelta,
    ) -> CommonResult<DeleteResult> {
        if src_inode.id() != dst_inode.id() || src_inode.id() == replaced.id() {
            return err_box!("invalid inode identities for replacement rename");
        }
        let mut batch = self.store.new_batch();
        let mut ttl_removals = Vec::new();
        batch.delete_child(src_parent.id(), src_name)?;
        batch.delete_child(dst_parent.id(), dst_inode.name())?;
        let result = if replaced.is_dir() {
            batch.delete_inode(replaced.id())?;
            batch.delete_directory_attributes(replaced.id())?;
            ttl_removals.push(replaced.clone_metadata());
            DeleteResult::new()
        } else {
            self.decrement_inode_nlink(replaced.id(), 1, ctime, &mut batch, &mut ttl_removals)?
        };
        batch.write_inode(dst_inode)?;
        batch.add_child(dst_parent.id(), dst_inode.name(), dst_inode.id())?;
        Self::merge_directory_attributes(&mut batch, src_parent.id(), src_delta)?;
        if src_parent.id() != dst_parent.id() {
            Self::merge_directory_attributes(&mut batch, dst_parent.id(), dst_delta)?;
        }
        batch.write_inode(src_parent)?;
        batch.write_inode(dst_parent)?;
        crate::fault_point! {
            sync,
            name: "master.metadata.rename.before_commit",
            description: "Before committing the rename metadata batch",
            context: { "source_name" => src_name },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;
        src_parent.mark_directory_attributes_persisted();
        dst_parent.mark_directory_attributes_persisted();
        for inode in &ttl_removals {
            self.ttl_bucket_list.remove(inode);
        }
        if replaced.is_dir() {
            self.fs_stats.add_dir_count(-1);
        } else {
            self.fs_stats.decrement_file_count();
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)] // rename delta matrix; refactor with RenameDeltas struct later
    pub fn apply_exchange(
        &self,
        src_parent: &InodeView,
        src_name: &str,
        dst_parent: &InodeView,
        dst_name: &str,
        at_src: &InodeView,
        at_dst: &InodeView,
        src_delta: DirectoryAttributeDelta,
        dst_delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.delete_child(src_parent.id(), src_name)?;
        batch.delete_child(dst_parent.id(), dst_name)?;
        batch.write_inode(at_src)?;
        batch.write_inode(at_dst)?;
        batch.add_child(src_parent.id(), at_src.name(), at_src.id())?;
        batch.add_child(dst_parent.id(), at_dst.name(), at_dst.id())?;
        if src_parent.id() == dst_parent.id() {
            Self::merge_directory_attributes(&mut batch, src_parent.id(), src_delta)?;
        } else {
            Self::merge_directory_attributes(&mut batch, src_parent.id(), src_delta)?;
            Self::merge_directory_attributes(&mut batch, dst_parent.id(), dst_delta)?;
        }
        batch.write_inode(src_parent)?;
        batch.write_inode(dst_parent)?;

        crate::fault_point! {
            sync,
            name: "master.metadata.exchange.before_commit",
            description: "Before committing the exchange metadata batch",
            context: { "source_name" => src_name },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;
        src_parent.mark_directory_attributes_persisted();
        dst_parent.mark_directory_attributes_persisted();

        Ok(())
    }

    pub fn apply_new_block(
        &self,
        file: &InodeView,
        commit_blocks: &[CommitBlock],
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(file)?;
        for commit in commit_blocks {
            for item in &commit.locations {
                batch.add_location(commit.block_id, item)?;
            }
        }

        crate::fault_point! {
            sync,
            name: "master.metadata.add_block.before_commit",
            description: "Before committing add-block metadata",
            context: { "inode_id" => file.id() },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()
    }

    pub fn apply_complete_file(
        &self,
        file: &InodeView,
        commit_blocks: &[CommitBlock],
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(file)?;
        for commit in commit_blocks {
            for item in &commit.locations {
                batch.add_location(commit.block_id, item)?;
            }
        }

        crate::fault_point! {
            sync,
            name: "master.metadata.complete_file.before_commit",
            description: "Before committing completed-file metadata",
            context: { "inode_id" => file.id() },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;

        // Refresh TTL index in case set_attr_opts changed ttl_ms/ttl_action or
        // file.complete() advanced mtime (TTL expiration = mtime + ttl_ms).
        self.ttl_bucket_list.add(file);

        Ok(())
    }

    pub fn apply_overwrite_file(&self, file: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.write_inode(file)?;

        crate::fault_point! {
            sync,
            name: "master.metadata.overwrite.before_commit",
            description: "Before committing overwrite metadata",
            context: { "inode_id" => file.id() },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;

        // Overwrite can change ttl_ms/ttl_action and the TTL-base marker;
        // refresh the index so the inode moves to the bucket matching its
        // new expiration instead of waiting for the stale one to fire.
        self.ttl_bucket_list.add(file);

        Ok(())
    }

    pub fn apply_reopen_file(&self, file: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.write_inode(file)?;
        crate::fault_point! {
            sync,
            name: "master.metadata.writer.before_commit",
            description: "Before committing writer metadata",
            context: { "inode_id" => file.id() },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()
    }

    pub fn apply_set_attr(&self, inodes: impl AsRef<[InodeView]>) -> CommonResult<()> {
        let inodes = inodes.as_ref();
        let mut batch = self.store.new_batch();
        for inode in inodes {
            batch.write_inode(inode)?;
            if inode.is_dir() {
                batch.write_directory_attributes(inode.id(), inode.directory_attributes())?;
            }
        }
        crate::fault_point! {
            sync,
            name: "master.metadata.set_attr.before_commit",
            description: "Before committing the set-attribute metadata batch",
            context: { "root_name" => inodes.first().map_or("", |inode| inode.name()) },
            return_error: |fault| Err(fault.message.into()),
        }
        batch.commit()?;

        for inode in inodes {
            if inode.is_dir() {
                inode.set_directory_attributes(inode.directory_attributes());
            }
            inode.mark_directory_attributes_persisted();
            self.ttl_bucket_list.add(inode);
        }

        Ok(())
    }

    /// Persists a symlink inode and its directory edge under `parent`.
    /// `is_add`: create a new symlink dentry (adds a directory entry); bump live file count.
    /// `!is_add`: update an existing symlink dentry in place; file count unchanged.
    pub fn apply_symlink(
        &self,
        parent: &InodeView,
        new_inode: &InodeView,
        old_id: Option<i64>,
        parent_delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        let mut ttl_removals = Vec::new();
        if let Some(id) = old_id.filter(|id| *id != new_inode.id()) {
            self.decrement_inode_nlink(id, 1, new_inode.ctime(), &mut batch, &mut ttl_removals)?;
        }

        batch.write_inode(parent)?;
        batch.write_inode(new_inode)?;
        if old_id.is_none() {
            Self::merge_directory_attributes(&mut batch, parent.id(), parent_delta)?;
        } else {
            let mut attributes = parent.directory_attributes();
            attributes.mtime = attributes.mtime.max(parent.mtime());
            attributes.ctime = attributes.ctime.max(parent.ctime());
            batch.write_directory_attributes(parent.id(), attributes)?;
        }
        batch.add_child(parent.id(), new_inode.name(), new_inode.id())?;

        crate::fault_point! {
            sync,
            name: "master.metadata.symlink.before_commit",
            description: "Before committing the symlink metadata batch",
            context: { "child_name" => new_inode.name() },
            return_error: |fault| Err(fault.message.into()),
        }

        batch.commit()?;

        for inode in &ttl_removals {
            self.ttl_bucket_list.remove(inode);
        }
        if old_id.is_none() {
            self.fs_stats.increment_file_count();
        }

        Ok(())
    }

    pub fn apply_link(
        &self,
        parent: &InodeView,
        new_entry: &InodeView,
        original_inode_id: i64,
        ctime: i64,
        parent_delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;
        Self::merge_directory_attributes(&mut batch, parent.id(), parent_delta)?;

        //link is a edge, link to same inode
        batch.add_child(parent.id(), new_entry.name(), original_inode_id)?;

        // Increment nlink count of the original inode (in the same batch for atomicity)
        self.increment_inode_nlink(original_inode_id, ctime, &mut batch)?;

        crate::fault_point! {
            sync,
            name: "master.metadata.link.before_commit",
            description: "Before committing the link metadata batch",
            context: { "child_name" => new_entry.name() },
            return_error: |fault| Err(fault.message.into()),
        }

        batch.commit()?;

        self.fs_stats.increment_file_count();

        Ok(())
    }

    fn increment_inode_nlink(
        &self,
        inode_id: i64,
        ctime: i64,
        batch: &mut InodeWriteBatch<'_>,
    ) -> CommonResult<()> {
        if let Some(mut inode_view) = self.get_inode(inode_id, None)? {
            match &mut inode_view {
                InodeView::File(_) => {
                    inode_view.incr_nlink(ctime);
                    batch.write_inode(&inode_view)?;
                }
                _ => {
                    return err_box!("Cannot increment nlink for non-file inode {}", inode_id);
                }
            }
        } else {
            return err_box!("Inode {} not found when incrementing nlink", inode_id);
        }
        Ok(())
    }

    pub fn apply_unlink(
        &self,
        parent: &InodeView,
        child: &InodeView,
        child_name: &str,
        ctime: i64,
        parent_delta: DirectoryAttributeDelta,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();

        // Write the updated parent directory (child will be removed by the caller)
        batch.write_inode(parent)?;
        Self::merge_directory_attributes(&mut batch, parent.id(), parent_delta)?;

        // Remove the child from the parent's children list
        batch.delete_child(parent.id(), child_name)?;

        // Decrement nlink count of the file being unlinked.
        // If nlink reaches 0 the inode is also deleted and del_res.blocks will be populated.
        let mut ttl_removals = Vec::new();
        let del_res = if let InodeView::File(_) = child {
            // The caller holds the namespace write path, so the in-memory
            // child already matches the store row; skip the point lookup.
            let mut staged: HashMap<i64, Option<InodeView>> = HashMap::new();
            staged.insert(child.id(), Some(child.clone()));
            self.stage_decrement_nlink(
                child.id(),
                1,
                ctime,
                &mut batch,
                Some(&mut staged),
                &mut ttl_removals,
            )?
        } else {
            DeleteResult::new()
        };

        batch.commit()?;

        for inode in &ttl_removals {
            self.ttl_bucket_list.remove(inode);
        }

        self.fs_stats.decrement_file_count();

        Ok(del_res)
    }

    pub fn apply_unlink_file_entry(
        &self,
        parent: &InodeView,
        child: &InodeView,
        child_name: &str,
        inode_id: i64,
        ctime: i64,
        parent_delta: DirectoryAttributeDelta,
    ) -> CommonResult<DeleteResult> {
        if child.id() != inode_id {
            return err_box!(
                "unlink FileEntry->inode id mismatch: entry references inode id {}, caller passed inode id {}",
                child.id(),
                inode_id
            );
        }

        let mut batch = self.store.new_batch();

        // Write the updated parent directory
        batch.write_inode(parent)?;
        Self::merge_directory_attributes(&mut batch, parent.id(), parent_delta)?;

        // Remove the FileEntry from the parent's children list
        batch.delete_child(parent.id(), child_name)?;

        // Decrement nlink count of the original inode.
        // If nlink reaches 0 the inode is also deleted and del_res.blocks will be populated.
        let mut ttl_removals = Vec::new();
        let del_res =
            self.decrement_inode_nlink(inode_id, 1, ctime, &mut batch, &mut ttl_removals)?;

        batch.commit()?;

        for inode in &ttl_removals {
            self.ttl_bucket_list.remove(inode);
        }

        self.fs_stats.decrement_file_count();

        Ok(del_res)
    }

    // Helper method to decrement nlink count of an inode
    fn decrement_inode_nlink(
        &self,
        inode_id: i64,
        links: u32,
        ctime: i64,
        batch: &mut InodeWriteBatch<'_>,
        ttl_removals: &mut Vec<InodeView>,
    ) -> CommonResult<DeleteResult> {
        self.stage_decrement_nlink(inode_id, links, ctime, batch, None, ttl_removals)
    }

    // Stages link removal, reading through `staged` so repeated names of the
    // same inode inside one delete batch accumulate instead of resetting.
    fn stage_decrement_nlink(
        &self,
        inode_id: i64,
        links: u32,
        ctime: i64,
        batch: &mut InodeWriteBatch<'_>,
        mut staged: Option<&mut HashMap<i64, Option<InodeView>>>,
        ttl_removals: &mut Vec<InodeView>,
    ) -> CommonResult<DeleteResult> {
        let mut del_res = DeleteResult::new();
        let mut inode_view = match staged.as_mut().and_then(|staged| staged.remove(&inode_id)) {
            Some(Some(inode)) => inode,
            Some(None) => return err_box!("Inode {} not found when decrementing nlink", inode_id),
            None => match self.get_inode(inode_id, None)? {
                Some(inode) => inode,
                None => return Ok(del_res),
            },
        };

        let last_link = match &mut inode_view {
            InodeView::File(f) => {
                if f.decrement_nlink_by(links, ctime) == 0 {
                    // Collect block info
                    let locs = f.get_locs(self)?;
                    del_res.inodes = 1;
                    del_res.bytes = u64::try_from(f.get_locs_bytes(&locs)).unwrap_or(0);
                    del_res.blocks.extend(locs);
                    true
                } else {
                    false
                }
            }
            _ => {
                return err_box!("Cannot decrement nlink for non-file inode {}", inode_id);
            }
        };

        if last_link {
            batch.delete_inode(inode_id)?;
            ttl_removals.push(inode_view);
            if let Some(staged) = staged {
                staged.insert(inode_id, None);
            }
        } else {
            // Write the updated inode back to storage
            batch.write_inode(&inode_view)?;
            if let Some(staged) = staged {
                staged.insert(inode_id, Some(inode_view));
            }
        }

        Ok(del_res)
    }

    // Restore to a directory tree from rocksdb.
    //
    // Two-phase bulk-load approach (Issue #964):
    //   Phase 1: Sequentially scan entire `edges` and `inodes` column families
    //            into in-memory HashMaps, eliminating O(N) point lookups.
    //            Inode deserialization is parallelised across available cores.
    //   Phase 2: Assemble the in-memory directory tree via BFS, moving InodeView
    //            values out of the HashMap (zero cloning) and applying the same
    //            repair logic as the previous per-inode-lookup implementation.
    pub fn create_tree(&self) -> CommonResult<(i64, InodeView)> {
        let mut timer = RestoreTimer::new();

        let data = self.load_snapshot_data(&mut timer)?;
        let (last_inode_id, root) = self.build_tree_from_data(data, &mut timer)?;

        self.fs_stats.set_counts(timer.file_count, timer.dir_count);
        timer.log_summary();

        Ok((last_inode_id, root))
    }

    /// Phase 1: Bulk-load all edges and inodes from RocksDB into memory.
    fn load_snapshot_data(&self, timer: &mut RestoreTimer) -> CommonResult<SnapshotData> {
        // --- 1a. Scan edges CF → group by parent_id ---
        let mut edges: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
        let edges_iter = self.store.bulk_scan_edges()?;
        for item in edges_iter {
            let (key, value) = try_err!(item);
            let (parent_id, child_name) = RocksUtils::i64_str_from_bytes(&key)?;
            let child_id = RocksUtils::i64_from_bytes(&value)?;
            edges
                .entry(parent_id)
                .or_default()
                .push((child_name.to_string(), child_id));
            timer.edge_count += 1;
        }
        timer.mark("scan_edges");

        // --- 1a2. Scan the registered dir_attrs CF ---
        let mut dir_attrs: HashMap<i64, DirectoryAttributes> = HashMap::new();
        for item in self.store.scan_directory_attributes()? {
            let (key, value) = match item {
                Ok(ok) => ok,
                Err(e) => {
                    return err_box!("scan dir_attrs failed: {}", e);
                }
            };
            let id = RocksUtils::i64_from_bytes(&key)?;
            let attributes = RocksInodeStore::decode_directory_attributes(id, Some(&value))?;
            if let Some(attributes) = attributes {
                dir_attrs.insert(id, attributes);
            }
        }
        timer.mark("scan_dir_attrs");

        // --- 1b. Scan inodes CF → collect raw bytes ---
        let mut raw_inodes: Vec<(i64, Vec<u8>)> = Vec::new();
        let inodes_iter = self.store.bulk_scan_inodes()?;
        for item in inodes_iter {
            let (key, value) = try_err!(item);
            let inode_id = RocksUtils::i64_from_bytes(&key)?;
            raw_inodes.push((inode_id, value.to_vec()));
        }
        timer.inode_count = raw_inodes.len();
        timer.mark("scan_inodes");

        // --- 1c. Parallel-deserialize inodes ---
        let inodes = Self::parallel_deserialize_inodes(raw_inodes)?;
        timer.mark("deserialize_inodes");

        Ok(SnapshotData {
            edges,
            inodes,
            dir_attrs,
        })
    }

    /// Parallel-deserialize raw inode bytes into a HashMap using std::thread::scope.
    /// Falls back to single-threaded for small datasets.
    fn parallel_deserialize_inodes(
        raw: Vec<(i64, Vec<u8>)>,
    ) -> CommonResult<HashMap<i64, InodeView>> {
        let total = raw.len();
        let mut inodes = HashMap::with_capacity(total);

        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(total.max(1));

        if num_threads <= 1 || total < 1000 {
            for (id, bytes) in raw {
                let inode = RocksInodeStore::decode_inode(id, &bytes)?;
                inodes.insert(id, inode);
            }
            return Ok(inodes);
        }

        let chunk_size = total.div_ceil(num_threads);

        // Borrow raw as slices — no cloning of Vec<u8> payloads.
        // std::thread::scope guarantees all threads are joined before the
        // scope returns, so borrowing raw for the scope lifetime is safe.
        let thread_results = std::thread::scope(|s| -> CommonResult<Vec<Vec<(i64, InodeView)>>> {
            let handles: Vec<_> = raw
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || -> CommonResult<Vec<(i64, InodeView)>> {
                        let mut result = Vec::with_capacity(chunk.len());
                        for (id, bytes) in chunk {
                            let inode = RocksInodeStore::decode_inode(*id, bytes)?;
                            result.push((*id, inode));
                        }
                        Ok(result)
                    })
                })
                .collect();

            let mut all = Vec::with_capacity(handles.len());
            for h in handles {
                let inner = match h.join() {
                    Ok(result) => result,
                    Err(_) => return err_box!("thread panicked during inode deserialization"),
                };
                all.push(inner?);
            }
            Ok(all)
        })?;

        for chunk_result in thread_results {
            for (id, inode) in chunk_result {
                inodes.insert(id, inode);
            }
        }

        Ok(inodes)
    }

    /// Phase 2 + 3: Assemble the in-memory directory tree from bulk-loaded data.
    ///
    /// Uses BFS with `HashMap::remove()` to move `InodeView` values into the tree
    /// (zero cloning). Repair logic (orphaned edges, duplicate directory edges,
    /// name/parent mismatches) is preserved exactly from the previous implementation.
    fn build_tree_from_data(
        &self,
        mut data: SnapshotData,
        timer: &mut RestoreTimer,
    ) -> CommonResult<(i64, InodeView)> {
        // Load root metadata from the bulk-loaded inodes (or create default).
        let mut root = data
            .inodes
            .remove(&ROOT_INODE_ID)
            .unwrap_or_else(FsDir::create_root);
        if let Some(attributes) = data.dir_attrs.get(&ROOT_INODE_ID) {
            root.as_dir_ref()?.set_directory_attributes(*attributes);
        }

        // BFS stack: (parent_ptr, parent_id)
        let mut stack: VecDeque<(InodePtr, i64)> = VecDeque::new();
        stack.push_back((InodePtr::from_ref(&mut root), ROOT_INODE_ID));

        let mut last_inode_id = ROOT_INODE_ID;
        let mut dir_edges: HashMap<i64, (i64, String)> = HashMap::new();
        let mut seen_ids: HashSet<i64> = HashSet::new();
        let mut repair_batch = self.store.new_batch();
        let mut has_repairs = false;

        while let Some((mut parent, parent_id)) = stack.pop_front() {
            // Look up children for this directory (O(1) HashMap — no RocksDB access).
            let children = match data.edges.get(&parent_id) {
                Some(c) => c,
                None => continue,
            };

            if let Ok(directory) = parent.as_dir_ref() {
                directory
                    .children_handle()
                    .seed_shards_for_bulk_load(children.len());
            }

            for (edge_name, child_id) in children {
                last_inode_id = last_inode_id.max(*child_id);

                // Check for duplicate directory edge BEFORE removing from inodes.
                // A directory can only have one parent; if we've already seen this
                // directory id, the current edge is a duplicate and must be deleted.
                if let Some((old_parent_id, old_name)) = dir_edges.get(child_id) {
                    log::warn!(
                        "create_tree: directory inode {} has multiple parent edges: keeping parent {} name '{}', dropping parent {} name '{}'",
                        child_id, old_parent_id, old_name, parent_id, edge_name
                    );
                    repair_batch.delete_child(parent_id, edge_name)?;
                    has_repairs = true;
                    timer.repair_count += 1;
                    continue;
                }

                // Check if this is a hard-linked file (already consumed for another edge).
                // Files can have multiple parent edges; each edge gets its own FileEntry.
                if seen_ids.contains(child_id) {
                    timer.file_count += 1;
                    let entry = InodeView::new_entry(edge_name.clone(), *child_id);
                    parent.add_child(entry)?;
                    continue;
                }

                // Move the inode out of the HashMap — zero cloning.
                // seen_ids is updated only after a successful lookup so that orphaned
                // edges (missing inode) are not recorded as "seen". Otherwise a second
                // edge to the same missing inode would be misclassified as a hard-link
                // and added to the tree as a FileEntry pointing at a non-existent inode.
                let store_inode = match data.inodes.remove(child_id) {
                    Some(v) => {
                        seen_ids.insert(*child_id);
                        v
                    }
                    None => {
                        // Orphaned edge: inode was deleted but edge was not cleaned up
                        // by older buggy metadata updates. Drop the edge durably so
                        // recovery does not keep replaying the same broken namespace entry.
                        log::warn!(
                            "create_tree: orphaned edge detected, parent_id={}, edge_name='{}', child_id={} has no inode, dropping edge",
                            parent_id,
                            edge_name,
                            child_id
                        );
                        repair_batch.delete_child(parent_id, edge_name)?;
                        has_repairs = true;
                        timer.orphaned_count += 1;
                        timer.repair_count += 1;
                        continue;
                    }
                };

                let inode = match store_inode {
                    InodeView::Dir(_) => {
                        let mut store_inode = store_inode;
                        dir_edges.insert(*child_id, (parent_id, edge_name.clone()));
                        if let Some(attributes) = data.dir_attrs.get(child_id) {
                            store_inode
                                .as_dir_ref()?
                                .set_directory_attributes(*attributes);
                        }

                        // Name / parent_id mismatch repair
                        if store_inode.name() != edge_name.as_str()
                            || store_inode.as_dir_ref()?.parent_id() != parent_id
                        {
                            store_inode.change_name(edge_name.clone());
                            store_inode.set_parent_id(parent_id);
                            repair_batch.write_inode(&store_inode)?;
                            has_repairs = true;
                            timer.repair_count += 1;
                        }
                        self.ttl_bucket_list.add(&store_inode);
                        // Don't count root directory
                        if *child_id != ROOT_INODE_ID {
                            timer.dir_count += 1;
                        }
                        store_inode
                    }
                    _ => {
                        self.ttl_bucket_list.add(&store_inode);
                        timer.file_count += 1;
                        // Use lightweight FileEntry instead of the full inode
                        InodeView::new_entry(edge_name.clone(), *child_id)
                    }
                };

                let child_ptr = parent.add_child(inode)?;

                // If directory, push onto stack for its children
                if let ChildAddResult::Dir { node, .. } = child_ptr {
                    // The queue owns its metadata copy instead of retaining a
                    // pointer into the parent's container. The directory's
                    // child index remains shared with the tree entry.
                    stack.push_back((
                        InodePtr::from_owned(InodeDir::from_dir_node_live(String::new(), node)),
                        *child_id,
                    ));
                }
            }
        }

        // Commit repairs atomically
        if has_repairs {
            crate::fault_point! {
                sync,
                name: "master.metadata.restore.repair_commit",
                description: "Before committing the tree repair batch",
                context: { "phase" => "restore_from_rocksdb" },
                return_error: |fault| Err(fault.message.into()),
            }
            repair_batch.commit()?;
        }
        timer.mark("build_tree");

        Ok((last_inode_id, root))
    }

    pub fn get_file_locations(
        &self,
        file: &InodeFile,
    ) -> CommonResult<HashMap<i64, Vec<BlockLocation>>> {
        if file.blocks.len() > 1 {
            return self
                .store
                .get_locations_many(file.blocks.iter().map(|meta| meta.id));
        }
        let mut res = HashMap::with_capacity(file.blocks.len());
        for meta in &file.blocks {
            let locs = self.store.get_locations(meta.id)?;
            res.insert(meta.id, locs);
        }

        Ok(res)
    }

    pub fn get_block_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        self.store.get_locations(block_id)
    }

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.add_location(block_id, &location)?;
        batch.commit()?;
        Ok(())
    }

    //get_inode should return the inode with the name of the FileEntry
    //TODO refactor: remove seq name from store_inode
    pub fn get_inode(&self, id: i64, name: Option<&str>) -> CommonResult<Option<InodeView>> {
        let mut inode_view = self.store.get_inode(id)?;
        if let Some(inode) = inode_view.as_ref() {
            if inode.is_dir() {
                if let Some(attributes) = self.store.get_directory_attributes(id)? {
                    inode.set_directory_attributes(attributes);
                }
            }
        }
        if let Some(name) = name {
            if let Some(ref mut inode) = inode_view {
                inode.change_name(name.to_string());
            }
        }
        Ok(inode_view)
    }

    pub fn batched_get_inodes(
        &self,
        inp: &InodePath,
        list: Vec<InodeView>,
    ) -> CommonResult<Vec<FileStatus>> {
        if list.is_empty() {
            return Ok(vec![]);
        }

        let mut out: Vec<Option<FileStatus>> = vec![None; list.len()];
        let mut scan_inodes = Vec::with_capacity(list.len());

        for (index, inode) in list.iter().enumerate() {
            match inode {
                InodeView::FileEntry(e) => {
                    scan_inodes.push((index, RocksUtils::i64_to_bytes(e.id)));
                }
                view => {
                    let child_path = inp.child_path(view.name());
                    out[index] = Some(view.to_file_status(&child_path)?);
                }
            }
        }

        if scan_inodes.is_empty() {
            return Ok(out.into_iter().flatten().collect());
        }

        let keys = scan_inodes.iter().map(|x| &x.1);
        let batch_res = self.store.batched_multi_get_inodes(keys, false)?;
        for (i, item) in batch_res.into_iter().enumerate() {
            let index = try_option!(
                scan_inodes.get(i),
                "batched_get_inodes: missing scan entry for batch result index {}",
                i
            )
            .0;
            let file_entry = try_option!(
                list.get(index),
                "batched_get_inodes: file entry index {} out of range, list len {}",
                index,
                list.len()
            );

            if let Some(bytes) = try_err!(item) {
                let mut inode: InodeView = MetadataCodec::decode(bytes.as_ref())?;
                if inode.id() != file_entry.id() {
                    return err_box!(
                        "batched_get_inodes: inode id mismatch for key index {} (expected id {}, stored inode id {})",
                        i,
                        file_entry.id(),
                        inode.id()
                    );
                }

                inode.change_name(file_entry.name().to_owned());
                let child_path = inp.child_path(file_entry.name());
                out[index] = Some(inode.to_file_status(&child_path)?);
            } else {
                log::warn!(
                    "batched_get_inodes: inode missing in store path {}, id {}, skipping",
                    inp.child_path(file_entry.name()),
                    file_entry.id()
                );
            }
        }

        Ok(out.into_iter().flatten().collect())
    }

    pub fn cf_hash(&self, cf: &str) -> CommonResult<u128> {
        let iter = self.store.iter_cf(cf)?;
        let mut hash = 0;
        for inode in iter {
            let kv = inode?;
            hash += Utils::crc32(kv.0.as_ref()) as u128;
            hash += Utils::crc32(kv.1.as_ref()) as u128;
        }
        Ok(hash)
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.ensure_available()?;
        self.store.db.create_checkpoint(id)
    }

    fn restore<T: AsRef<str>>(&mut self, path: T) -> CommonResult<()> {
        // Exclude cursor writes while checking the old database references.
        let mut cursor = self.cursor_store.write();
        if !cursor
            .as_ref()
            .is_some_and(|store| Arc::ptr_eq(store, &self.store))
        {
            return err_box!("cannot restore: journal cursor store is unavailable");
        }
        // The two expected references belong to self.store and cursor.
        let ref_count = Arc::strong_count(&self.store);
        if ref_count > 2 {
            return err_box!(
                "cannot restore: RocksInodeStore has {} references (expected 2). \
                Other components are still holding the database open.",
                ref_count
            );
        }

        let conf = self.store.db.conf().clone();
        crate::fault_point! {
            sync,
            name: "master.metadata.restore.before_temp_store",
            description: "Before opening the temporary metadata store",
            context: { "data_dir" => conf.data_dir.as_str() },
            return_error: |fault| Err(fault.message.into()),
        }

        // Open the temporary database before releasing either old handle: a
        // failure here leaves both the old store and cursor usable.
        let tmp_conf = DBConf::new(Utils::temp_file());
        let tmp_store = Arc::new(RocksInodeStore::new(tmp_conf, false)?);
        self.set_available(false);
        *cursor = None;
        self.store = tmp_store;
        drop(cursor);
        crate::fault_point! {
            sync,
            name: "master.metadata.restore.after_store_detach",
            description: "After detaching the original metadata store",
            context: { "data_dir" => conf.data_dir.as_str() },
            return_error: |fault| Err(fault.message.into()),
        }

        // Delete the original file and move the checkpoint to the data directory.
        FileUtils::delete_path(&conf.data_dir, true)?;
        FileUtils::copy_dir(path.as_ref(), &conf.data_dir)?;

        self.store = Arc::new(RocksInodeStore::new(conf, false)?);
        *self.cursor_store.write() = Some(self.store.clone());
        self.set_available(true);
        Ok(())
    }

    pub(crate) fn cursor_store_available(&self) -> bool {
        self.cursor_store
            .read()
            .as_ref()
            .is_some_and(|store| Arc::ptr_eq(store, &self.store))
    }

    pub(crate) fn invalidate_cursor_store(&self) {
        self.set_available(false);
        *self.cursor_store.write() = None;
    }

    /// Call with the namespace guard held while accessing metadata.
    pub(crate) fn ensure_available(&self) -> CommonResult<()> {
        if !self.store_available() {
            return err_box!("metadata store is unavailable after failed restore");
        }
        Ok(())
    }

    pub(crate) fn checked_read_snapshot(&self) -> CommonResult<DBReadSnapshot<'_>> {
        self.ensure_available()?;
        Ok(self.store.read_snapshot())
    }

    /// Restore under a previously acquired restore guard (see `begin_restore`).
    ///
    /// The guard must belong to this store; it proves scan leases have been
    /// drained and new ones are locked out for the whole replace window.
    pub fn restore_with_guard<T: AsRef<str>>(
        &mut self,
        guard: &MetadataRestoreGuard,
        path: T,
    ) -> CommonResult<()> {
        if !guard.belongs_to(&self.scan_control) {
            return err_box!("metadata restore guard belongs to a different store");
        }
        self.restore(path)
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        self.store.db.get_checkpoint_path(id)
    }

    pub fn new_batch(&self) -> InodeWriteBatch<'_> {
        self.store.new_batch()
    }

    pub fn apply_mount(&self, id: u32, info: &MountInfo) -> CommonResult<()> {
        self.store.add_mountpoint(id, info)
    }

    pub fn apply_umount(&self, id: u32) -> CommonResult<()> {
        self.store.remove_mountpoint(id)
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        self.store.get_mount_info(id)
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        self.store.get_mount_table()
    }

    pub fn save_mount_table_version(&self, version: u64) -> CommonResult<()> {
        self.store.save_mount_table_version(version)
    }

    pub fn get_mount_table_version(&self) -> CommonResult<u64> {
        self.store.get_mount_table_version()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        self.fs_stats.counts()
    }

    /// Same stats instance, for view-independent metric accessors.
    pub(crate) fn fs_stats_handle(&self) -> Arc<FileSystemStats> {
        self.fs_stats.clone()
    }

    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        self.store.get_locations(block_id)
    }

    pub fn get_locks(&self, id: i64) -> CommonResult<LockMeta> {
        self.store.get_locks(id)
    }

    pub fn apply_set_locks(&self, id: i64, lock: &[FileLock]) -> CommonResult<()> {
        self.store.set_locks(id, lock)
    }

    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        self.store.get_rocksdb_metrics()
    }

    pub fn store(&self) -> &RocksInodeStore {
        &self.store
    }
}

/// Typed, read-only view of one RocksDB snapshot over the inode/edge column
/// families.
///
/// Created through a [`MetadataScanLease`] so the underlying store Arc stays
/// alive for the whole scan. Exposes only the reads the dir-space scanner
/// needs; it never exposes the writable DB. All returned iterators borrow from
/// this wrapper and must be dropped before it.
pub struct InodeStoreSnapshot<'a> {
    snapshot: DBReadSnapshot<'a>,
}

impl<'a> InodeStoreSnapshot<'a> {
    pub(crate) fn new(snapshot: DBReadSnapshot<'a>) -> Self {
        Self { snapshot }
    }

    /// Full scan of the `inodes` column family.
    pub fn scan_inodes(&self) -> CommonResult<RocksIterator<'_>> {
        self.snapshot.bulk_scan(RocksInodeStore::CF_INODES)
    }

    /// Full scan of the `edges` column family.
    pub fn scan_edges(&self) -> CommonResult<RocksIterator<'_>> {
        self.snapshot.bulk_scan(RocksInodeStore::CF_EDGES)
    }

    /// Point read of a single inode within the snapshot.
    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let bytes = self
            .snapshot
            .get_pinned_cf(RocksInodeStore::CF_INODES, RocksUtils::i64_to_bytes(id))?;
        match bytes {
            None => Ok(None),
            Some(v) => Ok(Some(RocksInodeStore::decode_inode(id, &v)?)),
        }
    }

    /// Point read of directory attributes from the same snapshot as inode and edge reads.
    pub fn get_directory_attributes(&self, id: i64) -> CommonResult<Option<DirectoryAttributes>> {
        let value = self
            .snapshot
            .get_pinned_cf(RocksInodeStore::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(id))?;
        RocksInodeStore::decode_directory_attributes(id, value.as_deref())
    }

    /// Read a child inode ID from the same snapshot without exposing an in-memory pointer.
    pub fn get_child_id(&self, parent_id: i64, name: &str) -> CommonResult<Option<i64>> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, name);
        self.snapshot
            .get_pinned_cf(RocksInodeStore::CF_EDGES, key)?
            .map(|value| RocksUtils::i64_from_bytes(&value))
            .transpose()
    }

    /// Resolve an edge and its inode from one snapshot, returning owned metadata.
    /// A returned directory does not contain its in-memory children.
    pub fn get_child(&self, parent_id: i64, name: &str) -> CommonResult<Option<InodeView>> {
        let Some(id) = self.get_child_id(parent_id, name)? else {
            return Ok(None);
        };
        let Some(mut inode) = self.get_inode(id)? else {
            return err_box!(
                "Failed to load inode {}: directory edge references missing inode",
                id
            );
        };
        inode.change_name(name.to_owned());
        Ok(Some(inode))
    }

    /// Scan the child edges of one parent directory within the snapshot.
    pub fn scan_children(&self, parent_id: i64) -> CommonResult<RocksIterator<'_>> {
        self.snapshot.prefix_scan(
            RocksInodeStore::CF_EDGES,
            RocksUtils::i64_to_bytes(parent_id),
        )
    }

    /// Count child edges in the same snapshot without materializing entries.
    pub fn count_children(&self, parent_id: i64) -> CommonResult<usize> {
        self.snapshot.count_prefix(
            RocksInodeStore::CF_EDGES,
            RocksUtils::i64_to_bytes(parent_id),
        )
    }

    /// Read child edges and their owned inodes from the same snapshot.
    pub fn children(&self, parent_id: i64) -> CommonResult<Vec<InodeView>> {
        self.children_page(parent_id, &ListOptions::default())
    }

    pub fn children_page(
        &self,
        parent_id: i64,
        opts: &ListOptions,
    ) -> CommonResult<Vec<InodeView>> {
        let mut children = Vec::new();
        if opts.limit == Some(0) {
            return Ok(children);
        }
        let prefix = RocksUtils::i64_to_bytes(parent_id);
        let start = opts.start_after.as_ref().map_or_else(
            || prefix.to_vec(),
            |name| RocksUtils::i64_str_to_bytes(parent_id, name),
        );
        let mut count = 0;
        for edge in self
            .snapshot
            .prefix_scan_from(RocksInodeStore::CF_EDGES, &prefix, &start)?
        {
            if opts.limit.is_some_and(|limit| count >= limit) {
                break;
            }
            let (key, value) = edge?;
            let (_, name) = RocksUtils::i64_str_from_bytes(&key)?;
            if opts
                .start_after
                .as_ref()
                .is_some_and(|cursor| name <= *cursor)
            {
                continue;
            }
            let id = RocksUtils::i64_from_bytes(&value)?;
            let Some(mut inode) = self.get_inode(id)? else {
                log::warn!(
                    "listing skips missing inode {} for parent {}",
                    id,
                    parent_id
                );
                continue;
            };
            inode.change_name(name.to_string());
            children.push(inode);
            count += 1;
        }
        Ok(children)
    }

    /// Point-read up to `ids.len()` inodes within this snapshot (missing keys
    /// yield `None` at the same index).
    pub fn get_inodes(&self, ids: &[i64]) -> CommonResult<Vec<Option<InodeView>>> {
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            out.push(self.get_inode(id)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "tests/journal_cursor_test.rs"]
mod journal_cursor_test;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::meta::inode::{
        ChildEdge, DirNode, Inode, InodeDir, InodeFile, InodePath, ROOT_INODE_ID,
    };
    use crate::master::Master;

    pub(super) fn new_store(name: &str) -> CommonResult<InodeStore> {
        Master::init_test_metrics();
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "inode-store-test/{}-{}",
            name,
            Utils::rand_str(6)
        )));
        let rocks = RocksInodeStore::new(conf, true)?;
        InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000)?))
    }

    #[test]
    fn apply_rename_deletes_source_edge_name_not_directory_inode_name() -> CommonResult<()> {
        let store = new_store("rename-source-edge-name")?;
        let root = FsDir::create_root();
        let src_parent = root.clone();
        let dst_parent = root.clone();
        let src_inode = InodeView::new_dir("inode-name".to_string(), InodeDir::new(2001, 0));
        let mut dst_inode = src_inode.clone();
        dst_inode.change_name("renamed".to_string());

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&src_inode)?;
            batch.add_child(ROOT_INODE_ID, "edge-name", src_inode.id())?;
            batch.commit()?;
        }

        // The root has no dir_attrs row yet, so the first delta carries the
        // base exactly as fs_dir computes it from the live directory state.
        let delta = root.directory_attribute_delta(DirectoryAttributeDelta::new(100, 100, 0));
        store.apply_rename(
            &src_parent,
            &src_inode,
            "edge-name",
            &dst_parent,
            &dst_inode,
            false,
            delta,
            delta,
        )?;

        let (_, restored) = store.create_tree()?;
        let names: Vec<String> = restored
            .children()
            .into_iter()
            .map(|child| child.name().to_string())
            .collect();
        assert_eq!(names, vec!["renamed"]);

        Ok(())
    }

    #[test]
    fn create_tree_drops_duplicate_directory_edges() -> CommonResult<()> {
        let store = new_store("drop-dir-alias")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("dir".to_string(), InodeDir::new(2002, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "a", dir.id())?;
            batch.add_child(ROOT_INODE_ID, "b", dir.id())?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let names: Vec<String> = restored
            .children()
            .into_iter()
            .map(|c| c.name().to_string())
            .collect();
        assert_eq!(names, vec!["a"]);

        let edge_count = store.store.edges_iter(ROOT_INODE_ID)?.count();
        assert_eq!(edge_count, 1);

        Ok(())
    }

    #[test]
    fn create_tree_persists_directory_edge_hydration() -> CommonResult<()> {
        let store = new_store("persist-dir-edge-hydration")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("stale-name".to_string(), InodeDir::new(2003, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "edge-name", dir.id())?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let child = restored
            .children_handle()
            .expect("restored tree root must be a directory")
            .read_child_view("edge-name")
            .0
            .expect("edge name should be hydrated into restored tree");
        assert_eq!(child.name(), "edge-name");

        let persisted = store
            .store
            .get_inode(dir.id())?
            .expect("directory inode should still exist");
        assert_eq!(persisted.name(), "edge-name");
        assert_eq!(persisted.as_dir_ref()?.parent_id(), ROOT_INODE_ID);

        Ok(())
    }

    /// Regression test for the seen_ids-before-lookup bug (PR #978 review):
    /// Two edges point to the same missing inode. The first edge takes the
    /// orphan path; the second must NOT be misclassified as a hard-link.
    #[test]
    fn create_tree_handles_duplicate_orphaned_edges() -> CommonResult<()> {
        let store = new_store("dup-orphan-edges")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("real-dir".to_string(), InodeDir::new(2020, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "real-dir", dir.id())?;
            // Two orphan edges pointing to the same missing inode (9999).
            batch.add_child(ROOT_INODE_ID, "orphan1", 9999)?;
            batch.add_child(ROOT_INODE_ID, "orphan2", 9999)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let names: Vec<String> = restored
            .children()
            .into_iter()
            .map(|c| c.name().to_string())
            .collect();
        // Only the real directory should be in the tree; both orphaned edges are skipped.
        assert_eq!(names, vec!["real-dir"]);

        Ok(())
    }

    #[test]
    fn batched_get_inodes_skips_missing_file_entry() -> CommonResult<()> {
        let store = new_store("skip-missing-file-entry")?;
        let good_file_id = 2001;
        let missing_id = 9999;
        let root = FsDir::create_root();
        let good_file = InodeView::new_file(
            "good.txt".to_string(),
            InodeFile::new(good_file_id, ROOT_INODE_ID),
        );

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&good_file)?;
            batch.add_child(ROOT_INODE_ID, "good.txt", good_file_id)?;
            batch.commit()?;
        }

        let (_, mut root_view) = store.create_tree()?;
        root_view.add_child(InodeView::new_entry("missing.txt".to_string(), missing_id))?;

        // The live root is a shared node, so the resolved path keeps it alive.
        let root = DirNode::from_view(&root_view);
        let children = root.children.list_options(&Default::default());
        let inp = InodePath::resolve(root, "/", &store)?;
        let result = store.batched_get_inodes(&inp, children)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "good.txt");
        assert_eq!(result[0].id, good_file_id);
        assert_eq!(result[0].path, "/good.txt");

        Ok(())
    }

    #[test]
    fn apply_delete_skips_missing_file_entry() -> CommonResult<()> {
        let store = new_store("delete-missing-file-entry")?;
        let root = FsDir::create_root();
        let missing_id = 9999;
        let missing = InodeView::new_entry("missing.txt".to_string(), missing_id);

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.add_child(ROOT_INODE_ID, "missing.txt", missing_id)?;
            batch.commit()?;
        }

        let result = store.apply_delete(
            &root,
            &missing,
            "missing.txt",
            100,
            true,
            DirectoryAttributeDelta::new(100, 100, 0),
        )?;
        assert_eq!(result.inodes, 0);
        assert!(store.get_inode(missing_id, None)?.is_none());
        assert_eq!(store.store.edges_iter(ROOT_INODE_ID)?.count(), 0);

        Ok(())
    }

    #[test]
    fn apply_delete_handles_valid_and_stale_file_entries() -> CommonResult<()> {
        let store = new_store("delete-valid-and-stale-file-entries")?;
        let root = FsDir::create_root();
        let dir_id = 2001;
        let file_id = 2002;
        let missing_id = 9999;
        let dir = InodeView::new_dir("dir".to_string(), InodeDir::new(dir_id, ROOT_INODE_ID));
        let file = InodeView::new_file("valid.txt".to_string(), InodeFile::new(file_id, dir_id));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.write_inode(&file)?;
            batch.add_child(ROOT_INODE_ID, "dir", dir_id)?;
            batch.add_child(dir_id, "valid.txt", file_id)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let root_children = restored
            .children_handle()
            .expect("restored tree root must be a directory");
        let dir_node = match root_children
            .get_child_edge("dir")
            .expect("restored directory should exist")
        {
            ChildEdge::Dir(node) => node,
            ChildEdge::FileId(id) => panic!("dir edge resolved as a file: {id}"),
        };
        dir_node.add_child_edge(InodeView::new_entry("stale.txt".to_string(), missing_id))?;
        let dir = root_children.read_child_view("dir").0.unwrap();

        let result = store.apply_delete(
            &restored,
            &dir,
            "dir",
            100,
            true,
            DirectoryAttributeDelta::new(100, 100, -1).for_child_removal(),
        )?;
        assert_eq!(result.inodes, 1);
        assert!(store.get_inode(file_id, None)?.is_none());
        assert!(store.get_inode(missing_id, None)?.is_none());
        assert!(store.get_inode(dir_id, None)?.is_none());
        assert_eq!(store.store.edges_iter(ROOT_INODE_ID)?.count(), 0);

        Ok(())
    }

    #[test]
    fn apply_delete_aggregates_hard_link_counts() -> CommonResult<()> {
        let store = new_store("delete-hard-link-counts")?;
        let root = FsDir::create_root();
        let dir_id = 2001;
        let file_id = 2002;
        let dir = InodeView::new_dir("dir".to_string(), InodeDir::new(dir_id, ROOT_INODE_ID));
        let mut file = InodeView::new_file("file".to_string(), InodeFile::new(file_id, dir_id));
        file.incr_nlink(0);

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.write_inode(&file)?;
            batch.add_child(ROOT_INODE_ID, "dir", dir_id)?;
            batch.add_child(dir_id, "a", file_id)?;
            batch.add_child(dir_id, "b", file_id)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let dir = restored
            .children_handle()
            .expect("restored tree root must be a directory")
            .read_child_view("dir")
            .0
            .expect("restored directory should exist");

        let result = store.apply_delete(
            &restored,
            &dir,
            "dir",
            100,
            true,
            DirectoryAttributeDelta::new(100, 100, -1).for_child_removal(),
        )?;
        assert_eq!(result.inodes, 1);
        assert!(store.get_inode(file_id, None)?.is_none());
        assert!(store.get_inode(dir_id, None)?.is_none());
        assert_eq!(store.store.edges_iter(ROOT_INODE_ID)?.count(), 0);

        Ok(())
    }

    #[test]
    fn create_tree_handles_orphaned_edge() -> CommonResult<()> {
        let store = new_store("orphaned-edge")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("real-dir".to_string(), InodeDir::new(2010, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "real-dir", dir.id())?;
            // Add an orphaned edge: edge points to an inode that does not exist.
            batch.add_child(ROOT_INODE_ID, "orphan", 9999)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let names: Vec<String> = restored
            .children()
            .into_iter()
            .map(|c| c.name().to_string())
            .collect();
        // Only the real directory should be in the tree; orphaned edge is skipped.
        assert_eq!(names, vec!["real-dir"]);

        Ok(())
    }

    #[test]
    fn create_tree_large_tree_counts() -> CommonResult<()> {
        let store = new_store("large-tree-counts")?;
        let root = FsDir::create_root();

        let num_dirs = 100;
        let files_per_dir = 10;
        let mut next_id: i64 = 3000;

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;

            for i in 0..num_dirs {
                let dir_id = next_id;
                next_id += 1;
                let dir_name = format!("d{}", i);
                let dir = InodeView::new_dir(dir_name.clone(), InodeDir::new(dir_id, 0));
                batch.write_inode(&dir)?;
                batch.add_child(ROOT_INODE_ID, &dir_name, dir_id)?;

                for j in 0..files_per_dir {
                    let file_id = next_id;
                    next_id += 1;
                    let file_name = format!("f{}", j);
                    let file = InodeView::new_file(file_name.clone(), InodeFile::new(file_id, 0));
                    batch.write_inode(&file)?;
                    batch.add_child(dir_id, &file_name, file_id)?;
                }
            }
            batch.commit()?;
        }

        let (_, _restored) = store.create_tree()?;
        // get_file_counts() returns (dir_count, file_count)
        let (dir_count, file_count) = store.get_file_counts();

        assert_eq!(file_count, (num_dirs * files_per_dir) as i64);
        assert_eq!(dir_count, num_dirs as i64);

        Ok(())
    }

    #[test]
    fn load_snapshot_data_correctness() -> CommonResult<()> {
        let store = new_store("snapshot-data-correctness")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("subdir".to_string(), InodeDir::new(5001, 0));
        let file = InodeView::new_file("note.txt".to_string(), InodeFile::new(5002, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.write_inode(&file)?;
            batch.add_child(ROOT_INODE_ID, "subdir", dir.id())?;
            batch.add_child(dir.id(), "note.txt", file.id())?;
            batch.commit()?;
        }

        let mut timer = RestoreTimer::new();
        let data = store.load_snapshot_data(&mut timer)?;

        // Verify edges
        assert_eq!(data.edges.len(), 2);
        let root_children = data.edges.get(&ROOT_INODE_ID).expect("root edges");
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].0, "subdir");
        assert_eq!(root_children[0].1, dir.id());

        let dir_children = data.edges.get(&dir.id()).expect("dir edges");
        assert_eq!(dir_children.len(), 1);
        assert_eq!(dir_children[0].0, "note.txt");
        assert_eq!(dir_children[0].1, file.id());

        // Verify inodes
        assert_eq!(data.inodes.len(), 3);
        assert!(data.inodes.contains_key(&ROOT_INODE_ID));
        assert!(data.inodes.contains_key(&dir.id()));
        assert!(data.inodes.contains_key(&file.id()));

        Ok(())
    }

    /// Regression benchmark: 100 dirs × 1000 files = ~100K inodes.
    /// Run with: cargo test -p curvine-server --lib --release -- \
    ///   master::meta::store::inode_store::tests::bench_create_tree_large --ignored --nocapture
    #[test]
    #[ignore]
    fn bench_create_tree_large() -> CommonResult<()> {
        let store = new_store("bench-large")?;
        let root = FsDir::create_root();

        let num_dirs = 100;
        let files_per_dir = 1000;
        let mut next_id: i64 = 100000;

        // Write all inodes and edges in a single batch for speed.
        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;

            for i in 0..num_dirs {
                let dir_id = next_id;
                next_id += 1;
                let dir_name = format!("d{}", i);
                let dir = InodeView::new_dir(dir_name.clone(), InodeDir::new(dir_id, 0));
                batch.write_inode(&dir)?;
                batch.add_child(ROOT_INODE_ID, &dir_name, dir_id)?;

                for j in 0..files_per_dir {
                    let file_id = next_id;
                    next_id += 1;
                    let file_name = format!("f{}", j);
                    let file = InodeView::new_file(file_name.clone(), InodeFile::new(file_id, 0));
                    batch.write_inode(&file)?;
                    batch.add_child(dir_id, &file_name, file_id)?;
                }
            }
            batch.commit()?;
        }

        let total_inodes = 1 + num_dirs + num_dirs * files_per_dir;
        println!(
            "bench: {} inodes ({} dirs × {} files + root)",
            total_inodes, num_dirs, files_per_dir
        );

        let start = std::time::Instant::now();
        let (last_inode_id, _root) = store.create_tree()?;
        let elapsed_ms = start.elapsed().as_millis();

        let (dir_count, file_count) = store.get_file_counts();
        println!(
            "bench: create_tree total={} ms, last_inode_id={}, dir_count={}, file_count={}",
            elapsed_ms, last_inode_id, dir_count, file_count
        );

        assert_eq!(file_count, (num_dirs * files_per_dir) as i64);
        assert_eq!(dir_count, num_dirs as i64);

        // Assert total < 10 seconds for ~100K inodes.
        assert!(
            elapsed_ms < 10_000,
            "create_tree took {} ms (> 10s) for {} inodes",
            elapsed_ms,
            total_inodes
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Snapshot scan lease / restore coordination tests (CV-6 / Task 2)
    // -----------------------------------------------------------------------

    fn seed_two_dirs(store: &InodeStore) -> CommonResult<(i64, i64)> {
        let dir_a_id = 3001;
        let dir_b_id = 3002;
        let file_id = 3003;
        let root = FsDir::create_root();
        let dir_a = InodeView::new_dir("a".to_string(), InodeDir::new(dir_a_id, ROOT_INODE_ID));
        let dir_b = InodeView::new_dir("b".to_string(), InodeDir::new(dir_b_id, ROOT_INODE_ID));
        let file = InodeView::new_file("a.txt".to_string(), InodeFile::new(file_id, dir_a_id));

        let mut batch = store.new_batch();
        batch.write_inode(&root)?;
        batch.write_inode(&dir_a)?;
        batch.write_inode(&dir_b)?;
        batch.write_inode(&file)?;
        batch.add_child(ROOT_INODE_ID, "a", dir_a_id)?;
        batch.add_child(ROOT_INODE_ID, "b", dir_b_id)?;
        batch.add_child(dir_a_id, "a.txt", file_id)?;
        batch.commit()?;
        Ok((dir_a_id, dir_b_id))
    }

    /// Within one lease the typed snapshot readers must observe a single
    /// generation: writes committed after the lease's snapshot was created
    /// are invisible to `scan_inodes`, `scan_edges`, `get_inode` and
    /// `scan_children` alike.
    #[test]
    fn snapshot_scanner_reads_consistent_generation_across_apis() -> CommonResult<()> {
        let store = new_store("snapshot-consistent-generation")?;
        let (dir_a_id, _dir_b_id) = seed_two_dirs(&store)?;

        let lease = store.begin_snapshot_scan()?;
        {
            let reader = lease.snapshot_reader()?;

            // Mutate the store after the snapshot: add a file to dir a.
            let new_file_id = 3004;
            let _root = FsDir::create_root();
            let file = InodeView::new_file(
                "late.txt".to_string(),
                InodeFile::new(new_file_id, dir_a_id),
            );
            {
                let mut batch = store.new_batch();
                batch.write_inode(&file)?;
                batch.add_child(dir_a_id, "late.txt", new_file_id)?;
                batch.commit()?;
            }

            // inodes scan sees only pre-snapshot records.
            let mut inode_ids: Vec<i64> = Vec::new();
            for item in reader.scan_inodes()? {
                let (k, _) = item?;
                inode_ids.push(RocksUtils::i64_from_bytes(&k)?);
            }
            assert!(inode_ids.contains(&ROOT_INODE_ID));
            assert!(inode_ids.contains(&dir_a_id));
            assert!(!inode_ids.contains(&new_file_id));

            // Edges scan sees only pre-snapshot edges.
            let edge_count = reader.scan_edges()?.count();
            assert_eq!(edge_count, 3);

            // Point reads share the same generation.
            assert!(reader.get_inode(new_file_id)?.is_none());
            assert!(reader.get_inode(dir_a_id)?.is_some());

            // Child scan of dir a sees one edge only.
            let children = reader.scan_children(dir_a_id)?.count();
            assert_eq!(children, 1);
        }
        drop(lease);

        // Outside the lease the new data is visible again.
        assert!(store.get_inode(3004, None)?.is_some());

        Ok(())
    }

    /// Concurrent restore while a scan lease is live: restore cancels the
    /// lease, waits for the scanner to drop it and then succeeds. New leases
    /// are rejected as busy while restoring.
    #[test]
    fn scan_restore_coordination_cancels_scan_and_restores() -> CommonResult<()> {
        let store = new_store("scan-restore-cancel")?;
        seed_two_dirs(&store)?;

        // Checkpoint to restore from later.
        let checkpoint = store.create_checkpoint(1)?;
        assert!(std::path::Path::new(&checkpoint).exists());

        // Control handle for polling without touching the store mutex.
        let control = store.scan_control().clone();

        // 1. Acquire a lease and create a snapshot reader (iterator alive).
        let lease = store.begin_snapshot_scan()?;
        let reader = lease.snapshot_reader()?;
        let iter = reader.scan_inodes()?;
        assert!(!lease.is_cancelled());

        // Strong count: store's own Arc + the lease's Arc + the cursor row
        // handle that follows store replacement across restore.
        let strong = Arc::strong_count(&store.store);
        assert_eq!(strong, 3, "lease and cursor handle each hold one extra ref");

        // 2. Restore concurrently; scanner stays idle holding lease+iter.
        let restore_store = Arc::new(parking_lot::Mutex::new(store));
        let restore_thread_store = Arc::clone(&restore_store);
        let handle = std::thread::spawn(move || {
            let mut store = restore_thread_store.lock();
            let guard = store.begin_restore(std::time::Duration::from_secs(30))?;
            store.restore_with_guard(&guard, &checkpoint)
        });

        // 3. Wait until restore marks the control as restoring, then verify
        //    the lease observes cancellation and new leases are rejected.
        while !control.is_restoring() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(lease.is_cancelled());
        assert!(
            control.try_acquire_lease().is_err(),
            "lease must be rejected while restoring"
        );

        // 4. Scanner cooperates: drop iterator/reader/lease, releasing the
        //    store Arc and waking restore.
        drop(iter);
        drop(reader);
        drop(lease);

        let res = handle.join().unwrap();
        assert!(
            res.is_ok(),
            "restore should succeed after lease release: {:?}",
            res
        );

        // 5. New leases work again; the count is back to the store plus the
        // cursor row handle republished by restore.
        assert!(store_begin_scan_ok(&restore_store));
        assert_eq!(store_strong_count(&restore_store), 2);

        Ok(())
    }

    /// Helper: try to acquire a scan lease through the shared store wrapper.
    fn store_begin_scan_ok(store: &parking_lot::Mutex<InodeStore>) -> bool {
        let store = store.lock();
        match store.begin_snapshot_scan() {
            Ok(lease) => {
                drop(lease);
                true
            }
            Err(_) => false,
        }
    }

    /// Strong-count check via the shared store wrapper.
    fn store_strong_count(store: &parking_lot::Mutex<InodeStore>) -> usize {
        Arc::strong_count(&store.lock().store)
    }

    /// Restore timeout path: when the scanner refuses to release its lease in
    /// time, restore aborts BEFORE touching the DB; both the old DB and the
    /// store stay usable.
    #[test]
    fn scan_restore_coordination_timeout_keeps_original_data() -> CommonResult<()> {
        let mut store = new_store("scan-restore-timeout")?;
        seed_two_dirs(&store)?;
        let checkpoint = store.create_checkpoint(1)?;

        let lease = store.begin_snapshot_scan()?;
        let reader = lease.snapshot_reader()?;
        let iter = reader.scan_inodes()?;

        let res = store.begin_restore(std::time::Duration::from_millis(100));
        assert!(
            res.is_err(),
            "restore wait should time out while lease held"
        );

        // Control rolled back: scans can be acquired again.
        assert!(!store.scan_control().is_restoring());
        assert!(store.begin_snapshot_scan().is_ok());

        // Original data untouched.
        assert!(store.get_inode(3001, None)?.is_some());
        assert!(store.get_inode(3003, None)?.is_some());
        let _ = iter.count();
        drop(reader);
        drop(lease);

        // A subsequent restore (with the lease released) succeeds.
        let guard = store.begin_restore(std::time::Duration::from_secs(30))?;
        store.restore_with_guard(&guard, &checkpoint)?;

        Ok(())
    }

    /// Restore must fail cleanly when an extra store reference is held, and
    /// the original data must remain queryable afterwards.
    #[test]
    fn restore_rejects_when_store_arc_is_held() -> CommonResult<()> {
        let mut store = new_store("restore-extra-ref")?;
        seed_two_dirs(&store)?;

        let leaked: Arc<RocksInodeStore> = Arc::clone(&store.store);
        let guard = store.begin_restore(std::time::Duration::from_secs(1))?;
        let res = store.restore_with_guard(&guard, "/nonexistent-checkpoint");
        // restore_with_guard performs the strong-count defensive check.
        assert!(res.is_err());
        // The live store and cursor both retain the original database.
        assert_eq!(Arc::strong_count(&leaked), 3);
        drop(leaked);

        Ok(())
    }
}
