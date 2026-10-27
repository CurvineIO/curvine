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

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::follower_block_report::{
    DeferredBlockReport, FollowerBlockReportState, MAX_DEFERRED_FOLLOWER_REPORTS,
};
use crate::master::fs::full_block_report::{
    FullReportInventory, FullReportProgress, FullReportTracker,
};
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::{InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR};
use crate::master::meta::{
    trash, CacheInvalidationResult, FsDir, RestoredTrashEntry, TrashListing, TrashScanPage,
};

use crate::master::fs::worker_manager::WorkerManager;
use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::*;
use curvine_raft::raft::{FollowerReadRefusal, FollowerReadinessSnapshot, RoleState};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::ArcRwLock;
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// How a delete treats the recycle bin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteMode {
    /// Follow the recycle-bin configuration: soft delete when
    /// `master.trash_enabled` is set, permanent otherwise.
    Default,
    /// Always purge permanently. Used by mounted deletes, cache frees and every
    /// path that already lives under `/.Trash`.
    SkipTrash,
    /// Stage the entry under `/.Trash/.pending` and let the purger reclaim it in
    /// the background, regardless of `master.trash_enabled`. Falls back to a
    /// permanent delete for paths already inside the trash namespace.
    AsyncPurge,
}

pub struct CvMetadataSnapshotEntry {
    pub status: FileStatus,
    pub blocks: Option<FileBlocks>,
}

pub struct CvMetadataSnapshotPage {
    pub entries: Vec<CvMetadataSnapshotEntry>,
    pub next_page_token: Option<String>,
    pub epoch: u64,
}

struct CompleteFileOptions {
    only_flush: bool,
    return_file_blocks: bool,
    set_attr_opts: Option<SetAttrOpts>,
}

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    full_report_tracker: FullReportTracker,
    follower_full_report_tracker: FullReportTracker,
    follower_block_report_state: FollowerBlockReportState,
    follower_block_report_lock: Arc<RwLock<()>>,
    follower_read_contact_timeout_ms: u64,
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
    /// Serialises drains of `/.Trash/.pending`. The blocking empty-trash call
    /// owns the staging directory across staging and draining, and the
    /// background purger skips a cycle while that lock is held, so the two can
    /// never reclaim the same entry and the blocking caller always reports the
    /// totals it actually reclaimed.
    pending_drain: Arc<Mutex<()>>,
}

pub struct BlockReportResult {
    pub delete_blocks: Vec<i64>,
}

#[derive(Default)]
pub struct LostWorkerLocationCleanup {
    pub removed_block_ids: Vec<i64>,
    pub replication_block_ids: Vec<i64>,
}

pub(crate) enum BlockInodeState {
    File,
    Missing,
    NotFile,
}

fn child_snapshot_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn snapshot_token_in_subtree(token: &str, subtree_path: &str) -> bool {
    token == subtree_path
        || (subtree_path != "/"
            && token
                .strip_prefix(subtree_path)
                .map(|rest| rest.starts_with(PATH_SEPARATOR))
                .unwrap_or(false))
}

struct FullBlockReconcileState {
    running: bool,
    generation: u64,
    pending: Option<FullBlockReconcileJob>,
}

struct FullBlockReconcileJob {
    generation: u64,
    reported_blocks: HashSet<i64>,
}

struct FollowerFullBlockReconcileJob {
    worker_id: u32,
    generation: u64,
    term: u64,
    leadership_epoch: u64,
    worker_session_id: String,
    report_id: u64,
    inventory: FullReportInventory,
}

const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;
const FOLLOWER_FULL_REPORT_INVENTORY_LIMIT: usize = FULL_BLOCK_RECONCILE_THREADS;

impl MasterFilesystem {
    // Max block-report location updates applied under a single fs_dir write lock.
    const BLOCK_REPORT_WRITE_CHUNK: usize = 4096;
    // Max lost-worker block ids inspected under a single fs_dir write lock.
    const LOST_WORKER_INVALIDATION_CHUNK: usize = Self::BLOCK_REPORT_WRITE_CHUNK;

    fn validate_alloc_capacity(
        current_len: i64,
        replicas: u8,
        opts: &FileAllocOpts,
        available: i64,
    ) -> FsResult<()> {
        if opts.truncate || opts.len <= current_len {
            return Ok(());
        }

        let logical_growth = opts.len - current_len;
        let required = logical_growth.saturating_mul(i64::from(replicas));
        if required > available {
            return err_ext!(FsError::disk_out_of_space(format!(
                "fallocate requires {} bytes for {} replicas, but only {} bytes are available",
                logical_growth, replicas, available
            )));
        }

        Ok(())
    }

    pub fn new(
        conf: &ClusterConf,
        fs_dir: SyncFsDir,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> Self {
        let follower_read_contact_timeout_ms = conf
            .journal
            .follower_read_contact_timeout_ms()
            .unwrap_or_default();
        Self {
            fs_dir,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            follower_full_report_tracker: FullReportTracker::with_inventory_limit(
                FOLLOWER_FULL_REPORT_INVENTORY_LIMIT,
            ),
            follower_block_report_state: Default::default(),
            follower_block_report_lock: Default::default(),
            follower_read_contact_timeout_ms,
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
            pending_drain: Default::default(),
        }
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        let follower_read_contact_timeout_ms = conf
            .journal
            .follower_read_contact_timeout_ms()
            .unwrap_or_default();
        Self {
            fs_dir: js.fs().fs_dir.clone(),
            worker_manager: js.worker_manager(),
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            follower_full_report_tracker: FullReportTracker::with_inventory_limit(
                FOLLOWER_FULL_REPORT_INVENTORY_LIMIT,
            ),
            follower_block_report_state: Default::default(),
            follower_block_report_lock: Default::default(),
            follower_read_contact_timeout_ms,
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
            pending_drain: Default::default(),
        }
    }

    pub fn check_parent(path: &InodePath) -> FsResult<()> {
        // The root directory must exist.All /a does not require verification
        if path.len() > 2 {
            if let Some(v) = path.get_inode(-2) {
                if !v.is_dir() {
                    err_box!(
                        "Parent path is not a directory:: {}",
                        path.get_parent_path()
                    )
                } else {
                    Ok(())
                }
            } else {
                err_box!("Parent directory doesn't exist: {}", path.get_parent_path())
            }
        } else {
            Ok(())
        }
    }

    pub fn print_tree(&self) {
        let fs_dir = self.fs_dir.read();
        fs_dir.print_tree();
    }

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Self::check_not_reserved_path(inp.path())?;

        // Creation of root directory is not allowed
        if inp.is_root() {
            return err_box!("Not allowed to create existing root path: {}", inp.path());
        }

        if inp.is_full() {
            if opts.create_parent {
                if let Some(last_inode) = inp.get_last_inode() {
                    if last_inode.is_dir() {
                        let status = last_inode.to_file_status(inp.path())?;
                        return Ok(status);
                    }
                }
            }
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Check whether the directory can be created recursively.
        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = fs_dir.mkdir(inp, opts)?;
        let last = try_option!(
            inp.get_last_inode(),
            "Path {} has no inode after mkdir",
            inp.path()
        );
        let status = last.to_file_status(inp.path())?;
        Ok(status)
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    // Permanently delete a path, bypassing the recycle bin.
    // Internal callers (TTL eviction, quota) count the inode's bytes as
    // reclaimed, so this must never route through the recycle bin.
    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        self.delete_with_mode(path, recursive, DeleteMode::SkipTrash)
    }

    pub fn delete_with_mode<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
        mode: DeleteMode,
    ) -> FsResult<DeleteResult> {
        let mut delete_res = {
            let mut fs_dir = self.fs_dir.write();
            self.delete_locked(&mut fs_dir, path.as_ref(), recursive, mode)?
        };
        self.release_deleted_blocks(&mut delete_res);

        Ok(delete_res)
    }

    /// Leader-fenced variant of `delete_with_mode`. The role is re-checked
    /// while holding the same write lock that guards the metadata mutation, so
    /// a demotion cannot interleave between the check and the delete. Returns
    /// `None` when this node is no longer the active master.
    pub fn delete_with_mode_if_active<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
        mode: DeleteMode,
    ) -> FsResult<Option<DeleteResult>> {
        let mut delete_res = {
            let mut fs_dir = self.fs_dir.write();
            if !self.master_monitor.is_active() {
                return Ok(None);
            }
            self.delete_locked(&mut fs_dir, path.as_ref(), recursive, mode)?
        };
        self.release_deleted_blocks(&mut delete_res);

        Ok(Some(delete_res))
    }

    fn delete_locked(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        recursive: bool,
        mode: DeleteMode,
    ) -> FsResult<DeleteResult> {
        let inp = Self::resolve_path(fs_dir, path)?;

        // Removing the root would orphan every trashed entry. Entries below it
        // stay permanently deletable so the recycle bin can be emptied.
        if inp.path() == trash::TRASH_ROOT {
            return err_ext!(FsError::invalid_argument(
                "the recycle bin root cannot be deleted"
            ));
        }

        // Entries already inside /.Trash are always purged permanently, so the
        // bin cannot be used to hide data from a purge.
        let in_trash = trash::is_trash_path(inp.path());
        let soft_delete = match mode {
            DeleteMode::SkipTrash => false,
            DeleteMode::AsyncPurge => !in_trash,
            DeleteMode::Default => self.conf.trash_enabled && !in_trash,
        };

        // The callee reports how the delete was actually served: a shared inode
        // or an unresolved entry falls back to a permanent unlink even when the
        // trash is enabled, and must not claim to be recoverable or staged.
        let delete_res = match mode {
            DeleteMode::AsyncPurge if soft_delete => fs_dir.trash_pending(&inp, recursive)?,
            _ if soft_delete => fs_dir.trash(&inp, recursive)?,
            _ => fs_dir.delete(&inp, recursive)?,
        };
        Ok(delete_res)
    }

    pub(crate) fn release_deleted_blocks(&self, delete_res: &mut DeleteResult) {
        let blocks = std::mem::take(&mut delete_res.blocks);
        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks,
            trashed: false,
            purge_pending: false,
        });
    }

    /// Applies and journals one bounded trash purge batch.
    ///
    /// The batch is applied before it is journaled, like every other `FsDir`
    /// mutation: `JournalWriter::send` may emit a leader snapshot right after
    /// enqueueing the record, and that checkpoint must already reflect the
    /// purge, otherwise recovery from it would skip the batch and resurrect
    /// inodes whose blocks were already released. `purge_paths` commits the
    /// whole batch atomically, so a failed apply journals nothing. Returns
    /// `None` when this node is no longer the active master.
    pub(crate) fn purge_trash_batch(
        &self,
        entry: &str,
        paths: &[String],
    ) -> FsResult<Option<(u64, u64)>> {
        let mut delete_res = {
            let mut fs_dir = self.fs_dir.write();
            if !self.master_monitor.is_active() {
                return Ok(None);
            }

            let mtime = LocalTime::mills() as i64;
            let delete_res = fs_dir.purge_paths(paths, mtime)?;

            let writer = fs_dir.journal_writer.clone();
            writer.log_trash_purge(&fs_dir, entry, mtime, paths.to_vec())?;
            delete_res
        };

        self.release_deleted_blocks(&mut delete_res);
        Ok(Some((delete_res.inodes, delete_res.bytes)))
    }

    pub fn list_trash_page(
        &self,
        after: Option<&trash::TrashCursor>,
        limit: usize,
    ) -> FsResult<TrashListing> {
        let fs_dir = self.fs_dir.read();
        fs_dir.list_trash(after, limit)
    }

    pub fn list_trash(&self) -> FsResult<Vec<TrashEntry>> {
        const PAGE_SIZE: usize = 1000;
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_trash_page(cursor.as_ref(), PAGE_SIZE)?;
            entries.extend(page.entries);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(entries),
            }
        }
    }

    pub fn restore_dst<T: AsRef<str>>(&self, path: T, dst: Option<&str>) -> FsResult<String> {
        let fs_dir = self.fs_dir.read();
        fs_dir.restore_dst(path.as_ref(), dst)
    }

    /// Returns one bounded page of expired trash entry paths. The namespace
    /// read lock is held only while scanning that page.
    pub fn scan_expired_trash(
        &self,
        cutoff_ms: i64,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let fs_dir = self.fs_dir.read();
        fs_dir.scan_expired_trash(cutoff_ms, limit, start_after)
    }

    /// Leader-fenced [`FsDir::claim_trash_entry_for_purge`]. Returns
    /// `Ok(None)` when this node is no longer active and `Ok(Some(None))` when
    /// the entry already vanished.
    pub fn claim_trash_entry_for_purge(
        &self,
        trash_path: &str,
    ) -> FsResult<Option<Option<String>>> {
        let mut fs_dir = self.fs_dir.write();
        if !self.master_monitor.is_active() {
            return Ok(None);
        }
        fs_dir.claim_trash_entry_for_purge(trash_path).map(Some)
    }

    /// One bounded page of `/.Trash/.pending` entries, i.e. entries staged by
    /// `async_delete` that are due for immediate reclamation.
    pub fn scan_pending_trash(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let fs_dir = self.fs_dir.read();
        fs_dir.scan_pending_trash(limit, start_after)
    }

    pub fn restore_trash<T: AsRef<str>>(
        &self,
        path: T,
        dst: Option<&str>,
        overwrite: bool,
    ) -> FsResult<RestoredTrashEntry> {
        let mut fs_dir = self.fs_dir.write();
        let restored = fs_dir.restore_trash(path.as_ref(), dst, overwrite)?;
        drop(fs_dir);

        // Overwriting an existing destination deletes it; release its blocks
        // just like a regular rename-with-replace.
        if let Some(replaced) = &restored.replaced {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(replaced);
        }

        Ok(restored)
    }

    /// Stages every visible recycle-bin entry for the background purger and
    /// returns how many were accepted. Entries are renamed, not walked, so the
    /// call does not depend on the size of the trashed subtrees.
    ///
    /// Leadership is re-checked under each batch's write lock: a demotion while
    /// the loop is between batches must stop the staging instead of letting a
    /// former leader persist further renames and journal records.
    pub fn stage_trash_for_purge(&self) -> FsResult<u64> {
        const BATCH_SIZE: usize = 1000;

        // Snapshot the names once: entries added after the snapshot stay for the
        // next empty, and each batch releases the write lock between renames.
        let names = self.fs_dir.read().trash_entry_names()?;
        let mut staged = 0;
        for batch in names.chunks(BATCH_SIZE) {
            let mut fs_dir = self.fs_dir.write();
            if !self.master_monitor.is_active() {
                return err_ext!(FsError::not_leader(
                    "empty-trash staging lost leadership mid-sweep"
                ));
            }
            staged += fs_dir.stage_trash_entries(batch)?;
        }
        Ok(staged)
    }

    /// Takes ownership of `/.Trash/.pending` for a blocking drain.
    pub(crate) fn lock_pending_drain(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.pending_drain.lock()
    }

    /// Non-blocking variant used by the background purger, which must never
    /// queue behind a blocking empty-trash sweep.
    pub(crate) fn try_lock_pending_drain(&self) -> Option<parking_lot::MutexGuard<'_, ()>> {
        self.pending_drain.try_lock()
    }

    /// Drains `/.Trash/.pending` through the bounded purger until it is empty.
    /// Returns the real reclaimed counts; `blocks` are already handed to the
    /// worker managers by the purge batches themselves.
    pub fn drain_pending_trash(&self) -> FsResult<Option<DeleteResult>> {
        let _guard = self.lock_pending_drain();
        self.drain_pending_trash_owned()
    }

    /// Drain body that assumes the caller already owns the pending directory.
    fn drain_pending_trash_owned(&self) -> FsResult<Option<DeleteResult>> {
        const BATCH_SIZE: usize = 64;
        let purger = crate::master::fs::TrashPurger::new(self.clone(), self.master_monitor.clone());
        let mut total = DeleteResult::new();
        let mut start_after = None;

        loop {
            let page = self.scan_pending_trash(BATCH_SIZE, start_after.as_deref())?;
            for path in page.paths {
                let Some(outcome) = purger.purge_entry(&path)? else {
                    return Ok(None);
                };
                total.inodes += outcome.inodes;
                total.bytes += outcome.bytes;
            }
            match page.next_after {
                Some(next) => start_after = Some(next),
                None => return Ok(Some(total)),
            }
        }
    }

    /// Empties the recycle bin. Returns the number of accepted entries and, in
    /// wait mode, the counts actually reclaimed by the bounded purger.
    ///
    /// Both modes stage the entries first, so there is exactly one purge
    /// implementation and an interrupted sweep resumes from `/.Trash/.pending`
    /// instead of from an in-memory queue.
    pub fn empty_trash0(&self, wait: bool) -> FsResult<(u64, DeleteResult)> {
        if !wait {
            let accepted = self.stage_trash_for_purge()?;
            return Ok((accepted, DeleteResult::new()));
        }

        // Hold the drain lock across staging and draining: the background
        // purger must not reclaim an entry between the rename that stages it and
        // the drain that has to report its totals.
        let _guard = self.lock_pending_drain();
        let accepted = self.stage_trash_for_purge()?;
        match self.drain_pending_trash_owned()? {
            Some(res) => Ok((accepted, res)),
            // The master was demoted mid-sweep. The staged entries stay in the
            // durable pending directory for the next active master, but this
            // call cannot report the blocking totals the caller waits for, so it
            // must fail retryably instead of caching a success with zero counts.
            None => err_ext!(FsError::not_leader(
                "empty-trash lost leadership before the staged entries were reclaimed"
            )),
        }
    }

    /// Empties the recycle bin synchronously, keeping the pre-async contract.
    pub fn empty_trash(&self) -> FsResult<DeleteResult> {
        let (_, res) = self.empty_trash0(true)?;
        Ok(res)
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Self::check_not_reserved_path(inp.path())?;

        let mut free_res = fs_dir.free(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut free_res.blocks),
            trashed: false,
            purge_pending: false,
        });

        Ok(free_res)
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        let mut fs_dir = self.fs_dir.write();
        let src_inp = Self::resolve_path(&fs_dir, src)?;
        let dst_inp = Self::resolve_path(&fs_dir, dst)?;

        Self::check_not_reserved_path(src_inp.path())?;
        Self::check_not_reserved_path(dst_inp.path())?;

        if src_inp.is_root() {
            return err_box!("Cannot rename root path");
        }

        if src == dst {
            return Ok(false);
        }

        // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed (POSIX EINVAL).
        if let Some(rest) = dst.strip_prefix(src) {
            if rest.starts_with(PATH_SEPARATOR) {
                return err_ext!(FsError::invalid_argument(format!(
                    "cannot rename {} to {}: destination is under source",
                    src, dst
                )));
            }
        }

        // EXCHANGE also rejects src under dst (/a/b <-> /a would make /a its own descendant).
        if flags.exchange_mode() {
            if let Some(rest) = src.strip_prefix(dst) {
                if rest.starts_with(PATH_SEPARATOR) {
                    return err_ext!(FsError::invalid_argument(format!(
                        "cannot exchange {} with {}: source is under destination",
                        src, dst
                    )));
                }
            }
        }

        if let Some(del_res) = fs_dir.rename(&src_inp, &dst_inp, flags)? {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&del_res);
        }

        Ok(true)
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx, OpenFlags::new_create().set_overwrite(true))
    }

    fn truncate(&self, fs_dir: &mut FsDir, inp: &InodePath, opts: CreateFileOpts) -> FsResult<()> {
        let clean_result = fs_dir.overwrite_file(inp, opts)?;
        if !clean_result.blocks.is_empty() {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&clean_result);
        }
        Ok(())
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if !flags.create() {
            return err_box!("create_with_opts requires O_CREAT flag");
        }
        let path = path.as_ref();

        // Check the path length
        self.check_path_length(path)?;

        if opts.replicas < self.conf.min_replication || opts.replicas >= self.conf.max_replication {
            return err_box!(
                "The replica number {} needs to be between {} and {}",
                opts.replicas,
                self.conf.min_replication,
                self.conf.max_replication
            );
        }

        if opts.block_size < self.conf.min_block_size || opts.block_size >= self.conf.max_block_size
        {
            return err_box!(
                "Block size needs to be between {} and {}",
                self.conf.min_block_size,
                self.conf.max_block_size
            );
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;
        Self::check_not_reserved_path(inp.path())?;

        let last_inode = inp.get_last_inode();
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }

            if flags.exclusive() {
                return err_ext!(FsError::file_exists(inp.path()));
            }
        }

        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = if last_inode.is_some() {
            if flags.overwrite() {
                self.truncate(&mut fs_dir, &inp, opts)?;
            } else {
                return err_ext!(FsError::file_exists(inp.path()));
            }
            inp
        } else {
            fs_dir.create_file(inp, opts)?
        };

        let status = fs_dir.file_status(&inp)?;

        Ok(status)
    }

    pub fn open_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();

        if flags.read_only() {
            if flags.truncate() {
                return err_box!("cannot combine O_RDONLY with O_TRUNC");
            }
            if flags.create() {
                return err_box!("cannot combine O_RDONLY with O_CREAT");
            }
            return self.get_block_locations(path);
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;
        Self::check_not_reserved_path(inp.path())?;

        let inode = match inp.get_last_inode() {
            None => {
                return if flags.create() {
                    drop(fs_dir);
                    let status = self.create_with_opts(path, opts, flags)?;
                    Ok(FileBlocks::new(status, vec![]))
                } else {
                    err_ext!(FsError::file_not_found(inp.path()))
                }
            }

            Some(inode) => {
                if inode.is_dir() {
                    return err_box!("{} is a directory", inp.path());
                }
                inode
            }
        };

        if flags.truncate() {
            self.truncate(&mut fs_dir, &inp, opts)?;
            let status = fs_dir.file_status(&inp)?;
            return Ok(FileBlocks::new(status, vec![]));
        }

        let status = fs_dir.reopen_file(&inp, opts.client_name)?;
        let file = inode.as_file_ref()?;
        let blocks = if !file.blocks.is_empty() {
            self.get_block_locs(path, &fs_dir, file)?
        } else {
            vec![]
        };
        Ok(FileBlocks::new(status, blocks))
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        self.file_status_by_id(path, None)
    }

    pub fn file_status_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
    ) -> FsResult<FileStatus> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        if inode_id.is_none_or(|id| id <= 0) {
            let inp = Self::resolve_path(&fs_dir, path)?;
            return match inp.get_last_inode() {
                Some(inode) => Ok(inode.to_file_status(path)?),
                None => err_ext!(FsError::file_not_found(path)),
            };
        }
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        Ok(inode.to_file_status(path)?)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path_raw(&fs_dir, path.as_ref())?;
        if trash::is_pending_path(inp.path()) {
            return Ok(false);
        }
        Ok(inp.get_last_inode().is_some())
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path.as_ref());
        if is_glob_pattern {
            let paths = Self::resolve_path_by_glob_pattern(&fs_dir, path.as_ref())?;
            let mut all_statuses = Vec::new();
            for path in &paths {
                if trash::is_pending_path(path.path()) {
                    continue;
                }
                let statuses = fs_dir.list_status(path)?;
                all_statuses.extend(Self::hide_pending(path.path(), statuses));
            }
            Ok(all_statuses)
        } else {
            let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
            let statuses = fs_dir.list_status(&inp)?;
            Ok(Self::hide_pending(inp.path(), statuses))
        }
    }

    pub fn list_options<T: AsRef<str>>(
        &self,
        path: T,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            err_box!("list_options does not support glob pattern, path {}", path)
        } else {
            let inp = Self::resolve_path(&fs_dir, path)?;
            let statuses = fs_dir.list_options(&inp, &opts)?;
            Ok(Self::hide_pending(inp.path(), statuses))
        }
    }

    /// Resolves a client-supplied path. `/.Trash/.pending` is master-owned
    /// staging for async deletes and reclamation, so it resolves as missing:
    /// clients can neither stat, list, open, rename nor synchronously delete it.
    /// Internal reclamation goes through `FsDir` directly and is not affected.
    fn resolve_path(fs_dir: &FsDir, path: &str) -> FsResult<InodePath> {
        let inp = Self::resolve_path_raw(fs_dir, path)?;
        Self::check_not_pending_path(inp.path())?;
        Ok(inp)
    }

    fn resolve_path_raw(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn check_not_pending_path(path: &str) -> FsResult<()> {
        if trash::is_pending_path(path) {
            return err_ext!(FsError::file_not_found(path));
        }
        Ok(())
    }

    /// Drops the `.pending` staging directory from a listing of `/.Trash`.
    fn hide_pending(dir_path: &str, mut statuses: Vec<FileStatus>) -> Vec<FileStatus> {
        if dir_path == trash::TRASH_ROOT {
            statuses.retain(|status| status.name != trash::TRASH_PENDING_NAME);
        }
        statuses
    }

    fn check_not_reserved_path(path: &str) -> FsResult<()> {
        if trash::is_reserved_path(path) {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is reserved for the recycle bin",
                path
            )));
        }
        Ok(())
    }

    // NUL-prefixed keys are master-owned (ctime, trash markers) and never
    // surface in FileStatus, so a client cannot legitimately send them. The
    // check itself lives on `FsDir` so every client-facing writer shares it.
    fn check_not_internal_x_attr(opts: &SetAttrOpts) -> FsResult<()> {
        FsDir::check_not_internal_x_attr(opts)
    }

    fn resolve_path_by_glob_pattern(fs_dir: &FsDir, path: &str) -> CommonResult<Vec<InodePath>> {
        InodePath::resolve_for_glob_pattern(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    pub fn check_path_length(&self, path: &str) -> CommonResult<()> {
        if path.len() > self.conf.max_path_len {
            return err_box!(
                "create: Path too long, limit {} characters",
                self.conf.max_path_len
            );
        }

        let depth = path.split(PATH_SEPARATOR).count();
        if depth > self.conf.max_path_depth {
            return err_box!(
                "create: Path too long, limit {} levels",
                self.conf.max_path_depth
            );
        }

        Ok(())
    }

    pub fn validate_add_block(
        file: &InodeFile,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        if let Some(v) = previous {
            if v.block_len != file.block_size as i64 {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas as u16,
            block_size: file.block_size as i64,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn choose_worker(
        &self,
        inp: &InodePath,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;
        self.choose_worker_for_file(file, client_addr, exclude_workers)
    }

    pub fn choose_worker_for_file(
        &self,
        file: &InodeFile,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut wm = self.worker_manager.write();
        let validate_block = Self::validate_add_block(file, &client_addr, None)?;
        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);
        Ok(wm.choose_worker(choose_ctx)?)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        self.worker_manager
            .read()
            .create_locate_block(path, block, locs)
    }

    pub fn resolve_file_inode(
        fs_dir: &FsDir,
        path: &str,
        inode_id: Option<i64>,
    ) -> FsResult<InodePtr> {
        match inode_id {
            Some(v) if v > 0 => match fs_dir.store.get_inode(v, None)? {
                Some(view) => Ok(InodePtr::from_owned(view)),
                None => err_ext!(FsError::file_not_found(path).ctx(format!("inode_id={}", v))),
            },

            _ => {
                // Raw resolution: journal replay addresses `.pending` paths.
                let inp = Self::resolve_path_raw(fs_dir, path)?;
                match inp.task_last() {
                    Some(ptr) => Ok(ptr),
                    None => err_ext!(FsError::file_not_found(path)),
                }
            }
        }
    }

    /// Document application to allocate a new block.
    #[allow(clippy::too_many_arguments)]
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        let file = inode.as_file_ref()?;

        // File allows concurrent writes, 'previous' is the previous block,
        // need to check if the next block has already been allocated。
        // If it has been allocated, return that block
        if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
            let locs = fs_dir.get_block_locations(next.id)?;
            let extend_block = ExtendedBlock {
                id: next.id,
                len: next.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: next.alloc_opts.clone(),
            };

            return self.create_locate_block(path, extend_block, &locs);
        }

        let block_size = file.block_size as i64;
        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block =
            match fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len) {
                Ok(block) => block,
                Err(e) => {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                    return Err(e);
                }
            };
        let located = LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        };

        Ok(located)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush,
                return_file_blocks: true,
                set_attr_opts,
            },
        )
    }

    /// Flushes file metadata without building the full block-location snapshot.
    pub fn flush_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush: true,
                return_file_blocks: false,
                set_attr_opts: None,
            },
        )
        .map(|_| ())
    }

    fn complete_file0<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        fs_dir.complete_file(
            path,
            &mut inode,
            len,
            commit_blocks,
            client_name,
            options.only_flush,
            options.set_attr_opts,
        )?;

        if options.only_flush && options.return_file_blocks {
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            let status = inode.to_file_status(path)?;
            return Ok(Some(FileBlocks::new(status, locs)));
        }

        Ok(None)
    }

    pub fn get_file_blocks(
        &self,
        path: &str,
        fs_dir: &FsDir,
        inp: &InodePath,
    ) -> FsResult<FileBlocks> {
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        let file = inode.as_file_ref()?;
        let blocks = self.get_block_locs(path, fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let file_locs = fs_dir.get_file_locations(file)?;
        // Location reads only need the caller's namespace guard. Acquire worker
        // state afterwards so RocksDB lookups do not delay heartbeats.
        let wm = self.worker_manager.read();
        Self::get_block_locs_from_locations(path, file, &file_locs, &wm)
    }

    fn get_block_locs_from_locations(
        path: &str,
        file: &InodeFile,
        file_locs: &HashMap<i64, Vec<BlockLocation>>,
        wm: &WorkerManager,
    ) -> FsResult<Vec<LocatedBlock>> {
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let lc = try_option!(
                file_locs.get(&meta.id),
                "File {}, block {} Lost (no worker can read)",
                path,
                meta.id
            );
            let lb = wm.create_locate_block(path, extend_block, lc)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        let fs_dir = self.fs_dir.read();
        let path = path.as_ref();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(path)),
        };
        let file = inode.as_file_ref()?;
        let block_locs = self.get_block_locs(path, &fs_dir, file)?;
        let locate_blocks = FileBlocks {
            status: inode.to_file_status(path)?,
            block_locs,
        };

        Ok(locate_blocks)
    }

    pub fn get_file_block_details<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlockDetails> {
        let path = path.as_ref();

        let (status, block_snapshots, worker_snapshots) = {
            let fs_dir = self.fs_dir.read();
            let inp = Self::resolve_path(&fs_dir, path)?;
            let inode = match inp.get_last_inode() {
                Some(inode) => inode,
                None => return err_ext!(FsError::file_not_found(path)),
            };
            let file = inode.as_file_ref()?;
            let file_locs = fs_dir.get_file_locations(file)?;
            let status = inode.to_file_status(path)?;
            let mut offset = 0;
            let mut block_snapshots = Vec::with_capacity(file.blocks.len());
            let mut worker_ids = HashSet::new();

            for meta in &file.blocks {
                let len = meta.len();
                let replicas = file_locs.get(&meta.id).cloned().unwrap_or_default();
                for replica in &replicas {
                    worker_ids.insert(replica.worker_id);
                }
                block_snapshots.push((meta.id, len, offset, replicas));
                offset += len;
            }

            // Snapshot worker state under the existing fs_dir -> worker_manager lock order.
            let worker_snapshots = {
                let worker_manager = self.worker_manager.read();
                worker_ids
                    .into_iter()
                    .map(|worker_id| {
                        let (address, state) = if let Some(worker) =
                            worker_manager.get_worker(worker_id)
                        {
                            (Some(worker.address.clone()), worker.status)
                        } else if let Some(worker) = worker_manager.get_known_worker(worker_id) {
                            (Some(worker.address.clone()), WorkerStatus::Lost)
                        } else {
                            (None, WorkerStatus::Unknown)
                        };
                        (worker_id, (address, state))
                    })
                    .collect::<HashMap<_, _>>()
            };

            (status, block_snapshots, worker_snapshots)
        };

        let blocks = block_snapshots
            .into_iter()
            .map(|(block_id, len, offset, locations)| {
                let mut replicas = locations
                    .into_iter()
                    .map(|location| BlockReplicaDetail {
                        worker_id: location.worker_id,
                        storage_type: location.storage_type,
                        address: worker_snapshots
                            .get(&location.worker_id)
                            .and_then(|(address, _)| address.clone()),
                        state: worker_snapshots
                            .get(&location.worker_id)
                            .map(|(_, state)| *state)
                            .unwrap_or(WorkerStatus::Unknown),
                    })
                    .collect::<Vec<_>>();
                replicas.sort_by_key(|replica| replica.worker_id);

                FileBlockDetail {
                    block_id,
                    len,
                    offset,
                    replicas,
                }
            })
            .collect();

        Ok(FileBlockDetails { status, blocks })
    }

    pub fn cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataSnapshotPage> {
        if page_size == 0 {
            return err_box!("cv metadata snapshot page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let epoch = fs_dir.op_id.get();
        let start_after = page_token.filter(|token| !token.is_empty());
        let mut entries = Vec::with_capacity(page_size.saturating_add(1));
        self.collect_cv_metadata_snapshot_page(
            &fs_dir,
            fs_dir.root_dir(),
            "/",
            start_after.as_deref(),
            page_size.saturating_add(1),
            &mut entries,
        )?;

        let next_page_token = if entries.len() > page_size {
            let token = entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.status.path.clone());
            entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataSnapshotPage {
            entries,
            next_page_token,
            epoch,
        })
    }

    fn collect_cv_metadata_snapshot_page(
        &self,
        fs_dir: &FsDir,
        inode: &InodeView,
        path: &str,
        start_after: Option<&str>,
        limit: usize,
        entries: &mut Vec<CvMetadataSnapshotEntry>,
    ) -> FsResult<()> {
        if entries.len() >= limit {
            return Ok(());
        }
        if let Some(token) = start_after {
            if path != "/" && token > path && !snapshot_token_in_subtree(token, path) {
                return Ok(());
            }
        }

        let resolved = self.resolve_snapshot_inode(fs_dir, inode)?;
        if start_after.map(|token| path > token).unwrap_or(true) {
            let status = resolved.to_file_status(path)?;
            let blocks = if let Ok(file) = resolved.as_file_ref() {
                Some(FileBlocks::new(
                    status.clone(),
                    self.get_block_locs(path, fs_dir, file)?,
                ))
            } else {
                None
            };
            entries.push(CvMetadataSnapshotEntry { status, blocks });
            if entries.len() >= limit {
                return Ok(());
            }
        }

        if let InodeView::Dir(dir) = resolved.as_ref() {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_snapshot_page(
                    fs_dir,
                    child,
                    &child_path,
                    start_after,
                    limit,
                    entries,
                )?;
                if entries.len() >= limit {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Directory children live only in the in-memory tree. Borrow that node.
    /// `FileEntry` stores a name and id, so load the file inode for its blocks
    /// and hard-link name. The caller holds the namespace read lock for the walk.
    fn resolve_snapshot_inode<'a>(
        &self,
        fs_dir: &FsDir,
        inode: &'a InodeView,
    ) -> FsResult<Cow<'a, InodeView>> {
        if let InodeView::FileEntry(entry) = inode {
            let loaded = fs_dir
                .store
                .get_inode(entry.id, Some(&entry.name))?
                .ok_or_else(|| FsError::file_not_found(entry.name.clone()))?;
            return Ok(Cow::Owned(loaded));
        }
        Ok(Cow::Borrowed(inode))
    }

    pub fn filesystem_info(&self) -> FsResult<FilesystemInfo> {
        let metrics = Master::get_metrics()?;
        let mut info = FilesystemInfo {
            inode_dir_num: metrics.inode_dir_num.get(),
            inode_file_num: metrics.inode_file_num.get(),
            ..Default::default()
        };

        let wm = self.worker_manager.read();

        // Requests can only reach active master
        info.active_master = wm.conf.master_addr().to_string();
        for peer in &wm.conf.journal.journal_addrs {
            info.journal_nodes.push(peer.to_string())
        }

        for (_, worker) in wm.worker_map.workers() {
            info.capacity += worker.capacity;
            info.available += worker.available;
            info.fs_used += worker.fs_used;
            info.non_fs_used += worker.non_fs_used;
            info.reserved_bytes += worker.reserved_bytes;
            info.block_num += worker.block_num;

            match worker.status {
                WorkerStatus::Live => {
                    info.live_workers.push(worker.clone());
                    // Only Live workers are eligible for new allocations, so the
                    // allocatable view mirrors the allocation policy. Failed
                    // storage dirs are already excluded from worker.capacity.
                    info.allocatable_capacity += worker.capacity;
                    info.allocatable_available += worker.allocatable_available().max(0);
                }
                WorkerStatus::Blacklist => info.blacklist_workers.push(worker.clone()),
                WorkerStatus::Decommission => info.decommission_workers.push(worker.clone()),
                _ => (),
            }
        }

        for (_, worker) in wm.worker_map.lost_workers() {
            info.lost_workers.push(worker.clone());
        }

        Ok(info)
    }

    pub fn fs_dir(&self) -> ArcRwLock<FsDir> {
        self.fs_dir.clone()
    }

    // Add a test worker and unit tests will use it.
    pub fn add_test_worker(&self, worker: WorkerInfo) {
        let mut wm = self.worker_manager.write();
        wm.add_test_worker(worker);
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let fs_dir = self.fs_dir.read();
        fs_dir.sum_hash()
    }

    pub fn last_inode_id(&self) -> i64 {
        let fs_dir = self.fs_dir.read();
        fs_dir.last_inode_id()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        let fs_dir = self.fs_dir.read();
        fs_dir.get_file_counts()
    }

    // Create a directory number based on rocksdb data for testing.
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        let fs_dir = self.fs_dir.read();
        fs_dir.create_tree()
    }

    // Restore in-memory tree from RocksDB (for testing without Raft).
    // In production, Raft automatically restores via apply_snapshot().
    pub fn restore_from_rocksdb(&self) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore_from_rocksdb()
    }

    fn block_inode_state(&self, id: i64) -> FsResult<BlockInodeState> {
        let fs_dir = self.fs_dir.read();
        fs_dir.block_inode_state(id)
    }

    fn follower_worker_state_enabled(&self) -> bool {
        self.conf.follower_worker_state_enabled || self.conf.follower_reads_enabled
    }

    /// Evaluate the configured follower-read freshness policy against one
    /// immutable Raft/journal observation.
    pub fn follower_read_refusal(
        &self,
        snapshot: &FollowerReadinessSnapshot,
    ) -> Option<FollowerReadRefusal> {
        snapshot.refusal_reason(
            Duration::from_millis(self.follower_read_contact_timeout_ms),
            self.conf.follower_read_max_commit_lag,
            Duration::from_millis(self.conf.follower_read_apply_stall_timeout_ms()),
        )
    }

    /// Caller holds the write side of `follower_block_report_lock`.
    fn reset_passive_view_for_current_epoch(&self) {
        let snapshot = self.master_monitor.follower_readiness_snapshot();
        if snapshot.role != curvine_raft::raft::RoleState::Follower {
            return;
        }
        for worker_id in self
            .follower_block_report_state
            .reset_for_epoch(snapshot.term, snapshot.leadership_epoch)
        {
            self.follower_full_report_tracker.reset(worker_id);
        }
    }

    /// Record the registration state needed by follower block-location reads.
    /// This state is local and is reset whenever the worker starts a new session.
    #[cfg(test)]
    fn observe_passive_worker_heartbeat(&self, status: HeartbeatStatus, worker_id: u32) {
        let _serial = self.follower_block_report_lock.write();
        if self.master_monitor.is_active()
            || self.master_monitor.follower_readiness_snapshot().role
                != curvine_raft::raft::RoleState::Follower
        {
            return;
        }
        self.reset_passive_view_for_current_epoch();
        let worker_manager = self.worker_manager.read();
        self.observe_passive_worker_heartbeat_locked(status, worker_id, &worker_manager);
    }

    fn observe_passive_worker_heartbeat_locked(
        &self,
        status: HeartbeatStatus,
        worker_id: u32,
        worker_manager: &WorkerManager,
    ) {
        let mut retained_workers: HashSet<u32> = worker_manager
            .get_last_heartbeat()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        if status == HeartbeatStatus::Start {
            // A provisional Start may not yet be visible in WorkerManager,
            // but its new session still needs a full report.
            retained_workers.insert(worker_id);
        }
        for stale_worker in self
            .follower_block_report_state
            .retain_workers(&retained_workers)
        {
            self.follower_full_report_tracker.reset(stale_worker);
        }
        match status {
            HeartbeatStatus::Start => {
                self.follower_full_report_tracker.reset(worker_id);
                self.follower_block_report_state
                    .observe_worker(worker_id, true);
            }
            HeartbeatStatus::Running => {
                self.follower_block_report_state
                    .observe_worker(worker_id, false);
            }
            HeartbeatStatus::End => {
                self.follower_full_report_tracker.reset(worker_id);
                self.follower_block_report_state.forget_worker(worker_id);
            }
        }
    }

    /// Apply a passive worker heartbeat while holding the follower report lock
    /// across registration validation, readiness invalidation, and the worker
    /// map update. Reports and location reads take the same lock before the
    /// worker map, so an old session cannot restore readiness between a Start
    /// reset and installation of the new session.
    pub(crate) fn process_passive_worker_heartbeat<V, U>(
        &self,
        status: HeartbeatStatus,
        worker_id: u32,
        validate: V,
        update: U,
    ) -> FsResult<()>
    where
        V: FnOnce(&WorkerManager) -> FsResult<()>,
        U: FnOnce(&mut WorkerManager) -> FsResult<()>,
    {
        let _serial = self.follower_block_report_lock.write();
        let initial_snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active() || initial_snapshot.role != RoleState::Follower {
            return err_box!("Passive worker heartbeats are not accepted by the active master");
        }
        self.reset_passive_view_for_current_epoch();

        let mut worker_manager = self.worker_manager.write();
        let current_snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active()
            || current_snapshot.role != RoleState::Follower
            || current_snapshot.leadership_epoch != initial_snapshot.leadership_epoch
        {
            return err_box!("Passive worker heartbeat crossed a leadership epoch");
        }
        validate(&worker_manager)?;
        if status == HeartbeatStatus::Start {
            // Revoke readiness after validation but before publishing the new
            // worker session. The report lock keeps reports from observing the
            // old map between these operations.
            self.observe_passive_worker_heartbeat_locked(status, worker_id, &worker_manager);
        }
        update(&mut worker_manager)?;
        let current_snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active()
            || current_snapshot.role != RoleState::Follower
            || current_snapshot.leadership_epoch != initial_snapshot.leadership_epoch
        {
            return err_box!("Passive worker heartbeat crossed a leadership epoch");
        }
        if status != HeartbeatStatus::Start {
            self.observe_passive_worker_heartbeat_locked(status, worker_id, &worker_manager);
        }
        drop(worker_manager);

        if status == HeartbeatStatus::Running
            && self
                .follower_block_report_state
                .requires_full_report(worker_id)
            && !self
                .follower_block_report_state
                .reconcile_pending(worker_id)
            && !self
                .follower_full_report_tracker
                .has_open_session(worker_id, LocalTime::mills())
        {
            return Err(FsError::in_progress_msg(format!(
                "ResyncRequired: worker {} must send a full report to refresh follower state",
                worker_id
            )));
        }

        Ok(())
    }

    /// Forget a worker's passive state after local heartbeat expiry. No block
    /// rows are removed here; only the active leader may run lost-worker cleanup.
    pub fn forget_passive_worker(&self, worker_id: u32) {
        let _serial = self.follower_block_report_lock.write();
        if self.master_monitor.is_active()
            || self.master_monitor.follower_readiness_snapshot().role
                != curvine_raft::raft::RoleState::Follower
        {
            return;
        }
        self.reset_passive_view_for_current_epoch();
        self.follower_full_report_tracker.reset(worker_id);
        self.follower_block_report_state.forget_worker(worker_id);
    }

    fn follower_file_locations_ready(
        &self,
        file: &InodeFile,
        file_locations: &HashMap<i64, Vec<BlockLocation>>,
        worker_manager: &WorkerManager,
    ) -> Option<HashMap<i64, Vec<BlockLocation>>> {
        if file.blocks.is_empty() {
            // Empty files and UFS-only files legitimately have no Curvine
            // block locations. A nonempty Curvine-backed file does not.
            return (file.len == 0 || file.storage_policy.ufs_only()).then(HashMap::new);
        }

        let now_ms = LocalTime::mills();
        let max_worker_age_ms = self.conf.worker_blacklist_interval_ms();
        self.follower_block_report_state
            .with_readiness(|readiness| {
                let mut safe_locations = HashMap::with_capacity(file.blocks.len());
                for block in &file.blocks {
                    if readiness.has_deferred_block(block.id) {
                        return None;
                    }
                    let locations = file_locations.get(&block.id)?;
                    let usable = locations
                        .iter()
                        .filter(|location| {
                            readiness.worker_ready(location.worker_id)
                                && worker_manager.get_worker(location.worker_id).is_some_and(
                                    |worker| {
                                        worker.is_live()
                                            && now_ms.saturating_sub(worker.last_update)
                                                <= max_worker_age_ms
                                    },
                                )
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if usable.is_empty() {
                        return None;
                    }
                    safe_locations.insert(block.id, usable);
                }
                Some(safe_locations)
            })
    }

    /// Return a block-location snapshot only when both the Raft/journal state
    /// and at least one current, report-ready replica per block are available.
    /// The follower report read lock, namespace guard, and worker-state guard
    /// remain held through construction and the final epoch check. A report
    /// writer makes this method fail fast with `None`, allowing RPC fallback
    /// instead of queueing behind a potentially long report scan.
    pub fn follower_get_block_locations_if_ready<T: AsRef<str>>(
        &self,
        path: T,
    ) -> FsResult<Option<FileBlocks>> {
        if !self.follower_worker_state_enabled() {
            return Ok(None);
        }

        let Some(mut _serial) = self.follower_block_report_lock.try_read() else {
            return Ok(None);
        };
        let initial_snapshot = self.master_monitor.follower_readiness_snapshot();
        if initial_snapshot.role != RoleState::Follower
            || self.follower_read_refusal(&initial_snapshot).is_some()
        {
            return Ok(None);
        }
        if !self
            .follower_block_report_state
            .has_epoch(initial_snapshot.term, initial_snapshot.leadership_epoch)
        {
            drop(_serial);
            let Some(_reset_guard) = self.follower_block_report_lock.try_write() else {
                return Ok(None);
            };
            let reset_snapshot = self.master_monitor.follower_readiness_snapshot();
            if self.master_monitor.is_active()
                || reset_snapshot.role != RoleState::Follower
                || reset_snapshot.leadership_epoch != initial_snapshot.leadership_epoch
                || self.follower_read_refusal(&reset_snapshot).is_some()
            {
                return Ok(None);
            }
            self.reset_passive_view_for_current_epoch();
            drop(_reset_guard);
            let Some(read_guard) = self.follower_block_report_lock.try_read() else {
                return Ok(None);
            };
            _serial = read_guard;
            let reset_complete_snapshot = self.master_monitor.follower_readiness_snapshot();
            if self.master_monitor.is_active()
                || reset_complete_snapshot.role != RoleState::Follower
                || reset_complete_snapshot.leadership_epoch != initial_snapshot.leadership_epoch
                || self
                    .follower_read_refusal(&reset_complete_snapshot)
                    .is_some()
                || !self.follower_block_report_state.has_epoch(
                    reset_complete_snapshot.term,
                    reset_complete_snapshot.leadership_epoch,
                )
            {
                return Ok(None);
            }
        }

        let fs_dir = self.fs_dir.read();
        let worker_manager = self.worker_manager.read();
        let path = path.as_ref();
        let result: FsResult<Option<FileBlocks>> = (|| {
            let inp = Self::resolve_path(&fs_dir, path)?;
            let Some(inode) = inp.get_last_inode() else {
                return Err(FsError::file_not_found(path));
            };
            let file = inode.as_file_ref()?;
            let file_locations = fs_dir.get_file_locations(file)?;
            let Some(safe_file_locations) =
                self.follower_file_locations_ready(file, &file_locations, &worker_manager)
            else {
                return Ok(None);
            };
            let block_locs = Self::get_block_locs_from_locations(
                path,
                file,
                &safe_file_locations,
                &worker_manager,
            )?;
            Ok(Some(FileBlocks {
                status: inode.to_file_status(path)?,
                block_locs,
            }))
        })();

        let final_snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active()
            || initial_snapshot.leadership_epoch != final_snapshot.leadership_epoch
            || final_snapshot.role != RoleState::Follower
            || self.follower_read_refusal(&final_snapshot).is_some()
        {
            return Ok(None);
        }

        result
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_report_tracker.reset(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    /// Report id of the last fully processed full report of the worker.
    #[doc(hidden)]
    pub fn full_report_completed_round(&self, worker_id: u32) -> Option<u64> {
        self.full_report_tracker.completed_round(worker_id)
    }

    /// Number of workers with a partially accumulated full report.
    #[doc(hidden)]
    pub fn full_report_open_sessions(&self) -> usize {
        self.full_report_tracker.open_sessions()
    }

    fn invalidate_full_block_report_session(&self, worker_id: u32) {
        self.full_report_tracker
            .invalidate_session(worker_id, LocalTime::mills());
    }

    fn invalidate_full_block_state(&self, worker_id: u32) {
        self.invalidate_full_block_report_session(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    fn invalidate_full_block_reconcile(&self, worker_id: u32) {
        let mut reconciles = self.full_block_reconciles.lock();
        if let Some(state) = reconciles.get_mut(&worker_id) {
            state.generation = state.generation.saturating_add(1);
            state.pending = None;
            if !state.running {
                reconciles.remove(&worker_id);
            }
        }
    }

    /// Process block reports
    pub fn block_report(
        &self,
        list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        // @todo check cluster.
        let invalidate_full_reconcile = !list.full_report
            && list.blocks.iter().any(|block| {
                matches!(
                    block.status,
                    BlockReportStatus::Finalized | BlockReportStatus::Writing
                )
            });
        if invalidate_full_reconcile {
            self.invalidate_full_block_state(list.worker_id);
        }

        let full_reported_blocks = match self.full_report_tracker.collect(&list, LocalTime::mills())
        {
            FullReportProgress::Duplicate => {
                // The response to this batch may have been lost, so its delete
                // commands and location updates must still be delivered. Both are
                // idempotent; only the session and the reconcile are skipped.
                info!(
                    "resent batch of completed full block report {} from worker {}: not reconciling again",
                    list.report_id, list.worker_id
                );
                None
            }
            FullReportProgress::InFlight => {
                // The first attempt may still fail, so do not acknowledge the resend.
                // The worker retries IO errors after a short backoff.
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "full block report {} from worker {} is still being processed",
                        list.report_id, list.worker_id
                    ),
                )));
            }
            FullReportProgress::Complete(inventory) => Some(inventory.into_blocks()),
            FullReportProgress::Partial => None,
            FullReportProgress::CapacityExceeded => {
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "full block report inventory limit reached for worker {}",
                        list.worker_id
                    ),
                )))
            }
        };
        if list.blocks.is_empty() && full_reported_blocks.is_none() {
            return Ok(BlockReportResult {
                delete_blocks: Vec::new(),
            });
        }

        //(Whether to increase, block id, block location)
        let mut checked = Vec::with_capacity(list.blocks.len());
        let mut delete_blocks = Vec::new();
        let mut missing_blocks = 0usize;
        let mut not_file_blocks = 0usize;
        for item in list.blocks {
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let defer_writing_delete =
                        item.status == BlockReportStatus::Writing && !list.full_report;
                    let state = match self.block_inode_state(item.id) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("block_report {item:?}: {e}");
                            continue;
                        }
                    };
                    match state {
                        BlockInodeState::File => checked.push((item, Some(BlockInodeState::File))),
                        BlockInodeState::Missing if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is missing",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::NotFile if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is not a file",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::Missing => {
                            missing_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::Missing)));
                        }
                        BlockInodeState::NotFile => {
                            not_file_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::NotFile)));
                        }
                    }
                }
                BlockReportStatus::Deleted => checked.push((item, None)),
            }
        }
        if missing_blocks > 0 || not_file_blocks > 0 {
            warn!(
                "block_report found {} missing-inode and {} non-file-inode blocks for worker {}; scheduling worker deletion",
                missing_blocks, not_file_blocks, list.worker_id
            );
        }

        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for (item, exists) in checked {
            let loc = BlockLocation::new(list.worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let state = match exists {
                        Some(v) => v,
                        None => {
                            warn!(
                                "block_report invariant violated: missing inode state for block {}",
                                item.id
                            );
                            continue;
                        }
                    };

                    match state {
                        BlockInodeState::File => batch.push((true, item.id, loc)),
                        BlockInodeState::Missing | BlockInodeState::NotFile => {
                            batch.push((false, item.id, loc));
                            wm.remove_block(list.worker_id, item.id);
                        }
                    }
                }
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(list.worker_id, item.id);
                }
            }
        }
        drop(wm);

        // Keep the block ids of a completing batch so a failure can be retried.
        let retry_blocks = full_reported_blocks.clone();
        let applied = (|| -> FsResult<()> {
            if let Some(reported_blocks) = full_reported_blocks {
                self.submit_full_block_reconcile(
                    list.worker_id,
                    reported_blocks,
                    replication_handler,
                )?;
            }
            self.apply_block_report_batch(batch)
        })();

        if let Some(blocks) = retry_blocks {
            let now = LocalTime::mills();
            match &applied {
                Ok(()) => self
                    .full_report_tracker
                    .finish(list.worker_id, list.report_id, now),
                Err(_) => self.full_report_tracker.abort(
                    list.worker_id,
                    list.report_id,
                    list.total_len,
                    FullReportInventory::without_slot(blocks),
                    now,
                ),
            }
        }
        applied?;

        Ok(BlockReportResult { delete_blocks })
    }

    /// Apply worker reports to a follower's local location view only. This
    /// method never updates BlockMap, returns delete commands, or schedules
    /// replication, quota, blacklist, or lost-worker cleanup work.
    pub fn passive_block_report(
        &self,
        list: BlockReportList,
        worker_session_id: Option<&str>,
    ) -> FsResult<()> {
        if !self.follower_worker_state_enabled()
            || self.master_monitor.follower_readiness_snapshot().role
                != curvine_raft::raft::RoleState::Follower
        {
            return err_box!("Passive worker reports are not enabled on this master");
        }

        let _serial = self.follower_block_report_lock.write();
        let report_snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active() || report_snapshot.role != RoleState::Follower {
            return err_box!("Passive worker reports are not accepted by the active master");
        }
        self.reset_passive_view_for_current_epoch();
        let worker_session_id = {
            let wm = self.worker_manager.read();
            if list.cluster_id != wm.cluster_id {
                return err_box!(
                    "Registered cluster_id mismatch, expected {}, actual: {}",
                    wm.cluster_id,
                    list.cluster_id
                );
            }
            let Some(worker) = wm.get_worker(list.worker_id) else {
                return Err(FsError::in_progress_msg(format!(
                    "NotRegistered: worker {} must complete registration before reporting blocks",
                    list.worker_id
                )));
            };
            if worker.status == WorkerStatus::Lost {
                return Err(FsError::in_progress_msg(format!(
                    "NotRegistered: worker {} has expired on this follower",
                    list.worker_id
                )));
            }
            if !worker_session_id
                .is_some_and(|session| !session.is_empty() && session == worker.worker_session_id)
            {
                return Err(FsError::in_progress_msg(format!(
                    "NotRegistered: worker {} report does not match its current registration session",
                    list.worker_id
                )));
            }
            worker.worker_session_id.clone()
        };

        let worker_id = list.worker_id;
        let report_id = list.report_id;
        let total_len = list.total_len;
        let full_report = list.full_report;
        if self
            .follower_block_report_state
            .reconcile_pending(worker_id)
        {
            return Err(FsError::io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "full block report reconciliation is still running for worker {}",
                    worker_id
                ),
            )));
        }
        if !full_report
            && self
                .follower_block_report_state
                .requires_full_report(worker_id)
        {
            return Err(FsError::in_progress_msg(format!(
                "ResyncRequired: worker {} must complete a full report before incremental reports",
                worker_id
            )));
        }
        let now = LocalTime::mills();
        self.follower_block_report_state
            .observe_worker(worker_id, false);

        if !full_report
            && list.blocks.iter().any(|block| {
                matches!(
                    block.status,
                    BlockReportStatus::Finalized | BlockReportStatus::Writing
                )
            })
        {
            self.follower_full_report_tracker
                .invalidate_session(worker_id, now);
        }

        let progress = self.follower_full_report_tracker.collect(&list, now);
        if full_report && !matches!(&progress, FullReportProgress::Duplicate) {
            self.follower_block_report_state
                .require_full_report(worker_id);
        }
        let mut full_report_inventory = match progress {
            FullReportProgress::Duplicate => None,
            FullReportProgress::InFlight => {
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "full block report {} from worker {} is still being processed",
                        report_id, worker_id
                    ),
                )));
            }
            FullReportProgress::Complete(inventory) => Some(inventory),
            FullReportProgress::Partial => None,
            FullReportProgress::CapacityExceeded => {
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "follower full block report inventory limit reached for worker {}",
                        worker_id
                    ),
                )))
            }
        };

        // Record adds whose inode has not reached this follower's applied
        // journal state yet. A report is acknowledged only if every deferred
        // entry fits in the bounded queue.
        let observed_contact_sequence = self
            .master_monitor
            .follower_readiness_snapshot()
            .leader_contact_sequence;
        let mut batch = Vec::with_capacity(list.blocks.len());
        let mut deferred_reports = Vec::new();
        let mut clear_deferred = Vec::new();
        for item in list.blocks {
            let location = BlockLocation::new(worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, location));
                    clear_deferred.push(item.id);
                }
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let inode_state = match self.block_inode_state(item.id) {
                        Ok(state) => state,
                        Err(error) => {
                            if let Some(inventory) = full_report_inventory.take() {
                                self.follower_full_report_tracker
                                    .abort(worker_id, report_id, total_len, inventory, now);
                            }
                            return Err(error);
                        }
                    };
                    match inode_state {
                        BlockInodeState::File => {
                            batch.push((true, item.id, location));
                            clear_deferred.push(item.id);
                        }
                        BlockInodeState::Missing | BlockInodeState::NotFile => {
                            deferred_reports.push(DeferredBlockReport {
                                worker_id,
                                block_id: item.id,
                                storage_type: item.storage_type,
                                remove_if_missing: full_report
                                    || item.status == BlockReportStatus::Finalized,
                                leader_contact_sequence: observed_contact_sequence,
                            });
                        }
                    }
                }
            }
        }

        for report in deferred_reports.iter().copied() {
            if !self.follower_block_report_state.defer(report) {
                if let Some(inventory) = full_report_inventory.take() {
                    self.follower_full_report_tracker
                        .abort(worker_id, report_id, total_len, inventory, now);
                }
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "deferred block report queue is full for worker {} (limit {})",
                        worker_id, MAX_DEFERRED_FOLLOWER_REPORTS
                    ),
                )));
            }
        }

        let applied = self.apply_block_report_batch_fenced(batch, || {
            let current_snapshot = self.master_monitor.follower_readiness_snapshot();
            if self.master_monitor.is_active()
                || current_snapshot.role != RoleState::Follower
                || current_snapshot.term != report_snapshot.term
                || current_snapshot.leadership_epoch != report_snapshot.leadership_epoch
            {
                return Err(FsError::in_progress_msg(
                    "Passive worker report crossed its follower leadership epoch",
                ));
            }
            if !self
                .worker_manager
                .read()
                .get_worker(worker_id)
                .is_some_and(|worker| {
                    worker.worker_session_id == worker_session_id
                        && worker.status != WorkerStatus::Lost
                })
            {
                return Err(FsError::in_progress_msg(format!(
                    "NotRegistered: worker {} session changed while applying its report",
                    worker_id
                )));
            }
            Ok(true)
        });

        let applied = match applied {
            Ok(true) => Ok(()),
            Ok(false) => Err(FsError::in_progress_msg(
                "Passive worker report crossed its follower leadership epoch",
            )),
            Err(error) => Err(error),
        };
        if let Err(error) = applied {
            if let Some(inventory) = full_report_inventory.take() {
                self.follower_full_report_tracker
                    .abort(worker_id, report_id, total_len, inventory, now);
            }
            return Err(error);
        }

        if let Some(inventory) = full_report_inventory.take() {
            let generation = self.follower_block_report_state.begin_reconcile(worker_id);
            let job = FollowerFullBlockReconcileJob {
                worker_id,
                generation,
                term: report_snapshot.term,
                leadership_epoch: report_snapshot.leadership_epoch,
                worker_session_id,
                report_id,
                inventory,
            };
            self.submit_follower_full_block_reconcile(job, total_len)?;
        }

        for block_id in clear_deferred {
            self.follower_block_report_state
                .remove_deferred(worker_id, block_id);
        }

        Ok(())
    }

    /// Retry unknown-inode reports after journal replay catches up. Missing or
    /// non-file inodes are discarded only with fresh leader-commit proof and an
    /// applied index that covers both the local and leader commit watermarks.
    pub fn retry_passive_block_reports(&self) -> FsResult<()> {
        if !self.follower_worker_state_enabled() {
            return Ok(());
        }

        let _serial = self.follower_block_report_lock.write();
        if self.master_monitor.is_active()
            || self.master_monitor.follower_readiness_snapshot().role
                != curvine_raft::raft::RoleState::Follower
        {
            return Ok(());
        }
        self.reset_passive_view_for_current_epoch();
        let deferred = self.follower_block_report_state.deferred_snapshot();
        let snapshot = self.master_monitor.follower_readiness_snapshot();
        let contact_timeout = Duration::from_millis(self.follower_read_contact_timeout_ms);
        let replay_is_current = |snapshot: &FollowerReadinessSnapshot| {
            snapshot.role == RoleState::Follower
                && snapshot.replay_ready
                && snapshot.leader_commit_index.is_some_and(|leader_commit| {
                    snapshot
                        .applied_index
                        .is_some_and(|applied| applied >= snapshot.commit_index.max(leader_commit))
                })
                && snapshot
                    .leader_contact_age
                    .is_some_and(|age| age <= contact_timeout)
        };
        let mut candidates = Vec::new();
        for report in deferred {
            if self
                .follower_block_report_state
                .reconcile_pending(report.worker_id)
            {
                continue;
            }
            if !replay_is_current(&snapshot)
                || snapshot.leader_contact_sequence <= report.leader_contact_sequence
            {
                continue;
            }
            candidates.push(report);
        }

        if !self.apply_deferred_block_reports_fenced(&candidates, &snapshot, contact_timeout)? {
            return Ok(());
        }
        Ok(())
    }

    fn apply_deferred_block_reports_fenced(
        &self,
        reports: &[DeferredBlockReport],
        initial_snapshot: &FollowerReadinessSnapshot,
        contact_timeout: Duration,
    ) -> FsResult<bool> {
        let replay_is_current = |snapshot: &FollowerReadinessSnapshot| {
            snapshot.role == RoleState::Follower
                && snapshot.replay_ready
                && snapshot.leader_commit_index.is_some_and(|leader_commit| {
                    snapshot
                        .applied_index
                        .is_some_and(|applied| applied >= snapshot.commit_index.max(leader_commit))
                })
                && snapshot
                    .leader_contact_age
                    .is_some_and(|age| age <= contact_timeout)
        };
        let mut resolved = Vec::with_capacity(reports.len());
        for chunk in reports.chunks(Self::BLOCK_REPORT_WRITE_CHUNK) {
            let mut fs_dir = self.fs_dir.write();
            let current_snapshot = self.master_monitor.follower_readiness_snapshot();
            if self.master_monitor.is_active()
                || !replay_is_current(&current_snapshot)
                || current_snapshot.leadership_epoch != initial_snapshot.leadership_epoch
            {
                return Ok(false);
            }

            let mut batch = Vec::with_capacity(chunk.len());
            let mut resolved_chunk = Vec::with_capacity(chunk.len());
            for report in chunk {
                if current_snapshot.leader_contact_sequence <= report.leader_contact_sequence {
                    continue;
                }
                match fs_dir.block_inode_state(report.block_id)? {
                    BlockInodeState::File => {
                        batch.push((
                            true,
                            report.block_id,
                            BlockLocation::new(report.worker_id, report.storage_type),
                        ));
                        resolved_chunk.push((report.worker_id, report.block_id));
                    }
                    BlockInodeState::Missing | BlockInodeState::NotFile => {
                        if report.remove_if_missing {
                            batch.push((
                                false,
                                report.block_id,
                                BlockLocation::with_id(report.worker_id),
                            ));
                        }
                        resolved_chunk.push((report.worker_id, report.block_id));
                    }
                }
            }

            if !batch.is_empty() {
                fs_dir.block_report(batch)?;
            }
            resolved.extend(resolved_chunk);
        }

        let final_snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active()
            || !replay_is_current(&final_snapshot)
            || final_snapshot.leadership_epoch != initial_snapshot.leadership_epoch
        {
            return Ok(false);
        }
        for (worker_id, block_id) in resolved {
            self.follower_block_report_state
                .mark_deferred_resolved(worker_id, block_id);
        }
        Ok(true)
    }

    /// Submit the follower's stale-location scan without retaining the global
    /// report writer. The tracker owns the bounded inventory slot until this
    /// job either completes or resets the report session.
    fn submit_follower_full_block_reconcile(
        &self,
        job: FollowerFullBlockReconcileJob,
        total_len: u64,
    ) -> FsResult<()> {
        let worker_id = job.worker_id;
        let report_id = job.report_id;
        let generation = job.generation;
        let job_slot = Arc::new(Mutex::new(Some(job)));
        let task_slot = job_slot.clone();
        let fs = self.clone();
        match self
            .full_block_reconcile_executor
            .fixed_try_spawn(worker_id as i64, move || {
                if let Some(job) = task_slot.lock().take() {
                    fs.run_follower_full_block_reconcile(job);
                }
            }) {
            Ok(()) => Ok(()),
            Err(error) => {
                let Some(job) = job_slot.lock().take() else {
                    return err_box!(
                        "follower full report reconcile submission failed after task start"
                    );
                };
                self.follower_block_report_state
                    .fail_reconcile(worker_id, generation);
                self.follower_full_report_tracker.abort(
                    worker_id,
                    report_id,
                    total_len,
                    job.inventory,
                    LocalTime::mills(),
                );
                error!(
                    "submit follower full block report reconcile for worker {} failed: {}",
                    worker_id, error
                );
                Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "follower full block report reconciliation could not be queued for worker {}",
                        worker_id
                    ),
                )))
            }
        }
    }

    fn run_follower_full_block_reconcile(&self, job: FollowerFullBlockReconcileJob) {
        if !self
            .follower_block_report_state
            .reconcile_is_current(job.worker_id, job.generation)
        {
            return;
        }

        // The namespace scan can be large. Do it outside the follower report
        // lock; pending state rejects other reports for this worker and defers
        // replay reconciliation, so its location set cannot change meanwhile.
        let stale_block_ids = {
            let fs_dir = self.fs_dir.read();
            fs_dir.get_worker_block_ids(job.worker_id).map(|existing| {
                existing
                    .into_iter()
                    .filter(|block_id| !job.inventory.blocks().contains(block_id))
                    .collect::<Vec<_>>()
            })
        };
        self.finish_follower_full_block_reconcile(job, stale_block_ids);
    }

    fn follower_reconcile_context_current(&self, job: &FollowerFullBlockReconcileJob) -> bool {
        let snapshot = self.master_monitor.follower_readiness_snapshot();
        if self.master_monitor.is_active()
            || snapshot.role != RoleState::Follower
            || snapshot.term != job.term
            || snapshot.leadership_epoch != job.leadership_epoch
        {
            return false;
        }

        let worker_matches = self
            .worker_manager
            .read()
            .get_worker(job.worker_id)
            .is_some_and(|worker| {
                worker.worker_session_id == job.worker_session_id
                    && worker.status != WorkerStatus::Lost
            });
        worker_matches
            && self
                .follower_block_report_state
                .reconcile_is_current(job.worker_id, job.generation)
    }

    fn finish_follower_full_block_reconcile(
        &self,
        job: FollowerFullBlockReconcileJob,
        stale_block_ids: FsResult<Vec<i64>>,
    ) {
        let _serial = self.follower_block_report_lock.write();
        if !self
            .follower_block_report_state
            .reconcile_is_current(job.worker_id, job.generation)
        {
            return;
        }

        let stale_block_ids = match stale_block_ids {
            Ok(block_ids) if self.follower_reconcile_context_current(&job) => block_ids,
            Ok(_) => {
                self.fail_follower_full_block_reconcile(&job, "fencing context changed");
                return;
            }
            Err(error) => {
                self.fail_follower_full_block_reconcile(&job, &error.to_string());
                return;
            }
        };

        let stale_block_count = stale_block_ids.len();
        let batch = stale_block_ids
            .into_iter()
            .map(|block_id| (false, block_id, BlockLocation::with_id(job.worker_id)))
            .collect();
        match self.apply_block_report_batch_fenced(batch, || {
            Ok(self.follower_reconcile_context_current(&job))
        }) {
            Ok(true) => {}
            Ok(false) => {
                self.fail_follower_full_block_reconcile(
                    &job,
                    "fencing context changed during apply",
                );
                return;
            }
            Err(error) => {
                self.fail_follower_full_block_reconcile(&job, &error.to_string());
                return;
            }
        }

        // The worker or Raft role may have changed while chunked removals were
        // applied. Leave the view unreadable and require a new report if so.
        if !self.follower_reconcile_context_current(&job) {
            self.fail_follower_full_block_reconcile(&job, "fencing context changed during apply");
            return;
        }

        if !self.follower_block_report_state.finish_reconcile(
            job.worker_id,
            job.generation,
            job.inventory.blocks(),
        ) {
            return;
        }
        self.follower_full_report_tracker
            .finish(job.worker_id, job.report_id, LocalTime::mills());
        if stale_block_count > 0 {
            info!(
                "follower full block report reconciled {} stale block locations for worker {}",
                stale_block_count, job.worker_id
            );
        }
    }

    fn fail_follower_full_block_reconcile(
        &self,
        job: &FollowerFullBlockReconcileJob,
        reason: &str,
    ) {
        if self
            .follower_block_report_state
            .fail_reconcile(job.worker_id, job.generation)
        {
            // The report RPC has already returned success. Drop its inventory
            // and round so the next Running heartbeat requests a fresh report.
            self.follower_full_report_tracker.reset(job.worker_id);
            warn!(
                "follower full block report reconcile failed for worker {}: {}; requesting a fresh report",
                job.worker_id, reason
            );
        }
    }

    fn submit_full_block_reconcile(
        &self,
        worker_id: u32,
        reported_blocks: HashSet<i64>,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<()> {
        let should_spawn = {
            let mut reconciles = self.full_block_reconciles.lock();
            let state = reconciles
                .entry(worker_id)
                .or_insert_with(|| FullBlockReconcileState {
                    running: false,
                    generation: 0,
                    pending: None,
                });
            state.generation = state.generation.saturating_add(1);
            let generation = state.generation;
            state.pending = Some(FullBlockReconcileJob {
                generation,
                reported_blocks,
            });
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if !should_spawn {
            return Ok(());
        }

        let fs = self.clone();
        let res = self
            .full_block_reconcile_executor
            .fixed_spawn(worker_id as i64, move || {
                fs.run_full_block_reconcile(worker_id, replication_handler);
            });
        if let Err(e) = &res {
            self.full_block_reconciles.lock().remove(&worker_id);
            error!("submit full block report reconcile for worker {worker_id} failed: {e}");
        }
        res?;
        Ok(())
    }

    fn run_full_block_reconcile(
        &self,
        worker_id: u32,
        replication_handler: Option<MasterReplicationHandler>,
    ) {
        loop {
            let job = {
                let mut reconciles = self.full_block_reconciles.lock();
                match reconciles.get_mut(&worker_id) {
                    Some(state) => match state.pending.take() {
                        Some(v) => v,
                        None => {
                            reconciles.remove(&worker_id);
                            return;
                        }
                    },
                    None => return,
                }
            };

            if !self.is_full_block_reconcile_current(worker_id, job.generation) {
                info!(
                    "skip stale full block report reconcile for worker {}, generation {}",
                    worker_id, job.generation
                );
                continue;
            }

            match self.reconcile_full_block_report(worker_id, job.generation, job.reported_blocks) {
                Ok(stale_block_ids) => {
                    let stale_block_count = stale_block_ids.len();
                    if stale_block_count > 0 {
                        info!(
                            "full block report reconciled {} stale block locations for worker {}",
                            stale_block_count, worker_id
                        );
                        if let Some(replication_handler) = &replication_handler {
                            if let Err(e) = replication_handler
                                .report_under_replicated_blocks(worker_id, stale_block_ids)
                            {
                                error!(
                                    "Errors on reporting under-replicated {} blocks from full block report reconciliation. err: {:?}",
                                    stale_block_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "full block report reconcile for worker {} failed: {}",
                        worker_id, e
                    );
                }
            }
        }
    }

    fn reconcile_full_block_report(
        &self,
        worker_id: u32,
        generation: u64,
        reported_blocks: HashSet<i64>,
    ) -> FsResult<Vec<i64>> {
        let existing_blocks = {
            let fs_dir = self.fs_dir.read();
            fs_dir.get_worker_block_ids(worker_id)?
        };

        let mut stale_block_ids = Vec::new();
        let mut batch = Vec::new();
        for block_id in existing_blocks {
            if !reported_blocks.contains(&block_id) {
                batch.push((false, block_id, BlockLocation::with_id(worker_id)));
                stale_block_ids.push(block_id);
            }
        }

        if !batch.is_empty() {
            let reconciles = self.full_block_reconciles.lock();
            if !reconciles
                .get(&worker_id)
                .map(|state| state.generation == generation)
                .unwrap_or(false)
            {
                info!(
                    "skip stale full block report reconcile apply for worker {}, generation {}",
                    worker_id, generation
                );
                return Ok(Vec::new());
            }
            self.apply_block_report_batch(batch)?;
        }

        Ok(stale_block_ids)
    }

    /// Applies block-report location updates in bounded chunks so the global
    /// fs_dir write lock is held only briefly per chunk. Each entry is an
    /// independent add/remove for one block location, so chunk boundaries do
    /// not break cross-entry invariants.
    fn apply_block_report_batch(&self, batch: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        self.apply_block_report_batch_fenced(batch, || Ok(true))
            .map(|_| ())
    }

    /// Apply block-location chunks only after acquiring the namespace writer
    /// and validating the caller's fence under that same lock. A role change
    /// after the check cannot let an active writer overtake this chunk: active
    /// block-report and replication updates use the same fs_dir writer.
    fn apply_block_report_batch_fenced<F>(
        &self,
        batch: Vec<(bool, i64, BlockLocation)>,
        mut validate: F,
    ) -> FsResult<bool>
    where
        F: FnMut() -> FsResult<bool>,
    {
        if batch.is_empty() {
            return validate();
        }

        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }
            let mut fs_dir = self.fs_dir.write();
            if !validate()? {
                return Ok(false);
            }
            fs_dir.block_report(chunk)?;
        }
        Ok(true)
    }

    fn is_full_block_reconcile_current(&self, worker_id: u32, generation: u64) -> bool {
        self.full_block_reconciles
            .lock()
            .get(&worker_id)
            .map(|state| state.generation == generation)
            .unwrap_or(false)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let removed_block_ids = {
            let fs_dir = self.fs_dir.write();
            fs_dir.delete_locations(worker_id)?
        };
        let mut invalidated = CacheInvalidationResult::default();

        for chunk in removed_block_ids.chunks(Self::LOST_WORKER_INVALIDATION_CHUNK) {
            let result = {
                let mut fs_dir = self.fs_dir.write();
                fs_dir.invalidate_lost_cache_files(chunk)
            };
            match result {
                Ok(result) => invalidated.extend(result),
                Err(e) => warn!(
                    "failed to invalidate lost cache files for worker {} ({} block ids); \\
                     continuing with normal replica recovery: {}",
                    worker_id,
                    chunk.len(),
                    e
                ),
            }
        }

        let replication_block_ids = removed_block_ids
            .iter()
            .copied()
            .filter(|block_id| !invalidated.invalidated_block_ids.contains(block_id))
            .collect();

        if !invalidated.delete_result.blocks.is_empty() {
            self.worker_manager
                .write()
                .remove_blocks(&invalidated.delete_result);
        }

        Ok(LostWorkerLocationCleanup {
            removed_block_ids,
            replication_block_ids,
        })
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Self::check_not_reserved_path(inp.path())?;
        Self::check_not_internal_x_attr(&opts)?;
        fs_dir.set_attr(inp, opts)
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, mode, None, None)
    }

    pub fn symlink_with_owner_group<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let target = target.as_ref().to_string();
        let link = Self::resolve_path(&fs_dir, link.as_ref())?;
        Self::check_not_reserved_path(link.path())?;
        fs_dir.symlink(target, link, force, mode, owner, group)
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let src_path = Self::resolve_path(&fs_dir, src_path.as_ref())?;
        let dst_path = Self::resolve_path(&fs_dir, dst_path.as_ref())?;
        Self::check_not_reserved_path(src_path.path())?;
        Self::check_not_reserved_path(dst_path.path())?;
        fs_dir.link(src_path, dst_path)
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.resize_by_id(path, None, opts)
    }

    pub fn resize_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        opts: FileAllocOpts,
    ) -> FsResult<FileBlocks> {
        opts.validate()?;

        let path = path.as_ref();
        Self::check_not_reserved_path(path)?;
        // This snapshot only rejects individually impossible requests; it is not a
        // reservation, so concurrent fallocates may observe the same capacity.
        // Worker-side block allocation remains the hard enforcement point.
        let available = if opts.truncate {
            i64::MAX
        } else {
            self.worker_manager.read().available_bytes()
        };

        let (del_res, inode_id) = {
            let mut fs_dir = self.fs_dir.write();
            let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
            if inode_id.is_some() {
                // A by-id request may carry a stale path, so also check where
                // the inode actually lives.
                let live_path = fs_dir.get_inode_path(inode.id())?;
                Self::check_not_reserved_path(&live_path)?;
            }
            let file = inode.as_file_ref()?;
            Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
            let inode_id = inode.id();
            let del_res = fs_dir.resize_inode(path, &mut inode, opts)?;
            (del_res, inode_id)
        };

        if !del_res.blocks.is_empty() {
            self.worker_manager.write().remove_blocks(&del_res);
        }

        let blocks = {
            let fs_dir = self.fs_dir.read();
            let inode = Self::resolve_file_inode(&fs_dir, path, Some(inode_id))?;
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            FileBlocks::new(inode.to_file_status(path)?, locs)
        };

        Ok(blocks)
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        self.assign_worker_by_id(path, None, block, client_addr, exclude_workers)
    }

    pub fn assign_worker_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;

        let file = inode.as_file_ref()?;
        let block_size = file.block_size as i64;
        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block = match fs_dir.assign_worker_inode(path, &mut inode, block.id, &choose_workers) {
            Ok((block, assigned)) => {
                if !assigned {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                }
                block
            }
            Err(e) => {
                self.worker_manager
                    .write()
                    .unschedule_chosen_workers(&choose_workers, block_size);
                return Err(e);
            }
        };

        Ok(LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        })
    }

    pub fn get_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path)?;
        let expire_ms = self.conf.lock_expire_time_ms();

        fs_dir.get_lock(inp, &lock, expire_ms)
    }

    pub fn set_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        fs_dir.set_lock(inp, lock, self.conf.lock_expire_time_ms())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::ErrorExt;
    use curvine_error::ErrorKind;
    use curvine_raft::raft::{FollowerReadinessMonitor, RoleState};
    use curvine_runtime::common::Utils;
    use curvine_runtime::sync::StateCtl;

    fn test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn use_current_follower_view(fs: &mut MasterFilesystem) -> FollowerReadinessMonitor {
        let readiness = FollowerReadinessMonitor::default();
        readiness.observe_raft_state(RoleState::Follower, 1, 2, 0);
        readiness.observe_applied(0);
        readiness.set_replay_ready(true);
        readiness.observe_leader_contact(1, 2, Some(0));
        fs.master_monitor = MasterMonitor::with_follower_readiness(
            StateCtl::new(RoleState::Follower.into()),
            StateCtl::new(0),
            readiness.clone(),
        );
        readiness
    }

    fn apply_test_passive_heartbeat(
        fs: &MasterFilesystem,
        status: HeartbeatStatus,
        address: WorkerAddress,
        worker_session_id: &str,
    ) -> FsResult<()> {
        let worker_id = address.worker_id;
        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        let validate_cluster_id = cluster_id.clone();
        let validate_address = address.clone();
        let update_address = address.clone();
        let update_session = worker_session_id.to_string();
        fs.process_passive_worker_heartbeat(
            status,
            worker_id,
            move |wm| {
                if status == HeartbeatStatus::Start {
                    wm.validate_passive_start(&validate_cluster_id, &validate_address)
                } else {
                    Ok(())
                }
            },
            move |wm| {
                wm.heartbeat_passive(
                    &cluster_id,
                    status,
                    update_address,
                    1,
                    update_session,
                    TransferWorkerCapabilities::default(),
                    String::new(),
                    0,
                    vec![],
                    None,
                )
            },
        )
    }

    fn wait_for_follower_reconcile(fs: &MasterFilesystem, worker_id: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while fs.follower_block_report_state.reconcile_pending(worker_id) {
            assert!(
                std::time::Instant::now() < deadline,
                "follower full report reconciliation timed out for worker {worker_id}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn passive_block_report_from_unregistered_worker_is_retryable() {
        let mut fs = test_fs("passive-block-report-unregistered");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        let error = fs
            .passive_block_report(
                BlockReportList {
                    cluster_id,
                    worker_id: 7,
                    full_report: false,
                    total_len: 0,
                    report_id: 0,
                    blocks: Vec::new(),
                },
                None,
            )
            .unwrap_err();

        assert!(matches!(error.kind(), ErrorKind::InProgress));
        assert!(error.to_string().starts_with("NotRegistered:"));
    }

    #[test]
    fn passive_full_report_must_match_current_worker_session() {
        let mut fs = test_fs("passive-block-report-session");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let address = WorkerAddress {
            worker_id: 7,
            hostname: "worker-7".to_string(),
            ..Default::default()
        };
        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        fs.observe_passive_worker_heartbeat(HeartbeatStatus::Start, 7);
        fs.worker_manager
            .write()
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Start,
                address,
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        let report = || BlockReportList {
            cluster_id: cluster_id.clone(),
            worker_id: 7,
            full_report: true,
            total_len: 1,
            report_id: 10,
            blocks: vec![BlockReportInfo::new(
                (50_000_i64 << 24) | 1,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                128,
            )],
        };
        let error = fs
            .passive_block_report(report(), Some("previous-session"))
            .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::InProgress));
        assert!(error.to_string().starts_with("NotRegistered:"));
        assert!(!fs.follower_block_report_state.worker_ready(7));

        fs.passive_block_report(report(), Some("current-session"))
            .unwrap();
        wait_for_follower_reconcile(&fs, 7);
        assert!(fs.follower_block_report_state.worker_ready(7));
        assert!(fs
            .follower_block_report_state
            .has_deferred_block((50_000_i64 << 24) | 1));
    }

    #[test]
    fn passive_start_serializes_readiness_reset_with_session_replacement() {
        let mut fs = test_fs("passive-start-session-serialization");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let address = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Start, address.clone(), "old-session")
            .unwrap();
        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        let block_id = (50_000_i64 << 24) | 1;
        let report = |report_id| BlockReportList {
            cluster_id: cluster_id.clone(),
            worker_id: 7,
            full_report: true,
            total_len: 1,
            report_id,
            blocks: vec![BlockReportInfo::new(
                block_id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                128,
            )],
        };
        fs.passive_block_report(report(10), Some("old-session"))
            .unwrap();
        wait_for_follower_reconcile(&fs, 7);
        assert!(fs.follower_block_report_state.worker_ready(7));

        let rejected = fs.process_passive_worker_heartbeat(
            HeartbeatStatus::Start,
            7,
            |wm| wm.validate_passive_start("wrong-cluster", &address),
            |_| panic!("invalid Start must not update the worker"),
        );
        assert!(rejected.is_err());
        assert!(fs.follower_block_report_state.worker_ready(7));
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .worker_session_id,
            "old-session"
        );

        let start_fs = fs.clone();
        let callback_fs = fs.clone();
        let start_cluster_id = cluster_id.clone();
        let start_update_cluster_id = cluster_id.clone();
        let start_address = address.clone();
        let (start_paused_tx, start_paused_rx) = std::sync::mpsc::channel();
        let (resume_start_tx, resume_start_rx) = std::sync::mpsc::channel();
        let start_thread = std::thread::spawn(move || {
            start_fs.process_passive_worker_heartbeat(
                HeartbeatStatus::Start,
                7,
                move |wm| wm.validate_passive_start(&start_cluster_id, &start_address),
                move |wm| {
                    assert!(callback_fs.follower_block_report_lock.try_write().is_none());
                    assert!(!callback_fs.follower_block_report_state.worker_ready(7));
                    start_paused_tx.send(()).unwrap();
                    resume_start_rx.recv().unwrap();
                    wm.heartbeat_passive(
                        &start_update_cluster_id,
                        HeartbeatStatus::Start,
                        address,
                        1,
                        "new-session".to_string(),
                        TransferWorkerCapabilities::default(),
                        String::new(),
                        0,
                        vec![],
                        None,
                    )
                },
            )
        });
        start_paused_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();

        let report_fs = fs.clone();
        let old_report = report(11);
        let (report_attempted_tx, report_attempted_rx) = std::sync::mpsc::channel();
        let (report_result_tx, report_result_rx) = std::sync::mpsc::channel();
        let report_thread = std::thread::spawn(move || {
            assert!(report_fs.follower_block_report_lock.try_write().is_none());
            report_attempted_tx.send(()).unwrap();
            report_result_tx
                .send(report_fs.passive_block_report(old_report, Some("old-session")))
                .unwrap();
        });
        report_attempted_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();

        resume_start_tx.send(()).unwrap();
        start_thread.join().unwrap().unwrap();
        let stale_report_error = report_result_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        report_thread.join().unwrap();

        assert!(matches!(stale_report_error.kind(), ErrorKind::InProgress));
        assert!(stale_report_error.to_string().starts_with("NotRegistered:"));
        assert!(!fs.follower_block_report_state.worker_ready(7));
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .worker_session_id,
            "new-session"
        );
    }

    #[test]
    fn follower_reconcile_runs_without_report_lock_and_old_session_cannot_finish_it() {
        let mut fs = test_fs("follower-reconcile-restart-fence");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let address = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Start, address.clone(), "old-session")
            .unwrap();
        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        let report = |report_id| BlockReportList {
            cluster_id: cluster_id.clone(),
            worker_id: 7,
            full_report: true,
            total_len: 0,
            report_id,
            blocks: Vec::new(),
        };

        // An empty report needs no namespace lookup on the RPC thread. Holding
        // the namespace writer therefore pauses only the asynchronous scan.
        let namespace_guard = fs.fs_dir.write();
        fs.passive_block_report(report(10), Some("old-session"))
            .unwrap();
        assert!(fs.follower_block_report_state.reconcile_pending(7));
        assert!(!fs.follower_block_report_state.worker_ready(7));
        let report_lock = fs
            .follower_block_report_lock
            .try_write()
            .expect("the completed report must release its global writer before scanning");
        drop(report_lock);
        assert!(apply_test_passive_heartbeat(
            &fs,
            HeartbeatStatus::Running,
            address.clone(),
            "old-session"
        )
        .is_ok());

        // Restart invalidates the generation while the scan still owns the old
        // inventory. A queue barrier on the same fixed executor shard proves
        // the stale job has exited before checking the new session's state.
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Start, address.clone(), "new-session")
            .unwrap();
        drop(namespace_guard);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        fs.full_block_reconcile_executor
            .fixed_try_spawn(7, move || done_tx.send(()).unwrap())
            .unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the stale reconcile should leave the executor shard");
        assert!(!fs.follower_block_report_state.worker_ready(7));

        fs.passive_block_report(report(11), Some("new-session"))
            .unwrap();
        wait_for_follower_reconcile(&fs, 7);
        assert!(fs.follower_block_report_state.worker_ready(7));
    }

    #[test]
    fn failed_follower_reconcile_requests_a_fresh_full_report() {
        let mut fs = test_fs("follower-reconcile-retry");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let address = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Start, address.clone(), "session-7")
            .unwrap();
        fs.follower_block_report_state.require_full_report(7);
        let report = BlockReportList {
            cluster_id: fs.worker_manager.read().cluster_id.clone(),
            worker_id: 7,
            full_report: true,
            total_len: 1,
            report_id: 10,
            blocks: vec![BlockReportInfo::new(
                1,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                1,
            )],
        };
        let inventory = match fs
            .follower_full_report_tracker
            .collect(&report, LocalTime::mills())
        {
            FullReportProgress::Complete(inventory) => inventory,
            _ => panic!("test report should be complete"),
        };
        let snapshot = fs.master_monitor.follower_readiness_snapshot();
        let generation = fs.follower_block_report_state.begin_reconcile(7);
        let job = FollowerFullBlockReconcileJob {
            worker_id: 7,
            generation,
            term: snapshot.term,
            leadership_epoch: snapshot.leadership_epoch,
            worker_session_id: "session-7".to_string(),
            report_id: 10,
            inventory,
        };

        fs.finish_follower_full_block_reconcile(
            job,
            Err(FsError::io(std::io::Error::other("injected scan failure"))),
        );
        assert!(!fs.follower_block_report_state.reconcile_pending(7));
        assert!(!fs.follower_block_report_state.worker_ready(7));
        let retry =
            apply_test_passive_heartbeat(&fs, HeartbeatStatus::Running, address, "session-7")
                .unwrap_err();
        assert!(retry.to_string().starts_with("ResyncRequired:"));

        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        fs.passive_block_report(
            BlockReportList {
                cluster_id,
                worker_id: 7,
                full_report: true,
                total_len: 0,
                report_id: 11,
                blocks: Vec::new(),
            },
            Some("session-7"),
        )
        .unwrap();
        wait_for_follower_reconcile(&fs, 7);
        assert!(fs.follower_block_report_state.worker_ready(7));
    }

    #[test]
    fn follower_stale_chunk_rechecks_role_after_acquiring_namespace_writer() {
        let mut fs = test_fs("follower-reconcile-promotion-fence");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        let readiness = use_current_follower_view(&mut fs);

        let address = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Start, address, "session-7").unwrap();
        fs.follower_block_report_state.require_full_report(7);
        let snapshot = fs.master_monitor.follower_readiness_snapshot();
        let generation = fs.follower_block_report_state.begin_reconcile(7);
        let job = FollowerFullBlockReconcileJob {
            worker_id: 7,
            generation,
            term: snapshot.term,
            leadership_epoch: snapshot.leadership_epoch,
            worker_session_id: "session-7".to_string(),
            report_id: 10,
            inventory: FullReportInventory::without_slot(HashSet::new()),
        };
        let block_id = 1_i64;

        // Hold FsDir so the stale chunk can only validate after this test has
        // promoted the node and installed the authoritative location.
        let namespace_guard = fs.fs_dir.write();
        let chunk_fs = fs.clone();
        let (attempted_tx, attempted_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let chunk = std::thread::spawn(move || {
            assert!(chunk_fs.follower_reconcile_context_current(&job));
            attempted_tx.send(()).unwrap();
            let result = chunk_fs.apply_block_report_batch_fenced(
                vec![(false, block_id, BlockLocation::with_id(7))],
                || Ok(chunk_fs.follower_reconcile_context_current(&job)),
            );
            result_tx.send(result).unwrap();
        });
        attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stale chunk should start while namespace writer is held");

        readiness.observe_raft_state(RoleState::Leader, 2, 1, 0);
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        namespace_guard
            .add_block_location(block_id, BlockLocation::with_id(7))
            .unwrap();
        drop(namespace_guard);

        assert!(!result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stale chunk should recheck the promoted role")
            .unwrap());
        chunk.join().unwrap();
        assert_eq!(
            fs.fs_dir
                .read()
                .get_block_locations(block_id)
                .unwrap()
                .iter()
                .map(|location| location.worker_id)
                .collect::<Vec<_>>(),
            vec![7],
            "a follower stale chunk must not delete a location installed after promotion"
        );
    }

    #[test]
    fn deferred_replay_rechecks_commit_fence_under_namespace_writer() {
        let mut fs = test_fs("deferred-replay-promotion-fence");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        let readiness = use_current_follower_view(&mut fs);
        let report = DeferredBlockReport {
            worker_id: 7,
            block_id: 1,
            storage_type: StorageType::Disk,
            remove_if_missing: true,
            leader_contact_sequence: 0,
        };
        let initial_snapshot = fs.master_monitor.follower_readiness_snapshot();

        let namespace_guard = fs.fs_dir.write();
        namespace_guard
            .add_block_location(report.block_id, BlockLocation::with_id(report.worker_id))
            .unwrap();
        let replay_fs = fs.clone();
        let replay_snapshot = initial_snapshot.clone();
        let (attempted_tx, attempted_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let replay = std::thread::spawn(move || {
            attempted_tx.send(()).unwrap();
            let result = replay_fs.apply_deferred_block_reports_fenced(
                &[report],
                &replay_snapshot,
                Duration::from_secs(5),
            );
            result_tx.send(result).unwrap();
        });
        attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deferred replay should start while namespace writer is held");

        readiness.observe_raft_state(RoleState::Leader, 2, 1, 0);
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        drop(namespace_guard);

        assert!(!result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deferred replay should recheck role under the writer")
            .unwrap());
        replay.join().unwrap();
        assert_eq!(
            fs.fs_dir
                .read()
                .get_block_locations(1)
                .unwrap()
                .iter()
                .map(|location| location.worker_id)
                .collect::<Vec<_>>(),
            vec![7],
            "deferred replay must not remove locations after leadership changes"
        );
    }

    #[test]
    fn follower_resyncs_surviving_worker_after_term_change() {
        let mut fs = test_fs("follower-term-worker-resync");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        let readiness = use_current_follower_view(&mut fs);

        let file_path = "/term-resync.log";
        let block_id = replication_file(&fs, file_path);
        let mut address = WorkerInfo::default().address;
        address.worker_id = 100;
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Start, address.clone(), "session-100")
            .unwrap();
        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        let full_report = |report_id, total_len, blocks| BlockReportList {
            cluster_id: cluster_id.clone(),
            worker_id: 100,
            full_report: true,
            total_len,
            report_id,
            blocks,
        };
        fs.passive_block_report(
            full_report(
                10,
                1,
                vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    128,
                )],
            ),
            Some("session-100"),
        )
        .unwrap();
        wait_for_follower_reconcile(&fs, 100);
        assert!(fs
            .follower_get_block_locations_if_ready(file_path)
            .unwrap()
            .is_some());

        readiness.observe_raft_state(RoleState::Follower, 2, 3, 0);
        readiness.observe_leader_contact(2, 3, Some(0));
        let resync_signal = apply_test_passive_heartbeat(
            &fs,
            HeartbeatStatus::Running,
            address.clone(),
            "session-100",
        )
        .unwrap_err();
        assert!(matches!(resync_signal.kind(), ErrorKind::InProgress));
        assert!(resync_signal.to_string().starts_with("ResyncRequired:"));
        assert!(fs
            .follower_get_block_locations_if_ready(file_path)
            .unwrap()
            .is_none());

        let incremental_error = fs
            .passive_block_report(
                BlockReportList {
                    cluster_id: cluster_id.clone(),
                    worker_id: 100,
                    full_report: false,
                    total_len: 0,
                    report_id: 0,
                    blocks: Vec::new(),
                },
                Some("session-100"),
            )
            .unwrap_err();
        assert!(incremental_error.to_string().starts_with("ResyncRequired:"));

        let later_block_id = (50_000_i64 << 24) | 99;
        fs.passive_block_report(
            full_report(
                11,
                2,
                vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    128,
                )],
            ),
            Some("session-100"),
        )
        .unwrap();
        assert!(apply_test_passive_heartbeat(
            &fs,
            HeartbeatStatus::Running,
            address.clone(),
            "session-100",
        )
        .is_ok());
        fs.passive_block_report(
            full_report(
                11,
                2,
                vec![BlockReportInfo::new(
                    later_block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    128,
                )],
            ),
            Some("session-100"),
        )
        .unwrap();
        wait_for_follower_reconcile(&fs, 100);
        assert!(fs
            .follower_get_block_locations_if_ready(file_path)
            .unwrap()
            .is_some());
        apply_test_passive_heartbeat(&fs, HeartbeatStatus::Running, address, "session-100")
            .unwrap();
    }

    #[test]
    fn passive_block_report_defers_unknown_inode_until_fresh_replay_contact() {
        let mut fs = test_fs("passive-block-report-replay");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        let readiness = use_current_follower_view(&mut fs);

        let address = WorkerAddress {
            worker_id: 7,
            hostname: "worker-7".to_string(),
            ..Default::default()
        };
        let cluster_id = fs.worker_manager.read().cluster_id.clone();
        fs.worker_manager
            .write()
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Running,
                address,
                1,
                "worker-session-7".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        fs.observe_passive_worker_heartbeat(HeartbeatStatus::Running, 7);

        let block_id = (50_000_i64 << 24) | 1;
        fs.passive_block_report(
            BlockReportList {
                cluster_id,
                worker_id: 7,
                full_report: true,
                total_len: 1,
                report_id: 10,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    128,
                )],
            },
            Some("worker-session-7"),
        )
        .unwrap();

        wait_for_follower_reconcile(&fs, 7);

        assert_eq!(fs.follower_block_report_state.deferred_len(), 1);
        fs.retry_passive_block_reports().unwrap();
        assert_eq!(fs.follower_block_report_state.deferred_len(), 1);

        // Only accepted current-term leader traffic after the report can
        // authorize treating the still-missing inode as a stale worker block.
        readiness.observe_leader_contact(1, 2, None);
        fs.retry_passive_block_reports().unwrap();
        assert_eq!(fs.follower_block_report_state.deferred_len(), 1);

        // A known remote watermark that the local FSM has not applied cannot
        // authorize removal of a report whose inode is still missing locally.
        readiness.observe_leader_contact(1, 2, Some(10));
        fs.retry_passive_block_reports().unwrap();
        assert_eq!(fs.follower_block_report_state.deferred_len(), 1);

        readiness.observe_raft_state(RoleState::Follower, 1, 2, 10);
        readiness.observe_applied(10);
        readiness.observe_leader_contact(1, 2, Some(10));
        fs.retry_passive_block_reports().unwrap();
        assert_eq!(fs.follower_block_report_state.deferred_len(), 0);
        assert!(fs.follower_block_report_state.worker_ready(7));
    }

    #[test]
    fn follower_location_reads_filter_to_any_safe_replica_per_block() {
        let mut fs = test_fs("follower-location-readiness");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let file_path = "/replicated.log";
        let block_id = replication_file(&fs, file_path);
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = 400;
        fs.add_test_worker(worker);
        for worker_id in [100, 200, 300, 400] {
            fs.observe_passive_worker_heartbeat(HeartbeatStatus::Running, worker_id);
        }

        assert!(fs
            .follower_get_block_locations_if_ready(file_path)
            .unwrap()
            .is_none());
        fs.follower_block_report_state
            .mark_full_report_complete(100);
        for worker_id in [300, 400] {
            fs.follower_block_report_state
                .mark_full_report_complete(worker_id);
        }
        for worker_id in [200, 300, 400] {
            fs.fs_dir
                .write()
                .add_block_location(block_id, BlockLocation::with_id(worker_id))
                .unwrap();
        }
        {
            let mut wm = fs.worker_manager.write();
            wm.worker_map.workers.get_mut(&100).unwrap().last_update = LocalTime::mills();
            wm.worker_map.workers.get_mut(&300).unwrap().status = WorkerStatus::Blacklist;
            wm.worker_map.workers.get_mut(&400).unwrap().last_update =
                LocalTime::mills().saturating_sub(fs.conf.worker_blacklist_interval_ms() + 1);
        }

        let locations = fs
            .follower_get_block_locations_if_ready(file_path)
            .unwrap()
            .expect("one safe replica should keep the block readable");
        assert_eq!(locations.block_locs.len(), 1);
        assert_eq!(
            locations.block_locs[0]
                .locs
                .iter()
                .map(|address| address.worker_id)
                .collect::<Vec<_>>(),
            vec![100],
            "unready, blacklisted, and heartbeat-stale replicas must be omitted"
        );

        fs.worker_manager
            .write()
            .worker_map
            .workers
            .get_mut(&100)
            .unwrap()
            .status = WorkerStatus::Lost;
        assert!(
            fs.follower_get_block_locations_if_ready(file_path)
                .unwrap()
                .is_none(),
            "a block with no safe replicas must fall back to the leader"
        );

        assert!(fs
            .create_with_opts(
                "/empty.log",
                CreateFileOpts::with_create(false),
                OpenFlags::new_create(),
            )
            .is_ok());
        assert!(fs
            .follower_get_block_locations_if_ready("/empty.log")
            .unwrap()
            .is_some());
    }

    #[test]
    fn follower_read_lock_is_shared_and_fails_fast_during_report_writes() {
        let mut fs = test_fs("follower-read-lock-contention");
        let mut conf = (*fs.conf).clone();
        conf.follower_worker_state_enabled = true;
        fs.conf = Arc::new(conf);
        use_current_follower_view(&mut fs);

        let file_path = "/read-lock.log";
        replication_file(&fs, file_path);
        fs.observe_passive_worker_heartbeat(HeartbeatStatus::Running, 100);
        fs.follower_block_report_state
            .mark_full_report_complete(100);
        fs.worker_manager
            .write()
            .worker_map
            .workers
            .get_mut(&100)
            .unwrap()
            .last_update = LocalTime::mills();
        assert!(fs
            .follower_get_block_locations_if_ready(file_path)
            .unwrap()
            .is_some());

        let first_read = fs.follower_block_report_lock.try_read().unwrap();
        let second_read = fs.follower_block_report_lock.try_read().unwrap();
        assert!(
            fs.follower_block_report_lock.try_write().is_none(),
            "a report writer must not interleave with a location snapshot"
        );
        drop(second_read);
        drop(first_read);

        let report_guard = fs.follower_block_report_lock.write();
        let read_fs = fs.clone();
        let read_path = file_path.to_string();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            result_tx
                .send(read_fs.follower_get_block_locations_if_ready(read_path))
                .unwrap();
        });
        let result = result_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("read RPC must fail fast instead of waiting for a report scan")
            .unwrap();
        assert!(result.is_none());
        drop(report_guard);
        reader.join().unwrap();
    }

    // Seeds an entry under the reserved root through FsDir, since the public
    // API refuses to create anything inside /.Trash.
    fn seed_trash_entry(fs: &MasterFilesystem, name: &str) -> (String, i64) {
        let mut fs_dir = fs.fs_dir.write();
        let root = InodePath::resolve(fs_dir.root_ptr(), trash::TRASH_ROOT, &fs_dir.store).unwrap();
        if !root.is_full() {
            fs_dir.mkdir(root, MkdirOpts::with_create(true)).unwrap();
        }

        let path = format!("{}/{}", trash::TRASH_ROOT, name);
        let inp = InodePath::resolve(fs_dir.root_ptr(), &path, &fs_dir.store).unwrap();
        let inp = fs_dir
            .create_file(inp, CreateFileOpts::with_create(false))
            .unwrap();
        let inode_id = inp.get_last_inode().unwrap().id();
        (path, inode_id)
    }

    fn test_fs_with_trash(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.trash_enabled = true;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-trash-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-trash-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn assert_file_not_found_roundtrip(err: &FsError) {
        assert!(
            matches!(err.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound, got {:?}",
            err.kind()
        );
        let decoded = FsError::decode(err.encode());
        assert!(
            matches!(decoded.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound after encode/decode, got {:?}",
            decoded.kind()
        );
        assert!(
            matches!(decoded, FsError::FileNotFound(_)),
            "decoded error collapsed away from FileNotFound: {}",
            decoded
        );
    }

    #[test]
    fn fallocate_rejects_growth_larger_than_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        let err = MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 359).unwrap_err();
        assert!(matches!(err, FsError::DiskOutOfSpace(_)));
    }

    #[test]
    fn fallocate_accepts_exact_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 360).is_ok());
    }

    #[test]
    fn truncate_growth_does_not_require_physical_capacity() {
        let opts = FileAllocOpts::with_truncate(200);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 0).is_ok());
    }

    fn replication_file(fs: &MasterFilesystem, path: &str) -> i64 {
        for worker_id in [100, 200, 300] {
            let mut worker = WorkerInfo::default();
            worker.address.worker_id = worker_id;
            fs.add_test_worker(worker);
        }
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create())
            .unwrap();
        let block = fs
            .add_block(
                path,
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
        let block_id = block.block.id;
        fs.fs_dir
            .write()
            .add_block_location(block_id, BlockLocation::with_id(100))
            .unwrap();
        block_id
    }

    #[test]
    fn multiblock_open_preserves_order_and_filters_current_worker_state() {
        for count in [0, 1, 16, 256] {
            let fs = test_fs(&format!("multiblock-open-{count}"));
            let worker = |id, status| {
                let mut worker = WorkerInfo::default();
                worker.address.worker_id = id;
                worker.status = status;
                worker
            };
            for id in [100, 200] {
                fs.add_test_worker(worker(id, WorkerStatus::Live));
            }
            let path = "/multiblock";
            fs.create_with_opts(
                path,
                CreateFileOptsBuilder::new()
                    .block_size(1_048_576)
                    .replicas(2)
                    .build(),
                OpenFlags::new_create(),
            )
            .unwrap();
            let mut commits = Vec::new();
            let mut previous = None;
            for index in 0..count {
                let located = fs
                    .add_block(
                        path,
                        None,
                        ClientAddress::default(),
                        commits.clone(),
                        vec![],
                        index * 1_048_576,
                        previous,
                    )
                    .unwrap();
                previous = Some(located.block.clone());
                commits.push(CommitBlock {
                    block_id: located.block.id,
                    block_len: 1_048_576,
                    locations: vec![BlockLocation::with_id(100), BlockLocation::with_id(200)],
                });
            }
            fs.complete_file(
                path,
                None,
                count * 1_048_576,
                commits.clone(),
                "",
                false,
                None,
            )
            .unwrap();
            let expected_ids = commits
                .iter()
                .map(|block| block.block_id)
                .collect::<Vec<_>>();
            let check = |blocks: FileBlocks, workers: &[u32]| {
                assert_eq!(blocks.status.len, count * 1_048_576);
                assert_eq!(
                    blocks
                        .block_locs
                        .iter()
                        .map(|block| block.block.id)
                        .collect::<Vec<_>>(),
                    expected_ids
                );
                for block in blocks.block_locs {
                    assert_eq!(block.block.len, 1_048_576);
                    assert_eq!(
                        block
                            .locs
                            .iter()
                            .map(|loc| loc.worker_id)
                            .collect::<Vec<_>>(),
                        workers
                    );
                }
            };
            check(fs.get_block_locations(path).unwrap(), &[100, 200]);
            check(
                fs.open_file(path, CreateFileOpts::with_create(false), OpenFlags::new(0))
                    .unwrap(),
                &[100, 200],
            );
            let details = fs.get_file_block_details(path).unwrap();
            assert_eq!(
                details
                    .blocks
                    .iter()
                    .map(|block| block.block_id)
                    .collect::<Vec<_>>(),
                expected_ids
            );

            fs.add_test_worker(worker(100, WorkerStatus::Blacklist));
            check(fs.get_block_locations(path).unwrap(), &[200]);
            fs.add_test_worker(worker(200, WorkerStatus::Decommission));
            if count == 0 {
                check(fs.get_block_locations(path).unwrap(), &[]);
            } else {
                assert!(fs
                    .get_block_locations(path)
                    .unwrap_err()
                    .to_string()
                    .contains("all replicas has been lost"));
            }
        }
    }

    #[test]
    fn replication_state_rejects_deleted_block() {
        let fs = test_fs("replication-state-deleted");
        let path = "/replication-state-deleted.log";
        let block_id = replication_file(&fs, path);
        assert!(fs
            .fs_dir
            .read()
            .replication_block_state(block_id)
            .unwrap()
            .is_some());

        fs.delete(path, false).unwrap();
        let mut fs_dir = fs.fs_dir.write();
        assert!(fs_dir.replication_block_state(block_id).unwrap().is_none());
        assert!(!fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
            .unwrap());
    }

    #[test]
    fn replication_state_rejects_overwritten_block() {
        let fs = test_fs("replication-state-overwritten");
        let path = "/replication-state-overwritten.log";
        let old_block_id = replication_file(&fs, path);

        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();

        let mut fs_dir = fs.fs_dir.write();
        assert!(fs_dir
            .replication_block_state(old_block_id)
            .unwrap()
            .is_none());
        assert!(!fs_dir
            .add_replication_location_if_needed(old_block_id, BlockLocation::with_id(200))
            .unwrap());
    }

    #[test]
    fn replication_location_update_is_idempotent_and_respects_replica_count() {
        let fs = test_fs("replication-location-guard");
        let block_id = replication_file(&fs, "/replication-location-guard.log");
        let mut fs_dir = fs.fs_dir.write();

        assert!(fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
            .unwrap());
        assert!(
            !fs_dir
                .add_replication_location_if_needed(
                    block_id,
                    BlockLocation::new(200, StorageType::Ssd),
                )
                .unwrap()
        );
        assert!(!fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(300))
            .unwrap());

        let state = fs_dir.replication_block_state(block_id).unwrap().unwrap();
        assert_eq!(state.replicas, 2);
        assert_eq!(state.locations.len(), 2);
        assert_eq!(
            state
                .locations
                .iter()
                .map(|location| location.worker_id)
                .collect::<Vec<_>>(),
            vec![100, 200]
        );
    }

    #[test]
    fn resolve_file_inode_missing_inode_id_returns_file_not_found() {
        let fs = test_fs("missing-inode-id");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let missing_id = 9_999_999_i64;
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, "/missing", Some(missing_id))
            .unwrap_err();

        assert_file_not_found_roundtrip(&err);
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            msg
        );
    }

    #[test]
    fn resolve_file_inode_unresolved_path_returns_file_not_found() {
        let fs = test_fs("unresolved-path");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let path = "/does/not/exist";
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, path, None).unwrap_err();

        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string().contains(path),
            "expected path in error, got: {}",
            err
        );
    }

    #[test]
    fn get_inode_path_follows_parent_links() {
        let fs = test_fs("get-inode-path");
        let status = fs.create("/dir/file.log", true).unwrap();
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/file.log");
    }

    #[test]
    fn get_inode_path_after_rename_returns_new_path() {
        let fs = test_fs("get-inode-path-rename");
        let status = fs.create("/dir/old.log", true).unwrap();
        fs.rename("/dir/old.log", "/dir/new.log", RenameFlags::empty())
            .unwrap();

        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/new.log");
    }

    #[test]
    fn cv_metadata_snapshot_page_walks_live_children_and_loads_file_entries() {
        let fs = test_fs("snapshot-page-borrow");
        let source = fs.create("/a/b.txt", true).unwrap();
        fs.create("/a/c/d.txt", true).unwrap();
        fs.create("/z.txt", true).unwrap();
        fs.link("/a/b.txt", "/a/b-link.txt").unwrap();

        let mut token = None;
        let mut paths = Vec::new();
        let mut pages = 0;
        loop {
            let page = fs.cv_metadata_snapshot_page(token, 2).unwrap();
            assert!(pages < 8, "snapshot pagination did not finish");
            pages += 1;
            assert!(page.entries.len() <= 2);
            paths.extend(page.entries.iter().map(|entry| entry.status.path.clone()));
            match page.next_page_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }

        assert_eq!(
            paths,
            vec![
                "/".to_string(),
                "/a".to_string(),
                "/a/b-link.txt".to_string(),
                "/a/b.txt".to_string(),
                "/a/c".to_string(),
                "/a/c/d.txt".to_string(),
                "/z.txt".to_string(),
            ]
        );

        let full = fs.cv_metadata_snapshot_page(None, 100).unwrap();
        assert!(full.next_page_token.is_none());
        let dir = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a")
            .unwrap();
        assert!(dir.status.is_dir);
        assert!(dir.blocks.is_none());
        assert_eq!(dir.status.children_num, 3);

        let link = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a/b-link.txt")
            .unwrap();
        let file = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a/b.txt")
            .unwrap();
        assert_eq!(link.status.name, "b-link.txt");
        assert_eq!(link.status.id, source.id);
        assert_eq!(file.status.id, source.id);
        assert!(link.blocks.is_some());
        assert!(file.blocks.is_some());
    }

    #[test]
    fn file_status_by_id_after_rename_resolves_inode_keeps_request_path() {
        // By-id status resolves the inode (id/name) but keeps the request path.
        let fs = test_fs("file-status-by-id-stale-path");
        let created = fs.create("/a/old.log", true).unwrap();
        fs.rename("/a/old.log", "/a/new.log", RenameFlags::empty())
            .unwrap();

        let status = fs
            .file_status_by_id("/a/old.log", Some(created.id))
            .unwrap();
        assert_eq!(status.id, created.id);
        assert_eq!(status.path, "/a/old.log");
        assert_eq!(status.name, "new.log");

        // Path-based status still requires the live path.
        let by_path = fs.file_status("/a/new.log").unwrap();
        assert_eq!(by_path.id, created.id);
        assert_eq!(by_path.path, "/a/new.log");

        // Write path still journals/returns the request path (no hot-path rebuild).
        let blocks = fs
            .resize_by_id(
                "/a/old.log",
                Some(created.id),
                FileAllocOpts::with_truncate(128),
            )
            .unwrap();
        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/a/old.log");
        assert_eq!(blocks.status.name, "new.log");
        assert_eq!(blocks.status.len, 128);
    }

    #[test]
    fn file_status_by_id_missing_inode_returns_file_not_found() {
        let fs = test_fs("file-status-missing-inode");
        let missing_id = 9_999_999_i64;
        let err = fs
            .file_status_by_id("/missing.log", Some(missing_id))
            .unwrap_err();
        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string()
                .contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            err
        );
    }

    #[test]
    fn complete_file_by_id_keeps_request_path() {
        let fs = test_fs("complete-by-id-stale-path");
        let created = fs.create("/b/old.log", true).unwrap();
        fs.rename("/b/old.log", "/b/new.log", RenameFlags::empty())
            .unwrap();

        let blocks = fs
            .complete_file(
                "/b/old.log",
                Some(created.id),
                64,
                vec![],
                "test-client",
                true,
                None,
            )
            .unwrap()
            .expect("only_flush should return FileBlocks");

        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/b/old.log");
        assert_eq!(blocks.status.name, "new.log");
    }

    fn worker_with_status(
        id: u32,
        status: WorkerStatus,
        capacity: i64,
        available: i64,
    ) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            1,
        );
        worker.status = status;
        worker.capacity = capacity;
        worker.available = available;
        worker
    }

    #[test]
    fn filesystem_info_allocatable_only_counts_live_workers() {
        // See issue #1460: statfs must report capacity eligible for new writes,
        // i.e. only Live workers. Blacklist/Decommission workers must not
        // contribute to the allocatable view (they still count toward the total
        // physical capacity).
        let fs = test_fs("allocatable-live-only");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.add_test_worker(worker_with_status(2, WorkerStatus::Blacklist, 2000, 1500));
        fs.add_test_worker(worker_with_status(
            3,
            WorkerStatus::Decommission,
            3000,
            2500,
        ));

        let info = fs.filesystem_info().unwrap();

        // Total physical capacity across all non-lost worker states.
        assert_eq!(info.capacity, 6000, "total capacity should sum all workers");
        assert_eq!(
            info.available, 4800,
            "total available should sum all workers"
        );
        assert_eq!(info.live_workers.len(), 1);
        assert_eq!(info.blacklist_workers.len(), 1);
        assert_eq!(info.decommission_workers.len(), 1);

        // Allocatable view mirrors allocation eligibility: Live workers only.
        assert_eq!(
            info.allocatable_capacity, 1000,
            "allocatable capacity must be Live workers only"
        );
        assert_eq!(
            info.allocatable_available, 800,
            "allocatable available must be Live workers only"
        );
    }

    #[test]
    fn filesystem_info_allocatable_excludes_failed_storage_dirs() {
        // A Live worker with a healthy dir plus a failed dir: add_storage already
        // skips failed dirs, so neither total nor allocatable capacity should
        // include the failed dir's bytes.
        let fs = test_fs("allocatable-excludes-failed");
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 10,
                ..Default::default()
            },
            1,
        );
        worker.status = WorkerStatus::Live;
        worker.add_storage(StorageInfo {
            dir_id: 1,
            storage_id: "healthy".into(),
            failed: false,
            capacity: 1000,
            available: 800,
            ..Default::default()
        });
        worker.add_storage(StorageInfo {
            dir_id: 2,
            storage_id: "failed".into(),
            failed: true,
            capacity: 500,
            available: 400,
            ..Default::default()
        });
        fs.add_test_worker(worker);

        let info = fs.filesystem_info().unwrap();

        assert_eq!(
            info.capacity, 1000,
            "failed dir must not count toward total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(info.allocatable_capacity, 1000);
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn filesystem_info_allocatable_excludes_lost_workers() {
        // Lost workers live in a separate map and must not contribute to either
        // the total or the allocatable view. Insert a lost worker directly into
        // the lost map (add_test_worker only touches the live map) and assert its
        // capacity never leaks into aggregation.
        let fs = test_fs("allocatable-excludes-lost");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.worker_manager
            .write()
            .worker_map
            .lost_workers
            .insert(99, worker_with_status(99, WorkerStatus::Lost, 9999, 9999));

        let info = fs.filesystem_info().unwrap();

        // The lost worker is reported but its bytes stay out of both views.
        assert_eq!(info.lost_workers.len(), 1);
        assert_eq!(
            info.capacity, 1000,
            "lost worker capacity must not leak into total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(
            info.allocatable_capacity, 1000,
            "lost worker capacity must not leak into allocatable"
        );
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn worker_info_rest_json_omits_scheduled_fields() {
        let mut worker = worker_with_status(1, WorkerStatus::Live, 1000, 800);
        worker.scheduled_bytes = 128;
        worker.scheduled_since_ms = 99;
        let json = serde_json::to_string(&worker).unwrap();
        assert!(!json.contains("scheduled_bytes"));
        assert!(!json.contains("scheduled_since_ms"));
    }

    #[test]
    fn assign_worker_retry_does_not_leak_scheduled_bytes() {
        let fs = test_fs("assign-worker-retry-unschedule");
        let capacity = 1 << 30;
        fs.add_test_worker(worker_with_status(
            1,
            WorkerStatus::Live,
            capacity,
            capacity,
        ));

        let created = fs.create("/retry.log", true).unwrap();
        let blocks = fs
            .resize(
                "/retry.log",
                FileAllocOpts::with_alloc(created.block_size, FileAllocMode::DEFAULT),
            )
            .unwrap();
        let located = &blocks.block_locs[0];
        assert!(located.block.alloc_opts.is_some());
        assert!(located.locs.is_empty());
        let block = located.block.clone();
        let client = ClientAddress::default();

        fs.assign_worker("/retry.log", block.clone(), client.clone(), vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            created.block_size
        );

        // Idempotent retry must not stack a second reservation on top of the
        // first successful assignment.
        fs.assign_worker("/retry.log", block.clone(), client.clone(), vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            created.block_size
        );

        // After heartbeat reports the write, leftover reservation is gone.
        // Another retry still must not leak scheduled_bytes.
        {
            let mut wm = fs.worker_manager.write();
            let worker = wm.worker_map.workers.get_mut(&1).unwrap();
            let prev = worker.available;
            worker.available = prev - created.block_size;
            worker.reclaim_scheduled_from_heartbeat(prev);
            assert_eq!(worker.scheduled_bytes, 0);
        }

        fs.assign_worker("/retry.log", block, client, vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            0
        );
    }

    #[test]
    fn soft_delete_moves_file_into_trash_with_origin_metadata() {
        let fs = test_fs_with_trash("soft-file");
        fs.create("/data/a.log", true).unwrap();

        fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
            .unwrap();

        assert!(matches!(
            fs.get_block_locations("/data/a.log").unwrap_err(),
            FsError::FileNotFound(_)
        ));

        let mut entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.remove(0);
        assert_eq!(
            entry
                .x_attr
                .get(trash::TRASH_XATTR_ORIGIN)
                .map(Vec::as_slice),
            Some(b"/data/a.log".as_slice())
        );
        let delete_ms = i64::from_le_bytes(
            entry
                .x_attr
                .get(trash::TRASH_XATTR_DELETE_MS)
                .unwrap()
                .as_slice()
                .try_into()
                .unwrap(),
        );
        assert!(delete_ms > 0);
        assert!(entry.name.starts_with(&format!("{}-", delete_ms)));
    }

    #[test]
    fn soft_delete_moves_directory_subtree_into_trash() {
        let fs = test_fs_with_trash("soft-dir");
        fs.create("/dir/child.log", true).unwrap();

        let err = fs
            .delete_with_mode("/dir", false, DeleteMode::Default)
            .unwrap_err();
        assert!(matches!(err, FsError::DirNotEmpty(_)), "got {:?}", err);

        fs.delete_with_mode("/dir", true, DeleteMode::Default)
            .unwrap();

        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        assert_eq!(entries.len(), 1);
        let trashed_child = format!("/.Trash/{}/child.log", entries[0].name);
        assert!(fs.get_block_locations(&trashed_child).is_ok());
        assert!(fs.get_block_locations("/dir/child.log").is_err());
    }

    #[test]
    fn deleting_a_hard_link_keeps_hard_unlink_semantics() {
        let fs = test_fs_with_trash("hardlink");
        fs.create("/a.log", true).unwrap();
        fs.link("/a.log", "/b.log").unwrap();

        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();

        assert!(fs.get_block_locations("/b.log").is_ok());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn skip_trash_deletes_permanently() {
        let fs = test_fs_with_trash("skip-trash");
        fs.create("/keep.log", true).unwrap();

        fs.delete_with_mode("/keep.log", false, DeleteMode::SkipTrash)
            .unwrap();

        assert!(fs.get_block_locations("/keep.log").is_err());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn soft_delete_falls_back_to_hard_delete_when_disabled() {
        let fs = test_fs("trash-disabled");
        fs.create("/a.log", true).unwrap();

        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();

        assert!(fs.get_block_locations("/a.log").is_err());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn trash_move_and_purge_transfer_directory_link_counts() {
        let fs = test_fs_with_trash("trash-nlink");
        fs.mkdir("/parent", true).unwrap();
        fs.mkdir("/parent/child", true).unwrap();
        let parent_nlink = fs.file_status("/parent").unwrap().nlink;

        fs.delete_with_mode("/parent/child", true, DeleteMode::Default)
            .unwrap();

        assert_eq!(
            fs.file_status("/parent").unwrap().nlink,
            parent_nlink - 1,
            "the source parent must drop the moved directory link"
        );
        let mut entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let trash_nlink = fs.file_status("/.Trash").unwrap().nlink;
        let entry_path = format!("/.Trash/{}", entries.remove(0).name);

        fs.delete_with_mode(&entry_path, true, DeleteMode::Default)
            .unwrap();

        assert_eq!(
            fs.file_status("/.Trash").unwrap().nlink,
            trash_nlink - 1,
            "purging the entry must drop the trash root link"
        );
    }

    #[test]
    fn client_set_attr_cannot_touch_internal_xattrs() {
        let fs = test_fs("internal-xattr");
        fs.create("/a.log", true).unwrap();

        for key in [trash::TRASH_XATTR_ORIGIN, trash::TRASH_XATTR_DELETE_MS] {
            let err = fs
                .set_attr(
                    "/a.log",
                    SetAttrOptsBuilder::new()
                        .add_x_attr(key, b"x".to_vec())
                        .build(),
                )
                .unwrap_err();
            assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

            let err = fs
                .set_attr(
                    "/a.log",
                    SetAttrOptsBuilder::new().remove_x_attr(key).build(),
                )
                .unwrap_err();
            assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
        }
    }

    #[test]
    fn trash_root_is_protected_and_entries_are_purged_permanently() {
        let fs = test_fs_with_trash("trash-purge");
        fs.create("/a.log", true).unwrap();
        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();

        let err = fs
            .delete_with_mode("/.Trash", true, DeleteMode::Default)
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let entry_path = format!("/.Trash/{}", entries[0].name);
        fs.delete_with_mode(&entry_path, true, DeleteMode::Default)
            .unwrap();
        assert!(fs
            .list_options("/.Trash", ListOptions::default())
            .unwrap()
            .is_empty());
    }

    // Expiry is a permanent removal. Sending it through the recycle bin kept
    // the blocks for another retention window while the executor reported the
    // length as reclaimed, so the freed space never showed up.
    #[test]
    fn ttl_expiry_bypasses_the_recycle_bin() {
        use crate::master::meta::inode::ttl::InodeTtlExecutor;

        let fs = test_fs_with_trash("ttl-permanent");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        fs.create_with_opts(
            "/ttl/expired.log",
            opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        fs.complete_file("/ttl/expired.log", None, 10, vec![], "", false, None)
            .unwrap();
        let inode_id = fs.file_status("/ttl/expired.log").unwrap().id;
        std::thread::sleep(std::time::Duration::from_millis(10));

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let (handled, _, reclaimed) = executor.execute_by_id(inode_id).unwrap();

        assert!(handled);
        assert_eq!(reclaimed, 10, "the expired length is reported as reclaimed");
        assert!(fs.file_status("/ttl/expired.log").is_err());
        assert!(
            fs.list_trash().unwrap().is_empty(),
            "an expired file must not become restorable"
        );
    }

    #[test]
    fn ttl_does_not_expunge_trashed_entries() {
        use crate::master::meta::inode::ttl::InodeTtlExecutor;

        let fs = test_fs_with_trash("ttl-trash");
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        fs.create_with_opts(
            "/ttl/expired.log",
            opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        fs.complete_file("/ttl/expired.log", None, 0, vec![], "", false, None)
            .unwrap();
        let inode_id = fs.file_status("/ttl/expired.log").unwrap().id;
        std::thread::sleep(std::time::Duration::from_millis(50));

        fs.delete_with_mode("/ttl/expired.log", false, DeleteMode::Default)
            .unwrap();
        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let trashed_path = format!("/.Trash/{}", entries[0].name);

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let (handled, _, reclaimed) = executor.execute_by_id(inode_id).unwrap();

        assert!(handled);
        assert_eq!(
            reclaimed, 0,
            "a skipped trash entry must not report reclaimed bytes"
        );
        assert!(fs.get_block_locations(&trashed_path).is_ok());
    }

    #[test]
    fn list_trash_reports_origin_time_and_status() {
        let fs = test_fs_with_trash("trash-list");
        fs.create("/a.log", true).unwrap();
        fs.create("/b.log", true).unwrap();
        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();
        fs.delete_with_mode("/b.log", false, DeleteMode::Default)
            .unwrap();

        let entries = fs.list_trash().unwrap();
        assert_eq!(entries.len(), 2);

        let mut origins: Vec<&str> = entries.iter().map(|e| e.origin.as_str()).collect();
        origins.sort();
        assert_eq!(origins, vec!["/a.log", "/b.log"]);

        for entry in &entries {
            assert!(entry.path.starts_with("/.Trash/"));
            assert_eq!(entry.status.path, entry.path);
            assert!(entry.delete_ms > 0);
            assert!(entry
                .status
                .name
                .starts_with(&format!("{}-", entry.delete_ms)));
        }
    }

    #[test]
    fn list_trash_page_resumes_without_repeating_entries() {
        let fs = test_fs_with_trash("trash-list-page");
        for i in 0..250 {
            fs.create(format!("/p-{}.log", i), true).unwrap();
        }
        for i in 0..250 {
            fs.delete_with_mode(format!("/p-{}.log", i), false, DeleteMode::Default)
                .unwrap();
        }

        let mut page_sizes = Vec::new();
        let mut paths = Vec::new();
        let mut cursor = None;
        loop {
            let page = fs.list_trash_page(cursor.as_ref(), 100).unwrap();
            page_sizes.push(page.entries.len());
            paths.extend(page.entries.into_iter().map(|entry| entry.path));
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(page_sizes, vec![100, 100, 50]);
        assert_eq!(paths.len(), 250);
        let unique: HashSet<&String> = paths.iter().collect();
        assert_eq!(unique.len(), 250, "a page must not repeat an entry");

        assert_eq!(fs.list_trash().unwrap().len(), 250);
    }

    #[test]
    fn restore_trash_reports_the_effective_destination_and_status() {
        let fs = test_fs_with_trash("trash-restore-outcome");
        fs.create("/a.log", true).unwrap();
        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();
        let entry = fs.list_trash().unwrap().remove(0);

        assert_eq!(fs.restore_dst(&entry.path, None).unwrap(), "/a.log");
        assert_eq!(
            fs.restore_dst(&entry.path, Some("/b.log")).unwrap(),
            "/b.log"
        );

        let restored = fs.restore_trash(&entry.path, None, false).unwrap();
        assert_eq!(restored.dst, "/a.log");
        assert_eq!(restored.status.path, "/a.log");
        assert!(restored.replaced.is_none());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_without_dst_reports_a_missing_origin_parent() {
        let fs = test_fs_with_trash("trash-restore-missing-parent");
        fs.mkdir("/data", true).unwrap();
        fs.create("/data/a.log", true).unwrap();
        fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
            .unwrap();
        fs.delete_with_mode("/data", true, DeleteMode::Default)
            .unwrap();

        let entry = fs
            .list_trash()
            .unwrap()
            .into_iter()
            .find(|entry| entry.origin == "/data/a.log")
            .unwrap();

        let err = fs.restore_trash(&entry.path, None, false).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {err:?}");
        assert!(
            err.to_string().contains("parent directory"),
            "message should explain the missing parent, got {err}"
        );

        let err = fs
            .restore_trash(&entry.path, Some("/data/a.log"), false)
            .unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {err:?}");
    }

    #[test]
    fn restore_trash_moves_entry_back_to_origin() {
        let fs = test_fs_with_trash("trash-restore");
        fs.create("/data/a.log", true).unwrap();
        fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
            .unwrap();

        let entry = fs.list_trash().unwrap().remove(0);
        assert!(fs.get_block_locations(&entry.path).is_ok());

        fs.restore_trash(&entry.path, None, false).unwrap();

        assert!(fs.get_block_locations("/data/a.log").is_ok());
        assert!(fs.get_block_locations(&entry.path).is_err());
        assert!(fs.list_trash().unwrap().is_empty());
        let status = fs.file_status("/data/a.log").unwrap();
        assert!(!status.x_attr.contains_key(trash::TRASH_XATTR_ORIGIN));
        assert!(!status.x_attr.contains_key(trash::TRASH_XATTR_DELETE_MS));
    }

    #[test]
    fn restore_trash_restores_directory_subtrees() {
        let fs = test_fs_with_trash("trash-restore-dir");
        fs.create("/data/child.log", true).unwrap();
        fs.delete_with_mode("/data", true, DeleteMode::Default)
            .unwrap();

        let entry = fs.list_trash().unwrap().remove(0);
        fs.restore_trash(&entry.path, None, false).unwrap();

        assert!(fs.get_block_locations("/data/child.log").is_ok());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_validates_destination_and_overwrites_on_request() {
        let fs = test_fs_with_trash("trash-restore-dst");
        fs.create("/src.log", true).unwrap();
        fs.delete_with_mode("/src.log", false, DeleteMode::Default)
            .unwrap();
        let entry = fs.list_trash().unwrap().remove(0);

        let err = fs
            .restore_trash(&entry.path, Some("/.Trash/other"), false)
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .restore_trash(&entry.path, Some("/missing/src.log"), false)
            .unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        fs.mkdir("/dst", true).unwrap();
        fs.create("/dst/src.log", true).unwrap();
        let err = fs
            .restore_trash(&entry.path, Some("/dst/src.log"), false)
            .unwrap_err();
        assert!(
            matches!(err, FsError::FileAlreadyExists(_)),
            "got {:?}",
            err
        );
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        fs.restore_trash(&entry.path, Some("/dst/src.log"), true)
            .unwrap();
        assert!(fs.get_block_locations("/dst/src.log").is_ok());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_rejects_paths_outside_the_trash() {
        let fs = test_fs_with_trash("trash-restore-invalid");
        fs.create("/plain.log", true).unwrap();

        for path in ["/plain.log", "/.Trash", "/.Trash/", "/.Trash/a/b"] {
            let err = fs.restore_trash(path, None, false).unwrap_err();
            assert!(
                matches!(err, FsError::InvalidArgument(_)),
                "{} -> {:?}",
                path,
                err
            );
        }

        let err = fs
            .restore_trash("/.Trash/missing", None, false)
            .unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
    }

    #[test]
    fn empty_trash_purges_entries_but_keeps_the_trash_root() {
        let fs = test_fs_with_trash("trash-empty");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        let empty = fs.empty_trash().unwrap();
        assert_eq!(empty.inodes, 0);
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());

        fs.create("/dir/child.log", true).unwrap();
        fs.create("/keep.log", true).unwrap();
        fs.delete_with_mode("/dir", true, DeleteMode::Default)
            .unwrap();
        fs.delete_with_mode("/keep.log", false, DeleteMode::Default)
            .unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 2);

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 2);
        // Every visible trash entry is gone, and the internal async-staging
        // directory stays behind without being listed to clients.
        assert!(fs.list_trash().unwrap().is_empty());
        let children: Vec<String> = fs
            .list_options("/.Trash", ListOptions::default())
            .unwrap()
            .into_iter()
            .map(|child| child.name)
            .collect();
        assert!(
            children.is_empty(),
            "the staging dir must be hidden: {:?}",
            children
        );
        assert!(fs.get_block_locations("/dir/child.log").is_err());

        fs.create("/next.log", true).unwrap();
        fs.delete_with_mode("/next.log", false, DeleteMode::Default)
            .unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 1);
    }

    #[test]
    fn empty_trash_does_not_purge_foreign_children() {
        let fs = test_fs_with_trash("trash-empty-foreign");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        fs.create("/keep.log", true).unwrap();
        fs.delete_with_mode("/keep.log", false, DeleteMode::Default)
            .unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        // A child whose name is not a trash entry name is not shown by
        // ListTrash, so EmptyTrash must leave it alone instead of deleting it
        // silently and without an undo path.
        let (foreign_path, _) = seed_trash_entry(&fs, "not-a-trash-entry");

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 1);
        assert!(fs.get_block_locations(&foreign_path).is_ok());
        let children = fs
            .list_options(trash::TRASH_ROOT, ListOptions::default())
            .unwrap();
        assert!(
            children
                .iter()
                .any(|child| child.name == "not-a-trash-entry"),
            "foreign child must survive EmptyTrash: {:?}",
            children.iter().map(|child| &child.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_trash_purges_healthy_entries_when_one_entry_is_corrupt() {
        let fs = test_fs_with_trash("trash-empty-partial");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        let worker_id = 100;
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);

        fs.create("/good.log", true).unwrap();
        let block = fs
            .add_block(
                "/good.log",
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
        let block_id = block.block.id;
        fs.fs_dir
            .write()
            .add_block_location(block_id, BlockLocation::with_id(worker_id))
            .unwrap();
        fs.complete_file(
            "/good.log",
            None,
            1,
            vec![CommitBlock {
                block_id,
                block_len: 1,
                locations: vec![BlockLocation::with_id(worker_id)],
            }],
            "",
            false,
            None,
        )
        .unwrap();
        fs.delete_with_mode("/good.log", false, DeleteMode::Default)
            .unwrap();

        fs.create("/broken.log", true).unwrap();
        fs.delete_with_mode("/broken.log", false, DeleteMode::Default)
            .unwrap();

        // Drop the store record of one trash entry so it can no longer be
        // resolved. Staging skips it instead of failing the sweep, and the
        // healthy entry must still be purged with its blocks released.
        let entries = fs.list_trash().unwrap();
        let broken_id = entries
            .iter()
            .find(|entry| entry.origin == "/broken.log")
            .unwrap()
            .status
            .id;
        let good_entry = entries
            .iter()
            .find(|entry| entry.origin == "/good.log")
            .unwrap()
            .path
            .clone();
        let fs_dir = fs.fs_dir.read();
        let mut batch = fs_dir.store.new_batch();
        batch.delete_inode(broken_id).unwrap();
        batch.commit().unwrap();
        drop(fs_dir);

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 1, "only the healthy entry is reclaimed");
        assert!(fs.get_block_locations("/good.log").is_err());
        assert!(
            !fs.exists(&good_entry).unwrap(),
            "the healthy entry must leave the recycle bin"
        );

        let cmds = fs
            .worker_manager
            .write()
            .block_map
            .handle_heartbeat(worker_id);
        let pending: HashSet<i64> = cmds
            .iter()
            .flat_map(|cmd| match cmd {
                WorkerCommand::DeleteBlock(cmd) => cmd.blocks.clone(),
            })
            .collect();
        assert!(
            pending.contains(&block_id),
            "block {block_id} was not queued for removal: {pending:?}"
        );
    }

    #[test]
    fn trash_namespace_is_reserved_for_creates() {
        let fs = test_fs("reserved-create");

        let err = fs.create("/.Trash/entry.log", true).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs.mkdir("/.Trash/entry-dir", true).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        // Names that merely start with the reserved prefix are allowed.
        fs.mkdir("/.Trashy", true).unwrap();
    }

    #[test]
    fn trash_namespace_is_reserved_for_rename() {
        let fs = test_fs("reserved-rename");
        fs.mkdir("/data", true).unwrap();

        let err = fs
            .rename("/data", "/.Trash/moved", RenameFlags::empty())
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .rename("/.Trash", "/moved", RenameFlags::empty())
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn trash_namespace_is_reserved_for_links() {
        let fs = test_fs("reserved-links");
        fs.create("/data.log", true).unwrap();

        let err = fs.link("/data.log", "/.Trash/link.log").unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .symlink("/data.log", "/.Trash/sym.log", false, 0o777)
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn trash_namespace_rejects_writes_but_allows_reads() {
        let fs = test_fs("reserved-write");
        fs.create("/data.log", true).unwrap();

        let err = fs
            .open_file(
                "/.Trash/new.log",
                CreateFileOpts::with_create(false),
                OpenFlags::new_create(),
            )
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        // Read-only lookups do not touch the reserved namespace guard.
        let err = fs.get_block_locations("/.Trash/absent.log").unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
    }

    #[test]
    fn trash_root_cannot_be_deleted() {
        let fs = test_fs("reserved-delete-root");

        let err = fs.delete(trash::TRASH_ROOT, true).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn trash_entries_remain_permanently_deletable() {
        let fs = test_fs("reserved-delete-entry");
        let (path, _) = seed_trash_entry(&fs, "entry.log");

        fs.delete(&path, false).unwrap();

        let err = fs.get_block_locations(&path).unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
    }

    #[test]
    fn trash_namespace_is_reserved_for_free_and_set_attr() {
        let fs = test_fs("reserved-free-attr");
        let (path, _) = seed_trash_entry(&fs, "entry.log");

        for (label, err) in [
            ("free entry", fs.free(&path, false).unwrap_err()),
            ("free root", fs.free(trash::TRASH_ROOT, true).unwrap_err()),
            (
                "set_attr entry",
                fs.set_attr(&path, SetAttrOptsBuilder::new().mode(0o600).build())
                    .unwrap_err(),
            ),
            (
                "set_attr root",
                fs.set_attr(
                    trash::TRASH_ROOT,
                    SetAttrOptsBuilder::new().mode(0o600).build(),
                )
                .unwrap_err(),
            ),
        ] {
            assert!(
                matches!(err, FsError::InvalidArgument(_)),
                "{}: got {:?}",
                label,
                err
            );
        }

        // Regular paths keep their previous behavior.
        fs.create("/data.log", true).unwrap();
        fs.set_attr("/data.log", SetAttrOptsBuilder::new().mode(0o600).build())
            .unwrap();
        fs.free("/data.log", false).unwrap();
    }

    #[test]
    fn trash_namespace_is_reserved_for_resize() {
        let fs = test_fs("reserved-resize");
        let (path, inode_id) = seed_trash_entry(&fs, "entry.log");

        let err = fs
            .resize(&path, FileAllocOpts::with_truncate(0))
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        // By-id requests may carry a stale path, so the live path is checked too.
        let err = fs
            .resize_by_id(
                "/data/stale.log",
                Some(inode_id),
                FileAllocOpts::with_truncate(0),
            )
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let created = fs.create("/data.log", true).unwrap();
        let blocks = fs
            .resize_by_id(
                "/data.log",
                Some(created.id),
                FileAllocOpts::with_truncate(128),
            )
            .unwrap();
        assert_eq!(blocks.status.len, 128);
    }

    // Journal enabled in testing mode: entries stay on the writer's channel and
    // every journaled mutation emits a leader checkpoint.
    fn test_fs_snapshot_every_entry(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = true;
        conf.journal.leader_create_snapshot = Some(true);
        conf.journal.snapshot_entries = 1;
        conf.journal.snapshot_min_interval = curvine_runtime::common::DurationUnit::new(0);
        conf.master.trash_enabled = true;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-purge-snapshot/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-purge-snapshot/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        fs
    }

    // Regression: the leader checkpoint emitted while journaling a purge batch
    // must already exclude the purged inodes. Journaling before applying left
    // the victims in a checkpoint whose watermark covered the purge, so a
    // recovery from it resurrected metadata for released blocks.
    #[test]
    fn purge_checkpoint_excludes_the_purged_inodes() {
        use crate::master::fs::TrashPurger;
        use crate::master::journal::JournalEntry;
        use crate::master::meta::store::RocksInodeStore;

        let fs = test_fs_snapshot_every_entry("apply-before-log");
        let victim = fs.create("/data/victim.log", true).unwrap();
        fs.delete_with_mode("/data/victim.log", false, DeleteMode::Default)
            .unwrap();
        let entry = fs.list_trash().unwrap().remove(0);
        let writer = fs.fs_dir.read().journal_writer.clone();
        writer.take_entries();

        TrashPurger::new(fs.clone(), fs.master_monitor.clone())
            .purge_entry(&entry.path)
            .unwrap()
            .expect("leader");

        let entries = writer.take_entries();
        let purge_at = entries
            .iter()
            .position(|e| matches!(e, JournalEntry::TrashPurge(_)))
            .expect("purge must be journaled");
        let checkpoint = entries[purge_at..]
            .iter()
            .find_map(|e| match e {
                JournalEntry::Snapshot(s) => Some(s.dir.clone()),
                _ => None,
            })
            .expect("snapshot_entries = 1 emits a checkpoint after the purge record");

        // Open the checkpoint itself as a store (a separate scratch dir keeps its
        // own checkpoints away from the live DB).
        let mut db_conf = fs.conf.rocksdb.clone().set_dir(Utils::test_sub_dir(format!(
            "master-fs-purge-snapshot/open-{}",
            Utils::rand_str(6)
        )));
        db_conf.data_dir = checkpoint.clone();
        let store = RocksInodeStore::new(db_conf, false).unwrap();
        assert!(
            store.get_inode(victim.id).unwrap().is_none(),
            "checkpoint {} still holds purged inode {}",
            checkpoint,
            victim.id
        );
    }

    // `delete()` serves TTL eviction and quota, which count the inode's bytes as
    // reclaimed; it must bypass the recycle bin even when trash is enabled.
    #[test]
    fn delete_bypasses_the_recycle_bin_when_trash_is_enabled() {
        let fs = test_fs_with_trash("delete-skips-trash");
        fs.create("/data/ttl.log", true).unwrap();

        let res = fs.delete("/data/ttl.log", false).unwrap();

        assert!(!res.trashed, "delete() must not soft-delete");
        assert!(fs.list_trash().unwrap().is_empty());
        assert!(!fs.exists(trash::TRASH_ROOT).unwrap());
    }

    // `/.Trash/.pending` is master-owned staging: no client RPC may stat, list,
    // open, rename, restore or synchronously delete it.
    #[test]
    fn pending_staging_dir_is_not_addressable_by_clients() {
        let fs = test_fs_with_trash("pending-hidden");
        fs.create("/kept/k.log", true).unwrap();
        fs.delete_with_mode("/kept", true, DeleteMode::Default)
            .unwrap();
        fs.create("/victim/a.log", true).unwrap();
        fs.delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
            .unwrap();
        let staged = fs.scan_pending_trash(16, None).unwrap().paths.remove(0);
        let staged_child = format!("{}/a.log", staged);

        for path in [
            trash::TRASH_PENDING_ROOT,
            staged.as_str(),
            staged_child.as_str(),
        ] {
            assert!(!fs.exists(path).unwrap(), "{} must not exist", path);
            assert_file_not_found_roundtrip(&fs.file_status(path).unwrap_err());
            assert_file_not_found_roundtrip(&fs.list_status(path).unwrap_err());
            assert_file_not_found_roundtrip(
                &fs.list_options(path, ListOptions::default()).unwrap_err(),
            );
            assert_file_not_found_roundtrip(&fs.get_block_locations(path).unwrap_err());
            assert_file_not_found_roundtrip(&fs.delete(path, true).unwrap_err());
            assert_file_not_found_roundtrip(
                &fs.delete_with_mode(path, true, DeleteMode::AsyncPurge)
                    .unwrap_err(),
            );
            assert!(fs.rename(path, "/moved", RenameFlags::empty()).is_err());
            assert!(fs.restore_trash(path, None, false).is_err());
        }

        // Listing the bin shows the restorable entry but never `.pending`.
        let names = |statuses: Vec<FileStatus>| -> Vec<String> {
            statuses.into_iter().map(|s| s.name).collect()
        };
        let listed = names(fs.list_status(trash::TRASH_ROOT).unwrap());
        assert_eq!(listed.len(), 1, "got {:?}", listed);
        assert!(!listed.iter().any(|n| n == trash::TRASH_PENDING_NAME));
        let listed = names(
            fs.list_options(trash::TRASH_ROOT, ListOptions::default())
                .unwrap(),
        );
        assert_eq!(listed.len(), 1, "got {:?}", listed);
        let globbed = names(fs.list_status("/.Trash/*").unwrap());
        assert!(
            globbed.iter().all(|n| n != "a.log"),
            "glob must not descend into .pending: {:?}",
            globbed
        );

        // The staged entry is still drained by the purger.
        assert_eq!(fs.scan_pending_trash(16, None).unwrap().paths.len(), 1);
    }

    // A blocking empty-trash owns `/.Trash/.pending` from the staging rename to
    // the drain that has to report its totals. The background purger must not
    // reclaim a staged entry in that window, otherwise the blocking caller
    // reports zeros for entries it accepted.
    #[test]
    fn background_purger_yields_to_a_blocking_empty_trash() {
        let fs = test_fs_with_trash("trash-drain-owner");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        fs.create("/owned.log", true).unwrap();
        fs.delete_with_mode("/owned.log", false, DeleteMode::Default)
            .unwrap();

        // Stand in for a blocking empty-trash: it holds the pending directory
        // while it stages and drains.
        let guard = fs.lock_pending_drain();
        assert_eq!(fs.stage_trash_for_purge().unwrap(), 1);

        let purger =
            crate::master::fs::PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone());
        assert_eq!(
            purger.drain_pending().unwrap(),
            Some((0, 0, 0)),
            "the background cycle must skip instead of stealing the entry"
        );
        assert_eq!(
            fs.scan_pending_trash(16, None).unwrap().paths.len(),
            1,
            "the staged entry is still there for the owner"
        );

        // Once the owner releases the directory the next cycle reclaims it.
        drop(guard);
        assert_eq!(purger.drain_pending().unwrap(), Some((1, 1, 0)));
        assert!(fs.scan_pending_trash(16, None).unwrap().paths.is_empty());
    }

    // A blocking empty cannot promise reclaimed totals once it is demoted, so it
    // must fail retryably instead of answering success with zero counts while
    // the accepted entries are still staged.
    #[test]
    fn blocking_empty_trash_fails_retryably_when_demoted() {
        let fs = test_fs_with_trash("trash-empty-demoted");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        fs.create("/lost.log", true).unwrap();
        fs.delete_with_mode("/lost.log", false, DeleteMode::Default)
            .unwrap();

        // Accept the entries while still active, then lose leadership before the
        // drain can run.
        assert_eq!(fs.empty_trash0(false).unwrap().0, 1);
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Follower);

        let err = fs.empty_trash0(true).unwrap_err();
        assert!(
            matches!(err, FsError::NotLeaderMaster(_)),
            "a demoted master must not report a successful empty: {:?}",
            err
        );
        assert_eq!(
            fs.scan_pending_trash(16, None).unwrap().paths.len(),
            1,
            "the accepted entry stays staged for the next active master"
        );

        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        assert_eq!(fs.drain_pending_trash().unwrap().unwrap().inodes, 1);
    }
}
