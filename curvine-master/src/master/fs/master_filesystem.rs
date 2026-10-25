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
use crate::master::fs::full_block_report::{FullReportProgress, FullReportTracker};
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::{InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR};
use crate::master::meta::{
    trash, CacheInvalidationResult, FsDir, RestoredTrashEntry, TrashListing, TrashScanPage,
};

use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::*;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::ArcRwLock;
use log::{error, info, warn};
use parking_lot::Mutex;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
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

const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;

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
        Self {
            fs_dir,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
        }
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        Self {
            fs_dir: js.fs().fs_dir.clone(),
            worker_manager: js.worker_manager(),
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
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
    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        self.delete_with_opts(path, recursive, true)
    }

    pub fn delete_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
        skip_trash: bool,
    ) -> FsResult<DeleteResult> {
        let mut delete_res = {
            let mut fs_dir = self.fs_dir.write();
            self.delete_locked(&mut fs_dir, path.as_ref(), recursive, skip_trash)?
        };
        self.release_deleted_blocks(&mut delete_res);

        Ok(delete_res)
    }

    /// Leader-fenced variant of `delete_with_opts`. The role is re-checked
    /// while holding the same write lock that guards the metadata mutation, so
    /// a demotion cannot interleave between the check and the delete. Returns
    /// `None` when this node is no longer the active master.
    pub fn delete_with_opts_if_active<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
        skip_trash: bool,
    ) -> FsResult<Option<DeleteResult>> {
        let mut delete_res = {
            let mut fs_dir = self.fs_dir.write();
            if !self.master_monitor.is_active() {
                return Ok(None);
            }
            self.delete_locked(&mut fs_dir, path.as_ref(), recursive, skip_trash)?
        };
        self.release_deleted_blocks(&mut delete_res);

        Ok(Some(delete_res))
    }

    fn delete_locked(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        recursive: bool,
        skip_trash: bool,
    ) -> FsResult<DeleteResult> {
        let inp = Self::resolve_path(fs_dir, path)?;

        // Removing the root would orphan every trashed entry. Entries below it
        // stay permanently deletable so the recycle bin can be emptied.
        if inp.path() == trash::TRASH_ROOT {
            return err_ext!(FsError::invalid_argument(
                "the recycle bin root cannot be deleted"
            ));
        }

        // Entries already inside /.Trash are always purged permanently.
        let soft_delete =
            !skip_trash && self.conf.trash_enabled && !trash::is_trash_path(inp.path());

        let mut delete_res = if soft_delete {
            fs_dir.trash(&inp, recursive)?
        } else {
            fs_dir.delete(&inp, recursive)?
        };
        delete_res.trashed = soft_delete;
        Ok(delete_res)
    }

    fn release_deleted_blocks(&self, delete_res: &mut DeleteResult) {
        let blocks = std::mem::take(&mut delete_res.blocks);
        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks,
            trashed: false,
        });
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

    pub fn empty_trash(&self) -> FsResult<DeleteResult> {
        // Snapshot the names once so the purge releases the write lock between
        // batches instead of blocking every metadata mutation for the whole
        // purge. Entries added after the snapshot stay for the next purge.
        const BATCH_SIZE: usize = 1000;
        let names = self.fs_dir.read().trash_entry_names()?;

        let mut total = DeleteResult::new();
        let mut first_err = None;
        for batch in names.chunks(BATCH_SIZE) {
            let (mut delete_res, res) = {
                let mut fs_dir = self.fs_dir.write();
                fs_dir.empty_trash_batch(batch)
            };

            // Entries deleted before a failure have already left the namespace,
            // so their block locations must be released even when the purge
            // fails partway through the batch.
            self.worker_manager.write().remove_blocks(&DeleteResult {
                inodes: 0,
                bytes: 0,
                blocks: std::mem::take(&mut delete_res.blocks),
                trashed: false,
            });

            total.inodes += delete_res.inodes;
            total.bytes += delete_res.bytes;
            if let Err(e) = res {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(total),
        }
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
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        Ok(inode.to_file_status(path)?)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Ok(inp.get_last_inode().is_some())
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path.as_ref());
        if is_glob_pattern {
            let paths = Self::resolve_path_by_glob_pattern(&fs_dir, path.as_ref())?;
            let mut all_statuses = Vec::new();
            for path in &paths {
                let statuses = fs_dir.list_status(path)?;
                all_statuses.extend(statuses);
            }
            Ok(all_statuses)
        } else {
            let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
            fs_dir.list_status(&inp)
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
            fs_dir.list_options(&inp, &opts)
        }
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
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
                let inp = Self::resolve_path(fs_dir, path)?;
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
            FullReportProgress::Complete(blocks) => Some(blocks),
            FullReportProgress::Partial => None,
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
                    blocks,
                    now,
                ),
            }
        }
        applied?;

        Ok(BlockReportResult { delete_blocks })
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
        if batch.is_empty() {
            return Ok(());
        }

        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }
            let mut fs_dir = self.fs_dir.write();
            fs_dir.block_report(chunk)?;
        }
        Ok(())
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
    use curvine_runtime::common::Utils;

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

        fs.delete_with_opts("/data/a.log", false, false).unwrap();

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

        let err = fs.delete_with_opts("/dir", false, false).unwrap_err();
        assert!(matches!(err, FsError::DirNotEmpty(_)), "got {:?}", err);

        fs.delete_with_opts("/dir", true, false).unwrap();

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

        fs.delete_with_opts("/a.log", false, false).unwrap();

        assert!(fs.get_block_locations("/b.log").is_ok());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn skip_trash_deletes_permanently() {
        let fs = test_fs_with_trash("skip-trash");
        fs.create("/keep.log", true).unwrap();

        fs.delete_with_opts("/keep.log", false, true).unwrap();

        assert!(fs.get_block_locations("/keep.log").is_err());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn soft_delete_falls_back_to_hard_delete_when_disabled() {
        let fs = test_fs("trash-disabled");
        fs.create("/a.log", true).unwrap();

        fs.delete_with_opts("/a.log", false, false).unwrap();

        assert!(fs.get_block_locations("/a.log").is_err());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn trash_move_and_purge_transfer_directory_link_counts() {
        let fs = test_fs_with_trash("trash-nlink");
        fs.mkdir("/parent", true).unwrap();
        fs.mkdir("/parent/child", true).unwrap();
        let parent_nlink = fs.file_status("/parent").unwrap().nlink;

        fs.delete_with_opts("/parent/child", true, false).unwrap();

        assert_eq!(
            fs.file_status("/parent").unwrap().nlink,
            parent_nlink - 1,
            "the source parent must drop the moved directory link"
        );
        let mut entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let trash_nlink = fs.file_status("/.Trash").unwrap().nlink;
        let entry_path = format!("/.Trash/{}", entries.remove(0).name);

        fs.delete_with_opts(&entry_path, true, false).unwrap();

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
        fs.delete_with_opts("/a.log", false, false).unwrap();

        let err = fs.delete_with_opts("/.Trash", true, false).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let entry_path = format!("/.Trash/{}", entries[0].name);
        fs.delete_with_opts(&entry_path, true, false).unwrap();
        assert!(fs
            .list_options("/.Trash", ListOptions::default())
            .unwrap()
            .is_empty());
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

        fs.delete_with_opts("/ttl/expired.log", false, false)
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
        fs.delete_with_opts("/a.log", false, false).unwrap();
        fs.delete_with_opts("/b.log", false, false).unwrap();

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
            fs.delete_with_opts(format!("/p-{}.log", i), false, false)
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
        fs.delete_with_opts("/a.log", false, false).unwrap();
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
        fs.delete_with_opts("/data/a.log", false, false).unwrap();
        fs.delete_with_opts("/data", true, false).unwrap();

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
        fs.delete_with_opts("/data/a.log", false, false).unwrap();

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
        fs.delete_with_opts("/data", true, false).unwrap();

        let entry = fs.list_trash().unwrap().remove(0);
        fs.restore_trash(&entry.path, None, false).unwrap();

        assert!(fs.get_block_locations("/data/child.log").is_ok());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_validates_destination_and_overwrites_on_request() {
        let fs = test_fs_with_trash("trash-restore-dst");
        fs.create("/src.log", true).unwrap();
        fs.delete_with_opts("/src.log", false, false).unwrap();
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

        let empty = fs.empty_trash().unwrap();
        assert_eq!(empty.inodes, 0);
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());

        fs.create("/dir/child.log", true).unwrap();
        fs.create("/keep.log", true).unwrap();
        fs.delete_with_opts("/dir", true, false).unwrap();
        fs.delete_with_opts("/keep.log", false, false).unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 2);

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 2);
        assert!(fs
            .list_options("/.Trash", ListOptions::default())
            .unwrap()
            .is_empty());
        assert!(fs.get_block_locations("/dir/child.log").is_err());

        fs.create("/next.log", true).unwrap();
        fs.delete_with_opts("/next.log", false, false).unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 1);
    }

    #[test]
    fn empty_trash_releases_blocks_of_entries_purged_before_a_failure() {
        let fs = test_fs_with_trash("trash-empty-partial");
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
        fs.delete_with_opts("/good.log", false, false).unwrap();

        fs.create("/broken.log", true).unwrap();
        fs.delete_with_opts("/broken.log", false, false).unwrap();

        // Drop the store record of one trash entry so purging it fails. The
        // loop must still purge the other entry and report its blocks.
        let broken_id = fs
            .list_trash()
            .unwrap()
            .into_iter()
            .find(|entry| entry.origin == "/broken.log")
            .unwrap()
            .status
            .id;
        let fs_dir = fs.fs_dir.read();
        let mut batch = fs_dir.store.new_batch();
        batch.delete_inode(broken_id).unwrap();
        batch.commit().unwrap();
        drop(fs_dir);

        assert!(fs.empty_trash().is_err());

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
}
