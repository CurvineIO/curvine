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
use crate::master::journal::{JournalEntry, JournalWriter};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::*;
use crate::master::meta::store::{InodeStore, RocksInodeStore};
use crate::master::meta::{BlockMeta, InodeId};
use crate::master::quota::eviction::evictor::Evictor;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    BlockLocation, BlockReportInfo, BlockReportStatus, CommitBlock, CreateFileOpts, ExtendedBlock,
    FileAllocOpts, FileLock, FileStatus, FreeResult, ListOptions, MkdirOpts, MountInfo,
    RenameFlags, SetAttrOpts, TtlAction, WorkerAddress, INTERNAL_CTIME_XATTR,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, LinkedList};
use std::mem;
use std::sync::Arc;

const MODE_SETGID: u32 = 0o2000;

/// Note: The modification operation uses &mut self, which is a necessary improvement. We use the unsafe API to perform modifications.
pub struct FsDir {
    pub(crate) root_dir: InodeView,
    pub(crate) inode_id: InodeId,
    pub(crate) store: InodeStore,
    pub(crate) journal_writer: Arc<JournalWriter>,
    pub(crate) evictor: Arc<dyn Evictor>,
    pub(crate) op_id: AtomicCounter,
}

enum ReportedInodeBlocks {
    File(HashSet<i64>),
    Missing,
    NotFile,
}

const BLOCK_REPORT_LOG_TARGET: &str = "curvine_master::master::fs::master_filesystem";

enum DeferredWritingWarning {
    Missing(i64),
    NotFile(i64),
}

#[derive(Default)]
pub(crate) struct BlockReportDiagnostics {
    missing_blocks: usize,
    not_file_blocks: usize,
    obsolete_file_blocks: usize,
    deferred_writing: Vec<DeferredWritingWarning>,
    failed_inode_block: Option<BlockReportInfo>,
}

impl BlockReportDiagnostics {
    pub(crate) fn extend(&mut self, chunk: Self) {
        self.missing_blocks += chunk.missing_blocks;
        self.not_file_blocks += chunk.not_file_blocks;
        self.obsolete_file_blocks += chunk.obsolete_file_blocks;
    }

    pub(crate) fn log_chunk(&mut self, worker_id: u32, error: Option<&FsError>) {
        for warning in self.deferred_writing.drain(..) {
            match warning {
                DeferredWritingWarning::Missing(id) => warn!(target: BLOCK_REPORT_LOG_TARGET,
                    "block_report deferred deletion for writing block {} on worker {} because its inode is missing",
                    id, worker_id
                ),
                DeferredWritingWarning::NotFile(id) => warn!(target: BLOCK_REPORT_LOG_TARGET,
                    "block_report deferred deletion for writing block {} on worker {} because its inode is not a file",
                    id, worker_id
                ),
            }
        }
        if let (Some(item), Some(e)) = (self.failed_inode_block.take(), error) {
            warn!(target: BLOCK_REPORT_LOG_TARGET, "block_report {item:?}: {e}");
        }
    }

    pub(crate) fn log_summary(&self, worker_id: u32) {
        if self.missing_blocks > 0 || self.not_file_blocks > 0 {
            warn!(target: BLOCK_REPORT_LOG_TARGET,
                "block_report found {} missing-inode and {} non-file-inode blocks for worker {}; scheduling worker deletion",
                self.missing_blocks, self.not_file_blocks, worker_id
            );
        }
        if self.obsolete_file_blocks > 0 {
            warn!(target: BLOCK_REPORT_LOG_TARGET,
                "block_report found {} obsolete blocks in existing files for worker {}; scheduling worker deletion",
                self.obsolete_file_blocks, worker_id
            );
        }
    }
}

#[cfg(test)]
#[path = "shared_report_tests.rs"]
mod shared_report_tests;

#[cfg(test)]
#[path = "shared_report_concurrency_tests.rs"]
mod shared_report_concurrency_tests;

#[cfg(test)]
#[path = "block_report_logging_tests.rs"]
mod block_report_logging_tests;

#[derive(Default)]
pub(crate) struct CacheInvalidationResult {
    pub delete_result: DeleteResult,
    pub invalidated_block_ids: HashSet<i64>,
}

impl CacheInvalidationResult {
    pub(crate) fn extend(&mut self, other: Self) {
        self.delete_result.inodes += other.delete_result.inodes;
        for (block_id, locations) in other.delete_result.blocks {
            self.delete_result
                .blocks
                .entry(block_id)
                .or_default()
                .extend(locations);
        }
        self.invalidated_block_ids
            .extend(other.invalidated_block_ids);
    }
}

impl FsDir {
    pub fn new(
        conf: &ClusterConf,
        journal_writer: Arc<JournalWriter>,
        ttl_bucket_list: Arc<TtlBucketList>,
        evictor: Arc<dyn Evictor>,
    ) -> FsResult<Self> {
        let db_conf = conf.db_conf();

        let store = RocksInodeStore::new(db_conf, conf.format_master)?;
        let state = InodeStore::new(store, ttl_bucket_list)?;
        let (last_inode_id, root_dir) = state.create_blank_tree()?;

        let fs_dir = Self {
            root_dir,
            inode_id: InodeId::new(),
            store: state,
            journal_writer,
            evictor,
            op_id: AtomicCounter::new(0),
        };
        fs_dir.update_last_inode_id(last_inode_id)?;

        Ok(fs_dir)
    }

    // Create root directory
    pub fn create_root() -> InodeView {
        InodeView::new_dir(ROOT_INODE_NAME.to_string(), InodeDir::new(ROOT_INODE_ID, 0))
    }

    pub fn root_ptr(&self) -> InodePtr {
        InodePtr::from_ref(&self.root_dir)
    }

    pub fn root_dir(&self) -> &InodeView {
        &self.root_dir
    }

    fn next_inode_id(&self) -> FsResult<i64> {
        let id = self.inode_id.next()?;
        Ok(id)
    }

    pub fn next_op_id(&self) -> u64 {
        self.op_id.next()
    }

    /// Rebuild the absolute path of an inode by walking parent_id links.
    ///
    /// Used when an RPC addresses a file by inode id: the caller-supplied path may be
    /// stale after rename, but journal / FileStatus / UFS export still need the current path.
    pub fn get_inode_path(&self, inode_id: i64) -> FsResult<String> {
        if inode_id == ROOT_INODE_ID {
            return Ok("/".to_string());
        }

        let mut current_id = inode_id;
        let mut components = Vec::new();
        let mut visited = Vec::new();

        while current_id != ROOT_INODE_ID {
            if visited.contains(&current_id) {
                return err_box!("Cycle detected while resolving inode path {}", inode_id);
            }
            visited.push(current_id);

            let inode_view = match self.store.get_inode(current_id, None)? {
                Some(inode_view) => inode_view,
                None => {
                    return err_ext!(FsError::file_not_found(format!(
                        "inode_id={} (missing ancestor {})",
                        inode_id, current_id
                    )));
                }
            };

            match &inode_view {
                File(f) => {
                    components.push(f.name.clone());
                    current_id = f.parent_id();
                }
                Dir(d) => {
                    components.push(d.name.clone());
                    current_id = d.parent_id();
                }
                FileEntry(e) => {
                    // Top-level inode CF should not store FileEntry rows; a truncated
                    // name-only path would be unsafe for UFS mount selection.
                    return err_box!(
                        "Cannot resolve path for inode {}: unexpected FileEntry '{}'",
                        inode_id,
                        e.name
                    );
                }
            }
        }

        components.reverse();
        Ok(format!("/{}", components.join("/")))
    }

    pub fn update_op_id(&self, op_id: u64) {
        if op_id > self.op_id.get() {
            self.op_id.set(op_id);
        }
    }

    pub fn get_ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.store.get_ttl_bucket_list()
    }

    pub fn mkdir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        // Create parent directory
        inp = self.create_parent_dir(inp, opts.parent_opts())?;

        // Create the final directory.
        inp = self.create_single_dir(inp, opts)?;
        Ok(inp)
    }

    // Create the first subdirectory that does not exist.
    // 1. If all directories on the path already exist, skip and return successful.
    // 2. If the parent directory does not exist, an error is returned.
    fn create_single_dir(
        &mut self,
        mut inp: InodePath,
        mut opts: MkdirOpts,
    ) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() - 1;
        let name = inp.get_component(pos + 1)?.to_string();

        if let Some(group) = Self::setgid_parent_group(&inp)? {
            opts.group = group;
            opts.mode |= MODE_SETGID;
        }

        let dir = InodeDir::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);

        inp = self.add_last_inode(inp, InodeView::new_dir(name.clone(), dir.clone()))?;

        let parent_path = inp.get_valid_parent_path();
        self.journal_writer.log_mkdir(self, &parent_path, &dir)?;

        Ok(inp)
    }

    fn setgid_parent_group(inp: &InodePath) -> FsResult<Option<String>> {
        if inp.existing_len() == 0 {
            return Ok(None);
        }
        let parent_pos = inp.existing_len() as i32 - 1;
        let parent = match inp.get_inode(parent_pos) {
            Some(parent) => parent,
            None => return Ok(None),
        };
        let acl = parent.as_ref().acl()?;
        if acl.mode & MODE_SETGID != 0 {
            Ok(Some(acl.group.clone()))
        } else {
            Ok(None)
        }
    }

    // Create all previous directories that may be missing on the path.
    fn create_parent_dir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        let mut index = inp.existing_len();

        // The parent directory already exists and does not need to be created.
        if inp.is_full() || index + 1 >= inp.len() {
            return Ok(inp);
        }

        while index <= inp.len() - 2 {
            inp = self.create_single_dir(inp, opts.clone())?;
            index += 1;
        }

        Ok(inp)
    }

    // Delete files or directories
    pub fn delete(&mut self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(inp.path()));
        }

        if !inp.is_empty_dir() && !recursive {
            return err_ext!(FsError::dir_not_empty(inp.path()));
        }

        let del_res = self.unprotected_delete(inp, op_ms as i64)?;
        self.journal_writer
            .log_delete(self, inp.path(), op_ms as i64)?;

        Ok(del_res)
    }

    pub(crate) fn unprotected_delete(
        &mut self,
        inp: &InodePath,
        mtime: i64,
    ) -> FsResult<DeleteResult> {
        let target = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("Path not exists: {}", inp.path()),
        };

        let mut parent = match inp.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Abnormal data status"),
        };
        let child = target.as_ref();
        let child_name = inp.name();

        // Handle different types of nodes.
        // An already-expired parent TTL must keep its mtime: the checker can
        // delete child files first, and bumping mtime would un-expire the
        // directory for the rest of the same cleanup pass.
        // Evaluate expiry at this delete's `mtime` so journal replay matches the
        // leader. `expiration_ms` can fail (e.g. FileEntry); never fail the delete.
        match parent.as_ref().expiration_ms() {
            Ok(Some(exp)) if mtime > exp => {}
            _ => parent.update_mtime(mtime),
        }

        let del_res = match child {
            File(f) => {
                if f.nlink() > 1 {
                    let target_inode = target.clone();
                    if let File(ref mut nf) = target_inode.as_mut() {
                        nf.decrement_nlink(mtime);
                    }
                    self.store
                        .apply_unlink(parent.as_ref(), child, child_name, mtime)?
                } else {
                    // This is the last link, delete the inode
                    self.store
                        .apply_delete(parent.as_ref(), child, child_name, mtime)?
                }
            }
            FileEntry(e) => {
                // This is a link entry, just remove the directory entry
                // The actual inode's nlink count should be decremented
                self.store.apply_unlink_file_entry(
                    parent.as_ref(),
                    child,
                    child_name,
                    e.id,
                    mtime,
                )?
            }
            Dir(_) => {
                parent.dec_nlink(mtime);
                // Directories are always deleted
                self.store
                    .apply_delete(parent.as_ref(), child, child_name, mtime)?
            }
        };

        // After deletion occurs, the target address cannot be used.
        let _ = parent.delete_child(child.id(), child_name)?;
        Ok(del_res)
    }

    pub fn free(&mut self, inp: &InodePath, recursive: bool) -> FsResult<FreeResult> {
        let op_ms = LocalTime::mills() as i64;

        if inp.is_root() {
            return err_box!("The root is not allowed to be free");
        }

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let free_res = self.unprotected_free(inode, op_ms, recursive)?;
        self.journal_writer
            .log_free(self, inp.path(), op_ms, recursive)?;

        Ok(free_res)
    }

    pub(crate) fn unprotected_free(
        &mut self,
        inode: InodePtr,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<FreeResult> {
        let mut free_res = FreeResult::default();
        let mut change_inodes = vec![];

        let mut stack = LinkedList::new();
        stack.push_back(inode);
        while let Some(inode) = stack.pop_front() {
            match inode.as_mut() {
                FileEntry(e) => {
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back(InodePtr::from_owned(store_inode));
                    }
                }

                Dir(d) => {
                    if recursive {
                        for child in d.children_iter() {
                            stack.push_back(InodePtr::from_ref(child));
                        }
                    }
                }

                File(f) => {
                    let locs = f.get_locs(&self.store)?;
                    let bytes = f.get_locs_bytes(&locs);
                    if f.free(mtime) {
                        free_res.add(bytes, locs);
                        change_inodes.push(inode.as_ref().clone());
                    }
                }
            }
        }

        self.store.apply_free(change_inodes)?;
        Ok(free_res)
    }

    pub fn rename(
        &mut self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        flags: RenameFlags,
    ) -> FsResult<Option<DeleteResult>> {
        let op_ms = LocalTime::mills();
        let exchange_pre_swap_ids = if flags.exchange_mode() {
            let src_id = src_inp
                .get_last_inode()
                .map(|inode| inode.id())
                .unwrap_or(0);
            let dst_id = dst_inp
                .get_last_inode()
                .map(|inode| inode.id())
                .unwrap_or(0);
            Some((src_id, dst_id))
        } else {
            None
        };
        let res =
            self.unprotected_rename(src_inp, dst_inp, op_ms as i64, flags, exchange_pre_swap_ids)?;
        self.journal_writer.log_rename(
            self,
            src_inp.path(),
            dst_inp.path(),
            op_ms as i64,
            flags,
            exchange_pre_swap_ids,
        )?;
        Ok(res)
    }

    pub(crate) fn unprotected_rename(
        &mut self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        flags: RenameFlags,
        exchange_pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<Option<DeleteResult>> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        if flags.exchange_mode() {
            self.unprotected_exchange(src_inp, dst_inp, mtime, exchange_pre_swap_ids)?;
            return Ok(None);
        }

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };

        let del_res = if let Some(dst_inode) = dst_inp.get_last_inode() {
            let src_is_file = src_inode.is_file();
            let dst_is_file = dst_inode.is_file();
            if flags.no_replace() {
                return err_ext!(FsError::file_exists(dst_inp.path()));
            }
            if src_is_file && !dst_is_file {
                return err_ext!(FsError::is_a_directory(dst_inp.path()));
            }
            if !src_is_file && dst_is_file {
                return err_ext!(FsError::not_a_directory(dst_inp.path()));
            }
            if !src_is_file && !dst_is_file && !dst_inp.is_empty_dir() {
                return err_ext!(FsError::dir_not_empty(dst_inp.path()));
            }
            Some(self.unprotected_delete(dst_inp, mtime)?)
        } else {
            None
        };

        let new_name = dst_inp.name().to_string();
        // /1.log -> /a/b : b is the new inode name and /a is the destination parent.
        // For replace semantics (dst exists), dst has been deleted above and we still
        // rename to dst's original parent with dst's original name.
        let mut dst_parent = match dst_inp.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent {} does not exist", dst_inp.get_parent_path()),
        };

        // Modify the time and name of the rename node.
        let mut new_inode = src_inode.as_ref().clone();
        new_inode.change_name(new_name);
        new_inode.set_parent_id(dst_parent.id());
        new_inode.update_ctime(mtime);

        // Update the parent directory for the last modification time.
        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        // Update state.
        self.store.apply_rename(
            src_parent.as_ref(),
            src_inode.as_ref(),
            src_inp.name(),
            dst_parent.as_ref(),
            &new_inode,
        )?;

        // Update memory status.
        // step 1: Delete the original node.
        // step 2: Add a new node.
        let _ = src_parent.delete_child(src_inode.id(), src_inode.name())?;
        let _ = dst_parent.add_child(new_inode)?;

        Ok(del_res)
    }

    fn unprotected_exchange(
        &mut self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<()> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        let dst_inode = match dst_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(dst_inp.path())),
            Some(v) => v,
        };

        let src_id = src_inode.id();
        let dst_id = dst_inode.id();

        if src_id == dst_id {
            return Ok(());
        }

        if let Some((expected_src, expected_dst)) = pre_swap_ids {
            if expected_src != 0 && expected_dst != 0 {
                if src_id == expected_dst && dst_id == expected_src {
                    return Ok(());
                }
                if src_id != expected_src || dst_id != expected_dst {
                    warn!(
                        "Exchange replay inode id mismatch at {} and {}: current ({}, {}), expected ({}, {})",
                        src_inp.path(),
                        dst_inp.path(),
                        src_id,
                        dst_id,
                        expected_src,
                        expected_dst
                    );
                    return Ok(());
                }
            }
        }

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };
        let mut dst_parent = match dst_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", dst_inp.path()),
            Some(v) => v,
        };

        let src_name = src_inp.name().to_string();
        let dst_name = dst_inp.name().to_string();

        let mut at_src = dst_inode.as_ref().clone();
        at_src.change_name(src_name.clone());
        at_src.set_parent_id(src_parent.id());

        let mut at_dst = src_inode.as_ref().clone();
        at_dst.change_name(dst_name.clone());
        at_dst.set_parent_id(dst_parent.id());

        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        if src_parent.id() != dst_parent.id() {
            let src_was_dir = src_inode.is_dir();
            let dst_was_dir = dst_inode.is_dir();
            if src_was_dir && !dst_was_dir {
                src_parent.dec_nlink(mtime);
            } else if !src_was_dir && dst_was_dir {
                src_parent.incr_nlink(mtime);
            }
            if dst_was_dir && !src_was_dir {
                dst_parent.dec_nlink(mtime);
            } else if !dst_was_dir && src_was_dir {
                dst_parent.incr_nlink(mtime);
            }
        }

        self.store.apply_exchange(
            src_parent.as_ref(),
            &src_name,
            dst_parent.as_ref(),
            &dst_name,
            &at_src,
            &at_dst,
        )?;

        let _ = src_parent.delete_child(src_inode.id(), &src_name)?;
        let _ = dst_parent.delete_child(dst_inode.id(), &dst_name)?;
        let _ = src_parent.add_child(at_src)?;
        let _ = dst_parent.add_child(at_dst)?;

        Ok(())
    }

    pub fn create_file(
        &mut self,
        mut inp: InodePath,
        mut opts: CreateFileOpts,
    ) -> FsResult<InodePath> {
        if inp.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Create a directory that does not exist.
        inp = self.create_parent_dir(inp, opts.dir_opts())?;

        if let Some(group) = Self::setgid_parent_group(&inp)? {
            opts.group = group;
        }

        let name = inp.name().to_string();

        // Create an inode file node.
        let file = InodeFile::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
        inp = self.add_last_inode(inp, InodeView::new_file(name, file))?;
        self.journal_writer.log_create_file(self, &inp)?;

        Ok(inp)
    }

    pub(crate) fn add_last_inode(
        &mut self,
        mut inp: InodePath,
        child: InodeView,
    ) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() as i32;

        // parent must be an existing directory.
        let mut parent = match inp.get_inode(pos - 1) {
            Some(v) => {
                if !v.is_dir() {
                    return err_box!("Parent path is not a directory: {}", inp.get_parent_path());
                } else {
                    v
                }
            }

            None => return err_box!("Parent path not exists: {}", inp.get_parent_path()),
        };

        // Update the parent directory for the last modification time.
        parent.update_mtime(child.mtime());
        if child.is_dir() {
            parent.incr_nlink(child.mtime());
        }

        let added = parent.add_child(child)?;
        self.store.apply_add(parent.as_ref(), added.as_ref())?;
        inp.append(added)?;

        Ok(inp)
    }

    pub fn file_status(&self, inp: &InodePath) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let status = match inode.as_ref() {
            File(..) | Dir(..) => inode.to_file_status(inp.path())?,
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        Ok(status)
    }

    pub fn list_status(&self, inp: &InodePath) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        match inode.as_ref() {
            File(_) => Ok(vec![inode.to_file_status(inp.path())?]),

            Dir(d) => {
                let iter = d.children_iter().collect();
                let res = self.store.batched_get_inodes(inp, iter)?;
                Ok(res)
            }

            FileEntry(e) => {
                let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
                match inode_opt {
                    Some(inode_view) => Ok(vec![inode_view.to_file_status(inp.path())?]),
                    None => err_ext!(FsError::file_not_found(inp.path())),
                }
            }
        }
    }

    fn list_single_file(status: FileStatus, opts: &ListOptions) -> Vec<FileStatus> {
        if matches!(opts.limit, Some(0)) {
            return vec![];
        }
        if let Some(sa) = opts.start_after.as_deref() {
            if status.name.as_str() <= sa {
                return vec![];
            }
        }
        vec![status]
    }

    pub fn list_options(&self, inp: &InodePath, opts: &ListOptions) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        match inode.as_ref() {
            File(_) => {
                let status = inode.to_file_status(inp.path())?;
                Ok(Self::list_single_file(status, opts))
            }

            Dir(d) => {
                let children = d.list_options(opts);
                let res = self.store.batched_get_inodes(inp, children)?;
                Ok(res)
            }

            FileEntry(e) => {
                let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
                match inode_opt {
                    Some(inode_view) => {
                        let status = inode_view.to_file_status(inp.path())?;
                        Ok(Self::list_single_file(status, opts))
                    }
                    None => err_ext!(FsError::file_not_found(inp.path())),
                }
            }
        }
    }

    pub fn acquire_new_block(
        &mut self,
        path: impl AsRef<str>,
        mut inode: InodePtr,
        commit_blocks: Vec<CommitBlock>,
        choose_workers: &[WorkerAddress],
        file_len: i64,
    ) -> FsResult<ExtendedBlock> {
        let file = inode.as_file_mut()?;

        let new_block_id = file.next_block_id()?;

        // flush file and commit block
        file.complete(file_len, &commit_blocks, "", true)?;

        // create block.
        file.add_block(BlockMeta::with_pre(new_block_id, choose_workers));

        let block = ExtendedBlock {
            id: new_block_id,
            len: 0,
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
            alloc_opts: None,
        };

        // state add block.
        self.store.apply_new_block(inode.as_ref(), &commit_blocks)?;
        self.journal_writer
            .log_add_block(self, path, inode.as_file_ref()?, commit_blocks)?;
        Ok(block)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file(
        &mut self,
        path: impl AsRef<str>,
        inode: &mut InodePtr,
        len: i64,
        commit_block: Vec<CommitBlock>,
        client_name: impl AsRef<str>,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<bool> {
        let file = inode.as_file_mut()?;
        let id = file.id();
        file.complete(len, &commit_block, client_name, only_flush)?;

        // Only apply set_attr_opts on real complete/close, not on flush.
        // Flush semantics should remain narrow (durability only).
        if !only_flush {
            if let Some(opts) = set_attr_opts {
                inode.set_attr(opts)?;
            }
        }

        self.evictor.on_access(id);

        self.store
            .apply_complete_file(inode.as_ref(), &commit_block)?;
        self.journal_writer
            .log_complete_file(self, path, inode.as_file_ref()?, commit_block)?;

        Ok(true)
    }

    pub fn get_file_locations(
        &self,
        file: &InodeFile,
    ) -> FsResult<HashMap<i64, Vec<BlockLocation>>> {
        let locs = self.store.get_file_locations(file)?;
        self.evictor.on_access(file.id());
        Ok(locs)
    }

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> FsResult<()> {
        self.store.add_block_location(block_id, location)?;
        Ok(())
    }

    pub fn get_block_locations(&self, block_id: i64) -> FsResult<Vec<BlockLocation>> {
        Ok(self.store.get_block_locations(block_id)?)
    }

    pub fn reopen_file(
        &mut self,
        inp: &InodePath,
        client_name: impl AsRef<str>,
    ) -> FsResult<FileStatus> {
        let inode_ptr = match inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(inp.path())),
            Some(v) => v,
        };

        let mut inode = match inode_ptr.as_ref() {
            File(..) => inode_ptr.as_ref().clone(),
            Dir(..) => {
                let err_msg = format!("Cannot append to already exists {} directory", inp.path());
                return err_ext!(FsError::file_exists(err_msg));
            }
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        let file = inode.as_file_mut()?;
        let _ = file.reopen(client_name);
        let status = inode.to_file_status(inp.path())?;

        self.store.apply_reopen_file(&inode)?;
        self.journal_writer
            .log_reopen_file(self, inp.path(), inode.as_file_ref()?)?;

        Ok(status)
    }

    /// Overwrite a file by cleaning all blocks and updating metadata.
    /// If file doesn't exist, create a new one.
    /// Returns DeleteResult containing blocks that need to be removed from workers.
    pub fn overwrite_file(
        &mut self,
        inp: &InodePath,
        opts: CreateFileOpts,
    ) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();
        let mut delete_result = DeleteResult::new();

        match inp.get_last_inode() {
            Some(inode) => {
                if !inode.is_file() {
                    return err_box!("Path is not a file: {}", inp.path());
                }

                let file = inode.as_mut().as_file_mut()?;
                for block_meta in &file.blocks {
                    if let Ok(locations) = self.get_block_locations(block_meta.id) {
                        delete_result.blocks.insert(block_meta.id, locations);
                    }
                }
                file.overwrite(opts, op_ms as i64);

                self.store.apply_overwrite_file(inode.as_ref())?;
            }
            None => {
                return err_ext!(FsError::file_not_found(inp.path()));
            }
        }

        // Log the operation
        self.journal_writer.log_overwrite_file(self, inp)?;

        Ok(delete_result)
    }

    pub fn print_tree(&self) {
        self.root_dir.print_tree()
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let mut tree_hash = self.root_dir.sum_hash()?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_INODES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_EDGES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_LOCATION)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_BLOCK)?;
        Ok(tree_hash)
    }

    pub fn last_inode_id(&self) -> i64 {
        self.inode_id.current()
    }

    pub fn update_last_inode_id(&self, new_value: i64) -> CommonResult<()> {
        let old_value = self.last_inode_id();
        if new_value > old_value {
            self.inode_id.reset(new_value)
        } else {
            Ok(())
        }
    }

    // Read data from rocksdb to build a directory tree
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        self.store.create_tree().map(|x| x.1)
    }

    // Restore in-memory tree from RocksDB without checkpoint (for testing only).
    // In production, use restore() with checkpoint path via Raft snapshot.
    pub fn restore_from_rocksdb(&mut self) -> CommonResult<()> {
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        self.root_dir = root_dir;
        self.update_last_inode_id(last_inode_id)?;
        Ok(())
    }

    /// Exclude shared report commits while checkpoint creation flushes column families.
    pub fn create_checkpoint(&mut self, id: u64) -> CommonResult<String> {
        self.store.create_checkpoint(id)
    }

    pub fn restore<T: AsRef<str>>(&mut self, path: T, checkpoint_size: u64) -> CommonResult<()> {
        let mut spend = TimeSpent::new();
        let path = path.as_ref();

        // Set to other value first to facilitate memory recycling.
        self.root_dir = Self::create_root();

        // Reset rocksdb
        self.store.restore(path)?;
        let time1 = spend.used_ms();
        spend.reset();

        // Update the directory tree
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        self.root_dir = root_dir;
        self.update_last_inode_id(last_inode_id)?;
        let time2 = spend.used_ms();

        info!(
            "restore from {}, checkpoint_size={} bytes, restore_rocksdb={} ms, \
        build_tree={} ms (see create_tree log for sub-phase breakdown), \
        last_inode_id={}",
            path, checkpoint_size, time1, time2, last_inode_id
        );
        Ok(())
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        self.store.get_checkpoint_path(id)
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        self.store.get_file_counts()
    }

    pub fn block_report(&mut self, blocks: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        let mut batch = self.store.new_batch();
        for (add, id, loc) in blocks {
            if add {
                batch.add_location(id, &loc)?;
            } else {
                batch.delete_location(id, loc.worker_id)?;
            }
        }

        batch.commit()?;
        Ok(())
    }

    /// Keep inode membership stable while reports update independent location keys.
    pub(crate) fn apply_reported_blocks_with_read(
        fs_dir: &RwLock<Self>,
        worker_id: u32,
        full_report: bool,
        diagnostics: &mut BlockReportDiagnostics,
        blocks: Vec<BlockReportInfo>,
    ) -> FsResult<Vec<i64>> {
        Self::apply_reported_blocks_with_read_hook(
            fs_dir,
            worker_id,
            full_report,
            diagnostics,
            blocks,
            || {},
        )
    }

    fn apply_reported_blocks_with_read_hook<F: FnOnce()>(
        fs_dir: &RwLock<Self>,
        worker_id: u32,
        full_report: bool,
        diagnostics: &mut BlockReportDiagnostics,
        blocks: Vec<BlockReportInfo>,
        before_commit: F,
    ) -> FsResult<Vec<i64>> {
        let mut reported_ids: HashMap<i64, HashSet<i64>> = HashMap::new();
        for block in &blocks {
            if block.status != BlockReportStatus::Deleted {
                reported_ids
                    .entry(InodeId::get_id(block.id))
                    .or_default()
                    .insert(block.id);
            }
        }

        // Writers cannot change inode membership or restore the store until
        // this batch commits. Concurrent reports own separate batches; each
        // location updates only its (block, worker) key and reverse-index key.
        let guard = fs_dir.read();
        let mut batch = guard.store.new_batch();
        let mut delete_blocks = Vec::new();
        // Sparse reports need membership for only a few IDs from each file.
        let mut current_blocks = HashMap::new();
        for block in blocks {
            if block.status == BlockReportStatus::Deleted {
                batch.delete_location(block.id, worker_id)?;
                continue;
            }

            let inode_id = InodeId::get_id(block.id);
            let current = match current_blocks.entry(inode_id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let mut ids = reported_ids
                        .remove(&inode_id)
                        .expect("non-deleted report IDs are grouped by inode");
                    let inode = match guard.store.get_inode(inode_id, None) {
                        Ok(inode) => inode,
                        Err(e) => {
                            diagnostics.failed_inode_block = Some(block);
                            return Err(e.into());
                        }
                    };
                    let current = match inode {
                        Some(File(file)) if ids.len() == 1 => {
                            ids.retain(|id| file.blocks.iter().any(|meta| meta.id == *id));
                            ReportedInodeBlocks::File(ids)
                        }
                        Some(File(file)) => {
                            let mut current = HashSet::with_capacity(ids.len());
                            for meta in &file.blocks {
                                if ids.remove(&meta.id) {
                                    current.insert(meta.id);
                                    if ids.is_empty() {
                                        break;
                                    }
                                }
                            }
                            ReportedInodeBlocks::File(current)
                        }
                        None => ReportedInodeBlocks::Missing,
                        Some(_) => ReportedInodeBlocks::NotFile,
                    };
                    entry.insert(current)
                }
            };
            if matches!(current, ReportedInodeBlocks::File(ids) if ids.contains(&block.id)) {
                batch.add_location(block.id, &BlockLocation::new(worker_id, block.storage_type))?;
            } else if full_report || block.status == BlockReportStatus::Finalized {
                match current {
                    ReportedInodeBlocks::Missing => diagnostics.missing_blocks += 1,
                    ReportedInodeBlocks::NotFile => diagnostics.not_file_blocks += 1,
                    ReportedInodeBlocks::File(_) => diagnostics.obsolete_file_blocks += 1,
                }
                batch.delete_location(block.id, worker_id)?;
                delete_blocks.push(block.id);
            } else {
                match current {
                    ReportedInodeBlocks::Missing => diagnostics
                        .deferred_writing
                        .push(DeferredWritingWarning::Missing(block.id)),
                    ReportedInodeBlocks::NotFile => diagnostics
                        .deferred_writing
                        .push(DeferredWritingWarning::NotFile(block.id)),
                    ReportedInodeBlocks::File(_) => {}
                }
            }
            // An incremental Writing report can race metadata attachment.
            // Leave unknown IDs alone until a finalized or full report arrives.
        }
        before_commit();
        batch.commit()?;
        // Scratch membership maps no longer need to keep metadata writers waiting.
        drop(guard);
        Ok(delete_blocks)
    }

    pub fn get_rocks_store(&self) -> &RocksInodeStore {
        &self.store.store
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        let block_ids = self.store.store.delete_locations(worker_id)?;
        Ok(block_ids)
    }

    /// Invalidate UFS-backed files whose cache is no longer readable after a
    /// worker's locations are removed. A single missing block makes the whole
    /// cache copy unusable, even when other blocks or replicas still exist.
    ///
    /// The returned locations belong to the discarded cache copies and should
    /// be scheduled for deletion from any surviving workers.
    pub(crate) fn invalidate_lost_cache_files(
        &mut self,
        block_ids: &[i64],
    ) -> FsResult<CacheInvalidationResult> {
        let inode_ids: HashSet<_> = block_ids.iter().map(|id| InodeId::get_id(*id)).collect();
        let mut result = CacheInvalidationResult::default();
        let mut changed_inodes = Vec::new();

        for inode_id in inode_ids {
            let Some(mut inode) = self.store.get_inode(inode_id, None)? else {
                continue;
            };
            let File(file) = &mut inode else {
                continue;
            };

            // Cache-mode load jobs use the Delete TTL action. Files in fs-mode
            // use Free instead and retain their normal replica-recovery path.
            if !file.storage_policy.both_exists()
                || file.storage_policy.ttl_action != TtlAction::Delete
            {
                continue;
            }

            // `get_locs` purposefully omits empty location lists, so inspect
            // each file block directly to find cache blocks with no replicas.
            let mut cache_unreadable = false;
            for block in &file.blocks {
                if self.store.get_block_locations(block.id)?.is_empty() {
                    cache_unreadable = true;
                    break;
                }
            }
            if !cache_unreadable {
                continue;
            }

            let locations = file.get_locs(&self.store)?;
            result
                .invalidated_block_ids
                .extend(file.blocks.iter().map(|block| block.id));
            if file.invalidate_cache() {
                result.delete_result.blocks.extend(locations);
                changed_inodes.push(inode);
            }
        }

        let journal_inodes = changed_inodes.clone();
        self.store.apply_cache_invalidations(changed_inodes)?;
        self.journal_writer
            .log_cache_invalidations(self, journal_inodes)?;
        Ok(result)
    }

    pub fn get_worker_block_ids(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        Ok(self.store.store.get_block_ids(worker_id)?)
    }

    pub(crate) fn get_worker_block_ids_with_capacity(
        &self,
        worker_id: u32,
        capacity_hint: usize,
    ) -> FsResult<Vec<i64>> {
        Ok(self
            .store
            .store
            .get_block_ids_with_capacity(worker_id, capacity_hint)?)
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        self.journal_writer.take_entries()
    }

    pub fn store_mount(&mut self, info: MountInfo, send_log: bool) -> FsResult<()> {
        self.store.store.add_mountpoint(info.mount_id, &info)?;

        if send_log {
            self.journal_writer.log_mount(self, info)?;
        }

        Ok(())
    }

    pub fn unprotected_store_mount(&mut self, info: MountInfo) -> FsResult<()> {
        self.store.store.add_mountpoint(info.mount_id, &info)?;
        Ok(())
    }

    pub fn unmount(&mut self, id: u32) -> FsResult<()> {
        self.store.store.remove_mountpoint(id)?;
        self.journal_writer.log_unmount(self, id)?;
        Ok(())
    }

    pub fn unprotected_unmount(&mut self, id: u32) -> FsResult<()> {
        self.store.store.remove_mountpoint(id)?;
        Ok(())
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        self.store.get_mount_table()
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        self.store.get_mount_point(id)
    }

    pub fn set_attr(&mut self, inp: InodePath, mut opts: SetAttrOpts) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        // Internal metadata is master-owned. Persist the operation timestamp in the
        // journal so replay restores it without changing the bincode inode layout.
        opts.add_x_attr.remove(INTERNAL_CTIME_XATTR);
        opts.remove_x_attr.retain(|key| key != INTERNAL_CTIME_XATTR);
        let ctime = LocalTime::mills() as i64;
        opts.add_x_attr.insert(
            INTERNAL_CTIME_XATTR.to_string(),
            ctime.to_le_bytes().to_vec(),
        );
        self.unprotected_set_attr(inode.clone(), opts.clone())?;
        self.journal_writer.log_set_attr(self, &inp, opts)?;
        Ok(inode.to_file_status(inp.path())?)
    }

    pub fn unprotected_set_attr(&mut self, inode: InodePtr, opts: SetAttrOpts) -> FsResult<()> {
        if inode.is_file_entry() {
            return err_box!("set_attr is not supported on unresolved FileEntry inodes; resolve/load the full inode before calling set_attr");
        }

        let mut change_inodes = vec![];
        let mut stack = LinkedList::new();
        stack.push_back(inode.clone());
        let child_opts = opts.child_opts();
        while let Some(cur_inode) = stack.pop_front() {
            if !cur_inode.is_file_entry() {
                let set_opts = if cur_inode.id() != inode.id() {
                    child_opts.clone()
                } else {
                    opts.clone()
                };
                cur_inode.as_mut().set_attr(set_opts)?;
                change_inodes.push(cur_inode.as_ref().clone());
            }

            match cur_inode.as_mut() {
                Dir(dir) if opts.recursive => {
                    for child in dir.children_iter() {
                        stack.push_back(InodePtr::from_ref(child));
                    }
                }

                FileEntry(e) => {
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back(InodePtr::from_owned(store_inode));
                    } else {
                        warn!(
                            "unprotected_set_attr: missing inode {} for FileEntry '{}'",
                            e.id, e.name
                        );
                    }
                }

                _ => (),
            }
        }

        self.store.apply_set_attr(change_inodes)?;
        Ok(())
    }

    pub fn symlink(
        &mut self,
        target: String,
        link: InodePath,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        let new_inode = InodeFile::with_link(
            self.inode_id.next()?,
            op_ms as i64,
            target,
            mode,
            owner,
            group,
        );

        let link = self.unprotected_symlink(link, new_inode.clone(), force)?;
        self.journal_writer
            .log_symlink(self, link.path(), new_inode, force)?;
        Ok(())
    }

    pub fn unprotected_symlink(
        &mut self,
        mut link: InodePath,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<InodePath> {
        // check parent
        let mut parent = match link.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Directory does not exist"),
        };

        let old_inode = if let Some(v) = link.get_last_inode() {
            if !v.is_link() || (v.is_link() && !force) {
                return err_ext!(FsError::file_exists(link.path()));
            } else {
                Some(v)
            }
        } else {
            None
        };

        let name = link.name().to_string();
        parent.update_mtime(new_inode.mtime);
        let is_add = old_inode.is_none();
        let new_inode_ptr = match old_inode {
            Some(v) => {
                let _ = mem::replace(v.as_mut(), InodeView::new_file(name, new_inode));
                v
            }
            None => {
                let added = parent.add_child(InodeView::new_file(name, new_inode))?;
                link.append(added.clone())?;
                added
            }
        };

        self.store
            .apply_symlink(parent.as_ref(), new_inode_ptr.as_ref(), is_add)?;
        Ok(link)
    }

    // Create a link to an existing file
    pub fn link(&mut self, src_path: InodePath, dst_path: InodePath) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        // Get the original inode ID and update nlink in memory if it's a direct File
        let (original_inode_id, mut original_inode_ptr) = match src_path.get_last_inode() {
            Some(inode) => match inode.as_ref() {
                File(file) => {
                    // Hard links to regular files and symlinks are valid; directories are not.
                    if !matches!(
                        file.file_type,
                        curvine_model::FileType::File | curvine_model::FileType::Link
                    ) {
                        return err_ext!(FsError::common("Cannot create link to non-regular file"));
                    }
                    (file.id, Some(inode.clone()))
                }
                FileEntry(e) => (e.id, None), // FileEntry already points to an inode
                Dir(_) => return err_ext!(FsError::common("Cannot create link to directory")),
            },
            None => return err_ext!(FsError::file_not_found(src_path.path())),
        };

        // If we have the original inode in memory, increment its nlink count
        if let Some(ref mut inode_ptr) = original_inode_ptr {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(op_ms as i64);
            }
        }

        // Create the link
        let dst_path_str = dst_path.path().to_string();
        self.unprotected_link(dst_path, original_inode_id, op_ms)?;

        // Log the operation
        self.journal_writer
            .log_link(self, src_path.path(), &dst_path_str, op_ms as i64)?;

        Ok(())
    }

    pub fn unprotected_link(
        &mut self,
        mut new_path: InodePath,
        original_inode_id: i64,
        op_ms: u64,
    ) -> FsResult<InodePath> {
        // Check if the new path already exists
        if new_path.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(new_path.path()));
        }

        // Create parent directory if needed
        new_path = self.create_parent_dir(new_path, MkdirOpts::with_create(true))?;

        // Get the parent directory
        let mut parent = match new_path.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent directory does not exist"),
        };

        // Create a FileEntry that points to the original inode
        let name = new_path.name().to_string();
        let file_entry = InodeView::new_entry(name.clone(), original_inode_id);

        // Update parent directory
        parent.update_mtime(op_ms as i64);
        let added = parent.add_child(file_entry)?;
        new_path.append(added.clone())?;

        // Apply changes to storage - this creates an edge pointing to the original inode
        self.store.apply_link(
            parent.as_ref(),
            added.as_ref(),
            original_inode_id,
            op_ms as i64,
        )?;

        Ok(new_path)
    }

    /// Resize a file to the specified length.
    ///
    /// This method changes the file size by either extending or truncating it.
    /// - If the new size is larger than the current size, new blocks are allocated.
    /// - If the new size is smaller, blocks beyond the new size are marked for deletion.
    ///
    /// # Arguments
    /// * `inp` - The inode path of the file to resize
    /// * `opts` - File allocation options containing the target length and allocation mode
    ///
    /// # Returns
    /// * `DeleteResult` - Contains blocks that need to be deleted from workers
    ///
    /// # Process
    /// 1. Resize the file metadata (extend or truncate blocks)
    /// 2. Complete the file operation to update metadata state
    /// 3. Collect locations of blocks to be deleted
    /// 4. Persist changes to store and write journal entry
    pub fn resize(&mut self, inp: &InodePath, opts: FileAllocOpts) -> FsResult<DeleteResult> {
        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };
        self.resize_inode(inp.path(), &mut inode, opts)
    }

    pub fn resize_inode(
        &mut self,
        audit_path: &str,
        inode: &mut InodePtr,
        opts: FileAllocOpts,
    ) -> FsResult<DeleteResult> {
        let file = inode.as_file_mut()?;

        if file.len == opts.len {
            return Ok(DeleteResult::new());
        }
        let del_blocks = file.resize(opts.clone())?;
        debug!("resize file {} success, opts: {:?}", audit_path, opts);

        file.complete(file.len, &[], "", true)?;
        let mut del_res = DeleteResult::new();
        for meta in del_blocks {
            let locs = self.get_locations(&meta)?;
            if !locs.is_empty() {
                del_res.blocks.insert(meta.id, locs);
            }
        }

        self.store.apply_complete_file(inode.as_ref(), &[])?;
        self.journal_writer
            .log_complete_file(self, audit_path, inode.as_file_ref()?, vec![])?;

        Ok(del_res)
    }

    pub fn assign_worker(
        &mut self,
        inp: InodePath,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<ExtendedBlock> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        self.assign_worker_inode(inp.path(), &mut inode, block_id, workers)
            .map(|(block, _assigned)| block)
    }

    pub fn assign_worker_inode(
        &mut self,
        audit_path: &str,
        inode: &mut InodePtr,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<(ExtendedBlock, bool)> {
        let file = inode.as_file_mut()?;

        let block = file.search_block_mut_check(block_id)?;
        let assigned = block.assign_worker(workers);
        let block = ExtendedBlock {
            id: block.id,
            len: block.len as i64,
            alloc_opts: block.alloc_opts.clone(),
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
        };

        if assigned {
            self.store.apply_new_block(inode.as_ref(), &[])?;
            self.journal_writer
                .log_add_block(self, audit_path, inode.as_file_ref()?, vec![])?;
        }

        Ok((block, assigned))
    }

    pub fn get_locations(&self, meta: &BlockMeta) -> CommonResult<Vec<BlockLocation>> {
        if let Some(locs) = &meta.locs {
            Ok(locs.clone())
        } else {
            self.store.get_locations(meta.id)
        }
    }

    pub fn get_lock(
        &self,
        inp: InodePath,
        lock: &FileLock,
        expire_ms: u64,
    ) -> FsResult<Option<FileLock>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let mut meta = self.store.get_locks(inode.id())?;
        let conflict = meta.check_conflict(lock, expire_ms);
        Ok(conflict)
    }

    pub fn set_lock(
        &self,
        inp: InodePath,
        lock: FileLock,
        expire_ms: u64,
    ) -> FsResult<Option<FileLock>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let mut meta = self.store.get_locks(inode.id())?;
        let (conflict, changed) = meta.set_lock_with_change(lock, expire_ms);

        if !changed {
            return Ok(conflict);
        }

        let locks = meta.to_vec();
        self.store.apply_set_locks(inode.id(), &locks)?;
        self.journal_writer.log_set_locks(self, inode.id(), locks)?;

        Ok(conflict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::fs::MasterFilesystem;
    use crate::master::journal::JournalSystem;
    use crate::master::Master;
    use curvine_model::{
        BlockReportList, ClientAddress, CreateFileOptsBuilder, OpenFlags, SetAttrOptsBuilder,
        StorageType, WorkerInfo,
    };
    use curvine_rocksdb::RocksUtils;
    use curvine_runtime::common::Utils;

    pub(super) fn report_test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "reported-blocks/meta-{name}-{}",
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "reported-blocks/journal-{name}-{}",
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.add_test_worker(WorkerInfo::default());
        fs
    }

    pub(super) fn create_file_blocks(
        fs: &MasterFilesystem,
        path: &str,
        count: usize,
    ) -> CommonResult<Vec<i64>> {
        let status = fs.create(path, false)?;
        let fs_dir = fs.fs_dir.write();
        let mut inode = fs_dir.store.get_inode(status.id, None)?.unwrap();
        let file = inode.as_file_mut()?;
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            let id = file.next_block_id()?;
            file.add_block(BlockMeta::new(id, 128));
            ids.push(id);
        }

        // Persist once to keep the multi-chunk fixture inexpensive.
        let mut batch = fs_dir.store.new_batch();
        batch.write_inode(&inode)?;
        for &id in &ids {
            batch.add_location(id, &BlockLocation::with_id(100))?;
        }
        batch.commit()?;
        Ok(ids)
    }

    pub(super) fn replace_inode_record(
        fs: &MasterFilesystem,
        inode_id: i64,
        bytes: &[u8],
    ) -> CommonResult<Vec<u8>> {
        let fs_dir = fs.fs_dir.write();
        let db = &fs_dir.store.store.db;
        let key = RocksUtils::i64_to_bytes(inode_id);
        let previous = db.get_cf(RocksInodeStore::CF_INODES, key)?.unwrap();
        db.put_cf(RocksInodeStore::CF_INODES, key, bytes)?;
        Ok(previous)
    }

    pub(super) fn block_location_worker_ids(
        fs: &MasterFilesystem,
        id: i64,
    ) -> CommonResult<Vec<u32>> {
        let mut ids: Vec<_> = fs
            .fs_dir
            .read()
            .get_block_locations(id)?
            .iter()
            .map(|location| location.worker_id)
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    #[test]
    fn block_report_recovers_after_inode_error_without_applying_failed_chunk() -> CommonResult<()> {
        for full_report in [false, true] {
            let fs = report_test_fs("inode-error");
            let mut worker = WorkerInfo::default();
            worker.address.worker_id = 101;
            fs.add_test_worker(worker);
            let mut ids = create_file_blocks(&fs, "/healthy", 1)?;
            ids.extend(create_file_blocks(&fs, "/unreadable", 1)?);
            let inode_id = InodeId::get_id(ids[1]);
            let report = || BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 101,
                full_report,
                total_len: ids.len() as u64,
                blocks: ids
                    .iter()
                    .map(|&id| {
                        BlockReportInfo::new(
                            id,
                            BlockReportStatus::Finalized,
                            StorageType::Disk,
                            128,
                        )
                    })
                    .collect(),
            };

            let inode_bytes = replace_inode_record(&fs, inode_id, &[0xff])?;
            assert!(fs.block_report(report(), None).is_err());
            for &id in &ids {
                assert_eq!(block_location_worker_ids(&fs, id)?, vec![100]);
            }
            assert!(fs.fs_dir.read().get_worker_block_ids(101)?.is_empty());
            assert!(fs
                .worker_manager
                .write()
                .block_map
                .handle_heartbeat(101)
                .is_empty());
            fs.file_status("/healthy")?;

            replace_inode_record(&fs, inode_id, &inode_bytes)?;
            for _ in 0..2 {
                assert!(fs.block_report(report(), None)?.delete_blocks.is_empty());
                if full_report {
                    fs.wait_for_full_block_reconcile_for_test(101)?;
                }
                for &id in &ids {
                    assert_eq!(block_location_worker_ids(&fs, id)?, vec![100, 101]);
                }
                let mut reported = fs.fs_dir.read().get_worker_block_ids(101)?;
                reported.sort_unstable();
                let mut expected = ids.clone();
                expected.sort_unstable();
                assert_eq!(reported, expected);
                assert!(fs
                    .worker_manager
                    .write()
                    .block_map
                    .handle_heartbeat(101)
                    .is_empty());
            }
        }
        Ok(())
    }

    #[test]
    fn block_report_recovers_after_inode_error_in_later_chunk() -> CommonResult<()> {
        // Match the production boundary so one healthy update is staged in the failing chunk.
        const CHUNK_SIZE: usize = MasterFilesystem::BLOCK_REPORT_CHUNK;
        for full_report in [false, true] {
            let fs = report_test_fs("later-inode-error");
            let mut worker = WorkerInfo::default();
            worker.address.worker_id = 101;
            fs.add_test_worker(worker);
            let mut ids = create_file_blocks(&fs, "/healthy", CHUNK_SIZE + 1)?;
            ids.extend(create_file_blocks(&fs, "/unreadable", 1)?);
            let inode_id = InodeId::get_id(*ids.last().unwrap());
            let report = || BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 101,
                full_report,
                total_len: ids.len() as u64,
                blocks: ids
                    .iter()
                    .map(|&id| {
                        BlockReportInfo::new(
                            id,
                            BlockReportStatus::Finalized,
                            StorageType::Disk,
                            128,
                        )
                    })
                    .collect(),
            };

            let inode_bytes = replace_inode_record(&fs, inode_id, &[0xff])?;
            assert!(fs.block_report(report(), None).is_err());
            for &id in &ids[..CHUNK_SIZE] {
                assert_eq!(block_location_worker_ids(&fs, id)?, vec![100, 101]);
            }
            for &id in &ids[CHUNK_SIZE..] {
                assert_eq!(block_location_worker_ids(&fs, id)?, vec![100]);
            }
            let mut reported = fs.fs_dir.read().get_worker_block_ids(101)?;
            reported.sort_unstable();
            let mut expected = ids[..CHUNK_SIZE].to_vec();
            expected.sort_unstable();
            assert_eq!(reported, expected);
            assert!(fs
                .worker_manager
                .write()
                .block_map
                .handle_heartbeat(101)
                .is_empty());

            replace_inode_record(&fs, inode_id, &inode_bytes)?;
            for _ in 0..2 {
                assert!(fs.block_report(report(), None)?.delete_blocks.is_empty());
                if full_report {
                    fs.wait_for_full_block_reconcile_for_test(101)?;
                }
                for &id in &ids {
                    assert_eq!(block_location_worker_ids(&fs, id)?, vec![100, 101]);
                }
                let mut reported = fs.fs_dir.read().get_worker_block_ids(101)?;
                reported.sort_unstable();
                let mut expected = ids.clone();
                expected.sort_unstable();
                assert_eq!(reported, expected);
                assert!(fs
                    .worker_manager
                    .write()
                    .block_map
                    .handle_heartbeat(101)
                    .is_empty());
            }
        }
        Ok(())
    }

    #[test]
    fn apply_reported_blocks_rechecks_membership_after_stale_preflight() -> CommonResult<()> {
        for overwrite in [false, true] {
            let fs = report_test_fs(if overwrite { "overwrite" } else { "invalidate" });
            let path = "/cached-file";
            fs.create_with_opts(
                path,
                CreateFileOptsBuilder::new()
                    .ttl_action(TtlAction::Delete)
                    .build(),
                OpenFlags::new_create(),
            )?;
            let client = ClientAddress::default();
            let block = fs.add_block(path, None, client.clone(), vec![], vec![], 0, None)?;
            let worker_id = block.locs[0].worker_id;
            fs.complete_file(
                path,
                None,
                128,
                vec![CommitBlock {
                    block_id: block.block.id,
                    block_len: 128,
                    locations: vec![BlockLocation::with_id(worker_id)],
                }],
                &client.client_name,
                false,
                None,
            )?;
            fs.set_attr(path, SetAttrOptsBuilder::new().ufs_mtime(12_345).build())?;

            // A report preflight can see a valid ID before another operation
            // discards that cache generation and the report acquires its lock.
            let preflight_inode = fs
                .fs_dir
                .read()
                .store
                .get_inode(InodeId::get_id(block.block.id), None)?
                .unwrap();
            assert!(preflight_inode
                .as_file_ref()?
                .block_ids()
                .contains(&block.block.id));
            let delayed_report = vec![BlockReportInfo::new(
                block.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                128,
            )];

            if overwrite {
                fs.create(path, false)?;
            } else {
                fs.delete_locations(worker_id)?;
            }

            assert_eq!(
                FsDir::apply_reported_blocks_with_read(
                    &fs.fs_dir,
                    worker_id,
                    false,
                    &mut BlockReportDiagnostics::default(),
                    delayed_report,
                )?,
                vec![block.block.id]
            );
            assert!(fs
                .fs_dir
                .read()
                .get_block_locations(block.block.id)?
                .is_empty());
        }
        Ok(())
    }

    #[test]
    fn apply_reported_blocks_accepts_current_ids_independent_of_vector_order() -> CommonResult<()> {
        let fs = report_test_fs("unordered");
        let path = "/restored-file";
        let status = fs.create(path, false)?;
        let fs_dir = fs.fs_dir.write();
        let mut inode = fs_dir.store.get_inode(status.id, None)?.unwrap();
        let ids = [
            InodeId::create_block_id(status.id, 2)?,
            InodeId::create_block_id(status.id, 0)?,
            InodeId::create_block_id(status.id, 1)?,
        ];
        // Journal/snapshot restoration can supply a block vector directly.
        // Membership validation must not interpret its positional order as an
        // ordering guarantee for IDs.
        inode.as_file_mut()?.blocks = ids.iter().map(|id| BlockMeta::new(*id, 128)).collect();
        fs_dir.store.apply_reopen_file(&inode)?;
        drop(fs_dir);
        let reports = ids
            .iter()
            .map(|id| {
                BlockReportInfo::new(*id, BlockReportStatus::Finalized, StorageType::Disk, 128)
            })
            .collect();
        assert!(FsDir::apply_reported_blocks_with_read(
            &fs.fs_dir,
            100,
            true,
            &mut BlockReportDiagnostics::default(),
            reports,
        )?
        .is_empty());
        let fs_dir = fs.fs_dir.read();
        for id in ids {
            assert_eq!(fs_dir.get_block_locations(id)?.len(), 1);
        }
        Ok(())
    }
}
