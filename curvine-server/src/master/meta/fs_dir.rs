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
use crate::master::meta::inode::InodeView::{Dir, File};
use crate::master::meta::inode::*;
use crate::master::meta::store::{InodeStore, RocksInodeStore};
use crate::master::meta::{BlockMeta, InodeId};
use crate::master::quota::eviction::evictor::Evictor;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::state::{
    BlockLocation, CommitBlock, CreateFileOpts, ExtendedBlock, FileAllocOpts, FileLock, FileStatus,
    FreeResult, ListOptions, MkdirOpts, MountInfo, RenameFlags, SetAttrOpts, WorkerAddress,
};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::sync::AtomicCounter;
use orpc::{err_box, err_ext, try_option, CommonResult};
use std::collections::{HashMap, LinkedList};
use std::mem;
use std::sync::Arc;

/// Note: The modification operation uses &mut self, which is a necessary improvement. We use the unsafe API to perform modifications.
pub struct FsDir {
    /// The root of the lightweight tree (DirEntry) for navigation
    pub(crate) root_entry: DirEntry,
    pub(crate) inode_id: InodeId,
    pub(crate) store: InodeStore,
    pub(crate) journal_writer: Arc<JournalWriter>,
    pub(crate) evictor: Arc<dyn Evictor>,
    pub(crate) op_id: AtomicCounter,
}

impl FsDir {
    pub fn new(
        conf: &ClusterConf,
        journal_writer: Arc<JournalWriter>,
        ttl_bucket_list: Arc<TtlBucketList>,
        evictor: Arc<dyn Evictor>,
    ) -> FsResult<Self> {
        let db_conf = conf.meta_rocks_conf();

        let store = RocksInodeStore::new(db_conf, conf.format_master)?;
        let state = InodeStore::new(store, ttl_bucket_list);

        let (last_inode_id, root_entry) = if conf.format_master {
            state.create_blank_tree()?
        } else {
            state.create_tree()?
        };

        let fs_dir = Self {
            root_entry,
            inode_id: InodeId::new(),
            store: state,
            journal_writer,
            evictor,
            op_id: AtomicCounter::new(0),
        };
        fs_dir.update_last_inode_id(last_inode_id)?;

        Ok(fs_dir)
    }

    /// Returns a reference to the root DirEntry
    pub fn root_entry(&self) -> &DirEntry {
        &self.root_entry
    }

    /// Returns a mutable reference to the root DirEntry
    pub fn root_entry_mut(&mut self) -> &mut DirEntry {
        &mut self.root_entry
    }

    fn next_inode_id(&self) -> FsResult<i64> {
        let id = self.inode_id.next()?;
        Ok(id)
    }

    pub fn next_op_id(&self) -> u64 {
        self.op_id.next()
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
    fn create_single_dir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let dir = InodeDir::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);

        inp = self.add_last_inode(inp, InodeView::new_dir(dir.clone()))?;

        let parent_path = inp.get_valid_parent_path();
        self.journal_writer.log_mkdir(self, &parent_path, &dir)?;

        Ok(inp)
    }

    // Create all previous directories that may be missing on the path.
    pub(crate) fn create_parent_dir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
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

        if !recursive {
            if let Some(last) = inp.get_last_inode() {
                if last.is_dir() && !self.is_dir_empty(inp) {
                    return err_ext!(FsError::dir_not_empty(inp.path()));
                }
            }
        }

        let del_res = self.unprotected_delete(inp, op_ms as i64)?;
        self.journal_writer
            .log_delete(self, inp.path(), op_ms as i64)?;

        Ok(del_res)
    }

    fn is_dir_empty(&self, inp: &InodePath) -> bool {
        inp.get_last_entry()
            .and_then(|entry| entry.children())
            .map(|c| c.is_empty())
            .unwrap_or(true)
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

        parent.update_mtime(mtime);

        let del_res = match child {
            File(f) => {
                if f.nlink() > 1 {
                    let target_inode = target.clone();
                    if let File(ref mut nf) = target_inode.as_mut() {
                        nf.decrement_nlink();
                    }
                    self.store.apply_unlink(parent.as_ref(), child_name, child.id())?
                } else {
                    self.store.apply_delete(parent.as_ref(), child_name, child)?
                }
            }
            Dir(_) => {
                parent.decr_nlink();
                self.store.apply_delete(parent.as_ref(), child_name, child)?
            }
        };

        self.remove_child_from_tree(inp, child_name);

        Ok(del_res)
    }

    fn remove_child_from_tree(&mut self, inp: &InodePath, child_name: &str) {
        if let Some(parent_entry) = inp.get_parent_entry() {
            parent_entry.as_mut().remove_child(child_name);
        }
    }

    pub fn free(&mut self, inp: &InodePath, recursive: bool) -> FsResult<FreeResult> {
        let op_ms = LocalTime::mills() as i64;

        if inp.is_root() {
            return err_box!("The root is not allowed to be free");
        }

        if inp.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(inp.path()));
        }

        let free_res = self.unprotected_free(inp, op_ms, recursive)?;
        self.journal_writer
            .log_free(self, inp.path(), op_ms, recursive)?;

        Ok(free_res)
    }

    pub(crate) fn unprotected_free(
        &mut self,
        inp: &InodePath,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<FreeResult> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("Inode not found for free"),
        };

        let mut free_res = FreeResult::default();
        let mut change_inodes = vec![];

        let mut stack: LinkedList<InodePtr> = LinkedList::new();
        stack.push_back(inode);

        while let Some(cur_inode) = stack.pop_front() {
            match cur_inode.as_mut() {
                Dir(_) => {
                    if recursive {
                        if let Some(entry) = self.find_entry_by_id(cur_inode.id()) {
                            if let Some(children) = entry.children() {
                                let child_ids: Vec<(String, i64)> = children
                                    .iter()
                                    .map(|(name, entry)| (name.to_string(), entry.id()))
                                    .collect();
                                for (child_name, child_id) in child_ids {
                                    if let Some(child_inode) = self.store.get_inode(child_id, Some(&child_name))? {
                                        stack.push_back(InodePtr::from_owned(child_inode));
                                    }
                                }
                            }
                        }
                    }
                }

                File(f) => {
                    let locs = f.get_locs(&self.store)?;
                    let len = f.len;
                    if f.free(mtime) {
                        free_res.add(len, locs);
                        change_inodes.push(cur_inode.as_ref().clone());
                    }
                }
            }
        }

        self.store.apply_free(change_inodes)?;
        Ok(free_res)
    }

    fn find_entry_by_id(&self, id: i64) -> Option<&DirEntry> {
        if self.root_entry.id() == id {
            return Some(&self.root_entry);
        }
        self.find_entry_recursive(&self.root_entry, id)
    }

    fn find_entry_recursive<'a>(&'a self, entry: &'a DirEntry, id: i64) -> Option<&'a DirEntry> {
        if let Some(children) = entry.children() {
            for (_, child) in children.iter() {
                if child.id() == id {
                    return Some(child);
                }
                if let Some(found) = self.find_entry_recursive(child, id) {
                    return Some(found);
                }
            }
        }
        None
    }

    pub fn rename(
        &mut self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        flags: RenameFlags,
    ) -> FsResult<Option<DeleteResult>> {
        let op_ms = LocalTime::mills();
        let res = self.unprotected_rename(src_inp, dst_inp, op_ms as i64, flags)?;
        self.journal_writer.log_rename(
            self,
            src_inp.path(),
            dst_inp.path(),
            op_ms as i64,
            flags,
        )?;
        Ok(res)
    }

    pub(crate) fn unprotected_rename(
        &mut self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        flags: RenameFlags,
    ) -> FsResult<Option<DeleteResult>> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        if flags.exchange_mode() {
            return err_box!("Rename failed, because exchange mode is not supported");
        }

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };

        // If no_replace is true and target file exists, return error; otherwise delete target file first
        let del_res = match dst_inp.get_last_inode() {
            Some(v) if v.is_file() => {
                if flags.no_replace() {
                    return err_ext!(FsError::file_exists(dst_inp.path()));
                } else {
                    Some(self.unprotected_delete(dst_inp, mtime)?)
                }
            }
            _ => None,
        };

        let mut new_name = dst_inp.name().to_string();
        let mut dst_parent = match dst_inp.get_last_inode() {
            Some(v) if v.is_dir() => {
                new_name = src_inp.name().to_string();
                v
            }
            _ => {
                match dst_inp.get_inode(-2) {
                    Some(v) => v,
                    None => return err_box!("Parent {} does not exist", dst_inp.get_parent_path()),
                }
            }
        };

        let mut new_inode = src_inode.as_ref().clone();
        new_inode.update_mtime(mtime);

        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        let src_name = src_inp.name();
        self.store.apply_rename(
            src_parent.as_ref(),
            src_name,
            dst_parent.as_ref(),
            &new_name,
            &new_inode,
        )?;

        self.remove_child_from_tree(src_inp, src_inp.name());

        let child_entry = if new_inode.is_dir() {
            DirEntry::new_dir(new_inode.id())
        } else {
            DirEntry::new_file(new_inode.id())
        };
        self.add_child_to_tree(dst_inp, &new_name, child_entry);

        Ok(del_res)
    }

    pub fn create_file(&mut self, mut inp: InodePath, opts: CreateFileOpts) -> FsResult<InodePath> {
        if inp.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(inp.path()));
        }

        inp = self.create_parent_dir(inp, opts.dir_opts())?;

        let file = InodeFile::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
        inp = self.add_last_inode(inp, InodeView::new_file(file))?;
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

        parent.update_mtime(child.mtime());
        if child.is_dir() {
            parent.incr_nlink();
        }

        let child_name = inp.get_component(inp.existing_len())?.to_string();
        let child_id = child.id();
        let child_entry = if child.is_dir() {
            DirEntry::new_dir(child_id)
        } else {
            DirEntry::new_file(child_id)
        };

        let mut child_entry_ref = None;
        if let Some(parent_entry) = inp.get_parent_entry() {
            parent_entry.as_mut().add_child(child_name.clone(), child_entry);
            if let Some(child) = parent_entry.as_ref().get_child(&child_name) {
                child_entry_ref = Some(DirEntryRef::from_ref(child));
            }
        }

        let child_ptr = InodePtr::from_owned(child);
        self.store.apply_add(parent.as_ref(), &child_name, child_ptr.as_ref())?;

        if let Some(entry_ref) = child_entry_ref {
            inp.append_with_entry(child_ptr, entry_ref)?;
        } else {
            inp.append(child_ptr)?;
        }

        Ok(inp)
    }

    fn add_child_to_tree(&mut self, inp: &InodePath, child_name: &str, child_entry: DirEntry) {
        if let Some(parent_entry) = inp.get_last_entry() {
            parent_entry.as_mut().add_child(child_name.to_string(), child_entry);
        }
    }

    pub fn file_status(&self, inp: &InodePath) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let status = match inode.as_ref() {
            File(..) | Dir(..) => inode.to_file_status(inp.path(), inp.name()),
        };

        Ok(status)
    }

    pub fn list_status(&self, inp: &InodePath) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("File {} not exists", inp.path()),
        };

        let mut res = Vec::new();
        match inode.as_ref() {
            File(_) => res.push(inode.to_file_status(inp.path(), inp.name())),

            Dir(_d) => {
                let mut dir_entry = &self.root_entry;
                for i in 1..inp.existing_len() {
                    let name = inp.get_component(i)?;
                    match dir_entry.get_child(name) {
                        Some(child) => dir_entry = child,
                        None => break,
                    }
                }

                if let Some(children) = dir_entry.children() {
                    for (child_name, child_entry) in children.iter() {
                        if let Some(child_inode) = self.store.get_inode(child_entry.id(), Some(child_name))? {
                            let child_path = inp.child_path(child_name);
                            res.push(child_inode.to_file_status(&child_path, child_name));
                        }
                    }
                }
            }
        }

        Ok(res)
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
            None => return err_box!("File {} not exists", inp.path()),
        };

        match inode.as_ref() {
            File(_) => {
                let status = inode.to_file_status(inp.path(), inp.name());
                Ok(Self::list_single_file(status, opts))
            }

            Dir(_d) => {
                let mut dir_entry = &self.root_entry;
                for i in 1..inp.existing_len() {
                    let name = inp.get_component(i)?;
                    match dir_entry.get_child(name) {
                        Some(child) => dir_entry = child,
                        None => break,
                    }
                }

                let mut res = Vec::new();
                let mut count = 0i32;

                if let Some(children) = dir_entry.children() {
                    for (child_name, child_entry) in children.iter() {
                        if let Some(sa) = opts.start_after.as_deref() {
                            if child_name <= sa {
                                continue;
                            }
                        }

                        if let Some(limit) = opts.limit {
                            if count >= limit as i32 {
                                break;
                            }
                        }

                        if let Some(child_inode) = self.store.get_inode(child_entry.id(), Some(child_name))? {
                            let child_path = inp.child_path(child_name);
                            res.push(child_inode.to_file_status(&child_path, child_name));
                            count += 1;
                        }
                    }
                }

                Ok(res)
            }
        }
    }

    pub fn acquire_new_block(
        &mut self,
        inp: &InodePath,
        commit_blocks: Vec<CommitBlock>,
        choose_workers: &[WorkerAddress],
        file_len: i64,
    ) -> FsResult<ExtendedBlock> {
        let mut inode = try_option!(inp.get_last_inode());
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
            .log_add_block(self, inp.path(), inode.as_file_ref()?, commit_blocks)?;
        Ok(block)
    }

    pub fn complete_file(
        &mut self,
        inp: &InodePath,
        len: i64,
        commit_block: Vec<CommitBlock>,
        client_name: impl AsRef<str>,
        only_flush: bool,
    ) -> FsResult<bool> {
        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;
        file.complete(len, &commit_block, client_name, only_flush)?;

        self.evictor.on_access(file.id());

        self.store
            .apply_complete_file(inode.as_ref(), &commit_block)?;
        self.journal_writer.log_complete_file(
            self,
            inp.path(),
            inode.as_file_ref()?,
            commit_block,
        )?;

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
        };

        let file = inode.as_file_mut()?;
        let _ = file.reopen(client_name);
        let status = inode.to_file_status(inp.path(), inp.name());

        self.store.apply_reopen_file(&inode)?;
        self.journal_writer
            .log_reopen_file(self, inp.path(), inode.as_file_ref()?)?;

        Ok(status)
    }

    // Determine whether the current block has been deleted.
    //Judge whether the block's inode exists. Block will only be deleted if the inode is deleted. All this judgment is not problematic.
    pub fn block_exists(&self, block_id: i64) -> FsResult<bool> {
        let file_id = InodeId::get_id(block_id);
        let inode = self.store.get_inode(file_id, None)?;
        match inode {
            None => Ok(false),
            Some(v) => {
                if v.is_file() {
                    Ok(true)
                } else {
                    err_box!(
                        "block_id {} resolves to inode {:?} which is not a file",
                        block_id,
                        v
                    )
                }
            }
        }
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
        self.root_entry.print_tree()
    }

    pub fn sum_hash(&self) -> u128 {
        self.root_entry.sum_hash()
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
    pub fn create_tree(&self) -> CommonResult<DirEntry> {
        self.store.create_tree().map(|x| x.1)
    }

    // Restore in-memory tree from RocksDB without checkpoint (for testing only).
    // In production, use restore() with checkpoint path via Raft snapshot.
    pub fn restore_from_rocksdb(&mut self) -> CommonResult<()> {
        let (last_inode_id, root_entry) = self.store.create_tree()?;
        self.root_entry = root_entry;
        self.update_last_inode_id(last_inode_id)?;
        Ok(())
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.store.create_checkpoint(id)
    }

    pub fn restore<T: AsRef<str>>(&mut self, path: T) -> CommonResult<()> {
        let mut spend = TimeSpent::new();
        let path = path.as_ref();

        // Set to other value first to facilitate memory recycling.
        self.root_entry = DirEntry::new_dir(ROOT_INODE_ID);

        // Reset rocksdb
        self.store.restore(path)?;
        let time1 = spend.used_ms();
        spend.reset();

        // Update the directory tree
        let (last_inode_id, root_entry) = self.store.create_tree()?;
        self.root_entry = root_entry;
        self.update_last_inode_id(last_inode_id)?;
        let time2 = spend.used_ms();

        info!(
            "restore from {}, restore rocksdb used {} ms, \
        build in-memory directory tree used {} ms, \
        statistics updated during tree reconstruction, last_inode_id {}",
            path, time1, time2, last_inode_id
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

    pub fn get_rocks_store(&self) -> &RocksInodeStore {
        &self.store.store
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        let block_ids = self.store.store.delete_locations(worker_id)?;
        Ok(block_ids)
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

    pub fn set_attr(&mut self, inp: InodePath, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        self.unprotected_set_attr(&inp, opts.clone())?;
        self.journal_writer.log_set_attr(self, &inp, opts)?;
        Ok(inode.to_file_status(inp.path(), inp.name()))
    }

    pub fn unprotected_set_attr(&mut self, inp: &InodePath, opts: SetAttrOpts) -> FsResult<()> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("Inode not found for set_attr"),
        };

        let mut change_inodes = vec![];
        let mut stack: LinkedList<InodePtr> = LinkedList::new();
        stack.push_back(inode.clone());

        let child_opts = opts.child_opts();

        while let Some(cur_inode) = stack.pop_front() {
            let set_opts = if cur_inode.id() != inode.id() {
                child_opts.clone()
            } else {
                opts.clone()
            };
            cur_inode.as_mut().set_attr(set_opts);
            change_inodes.push(cur_inode.as_ref().clone());

            if opts.recursive {
                if let Some(entry) = self.find_entry_by_id(cur_inode.id()) {
                    if entry.is_dir() {
                        if let Some(children) = entry.children() {
                            let child_ids: Vec<(String, i64)> = children
                                .iter()
                                .map(|(name, entry)| (name.to_string(), entry.id()))
                                .collect();
                            for (child_name, child_id) in child_ids {
                                if let Some(child_inode) = self.store.get_inode(child_id, Some(&child_name))? {
                                    stack.push_back(InodePtr::from_owned(child_inode));
                                }
                            }
                        }
                    }
                }
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
    ) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        let new_inode = InodeFile::with_link(self.inode_id.next()?, op_ms as i64, target, mode);

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

        // Get inode id before moving new_inode into InodeView
        let new_inode_id = new_inode.id();
        let new_inode_view = InodeView::new_file(new_inode);
        let new_inode_ptr = match old_inode {
            Some(v) => {
                let _ = mem::replace(v.as_mut(), new_inode_view);
                v
            }
            None => {
                let child_entry = DirEntry::new_file(new_inode_id);
                self.add_child_to_tree(&link, &name, child_entry);

                let added = InodePtr::from_owned(new_inode_view);
                link.append(added)?;
                link.get_last_inode().unwrap()
            }
        };

        self.store
            .apply_symlink(parent.as_ref(), &name, new_inode_ptr.as_ref(), is_add)?;
        Ok(link)
    }

    pub fn link(&mut self, src_path: InodePath, dst_path: InodePath) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        let (original_inode_id, mut original_inode_ptr) = match src_path.get_last_inode() {
            Some(inode) => match inode.as_ref() {
                File(file) => {
                    if file.file_type != curvine_common::state::FileType::File {
                        return err_ext!(FsError::common("Cannot create link to non-regular file"));
                    }
                    (file.id, Some(inode.clone()))
                }
                Dir(_) => return err_ext!(FsError::common("Cannot create link to directory")),
            },
            None => return err_ext!(FsError::file_not_found(src_path.path())),
        };

        if let Some(ref mut inode_ptr) = original_inode_ptr {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink();
            }
        }

        let dst_path_str = dst_path.path().to_string();
        self.unprotected_link(dst_path, original_inode_id, op_ms)?;

        self.journal_writer
            .log_link(self, src_path.path(), &dst_path_str)?;

        Ok(())
    }

    pub fn unprotected_link(
        &mut self,
        mut new_path: InodePath,
        original_inode_id: i64,
        op_ms: u64,
    ) -> FsResult<InodePath> {
        if new_path.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(new_path.path()));
        }

        // Create parent directory if needed
        new_path = self.create_parent_dir(new_path, MkdirOpts::with_create(true))?;

        let mut parent = match new_path.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent directory does not exist"),
        };

        let original_inode = match self.store.get_inode(original_inode_id, None)? {
            Some(v) => v,
            None => return err_box!("Original inode {} not found", original_inode_id),
        };

        let name = new_path.name().to_string();
        let linked_inode = original_inode.clone();

        parent.update_mtime(op_ms as i64);
        let added = InodePtr::from_owned(linked_inode);

        let child_entry = DirEntry::new_file(original_inode_id);
        self.add_child_to_tree(&new_path, &name, child_entry);

        new_path.append(added)?;

        self.store
            .apply_link(parent.as_ref(), &name, original_inode_id)?;

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
        let file = inode.as_file_mut()?;

        if file.len == opts.len {
            return Ok(DeleteResult::new());
        }
        let del_blocks = file.resize(opts.clone())?;
        debug!("resize file {} success, opts: {:?}", inp.path(), opts);

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
            .log_complete_file(self, inp.path(), inode.as_file_ref()?, vec![])?;

        Ok(del_res)
    }

    pub fn assign_worker(
        &mut self,
        inp: InodePath,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<ExtendedBlock> {
        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;

        let block = file.search_block_mut_check(block_id)?;
        let res = block.assign_worker(workers);
        let block = ExtendedBlock {
            id: block.id,
            len: block.len as i64,
            alloc_opts: block.alloc_opts.clone(),
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
        };

        if res {
            self.store.apply_new_block(inode.as_ref(), &[])?;
            self.journal_writer
                .log_add_block(self, inp.path(), inode.as_file_ref()?, vec![])?;
        }

        Ok(block)
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
        let conflict = meta.set_lock(lock, expire_ms);

        let locks = meta.to_vec();
        self.store.apply_set_locks(inode.id(), &locks)?;
        self.journal_writer.log_set_locks(self, inode.id(), locks)?;

        Ok(conflict)
    }
}
