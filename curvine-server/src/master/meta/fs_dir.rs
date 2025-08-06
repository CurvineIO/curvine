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
use crate::master::meta::inode::ttl::ttl_bucket::TtlBucketList;
use crate::master::meta::inode::InodeView::{Dir, File};
use crate::master::meta::inode::*;
use crate::master::meta::store::{InodeStore, RocksInodeStore};
use crate::master::meta::{BlockMeta, InodeId};
use crate::master::mount::MountPointEntry;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::state::{
    BlockLocation, CommitBlock, CreateFileOpts, ExtendedBlock, FileStatus, MkdirOpts, SetAttrOpts,
    WorkerAddress,
};
use curvine_common::FsResult;
use log::info;
use orpc::common::{LocalTime, TimeSpent};
use orpc::{err_box, err_ext, try_option, CommonResult};
use std::collections::{HashMap, LinkedList};
use std::mem;
use std::sync::Arc;

/// Note: The modification operation uses &mut self, which is a necessary improvement. We use the unsafe API to perform modifications.
pub struct FsDir {
    pub(crate) root_dir: InodeView,
    pub(crate) inode_id: InodeId,
    pub(crate) store: InodeStore,
    pub(crate) journal_writer: JournalWriter,
}

impl FsDir {
    pub fn new(
        conf: &ClusterConf,
        journal_writer: JournalWriter,
        ttl_bucket_list: Arc<TtlBucketList>,
    ) -> FsResult<Self> {
        let db_conf = conf.meta_rocks_conf();

        let store = RocksInodeStore::new(db_conf, conf.format_master)?;
        let state = InodeStore::new(store, ttl_bucket_list);
        let (last_inode_id, root_dir) = state.create_tree()?;

        let fs_dir = Self {
            root_dir,
            inode_id: InodeId::new(),
            store: state,
            journal_writer,
        };
        fs_dir.update_last_inode_id(last_inode_id)?;

        Ok(fs_dir)
    }

    pub fn inode_store(&self) -> InodeStore {
        self.store.clone()
    }
    // Create root directory
    pub fn create_root() -> InodeView {
        Dir(InodeDir::new(ROOT_INODE_ID, ROOT_INODE_NAME, 0))
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
        let op_ms = LocalTime::mills();

        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() - 1;
        let name = inp.get_component(pos + 1)?;

        let dir = InodeDir::with_opts(self.next_inode_id()?, name, LocalTime::mills() as i64, opts);

        inp = self.add_last_inode(inp, Dir(dir))?;
        self.journal_writer.log_mkdir(op_ms, &inp)?;

        Ok(inp)
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
        if !inp.is_full() {
            return err_box!("Path not exists: {}", inp.path());
        }

        if !inp.is_empty_dir() && !recursive {
            return err_box!("{} is non empty", inp.path());
        }
        let del_res = self.unprotected_delete(inp, op_ms as i64)?;
        self.journal_writer
            .log_delete(op_ms, inp.path(), op_ms as i64)?;

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

        // Delete the data in rocksdb.
        parent.update_mtime(mtime);
        let del_res = self.store.apply_delete(parent.as_ref(), child)?;

        // After deletion occurs, the target address cannot be used.
        let _ = parent.delete_child(child.id(), child.name())?;
        Ok(del_res)
    }

    pub fn rename(&mut self, src_inp: &InodePath, dst_inp: &InodePath) -> FsResult<bool> {
        let op_ms = LocalTime::mills();
        self.unprotected_rename(src_inp, dst_inp, op_ms as i64)?;
        self.journal_writer
            .log_rename(op_ms, src_inp.path(), dst_inp.path(), op_ms as i64)?;
        Ok(true)
    }

    pub(crate) fn unprotected_rename(
        &mut self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
    ) -> FsResult<()> {
        // Delete src_inp
        let src_inode = match src_inp.get_last_inode() {
            None => return err_box!("File not exits: {}", src_inp.path()),
            Some(v) => v,
        };

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exits: {}", src_inp.path()),
            Some(v) => v,
        };

        // Get the target parent node that needs to be added
        // 1. If dst last is a directory, then a new node is created under that directory.
        // 2. If dst last does not exist, create a node in the parent directory.
        let mut new_name = dst_inp.name();
        let mut dst_parent = match dst_inp.get_last_inode() {
            Some(v) => {
                // /1.log -> /b is equivalent to /1.log /b/1.log
                // b is an existing directory, indicating that you can move to this directory.
                if v.is_dir() {
                    new_name = src_inp.name();
                    v
                } else {
                    return err_box!("Rename failed, because dst {} is exists", dst_inp.path());
                }
            }
            None => {
                // /1.log -> /a/b b does not exist, think that b is the new inode name
                match dst_inp.get_inode(-2) {
                    Some(v) => v,
                    None => return err_box!("Parent {} does not exist", dst_inp.get_parent_path()),
                }
            }
        };

        // Modify the time and name of the rename node.
        let mut new_inode = src_inode.as_ref().clone();
        new_inode.change_name(new_name);
        new_inode.update_mtime(mtime);

        // Update the parent directory for the last modification time.
        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        // Update state.
        self.store.apply_rename(
            src_parent.as_ref(),
            src_inode.as_ref(),
            dst_parent.as_ref(),
            &new_inode,
        )?;

        // Update memory status.
        // step 1: Delete the original node.
        // step 2: Add a new node.
        let _ = src_parent.delete_child(src_inode.id(), src_inode.name())?;
        let _ = dst_parent.add_child(new_inode)?;

        Ok(())
    }

    pub fn create_file(&mut self, mut inp: InodePath, opts: CreateFileOpts) -> FsResult<InodePath> {
        let op_ms = LocalTime::mills();
        // If overwrite = true, delete the existing file first.
        if inp.get_last_inode().is_some() {
            if opts.overwrite() {
                let _ = self.delete(&inp, false)?;
                inp.delete_last();
            } else {
                return err_ext!(FsError::file_exists(inp.path()));
            }
        }

        // Create a directory that does not exist.
        inp = self.create_parent_dir(inp, opts.dir_opts())?;

        // Create an inode file node.
        let file = InodeFile::with_opts(
            self.inode_id.next()?,
            inp.name(),
            LocalTime::mills() as i64,
            opts,
        );
        inp = self.add_last_inode(inp, File(file))?;
        self.journal_writer.log_create_file(op_ms, &inp)?;
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

        // Update inode data.
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

        Ok(inode.to_file_status(inp.path()))
    }

    pub fn list_status(&self, inp: &InodePath) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("File {} not exists", inp.path()),
        };

        let mut res = Vec::with_capacity(1.max(inode.child_len()));
        match inode.as_ref() {
            File(_) => res.push(inode.to_file_status(inp.path())),

            Dir(d) => {
                for item in d.children_iter() {
                    let child_path = inp.child_path(item.name());
                    res.push(item.to_file_status(&child_path));
                }
            }
        }

        Ok(res)
    }

    fn commit_block(file: &mut InodeFile, commit: Option<&CommitBlock>) -> FsResult<()> {
        let commit = match commit {
            None => return Ok(()),
            Some(v) => v,
        };

        let last_block = match file.blocks.last_mut() {
            None => {
                return err_box!(
                    "Inode file {}({}) block status is abnormal, no blocks",
                    file.id,
                    file.name
                )
            }
            Some(v) => v,
        };
        if last_block.id != commit.block_id {
            return err_box!("Inode file {}({}) block status is abnormal, expected last block id {}, actual submitted block id {}",
                 file.id, file.name, last_block.id, commit.block_id);
        }

        if !last_block.is_writing() {
            return err_box!(
                "Inode file {}({}), block {} not writing status",
                file.id,
                file.name,
                commit.block_id
            );
        }
        last_block.commit(commit);

        Ok(())
    }

    // Check the file block status. If it is a retry block, then return this block directly.
    pub fn analyze_block_state<'a>(
        file: &'a mut InodeFile,
        commit: Option<&CommitBlock>,
    ) -> FsResult<Option<&'a BlockMeta>> {
        let last_block = file.get_block(-1);
        if BlockMeta::matching_block(last_block, commit) {
            return Ok(None);
        }

        let penultimate = file.get_block(-2);
        if BlockMeta::matching_block(penultimate, commit) {
            if last_block.is_none() {
                err_box!("Abnormal block status")
            } else {
                Ok(last_block)
            }
        } else {
            Ok(None)
        }
    }

    pub fn acquire_new_block(
        &mut self,
        inp: &InodePath,
        commit_block: Option<CommitBlock>,
        choose_workers: &[WorkerAddress],
    ) -> FsResult<ExtendedBlock> {
        let op_ms = LocalTime::mills();
        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;

        // Check whether it is a retry request.
        let retry_block = Self::analyze_block_state(file, commit_block.as_ref())?;
        if let Some(b) = retry_block {
            return Ok(ExtendedBlock {
                id: b.id,
                len: b.len,
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
            });
        }

        let new_block_id = file.next_block_id()?;

        // commit block
        Self::commit_block(file, commit_block.as_ref())?;

        // create block.
        file.add_block(BlockMeta::with_pre(new_block_id, choose_workers));

        let block = ExtendedBlock {
            id: new_block_id,
            len: 0,
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
        };

        // state add block.
        self.store
            .apply_new_block(inode.as_ref(), commit_block.as_ref())?;
        self.journal_writer
            .log_add_block(op_ms, inp.path(), inode.as_file_ref()?, commit_block)?;
        Ok(block)
    }

    pub fn complete_file(
        &mut self,
        inp: &InodePath,
        len: i64,
        commit_block: Option<CommitBlock>,
    ) -> FsResult<bool> {
        let op_ms = LocalTime::mills();
        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;
        if file.is_complete() {
            // The file has been completed, it is a duplicate request from the client service.
            return Ok(false);
        }

        // commit block
        Self::commit_block(file, commit_block.as_ref())?;

        // Update file status.
        file.mtime = LocalTime::mills() as i64;
        file.features.file_write = None;
        file.len = len;

        self.store
            .apply_complete_file(inode.as_ref(), commit_block.as_ref())?;
        self.journal_writer.log_complete_file(
            op_ms,
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
        Ok(locs)
    }

    pub fn get_block_locations(&self, block_id: i64) -> FsResult<Vec<BlockLocation>> {
        Ok(self.store.get_block_locations(block_id)?)
    }

    pub fn append_file(
        &mut self,
        inp: &InodePath,
        client_name: impl AsRef<str>,
    ) -> FsResult<(Option<ExtendedBlock>, FileStatus)> {
        let op_ms = LocalTime::mills();
        let mut inode = match inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(inp.path())),

            Some(v) => {
                if v.is_dir() {
                    let err_msg =
                        format!("Cannot append to already exists {} directory", inp.path());
                    return err_ext!(FsError::file_exists(err_msg));
                } else {
                    v
                }
            }
        };

        let file = inode.as_file_mut()?;
        if !file.is_complete() {
            return err_box!("Cannot append not complete file {}", inp.path());
        }

        let last_block = file.append(client_name);
        let status = inode.to_file_status(inp.path());

        self.store.apply_append_file(inode.as_ref())?;
        self.journal_writer.log_append_file(op_ms, inp)?;

        Ok((last_block, status))
    }

    // Determine whether the current block has been deleted.
    //Judge whether the block's inode exists. Block will only be deleted if the inode is deleted. All this judgment is not problematic.
    pub fn block_exists(&self, block_id: i64) -> FsResult<bool> {
        let file_id = InodeId::get_id(block_id);
        let inode = self.store.get_inode(file_id)?;
        match inode {
            None => Ok(false),
            Some(v) => {
                if v.is_file() {
                    Ok(true)
                } else {
                    err_box!(
                        "block_id {} resolves to inode_id {} which is not a file",
                        block_id,
                        file_id
                    )
                }
            }
        }
    }

    pub fn print_tree(&self) {
        self.root_dir.print_tree()
    }

    pub fn sum_hash(&self) -> u128 {
        let mut tree_hash = self.root_dir.sum_hash();
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_INODES);
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_EDGES);
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_LOCATION);
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_BLOCK);
        tree_hash
    }

    pub fn last_inode_id(&self) -> i64 {
        self.inode_id.current()
    }

    pub fn update_last_inode_id(&self, new_value: i64) -> CommonResult<()> {
        if new_value > self.last_inode_id() {
            self.inode_id.reset(new_value)
        } else {
            Ok(())
        }
    }

    // Read data from rocksdb to build a directory tree
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        self.store.create_tree().map(|x| x.1)
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.store.create_checkpoint(id)
    }

    pub fn restore<T: AsRef<str>>(&mut self, path: T) -> CommonResult<()> {
        let mut spend = TimeSpent::new();
        let path = path.as_ref();

        // Set to other values ​​first to facilitate memory recycling.
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
            "Restore from {}, restore rocksdb used {} ms, \
        build in-memory directory tree used {} ms",
            path, time1, time2
        );

        Ok(())
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        self.store.get_checkpoint_path(id)
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

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<()> {
        self.store.store.delete_locations(worker_id)?;
        Ok(())
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        self.journal_writer.take_entries()
    }

    pub fn mount(&mut self, entry: MountPointEntry) -> FsResult<()> {
        // Create parent directory
        let op_ms = LocalTime::mills();
        self.store.store.add_mountpoint(entry.id, &entry)?;

        self.journal_writer.log_mount(
            op_ms,
            entry.id,
            &entry.curvine_uri,
            &entry.ufs_uri,
            entry.to_mount_options(),
        )?;
        Ok(())
    }

    pub fn unmount(&mut self, id: u32) -> FsResult<()> {
        // Create parent directory
        let op_ms = LocalTime::mills();
        self.store.store.remove_mountpoint(id)?;
        self.journal_writer.log_unmount(op_ms, id)?;
        Ok(())
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountPointEntry>> {
        self.store.get_mount_table()
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountPointEntry>> {
        self.store.get_mount_point(id)
    }

    pub fn set_attr(&mut self, inp: InodePath, opts: SetAttrOpts) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        self.unprotected_set_attr(inode, opts.clone())?;
        self.journal_writer.log_set_attr(op_ms, &inp, opts)?;
        Ok(())
    }

    pub fn unprotected_set_attr(&mut self, inode: InodePtr, opts: SetAttrOpts) -> FsResult<()> {
        let child_opts = opts.child_opts();
        let recursive = opts.recursive;
        let parent_inode_id = inode.id();
        let mut change_inodes: Vec<InodePtr> = vec![];

        // set current inode
        inode.as_mut().set_attr(opts);
        change_inodes.push(inode.clone());

        // recursive set child inode
        if recursive {
            let mut stack = LinkedList::new();
            stack.push_back(inode);
            while let Some(cur_inode) = stack.pop_front() {
                if cur_inode.id() != parent_inode_id {
                    cur_inode.as_mut().set_attr(child_opts.clone());
                    change_inodes.push(cur_inode.clone());
                }

                for child in cur_inode.children() {
                    stack.push_back(InodePtr::from_ref(child));
                }
            }
        }

        self.store.apply_set_attr(change_inodes)?;
        Ok(())
    }

    pub fn symlink(
        &mut self,
        target: InodePath,
        link: InodePath,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        if target.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(target.path()));
        }

        let new_inode = InodeFile::with_link(
            self.inode_id.next()?,
            link.name(),
            op_ms as i64,
            target.path(),
            mode,
        );

        let link = self.unprotected_symlink(link, new_inode, force)?;
        self.journal_writer.log_symlink(op_ms, &link, force)?;
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

        parent.update_mtime(new_inode.mtime);
        let new_inode_ptr = match old_inode {
            Some(v) => {
                let _ = mem::replace(v.as_mut(), File(new_inode));
                v
            }
            None => {
                let added = parent.add_child(File(new_inode))?;
                link.append(added.clone())?;
                added
            }
        };

        self.store
            .apply_symlink(parent.as_ref(), new_inode_ptr.as_ref())?;
        Ok(link)
    }
}
