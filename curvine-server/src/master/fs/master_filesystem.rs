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
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::{InodePath, InodeView, PATH_SEPARATOR};
use crate::master::meta::FsDir;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_common::conf::{ClusterConf, MasterConf};
use curvine_common::error::FsError;
use curvine_common::state::*;
use curvine_common::FsResult;
use log::warn;
use orpc::sync::ArcRwLock;
use orpc::{err_box, err_ext, try_option, CommonResult};
use std::sync::Arc;

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
}

impl MasterFilesystem {
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
        }
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        Self {
            fs_dir: js.fs().fs_dir.clone(),
            worker_manager: js.worker_manager(),
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
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

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<bool> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        // Creation of root directory is not allowed
        if inp.is_root() {
            return err_box!("Not allowed to create existing root path: {}", inp.path());
        }

        if inp.is_full() {
            return Ok(true);
        }

        // Check whether the directory can be created recursively.
        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let _ = fs_dir.mkdir(inp, opts)?;
        Ok(true)
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<bool> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<bool> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        if !inp.is_empty_dir() && !recursive {
            return err_box!("{} is non empty", inp.path());
        }

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_box!("F ailed to remove {} because it does not exist", inp.path());
        }

        let delete_result = fs_dir.delete(&inp, recursive)?;

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&delete_result);

        Ok(true)
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        let mut fs_dir = self.fs_dir.write();
        let src_inp = Self::resolve_path(&fs_dir, src)?;
        let dst_inp = Self::resolve_path(&fs_dir, dst)?;

        if src_inp.is_root() {
            return err_box!("Cannot rename root path");
        }

        if src == dst {
            return Ok(false);
        }

        // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed operations.
        if dst.starts_with(src) && &dst[src.len()..src.len() + 1] == PATH_SEPARATOR {
            return err_box!(
                "Rename dst {} is a directory or file under src {}",
                dst,
                src
            );
        }

        fs_dir.rename(&src_inp, &dst_inp)?;

        Ok(true)
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx)
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
    ) -> FsResult<FileStatus> {
        if !opts.create() {
            return err_box!("Flag error {}, cannot create file", opts.create_flag);
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

        let last_inode = inp.get_last_inode();
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }
        }

        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        // If the file already exists and overwrite is enabled, overwrite the file
        let inp = if last_inode.is_some() && opts.overwrite() {
            let clean_result = fs_dir.overwrite_file(&inp, opts)?;
            if !clean_result.blocks.is_empty() {
                let mut worker_manager = self.worker_manager.write();
                worker_manager.remove_blocks(&clean_result);
            }
            inp
        } else {
            // If the file does not exist, create a new file
            fs_dir.create_file(inp, opts)?
        };

        let status = fs_dir.file_status(&inp)?;

        Ok(status)
    }

    pub fn append_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
    ) -> FsResult<(Option<LocatedBlock>, FileStatus)> {
        if !opts.append() {
            return err_box!("Flag error {}, cannot append file", opts.create_flag);
        }
        let path = path.as_ref();

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        if inp.get_last_inode().is_none() {
            if opts.create() {
                drop(fs_dir);
                let status = self.create_with_opts(path, opts)?;
                Ok((None, status))
            } else {
                err_ext!(FsError::file_not_found(inp.path()))
            }
        } else {
            let (last_block, file_status) = fs_dir.append_file(&inp, &opts.client_name)?;

            let last_block = if let Some(last_block) = last_block {
                let block_locs = fs_dir.get_block_locations(last_block.id)?;
                let wm = self.worker_manager.read();
                let lb = wm.create_locate_block(inp.path(), last_block, &block_locs)?;
                drop(wm);
                Some(lb)
            } else {
                None
            };
            Ok((last_block, file_status))
        }
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        let status = fs_dir.file_status(&inp)?;
        Ok(status)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Ok(inp.get_last_inode().is_some())
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        fs_dir.list_status(&inp)
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
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
        inp: &InodePath,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("File {} not exists", inp.path()),
        };

        let file = inode.as_file_ref()?;

        if let Some(v) = previous {
            if v.block_len != file.block_size {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        match file.write_feature() {
            None => {
                return err_box!(
                    "File is not open for writing, path {}, inode id {}",
                    inp.path(),
                    inode.id()
                )
            }

            Some(v) if v.client_name != client_addr.client_name => {
                return err_box!(
                    "The client name written to file {} is incorrect, expected {}, actual {}",
                    inp.path(),
                    v.client_name,
                    client_addr.client_name
                )
            }

            _ => (),
        }

        let res = ValidateAddBlock {
            replicas: file.replicas,
            block_size: file.block_size,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
            file_inode: inode,
        };

        Ok(res)
    }

    /// Document application to allocate a new block.
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        client_addr: ClientAddress,
        previous: Option<CommitBlock>,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let mut fs_dir = self.fs_dir.write();

        // Verify file status.
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        let validate_block = Self::validate_add_block(&inp, &client_addr, previous.as_ref())?;

        let wm = self.worker_manager.read();

        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);

        // Select worker.
        let choose_workers = wm.choose_worker(choose_ctx)?;
        drop(wm);

        let block = fs_dir.acquire_new_block(&inp, previous, &choose_workers)?;
        let located = LocatedBlock {
            block,
            locs: choose_workers,
        };

        Ok(located)
    }

    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        len: i64,
        last: Option<CommitBlock>,
        client_name: T,
    ) -> FsResult<bool> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let inode = match inp.get_last_inode() {
            None => return err_box!("File does not exist: {}", inp.path()),
            Some(v) => v,
        };

        if !inode.is_file() {
            return err_box!("INode is not a regular file: {}", inp.path());
        }

        //from store
        let mut inode = fs_dir.store.get_inode(inode.id(), None)?;
        let inode = match inode.as_mut() {
            Some(inode_view) => inode_view,
            None => {
                return err_box!("File does not exist: {}", inp.path());
            }
        };

        let file = inode.as_file_mut()?;
        if file.is_complete() {
            return Ok(false);
        }

        // Verify file length
        let commit_len = file.commit_len(last.as_ref());
        if commit_len != len {
            return err_box!(
                "complete_file file size exception, expected {}, submitted {}",
                commit_len,
                len
            );
        }

        // Verify file write status.
        match &file.features.file_write {
            Some(future) if future.client_name != client_name.as_ref() => {
                return err_box!(
                    "Client (={}) is not the lease owner (={})",
                    client_name.as_ref(),
                    future.client_name
                )
            }

            _ => (),
        }

        fs_dir.complete_file(&inp, len, last)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        let fs_dir = self.fs_dir.read();
        let path = path.as_ref();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = try_option!(inp.get_last_inode(), "File {} not exits", path);
        let file = inode.as_file_ref()?;

        let wm = self.worker_manager.read();
        let file_locs = fs_dir.get_file_locations(file)?;
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len != file.block_size {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len,
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len,
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
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

        let locate_blocks = FileBlocks {
            status: inode.to_file_status(path),
            block_locs,
        };

        Ok(locate_blocks)
    }

    pub fn master_info(&self) -> FsResult<MasterInfo> {
        let metrics = Master::get_metrics();
        let mut info = MasterInfo {
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
                WorkerStatus::Live => info.live_workers.push(worker.clone()),
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

    pub fn sum_hash(&self) -> u128 {
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

    fn block_exists(&self, id: i64) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        fs_dir.block_exists(id)
    }

    /// Process block reports
    pub fn block_report(&self, list: BlockReportList) -> FsResult<()> {
        // @todo check cluster.
        if list.blocks.is_empty() {
            return Ok(());
        }

        //(Whether to increase, block id, block location)
        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for item in list.blocks {
            let loc = BlockLocation::new(list.worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let exists = match self.block_exists(item.id) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("block_report {:?}: {}", item, e);
                            continue;
                        }
                    };

                    if exists {
                        batch.push((true, item.id, loc));
                    } else {
                        // The block does not exist, and the mark block needs to be deleted.
                        wm.remove_block(list.worker_id, item.id);
                    }
                }

                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(list.worker_id, item.id);
                }
            }
        }
        drop(wm);

        let mut fs_dir = self.fs_dir.write();
        fs_dir.block_report(batch)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        let fs_dir = self.fs_dir.write();
        fs_dir.delete_locations(worker_id)
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        fs_dir.set_attr(inp, opts)
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let target = target.as_ref().to_string();
        let link = Self::resolve_path(&fs_dir, link.as_ref())?;
        fs_dir.symlink(target, link, force, mode)
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let src_path = Self::resolve_path(&fs_dir, src_path.as_ref())?;
        let dst_path = Self::resolve_path(&fs_dir, dst_path.as_ref())?;
        fs_dir.link(src_path, dst_path)
    }
}

impl Default for MasterFilesystem {
    fn default() -> Self {
        let conf = ClusterConf::format();
        let journal_system = JournalSystem::from_conf(&conf).unwrap();
        journal_system.fs()
    }
}
