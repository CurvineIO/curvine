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
use crate::master::meta::inode::{
    InodeContainer, InodeFile, InodePath, InodeView, SmallFileMeta, PATH_SEPARATOR,
};
use crate::master::meta::FsDir;

use crate::master::meta::parse_glob_pattern;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_common::conf::{ClusterConf, MasterConf};
use curvine_common::error::FsError;
use curvine_common::state::*;
use curvine_common::FsResult;
use log::warn;
use orpc::sync::ArcRwLock;
use orpc::{err_box, err_ext, try_option, CommonResult};
use std::collections::HashMap;
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

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        // Creation of root directory is not allowed
        if inp.is_root() {
            return err_box!("Not allowed to create existing root path: {}", inp.path());
        }

        if inp.is_full() {
            if opts.create_parent {
                if let Some(last_inode) = inp.get_last_inode() {
                    if last_inode.is_dir() {
                        let status = last_inode.to_file_status(inp.path());
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
        let last = try_option!(inp.get_last_inode());
        let status = last.to_file_status(inp.path());
        Ok(status)
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<bool> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        if !inp.is_empty_dir() && !recursive {
            return err_ext!(FsError::dir_not_empty(inp.path()));
        }

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_box!("Failed to remove {} because it does not exist", inp.path());
        }

        let delete_result = fs_dir.delete(&inp, recursive)?;

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&delete_result);

        Ok(true)
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
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
        if let Some(rest) = dst.strip_prefix(src) {
            if rest.starts_with(PATH_SEPARATOR) {
                return err_box!(
                    "Rename dst {} is a directory or file under src {}",
                    dst,
                    src
                );
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

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if !flags.create() {
            return err_box!("Flag error {}, cannot create file", flags.value());
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
        println!("DEBUG at MasterFileSystem, last_inode {:?}", last_inode);
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }
        }

        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        // If the file already exists and overwrite is enabled, overwrite the file
        let inp = if last_inode.is_some() && flags.overwrite() {
            let clean_result = fs_dir.overwrite_file(&inp, opts)?;
            if !clean_result.blocks.is_empty() {
                let mut worker_manager = self.worker_manager.write();
                worker_manager.remove_blocks(&clean_result);
            }
            inp
        } else {
            // If the file does not exist, create a new file
            if opts.file_type == FileType::File {
                fs_dir.create_file(inp, opts)?
            } else {
                fs_dir.create_container(inp, opts)?
            }
        };

        println!(
            "DEBUG at Master File System, at create_with_opts, inp= {:?}",
            inp
        );
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
            return self.get_block_locations(path);
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            None => {
                return if flags.create() || flags.append() {
                    drop(fs_dir);
                    let status = self.create_with_opts(path, opts, flags)?;
                    Ok(FileBlocks::new(status, vec![]))
                } else {
                    err_ext!(FsError::file_not_found(inp.path()))
                }
            }
            Some(inode) => inode,
        };

        let status = if flags.write() {
            fs_dir.reopen_file(&inp, opts.client_name)?
        } else {
            fs_dir.file_status(&inp)?
        };

        let file = inode.as_file_ref()?;
        let blocks = if !file.blocks.is_empty() {
            self.get_block_locs(path, &fs_dir, file)?
        } else {
            vec![]
        };
        Ok(FileBlocks::new(status, blocks))
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        println!(
            "DEBUG at Master File System, at file_status, inp= {:?}",
            inp
        );

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

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
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
            if v.block_len != file.block_size {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas,
            block_size: file.block_size,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn validate_add_block_for_container(
        container: &InodeContainer,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        // For containers, previous should typically be None since they have single blocks
        if let Some(v) = previous {
            if v.block_len != container.total_size {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    container.total_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: container.replicas,
            block_size: container.max_file_size,
            storage_policy: container.storage_policy.clone(),
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
        let wm = self.worker_manager.read();

        let inode = try_option!(inp.get_last_inode());
        // let file = inode.as_file_mut()?;

        // // Validate differently for containers
        // let validate_block = if file.is_container {
        //     Self::validate_add_block_for_container(file, &client_addr)?
        // } else {
        //     Self::validate_add_block(file, &client_addr, None)?
        // };

        let validate_block = match inode.as_ref() {
            InodeView::File(_, file) => Self::validate_add_block(file, &client_addr, None)?,
            InodeView::Container(_, container) => {
                Self::validate_add_block_for_container(container, &client_addr, None)?
            }
            _ => return err_box!("Path must be a file or container"),
        };

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

    /// Document application to allocate a new block.
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        println!("DEBUG at MasterFilesystem, at add_blocks, start with commit_blocks: {:?}, file_len: {:?}, last_block {:?}", commit_blocks, file_len, last_block);
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("File {} not exists", inp.path()),
        };

        println!(
            "DEBUG at MasterFilesystem, at add_blocks, inode: {:?}",
            inode
        );

        let result_located_block: LocatedBlock = match inode.as_ref() {
            InodeView::Container(_, _) => {
                let container = inode.as_container_ref()?;
                println!(
                    "DEBUG at MasterFileSystem, at add_block, container id {:?} and container {:?}",
                    container.id, container
                );
                // just one block in container:
                // let locs = fs_dir.get_block_locations(0)?;
                // let extend_block = ExtendedBlock {
                //     id: 0,
                //     len: 0,
                //     storage_type: StorageType::default(),
                //     file_type: FileType::Container,
                //     alloc_opts: Some(FileAllocOpts::with_truncate(0)),
                // };

                // let existing_block: &crate::master::meta::BlockMeta = &container.blocks[0];
                // let locs = fs_dir.get_block_locations(existing_block.id)?;
                // println!("at MasterFileSystem, at ad_block, match container, existing locs: {:?}", locs);

                // return self.create_locate_block(path, extend_block, &locs);

                let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
                let block =
                    fs_dir.acquire_new_block(&inp, commit_blocks, &choose_workers, file_len)?;
                let located = LocatedBlock {
                    block,
                    locs: choose_workers,
                };

                println!(
                    "DEBUG at MasterFileSystem, at add_block, end the add block for container {:?}, with locs {:?}",
                    container.id, located
                );
                located
                // Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            InodeView::File(_, _) => {
                let file = inode.as_file_ref()?;
                if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
                    let locs = fs_dir.get_block_locations(next.id)?;
                    let extend_block = ExtendedBlock {
                    id: next.id,
                        len: next.len,
                        storage_type: file.storage_policy.storage_type,
                        file_type: file.file_type,
                        alloc_opts: next.alloc_opts.clone(),
                    };

                    // return self.create_locate_block(path, extend_block, &locs);
                }
                let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
                let block =
                    fs_dir.acquire_new_block(&inp, commit_blocks, &choose_workers, file_len)?;
                let located = LocatedBlock {
                    block,
                    locs: choose_workers,
                };

                located
                // Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            _ => {
                let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
                let block =
                    fs_dir.acquire_new_block(&inp, commit_blocks, &choose_workers, file_len)?;
                let located = LocatedBlock {
                    block,
                    locs: choose_workers,
                };

                located
            } // Err::<(), Box<dyn std::error::Error + Send + Sync>>(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Path is not a file"))  ),
        };

        Ok(result_located_block)

        // println!("DEBUG at Master FileSystem, at add_block, continue");
        // let file = inode.as_file_ref()?;

        // File allows concurrent writes, 'previous' is the previous block,
        // need to check if the next block has already been allocated。
        // If it has been allocated, return that block
        // if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
        //     let locs = fs_dir.get_block_locations(next.id)?;
        //     let extend_block = ExtendedBlock {
        //         id: next.id,
        //         len: next.len,
        //         storage_type: file.storage_policy.storage_type,
        //         file_type: file.file_type,
        //         alloc_opts: next.alloc_opts.clone(),
        //     };

        //     return self.create_locate_block(path, extend_block, &locs);
        // }

        // let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
        // let block = fs_dir.acquire_new_block(&inp, commit_blocks, &choose_workers, file_len)?;
        // let located = LocatedBlock {
        //     block,
        //     locs: choose_workers,
        // };

        // Ok(located)
    }

    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            None => return err_box!("File does not exist: {}", inp.path()),
            Some(v) => v,
        };
        let file = inode.as_file_ref()?;
        fs_dir.complete_file(&inp, len, commit_blocks, client_name, only_flush)?;
        if only_flush {
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            let status = inode.to_file_status(path);
            Ok(Some(FileBlocks::new(status, locs)))
        } else {
            Ok(None)
        }
    }

    pub fn complete_container<T: AsRef<str>>(
        &self,
        path: T,
        len: i64,
        commit_block: CommitBlock,
        client_name: T,
        only_flush: bool,
        files: HashMap<String, SmallFileMeta>,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        println!("DEBUG at MasterFileSystem, at complete_container, inp: {:?}", inp);
        let inode = match inp.get_last_inode() {
            None => {
                println!("DEBUG at MasterFileSystem, at complete_container, inode: is None");
                return err_box!("File does not exist: {}", inp.path())
            },
            Some(v) => v,
        };

        println!("DEBUG at MasterFileSystem, at complete_container, inode: {:?}", inode);
        let file = inode.as_container_ref()?;
        println!("DEBUG at MasterFileSystem, at complete_container, file: {:?}", file);
        fs_dir.complete_container(&inp, len, commit_block, client_name, only_flush, files)?;

        Ok(None)
        // if only_flush {
        //     let locs = self.get_block_locs(path, &fs_dir, file)?;
        //     let status = inode.to_file_status(path);
        //     Ok(Some(FileBlocks::new(status, locs)))
        // } else {
        //     Ok(None)
        // }
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
        Ok(FileBlocks::new(inode.to_file_status(path), blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
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

    fn get_container_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        container: &InodeContainer,
    ) -> FsResult<Vec<LocatedBlock>> {
        let wm = self.worker_manager.read();

        println!("DEBUG at MasterFileSystem, at get_container_block_locs, container {:?}", container);
        // Containers should have exactly one block
        let block = match container.blocks.first() {
            Some(b) => b,
            None => return Ok(vec![]),
        };
        println!("DEBUG at MasterFileSystem, at get_container_block_locs, block {:?}", block);
        let locs = fs_dir.get_block_locations(block.id)?;
        println!("DEBUG at MasterFileSystem, at get_container_block_locs, locs {:?}", locs);
        let extend_block = ExtendedBlock {
            id: block.id,
            len: block.len,
            storage_type: container.storage_policy.storage_type,
            file_type: FileType::Container,
            alloc_opts: block.alloc_opts.clone(),
        };

        let lb = wm.create_locate_block(path, extend_block, &locs)?;
        Ok(vec![lb])
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        println!(
            "DEBUG at MasterFileSystem, at get_block_locations, start with path: {:?}",
            path.as_ref()
        );
        let fs_dir = self.fs_dir.read();
        let path = path.as_ref();
        let inp = Self::resolve_path(&fs_dir, path)?;
        println!(
            "DEBUG at MasterFileSystem,atget_block_locations, inp: {:?}",
            inp
        );
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        println!(
            "DEBUG at MasterFileSystem,atget_block_locations, inode: {:?}",
            inode
        );
        let block_locs = match inode.as_ref() {
            InodeView::File(_, file) => self.get_block_locs(path, &fs_dir, file)?,
            InodeView::Container(_, container) => {
                self.get_container_block_locs(path, &fs_dir, container)?
            }
            _ => return err_box!("Path must be a file or container"),
        };
        // let file = inode.as_file_ref()?;
        // let block_locs = self.get_block_locs(path, &fs_dir, file)?;
        println!("DEBUG at MasterFileSystem,atget_block_locations, inode: {:?}", inode);
        // let locate_blocks = FileBlocks {
        //     status: inode.to_file_status(path),
        //     block_locs,
        // };


        let locate_blocks: FileBlocks = match inode.as_ref() {
            InodeView::File(_, file) => {
                FileBlocks {
                    status: inode.to_file_status(path),
                    block_locs: block_locs.clone(),
                }
            },
            InodeView::Container(_, container) => {
                FileBlocks {
                    status: inode.to_file_status(path),
                    block_locs: block_locs,
                }
            }
            _ => return err_box!("Path must be a file or container"),
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
                            warn!("block_report {item:?}: {e}");
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

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
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

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        opts.validate()?;

        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let del_res = fs_dir.resize(&inp, opts)?;
        self.worker_manager.write().remove_blocks(&del_res);

        self.get_file_blocks(path, &fs_dir, &inp)
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
        let block = fs_dir.assign_worker(inp, block.id, &choose_workers)?;

        Ok(LocatedBlock {
            block,
            locs: choose_workers,
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

impl Default for MasterFilesystem {
    fn default() -> Self {
        let conf = ClusterConf::format();
        let journal_system = JournalSystem::from_conf(&conf)
            .expect("Failed to initialize JournalSystem from default ClusterConf");
        journal_system.fs()
    }
}
