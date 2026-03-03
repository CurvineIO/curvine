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

#![allow(clippy::needless_range_loop)]

use crate::master::journal::*;
use crate::master::meta::inode::InodePath;
use crate::master::meta::inode::InodeView::{Dir, File};
use crate::master::{JobManager, MountManager, SyncFsDir};
use curvine_common::conf::JournalConf;
use curvine_common::proto::raft::{FsmState, SnapshotData};
use curvine_common::raft::storage::{ApplyEntry, ApplyMsg, ApplyScan, AppStorage, LogStorage, RocksLogStorage};
use curvine_common::raft::{FsmStateMap, NodeId, RaftError, RaftResult, RaftUtils};
use curvine_common::state::RenameFlags;
use curvine_common::utils::SerdeUtils;
use log::{debug, error, info, warn};
use orpc::common::FileUtils;
use orpc::sync::{AtomicCounter, ErrorMonitor};
use orpc::{err_box, try_option, try_option_ref, CommonResult, CommonError};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{fs, mem};
use std::collections::HashMap;
use std::time::Duration;
use axum::handler::HandlerWithoutStateExt;
use futures::future::{lazy, ok};
use raft::eraftpb::Entry;
use curvine_common::error::FsError;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    fs_dir: SyncFsDir,
    mnt_mgr: Arc<MountManager>,
    ufs_loader: UfsLoader,
    sender: AsyncSender<ApplyMsg>,
    applied: Arc<Mutex<u64>>,
    error_monitor: Arc<ErrorMonitor<RaftError>>,
    retain_checkpoint_num: usize,
    cv_error_retry: bool,
    ufs_error_retry: bool,
    max_retry_num: u64,
    batch_size: u64,
    retry_interval: Duration,
}

impl JournalLoader {
    pub const BATCH_SIZE: u64 = 1000;
    pub const RETRY_INTERVAL: u64 = 1;

    pub fn new(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
    ) -> Self {
        let ufs_loader = UfsLoader::new(job_manager, conf);
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let loader = Self {
            fs_dir,
            mnt_mgr,
            ufs_loader,
            sender,
            applied: Arc::new(Mutex::new(0)),
            error_monitor: Arc::new(ErrorMonitor::new()),
            retain_checkpoint_num: 3.max(conf.retain_checkpoint_num),
            cv_error_retry: conf.cv_error_retry,
            max_retry_num: conf.max_retry_num,
            ufs_error_retry: conf.ufs_error_retry,
            batch_size: conf.scan_batch_size,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
        };

        let loader1 = loader.clone();
        rt.spawn(async move {
           Self::run_apply(loader1, receiver).await;
        });

        loader
    }

    async fn apply_batch(&self, is_leader: bool, data: &[u8]) -> CommonResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let entry: JournalEntry = SerdeUtils::deserialize(data)?;
        if is_leader {
            self.ufs_loader.apply_entry(&entry).await?;
        } else {
            self.apply_entry(entry)?;
        }

        Ok(())
    }

    async fn apply0(&self, msg: &ApplyMsg) -> CommonResult<()> {
        match msg {
            ApplyMsg::Entry(msg) => {
                self.apply_batch(msg.is_leader, &msg.data).await?;
                self.set_applied(msg.index);
                Ok(())
            }

            ApplyMsg::Scan(scan) => {
                let mut last_applied = scan.last_applied;
                loop {
                    let list = scan.get_entries(last_applied + 1, last_applied + self.batch_size)?;

                    if list.is_empty() {
                        return Ok(())
                    };

                    for entry in list {
                        self.apply_batch(scan.is_leader, &entry.data).await?;
                        self.set_applied(entry.index);
                    }
                }
            }
        }
    }

    async fn run_apply(self: Self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut error_apply: Option<ApplyMsg> = None;
        let mut retry_num: u64 = 0;

        loop {
            let msg = match error_apply.take() {
                Some(entry_type) => {
                    tokio::time::sleep(self.retry_interval).await;
                    entry_type
                }

                None => match receiver.recv().await {
                    Some(entry_type) => entry_type,
                    None => break,
                }
            };

            let error = if let Err(e) = self.apply0(&msg).await {
                e
            } else {
                continue;
            };

            retry_num += 1;
            warn!("apply entry retry {} times", retry_num);
            if retry_num >= self.max_retry_num {
                self.error_monitor.set_error(error.to_string().into());
                panic!("apply entry failed: {}", error);
            }

            let can_retry = (msg.is_leader() && self.ufs_error_retry) || (!msg.is_leader() && self.cv_error_retry);
            if can_retry {
                error!("apply entry failed: {}", error);
            } else {
                self.error_monitor.set_error(error.to_string().into());
                panic!("apply entry failed: {}", error);
            }

            match msg {
                ApplyMsg::Entry(entry) => {
                    error_apply.replace(ApplyMsg::Entry(entry));
                }

                ApplyMsg::Scan(scan) => {
                    let err_scan = ApplyMsg::Scan(ApplyScan {
                        is_leader: scan.is_leader,
                        last_applied: self.get_applied(),
                        log_store: scan.log_store,
                    });
                    error_apply.replace(err_scan);
                }
            }
        }
    }

    fn set_applied(&self, applied: u64) {
        *self.applied.lock().unwrap() = applied
    }

    pub fn apply_entry(&self, entry: JournalEntry) -> CommonResult<()> {
        debug!("replay entry: {:?}", entry);
        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e),

            JournalEntry::CreateFile(e) => self.create_file(e),

            JournalEntry::OverWriteFile(e) => self.overwrite_file(e),

            JournalEntry::AddBlock(e) => self.add_block(e),

            JournalEntry::CompleteFile(e) => self.complete_file(e),

            JournalEntry::Rename(e) => self.rename(e),

            JournalEntry::Delete(e) => self.delete(e),

            JournalEntry::Free(e) => self.free(e),

            JournalEntry::ReopenFile(e) => self.reopen_file(e),

            JournalEntry::Mount(e) => self.mount(e),

            JournalEntry::UnMount(e) => self.unmount(e),

            JournalEntry::SetAttr(e) => self.set_attr(e),

            JournalEntry::Symlink(e) => self.symlink(e),

            JournalEntry::Link(e) => self.link(e),

            JournalEntry::SetLocks(e) => self.set_locks(e),
        }
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.update_last_inode_id(entry.dir.id)?;
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, Dir(name, entry.dir))?;
        Ok(())
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.update_last_inode_id(entry.file.id)?;
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, File(name, entry.file))?;
        Ok(())
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;

        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_reopen_file(inode.as_ref())?;

        Ok(())
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;

        // For journal replay, we directly update the file with the entry's file data
        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_overwrite_file(inode.as_ref())?;

        Ok(())
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;

        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;
        let _ = mem::replace(&mut file.blocks, entry.blocks);
        fs_dir
            .store
            .apply_new_block(inode.as_ref(), &entry.commit_block)?;

        Ok(())
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;

        let mut inode = try_option!(inp.get_last_inode());
        let file = inode.as_file_mut()?;

        let _ = mem::replace(file, entry.file);
        // Update block location
        fs_dir
            .store
            .apply_complete_file(inode.as_ref(), &entry.commit_blocks)?;

        Ok(())
    }
    pub fn rename(&self, entry: RenameEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_src = entry.src;
        let src_inp = InodePath::resolve(fs_dir.root_ptr(), &entry_src, &fs_dir.store)?;
        let dst_inp = InodePath::resolve(fs_dir.root_ptr(), entry.dst, &fs_dir.store)?;
        if src_inp.get_last_inode().is_none() {
            warn!("Rename: source path not found: {}", entry_src);
            return Ok(());
        }
        fs_dir.unprotected_rename(
            &src_inp,
            &dst_inp,
            entry.mtime,
            RenameFlags::new(entry.flags),
        )?;

        Ok(())
    }

    pub fn delete(&self, entry: DeleteEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        if inp.get_last_inode().is_none() {
            warn!("Delete: path not found: {}", entry_path);
            return Ok(());
        }
        fs_dir.unprotected_delete(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn free(&self, entry: FreeEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        if inp.get_last_inode().is_none() {
            warn!("Free: path not found: {}", entry_path);
            return Ok(());
        }
        fs_dir.unprotected_free(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        self.mnt_mgr.unprotected_add_mount(entry.info.clone())?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_store_mount(entry.info)?;
        Ok(())
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        if !self.mnt_mgr.has_mounted(entry.id) {
            warn!("Unmount: id already unmounted: {}", entry.id);
            return Ok(());
        }
        self.mnt_mgr.unprotected_umount_by_id(entry.id)?;
        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_unmount(entry.id)?;
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        let last_inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("SetAttr: path not found: {}", entry_path);
                return Ok(());
            }
        };

        fs_dir.unprotected_set_attr(last_inode, entry.opts)?;
        Ok(())
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let link_path = entry.link;
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &link_path, &fs_dir.store)?;
        match fs_dir.unprotected_symlink(inp, entry.new_inode, entry.force) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Symlink: file already exists: {}", link_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let src_path = entry.src_path;
        let dst_path = entry.dst_path;
        let mut fs_dir = self.fs_dir.write();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), &src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), &dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => {
                warn!("Link: source path not found: {}", src_path);
                return Ok(());
            }
        };

        match fs_dir.unprotected_link(new_path, original_inode_id, entry.op_ms) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {}", dst_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_set_locks(entry.ino, &entry.locks)
    }

    // Clean up expired checkpoints.
    pub fn purge_checkpoint(&self, current_ck: impl AsRef<str>) -> CommonResult<()> {
        let ck_dir = match Path::new(current_ck.as_ref()).parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        let mut vec = vec![];
        for entry in fs::read_dir(ck_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            vec.push((FileUtils::mtime(&meta)?, entry.path()));
        }

        // Sort by modification time
        vec.sort_by_key(|x| x.0);
        let del_num = vec.len().saturating_sub(self.retain_checkpoint_num);

        for i in 0..del_num {
            let path = vec[i].1.as_path();
            FileUtils::delete_path(path, true)?;
            info!("delete expired checkpoint, dir: {}", path.to_string_lossy())
        }

        Ok(())
    }
}

impl AppStorage for JournalLoader {
    async fn apply(&self, wait_for_apply: bool, msg: ApplyMsg) -> RaftResult<()> {
        if wait_for_apply {
            self.apply0(&msg).await?;
        } else {
            self.error_monitor.check_error()?;
            self.sender.send(msg).await?;
        }
        Ok(())
    }

    fn get_applied(&self) -> u64 {
        *self.applied.lock().unwrap()
    }

    // Call rocksdb's API to create a snapshot.
    fn create_snapshot(&self, node_id: u64, last_applied: u64, fsm_state_map: FsmStateMap) -> RaftResult<SnapshotData> {
        let fs_dir = self.fs_dir.read();
        let dir = fs_dir.create_checkpoint(last_applied)?;

        let mut data = RaftUtils::create_file_snapshot(&dir, node_id, last_applied)?;
        fsm_state_map.set_snapshot(&mut data);

        // Delete historical snapshots.
        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }
        Ok(data)
    }

    fn apply_snapshot(&self, snapshot: &SnapshotData) -> RaftResult<()> {
        {
            let mut fs_dir = self.fs_dir.write();
            let data = try_option_ref!(snapshot.files_data);
            fs_dir.restore(&data.dir)?;
        }
        {
            self.mnt_mgr.restore();
        }
        Ok(())
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self.fs_dir.read();
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}
