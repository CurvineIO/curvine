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
use crate::master::{JobManager, Master, MasterMetrics, MountManager, SyncFsDir};
use curvine_common::conf::JournalConf;
use curvine_common::error::FsError;
use curvine_common::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_common::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_common::raft::{RaftClient, RaftResult, RaftUtils};
use curvine_common::state::RenameFlags;
use curvine_common::utils::SerdeUtils;
use log::{debug, error, info, warn};
use orpc::common::{FileUtils, LocalTime};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel};
use orpc::{err_box, CommonResult};
use raft::eraftpb::Entry;
use raft::StateRole;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, mem};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    node_id: u64,
    fs_dir: SyncFsDir,
    mnt_mgr: Arc<MountManager>,
    journal_writer: Arc<JournalWriter>,
    ufs_loader: UfsLoader,
    log_store: RocksLogStorage,
    sender: AsyncSender<ApplyMsg>,
    fsm_state: Arc<Mutex<FsmState>>,
    retain_checkpoint_num: usize,
    ignore_reply_error: bool,
    max_retry_num: u64,
    batch_size: u64,
    retry_interval: Duration,
    metrics: &'static MasterMetrics,
}

impl JournalLoader {
    pub fn new_replay_loader(
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
    ) -> Self {
        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), conf);
        let journal_writer = Arc::new(JournalWriter::new(true, client, conf));
        let log_store = RocksLogStorage::from_conf(conf, false);
        Self::new(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        testing: bool,
    ) -> Self {
        let ufs_loader = UfsLoader::new(job_manager, conf);
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let loader = Self {
            node_id: conf.node_id().unwrap(),
            fs_dir,
            mnt_mgr,
            journal_writer,
            ufs_loader,
            log_store,
            sender,
            fsm_state: Arc::new(Mutex::new(FsmState::default())),
            retain_checkpoint_num: 3.max(conf.retain_checkpoint_num),
            ignore_reply_error: conf.ignore_reply_error,
            max_retry_num: conf.max_retry_num,
            batch_size: conf.scan_batch_size,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
            metrics: Master::get_metrics(),
        };

        if !testing {
            let loader1 = loader.clone();
            rt.spawn(async move {
                Self::run_apply(loader1, receiver).await;
            });
        }

        loader
    }

    fn get_ufs_applied(&self) -> AppliedIndex {
        self.fsm_state.lock().unwrap().ufs_applied.clone()
    }

    fn set_applied(
        &self,
        is_leader: bool,
        applied: AppliedIndex,
        has_ufs_affecting: bool,
    ) -> CommonResult<()> {
        if is_leader && has_ufs_affecting {
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)?;
        }

        let mut state = self.fsm_state.lock().unwrap();
        if is_leader {
            state.ufs_applied = applied.clone();
            state.applied = applied;
        } else {
            state.applied = applied;
        }

        self.metrics.journal_applied.set(state.applied.index as i64);
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
        drop(state);

        let state = self.log_store.hard_state();
        self.metrics.journal_committed.set(state.commit as i64);
        self.metrics.journal_term.set(state.term as i64);

        Ok(())
    }

    fn build_applied(entry: &Entry) -> AppliedIndex {
        AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        }
    }

    async fn apply0(&self, is_leader: bool, entry: &Entry) -> CommonResult<()> {
        if entry.data.is_empty() {
            return Ok(());
        }

        let cur = self.get_fsm_state();
        if entry.index <= cur.applied.index {
            info!(
                "skip entry index {}, term {}, fsm_state {:?}",
                entry.index, entry.term, cur
            );
            return Ok(());
        }

        let batch: JournalBatch = SerdeUtils::deserialize(&entry.data)?;
        let batch_len = batch.len();
        let mut snapshot = None;
        let mut applied = Self::build_applied(entry);
        let mut has_ufs_affecting = false;

        for (seq, op_entry) in batch.batch.into_iter().enumerate() {
            applied.op_id = op_entry.op_id();
            applied.rpc_id = op_entry.rpc_id();

            match op_entry {
                JournalEntry::Snapshot(e) if is_leader && e.node_id == self.node_id => {
                    if seq + 1 != batch_len {
                        return err_box!("snapshot should be the last entry");
                    }
                    snapshot.replace(e);
                    continue;
                }

                JournalEntry::UfsApplied(_) => (),

                _ => has_ufs_affecting = true,
            }

            {
                let fs_dir = self.fs_dir.read();
                fs_dir.update_op_id(op_entry.op_id());
                if let Some(inode_id) = op_entry.inode_id() {
                    fs_dir.update_last_inode_id(inode_id)?;
                }
            }

            let res = if is_leader {
                self.ufs_loader.apply_entry(&op_entry).await
            } else {
                self.apply_entry(op_entry.clone())
            };

            if let Err(e) = res {
                return err_box!("failed to apply journal: {:?}: {}", op_entry, e);
            }
        }

        self.set_applied(is_leader, applied, has_ufs_affecting)?;

        if let Some(e) = snapshot {
            let snap_data = self.create_snapshot0(Some(e.dir.to_string()))?;

            self.log_store.create_snapshot(snap_data.clone())?;
            self.log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create leader snapshot, dir={}, fsm_state={:?}",
                e.dir, snap_data.fsm_state
            );
        }

        Ok(())
    }

    async fn apply_msg(&self, is_leader: bool, msg: &ApplyMsg) -> CommonResult<()> {
        match msg {
            ApplyMsg::Entry(entry) => {
                self.apply0(is_leader, entry).await?;
                Ok(())
            }

            ApplyMsg::Scan(applied_index) => {
                let mut last_applied = applied_index.index;
                let commit_index = self.log_store.hard_state().commit;
                loop {
                    if last_applied >= commit_index {
                        return Ok(());
                    }

                    let high = (last_applied + self.batch_size).min(commit_index + 1);
                    let list = self.log_store.scan_entries(last_applied + 1, high)?;

                    if list.is_empty() {
                        return Ok(());
                    };

                    info!(
                        "replay-scan, start_index: {}, entries: {}, commit_index: {}",
                        last_applied + 1,
                        list.len(),
                        commit_index
                    );

                    for entry in list {
                        self.apply0(is_leader, &entry).await?;
                        last_applied = entry.index;
                    }
                }
            }

            _ => unreachable!(),
        }
    }

    async fn next_apply_msg(
        &self,
        receiver: &mut AsyncReceiver<ApplyMsg>,
        retry_msg: &mut Option<ApplyMsg>,
    ) -> Option<ApplyMsg> {
        match retry_msg.take() {
            Some(msg) => {
                tokio::time::sleep(self.retry_interval).await;
                Some(msg)
            }
            None => receiver.recv().await,
        }
    }

    async fn run_apply(self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut retry_msg: Option<ApplyMsg> = None;
        let mut retry_num: u64 = 0;
        let mut is_leader = false;

        loop {
            let apply_msg = match self.next_apply_msg(&mut receiver, &mut retry_msg).await {
                Some(v) => v,
                None => break,
            };

            match apply_msg {
                ApplyMsg::CreateSnapshot(tx) => {
                    if let Err(e) = tx.send(self.create_snapshot0(None)) {
                        warn!("send create snapshot result failed: {}", e);
                    }
                    retry_num = 0;
                }

                ApplyMsg::ApplySnapshot((tx, snapshot)) => {
                    if let Err(e) = tx.send(self.apply_snapshot0(snapshot)) {
                        warn!("send apply snapshot result failed: {}", e);
                    }
                    retry_num = 0;
                }

                ApplyMsg::RoleChange(role) => {
                    is_leader = role == StateRole::Leader;
                    if is_leader {
                        let ufs_applied = self.get_ufs_applied();
                        info!("role changed to leader, scheduling UFS replay scan from ufs_applied: {:?}", ufs_applied);
                        retry_msg.replace(ApplyMsg::new_scan(ufs_applied));
                    }
                }

                msg => match self.apply_msg(is_leader, &msg).await {
                    Ok(_) => retry_num = 0,

                    Err(error) => {
                        if self.ignore_reply_error {
                            error!("apply entry failed(skip): {}", error);
                        } else if is_leader {
                            retry_num += 1;

                            if retry_num >= self.max_retry_num {
                                panic!("apply entry failed(retry_num={}): {}", retry_num, error);
                            } else {
                                error!("apply entry failed(retry_num={}): {}", retry_num, error);
                            }

                            retry_msg.replace(msg);
                        } else {
                            panic!("apply entry failed: {}", error);
                        }
                    }
                },
            }
        }
    }

    fn create_snapshot0(&self, dir_option: Option<String>) -> RaftResult<SnapshotData> {
        let fsm_state = self.get_fsm_state();
        let fs_dir = self.fs_dir.read();
        let dir = match dir_option {
            Some(dir) => dir,
            None => fs_dir.create_checkpoint(fsm_state.applied.index)?,
        };

        let data = RaftUtils::create_file_snapshot(&dir, self.node_id, fsm_state)?;

        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }

        Ok(data)
    }

    fn apply_snapshot0(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let mut fs_dir = self.fs_dir.write();
        match snapshot.files_data {
            None => {
                let dir = fs_dir.get_checkpoint_path(LocalTime::mills());
                FileUtils::create_dir(&dir, true)?;
                fs_dir.restore(dir)?;
                fs_dir.update_op_id(snapshot.fsm_state.op_id());
            }

            Some(data) => {
                fs_dir.restore(&data.dir)?;
                fs_dir.update_op_id(snapshot.fsm_state.op_id());
            }
        }
        drop(fs_dir);

        self.mnt_mgr.restore();

        *self.fsm_state.lock().unwrap() = snapshot.fsm_state;

        Ok(())
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

            JournalEntry::UfsApplied(e) => self.ufs_applied(e),

            _ => Ok(()),
        }
    }

    pub fn replay_entry(&self, entry: JournalEntry) -> CommonResult<()> {
        {
            let fs_dir = self.fs_dir.read();
            fs_dir.update_op_id(entry.op_id());
            if let Some(inode_id) = entry.inode_id() {
                fs_dir.update_last_inode_id(inode_id)?;
            }
        }

        self.apply_entry(entry)
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, Dir(name, entry.dir))?;
        Ok(())
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        if inp.is_full() {
            warn!("create_file: file already exists: {:?}", entry);
            return Ok(());
        }
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, File(name, entry.file))?;
        Ok(())
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("reopen_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_reopen_file(inode.as_ref())?;

        Ok(())
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("overwrite_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_overwrite_file(inode.as_ref())?;

        Ok(())
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("add_block: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(&mut file.blocks, entry.blocks);
        fs_dir
            .store
            .apply_new_block(inode.as_ref(), &entry.commit_block)?;

        Ok(())
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("complete_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
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
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let Some(inode) = inp.get_last_inode() else {
            warn!("Free: path not found: {:?}", entry);
            return Ok(());
        };
        fs_dir.unprotected_free(inode, entry.mtime, entry.recursive)?;
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
            warn!("Unmount: id already unmounted: {:?}", entry);
            return Ok(());
        }
        self.mnt_mgr.unprotected_umount_by_id(entry.id)?;
        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_unmount(entry.id)?;
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let last_inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("SetAttr: path not found: {:?}", entry);
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
                warn!("Symlink: file already exists: {:?}", link_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), &entry.src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), &entry.dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => {
                warn!("Link: source path not found: {:?}", entry);
                return Ok(());
            }
        };

        match fs_dir.unprotected_link(new_path, original_inode_id, entry.mtime as u64) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {:?}", entry);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_set_locks(entry.ino, &entry.locks)
    }

    pub fn ufs_applied(&self, entry: UfsAppliedEntry) -> CommonResult<()> {
        let mut lock = self.fsm_state.lock().unwrap();
        lock.ufs_applied = AppliedIndex {
            op_id: entry.op_id,
            rpc_id: entry.rpc_id,
            term: entry.term,
            index: entry.index,
        };
        Ok(())
    }

    // Clean up expired checkpoints.
    pub fn purge_checkpoint(&self, current_ck: impl AsRef<str>) -> CommonResult<()> {
        let current_ck = current_ck.as_ref();
        let ck_dir = match Path::new(current_ck).parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        let current_mtime = match Path::new(current_ck).metadata() {
            Ok(meta) => FileUtils::mtime(&meta)?,
            Err(_) => return Ok(()),
        };

        let mut vec = vec![];
        for entry in fs::read_dir(ck_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let mtime = FileUtils::mtime(&meta)?;
            if mtime < current_mtime {
                vec.push((mtime, entry.path()));
            }
        }

        // Sort oldest-first and keep at most (retain_checkpoint_num - 1) older
        // checkpoints so that together with current_ck the total is retain_checkpoint_num.
        vec.sort_by_key(|x| x.0);
        let keep = self.retain_checkpoint_num.saturating_sub(1);
        let del_num = vec.len().saturating_sub(keep);

        for i in 0..del_num {
            let path = vec[i].1.as_path();
            FileUtils::delete_path(path, true)?;
            info!("delete expired checkpoint: {}", path.to_string_lossy());
        }

        Ok(())
    }
}

impl AppStorage for JournalLoader {
    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if wait {
            if let Err(e) = self.apply_msg(false, &msg).await {
                if self.ignore_reply_error {
                    error!("apply entry failed: {}", e);
                    Ok(())
                } else {
                    Err(e.into())
                }
            } else {
                Ok(())
            }
        } else {
            self.sender.send(msg).await?;
            Ok(())
        }
    }

    fn get_fsm_state(&self) -> FsmState {
        self.fsm_state.lock().unwrap().clone()
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        self.sender.send(ApplyMsg::RoleChange(role)).await?;
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::CreateSnapshot(tx);

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::ApplySnapshot((tx, snapshot));

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self.fs_dir.read();
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}
