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
use crate::master::{MountManager, SyncFsDir};
use curvine_common::conf::JournalConf;
use curvine_common::proto::raft::SnapshotData;
use curvine_common::raft::storage::AppStorage;
use curvine_common::raft::{RaftResult, RaftUtils};
use curvine_common::state::RenameFlags;
use curvine_common::utils::SerdeUtils;
use log::{debug, error, info, warn};
use orpc::common::FileUtils;
use orpc::sync::AtomicCounter;
use orpc::{err_box, try_option, try_option_ref, CommonResult};
use std::path::Path;
use std::sync::Arc;
use std::{fs, mem};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    fs_dir: SyncFsDir,
    mnt_mgr: Arc<MountManager>,
    seq_id: Arc<AtomicCounter>,
    retain_checkpoint_num: usize,
    ignore_replay_error: bool,
}

impl JournalLoader {
    pub fn new(fs_dir: SyncFsDir, mnt_mgr: Arc<MountManager>, conf: &JournalConf) -> Self {
        Self {
            fs_dir,
            mnt_mgr,
            seq_id: Arc::new(AtomicCounter::new(0)),
            retain_checkpoint_num: 3.max(conf.retain_checkpoint_num),
            ignore_replay_error: conf.ignore_replay_error,
        }
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

            JournalEntry::ReopenFile(e) => self.reopen_file(e),

            JournalEntry::Mount(e) => self.mount(e),

            JournalEntry::UnMount(e) => self.unmount(e),

            JournalEntry::SetAttr(e) => self.set_attr(e),

            JournalEntry::Symlink(e) => self.symlink(e),
            JournalEntry::Link(e) => self.link(e),
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
            .apply_new_block(inode.as_ref(), entry.commit_block.as_ref())?;

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
        let src_inp = InodePath::resolve(fs_dir.root_ptr(), entry.src, &fs_dir.store)?;
        let dst_inp = InodePath::resolve(fs_dir.root_ptr(), entry.dst, &fs_dir.store)?;
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
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        fs_dir.unprotected_delete(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        self.mnt_mgr.unprotected_add_mount(entry.info.clone())?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_store_mount(entry.info)?;
        Ok(())
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        self.mnt_mgr.unprotected_umount_by_id(entry.id)?;
        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_unmount(entry.id)?;
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let last_inode = try_option!(inp.get_last_inode());
        fs_dir.unprotected_set_attr(last_inode, entry.opts)?;
        Ok(())
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.link, &fs_dir.store)?;
        fs_dir.unprotected_symlink(inp, entry.new_inode, entry.force)?;
        Ok(())
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), entry.src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), entry.dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => return err_box!("Original file not found during link recovery"),
        };

        fs_dir.unprotected_link(new_path, original_inode_id, entry.op_ms)?;
        Ok(())
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

    fn apply0(&self, is_leader: bool, message: &[u8]) -> RaftResult<()> {
        // The raft log has logs that do not contain referenced data.
        if message.is_empty() {
            return Ok(());
        }

        let batch: JournalBatch = SerdeUtils::deserialize(message)?;

        // The leader node ignores all logs because they have been applied to the master node before synchronization via raft.
        if is_leader {
            self.seq_id.set(batch.seq_id + 1);
            return Ok(());
        }

        self.seq_id.incr();
        for entry in batch.batch {
            match self.apply_entry(entry.clone()) {
                Ok(_) => (),
                Err(e) => {
                    return err_box!(
                        "Failed to apply journal entry to master, entry: {:?}: {}",
                        entry,
                        e
                    );
                }
            }
        }

        Ok(())
    }
}

impl AppStorage for JournalLoader {
    fn apply(&self, is_leader: bool, message: &[u8]) -> RaftResult<()> {
        match self.apply0(is_leader, message) {
            Ok(_) => Ok(()),

            Err(e) => {
                if self.ignore_replay_error {
                    error!("journal apply {}", e);
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    // Call rocksdb's API to create a snapshot.
    fn create_snapshot(&self, node_id: u64, last_applied: u64) -> RaftResult<SnapshotData> {
        let fs_dir = self.fs_dir.read();
        let dir = fs_dir.create_checkpoint(last_applied)?;
        let data = RaftUtils::create_file_snapshot(&dir, node_id, last_applied)?;

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
            self.seq_id.set(snapshot.snapshot_id + 1);
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
