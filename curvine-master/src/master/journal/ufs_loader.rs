//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::master::journal::{
    CompleteFileEntry, DeleteEntry, JournalEntry, MkdirEntry, RenameEntry,
};
use crate::master::meta::inode::InodeView;
use crate::master::{FsModeExport, JobManager, SyncFsDir};
use curvine_config::JournalConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{JobSourceType, JobTaskState, LoadJobCommand, StorageState};
use curvine_runtime::common::DurationUnit;
use curvine_unified_fs::{MountValue, UfsFileSystem};
use log::{debug, info, warn};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct UfsLoader {
    job_manager: Arc<JobManager>,
    fs_dir: SyncFsDir,
    copy_timeout: Duration,
}

impl UfsLoader {
    pub fn new(job_manager: Arc<JobManager>, fs_dir: SyncFsDir, conf: &JournalConf) -> Self {
        let copy_timeout = match DurationUnit::from_str(&conf.ufs_copy_timeout) {
            Ok(unit) => unit.as_duration(),
            Err(e) => {
                warn!(
                    "Invalid ufs_copy_timeout value '{}': {}. Falling back to default 20min",
                    conf.ufs_copy_timeout, e
                );
                Duration::from_secs(20 * 60)
            }
        };

        Self {
            job_manager,
            fs_dir,
            copy_timeout,
        }
    }

    pub fn get_real_path(&self, inode_id: i64) -> FsResult<Path> {
        let path = self.fs_dir.read().get_inode_path(inode_id)?;
        Ok(Path::from_str(path)?)
    }

    fn get_mnt(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        match self.job_manager.get_mnt(path)? {
            Some((path, mnt)) if mnt.info.is_fs_mode() => Ok(Some((path, mnt))),

            _ => Ok(None),
        }
    }

    async fn mkdir_parent(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        if let Some(parent) = mnt.info.get_ufs_path(path)?.parent()? {
            mnt.ufs()?.mkdir(&parent, true).await?;
        }
        Ok(())
    }

    async fn mkdir_with_parent_retry(&self, path: &Path, mnt: &MountValue) -> FsResult<bool> {
        let ufs_path = mnt.info.get_ufs_path(path)?;
        match mnt.ufs()?.mkdir(&ufs_path, false).await {
            Ok(created) => Ok(created),
            Err(FsError::FileNotFound(_)) => {
                self.mkdir_parent(path, mnt).await?;
                mnt.ufs()?.mkdir(&ufs_path, false).await
            }
            Err(e) => Err(e),
        }
    }

    pub async fn submit_load_task(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        let command = LoadJobCommand::builder(path.clone_uri())
            .source_type(JobSourceType::FsModeAuto)
            .build();
        let runner = self.job_manager.create_runner();
        let res = match runner.submit_load_task(command, mnt.info.clone()).await {
            Ok(res) => res,
            Err(e) => {
                return if matches!(e, FsError::FileNotFound(_)) {
                    info!("file {} not found, skipping load job", path.full_path());
                    Ok(())
                } else {
                    err_box!("load job failed: {}", e)
                };
            }
        };
        self.job_manager.record_submitted();

        if matches!(res.state, JobTaskState::Completed) {
            return Ok(());
        }

        let status = self
            .job_manager
            .wait_job_complete(res.job_id, self.copy_timeout)
            .await?;
        if matches!(status.state, JobTaskState::Completed) {
            Ok(())
        } else {
            err_box!(
                "load job failed: {} {}",
                status.state,
                status.progress.message
            )
        }
    }

    pub async fn submit_export_task(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        let command = LoadJobCommand::builder(path.clone_uri()).build();
        let runner = self.job_manager.create_runner();
        let res = match runner.submit_export_task(command, mnt.info.clone()).await {
            Ok(res) => res,
            Err(e) => {
                return if matches!(e, FsError::FileNotFound(_)) {
                    info!("file {} not found, skipping export job", path.full_path());
                    Ok(())
                } else {
                    err_box!("export job failed: {}", e)
                };
            }
        };
        self.job_manager.record_submitted();

        if matches!(res.state, JobTaskState::Completed) {
            return Ok(());
        }

        let status = self
            .job_manager
            .wait_job_complete(res.job_id, self.copy_timeout)
            .await?;
        if matches!(status.state, JobTaskState::Completed) {
            Ok(())
        } else {
            err_box!(
                "export job failed: {} {}",
                status.state,
                status.progress.message
            )
        }
    }

    pub async fn apply_entry(&self, entry: &JournalEntry) -> CommonResult<()> {
        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e).await,
            JournalEntry::CompleteFile(e) => self.complete_file(e).await,
            JournalEntry::Rename(e) => self.rename(e).await,
            JournalEntry::Delete(e) => self.delete(e).await,
            _ => Ok(()),
        }
    }

    pub async fn mkdir(&self, e: &MkdirEntry) -> CommonResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((_, mnt)) = self.get_mnt(&path)? {
            self.mkdir_with_parent_retry(&path, &mnt).await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// True when exports go through the Transfer service: the replayer then
    /// only submits and never waits for a copy.
    pub fn transfer_enabled(&self) -> bool {
        self.job_manager.transfer_enabled()
    }

    /// Whether replaying `entry` needs a live Curvine worker. Transfer mode never
    /// does: the replayer only enqueues, and Transfer schedules the copy.
    pub fn needs_worker(&self, entry: &JournalEntry) -> bool {
        !self.transfer_enabled() && entry.needs_worker_for_ufs()
    }

    /// Export described by a replayed `CompleteFile`, in transfer mode.
    ///
    /// The inode is re-read by id: the journal path can be stale after a rename,
    /// and a file that changed since (rewritten, deleted, already marked) has a
    /// newer `CompleteFile` or needs nothing. The copy then targets the file's
    /// current path and carries the generation the journal recorded.
    pub fn fs_mode_export(&self, e: &CompleteFileEntry) -> FsResult<Option<FsModeExport>> {
        if !e.file.is_complete() || e.file.ufs_only() {
            return Ok(None);
        }
        let gen = e.file.gen();
        let path = {
            let fs_dir = self.fs_dir.read();
            let Some(InodeView::File(file)) = fs_dir.store.get_inode(e.file.id, None)? else {
                return Ok(None);
            };
            if file.gen() != gen
                || file.storage_policy.state != StorageState::Cv
                || !file.is_complete()
                || file.is_writing()
            {
                return Ok(None);
            }
            match fs_dir.get_inode_path(e.file.id) {
                Ok(path) => path,
                Err(FsError::FileNotFound(_)) => return Ok(None),
                Err(err) => return Err(err),
            }
        };
        let source = Path::from_str(path)?;
        let Some((target, mnt)) = self.get_mnt(&source)? else {
            return Ok(None);
        };
        Ok(Some(FsModeExport {
            mount_id: mnt.info.mount_id,
            source,
            target,
            gen,
        }))
    }

    /// Submit `exports` without waiting for the copies. A failure leaves the
    /// files `Cv`; Free reconciliation re-exports them, so it never blocks the
    /// replay cursor (which also gates log compaction).
    pub async fn submit_fs_mode_exports(&self, exports: Vec<FsModeExport>) {
        if exports.is_empty() {
            return;
        }
        match self.job_manager.fs_mode_exporter() {
            Some(exporter) => {
                exporter.submit(exports).await;
            }
            None => warn!(
                "{} fs_mode export(s) skipped: no Transfer client",
                exports.len()
            ),
        }
    }

    pub async fn complete_file(&self, e: &CompleteFileEntry) -> CommonResult<()> {
        if self.transfer_enabled() {
            if let Some(export) = self.fs_mode_export(e)? {
                self.submit_fs_mode_exports(vec![export]).await;
            }
            return Ok(());
        }

        if !e.file.is_complete() || e.file.ufs_only() {
            return Ok(());
        }

        let entry_path = Path::from_str(&e.path)?;
        let Some((_, mnt)) = self.get_mnt(&entry_path)? else {
            return Ok(());
        };

        // Rebuild via inode id so rename-after-open still exports to the live path.
        // Only fall back to e.path on typed not-found; store / corruption errors propagate.
        let path = match self.get_real_path(e.file.id) {
            Ok(path) => path,
            Err(FsError::FileNotFound(_)) => {
                warn!(
                    "complete_file: inode {} not found, fallback to entry.path={}",
                    e.file.id, e.path
                );
                entry_path
            }
            Err(err) => return Err(err.into()),
        };
        self.submit_export_task(&path, &mnt).await?;
        Ok(())
    }

    pub async fn rename(&self, e: &RenameEntry) -> CommonResult<()> {
        let src = Path::from_str(&e.src)?;
        let dst = Path::from_str(&e.dst)?;
        if let Some((src_ufs_path, mnt)) = self.get_mnt(&src)? {
            let ufs = mnt.ufs()?;
            if !ufs.exists(&src_ufs_path).await? {
                if self.transfer_enabled() {
                    // Nothing to move. An unexported file is exported under its
                    // current path by its own CompleteFile or by Free
                    // reconciliation.
                    debug!("rename: src {} not on UFS, nothing to move", src_ufs_path);
                    return Ok(());
                }
                warn!(
                    "rename: src file not exists: {}, exporting dst {}",
                    src_ufs_path,
                    dst.full_path()
                );
                if let Some((_, dst_mnt)) = self.get_mnt(&dst)? {
                    // Keep journal apply aligned with fs_mode's durable export contract.
                    self.submit_export_task(&dst, &dst_mnt).await?;
                }
                return Ok(());
            }

            let src_dst_path = mnt.info.get_ufs_path(&dst)?;
            if ufs.fs_kind().support_rename() {
                match ufs.rename(&src_ufs_path, &src_dst_path).await {
                    Ok(_) => {}
                    Err(e @ FsError::FileNotFound(_)) => {
                        if !ufs.exists(&src_ufs_path).await? {
                            return Err(e.into());
                        }
                        self.mkdir_parent(&dst, &mnt).await?;
                        ufs.rename(&src_ufs_path, &src_dst_path).await?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            } else if self.transfer_enabled() {
                // Move the objects themselves (server-side copy + delete). The
                // legacy delete-then-export would destroy the only copy of a
                // released file and re-upload everything else.
                self.move_objects(&ufs, &src_ufs_path, &src_dst_path)
                    .await?;
                Ok(())
            } else {
                ufs.delete(&src_ufs_path, true).await?;
                self.submit_export_task(&dst, &mnt).await?;
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    /// Move `src` to `dst` on a UFS without native rename. Files move one by one
    /// (OpenDAL copies then deletes), so a rerun after a crash continues with
    /// whatever is still under `src`.
    async fn move_objects(&self, ufs: &UfsFileSystem, src: &Path, dst: &Path) -> FsResult<()> {
        if !ufs.get_status(src).await?.is_dir {
            ufs.rename(src, dst).await?;
            return Ok(());
        }
        let mut pending = vec![(src.clone(), dst.clone())];
        while let Some((from, to)) = pending.pop() {
            for child in ufs.list_status(&from).await? {
                let child_src = Path::from_str(&child.path)?;
                let child_dst = Path::from_str(format!(
                    "{}/{}",
                    to.full_path().trim_end_matches('/'),
                    child.name
                ))?;
                if child.is_dir {
                    pending.push((child_src, child_dst));
                } else {
                    ufs.rename(&child_src, &child_dst).await?;
                }
            }
        }
        match ufs.delete(src, true).await {
            Ok(_) | Err(FsError::FileNotFound(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn delete(&self, e: &DeleteEntry) -> CommonResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((ufs_path, mnt)) = self.get_mnt(&path)? {
            let ufs = mnt.ufs()?;
            if ufs.exists(&ufs_path).await? {
                ufs.delete(&ufs_path, true).await?;
            } else {
                warn!("delete: src file not exists: {}", ufs_path);
            }
            Ok(())
        } else {
            Ok(())
        }
    }
}
