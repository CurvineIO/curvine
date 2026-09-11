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
use crate::master::{JobManager, SyncFsDir};
use curvine_config::JournalConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{JobTaskState, LoadJobCommand};
use curvine_runtime::common::DurationUnit;
use curvine_unified_fs::MountValue;
use log::{info, warn};
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

    pub(crate) fn resolve_complete_entry_path(&self, e: &CompleteFileEntry) -> FsResult<Path> {
        match self.get_real_path(e.file.id) {
            Ok(path) => Ok(path),
            Err(err) => {
                warn!(
                    "complete_file: get_inode_path({}) failed: {}, fallback to entry.path={}",
                    e.file.id, err, e.path
                );
                Ok(Path::from_str(&e.path)?)
            }
        }
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

    pub async fn complete_file(&self, e: &CompleteFileEntry) -> CommonResult<()> {
        if !e.file.is_complete() || e.file.ufs_only() {
            return Ok(());
        }

        let path = self.resolve_complete_entry_path(e)?;
        if let Some((_, mnt)) = self.get_mnt(&path)? {
            self.submit_export_task(&path, &mnt).await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn rename(&self, e: &RenameEntry) -> CommonResult<()> {
        let src = Path::from_str(&e.src)?;
        let dst = Path::from_str(&e.dst)?;
        if let Some((src_ufs_path, mnt)) = self.get_mnt(&src)? {
            let ufs = mnt.ufs()?;
            if !ufs.exists(&src_ufs_path).await? {
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
            } else {
                ufs.delete(&src_ufs_path, true).await?;
                self.submit_export_task(&dst, &mnt).await?;
                Ok(())
            }
        } else {
            Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use crate::master::meta::inode::InodeView;
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_model::RenameFlags;
    use curvine_runtime::common::Utils;

    fn test_fs(name: &str) -> crate::master::fs::MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "ufs-loader-path-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "ufs-loader-path-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn complete_entry_with_stale_path(
        fs: &crate::master::fs::MasterFilesystem,
        stale_path: &str,
        inode_id: i64,
    ) -> CompleteFileEntry {
        let fs_dir = fs.fs_dir.read();
        let view = fs_dir
            .store
            .get_inode(inode_id, None)
            .unwrap()
            .expect("inode must exist");
        let file = match view {
            InodeView::File(f) => f.file.clone(),
            other => panic!("expected file inode, got {:?}", other.name()),
        };
        CompleteFileEntry {
            op_id: 1,
            rpc_id: 0,
            path: stale_path.to_string(),
            file,
            commit_blocks: vec![],
        }
    }

    #[test]
    fn resolve_complete_entry_path_uses_inode_after_rename() {
        let fs = test_fs("complete-path-rename");
        let created = fs.create("/x/old.log", true).unwrap();
        fs.rename("/x/old.log", "/x/new.log", RenameFlags::empty())
            .unwrap();

        let entry = complete_entry_with_stale_path(&fs, "/x/old.log", created.id);
        let resolved = resolve_entry_path_for_test(&fs, &entry).unwrap();
        assert_eq!(resolved.full_path(), "/x/new.log");
    }

    #[test]
    fn resolve_complete_entry_path_falls_back_when_inode_missing() {
        let fs = test_fs("complete-path-fallback");
        let created = fs.create("/y/file.log", true).unwrap();
        let mut entry = complete_entry_with_stale_path(&fs, "/y/file.log", created.id);
        entry.file.id = 9_999_999;

        let resolved = resolve_entry_path_for_test(&fs, &entry).unwrap();
        assert_eq!(resolved.full_path(), "/y/file.log");
    }

    /// Mirrors `UfsLoader::resolve_complete_entry_path` without needing JobManager.
    fn resolve_entry_path_for_test(
        fs: &crate::master::fs::MasterFilesystem,
        e: &CompleteFileEntry,
    ) -> FsResult<Path> {
        match fs.fs_dir.read().get_inode_path(e.file.id) {
            Ok(path) => Ok(Path::from_str(path)?),
            Err(_) => Ok(Path::from_str(&e.path)?),
        }
    }
}
