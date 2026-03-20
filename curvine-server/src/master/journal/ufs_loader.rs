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
use crate::master::JobManager;
use curvine_client::unified::MountValue;
use curvine_common::conf::JournalConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{JobTaskState, LoadJobCommand};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::DurationUnit;
use orpc::{err_box, CommonResult};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct UfsLoader {
    job_manager: Arc<JobManager>,
    copy_timeout: Duration,
}

impl UfsLoader {
    pub fn new(job_manager: Arc<JobManager>, conf: &JournalConf) -> Self {
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
            copy_timeout,
        }
    }

    fn get_mnt(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        match self.job_manager.get_mnt(path)? {
            Some((path, mnt)) if mnt.info.is_fs_mode() => Ok(Some((path, mnt))),

            _ => Ok(None),
        }
    }

    pub async fn submit_load_task(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        let command = LoadJobCommand::builder(path.clone_uri()).build();
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
        if let Some((ufs_path, mnt)) = self.get_mnt(&path)? {
            mnt.ufs.mkdir(&ufs_path, false).await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn complete_file(&self, e: &CompleteFileEntry) -> CommonResult<()> {
        if !e.file.is_complete() || e.file.ufs_only() {
            return Ok(());
        }

        let path = Path::from_str(&e.path)?;
        if let Some((_, mnt)) = self.get_mnt(&path)? {
            self.submit_load_task(&path, &mnt).await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn rename(&self, e: &RenameEntry) -> CommonResult<()> {
        let src = Path::from_str(&e.src)?;
        let dst = Path::from_str(&e.dst)?;
        if let Some((src_ufs_path, mnt)) = self.get_mnt(&src)? {
            if !mnt.ufs.exists(&src_ufs_path).await? {
                warn!("rename: src file not exists: {}", src_ufs_path);
                return Ok(());
            }

            let src_dst_path = mnt.info.get_ufs_path(&dst)?;
            if mnt.ufs.fs_kind().support_rename() {
                mnt.ufs.rename(&src_ufs_path, &src_dst_path).await?;
                Ok(())
            } else {
                mnt.ufs.delete(&src_ufs_path, true).await?;
                self.submit_load_task(&dst, &mnt).await?;
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    pub async fn delete(&self, e: &DeleteEntry) -> CommonResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((ufs_path, mnt)) = self.get_mnt(&path)? {
            if mnt.ufs.exists(&ufs_path).await? {
                mnt.ufs.delete(&ufs_path, true).await?;
            } else {
                warn!("delete: src file not exists: {}", ufs_path);
            }
            Ok(())
        } else {
            Ok(())
        }
    }
}
