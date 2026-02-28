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
    CompleteFileEntry, DeleteEntry, JournalBatch, JournalEntry, MkdirEntry,
    RenameEntry,
};
use crate::master::JobManager;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{JobTaskState, LoadJobCommand};
use curvine_common::FsResult;
use log::warn;
use orpc::{CommonResult, err_box};
use std::sync::Arc;
use std::time::Duration;
use curvine_client::unified::MountValue;
use curvine_common::error::FsError;

#[derive(Clone)]
pub struct UfsLoader {
    job_manager: Arc<JobManager>,
}

impl UfsLoader {
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        Self { job_manager }
    }

    pub async fn apply_batch(&self, batch: JournalBatch) -> CommonResult<()> {
        for entry in batch.batch {
            self.apply_entry(&entry).await?;
        }
        Ok(())
    }

    pub async fn wait_job_complete(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        loop {
            let res = self.job_manager.get_job_status(job_id)?;
            match res.state {
                JobTaskState::Failed | JobTaskState::Canceled => {
                    return err_box!("load job failed: {}", res.progress.message);
                }

                JobTaskState::Completed => {
                    return Ok(());
                }

                _ => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn submit_load_task(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        let command = LoadJobCommand::builder(path.clone_uri()).build();
        let runner = self.job_manager.create_runner();
        let _ = match runner.submit_load_task(command, mnt.info.clone()).await {
            Ok(res) => res,
            Err(e) => {
                return if matches!(e, FsError::FileNotFound(_)) {
                    // File may have been renamed
                    Ok(())
                } else {
                    err_box!("load job failed: {}", e)
                }
            }
        };
        Ok(())
    }

    pub async fn apply_entry(&self, entry: &JournalEntry) -> FsResult<()> {
        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e).await,
            JournalEntry::CompleteFile(e) => self.complete_file(e).await,
            JournalEntry::Rename(e) => self.rename(e).await,
            JournalEntry::Delete(e) => self.delete(e).await,
            _ => Ok(()),
        }
    }

    pub async fn mkdir(&self, e: &MkdirEntry) -> FsResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((ufs_path, mnt)) = self.job_manager.get_mnt(&path)? {
            mnt.ufs.mkdir(&ufs_path, false).await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn complete_file(&self, e: &CompleteFileEntry) -> FsResult<()> {
        if !e.file.is_complete() {
            return Ok(());
        }

        let path = Path::from_str(&e.path)?;
        if let Some((_, mnt)) = self.job_manager.get_mnt(&path)? {
            self.submit_load_task(&path, &mnt).await
        } else {
            Ok(())
        }
    }

    pub async fn rename(&self, e: &RenameEntry) -> FsResult<()> {
        let src = Path::from_str(&e.src)?;
        let dst = Path::from_str(&e.dst)?;
        if let Some((src_ufs_path, mnt)) = self.job_manager.get_mnt(&src)? {
            if mnt.ufs.exists(&src_ufs_path).await? {
                mnt.ufs.delete(&src_ufs_path,true).await?;
            }
            self.submit_load_task(&dst, &mnt).await
        } else {
            Ok(())
        }
    }

    pub async fn delete(&self, e: &DeleteEntry) -> FsResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((ufs_path, mnt)) = self.job_manager.get_mnt(&path)? {
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
