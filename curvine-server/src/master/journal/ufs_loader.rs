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
    CompleteFileEntry, DeleteEntry, JournalBatch, JournalEntry, MkdirEntry, OverWriteFileEntry,
    RenameEntry, ReopenFileEntry,
};
use crate::master::JobManager;
use curvine_common::conf::JournalConf;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::LoadJobCommand;
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use log::error;
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::BlockingChannel;
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct UfsLoader {
    job_manager: Arc<JobManager>,
    ignore_replay_error: bool,
}

impl UfsLoader {
    pub fn new(job_manager: Arc<JobManager>, conf: &JournalConf) -> Self {
        UfsLoader {
            job_manager,
            ignore_replay_error: conf.ignore_ufs_replay_error,
        }
    }

    pub fn cancel_job(&self, path: &Path) -> FsResult<()> {
        if self.job_manager.get_mnt(path)?.is_some() {
            let job_id = CommonUtils::create_job_id(path.full_path());
            self.job_manager.cancel_job(job_id)
        } else {
            Ok(())
        }
    }

    pub fn apply_batch(&self, batch: JournalBatch) -> CommonResult<()> {
        let (tx, rx) = BlockingChannel::new(1).split();

        let loader = self.clone();
        self.job_manager.rt().spawn(async move {
            let mut res: CommonResult<()> = Ok(());
            for entry in batch.batch {
                if let Err(e) = loader.apply_entry(&entry).await {
                    error!("apply ufs: {}", e);

                    if !loader.ignore_replay_error {
                        res = Err(e.into());
                        break;
                    }
                }
            }

            if let Err(e) = tx.send(res) {
                error!("send apply_entry result: {}", e);
            }
        });

        rx.recv_check()?
    }

    pub async fn apply_entry(&self, entry: &JournalEntry) -> FsResult<()> {
        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e).await,
            JournalEntry::OverWriteFile(e) => self.overwrite_file(e).await,
            JournalEntry::CompleteFile(e) => self.complete_file(e).await,
            JournalEntry::Rename(e) => self.rename(e).await,
            JournalEntry::Delete(e) => self.delete(e).await,
            JournalEntry::ReopenFile(e) => self.reopen_file(e).await,
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

    pub async fn overwrite_file(&self, e: &OverWriteFileEntry) -> FsResult<()> {
        let path = Path::from_str(&e.path)?;
        self.cancel_job(&path)
    }

    pub async fn complete_file(&self, e: &CompleteFileEntry) -> FsResult<()> {
        if !e.file.is_complete() {
            return Ok(());
        }

        let path = Path::from_str(&e.path)?;
        if self.job_manager.get_mnt(&path)?.is_some() {
            let command = LoadJobCommand::builder(path.clone_uri()).build();
            let _ = self.job_manager.submit_load_job(command)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn rename(&self, e: &RenameEntry) -> FsResult<()> {
        let src = Path::from_str(&e.src)?;
        let dst = Path::from_str(&e.dst)?;
        if let Some((src_ufs_path, mnt)) = self.job_manager.get_mnt(&src)? {
            let dst_ufs_path = mnt.get_ufs_path(&dst)?;
            mnt.ufs.rename(&src_ufs_path, &dst_ufs_path).await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn delete(&self, e: &DeleteEntry) -> FsResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((ufs_path, mnt)) = self.job_manager.get_mnt(&path)? {
            mnt.ufs.delete(&ufs_path, true).await
        } else {
            Ok(())
        }
    }

    pub async fn reopen_file(&self, e: &ReopenFileEntry) -> FsResult<()> {
        let path = Path::from_str(&e.path)?;
        self.cancel_job(&path)
    }
}
