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

use crate::common::UfsFactory;
use crate::worker::task::TaskContext;
use curvine_client::file::CurvineFileSystem;
use curvine_client::rpc::JobMasterClient;
use curvine_client::unified::{UnifiedReader, UnifiedWriter};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{CreateFileOptsBuilder, JobTaskState};
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::err_box;
use std::sync::Arc;

pub struct LoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

impl LoadTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        let master_client = JobMasterClient::new(fs.fs_client());
        Self {
            task,
            fs,
            factory,
            master_client,
            progress_interval_ms,
            task_timeout_ms,
        }
    }

    pub async fn run(&self) {
        if let Err(e) = self.run0().await {
            // The data replication process fails, set the status and report to the master
            error!("task {} execute failed: {}", self.task.info.task_id, e);
            let progress = self.task.set_failed(e.to_string());
            let res = self
                .master_client
                .report_task(
                    self.task.info.job.job_id.clone(),
                    self.task.info.task_id.clone(),
                    progress,
                )
                .await;

            if let Err(e) = res {
                warn!("report task {}", e)
            }
        }
    }

    async fn run0(&self) -> FsResult<()> {
        self.task
            .update_state(JobTaskState::Loading, "Task started");

        let (mut reader, mut writer) = self.create_stream().await?;
        let mut last_progress_time = LocalTime::mills();
        let mut read_cost_ms = 0;
        let mut total_cost_ms = 0;

        loop {
            if self.task.is_cancel() {
                info!("task {} was cancelled", self.task.info.task_id);
                break;
            }

            let spend = TimeSpent::new();
            let chunk = reader.async_read(None).await?;
            read_cost_ms += spend.used_ms();

            if chunk.is_empty() {
                break;
            }

            writer.async_write(chunk).await?;
            total_cost_ms += spend.used_ms();

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(writer.pos(), reader.len()).await;
            }

            if total_cost_ms > self.task_timeout_ms {
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
        }

        writer.complete().await?;
        reader.complete().await?;
        self.update_progress(writer.pos(), reader.len()).await;

        info!(
            "task {} completed, source_path {}, target_path {}, copy bytes {}, read cost {} ms, task cost {} ms",
            self.task.info.task_id,
            self.task.info.source_path,
            self.task.info.target_path,
            writer.pos(),
            read_cost_ms,
            total_cost_ms,
        );

        Ok(())
    }

    async fn create_stream(&self) -> FsResult<(UnifiedReader, UnifiedWriter)> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;

        // Create reader (automatically selects filesystem based on scheme)
        let reader = self.open_unified(&source_path).await?;

        // create cv writer
        let target_path = Path::from_str(&info.target_path)?;
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(info.job.replicas)
            .block_size(info.job.block_size)
            .storage_type(info.job.storage_type)
            .ttl_ms(info.job.ttl_ms)
            .ttl_action(info.job.ttl_action)
            .ufs_mtime(source_status.mtime)
            .build();
        let writer = self.fs.create_with_opts(&target_path, opts, true).await?;

        Ok((reader, writer))
    }

    async fn open_unified(&self, path: &Path) -> FsResult<UnifiedReader> {
        if path.is_cv() {
            // Curvine path
            let reader = self.fs.open(path).await?;
            Ok(UnifiedReader::Cv(reader))
        } else {
            // UFS path
            let ufs = self.factory.get_ufs(&self.task.info.job.mount_info)?;
            ufs.open(path).await
        }
    }

    async fn create_unified(&self, path: &Path) -> FsResult<UnifiedWriter> {
        if path.is_cv() {
            // Curvine path - get source mtime for UFS→Curvine import
            let source_path = Path::from_str(&self.task.info.source_path)?;
            let source_mtime = if !source_path.is_cv() {
                // Import from UFS, get source mtime
                let ufs = self.factory.get_ufs(&self.task.info.job.mount_info)?;
                let source_status = ufs.get_status(&source_path).await?;
                source_status.mtime
            } else {
                // Curvine→Curvine (not supported yet), use 0
                0
            };

            let opts = CreateFileOptsBuilder::new()
                .overwrite(true)
                .create_parent(true)
                .replicas(self.task.info.job.replicas)
                .block_size(self.task.info.job.block_size)
                .storage_type(self.task.info.job.storage_type)
                .ttl_ms(self.task.info.job.ttl_ms)
                .ttl_action(self.task.info.job.ttl_action)
                .ufs_mtime(source_mtime)
                .build();

            let writer = self.fs.create_with_opts(path, opts).await?;
            Ok(UnifiedWriter::Cv(writer))
        } else {
            // UFS path
            let ufs = self.factory.get_ufs(&self.task.info.job.mount_info)?;
            let overwrite = self.task.info.job.overwrite.unwrap_or(false);

            // Check if file exists when overwrite=false
            if !overwrite {
                if ufs.exists(path).await? {
                    warn!("UFS file already exists, skipping: {}", path.full_path());
                    return err_box!("File exists and overwrite=false");
                }
            }

            ufs.create(path, overwrite).await
        }
    }

    pub async fn update_progress(&self, loaded_size: i64, total_size: i64) {
        if let Err(e) = self.update_progress0(loaded_size, total_size).await {
            warn!("update progress failed, err: {:?}", e);
        }
    }

    pub async fn update_progress0(&self, loaded_size: i64, total_size: i64) -> FsResult<()> {
        let progress = self.task.update_progress(loaded_size, total_size);
        let task = &self.task;

        self.master_client
            .report_task(&task.info.job.job_id, &task.info.task_id, progress)
            .await
    }
}
