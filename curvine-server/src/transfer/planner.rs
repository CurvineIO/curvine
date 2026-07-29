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
use crate::transfer::{
    job_mount_snapshot, ClusterMetadataCache, CvMetadataReader, TransferMetrics,
};
use curvine_common::conf::ClientConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    FileStatus, LoadJobInfo, MountInfo, TransferCommand, TransferJobRecord, TransferKind,
    TransferTaskRecord, TransferTaskState,
};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::err_box;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_EXPORT_EPOCH_PLAN_RETRIES: usize = 3;

pub struct PlannedTransfer {
    pub job_info: LoadJobInfo,
    pub tasks: Vec<TransferTaskRecord>,
    pub total_size: i64,
    pub cv_metadata_epoch: Option<u64>,
}

#[derive(Clone)]
pub struct TransferPlanner {
    cv_metadata: Arc<dyn CvMetadataReader>,
    factory: Arc<UfsFactory>,
    cache: ClusterMetadataCache,
    client_conf: ClientConf,
    max_tasks_per_transfer: usize,
    ufs_limiter: Arc<UfsEndpointLimiter>,
}

impl TransferPlanner {
    pub fn new(
        cv_metadata: Arc<dyn CvMetadataReader>,
        factory: Arc<UfsFactory>,
        cache: ClusterMetadataCache,
        client_conf: ClientConf,
        max_tasks_per_transfer: usize,
        ufs_max_concurrency_per_endpoint: usize,
    ) -> Self {
        Self {
            cv_metadata,
            factory,
            cache,
            client_conf,
            max_tasks_per_transfer,
            ufs_limiter: Arc::new(UfsEndpointLimiter::new(ufs_max_concurrency_per_endpoint)),
        }
    }

    pub async fn plan(&self, job: &TransferJobRecord) -> FsResult<PlannedTransfer> {
        for attempt in 0..MAX_EXPORT_EPOCH_PLAN_RETRIES {
            let cv_metadata_epoch = self.export_epoch_before_plan(job)?;
            let planned = self.plan_once(job, cv_metadata_epoch).await?;
            if !self.export_epoch_changed(job, cv_metadata_epoch)? {
                return Ok(planned);
            }
            log::warn!(
                "CV metadata replica epoch changed while planning export {}, retry {}/{}",
                job.job_id,
                attempt + 1,
                MAX_EXPORT_EPOCH_PLAN_RETRIES
            );
        }
        Err(FsError::common(format!(
            "CV metadata replica epoch changed during export planning {} after {} retries",
            job.job_id, MAX_EXPORT_EPOCH_PLAN_RETRIES
        )))
    }

    async fn plan_once(
        &self,
        job: &TransferJobRecord,
        cv_metadata_epoch: Option<u64>,
    ) -> FsResult<PlannedTransfer> {
        let source = Path::from_str(&job.source_path)?;
        let target = Path::from_str(&job.target_path)?;
        let mount = job_mount_snapshot(job, &self.cache)?;
        let job_info = self.load_job_info(job, &mount);
        let source_status = self
            .get_status(job, &source, &mount, cv_metadata_epoch)
            .await?;
        if source_status.is_dir {
            self.validate_directory_target(job, &target, &mount, cv_metadata_epoch)
                .await?;
        }

        let mut tasks = Vec::new();
        let mut total_size = 0;
        let mut stack = VecDeque::new();
        stack.push_back(source_status);

        while let Some(status) = stack.pop_front() {
            let path = Path::from_str(&status.path)?;
            if status.is_dir {
                for child in self
                    .list_status(job, &path, &mount, cv_metadata_epoch)
                    .await?
                {
                    stack.push_back(child);
                }
                continue;
            }

            let task_target = append_relative_path(&source, &target, &path)?;
            let task_id = format!("{}_task_{}", job.job_id, tasks.len());
            let source_read_plan_json = self
                .source_read_plan_json(job, &path, cv_metadata_epoch)
                .await?;
            total_size += status.len;
            tasks.push(TransferTaskRecord {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id,
                attempt_id: 0,
                source_path: path.clone_uri(),
                target_path: task_target.clone_uri(),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json,
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
                progress: Default::default(),
                retry_count: 0,
                attempt_started_at: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: LocalTime::mills() as i64,
            });

            if tasks.len() > self.max_tasks_per_transfer {
                return err_box!(
                    "Transfer {} contains more files than the service allows; reduce the source scope",
                    job.job_id
                );
            }
        }

        Ok(PlannedTransfer {
            job_info,
            tasks,
            total_size,
            cv_metadata_epoch,
        })
    }

    async fn validate_directory_target(
        &self,
        job: &TransferJobRecord,
        target: &Path,
        mount: &MountInfo,
        cv_metadata_epoch: Option<u64>,
    ) -> FsResult<()> {
        match self.get_status(job, target, mount, cv_metadata_epoch).await {
            Ok(status) if status.is_dir => Ok(()),
            Ok(_) => err_box!(
                "Transfer target {} is a file; refusing to transfer directory into file",
                target.full_path()
            ),
            Err(FsError::FileNotFound(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn export_epoch_before_plan(&self, job: &TransferJobRecord) -> FsResult<Option<u64>> {
        if job.kind != TransferKind::Export {
            return Ok(None);
        }
        let Some(epoch) = self.cv_metadata.current_epoch()? else {
            return Ok(None);
        };
        Ok(Some(job.cv_metadata_epoch.unwrap_or(epoch)))
    }

    fn export_epoch_changed(
        &self,
        job: &TransferJobRecord,
        epoch_before: Option<u64>,
    ) -> FsResult<bool> {
        if job.kind != TransferKind::Export {
            return Ok(false);
        }
        if job.cv_metadata_epoch.is_some() {
            return Ok(false);
        }
        let Some(epoch_before) = epoch_before else {
            return Ok(false);
        };
        let Some(epoch_after) = self.cv_metadata.current_epoch()? else {
            return Ok(false);
        };
        Ok(epoch_before != epoch_after)
    }

    fn load_job_info(&self, job: &TransferJobRecord, mount: &MountInfo) -> LoadJobInfo {
        let overwrite = transfer_command(job)
            .map(|command| command.overwrite())
            .unwrap_or(true);
        LoadJobInfo {
            job_id: job.job_id.clone(),
            source_path: job.source_path.clone(),
            target_path: job.target_path.clone(),
            replicas: mount.replicas.unwrap_or(self.client_conf.replicas),
            block_size: mount.block_size.unwrap_or(self.client_conf.block_size),
            storage_type: mount.storage_type.unwrap_or(self.client_conf.storage_type),
            ttl_ms: mount.ttl_ms,
            ttl_action: mount.ttl_action,
            mount_info: mount.clone(),
            create_time: job.created_at,
            overwrite: Some(overwrite),
        }
    }

    async fn get_status(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        mount: &MountInfo,
        cv_metadata_epoch: Option<u64>,
    ) -> FsResult<FileStatus> {
        let start = Instant::now();
        if path.is_cv() {
            let result = self
                .cv_metadata
                .get_status_at_epoch(path, cv_metadata_epoch)
                .await;
            record_metadata_operation("cv", "get_status", result.is_ok(), start);
            result.map_err(|err| self.map_export_cv_not_found(job, path, err))
        } else {
            let _permit = self.ufs_limiter.acquire(mount).await?;
            let result = match self.factory.get_ufs(mount) {
                Ok(ufs) => ufs.get_status(path).await,
                Err(err) => Err(err),
            };
            record_metadata_operation("ufs", "get_status", result.is_ok(), start);
            result
        }
    }

    fn map_export_cv_not_found(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        err: FsError,
    ) -> FsError {
        if job.kind != TransferKind::Export || !matches!(err, FsError::FileNotFound(_)) {
            return err;
        }
        match self.cv_metadata.covers_time_ms(job.created_at) {
            Ok(false) => FsError::in_progress_msg(format!(
                "CV metadata replica has not caught up for export {}: path={}, job_created_at={}",
                job.job_id,
                path.full_path(),
                job.created_at
            )),
            Ok(true) => err,
            Err(refresh_err) => refresh_err,
        }
    }

    async fn list_status(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        mount: &MountInfo,
        cv_metadata_epoch: Option<u64>,
    ) -> FsResult<Vec<FileStatus>> {
        let start = Instant::now();
        if path.is_cv() {
            let result = self
                .cv_metadata
                .list_status_at_epoch(path, cv_metadata_epoch)
                .await;
            record_metadata_operation("cv", "list_status", result.is_ok(), start);
            result.map_err(|err| self.map_export_cv_not_found(job, path, err))
        } else {
            let _permit = self.ufs_limiter.acquire(mount).await?;
            let result = match self.factory.get_ufs(mount) {
                Ok(ufs) => ufs.list_status(path).await,
                Err(err) => Err(err),
            };
            record_metadata_operation("ufs", "list_status", result.is_ok(), start);
            result
        }
    }

    async fn source_read_plan_json(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        cv_metadata_epoch: Option<u64>,
    ) -> FsResult<String> {
        if !path.is_cv() {
            return Ok(String::new());
        }
        let start = Instant::now();
        let blocks = self
            .cv_metadata
            .get_block_locations_at_epoch(path, cv_metadata_epoch)
            .await;
        record_metadata_operation("cv", "get_block_locations", blocks.is_ok(), start);
        let blocks = blocks.map_err(|err| self.map_export_cv_not_found(job, path, err))?;
        serde_json::to_string(&blocks).map_err(|_| {
            FsError::common(format!(
                "Unable to prepare the CV read plan for {}",
                path.full_path()
            ))
        })
    }
}

struct UfsEndpointLimiter {
    max_per_endpoint: usize,
    semaphores: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl UfsEndpointLimiter {
    fn new(max_per_endpoint: usize) -> Self {
        Self {
            max_per_endpoint,
            semaphores: Mutex::new(HashMap::new()),
        }
    }

    async fn acquire(&self, mount: &MountInfo) -> FsResult<OwnedSemaphorePermit> {
        let endpoint = ufs_endpoint_key(mount);
        let semaphore = {
            let mut semaphores = self.semaphores.lock();
            semaphores
                .entry(endpoint)
                .or_insert_with(|| Arc::new(Semaphore::new(self.max_per_endpoint)))
                .clone()
        };
        semaphore.acquire_owned().await.map_err(|err| {
            FsError::common(format!(
                "Failed to acquire UFS endpoint planning permit: {}",
                err
            ))
        })
    }
}

fn ufs_endpoint_key(mount: &MountInfo) -> String {
    let Ok(path) = Path::from_str(&mount.ufs_path) else {
        return mount.ufs_path.clone();
    };
    match path.scheme() {
        Some("file") => "file://".to_string(),
        Some(scheme) => format!("{}://{}", scheme, path.authority().unwrap_or("")),
        None => mount.ufs_path.clone(),
    }
}

fn record_metadata_operation(
    source: &'static str,
    operation: &'static str,
    success: bool,
    start: Instant,
) {
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.observe_metadata_operation(
            source,
            operation,
            if success { "success" } else { "error" },
            start.elapsed().as_micros(),
        );
    }
}

fn append_relative_path(source_root: &Path, target_root: &Path, source: &Path) -> FsResult<Path> {
    let source_root_text = normalized_path_text(source_root);
    let source_text = normalized_path_text(source);
    let target_root_text = normalized_path_text(target_root);

    let relative = if source_text == source_root_text {
        ""
    } else {
        source_text
            .strip_prefix(source_root_text.as_str())
            .ok_or_else(|| {
                FsError::common(format!(
                    "Source path {} is not under transfer root {}",
                    source.full_path(),
                    source_root.full_path()
                ))
            })?
            .trim_start_matches('/')
    };

    let target = if relative.is_empty() {
        target_root_text
    } else {
        format!("{}/{}", target_root_text.trim_end_matches('/'), relative)
    };
    Ok(Path::from_str(target)?)
}

fn transfer_command(job: &TransferJobRecord) -> FsResult<TransferCommand> {
    serde_json::from_str(&job.command_json).map_err(|_| {
        FsError::common(format!(
            "Stored transfer command for job {} is invalid",
            job.job_id
        ))
    })
}

fn normalized_path_text(path: &Path) -> String {
    if path.is_cv() {
        path.path().trim_end_matches('/').to_string()
    } else {
        path.full_path().trim_end_matches('/').to_string()
    }
}
