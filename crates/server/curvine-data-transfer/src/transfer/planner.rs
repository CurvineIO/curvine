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
use crate::transfer::{job_mount_snapshot, ClusterMetadataCache, TransferMetrics};
use curvine_client_core::file::CurvineFileSystem;
use curvine_common::conf::ClientConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::ListOptions;
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

const CV_LIST_PAGE_SIZE: usize = 1_000;

pub struct PlannedTransfer {
    pub job_info: LoadJobInfo,
    pub tasks: Vec<TransferTaskRecord>,
    pub total_size: i64,
    pub skipped_files: usize,
    pub skipped_size: i64,
    pub cv_metadata_epoch: Option<u64>,
}

#[derive(Clone)]
pub struct TransferPlanner {
    master_fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    cache: ClusterMetadataCache,
    client_conf: ClientConf,
    max_tasks_per_transfer: usize,
    ufs_limiter: Arc<UfsEndpointLimiter>,
}

impl TransferPlanner {
    pub fn new(
        master_fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        cache: ClusterMetadataCache,
        client_conf: ClientConf,
        max_tasks_per_transfer: usize,
        ufs_max_concurrency_per_endpoint: usize,
    ) -> Self {
        Self {
            master_fs,
            factory,
            cache,
            client_conf,
            max_tasks_per_transfer,
            ufs_limiter: Arc::new(UfsEndpointLimiter::new(ufs_max_concurrency_per_endpoint)),
        }
    }

    pub async fn plan(&self, job: &TransferJobRecord) -> FsResult<PlannedTransfer> {
        let mount = job_mount_snapshot(job, &self.cache)?;
        self.plan_once(job, mount).await
    }

    async fn plan_once(
        &self,
        job: &TransferJobRecord,
        mount: MountInfo,
    ) -> FsResult<PlannedTransfer> {
        let source = Path::from_str(&job.source_path)?;
        let target = Path::from_str(&job.target_path)?;
        let job_info = self.load_job_info(job, &mount);
        let source_status = self.get_status(&source, &mount).await?;
        let incremental_load = job.kind == TransferKind::Load && !source.is_cv() && target.is_cv();
        let target_status = if incremental_load {
            self.get_optional_status(&target, &mount).await?
        } else {
            None
        };
        validate_transfer_target(&source_status, target_status.as_ref(), &target)?;
        if source_status.is_dir && !incremental_load {
            self.validate_directory_target(job, &target, &mount).await?;
        }

        let mut tasks = Vec::new();
        let mut total_size = 0;
        let mut skipped_files = 0;
        let mut skipped_size = 0;
        let mut stack = VecDeque::new();
        stack.push_back((source_status, target_status));

        while let Some((status, target_status)) = stack.pop_front() {
            let path = Path::from_str(&status.path)?;
            if status.is_dir {
                let target_children = if incremental_load && target_status.is_some() {
                    let target_path = append_relative_path(&source, &target, &path)?;
                    self.list_status(&target_path, &mount)
                        .await?
                        .into_iter()
                        .map(|status| (status.name.clone(), status))
                        .collect()
                } else {
                    HashMap::new()
                };
                for child in self.list_status(&path, &mount).await? {
                    let target_status = target_children.get(&child.name).cloned();
                    stack.push_back((child, target_status));
                }
                continue;
            }

            let task_target = append_relative_path(&source, &target, &path)?;
            validate_transfer_target(&status, target_status.as_ref(), &task_target)?;
            let unchanged =
                if incremental_load && unchanged_load_target(&status, target_status.as_ref()) {
                    true
                } else if incremental_load
                    && needs_source_status_refresh(&status, target_status.as_ref())
                {
                    let current_source_status = self.get_status(&path, &mount).await?;
                    unchanged_load_target(&current_source_status, target_status.as_ref())
                } else {
                    false
                };
            if unchanged {
                skipped_files += 1;
                skipped_size += status.len;
                continue;
            }
            let task_id = format!("{}_task_{}", job.job_id, tasks.len());
            let source_read_plan_json = self.source_read_plan_json(&path).await?;
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
                    "Transfer {} files exceeds {}",
                    job.job_id,
                    self.max_tasks_per_transfer
                );
            }
        }

        Ok(PlannedTransfer {
            job_info,
            tasks,
            total_size,
            skipped_files,
            skipped_size,
            cv_metadata_epoch: None,
        })
    }

    async fn validate_directory_target(
        &self,
        job: &TransferJobRecord,
        target: &Path,
        mount: &MountInfo,
    ) -> FsResult<()> {
        let target_status = if job.kind == TransferKind::Load && target.is_cv() {
            let start = Instant::now();
            let result = self.master_fs.get_status(target).await;
            record_metadata_operation("cv", "get_status", result.is_ok(), start);
            result
        } else {
            self.get_status(target, mount).await
        };
        match target_status {
            Ok(status) if status.is_dir => Ok(()),
            Ok(_) => err_box!(
                "Transfer target {} is a file; refusing to transfer directory into file",
                target.full_path()
            ),
            Err(FsError::FileNotFound(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn load_job_info(&self, job: &TransferJobRecord, mount: &MountInfo) -> LoadJobInfo {
        load_job_info(job, mount, &self.client_conf)
    }

    async fn get_status(&self, path: &Path, mount: &MountInfo) -> FsResult<FileStatus> {
        let start = Instant::now();
        if path.is_cv() {
            let result = self.master_fs.get_status(path).await;
            record_metadata_operation("cv", "get_status", result.is_ok(), start);
            result
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

    async fn get_optional_status(
        &self,
        path: &Path,
        mount: &MountInfo,
    ) -> FsResult<Option<FileStatus>> {
        match self.get_status(path, mount).await {
            Ok(status) => Ok(Some(status)),
            Err(FsError::FileNotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn list_status(&self, path: &Path, mount: &MountInfo) -> FsResult<Vec<FileStatus>> {
        let start = Instant::now();
        if path.is_cv() {
            let result = self.list_cv_status(path).await;
            record_metadata_operation("cv", "list_status", result.is_ok(), start);
            result
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

    async fn list_cv_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let mut statuses = Vec::new();
        let mut start_after = None;
        loop {
            let page = self
                .master_fs
                .list_options(
                    path,
                    ListOptions {
                        limit: Some(CV_LIST_PAGE_SIZE),
                        start_after: start_after.take(),
                    },
                )
                .await?;
            let count = page.len();
            start_after = page.last().map(|status| status.name.clone());
            statuses.extend(page);
            if count < CV_LIST_PAGE_SIZE {
                return Ok(statuses);
            }
        }
    }

    async fn source_read_plan_json(&self, path: &Path) -> FsResult<String> {
        if !path.is_cv() {
            return Ok(String::new());
        }
        let start = Instant::now();
        let blocks = self.master_fs.get_block_locations(path).await;
        record_metadata_operation("cv", "get_block_locations", blocks.is_ok(), start);
        let blocks = blocks?;
        serde_json::to_string(&blocks).map_err(|_| {
            FsError::common(format!(
                "Unable to prepare the CV read plan for {}",
                path.full_path()
            ))
        })
    }
}

pub(crate) fn unchanged_load_target(source: &FileStatus, target: Option<&FileStatus>) -> bool {
    let Some(target) = target else {
        return false;
    };
    !target.is_dir && target.is_complete() && target.cv_valid(Some(source))
}

pub(crate) fn needs_source_status_refresh(
    source: &FileStatus,
    target: Option<&FileStatus>,
) -> bool {
    let Some(target) = target else {
        return false;
    };
    !target.is_dir
        && target.is_complete()
        && target.cv_valid(None)
        && target.len == source.len
        && target.storage_policy.ufs_mtime != source.mtime
}

fn validate_transfer_target(
    source: &FileStatus,
    target: Option<&FileStatus>,
    target_path: &Path,
) -> FsResult<()> {
    let Some(target) = target else {
        return Ok(());
    };
    if source.is_dir && !target.is_dir {
        return err_box!(
            "Transfer target {} is a file; refusing to transfer directory into file",
            target_path.full_path()
        );
    }
    if !source.is_dir && target.is_dir {
        return err_box!(
            "Transfer target {} is a directory; refusing to overwrite directory with file",
            target_path.full_path()
        );
    }
    Ok(())
}

pub(crate) fn load_job_info(
    job: &TransferJobRecord,
    mount: &MountInfo,
    client_conf: &ClientConf,
) -> LoadJobInfo {
    let overwrite = transfer_command(job)
        .map(|command| command.overwrite())
        .unwrap_or(true);
    LoadJobInfo {
        job_id: job.job_id.clone(),
        source_path: job.source_path.clone(),
        target_path: job.target_path.clone(),
        replicas: mount.replicas.unwrap_or(client_conf.replicas),
        block_size: mount.block_size.unwrap_or(client_conf.block_size),
        storage_type: mount.storage_type.unwrap_or(client_conf.storage_type),
        ttl_ms: mount.ttl_ms,
        ttl_action: mount.ttl_action,
        mount_info: mount.clone(),
        create_time: job.created_at,
        overwrite: Some(overwrite),
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
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| FsError::common("Transfer planning is stopping; retry shortly"))
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
