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

use crate::worker::load::load_task::{TaskExecutionConfig, TaskOperation, WorkerLoadError};
use crate::worker::load::{LoadTask, UfsConnector};
use curvine_client::file::FsClient;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    LoadMetrics, LoadState, LoadTaskReportRequest, LoadTaskReportResponse,
};
use curvine_ufs::fs::BufferFileTransfer;
use curvine_ufs::fs::{AsyncChunkReader, AsyncChunkWriter};
use dashmap::DashMap;
use log::{debug, error, info, warn};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Task processor structure responsible for managing and executing load tasks
pub struct TaskProcessor {
    tasks: Arc<DashMap<String, LoadTask>>,
    task_rx: mpsc::Receiver<TaskOperation>,
    running: Arc<std::sync::RwLock<bool>>,
    fs_client: Arc<FsClient>,
    exec_config: TaskExecutionConfig,
    runtime: Arc<Runtime>,
}

impl TaskProcessor {
    pub fn new(
        tasks: Arc<DashMap<String, LoadTask>>,
        task_rx: mpsc::Receiver<TaskOperation>,
        running: Arc<std::sync::RwLock<bool>>,
        fs_client: Arc<FsClient>,
        exec_config: TaskExecutionConfig,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            tasks,
            task_rx,
            running,
            fs_client,
            exec_config,
            runtime,
        }
    }

    pub async fn run(mut self) {
        // Use timeout from config, if negative set to u64::MAX to indicate no timeout
        let timeout_seconds = if self.exec_config.task_timeout_seconds < 0 {
            u64::MAX
        } else {
            self.exec_config.task_timeout_seconds as u64
        };

        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.exec_config.max_concurrent_tasks as usize,
        ));

        while let Some(op) = self.task_rx.recv().await {
            if !*self.running.read().unwrap() {
                break;
            }

            match op {
                TaskOperation::Submit(task) => {
                    let task_id = task.task_id.clone();
                    let job_id = task.job_id.clone();
                    let source_path = task.path.clone();

                    info!(
                        "Processing task: job_id={}, task_id={}, path={}",
                        job_id, task_id, source_path
                    );

                    self.tasks.insert(task_id.clone(), task.clone());

                    let semaphore_clone = Arc::clone(&semaphore);
                    let tasks_clone = Arc::clone(&self.tasks);
                    let fs_client_clone = Arc::clone(&self.fs_client);
                    let exec_config_clone = self.exec_config.clone();

                    self.runtime.spawn(async move {
                        process_task(
                            task,
                            semaphore_clone,
                            tasks_clone,
                            fs_client_clone,
                            exec_config_clone,
                            timeout_seconds,
                        )
                        .await;
                    });
                }
                TaskOperation::Cancel(task_id) => {
                    info!("Canceling task: {}", task_id);

                    if let Some(mut task) = self.tasks.get_mut(&task_id) {
                        task.update_state(
                            LoadState::Canceled,
                            Some("Task canceled by user.".to_string()),
                        );

                        let task_clone = task.clone();
                        let fs_client_clone = Arc::clone(&self.fs_client);

                        self.runtime.spawn(async move {
                            report_task_status_to_master(fs_client_clone, &task_clone, 0).await;
                        });
                    } else {
                        warn!("Cannot cancel task {}: not found", task_id);
                    }
                }
                TaskOperation::GetStatus(task_id, resp_tx) => {
                    let task = self.tasks.get(&task_id).map(|t| t.clone());
                    drop(resp_tx.send(task));
                }
                TaskOperation::GetAll(resp_tx) => {
                    let all_tasks = self.tasks.iter().map(|t| t.clone()).collect();
                    drop(resp_tx.send(all_tasks))
                }
            }
        }
        info!("Task processor exiting");
    }
}

/// Process a task with timeout and concurrency control
async fn process_task(
    task: LoadTask,
    semaphore: Arc<tokio::sync::Semaphore>,
    tasks: Arc<DashMap<String, LoadTask>>,
    fs_client: Arc<FsClient>,
    exec_config: TaskExecutionConfig,
    timeout_seconds: u64,
) {
    let task_id = task.task_id.clone();
    let job_id = task.job_id.clone();

    // Acquire semaphore permit for concurrency control
    let _permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(e) => {
            error!("Failed to acquire semaphore: {}", e);
            if let Some(mut task) = tasks.get_mut(&task_id) {
                task.update_state(
                    LoadState::Failed,
                    Some(format!("Failed to acquire execution permit: {}", e)),
                );
            }
            return;
        }
    };

    // Execute task with or without timeout based on configuration
    let result = if timeout_seconds != u64::MAX {
        // Execute with timeout
        let timeout_result = tokio::time::timeout(
            Duration::from_secs(timeout_seconds),
            execute_load_task(&task, &tasks, &fs_client, &exec_config),
        )
        .await;

        match timeout_result {
            Ok(res) => res,
            Err(_) => {
                error!(
                    "Task execution timeout: job_id={}, task_id={}",
                    job_id, task_id
                );
                Err(WorkerLoadError::LoadError("Task execution timeout".to_string()).into())
            }
        }
    } else {
        // Execute without timeout
        execute_load_task(&task, &tasks, &fs_client, &exec_config).await
    };

    // Handle execution result
    if let Err(e) = result {
        error!(
            "Task execution failed: job_id={}, task_id={}, error={}",
            job_id, task_id, e
        );
        if let Some(mut task_ref) = tasks.get_mut(&task_id) {
            task_ref.update_state(LoadState::Failed, Some(e.to_string()));
            report_task_status_to_master(fs_client.clone(), &task_ref, 0).await;
        }
    }
}

/// Execute a load task, handling file transfer between external storage and Curvine
async fn execute_load_task(
    task: &LoadTask,
    tasks: &Arc<DashMap<String, LoadTask>>,
    fs_client: &Arc<FsClient>,
    exec_config: &TaskExecutionConfig,
) -> CommonResult<()> {
    info!(
        "Starting load task: job_id={}, task_id={}, path={}",
        task.job_id, task.task_id, task.path
    );

    // Update task state to Loading
    if let Some(mut task_ref) = tasks.get_mut(&task.task_id) {
        task_ref.update_state(LoadState::Loading, Some("Task started".to_string()));
    } else {
        return Err(WorkerLoadError::TaskNotFound(task.task_id.to_string()).into());
    }

    // Create external storage connector
    let mut external_client = UfsConnector::new(task.path.clone(), fs_client.clone()).await?;

    // Get file size from external storage
    let file_size = match external_client.get_file_size().await {
        Ok(size) => size,
        Err(e) => {
            update_task_failure(tasks, task, format!("Failed to get file size: {}", e));
            return Err(
                WorkerLoadError::LoadError(format!("Failed to get file size: {}", e)).into(),
            );
        }
    };

    // Update task with total file size
    if let Some(mut task_ref) = tasks.get_mut(&task.task_id) {
        task_ref.set_total_size(file_size);
    }

    // Create reader from external storage
    let reader = match external_client.create_reader().await {
        Ok(reader) => reader,
        Err(e) => {
            update_task_failure(tasks, task, format!("Failed to create reader: {}", e));
            return Err(
                WorkerLoadError::LoadError(format!("Failed to create reader: {}", e)).into(),
            );
        }
    };

    // Create writer to Curvine filesystem
    let mtime = reader.mtime();
    let writer = match external_client
        .create_curvine_writer(mtime, task.ttl_ms, task.ttl_action)
        .await
    {
        Ok(writer) => writer,
        Err(e) => {
            update_task_failure(tasks, task, format!("Failed to create writer: {}", e));
            return Err(
                WorkerLoadError::LoadError(format!("Failed to create writer: {}", e)).into(),
            );
        }
    };

    // Create progress callback and perform transfer
    let progress_callback =
        create_progress_callback(task, tasks.clone(), fs_client.clone(), exec_config);
    do_transfer(
        task,
        tasks,
        fs_client,
        exec_config,
        reader,
        writer,
        progress_callback,
    )
    .await
}

/// Helper function to update task state on failure
fn update_task_failure(
    tasks: &Arc<DashMap<String, LoadTask>>,
    task: &LoadTask,
    error_message: String,
) {
    if let Some(mut task_ref) = tasks.get_mut(&task.task_id) {
        task_ref.update_state(LoadState::Failed, Some(error_message));
    }
}

/// Perform file transfer between reader and writer with progress tracking
async fn do_transfer<R, W, F>(
    task: &LoadTask,
    tasks: &Arc<DashMap<String, LoadTask>>,
    fs_client: &Arc<FsClient>,
    exec_config: &TaskExecutionConfig,
    reader: R,
    writer: W,
    progress_callback: F,
) -> CommonResult<()>
where
    F: Fn(u64, u64) -> bool + Sync + Send + 'static,
    R: AsyncChunkReader + Send + 'static,
    W: AsyncChunkWriter + Send + 'static,
{
    // Create buffer file transfer with progress callback
    let transfer = BufferFileTransfer::new(
        reader,
        writer,
        exec_config.default_chunk_size,
        exec_config.default_buffer_size,
    )
    .with_progress_callback(progress_callback);

    // Execute transfer and handle result
    match transfer.do_transfer().await {
        Ok(transferred) => {
            if let Some(mut task_ref) = tasks.get_mut(&task.task_id) {
                task_ref.update_progress(
                    transferred.0,
                    Some("Task completed successfully".to_string()),
                );

                let task_clone = task_ref.clone();
                let fs_client = Arc::clone(fs_client);

                tokio::spawn(async move {
                    report_task_status_to_master(fs_client, &task_clone, 0).await;
                });
            }

            // Remove completed task from task map
            tasks.remove(&task.task_id);

            info!(
                "Load task completed: job_id={}, task_id={}, path={}",
                task.job_id, task.task_id, task.path
            );
            Ok(())
        }
        Err(e) => {
            if let Some(mut task_ref) = tasks.get_mut(&task.task_id) {
                task_ref.update_state(
                    LoadState::Failed,
                    Some(format!("Failed to transfer file: {}", e)),
                );

                let task_clone = task_ref.clone();
                let fs_client = Arc::clone(fs_client);

                tokio::spawn(async move {
                    report_task_status_to_master(fs_client, &task_clone, 0).await;
                });
            }

            error!(
                "Load task failed: job_id={}, task_id={}, path={}, error={}",
                task.job_id, task.task_id, task.path, e
            );
            Err(WorkerLoadError::LoadError(format!("Failed to transfer file: {}", e)).into())
        }
    }
}

/// Create a progress callback function for tracking and reporting transfer progress
fn create_progress_callback(
    task: &LoadTask,
    tasks: Arc<DashMap<String, LoadTask>>,
    fs_client: Arc<FsClient>,
    exec_config: &TaskExecutionConfig,
) -> impl Fn(u64, u64) -> bool + Send + 'static {
    let task_id = task.task_id.clone();
    let status_report_interval_ms = exec_config.status_report_interval_ms;
    let last_master_report_time = Arc::new(std::sync::Mutex::new(Instant::now()));

    move |transferred_size, total_size| {
        let now = Instant::now();

        let elapsed_since_last_master_report = {
            let last_time = *last_master_report_time.lock().unwrap();
            now.duration_since(last_time)
        };

        if let Some(mut task) = tasks.get_mut(&task_id) {
            // Update total size if not set yet
            if task.total_size == 0 && total_size > 0 {
                task.set_total_size(total_size);
            }

            // Update progress
            task.update_progress(transferred_size, Some("Task is loading.".to_string()));

            // Report to master at specified intervals
            if elapsed_since_last_master_report.as_millis() as u64 >= status_report_interval_ms {
                let task_clone = task.clone();
                let fs_client = Arc::clone(&fs_client);

                tokio::spawn(async move {
                    report_task_status_to_master(fs_client, &task_clone, 0).await;
                });

                *last_master_report_time.lock().unwrap() = now;
            }

            // Continue transfer unless task is canceled
            task.state != LoadState::Canceled
        } else {
            false
        }
    }
}

/// Report task status to master node
async fn report_task_status_to_master(fs_client: Arc<FsClient>, task: &LoadTask, worker_id: i32) {
    // Build metrics information
    let metrics = Some(LoadMetrics {
        job_id: task.job_id.clone(),
        task_id: task.task_id.clone(),
        path: task.path.clone(),
        target_path: task.target_path.clone(),
        total_size: if task.total_size > 0 {
            Some(task.total_size as i64)
        } else {
            None
        },
        loaded_size: Some(task.loaded_size as i64),
        create_time: Some(task.create_time.timestamp_millis()),
        update_time: Some(task.update_time.timestamp_millis()),
        expire_time: None,
    });

    // Build report request
    let report_request = LoadTaskReportRequest {
        job_id: task.job_id.clone(),
        state: task.state as i32,
        worker_id,
        metrics,
        message: if task.message.is_empty() {
            None
        } else {
            Some(task.message.clone())
        },
    };

    // Send report to master
    match fs_client
        .rpc::<LoadTaskReportRequest, LoadTaskReportResponse>(
            RpcCode::ReportLoadTask,
            report_request,
        )
        .await
    {
        Ok(_) => {
            debug!("Reported task status to master successfully");
        }
        Err(e) => {
            error!("Failed to report task status to master: {}", e);
        }
    }
}
