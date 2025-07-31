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

use crate::worker::load::load_task::{
    LoadTask, TaskExecutionConfig, TaskOperation, WorkerLoadError,
};
use crate::worker::load::task_processor::TaskProcessor;
use curvine_client::file::{FsClient, FsContext};
use curvine_common::conf::ClusterConf;
use curvine_common::proto::{
    CancelLoadRequest, CancelLoadResponse, LoadState, LoadTaskRequest, LoadTaskResponse,
};
use dashmap::DashMap;
use log::{error, info};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Worker loading processor
///
/// Responsible for receiving load tasks from Master on the Worker node and executing them
///
#[derive(Clone)]
pub struct FileLoadService {
    /// Task storage
    tasks: Arc<DashMap<String, LoadTask>>,
    ///T ask sending channel
    task_tx: mpsc::Sender<TaskOperation>,
    /// Is it running
    running: Arc<std::sync::RwLock<bool>>,
    /// Task execution configuration
    exxec_config: TaskExecutionConfig,
    /// File system client
    fs_client: Arc<FsClient>,
    /// Tokio runtime
    runtime: Arc<Runtime>,
}

impl FileLoadService {
    /// Create a new Worker load processor
    pub fn new(fs_context: Arc<FsContext>, runtime: Arc<Runtime>) -> Self {
        let (task_tx, _) = mpsc::channel(100);
        let fs_client = FsClient::new(fs_context.clone());

        Self {
            tasks: Arc::new(DashMap::new()),
            task_tx,
            running: Arc::new(std::sync::RwLock::new(false)),
            exxec_config: TaskExecutionConfig::default(),
            fs_client: Arc::new(fs_client),
            runtime,
        }
    }

    /// Create a new Worker load processor from cluster configuration
    pub fn from_cluster_conf(
        fs_context: Arc<FsContext>,
        runtime: Arc<Runtime>,
        conf: &ClusterConf,
    ) -> Self {
        let (task_tx, _) = mpsc::channel(100);
        let fs_client = FsClient::new(fs_context.clone());

        Self {
            tasks: Arc::new(DashMap::new()),
            task_tx,
            running: Arc::new(std::sync::RwLock::new(false)),
            exxec_config: TaskExecutionConfig::from_cluster_conf(conf),
            fs_client: Arc::new(fs_client),
            runtime,
        }
    }

    /// Start the load processor
    pub fn start(&mut self) -> CommonResult<()> {
        let mut running = self.running.write().unwrap();
        if *running {
            info!("WorkerLoadHandler is already running");
            return Ok(());
        }
        *running = true;

        // Create a new channel
        let (tx, rx) = mpsc::channel(100);
        self.task_tx = tx;

        // Clone the necessary state
        let tasks = Arc::clone(&self.tasks);
        let running = Arc::clone(&self.running);
        let fs_client = Arc::clone(&self.fs_client);
        let exec_config = self.exxec_config.clone();
        let rt = Arc::clone(&self.runtime);

        self.runtime.spawn(async move {
            // Create and run task processor
            let processor = TaskProcessor::new(tasks, rx, running, fs_client, exec_config, rt);
            processor.run().await;
        });

        info!("WorkerLoadHandler started");
        Ok(())
    }

    /// Handle load task requests from Master
    pub async fn handle_load_task(
        &self,
        request: LoadTaskRequest,
    ) -> CommonResult<LoadTaskResponse> {
        info!(
            "Received load task request: path={}, target={}",
            request.source_path, request.target_path
        );

        let task = LoadTask::new(
            request.job_id.clone(),
            request.source_path.clone(),
            request.target_path.clone(),
            request.ttl_ms,
            request.ttl_action,
        );

        match self.submit_task(task.clone()).await {
            Ok(_) => {
                let response = LoadTaskResponse {
                    job_id: request.job_id.clone(),
                    task_id: task.task_id.clone(),
                    target_path: request.target_path.clone(),
                    message: Some("Task accepted".to_string()),
                };
                info!(
                    "Load task accepted: job_id={}, task_id={}",
                    request.job_id,
                    task.task_id.clone()
                );
                Ok(response)
            }
            Err(e) => {
                let error_message = match e.downcast_ref::<WorkerLoadError>() {
                    Some(WorkerLoadError::TaskAlreadyExists(_)) => {
                        "Task already exists".to_string()
                    }
                    Some(WorkerLoadError::LoadError(msg)) => {
                        format!("Failed to submit task: {}", msg)
                    }
                    _ => "Unknown error occurred".to_string(),
                };

                let response = LoadTaskResponse {
                    job_id: request.job_id.clone(),
                    task_id: task.task_id.clone(),
                    target_path: request.target_path.clone(),
                    message: Some(error_message),
                };
                error!(
                    "Failed to submit task: job_id={}, task_id={}, error={}",
                    request.job_id,
                    task.task_id.clone(),
                    e
                );
                Ok(response)
            }
        }
    }

    /// Stop loading the processor
    pub fn stop(&mut self) -> CommonResult<()> {
        {
            let mut running = self.running.write().unwrap();
            if !*running {
                info!("WorkerLoadHandler is already stopped");
                return Ok(());
            }
            *running = false;
        }
        // Recreate the channel
        let (task_tx, _) = mpsc::channel(100);
        self.task_tx = task_tx;

        info!("WorkerLoadHandler stopped");
        Ok(())
    }

    /// Submit tasks to the processing queue
    async fn submit_task(&self, task: LoadTask) -> CommonResult<()> {
        if self.tasks.contains_key(&task.task_id) {
            return Err(WorkerLoadError::TaskAlreadyExists(task.task_id).into());
        }
        self.tasks.insert(task.task_id.clone(), task.clone());

        if let Err(e) = self.task_tx.send(TaskOperation::Submit(task.clone())).await {
            error!("Failed to submit task to queue: {}", e);

            if let Some(mut task) = self.tasks.get_mut(&task.task_id) {
                task.update_state(
                    LoadState::Failed,
                    Some(format!("Failed to submit task to queue: {}", e)),
                );
            }

            return Err(WorkerLoadError::LoadError(format!(
                "Failed to submit task to queue: {}",
                e
            ))
            .into());
        }

        Ok(())
    }

    /// Cancel the loading task
    pub async fn cancel_load_task(
        &self,
        request: CancelLoadRequest,
    ) -> CommonResult<CancelLoadResponse> {
        let job_id = request.job_id.clone();
        info!("Canceling load tasks for job: {}", job_id);

        let mut canceled_count = 0;
        let mut tasks_to_cancel = Vec::new();

        for task in self.tasks.iter() {
            if task.job_id == job_id && task.state != LoadState::Canceled {
                tasks_to_cancel.push(task.task_id.clone());
            }
        }

        for task_id in tasks_to_cancel {
            if let Err(e) = self
                .task_tx
                .send(TaskOperation::Cancel(task_id.clone()))
                .await
            {
                error!(
                    "Failed to send cancel operation for task {}: {}",
                    task_id, e
                );
                continue;
            }
            canceled_count += 1;
        }

        let success = canceled_count > 0;
        let message = if success {
            format!(
                "Successfully canceled {} tasks for job {}",
                canceled_count, job_id
            )
        } else {
            format!("No active tasks found for job {}", job_id)
        };

        Ok(CancelLoadResponse {
            success,
            message: Some(message),
        })
    }

    /// Get task status
    pub async fn get_task_status(&self, job_id: String) -> CommonResult<Option<LoadTask>> {
        let (resp_tx, mut resp_rx) = mpsc::channel(1);

        self.task_tx
            .send(TaskOperation::GetStatus(job_id, resp_tx))
            .await
            .map_err(|e| {
                std::io::Error::other(
                    format!("Failed to send status request: {}", e),
                )
            })?;

        Ok(resp_rx.recv().await.unwrap_or(None))
    }

    /// Get all tasks
    pub async fn get_all_tasks(&self) -> CommonResult<Vec<LoadTask>> {
        let (resp_tx, mut resp_rx) = mpsc::channel(1);

        self.task_tx
            .send(TaskOperation::GetAll(resp_tx))
            .await
            .map_err(|e| {
                std::io::Error::other(
                    format!("Failed to send get all request: {}", e),
                )
            })?;

        Ok(resp_rx.recv().await.unwrap_or_else(Vec::new))
    }
}
