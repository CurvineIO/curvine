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
use crate::worker::task::load_task_runner::LoadTaskRunner;
use crate::worker::task::{TaskContext, TaskStore};
use curvine_client::file::{CurvineFileSystem, FsContext};
use curvine_common::conf::ClusterConf;
use curvine_common::state::{JobTaskState, LoadTaskInfo};
use curvine_common::FsResult;
use dashmap::mapref::entry::Entry;
use log::{debug, info, warn};
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct TaskManager {
    rt: Arc<Runtime>,
    fs: CurvineFileSystem,
    tasks: TaskStore,
    factory: Arc<UfsFactory>,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
    worker_task_semaphore: Arc<Semaphore>,
}

impl TaskManager {
    /// Creates a new TaskManager with an existing runtime.
    ///
    /// This method initializes a task manager that handles load tasks execution
    /// with an external async runtime, providing better resource control and
    /// allowing runtime sharing across components.
    ///
    /// # Arguments
    ///
    /// * `rt` - An existing Arc-wrapped Runtime for async task execution
    /// * `conf` - The cluster configuration containing job and client settings
    ///
    /// # Returns
    ///
    /// Returns `FsResult<Self>` containing the initialized TaskManager or an error
    /// if filesystem initialization fails or configuration is invalid.
    ///
    /// # Behavior
    ///
    /// - Modifies client hostname to "localhost" to prevent local write priority
    /// - This ensures data distribution across all workers instead of local bias
    /// - Initializes filesystem client with the modified configuration
    /// - Sets up task store and timing configurations from job settings
    /// - **Concurrency Control**: Uses a Semaphore to limit concurrent load tasks
    ///   based on `conf.job.load_task_concurrency_limit` to prevent excessive
    ///   bandwidth and resource consumption during data copy operations.
    ///
    /// # Example Configuration
    ///
    /// ```toml
    /// [job]
    /// # Limit concurrent load tasks to prevent resource exhaustion
    /// worker_max_concurrent_tasks = 10
    /// ```
    pub fn with_rt(rt: Arc<Runtime>, conf: &ClusterConf) -> FsResult<Self> {
        let mut new_conf = conf.clone();
        new_conf.client.hostname = "localhost".to_string();

        let fs = CurvineFileSystem::with_rt(new_conf, rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let worker_task_semaphore = Arc::new(Semaphore::new(conf.job.worker_max_concurrent_tasks));
        let mgr = Self {
            rt,
            fs,
            tasks: TaskStore::new(),
            factory,
            progress_interval_ms: conf.job.task_report_interval.as_millis() as u64,
            task_timeout_ms: conf.job.task_timeout.as_millis() as u64,
            worker_task_semaphore,
        };

        Ok(mgr)
    }

    /// Submits a load task for execution with concurrency control.
    ///
    /// This method queues a data copy task to be executed by the TaskManager.
    /// The execution is controlled by a Semaphore to prevent too many concurrent
    /// tasks from overwhelming the system's bandwidth and resources.
    ///
    /// # Arguments
    ///
    /// * `task` - The LoadTaskInfo containing source path, target path, and job configuration
    ///
    /// # Returns
    ///
    /// Returns `FsResult<()>` indicating whether the task was successfully submitted.
    /// Note: This only indicates submission success, not task completion.
    ///
    /// # Concurrency Control
    ///
    /// - Tasks wait to acquire a permit from the load_task_semaphore before execution
    /// - Maximum concurrent tasks is limited by `conf.job.load_task_concurrency_limit`
    /// - Permits are automatically released when tasks complete or fail
    /// - This prevents excessive bandwidth usage during bulk data operations
    ///
    /// # Behavior
    ///
    /// 1. If a task with the same `task_id` is already in the store, it is
    ///    treated as **superseded**: its `TaskContext` is flipped to
    ///    `Canceled` (the running `LoadTaskRunner` observes this at the
    ///    next chunk boundary via `TaskContext::is_cancel`) and the map
    ///    entry is replaced with a fresh context in a single shard-locked
    ///    operation. This is intentional: silently de-duping would leave
    ///    the new dispatcher's `JobContext` without a reporter and the
    ///    master would hang until `ufs_copy_timeout`.
    /// 2. If no task exists for the `task_id`, the new context is inserted.
    /// 3. Spawns an async task that:
    ///    - Acquires a semaphore permit (blocks if limit reached)
    ///    - Executes `LoadTaskRunner::run`
    ///    - Automatically releases the permit on completion
    ///    - Removes the map entry **only if it still points at its own
    ///      context** (a later `submit_task` may have superseded it)
    pub fn submit_task(&self, task: LoadTaskInfo) -> FsResult<()> {
        let task_id = task.task_id.clone();
        let context = Arc::new(TaskContext::new(task));

        match self.tasks.entry(task_id.clone()) {
            Entry::Occupied(mut occ) => {
                let old = occ.insert(context.clone());
                old.update_state(JobTaskState::Canceled, "superseded by new submit");
                warn!(
                    "cancel duplicate task {} (source_path={})",
                    old.info.task_id, old.info.source_path
                );
            }

            Entry::Vacant(vac) => {
                vac.insert(context.clone());
            }
        }
        info!(
            "submit task {} {}",
            context.info.task_id, context.info.source_path
        );

        let runner = LoadTaskRunner::new(
            context.clone(),
            self.fs.clone(),
            self.factory.clone(),
            self.progress_interval_ms,
            self.task_timeout_ms,
        );

        let tasks = self.tasks.clone();
        let semaphore = self.worker_task_semaphore.clone();
        let context_this = context.clone();

        // Spawn task with concurrency control
        self.rt.spawn(async move {
            match semaphore.acquire().await {
                Ok(permit) => {
                    runner.run().await;
                    drop(permit);
                }
                Err(e) => {
                    log::error!("task {} failed to acquire permit: {}", task_id, e);
                }
            }

            let _ = tasks.remove_if(&task_id, |_, ctx| Arc::ptr_eq(ctx, &context_this));
        });

        Ok(())
    }

    pub fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let all_task = self.tasks.cancel(job_id);

        debug!(
            "Successfully canceled {} tasks for job {}",
            all_task.len(),
            job_id
        );
        Ok(())
    }

    pub fn get_fs_context(&self) -> Arc<FsContext> {
        self.fs.fs_context()
    }

    pub fn available_worker_task_permits(&self) -> usize {
        self.worker_task_semaphore.available_permits()
    }
}
