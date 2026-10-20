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
use crate::worker::task::task_progress_reporter::{TaskProgressReporter, TransferProgressBatcher};
use crate::worker::task::{
    reap_finished_tasks, record_metric, record_task_outcome, TaskContext, TaskStore,
    TerminalRetention, WorkerTransferMetrics,
};
use curvine_client_core::file::{CurvineFileSystem, FsContext};
use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_job_client::TransferClient;
use curvine_model::{JobTaskProgress, JobTaskState, LoadTaskInfo, TransferTaskReportInfo};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use dashmap::mapref::entry::Entry;
use log::{debug, info, warn};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

pub struct TaskManager {
    rt: Arc<Runtime>,
    fs: CurvineFileSystem,
    tasks: TaskStore,
    factory: Arc<UfsFactory>,
    transfer_client: Option<TransferClient>,
    transfer_reporters: Mutex<HashMap<Vec<String>, Weak<TransferProgressBatcher>>>,
    worker_session_id: String,
    worker_task_semaphore: Arc<Semaphore>,
    /// Serializes semaphore reconciliation so concurrent submits cannot
    /// over-add or double-retire permits while resizing.
    permit_lock: Arc<Mutex<()>>,
    /// Runtime ceiling for concurrent tasks. The semaphore is reconciled to
    /// this target by `update_worker_max_concurrent_tasks`.
    worker_max_concurrent_tasks: Arc<AtomicUsize>,
    /// Permits currently in circulation (available + held). Tracked so a
    /// reduced target retires surplus permits instead of returning them.
    issued_task_permits: Arc<AtomicUsize>,
    /// Runtime per-task timeout, read at submit; running tasks keep the
    /// value captured when they were submitted.
    task_timeout_ms: Arc<AtomicU64>,
    /// Runtime load-task progress report interval, read at submit.
    task_report_interval_ms: Arc<AtomicU64>,
    /// Bounded retention for finished task contexts kept for a stale probe.
    terminal_retention: TerminalRetention,
    /// Fixed transfer-report interval from conf (0 = follow the job one).
    transfer_report_interval_ms: u64,
    /// Runtime in-flight byte budget. It starts at
    /// `TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES` and can be safely
    /// updated without restarting this worker.
    inflight_budget_bytes: Arc<AtomicI64>,
    inflight_bytes: Arc<AtomicI64>,
}

pub struct TaskSubmitResult {
    pub accepted: bool,
    pub reject_reason: String,
}

/// Prefix marking a capacity-class rejection (design §5.2): the scheduler must
/// NOT evict the worker session on these; only session/capability-class
/// rejections do.
pub const WORKER_AT_CAPACITY_REJECT_PREFIX: &str = "worker_at_capacity";

/// RAII release of the in-flight byte budget (design §3.2): dropping the guard
/// releases the admitted bytes exactly once, even when the runner panics.
struct InflightByteGuard {
    inflight_bytes: Arc<AtomicI64>,
    bytes: i64,
}

/// Atomically reserves `estimated_bytes` against one snapshot of the runtime
/// budget. A concurrent configuration update affects subsequent admissions;
/// work already admitted is never interrupted.
fn admit_inflight_bytes(
    inflight_bytes: &AtomicI64,
    budget_bytes: i64,
    estimated_bytes: i64,
) -> Result<(), String> {
    let oversize = estimated_bytes > budget_bytes;
    loop {
        let current = inflight_bytes.load(Ordering::Acquire);
        let Some(next) = current.checked_add(estimated_bytes) else {
            return Err(format!(
                "{}: inflight bytes {} + task {} would overflow budget accounting",
                WORKER_AT_CAPACITY_REJECT_PREFIX, current, estimated_bytes
            ));
        };
        if oversize {
            if current != 0 {
                return Err(format!(
                    "{}: inflight bytes {} + oversized task {} would exceed budget {}",
                    WORKER_AT_CAPACITY_REJECT_PREFIX, current, estimated_bytes, budget_bytes
                ));
            }
        } else if next > budget_bytes {
            return Err(format!(
                "{}: inflight bytes {} + task {} would exceed budget {}",
                WORKER_AT_CAPACITY_REJECT_PREFIX, current, estimated_bytes, budget_bytes
            ));
        }
        if inflight_bytes
            .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(());
        }
    }
}

impl Drop for InflightByteGuard {
    fn drop(&mut self) {
        self.inflight_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl TaskSubmitResult {
    fn accepted() -> Self {
        Self {
            accepted: true,
            reject_reason: String::new(),
        }
    }

    fn rejected(reason: impl Into<String>) -> Self {
        Self {
            accepted: false,
            reject_reason: reason.into(),
        }
    }
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
    /// - Initializes the filesystem client with the worker's own configuration;
    ///   `client.hostname` stays equal to `worker.hostname` so the master
    ///   reports the local worker as the client host and block writes can take
    ///   the short-circuit path
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
    pub fn with_rt(
        rt: Arc<Runtime>,
        conf: &ClusterConf,
        worker_session_id: impl Into<String>,
    ) -> FsResult<Self> {
        // Do not override client.hostname here: it must stay equal to
        // worker.hostname on this node, otherwise `is_local_worker` never
        // matches and every chunk loops through the worker RPC port instead of
        // short-circuiting to the local disk.
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let transfer_client = TransferClient::with_context(&fs.fs_context()).ok();
        let worker_task_semaphore = Arc::new(Semaphore::new(conf.job.worker_max_concurrent_tasks));
        let _ = WorkerTransferMetrics::get()?;
        record_metric(|metrics| {
            metrics.set_permits_max(conf.job.worker_max_concurrent_tasks);
            metrics.set_permits_available(conf.job.worker_max_concurrent_tasks);
        });
        let transfer_progress_interval = if conf.transfer.task_report_interval.is_zero() {
            conf.job.task_report_interval
        } else {
            conf.job
                .task_report_interval
                .min(conf.transfer.task_report_interval)
        };
        let terminal_retention = TerminalRetention::new(conf.transfer.worker_terminal_retention);
        let mgr = Self {
            rt,
            fs,
            tasks: TaskStore::new(),
            factory,
            transfer_client,
            transfer_reporters: Mutex::new(HashMap::new()),
            worker_session_id: worker_session_id.into(),
            worker_task_semaphore,
            permit_lock: Arc::new(Mutex::new(())),
            worker_max_concurrent_tasks: Arc::new(AtomicUsize::new(
                conf.job.worker_max_concurrent_tasks,
            )),
            issued_task_permits: Arc::new(AtomicUsize::new(conf.job.worker_max_concurrent_tasks)),
            task_timeout_ms: Arc::new(AtomicU64::new(conf.job.task_timeout.as_millis() as u64)),
            task_report_interval_ms: Arc::new(AtomicU64::new(
                conf.job.task_report_interval.as_millis() as u64,
            )),
            transfer_report_interval_ms: transfer_progress_interval.as_millis() as u64,
            terminal_retention,
            inflight_budget_bytes: Arc::new(AtomicI64::new(
                curvine_config::TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
            )),
            inflight_bytes: Arc::new(AtomicI64::new(0)),
        };
        record_metric(|metrics| {
            metrics.set_inflight_budget_bytes(mgr.transfer_inflight_budget_bytes())
        });
        mgr.start_terminal_reaper();

        Ok(mgr)
    }

    /// Periodically releases finished contexts whose stale-probe window closed.
    ///
    /// The sweep interval is derived from the retention so it keeps the oldest
    /// retained context at most one sweep past its window; the floor keeps a very
    /// small window from spinning.
    fn start_terminal_reaper(&self) {
        let tasks = self.tasks.clone();
        let retention = self.terminal_retention.clone();
        let sweep = (retention.ttl() / 10)
            .max(Duration::from_secs(10))
            .min(Duration::from_secs(60));
        self.rt.spawn(async move {
            loop {
                tokio::time::sleep(sweep).await;
                reap_finished_tasks(&tasks, &retention);
            }
        });
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
    pub fn submit_task(&self, task: LoadTaskInfo) -> FsResult<TaskSubmitResult> {
        if let Some(report) = &task.transfer_report {
            if report.report_endpoints.is_empty()
                && report.report_target.is_empty()
                && self.transfer_client.is_none()
            {
                record_metric(|metrics| metrics.inc_submit_rejected("no_report_endpoint"));
                return Ok(TaskSubmitResult::rejected(format!(
                    "Reject transfer task {} because no Transfer report endpoint is available",
                    task.task_id
                )));
            }
            if report.worker_session_id != self.worker_session_id {
                record_metric(|metrics| metrics.inc_submit_rejected("stale_session"));
                return Ok(TaskSubmitResult::rejected(format!(
                    "Reject transfer task {} for stale worker session {}, current session {}",
                    task.task_id, report.worker_session_id, self.worker_session_id
                )));
            }
        }

        let task_id = task.task_id.clone();
        let source_path = task.source_path.clone();
        let context = Arc::new(TaskContext::new(task));
        // Byte-budget admit (design §3.2): reject synchronously before the task
        // enters the local queue, so a saturated worker never silently queues
        // (the incident's "queued 112s" failure mode). Unknown sizes count at
        // zero here; the scheduler-side default estimate already gates them.
        let estimated_bytes = context.info.estimated_size.max(0);
        let budget_bytes = self.transfer_inflight_budget_bytes();
        let inflight_bytes = self.inflight_bytes.clone();
        let reserve_inflight = || -> Result<Option<InflightByteGuard>, TaskSubmitResult> {
            if budget_bytes <= 0 {
                return Ok(None);
            }
            match admit_inflight_bytes(&inflight_bytes, budget_bytes, estimated_bytes) {
                Ok(()) => Ok(Some(InflightByteGuard {
                    inflight_bytes: inflight_bytes.clone(),
                    bytes: estimated_bytes,
                })),
                Err(reason) => {
                    record_metric(|metrics| metrics.inc_submit_rejected("inflight_budget"));
                    Err(TaskSubmitResult::rejected(reason))
                }
            }
        };

        // Reserve before replacing an existing context. A capacity rejection
        // must leave the current task runnable; otherwise a rejected retry
        // could cancel it and strand a Loading map entry without a runner.
        let byte_guard = match self.tasks.entry(task_id.clone()) {
            Entry::Occupied(mut occ) => {
                if let Some(new_report) = &context.info.transfer_report {
                    let old_report = occ.get().info.transfer_report.as_ref();
                    if let Some(old_report) = old_report {
                        if old_report.run_id == new_report.run_id
                            && old_report.attempt_id == new_report.attempt_id
                        {
                            debug!(
                                "ignore duplicate transfer task submit: job={} run={} task={} attempt={} source={} target={}",
                                context.info.job.job_id,
                                new_report.run_id,
                                task_id,
                                new_report.attempt_id,
                                context.info.source_path,
                                context.info.target_path,
                            );
                            record_metric(|metrics| metrics.inc_submit_duplicate());
                            return Ok(TaskSubmitResult::accepted());
                        }
                        if old_report.run_id > new_report.run_id
                            || (old_report.run_id == new_report.run_id
                                && old_report.attempt_id > new_report.attempt_id)
                        {
                            record_metric(|metrics| metrics.inc_submit_rejected("stale_attempt"));
                            return Ok(TaskSubmitResult::rejected(format!(
                                "Reject stale transfer task {} run {} attempt {}, current run {} attempt {}",
                                task_id,
                                new_report.run_id,
                                new_report.attempt_id,
                                old_report.run_id,
                                old_report.attempt_id
                            )));
                        }
                    }
                }

                let byte_guard = match reserve_inflight() {
                    Ok(guard) => guard,
                    Err(result) => return Ok(result),
                };
                let old = occ.insert(context.clone());
                old.set_canceled("superseded by new submit");
                warn!(
                    "cancel duplicate task {} (source_path={})",
                    old.info.task_id, old.info.source_path
                );
                byte_guard
            }

            Entry::Vacant(vac) => {
                let byte_guard = match reserve_inflight() {
                    Ok(guard) => guard,
                    Err(result) => return Ok(result),
                };
                vac.insert(context.clone());
                byte_guard
            }
        };
        record_metric(|metrics| metrics.inc_submit_accepted());
        record_metric(|metrics| metrics.set_active(self.tasks.len()));
        if let Some(report) = &context.info.transfer_report {
            info!(
                "accepted transfer task: job={} run={} task={} attempt={} source={} target={}",
                context.info.job.job_id,
                report.run_id,
                context.info.task_id,
                report.attempt_id,
                context.info.source_path,
                context.info.target_path,
            );
        } else {
            info!(
                "submit task {} {}",
                context.info.task_id, context.info.source_path
            );
        }

        // The scheduler treats an accepted task as running while it waits for
        // this worker's local concurrency permit.
        context.note_accepted();
        context.update_state(JobTaskState::Loading, "task accepted and queued");

        let progress_reporter = self.task_progress_reporter(&context);
        let progress_interval_ms = if context.info.transfer_report.is_some() {
            self.transfer_report_interval_ms
        } else {
            self.task_report_interval_ms.load(Ordering::Acquire)
        };
        let runner = LoadTaskRunner::new(
            context.clone(),
            self.fs.clone(),
            self.factory.clone(),
            progress_reporter,
            progress_interval_ms,
            self.task_timeout_ms.load(Ordering::Acquire),
        );

        let tasks = self.tasks.clone();
        let semaphore = self.worker_task_semaphore.clone();
        let context_this = context.clone();
        let permit_lock = self.permit_lock.clone();
        let issued_permits = self.issued_task_permits.clone();
        let max_tasks = self.worker_max_concurrent_tasks.clone();

        // Spawn task with concurrency control. The byte admission is released
        // together with the semaphore permit when the runner exits (success,
        // failure, or cancellation) - a RAII-style balance at task end.
        self.rt.spawn(async move {
            let started = Instant::now();
            let mut remove_task = true;
            record_metric(|metrics| metrics.inc_queued());
            match semaphore.acquire().await {
                Ok(permit) => {
                    record_metric(|metrics| {
                        metrics.dec_queued();
                        metrics.inc_running();
                        metrics.observe_queued("started", started.elapsed().as_micros());
                        metrics.set_permits_available(semaphore.available_permits());
                    });
                    remove_task = runner.run().await;
                    drop(permit);
                    // Under a pending shrink the just-returned permit is
                    // surplus capacity: forget it (serialized with updates).
                    {
                        let _guard = permit_lock.lock();
                        if issued_permits.load(Ordering::Acquire)
                            > max_tasks.load(Ordering::Acquire)
                        {
                            if let Ok(surplus) = semaphore.try_acquire() {
                                surplus.forget();
                                issued_permits.fetch_sub(1, Ordering::AcqRel);
                            }
                        }
                    }
                    record_metric(|metrics| {
                        metrics.dec_running();
                        metrics.set_permits_available(semaphore.available_permits());
                    });
                }
                Err(e) => {
                    record_metric(|metrics| {
                        metrics.dec_queued();
                        metrics.observe_queued("failed", started.elapsed().as_micros());
                        metrics.set_permits_available(semaphore.available_permits());
                    });
                    record_task_outcome(
                        &source_path,
                        JobTaskState::Failed,
                        started.elapsed().as_micros(),
                        0,
                        0,
                        0,
                        Some("permit_acquire_failed"),
                    );
                    log::error!("task {} failed to acquire permit: {}", task_id, e);
                }
            }
            // `byte_guard` drops here (RAII), releasing the admitted bytes even
            // if `runner.run()` panicked above.
            drop(byte_guard);

            if remove_task
                && tasks
                    .remove_if(&task_id, |_, ctx| Arc::ptr_eq(ctx, &context_this))
                    .is_some()
            {
                record_metric(|metrics| metrics.set_active(tasks.len()));
            }
        });

        Ok(TaskSubmitResult::accepted())
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

    pub fn query_transfer_task(
        &self,
        job_id: &str,
        task_id: &str,
        run_id: u64,
        attempt_id: u64,
        worker_session_id: &str,
    ) -> FsResult<Option<JobTaskProgress>> {
        let Some(task) = self.tasks.get(task_id) else {
            return Ok(None);
        };
        if task.info.job.job_id != job_id {
            return Ok(None);
        }
        if !transfer_report_matches(
            task.info.transfer_report.as_ref(),
            run_id,
            attempt_id,
            worker_session_id,
        ) {
            return Ok(None);
        }
        Ok(Some(task.progress()))
    }

    /// Current runtime Transfer byte-admission budget.
    pub fn transfer_inflight_budget_bytes(&self) -> i64 {
        self.inflight_budget_bytes.load(Ordering::Acquire)
    }

    /// Shared budget read by the worker heartbeat on every report.
    pub fn inflight_budget_bytes(&self) -> Arc<AtomicI64> {
        self.inflight_budget_bytes.clone()
    }

    /// Shared concurrency ceiling read by the worker heartbeat on every
    /// report; resizing it also propagates to the scheduler this way.
    pub fn worker_max_concurrent_tasks_shared(&self) -> Arc<AtomicUsize> {
        self.worker_max_concurrent_tasks.clone()
    }

    /// Updates the runtime Transfer byte-admission budget. Existing tasks keep
    /// running; new submissions observe the new value.
    pub fn update_transfer_inflight_budget_bytes(&self, budget_bytes: i64) -> FsResult<i64> {
        if budget_bytes <= 0 {
            return Err(FsError::common(
                "transfer_inflight_budget_bytes must be greater than zero",
            ));
        }
        let previous = self
            .inflight_budget_bytes
            .swap(budget_bytes, Ordering::AcqRel);
        record_metric(|metrics| metrics.set_inflight_budget_bytes(budget_bytes));
        info!(
            "updated worker transfer in-flight budget: previous={} current={}",
            previous, budget_bytes
        );
        Ok(previous)
    }

    pub fn get_fs_context(&self) -> Arc<FsContext> {
        self.fs.fs_context()
    }

    fn task_progress_reporter(&self, task: &TaskContext) -> Option<TaskProgressReporter> {
        let report_info = task.info.transfer_report.as_ref()?;
        let report_endpoints = if report_info.report_endpoints.is_empty() {
            if report_info.report_target.is_empty() {
                Vec::new()
            } else {
                vec![report_info.report_target.clone()]
            }
        } else {
            report_info.report_endpoints.clone()
        };
        let mut reporters = self.transfer_reporters.lock();
        let batcher = if let Some(batcher) =
            reporters.get(&report_endpoints).and_then(Weak::upgrade)
        {
            batcher
        } else {
            reporters.retain(|_, batcher| batcher.strong_count() > 0);
            let transfer_client = if report_endpoints.is_empty() {
                self.transfer_client.clone()
            } else {
                match TransferClient::with_report_endpoints(
                    &self.fs.fs_context(),
                    report_endpoints.clone(),
                ) {
                    Ok(client) => Some(client),
                    Err(err) => {
                        warn!(
                            "transfer task has invalid report endpoints {:?}: job={} run={} task={} attempt={} source={} target={} err={}",
                            report_endpoints,
                            task.info.job.job_id,
                            report_info.run_id,
                            task.info.task_id,
                            report_info.attempt_id,
                            task.info.source_path,
                            task.info.target_path,
                            err,
                        );
                        None
                    }
                }
            }?;
            let batcher = Arc::new(TransferProgressBatcher::new(
                self.rt.clone(),
                transfer_client,
                self.worker_max_concurrent_tasks.load(Ordering::Acquire),
                Duration::from_millis(self.transfer_report_interval_ms),
            ));
            reporters.insert(report_endpoints, Arc::downgrade(&batcher));
            batcher
        };
        Some(TaskProgressReporter::new(
            batcher,
            task.info.job.job_id.clone(),
            task.info.task_id.clone(),
            report_info.clone(),
            format!(
                "job={} run={} task={} attempt={} source={} target={}",
                task.info.job.job_id,
                report_info.run_id,
                task.info.task_id,
                report_info.attempt_id,
                task.info.source_path,
                task.info.target_path,
            ),
        ))
    }

    pub fn available_worker_task_permits(&self) -> usize {
        self.worker_task_semaphore.available_permits()
    }

    /// Current runtime per-task timeout in milliseconds.
    pub fn task_timeout_ms(&self) -> u64 {
        self.task_timeout_ms.load(Ordering::Acquire)
    }

    /// Updates the runtime per-task timeout. Running tasks keep the deadline
    /// captured at submit; new submissions observe the new value. Accepts
    /// durations like "30m" / "1h" via DurationUnit or raw milliseconds.
    pub fn update_task_timeout(&self, timeout_ms: u64) -> FsResult<u64> {
        if timeout_ms == 0 {
            return Err(FsError::common("task_timeout must be greater than zero"));
        }
        let previous = self.task_timeout_ms.swap(timeout_ms, Ordering::AcqRel);
        info!(
            "updated worker task timeout: previous={}ms current={}ms",
            previous, timeout_ms
        );
        Ok(previous)
    }

    /// Current runtime load-task progress report interval in milliseconds.
    pub fn task_report_interval_ms(&self) -> u64 {
        self.task_report_interval_ms.load(Ordering::Acquire)
    }

    /// Updates the runtime load-task progress report interval. Applies to
    /// tasks submitted afterwards; running tasks keep the old cadence.
    pub fn update_task_report_interval(&self, interval_ms: u64) -> FsResult<u64> {
        if interval_ms == 0 {
            return Err(FsError::common(
                "task_report_interval must be greater than zero",
            ));
        }
        let previous = self
            .task_report_interval_ms
            .swap(interval_ms, Ordering::AcqRel);
        info!(
            "updated worker task report interval: previous={}ms current={}ms",
            previous, interval_ms
        );
        Ok(previous)
    }

    /// Current runtime ceiling for concurrent transfer/load tasks.
    pub fn worker_max_concurrent_tasks(&self) -> usize {
        self.worker_max_concurrent_tasks.load(Ordering::Acquire)
    }

    /// Resizes the concurrency ceiling and reconciles the semaphore. Growing
    /// adds permits immediately; shrinking retires surplus permits as running
    /// tasks finish (holders keep running). Serialized so concurrent updates
    /// or submits cannot double-count permits.
    pub fn update_worker_max_concurrent_tasks(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(
                "worker_max_concurrent_tasks must be greater than zero",
            ));
        }
        let previous = self
            .worker_max_concurrent_tasks
            .swap(target, Ordering::AcqRel);
        {
            let _guard = self.permit_lock.lock();
            let issued = self.issued_task_permits.load(Ordering::Acquire);
            if target > issued {
                self.worker_task_semaphore.add_permits(target - issued);
                self.issued_task_permits.store(target, Ordering::Release);
            } else {
                // Retire surplus permits by forgetting them: a dropped permit
                // would return to the pool and undo the shrink. Permits held
                // by running tasks retire at task exit instead.
                while self.issued_task_permits.load(Ordering::Acquire) > target {
                    match self.worker_task_semaphore.clone().try_acquire_owned() {
                        Ok(permit) => {
                            permit.forget();
                            self.issued_task_permits.fetch_sub(1, Ordering::AcqRel);
                        }
                        // All remaining permits are held by running tasks.
                        Err(_) => break,
                    }
                }
            }
            record_metric(|metrics| {
                metrics.set_permits_max(target);
                metrics.set_permits_available(self.worker_task_semaphore.available_permits());
            });
        }
        info!(
            "updated worker max concurrent tasks: previous={} current={}",
            previous, target
        );
        Ok(previous)
    }
}

fn transfer_report_matches(
    report: Option<&TransferTaskReportInfo>,
    run_id: u64,
    attempt_id: u64,
    worker_session_id: &str,
) -> bool {
    matches!(
        report,
        Some(report)
            if report.run_id == run_id
                && report.attempt_id == attempt_id
                && report.worker_session_id == worker_session_id
    )
}

#[cfg(test)]
mod budget_tests {
    use super::{InflightByteGuard, WORKER_AT_CAPACITY_REJECT_PREFIX};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    #[test]
    fn capacity_reject_prefix_is_exact_and_stable() {
        // The scheduler classifies rejections by this prefix (design §5.2);
        // the classification test lives here so the contract stays adjacent
        // to the worker that emits it.
        let reason = format!(
            "{}: inflight bytes 100 + task 50 would exceed budget 128",
            WORKER_AT_CAPACITY_REJECT_PREFIX
        );
        assert!(reason.starts_with(WORKER_AT_CAPACITY_REJECT_PREFIX));
        assert!(!reason.starts_with("worker_session"));
        // Unrelated session-class rejections must NOT match the prefix.
        assert!(!"Reject transfer task x for stale worker session y"
            .starts_with(WORKER_AT_CAPACITY_REJECT_PREFIX));
    }

    #[test]
    fn byte_guard_releases_exactly_once_even_across_panic_path() {
        let bytes = Arc::new(AtomicI64::new(0));
        bytes.fetch_add(200, Ordering::Relaxed);
        // Two admissions; the first "runs" normally, the second panics while
        // holding its guard. Both guards must release exactly once, leaving
        // the ledger back at zero regardless of the unwind path.
        {
            let _guard = InflightByteGuard {
                inflight_bytes: bytes.clone(),
                bytes: 100,
            };
            assert_eq!(bytes.load(Ordering::Relaxed), 200);
            let result = std::panic::catch_unwind(|| {
                let _inner = InflightByteGuard {
                    inflight_bytes: bytes.clone(),
                    bytes: 100,
                };
                panic!("runner panicked");
            });
            assert!(result.is_err());
        }
        assert_eq!(bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn byte_guard_releases_zero_bytes_is_a_noop() {
        let bytes = Arc::new(AtomicI64::new(42));
        drop(InflightByteGuard {
            inflight_bytes: bytes.clone(),
            bytes: 0,
        });
        assert_eq!(bytes.load(Ordering::Relaxed), 42);
    }
}

#[cfg(test)]
#[path = "tests/task_manager_budget_test.rs"]
mod task_manager_budget_test;
