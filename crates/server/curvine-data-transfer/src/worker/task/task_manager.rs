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

use super::terminal_recovery::{PendingReport, RetainedTask, TerminalRecovery};
use super::terminal_report::{deliver_with_capacity, ReportOutcome, ReportPolicy};
use crate::common::UfsFactory;
use crate::worker::task::inflight_gate::{
    task_inflight_bytes, InflightBytesGate, InflightPermit, ProbePolicy,
};
use crate::worker::task::{
    JobExecutor, JobRegistry, ProgressReporter, ResourceClass, TaskContext, TaskEnvelope,
    TaskOutcome, TaskStore, TaskTerminalReport, TransferPlugin,
};
use curvine_client_core::file::{CurvineFileSystem, FsContext};
use curvine_config::{ClusterConf, JobConf};
use curvine_error::FsResult;
use curvine_metrics::{Gauge, GaugeVec, Metrics};
use curvine_model::{
    TransferKind, TransferTaskInfo, TransferTaskReportInfo, TransferTaskResult, WorkerTaskProgress,
    WorkerTaskState,
};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_transfer_client::TransferClient;
use dashmap::mapref::entry::Entry;
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static TASK_METRICS: Lazy<TaskMetrics> = Lazy::new(|| TaskMetrics {
    admitted: task_gauge(
        "worker_job_tasks_admitted",
        "Worker job tasks running or waiting for an execution permit",
    ),
    running: task_gauge(
        "worker_job_tasks_running",
        "Worker job tasks holding execution permits",
    ),
});

struct TaskMetrics {
    admitted: Option<GaugeVec>,
    running: Option<GaugeVec>,
}

fn task_gauge(name: &str, help: &str) -> Option<GaugeVec> {
    let get_or_create = || -> curvine_core_error::CommonResult<GaugeVec> {
        if Metrics::get(name).is_none() {
            Metrics::new_gauge_vec(name, help, &["task_type"])?;
        }
        // Read back the registered collector, not the newly allocated handle:
        // another caller may have registered the name in the meantime.
        let Some(metric) = Metrics::get(name) else {
            return curvine_core_error::err_box!("task gauge registration is missing");
        };
        if metric.boxed().desc()[0].variable_labels != ["task_type"] {
            return curvine_core_error::err_box!("expected a task_type label");
        }
        metric.try_into_gauge_vec()
    };
    match get_or_create() {
        Ok(gauge) => Some(gauge),
        Err(error) => {
            warn!("Worker task metric {name} is unavailable: {error}");
            None
        }
    }
}

struct InFlightMetric(Gauge);

impl InFlightMetric {
    fn new(metric: &GaugeVec, task_type: TransferKind) -> Self {
        let gauge = metric.with_label_values(&[task_type.as_str()]);
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for InFlightMetric {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Prices that fell back to the declared pipeline bytes (stat error, no UFS).
static INFLIGHT_PRICE_FALLBACK: std::sync::LazyLock<Option<curvine_metrics::Counter>> =
    std::sync::LazyLock::new(|| {
        curvine_metrics::Metrics::new_counter(
            "worker_load_inflight_price_fallback_total",
            "Transfer task prices that fell back to the declared pipeline bytes",
        )
        .ok()
    });

pub struct TaskManager {
    rt: Arc<Runtime>,
    fs: CurvineFileSystem,
    tasks: TaskStore,
    transfer_client: Option<TransferClient>,
    worker_session_id: String,
    worker_task_semaphore: Arc<Semaphore>,
    /// Final notifications have their own concurrency, independent of copy permits.
    terminal_report_semaphore: Arc<Semaphore>,
    /// Bounds active plus queued initial notifications.
    terminal_report_pending: Arc<Semaphore>,
    terminal_recovery: TerminalRecovery,
    retained_task_limit: usize,
    /// Tasks the worker has admitted and not finished: running, or waiting for
    /// a concurrency permit. Beyond `max_queued_tasks` new submissions are
    /// rejected with a retryable "queue full" response so the master paces its
    /// dispatch instead of growing this queue without bound. Counted separately
    /// from `tasks`, because a task context can outlive its runner (a failed
    /// final report keeps it for the scheduler probe) and must not hold a slot.
    admitted: Arc<AtomicUsize>,
    max_queued_tasks: usize,
    class_limits: HashMap<ResourceClass, ClassLimits>,
    metrics: &'static TaskMetrics,
    registry: Arc<JobRegistry>,
    /// Adaptive in-flight byte admission; `None` when disabled.
    inflight_gate: Option<Arc<InflightBytesGate>>,
    /// Bytes already fed into the gate's goodput, per task.
    sampled_bytes: Arc<Mutex<HashMap<String, i64>>>,
}

pub struct TaskSubmitResult {
    pub accepted: bool,
    /// The rejection is backpressure: the same task will be accepted later.
    pub retryable: bool,
    pub reject_reason: String,
}

impl TaskSubmitResult {
    fn retention_full(limit: usize) -> Self {
        Self {
            accepted: false,
            retryable: true,
            reject_reason: format!(
                "terminal result capacity full: {limit} admitted or unconfirmed tasks; retry after recovery"
            ),
        }
    }
    fn accepted() -> Self {
        Self {
            accepted: true,
            retryable: false,
            reject_reason: String::new(),
        }
    }

    fn rejected(reason: impl Into<String>) -> Self {
        Self {
            accepted: false,
            retryable: false,
            reject_reason: reason.into(),
        }
    }

    /// The worker is out of queue room. Nothing is wrong with the task, so the
    /// submitter should retry it instead of failing the job.
    fn queue_full(pending: usize, limit: usize) -> Self {
        Self {
            accepted: false,
            retryable: true,
            reject_reason: format!(
                "queue full: {} tasks pending (running + queued), limit {}",
                pending, limit
            ),
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
    /// - Modifies client hostname to "localhost" to prevent local write priority
    /// - This ensures data distribution across all workers instead of local bias
    /// - Initializes filesystem client with the modified configuration
    /// - Sets up task store and timing configurations from job settings
    /// - **Concurrency Control**: Enforces `conf.job.worker_max_concurrent_tasks`
    ///   and any configured resource-class sub-limit.
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
        conf.job.validate_worker_task_class_limits()?;
        let mut new_conf = conf.clone();
        new_conf.client.hostname = "localhost".to_string();

        let fs = CurvineFileSystem::with_rt(new_conf, rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let transfer_client = TransferClient::with_context(&fs.fs_context()).ok();
        let worker_task_semaphore = Arc::new(Semaphore::new(conf.job.worker_max_concurrent_tasks));
        let retained_task_limit = conf
            .job
            .worker_max_queued_tasks
            .max(conf.job.worker_max_concurrent_tasks)
            .max(1);
        let terminal_report_semaphore =
            Arc::new(Semaphore::new(conf.job.worker_max_concurrent_tasks.max(1)));
        let terminal_recovery =
            TerminalRecovery::new(&rt, retained_task_limit, terminal_report_semaphore.clone());
        let transfer_plugin = TransferPlugin::new(
            fs.clone(),
            factory.clone(),
            conf.job.task_report_interval.as_millis() as u64,
            conf.job.task_timeout.as_millis() as u64,
        );
        let mut registry = JobRegistry::new();
        registry.register(transfer_plugin)?;
        let registry = Arc::new(registry);
        let (inflight_gate, probe) = if conf.job.adaptive_concurrency.enabled {
            info!(
                "adaptive transfer concurrency enabled: budget {}..{} bytes, probe window {:?}, count cap {}",
                conf.job.adaptive_concurrency.min_bytes,
                conf.job.adaptive_concurrency.max_bytes,
                conf.job.adaptive_concurrency.window,
                conf.job.worker_max_concurrent_tasks
            );
            let policy = ProbePolicy::new(
                conf.job.adaptive_concurrency.min_bytes,
                conf.job.adaptive_concurrency.max_bytes,
                conf.job.adaptive_concurrency.window,
            );
            (
                Some(Arc::new(InflightBytesGate::with_probe(policy))),
                Some(policy.window),
            )
        } else {
            (None, None)
        };
        let mgr = Self {
            rt,
            fs,
            tasks: TaskStore::new(),
            transfer_client,
            worker_session_id: worker_session_id.into(),
            worker_task_semaphore,
            terminal_report_semaphore,
            terminal_recovery,
            retained_task_limit,
            terminal_report_pending: Arc::new(Semaphore::new(
                conf.job
                    .worker_max_queued_tasks
                    .max(conf.job.worker_max_concurrent_tasks)
                    .max(1),
            )),
            admitted: Arc::new(AtomicUsize::new(0)),
            max_queued_tasks: conf.job.worker_max_queued_tasks,
            class_limits: ClassLimits::from_job(&conf.job),
            metrics: &TASK_METRICS,
            registry,
            inflight_gate,
            sampled_bytes: Arc::new(Mutex::new(HashMap::new())),
        };

        if let (Some(window), Some(gate)) = (probe, mgr.inflight_gate.clone()) {
            // `loaded_size` advances at `task_report_interval`, not at the probe
            // window. Sample and advance the probe on the larger cadence so a
            // 10-second byte delta is not divided by a 2-second window.
            let sample_interval = probe_sample_interval(window, conf.job.task_report_interval);
            let tasks = mgr.tasks.clone();
            let sampled = mgr.sampled_bytes.clone();
            mgr.rt.spawn(async move {
                loop {
                    tokio::time::sleep(sample_interval).await;
                    feed_task_progress(&gate, &tasks, &sampled);
                    if gate.probe_tick(sample_interval).is_none() && !gate.is_probing() {
                        break;
                    }
                }
            });
        }

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
    /// * `task` - The TransferTaskInfo containing source path, target path, and job configuration
    ///
    /// # Returns
    ///
    /// Returns `FsResult<()>` indicating whether the task was successfully submitted.
    /// Note: This only indicates submission success, not task completion.
    ///
    /// # Concurrency Control
    ///
    /// - Tasks with a class concurrency cap acquire it before joining the global
    ///   execution queue. Other classes can overtake a task still waiting on its class.
    /// - Queued cancellation does not require an execution permit.
    /// - Maximum concurrent tasks is limited by `conf.job.worker_max_concurrent_tasks`
    /// - Permits are automatically released when tasks complete or fail
    /// - This prevents excessive bandwidth usage during bulk data operations
    /// - The number of tasks held at once (running + waiting for a permit) is
    ///   limited by `conf.job.worker_max_queued_tasks`; above that the submit is
    ///   rejected with `retryable = true` so the master retries it later instead
    ///   of the worker growing an unbounded queue in memory
    ///
    /// # Behavior
    ///
    /// 1. If a task with the same `task_id` is already in the store, it is
    ///    treated as **superseded**: its `TaskContext` is flipped to
    ///    `Canceled` (the running `TransferTaskRunner` observes this at the
    ///    next chunk boundary via `TaskContext::is_cancel`) and the map
    ///    entry is replaced with a fresh context in a single shard-locked
    ///    operation. This is intentional: silently de-duping would leave
    ///    the new dispatcher's `JobContext` without a reporter and the
    ///    master would hang until `ufs_copy_timeout`.
    /// 2. If no task exists for the `task_id`, the new context is inserted.
    /// 3. Spawns an async task that:
    ///    - Acquires a semaphore permit (blocks if limit reached)
    ///    - Executes `TransferTaskRunner::run`
    ///    - Automatically releases the permit on completion
    ///    - Removes the map entry **only if it still points at its own
    ///      context** (a later `submit_task` may have superseded it)
    pub fn submit_task(&self, task: TransferTaskInfo) -> FsResult<TaskSubmitResult> {
        self.submit_task_with_kind(TransferKind::Load, task)
    }

    pub fn submit_task_with_kind(
        &self,
        task_kind: TransferKind,
        task: TransferTaskInfo,
    ) -> FsResult<TaskSubmitResult> {
        let task_id = task.task_id.clone();
        let envelope = TaskEnvelope::from_transfer_task(&task, task_kind);
        let has_report_endpoint = task
            .transfer_report
            .as_ref()
            .map(|report| {
                !report.report_endpoints.is_empty()
                    || !report.report_target.is_empty()
                    || self.transfer_client.is_some()
            })
            .unwrap_or(true);
        if let Err(error) = envelope.validate(&self.worker_session_id, has_report_endpoint) {
            return Ok(TaskSubmitResult::rejected(error.to_string()));
        }

        let plugin = self.registry.get(envelope.task_kind)?;
        let class = plugin.resource_class();
        let limits = self.class_limits.get(&class);
        let admitted = self.admitted.clone();
        let max_queued = self.max_queued_tasks;
        let context = Arc::new(TaskContext::new(task));
        let reporter = match self.progress_reporter(context.clone()) {
            Ok(reporter) => reporter,
            Err(err) => return Ok(TaskSubmitResult::rejected(err.to_string())),
        };

        // The executor is built before the map or the running attempt changes,
        // so a plugin that cannot build one leaves both untouched.
        let (runner, class_slot, slot, retention) = match publish(
            &self.tasks,
            || admit_task(&admitted, max_queued, class, limits),
            &envelope,
            context.clone(),
            || {
                self.terminal_recovery
                    .reserve()
                    .ok_or_else(|| TaskSubmitResult::retention_full(self.retained_task_limit))
            },
            || plugin.create_executor(context.clone(), reporter.clone()),
        )? {
            Published::Duplicate => return Ok(TaskSubmitResult::accepted()),
            Published::Rejected(rejected) => return Ok(rejected),
            Published::Run {
                runner,
                class_slot,
                slot,
                retention,
            } => (runner, class_slot, slot, retention),
        };

        let retained = RetainedTask::new(context.clone(), self.tasks.clone(), retention);
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
        context.update_state(WorkerTaskState::Loading, "task accepted and queued");

        let semaphore = self.worker_task_semaphore.clone();
        let report_semaphore = self.terminal_report_semaphore.clone();
        let report_pending = self.terminal_report_pending.clone();
        let recovery_sender = self.terminal_recovery.sender();
        let stopped = self.terminal_recovery.stopped();
        let gate = self.inflight_gate.clone();
        let sampled_bytes = self.sampled_bytes.clone();
        let declared_bytes = task_inflight_bytes(&context.info);
        let slot_guard = slot;
        let class_semaphore = limits.and_then(|limits| limits.concurrent.clone());
        let metrics = self.metrics;
        let kind = envelope.task_kind;
        let admitted_metric = metrics
            .admitted
            .as_ref()
            .map(|metric| InFlightMetric::new(metric, kind));

        // Spawn task with concurrency control: count slot first, then the
        // adaptive byte reservation when enabled.
        self.rt.spawn(async move {
            let before_start = |outcome: TaskOutcome, progress| super::TaskExecution {
                terminal_report: reporter.client().map(|client| TaskTerminalReport {
                    progress,
                    result: None,
                    client: Some(client),
                }),
                ..outcome.into()
            };
            let permits = tokio::select! {
                biased;
                _ = context.wait_for_cancel() => None,
                permits = acquire_execution_permits(
                    semaphore,
                    class_semaphore,
                    gate.clone(),
                    context.info.clone(),
                    declared_bytes,
                ) => Some(permits),
            };
            // Cancellation may race a permit becoming ready on another thread.
            let permits = permits.filter(|_| !context.is_cancel());
            let mut execution = match permits {
                None => Some(before_start(
                    TaskOutcome::Canceled,
                    context.set_canceled("task canceled before execution"),
                )),
                Some(Ok(_permits)) => {
                    let _running_metric = metrics
                        .running
                        .as_ref()
                        .map(|metric| InFlightMetric::new(metric, kind));
                    let result = match runner.run(super::TaskExecCtx::new(context.clone())).await {
                        Ok(result) => Some(result),
                        Err(e) => {
                            log::error!("task {} failed: {}", task_id, e);
                            None
                        }
                    };
                    if let Some(gate) = &gate {
                        let size = context.progress().loaded_size;
                        let key = sample_key(&context.info);
                        let prev = sampled_bytes
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .remove(&key)
                            .unwrap_or(0);
                        gate.record_completed(size - prev);
                    }
                    result
                }
                Some(Err(e)) => {
                    let message = format!("{class:?} task failed to acquire execution permit: {e}");
                    log::error!("task {task_id}: {message}");
                    let progress = context.set_failed(&message);
                    Some(before_start(
                        TaskOutcome::Failed {
                            retryable: true,
                            message,
                        },
                        progress,
                    ))
                }
            };

            // Notification retries must not occupy data-copy admission or
            // execution slots. Drop the gauge before making the admission
            // slots available again.
            drop(admitted_metric);
            drop(class_slot);
            drop(slot_guard);
            if let Some(terminal) = execution
                .as_mut()
                .and_then(|result| result.terminal_report.take())
            {
                let report = PendingReport {
                    task: retained,
                    progress: terminal.progress,
                    result: terminal.result,
                    client: terminal.client,
                };
                // Cancellation after data commit must not rewrite or suppress
                // the frozen result. Only replacement/shutdown interrupts it.
                let started = std::time::Instant::now();
                let outcome = deliver_with_capacity(
                    &report_pending,
                    &report_semaphore,
                    || report.send(),
                    || !report.task.is_current() || stopped.has_changed().is_err(),
                    ReportPolicy::default(),
                )
                .await;
                info!(
                    "terminal report finished: job={} task={} outcome={:?} elapsed_ms={}",
                    report.task.context.info.job.job_id,
                    task_id,
                    outcome,
                    started.elapsed().as_millis()
                );
                if !matches!(outcome, ReportOutcome::Accepted | ReportOutcome::Rejected)
                    && report.task.is_current()
                    && stopped.has_changed().is_ok()
                {
                    TerminalRecovery::defer(&recovery_sender, report);
                }
                // Otherwise the guard removes only this context and frees retention.
            } else if execution.is_some_and(|result| !result.remove_task) {
                retained.keep_for_probe();
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
    ) -> Option<(WorkerTaskProgress, Option<TransferTaskResult>)> {
        let task = self.tasks.get(task_id)?;
        if task.info.job.job_id != job_id
            || !transfer_report_matches(
                task.info.transfer_report.as_ref(),
                run_id,
                attempt_id,
                worker_session_id,
            )
        {
            return None;
        }
        Some(task.progress_with_result())
    }

    fn progress_reporter(&self, context: Arc<TaskContext>) -> FsResult<ProgressReporter> {
        ProgressReporter::new(
            context,
            &self.fs,
            self.transfer_client.as_ref(),
            self.fs
                .fs_context()
                .conf()
                .job
                .worker_max_inline_result_bytes,
        )
    }

    pub fn supported_task_kinds(&self) -> Vec<TransferKind> {
        self.registry.supported_kinds()
    }
    pub fn get_fs_context(&self) -> Arc<FsContext> {
        self.fs.fs_context()
    }

    pub fn available_worker_task_permits(&self) -> usize {
        self.worker_task_semaphore.available_permits()
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

struct ClassLimits {
    concurrent: Option<Arc<Semaphore>>,
    queue: Option<(Arc<AtomicUsize>, usize)>,
}

impl ClassLimits {
    fn from_job(job: &JobConf) -> HashMap<ResourceClass, Self> {
        let limits = &job.worker_task_class_limits;
        [
            (ResourceClass::Network, limits.network),
            (ResourceClass::Remote, limits.remote),
            (ResourceClass::DiskIo, limits.disk_io),
            (ResourceClass::Cpu, limits.cpu),
        ]
        .into_iter()
        .filter(|(_, limit)| {
            limit.max_concurrent_tasks.is_some() || limit.max_queued_tasks.is_some()
        })
        .map(|(class, limit)| {
            (
                class,
                Self {
                    concurrent: limit
                        .max_concurrent_tasks
                        .map(|limit| Arc::new(Semaphore::new(limit))),
                    queue: limit
                        .max_queued_tasks
                        .map(|limit| (Arc::new(AtomicUsize::new(0)), limit)),
                },
            )
        })
        .collect()
    }
}

fn admit_task(
    global: &Arc<AtomicUsize>,
    global_limit: usize,
    class: ResourceClass,
    limits: Option<&ClassLimits>,
) -> Result<(Option<AdmissionSlot>, AdmissionSlot), TaskSubmitResult> {
    // A class rejection doesn't touch the global budget. On global rejection,
    // the class guard drops and rolls back its reservation.
    let class_slot = limits
        .and_then(|limits| limits.queue.as_ref())
        .map(|(admitted, limit)| {
            AdmissionSlot::reserve(admitted, *limit).map_err(|mut rejection| {
                rejection.reject_reason = format!("{:?} class {}", class, rejection.reject_reason);
                rejection
            })
        })
        .transpose()?;
    Ok((class_slot, AdmissionSlot::reserve(global, global_limit)?))
}

async fn acquire_execution_slots(
    global: Arc<Semaphore>,
    class: Option<Arc<Semaphore>>,
) -> Result<(OwnedSemaphorePermit, Option<OwnedSemaphorePermit>), String> {
    let class_permit = match class {
        Some(class) => Some(class.acquire_owned().await.map_err(|err| err.to_string())?),
        None => None,
    };
    Ok((
        global
            .acquire_owned()
            .await
            .map_err(|err| err.to_string())?,
        class_permit,
    ))
}

async fn acquire_execution_permits(
    global: Arc<Semaphore>,
    class: Option<Arc<Semaphore>>,
    gate: Option<Arc<InflightBytesGate>>,
    task: TransferTaskInfo,
    declared_bytes: i64,
) -> Result<
    (
        OwnedSemaphorePermit,
        Option<OwnedSemaphorePermit>,
        Option<InflightPermit>,
    ),
    String,
> {
    let (permit, class_permit) = acquire_execution_slots(global, class).await?;
    let inflight = match gate {
        Some(gate) => {
            let cost = priced_inflight_bytes(&task, declared_bytes);
            Some(gate.acquire(cost).await)
        }
        None => None,
    };
    Ok((permit, class_permit, inflight))
}

/// Outcome of publishing a submission into the task map.
enum Published {
    /// The context is the current attempt; run it under this admission slot.
    Run {
        runner: Box<dyn JobExecutor>,
        class_slot: Option<AdmissionSlot>,
        slot: AdmissionSlot,
        retention: OwnedSemaphorePermit,
    },
    /// The submission repeats the current attempt: nothing was admitted and
    /// nothing was replaced.
    Duplicate,
    /// Stale attempt or over the admission bound.
    Rejected(TaskSubmitResult),
}

/// Make `context` the current attempt for its task id.
///
/// The executor is created before the map or the running attempt changes, so a
/// factory error returns `Err` with no admission reserved, no context
/// published and the previous attempt still running and not canceled.
fn publish(
    tasks: &TaskStore,
    admit: impl Fn() -> Result<(Option<AdmissionSlot>, AdmissionSlot), TaskSubmitResult>,
    envelope: &TaskEnvelope,
    context: Arc<TaskContext>,
    reserve_retention: impl FnOnce() -> Result<OwnedSemaphorePermit, TaskSubmitResult>,
    create_executor: impl FnOnce() -> FsResult<Box<dyn JobExecutor>>,
) -> FsResult<Published> {
    let task_id = envelope.task_id.clone();

    match tasks.map().entry(task_id.clone()) {
        Entry::Occupied(mut occ) => {
            let old_envelope =
                TaskEnvelope::from_transfer_task(&occ.get().info, envelope.task_kind);
            if envelope.has_same_attempt(&old_envelope) {
                let report = context.info.transfer_report.as_ref().unwrap();
                debug!(
                    "ignore duplicate transfer task submit: job={} run={} task={} attempt={} source={} target={}",
                    context.info.job.job_id,
                    report.run_id,
                    task_id,
                    report.attempt_id,
                    context.info.source_path,
                    context.info.target_path,
                );
                return Ok(Published::Duplicate);
            }
            if envelope.is_older_than(&old_envelope) {
                let new_report = context.info.transfer_report.as_ref().unwrap();
                let old_report = occ.get().info.transfer_report.as_ref().unwrap();
                return Ok(Published::Rejected(TaskSubmitResult::rejected(format!(
                    "Reject stale transfer task {} run {} attempt {}, current run {} attempt {}",
                    task_id,
                    new_report.run_id,
                    new_report.attempt_id,
                    old_report.run_id,
                    old_report.attempt_id
                ))));
            }

            // A supersede replaces the context, so it needs its own slot: the
            // old runner releases its slot when it notices the cancel.
            let (class_slot, slot) = match admit() {
                Ok(slots) => slots,
                Err(rejected) => return Ok(Published::Rejected(rejected)),
            };
            let retention = match reserve_retention() {
                Ok(permit) => permit,
                Err(rejected) => return Ok(Published::Rejected(rejected)),
            };

            let runner = create_executor()?;
            let old = occ.insert(context.clone());
            old.set_canceled("superseded by new submit");
            warn!(
                "cancel duplicate task {} (source_path={})",
                old.info.task_id, old.info.source_path
            );

            Ok(Published::Run {
                runner,
                class_slot,
                slot,
                retention,
            })
        }

        Entry::Vacant(vac) => {
            let (class_slot, slot) = match admit() {
                Ok(slots) => slots,
                Err(rejected) => return Ok(Published::Rejected(rejected)),
            };
            let retention = match reserve_retention() {
                Ok(permit) => permit,
                Err(rejected) => return Ok(Published::Rejected(rejected)),
            };

            let runner = create_executor()?;
            vac.insert(context.clone());

            Ok(Published::Run {
                runner,
                class_slot,
                slot,
                retention,
            })
        }
    }
}

/// Holds one admission slot for the life of a runner.
///
/// Releasing in `Drop` (rather than at the end of the spawned future) means a
/// panic inside the runner cannot leak capacity and slowly close the worker's
/// admission bound.
struct AdmissionSlot {
    admitted: Arc<AtomicUsize>,
}

impl AdmissionSlot {
    fn reserve(admitted: &Arc<AtomicUsize>, limit: usize) -> Result<Self, TaskSubmitResult> {
        reserve_slot(admitted, limit)?;
        Ok(Self {
            admitted: admitted.clone(),
        })
    }
}

impl Drop for AdmissionSlot {
    fn drop(&mut self) {
        release_admission(&self.admitted);
    }
}

/// Releases the admission slot a finished runner held.
fn release_admission(admitted: &AtomicUsize) {
    let previous = admitted.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(
        previous > 0,
        "admission slot released without a reservation"
    );
}

/// Reserve one slot against `limit`, atomically.
///
/// Kept separate from [`TaskManager`] so the concurrency of the admission path
/// can be exercised without a running cluster.
fn count_price_fallback() {
    if let Some(counter) = INFLIGHT_PRICE_FALLBACK.as_ref() {
        counter.inc();
    }
}

/// What a task can really keep in flight: its read pipeline, capped by the
/// source object length.
fn priced_inflight_bytes(task: &TransferTaskInfo, declared: i64) -> i64 {
    if task.source_meta.len > 0 && !task.source_meta.is_dir {
        declared.min(task.source_meta.len)
    } else {
        count_price_fallback();
        declared
    }
}

fn probe_sample_interval(probe_window: Duration, report_interval: Duration) -> Duration {
    probe_window.max(report_interval)
}

fn sample_key(info: &TransferTaskInfo) -> String {
    let (run_id, attempt_id) = info
        .transfer_report
        .as_ref()
        .map(|report| (report.run_id, report.attempt_id))
        .unwrap_or((0, 0));
    format!("{}:{}:{}", info.task_id, run_id, attempt_id)
}

fn feed_task_progress(
    gate: &InflightBytesGate,
    tasks: &TaskStore,
    sampled: &Mutex<HashMap<String, i64>>,
) {
    let mut fresh = 0i64;
    let mut sampled = sampled.lock().unwrap_or_else(|err| err.into_inner());
    for ctx in tasks.all() {
        // Finished contexts may stay in the store for terminal-report retries
        // or probe-only retention. Their final delta belongs to the completion
        // flush; sampling them again would count the whole task twice.
        if ctx.get_state().is_finish() || ctx.is_cancel() {
            continue;
        }
        let size = ctx.progress().loaded_size;
        if size <= 0 {
            continue;
        }
        let key = sample_key(&ctx.info);
        let prev = sampled.get(&key).copied().unwrap_or(0);
        if size > prev {
            sampled.insert(key, size);
            fresh += size - prev;
        }
    }
    if fresh > 0 {
        gate.record_completed(fresh);
    }
}

fn reserve_slot(admitted: &AtomicUsize, limit: usize) -> Result<(), TaskSubmitResult> {
    if limit == 0 {
        admitted.fetch_add(1, Ordering::AcqRel);
        return Ok(());
    }

    let mut current = admitted.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return Err(TaskSubmitResult::queue_full(current, limit));
        }
        match admitted.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
#[path = "task_resource_tests.rs"]
mod task_resource_tests;

#[cfg(test)]
#[path = "task_result_tests.rs"]
mod task_result_tests;

#[cfg(test)]
mod tests {
    use super::{
        admit_task, priced_inflight_bytes, publish, release_admission, reserve_slot, AdmissionSlot,
        JobExecutor, JobRegistry, Published, TaskContext, TaskEnvelope, TaskManager, TaskStore,
        TaskSubmitResult,
    };
    use crate::worker::task::{JobFuture, JobPlugin, ProgressReporter, ResourceClass, TaskOutcome};
    use curvine_config::ClusterConf;
    use curvine_error::{FsError, FsResult};
    use curvine_model::{
        MountInfo, StorageType, TransferJobInfo, TransferKind, TransferTaskInfo,
        TransferTaskReportInfo, TtlAction, WorkerAddress, WorkerTaskState,
    };
    use curvine_runtime::runtime::AsyncRuntime;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn admission_price_uses_captured_source_len_without_head() {
        let mut task = task(1, 1);
        task.source_meta.len = 128 * 1024;
        assert_eq!(priced_inflight_bytes(&task, 4 * 1024 * 1024), 128 * 1024);

        task.source_meta.len = 0;
        assert_eq!(
            priced_inflight_bytes(&task, 4 * 1024 * 1024),
            4 * 1024 * 1024
        );
    }

    fn task(run_id: u64, attempt_id: u64) -> TransferTaskInfo {
        TransferTaskInfo {
            job: TransferJobInfo {
                job_id: "job".to_string(),
                source_path: "source".to_string(),
                target_path: "target".to_string(),
                block_size: 4096,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: MountInfo::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: "task".to_string(),
            worker: WorkerAddress::default(),
            source_path: "source".to_string(),
            target_path: "target".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            source_meta: Default::default(),
            transfer_report: Some(TransferTaskReportInfo {
                run_id,
                attempt_id,
                worker_id: 1,
                worker_session_id: "session".to_string(),
                report_target: String::new(),
                report_endpoints: vec!["127.0.0.1:9010".to_string()],
            }),
        }
    }

    fn envelope(task: &TransferTaskInfo) -> TaskEnvelope {
        TaskEnvelope::from_transfer_task(task, TransferKind::Load)
    }

    struct NoopExecutor;

    impl JobExecutor for NoopExecutor {
        fn run(self: Box<Self>, _: super::super::TaskExecCtx) -> JobFuture {
            Box::pin(async { Ok(TaskOutcome::Completed.into()) })
        }
    }

    fn failing_factory() -> FsResult<Box<dyn JobExecutor>> {
        Err(FsError::common("plugin cannot build an executor"))
    }

    struct FailingPlugin;

    impl JobPlugin for FailingPlugin {
        fn task_kinds(&self) -> Vec<TransferKind> {
            vec![TransferKind::Load]
        }

        fn name(&self) -> &'static str {
            "failing-test-plugin"
        }

        fn resource_class(&self) -> ResourceClass {
            ResourceClass::Cpu
        }

        fn create_executor(
            &self,
            _task: Arc<TaskContext>,
            _reporter: ProgressReporter,
        ) -> FsResult<Box<dyn JobExecutor>> {
            failing_factory()
        }
    }

    #[test]
    fn submit_task_with_kind_rolls_back_when_plugin_factory_fails() {
        let rt = Arc::new(AsyncRuntime::single());
        let mut conf = ClusterConf::default();
        conf.job.worker_max_queued_tasks = 2;
        let mut manager = TaskManager::with_rt(rt, &conf, "session").unwrap();
        let mut registry = JobRegistry::new();
        registry.register(FailingPlugin).unwrap();
        manager.registry = Arc::new(registry);

        let submit = |attempt_id| {
            let error = manager
                .submit_task_with_kind(TransferKind::Load, task(1, attempt_id))
                .err()
                .expect("a factory error must reach the submitter");
            assert!(error.to_string().contains("cannot build an executor"));
        };
        submit(1);
        assert!(!manager.tasks.contains("task"));
        assert_eq!(manager.admitted.load(Ordering::Acquire), 0);

        // Hold an admission slot on behalf of the old runner without spawning it.
        assert!(reserve_slot(&manager.admitted, manager.max_queued_tasks).is_ok());
        let _running_slot = AdmissionSlot {
            admitted: manager.admitted.clone(),
        };
        let running = Arc::new(TaskContext::new(task(1, 1)));
        running.update_state(WorkerTaskState::Loading, "test attempt running");
        manager
            .tasks
            .map()
            .insert("task".to_string(), running.clone());

        submit(2);
        assert_eq!(manager.admitted.load(Ordering::Acquire), 1);
        let published = manager.tasks.map().get("task").expect("old attempt stays");
        assert!(Arc::ptr_eq(published.value(), &running));
        assert_eq!(running.get_state(), WorkerTaskState::Loading);
    }

    #[test]
    fn review_regression_probe_only_retention_blocks_new_submissions_but_not_duplicates() {
        let rt = Arc::new(AsyncRuntime::single());
        let mut conf = ClusterConf::default();
        conf.job.worker_max_queued_tasks = 1;
        conf.job.worker_max_concurrent_tasks = 1;
        let manager = TaskManager::with_rt(rt, &conf, "session").unwrap();
        let context = manager.tasks.insert(task(1, 1));
        let frozen = context.update_progress(4096, 4096, true);
        super::RetainedTask::new(
            context.clone(),
            manager.tasks.clone(),
            manager.terminal_recovery.reserve().unwrap(),
        )
        .keep_for_probe();
        manager.cancel_job("job").unwrap();
        let (probed, result) = manager
            .query_transfer_task("job", "task", 1, 1, "session")
            .expect("frozen terminal progress stays queryable");
        assert_eq!(result, None);
        assert_eq!(
            serde_json::to_value(probed).unwrap(),
            serde_json::to_value(frozen).unwrap()
        );
        assert!(manager
            .query_transfer_task("job", "task", 1, 2, "session")
            .is_none());
        assert!(manager.submit_task(task(1, 1)).unwrap().accepted);
        let mut another = task(1, 1);
        another.task_id = "another".into();
        let rejected = manager.submit_task(another).unwrap();
        assert!(!rejected.accepted && rejected.retryable);
        assert!(rejected
            .reject_reason
            .contains("terminal result capacity full"));
        assert_eq!(manager.tasks.len(), 1);
        assert_eq!(manager.admitted.load(Ordering::Acquire), 0);
        drop(context);
        drop(manager.tasks.remove("task"));
        assert!(manager.terminal_recovery.reserve().is_some());
    }

    fn reserve_test_retention() -> Result<tokio::sync::OwnedSemaphorePermit, TaskSubmitResult> {
        Ok(Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .unwrap())
    }

    fn ok_factory() -> FsResult<Box<dyn JobExecutor>> {
        Ok(Box::new(NoopExecutor))
    }

    /// A factory error must not reserve admission, publish a context or touch
    /// the running attempt.
    #[test]
    fn a_failed_factory_reserves_nothing_on_an_empty_slot() {
        let tasks = TaskStore::new();
        let admitted = Arc::new(AtomicUsize::new(0));
        let submitted = task(1, 1);
        let context = Arc::new(TaskContext::new(submitted.clone()));

        let retention = Arc::new(tokio::sync::Semaphore::new(1));
        let error = publish(
            &tasks,
            || admit_task(&admitted, 8, ResourceClass::Network, None),
            &envelope(&submitted),
            context,
            || Ok(retention.clone().try_acquire_owned().unwrap()),
            failing_factory,
        )
        .err()
        .expect("a factory error must reach the caller");

        assert!(error.to_string().contains("cannot build an executor"));
        assert_eq!(retention.available_permits(), 1);
        assert!(
            !tasks.contains("task"),
            "a rejected submission must not be published"
        );
        assert_eq!(
            admitted.load(Ordering::Acquire),
            0,
            "a rejected submission must not keep its admission slot"
        );
    }

    /// The supersede path admits a second slot before the factory runs; a
    /// factory error must release it again and leave the old attempt running.
    #[test]
    fn a_failed_factory_keeps_the_running_attempt_and_frees_its_slot() {
        let tasks = TaskStore::new();
        let admitted = Arc::new(AtomicUsize::new(0));
        let running = Arc::new(TaskContext::new(task(1, 1)));
        tasks.map().insert("task".to_string(), running.clone());
        assert!(reserve_slot(&admitted, 8).is_ok());
        assert_eq!(admitted.load(Ordering::Acquire), 1);

        let newer = task(1, 2);
        let retention = Arc::new(tokio::sync::Semaphore::new(1));
        let error = publish(
            &tasks,
            || admit_task(&admitted, 8, ResourceClass::Network, None),
            &envelope(&newer),
            Arc::new(TaskContext::new(newer.clone())),
            || Ok(retention.clone().try_acquire_owned().unwrap()),
            failing_factory,
        )
        .err()
        .expect("a factory error must reach the caller");

        assert!(error.to_string().contains("cannot build an executor"));
        assert_eq!(retention.available_permits(), 1);
        assert_eq!(
            admitted.load(Ordering::Acquire),
            1,
            "the extra supersede slot must be released when the factory fails"
        );
        let published = tasks.map().get("task").expect("old attempt stays").clone();
        assert!(
            Arc::ptr_eq(&published, &running),
            "the superseded attempt must not be replaced"
        );
        assert_ne!(
            published.get_state(),
            curvine_model::WorkerTaskState::Canceled,
            "the running attempt must not be canceled when the factory fails"
        );
    }

    /// The happy path still replaces the old attempt and cancels it.
    #[test]
    fn a_published_supersede_cancels_the_old_attempt() {
        let tasks = TaskStore::new();
        let admitted = Arc::new(AtomicUsize::new(0));
        let running = Arc::new(TaskContext::new(task(1, 1)));
        tasks.map().insert("task".to_string(), running.clone());
        assert!(reserve_slot(&admitted, 8).is_ok());

        let newer = task(1, 2);
        let newer_context = Arc::new(TaskContext::new(newer.clone()));
        let published = publish(
            &tasks,
            || admit_task(&admitted, 8, ResourceClass::Network, None),
            &envelope(&newer),
            newer_context.clone(),
            reserve_test_retention,
            ok_factory,
        )
        .expect("a working factory must publish");
        assert!(matches!(published, Published::Run { .. }));

        let current = tasks
            .map()
            .get("task")
            .expect("new attempt published")
            .clone();
        assert!(Arc::ptr_eq(&current, &newer_context));
        assert_eq!(
            running.get_state(),
            curvine_model::WorkerTaskState::Canceled,
            "the replaced attempt must be canceled"
        );
        assert_eq!(admitted.load(Ordering::Acquire), 2);
    }

    /// A duplicate attempt is neither admitted nor rebuilt.
    #[test]
    fn a_duplicate_submission_builds_no_executor() {
        let tasks = TaskStore::new();
        let admitted = Arc::new(AtomicUsize::new(0));
        let running = TaskContext::new(task(1, 1));
        tasks.map().insert("task".to_string(), Arc::new(running));
        assert!(reserve_slot(&admitted, 8).is_ok());

        let duplicate = task(1, 1);
        let published = publish(
            &tasks,
            || admit_task(&admitted, 8, ResourceClass::Network, None),
            &envelope(&duplicate),
            Arc::new(TaskContext::new(duplicate.clone())),
            || panic!("a duplicate must not reserve retention"),
            || panic!("a duplicate must not build an executor"),
        )
        .expect("a duplicate is not an error");
        assert!(matches!(published, Published::Duplicate));
        assert_eq!(admitted.load(Ordering::Acquire), 1);
    }

    #[test]
    fn retention_backpressure_does_not_publish_or_cancel_an_attempt() {
        for replacing in [false, true] {
            let tasks = TaskStore::new();
            let admitted = Arc::new(AtomicUsize::new(0));
            let running = Arc::new(TaskContext::new(task(1, 1)));
            running.update_state(WorkerTaskState::Loading, "running");
            if replacing {
                tasks.map().insert("task".into(), running.clone());
            }
            let newer = task(1, 2);
            let result = publish(
                &tasks,
                || admit_task(&admitted, 8, ResourceClass::Network, None),
                &envelope(&newer),
                Arc::new(TaskContext::new(newer.clone())),
                || Err(TaskSubmitResult::retention_full(1)),
                || panic!("backpressure must not build an executor"),
            )
            .unwrap();
            let Published::Rejected(rejected) = result else {
                panic!("must reject")
            };
            assert!(rejected.retryable);
            assert_eq!(admitted.load(Ordering::Acquire), 0);
            if replacing {
                assert!(Arc::ptr_eq(tasks.get("task").unwrap().value(), &running));
                assert_eq!(running.get_state(), WorkerTaskState::Loading);
            } else {
                assert!(!tasks.contains("task"));
            }
        }
    }

    #[test]
    fn admission_backpressure_releases_retention() {
        let tasks = TaskStore::new();
        let admitted = Arc::new(AtomicUsize::new(1));
        let retention = Arc::new(tokio::sync::Semaphore::new(1));
        let submitted = task(1, 1);
        let result = publish(
            &tasks,
            || admit_task(&admitted, 1, ResourceClass::Network, None),
            &envelope(&submitted),
            Arc::new(TaskContext::new(submitted.clone())),
            || Ok(retention.clone().try_acquire_owned().unwrap()),
            || panic!("backpressure must not build an executor"),
        )
        .unwrap();
        assert!(matches!(
            result,
            Published::Rejected(TaskSubmitResult {
                retryable: true,
                ..
            })
        ));
        assert_eq!(retention.available_permits(), 1);
        assert_eq!(admitted.load(Ordering::Acquire), 1);
        assert!(!tasks.contains("task"));
    }

    #[test]
    fn admission_never_exceeds_the_limit_under_contention() {
        const LIMIT: usize = 8;
        const THREADS: usize = 32;

        let admitted = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(THREADS));
        let accepted = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let admitted = admitted.clone();
                let barrier = barrier.clone();
                let accepted = accepted.clone();
                thread::spawn(move || {
                    barrier.wait();
                    if reserve_slot(&admitted, LIMIT).is_ok() {
                        accepted.fetch_add(1, Ordering::AcqRel);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            accepted.load(Ordering::Acquire),
            LIMIT,
            "exactly the configured number of slots may be handed out"
        );
        assert_eq!(admitted.load(Ordering::Acquire), LIMIT);
    }

    /// A panic inside the runner must not leak capacity: the slot is released by
    /// `Drop`, not by the code path that follows the runner.
    #[test]
    fn a_panicking_runner_still_releases_its_slot() {
        use super::AdmissionSlot;
        use std::panic::AssertUnwindSafe;

        let admitted = Arc::new(AtomicUsize::new(1));
        assert!(
            reserve_slot(&admitted, 1).is_err(),
            "the only slot is taken"
        );

        let guard = AdmissionSlot {
            admitted: admitted.clone(),
        };
        let released = std::panic::catch_unwind(AssertUnwindSafe(move || {
            let _guard = guard;
            panic!("runner blew up");
        }));
        assert!(released.is_err(), "the test did not panic");
        assert_eq!(
            admitted.load(Ordering::Acquire),
            0,
            "an unwinding runner left its admission slot reserved"
        );
    }

    #[test]
    fn a_released_slot_is_available_again() {
        let admitted = AtomicUsize::new(0);
        assert!(reserve_slot(&admitted, 1).is_ok());
        let rejected = reserve_slot(&admitted, 1).expect_err("queue is full");
        assert!(rejected.retryable);
        release_admission(&admitted);
        assert!(reserve_slot(&admitted, 1).is_ok());
    }

    #[test]
    fn a_full_queue_is_retryable_and_says_how_full() {
        let admitted = AtomicUsize::new(10);
        let rejected = reserve_slot(&admitted, 10).expect_err("queue is full");
        assert!(!rejected.accepted);
        assert!(
            rejected.retryable,
            "a full queue must be retryable; the task is fine, the worker is busy"
        );
        assert!(
            rejected.reject_reason.contains("10 tasks pending"),
            "reason should say how full the queue is: {}",
            rejected.reject_reason
        );
    }

    #[test]
    fn zero_disables_the_queue_limit() {
        let admitted = AtomicUsize::new(usize::MAX - 1);
        assert!(reserve_slot(&admitted, 0).is_ok());
    }

    #[test]
    fn a_verdict_rejection_is_not_retryable() {
        let rejected = TaskSubmitResult::rejected("stale transfer task");
        assert!(!rejected.accepted);
        assert!(!rejected.retryable);
    }
}

#[cfg(test)]
mod progress_feed_tests {
    use super::*;
    use curvine_model::{StorageType, TtlAction, WorkerAddress};

    fn task(id: &str) -> TransferTaskInfo {
        TransferTaskInfo {
            job: curvine_model::TransferJobInfo {
                job_id: "j".to_string(),
                source_path: "s3://bucket/".to_string(),
                target_path: "/cv/".to_string(),
                block_size: 0,
                replicas: 1,
                storage_type: StorageType::Disk,
                ttl_ms: 0,
                ttl_action: TtlAction::None,
                mount_info: Default::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: id.to_string(),
            worker: WorkerAddress::default(),
            source_path: format!("s3://bucket/{id}"),
            target_path: format!("/cv/{id}"),
            create_time: 0,
            source_read_plan_json: String::new(),
            source_meta: Default::default(),
            transfer_report: None,
        }
    }

    #[test]
    fn probe_sampling_uses_the_slower_report_interval() {
        use std::time::Duration;
        assert_eq!(
            probe_sample_interval(Duration::from_secs(2), Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        assert_eq!(
            probe_sample_interval(Duration::from_secs(10), Duration::from_secs(2)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn sample_keys_are_scoped_to_run_and_attempt() {
        let mut info = task("same");
        info.transfer_report = Some(curvine_model::TransferTaskReportInfo {
            run_id: 7,
            attempt_id: 1,
            worker_id: 1,
            worker_session_id: "s".to_string(),
            report_target: String::new(),
            report_endpoints: Vec::new(),
        });
        let first = sample_key(&info);
        info.transfer_report.as_mut().unwrap().attempt_id = 2;
        assert_ne!(first, sample_key(&info));
    }

    #[test]
    fn progress_feed_ignores_finished_contexts() {
        let gate = InflightBytesGate::new(1024 * 1024);
        let store = TaskStore::new();
        let sampled = Mutex::new(HashMap::new());
        let ctx = store.insert(task("finished"));
        ctx.update_progress(100, 1000, false);
        ctx.update_state(WorkerTaskState::Completed, "done");
        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 0);
    }

    #[test]
    fn progress_feed_counts_only_new_bytes() {
        let gate = InflightBytesGate::new(1024 * 1024);
        let store = TaskStore::new();
        let sampled = Mutex::new(HashMap::new());
        let first = store.insert(task("t1"));
        let second = store.insert(task("t2"));

        first.update_progress(100, 1000, false);
        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 100);

        first.update_progress(400, 1000, false);
        second.update_progress(50, 500, false);
        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 450);

        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 450);
    }
}
