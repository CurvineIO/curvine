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
use crate::worker::task::inflight_gate::{
    task_inflight_bytes, InflightBytesGate, InflightPermit, ProbePolicy,
};
use crate::worker::task::load_task_runner::LoadTaskRunner;
use crate::worker::task::{TaskContext, TaskStore};
use curvine_client_core::file::{CurvineFileSystem, FsContext};
use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_job_client::TransferClient;
use curvine_model::{JobTaskProgress, JobTaskState, LoadTaskInfo, TransferTaskReportInfo};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use log::{debug, info, warn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Prices that fell back to the declared pipeline bytes (stat error, no UFS).
/// A large count here means the gate is pricing small files as if they were
/// full pipelines, which silently lowers concurrency.
static INFLIGHT_PRICE_FALLBACK: std::sync::LazyLock<Option<curvine_metrics::Counter>> =
    std::sync::LazyLock::new(|| {
        curvine_metrics::Metrics::new_counter(
            "worker_load_inflight_price_fallback_total",
            "Load task prices that fell back to the declared pipeline bytes",
        )
        .ok()
    });

pub struct TaskManager {
    rt: Arc<Runtime>,
    fs: CurvineFileSystem,
    tasks: TaskStore,
    factory: Arc<UfsFactory>,
    transfer_client: Option<TransferClient>,
    worker_session_id: String,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
    worker_task_semaphore: Arc<Semaphore>,
    /// Tasks the worker has admitted and not finished: running, or waiting for
    /// a concurrency permit. Beyond `max_queued_tasks` new submissions are
    /// rejected with a retryable "queue full" response so the master paces its
    /// dispatch instead of growing this queue without bound. Counted separately
    /// from `tasks`, because a task context can outlive its runner (a failed
    /// final report keeps it for the scheduler probe) and must not hold a slot.
    admitted: Arc<AtomicUsize>,
    max_queued_tasks: usize,
    /// Adaptive in-flight byte admission; `None` when disabled.
    inflight_gate: Option<Arc<InflightBytesGate>>,
    /// Bytes already fed into the gate's goodput, per task, so the probe sees
    /// progress while a large object is still being copied instead of only at
    /// task completion.
    sampled_bytes: Arc<DashMap<String, i64>>,
}

pub struct TaskSubmitResult {
    pub accepted: bool,
    /// The rejection is backpressure: the same task will be accepted later.
    pub retryable: bool,
    pub reject_reason: String,
}

impl TaskSubmitResult {
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
        let mut new_conf = conf.clone();
        new_conf.client.hostname = "localhost".to_string();

        let fs = CurvineFileSystem::with_rt(new_conf, rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let transfer_client = TransferClient::with_context(&fs.fs_context()).ok();
        let worker_task_semaphore = Arc::new(Semaphore::new(conf.job.worker_max_concurrent_tasks));
        let (inflight_gate, probe) = if conf.job.adaptive_concurrency.enabled {
            info!(
                "adaptive load concurrency enabled: budget {}..{} bytes, probe window {:?}, count cap {}",
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
            factory,
            transfer_client,
            worker_session_id: worker_session_id.into(),
            progress_interval_ms: conf.job.task_report_interval.as_millis() as u64,
            task_timeout_ms: conf.job.task_timeout.as_millis() as u64,
            worker_task_semaphore,
            admitted: Arc::new(AtomicUsize::new(0)),
            max_queued_tasks: conf.job.worker_max_queued_tasks,
            inflight_gate,
            sampled_bytes: Arc::new(DashMap::new()),
        };

        if let (Some(window), Some(gate)) = (probe, mgr.inflight_gate.clone()) {
            let tasks = mgr.tasks.clone();
            let sampled = mgr.sampled_bytes.clone();
            mgr.rt.spawn(async move {
                loop {
                    tokio::time::sleep(window).await;
                    feed_task_progress(&gate, &tasks, &sampled);
                    if gate.probe_tick(window).is_none() && !gate.is_probing() {
                        break;
                    }
                }
            });
        }

        Ok(mgr)
    }

    /// Reserves one admission slot, or returns the backpressure rejection.
    ///
    /// The reservation is atomic so concurrent handlers cannot all observe the
    /// last free slot and exceed the bound together. The slot is released when
    /// the runner this submission spawns exits.
    fn try_admit(&self) -> Result<(), TaskSubmitResult> {
        reserve_slot(&self.admitted, self.max_queued_tasks)
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
                return Ok(TaskSubmitResult::rejected(format!(
                    "Reject transfer task {} because no Transfer report endpoint is available",
                    task.task_id
                )));
            }
            if report.worker_session_id != self.worker_session_id {
                return Ok(TaskSubmitResult::rejected(format!(
                    "Reject transfer task {} for stale worker session {}, current session {}",
                    task.task_id, report.worker_session_id, self.worker_session_id
                )));
            }
        }

        let task_id = task.task_id.clone();
        let context = Arc::new(TaskContext::new(task));
        // Released when the runner exits, including by panic. Every arm below
        // either returns early (duplicate / stale, which reserve nothing) or
        // assigns it.
        let slot: AdmissionSlot;

        match self.tasks.entry(task_id.clone()) {
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
                            return Ok(TaskSubmitResult::accepted());
                        }
                        if old_report.run_id > new_report.run_id
                            || (old_report.run_id == new_report.run_id
                                && old_report.attempt_id > new_report.attempt_id)
                        {
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

                // A supersede replaces the context, so it needs its own slot:
                // the old runner releases its slot when it notices the cancel.
                if let Err(rejected) = self.try_admit() {
                    return Ok(rejected);
                }
                slot = AdmissionSlot {
                    admitted: self.admitted.clone(),
                };

                let old = occ.insert(context.clone());
                old.set_canceled("superseded by new submit");
                warn!(
                    "cancel duplicate task {} (source_path={})",
                    old.info.task_id, old.info.source_path
                );
            }

            Entry::Vacant(vac) => {
                if let Err(rejected) = self.try_admit() {
                    return Ok(rejected);
                }

                slot = AdmissionSlot {
                    admitted: self.admitted.clone(),
                };
                vac.insert(context.clone());
            }
        }
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
        context.update_state(JobTaskState::Loading, "task accepted and queued");

        let runner = LoadTaskRunner::new(
            context.clone(),
            self.fs.clone(),
            self.factory.clone(),
            self.transfer_client.clone(),
            self.progress_interval_ms,
            self.task_timeout_ms,
        );

        let tasks = self.tasks.clone();
        let semaphore = self.worker_task_semaphore.clone();
        let gate = self.inflight_gate.clone();
        let sampled_bytes = self.sampled_bytes.clone();
        let factory = self.factory.clone();
        let context_this = context.clone();
        let declared_bytes = task_inflight_bytes(&context.info);
        let slot_guard = slot;

        // Spawn task with concurrency control: the count slot is always taken
        // first, then (when enabled) the adaptive in-flight byte reservation.
        self.rt.spawn(async move {
            let mut remove_task = true;
            match acquire_admission(
                &semaphore,
                &gate,
                &factory,
                &context_this.info,
                declared_bytes,
            )
            .await
            {
                Ok(permits) => {
                    remove_task = runner.run().await;
                    if let Some(gate) = &gate {
                        // The task is about to leave the store, so the last
                        // progress sample travels through this path.
                        let size = context_this.progress().loaded_size;
                        let prev = sampled_bytes
                            .remove(&task_id)
                            .map(|(_, bytes)| bytes)
                            .unwrap_or(0);
                        gate.record_completed(size - prev);
                    }
                    drop(permits);
                }
                Err(e) => {
                    log::error!("task {} failed to acquire admission: {}", task_id, e);
                }
            }

            if remove_task {
                let _ = tasks.remove_if(&task_id, |_, ctx| Arc::ptr_eq(ctx, &context_this));
            }

            // The slot covers execution, not the retained context: a task whose
            // final report failed stays in the map for the scheduler probe but
            // must not keep the worker's admission bound closed.
            drop(slot_guard);
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

    pub fn get_fs_context(&self) -> Arc<FsContext> {
        self.fs.fs_context()
    }

    pub fn available_worker_task_permits(&self) -> usize {
        self.worker_task_semaphore.available_permits()
    }
}

/// Feed every task's newly copied bytes into the gate, so goodput tracks
/// progress instead of only task completions (large objects otherwise produce
/// whole probe windows with no signal).
fn feed_task_progress(gate: &InflightBytesGate, tasks: &TaskStore, sampled: &DashMap<String, i64>) {
    let mut fresh = 0i64;
    for ctx in tasks.all() {
        let size = ctx.progress().loaded_size;
        if size <= 0 {
            continue;
        }
        let id = ctx.info.task_id.clone();
        let prev = sampled.get(&id).map(|bytes| *bytes).unwrap_or(0);
        if size > prev {
            sampled.insert(id, size);
            fresh += size - prev;
        }
    }
    if fresh > 0 {
        gate.record_completed(fresh);
    }
}

fn count_price_fallback() {
    if let Some(counter) = INFLIGHT_PRICE_FALLBACK.as_ref() {
        counter.inc();
    }
}

/// What a task can really keep in flight: its read pipeline, capped by the
/// source object length.
///
/// A 4 KiB object cannot fill a 64 MiB pipeline, and pricing it at 64 MiB would
/// serialise a small-file load. The stat runs while the task already holds a
/// count slot (so at most `worker_max_concurrent_tasks` stats are in flight),
/// and any failure falls back to the pipeline price. CV sources (export tasks)
/// are already local and keep the pipeline price.
async fn priced_inflight_bytes(factory: &UfsFactory, task: &LoadTaskInfo, declared: i64) -> i64 {
    let Ok(source) = Path::from_str(&task.source_path) else {
        count_price_fallback();
        return declared;
    };
    if source.is_cv() {
        return declared;
    }
    let Ok(ufs) = factory.get_ufs(&task.job.mount_info) else {
        count_price_fallback();
        return declared;
    };
    match ufs.get_status(&source).await {
        Ok(status) if status.len > 0 && !status.is_dir => {
            let priced = declared.min(status.len);
            log::debug!(
                "inflight price {}: {} declared {} priced {}",
                task.task_id,
                source.full_path(),
                declared,
                priced
            );
            priced
        }
        Ok(status) => {
            count_price_fallback();
            log::debug!(
                "inflight price {}: {} has no length (dir={}, len={}), using {}",
                task.task_id,
                source.full_path(),
                status.is_dir,
                status.len,
                declared
            );
            declared
        }
        Err(err) => {
            count_price_fallback();
            log::debug!(
                "inflight price {}: stat {} failed ({}), using {}",
                task.task_id,
                source.full_path(),
                err,
                declared
            );
            declared
        }
    }
}

/// Take the count slot first, then the byte reservation.
///
/// Count-first is deadlock free: count holders may wait for bytes, byte holders
/// never wait for count, so there is no cycle. Keeping the count cap also
/// bounds tiny-file concurrency, and it bounds the UFS stat done to price a
/// task, so a large load cannot stampede the object store with HEADs.
async fn acquire_admission(
    semaphore: &Arc<Semaphore>,
    gate: &Option<Arc<InflightBytesGate>>,
    factory: &UfsFactory,
    task: &LoadTaskInfo,
    declared_bytes: i64,
) -> Result<(OwnedSemaphorePermit, Option<InflightPermit>), String> {
    let count = semaphore
        .clone()
        .acquire_owned()
        .await
        .map_err(|err| err.to_string())?;
    let bytes = match gate {
        Some(gate) => {
            let cost = priced_inflight_bytes(factory, task, declared_bytes).await;
            Some(gate.acquire(cost).await)
        }
        None => None,
    };
    Ok((count, bytes))
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

/// Holds one admission slot for the life of a runner.
///
/// Releasing in `Drop` (rather than at the end of the spawned future) means a
/// panic inside the runner cannot leak capacity and slowly close the worker's
/// admission bound.
struct AdmissionSlot {
    admitted: Arc<AtomicUsize>,
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
mod tests {
    use super::{release_admission, reserve_slot, TaskSubmitResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

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
    use curvine_model::{LoadJobInfo, StorageType, TtlAction, WorkerAddress};

    fn task(id: &str) -> LoadTaskInfo {
        LoadTaskInfo {
            job: LoadJobInfo {
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
            transfer_report: None,
        }
    }

    #[test]
    fn progress_feed_counts_only_new_bytes() {
        let gate = InflightBytesGate::new(1024 * 1024);
        let store = TaskStore::new();
        let sampled = DashMap::new();
        let first = store.insert(task("t1"));
        let second = store.insert(task("t2"));

        first.update_progress(100, 1000, false);
        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 100);

        // A large object keeps copying: the probe must see the delta even
        // though no task has finished.
        first.update_progress(400, 1000, false);
        second.update_progress(50, 500, false);
        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 450);

        // Nothing new: repeated sampling must not inflate goodput.
        feed_task_progress(&gate, &store, &sampled);
        assert_eq!(gate.snapshot().completed_bytes, 450);
    }
}
