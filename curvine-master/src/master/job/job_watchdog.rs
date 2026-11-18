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

//! Fail load tasks that can never report again.
//!
//! A job's state only moves when a worker reports one of its tasks, and the
//! worker keeps that state in memory. If a worker dies (OOM, restart, node
//! reboot) the tasks it held are gone with it: nothing will ever report them, so
//! the job sits in `Loading` forever. The same is true for a task whose worker
//! stays alive but stops making progress.
//!
//! The watchdog closes that hole from the master side, without needing a worker
//! to cooperate: it marks such tasks failed, which lets the normal failure path
//! (task-level retry, then the job) decide what happens next.

use crate::master::job::job_context::TaskDetail;
use crate::master::job::{JobContext, JobStore};
use crate::master::{MasterMonitor, SyncWorkerManager};
use curvine_error::FsError;
use curvine_model::{JobTaskProgress, JobTaskState, WorkerAddress};
use curvine_runtime::common::{FastHashMap, LocalTime};
use curvine_runtime::runtime::LoopTask;
use log::{debug, info, warn};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// Why a task will never be reported again.
#[derive(Debug, PartialEq, Eq)]
pub enum StuckReason {
    /// The worker the task was dispatched to is no longer alive, so its task
    /// table (and the task itself) went away with that worker.
    WorkerLost,
    /// The worker is alive but has not reported the task for too long; whatever
    /// it is waiting on may never return.
    NoProgress { idle_ms: i64, timeout_ms: i64 },
}

impl StuckReason {
    pub fn message(&self) -> String {
        match self {
            StuckReason::WorkerLost => {
                "worker lost: the task will never report, failing it".to_string()
            }
            StuckReason::NoProgress {
                idle_ms,
                timeout_ms,
            } => format!(
                "no progress for {}s (task timeout {}s), failing the task",
                idle_ms / 1000,
                timeout_ms / 1000
            ),
        }
    }
}

/// Decide whether one task is stuck.
///
/// Only dispatched tasks are candidates: a task that has not been handed to a
/// worker yet is still waiting in the dispatch loop, and failing it would abort a
/// perfectly healthy job (a million-file plan is dispatched over hours).
#[cfg(test)]
fn stuck_reason(
    detail: &TaskDetail,
    live_workers: &LiveWorkers,
    now_ms: i64,
    timeout_ms: i64,
) -> Option<StuckReason> {
    stuck_reason_since(detail, live_workers, now_ms, timeout_ms, 0, 0)
}

/// [`stuck_reason`] with the watchdog's clock floor and session grace.
pub fn stuck_reason_since(
    detail: &TaskDetail,
    live_workers: &LiveWorkers,
    now_ms: i64,
    timeout_ms: i64,
    clock_floor_ms: i64,
    session_grace_ms: i64,
) -> Option<StuckReason> {
    evaluate(
        &detail.task.worker,
        detail.progress.state,
        detail.progress.update_time,
        detail.dispatched_at,
        &detail.dispatched_session,
        live_workers,
        now_ms,
        timeout_ms,
        clock_floor_ms,
        session_grace_ms,
    )
}

/// Live workers and the session id of the instance that answers for them.
///
/// The address alone is not enough: a restarted worker comes back on the same
/// address with an empty task table, so a task dispatched to the previous
/// session is gone even though the address looks healthy. `WorkerInfo::
/// startup_time_ms` would be the natural signal, but the master never fills it
/// in, while `worker_session_id` is refreshed on every registration.
pub type LiveWorkers = FastHashMap<WorkerAddress, String>;

/// The decision itself, without a task fixture.
#[allow(clippy::too_many_arguments)]
fn evaluate(
    worker: &WorkerAddress,
    state: JobTaskState,
    update_time: i64,
    dispatched_at: i64,
    dispatched_session: &str,
    live_workers: &LiveWorkers,
    now_ms: i64,
    timeout_ms: i64,
    clock_floor_ms: i64,
    session_grace_ms: i64,
) -> Option<StuckReason> {
    if state.is_finish() || dispatched_at == 0 {
        return None;
    }

    // A demoted master cannot accept reports, so the time it spent inactive is
    // not silence: the floor is the last moment it was active.
    let last_signal = update_time.max(dispatched_at).max(clock_floor_ms);
    let idle_ms = now_ms.saturating_sub(last_signal);

    let Some(session) = live_workers.get(worker) else {
        // Missing from the live map only means "missed heartbeats": a network
        // partition evicts the worker after `worker_lost_interval` while its tasks
        // keep copying, and it re-registers with the same session when the
        // partition heals. Failing those tasks would kill a healthy job, so an
        // evicted worker's task has to be silent past the timeout as well.
        return no_progress(idle_ms, timeout_ms);
    };

    // Dispatched to the previous session of this worker: the restart wiped its
    // task table, so the report will never come. The recorded session is the
    // master's heartbeat view, which can lag the process that accepted the task
    // (a worker listens before it registers), so only judge this after a grace
    // period: a real restart stays silent, a lagging registration reports.
    if !dispatched_session.is_empty() && dispatched_session != session {
        return (idle_ms >= session_grace_ms).then_some(StuckReason::WorkerLost);
    }

    no_progress(idle_ms, timeout_ms)
}

fn no_progress(idle_ms: i64, timeout_ms: i64) -> Option<StuckReason> {
    (idle_ms > timeout_ms).then_some(StuckReason::NoProgress {
        idle_ms,
        timeout_ms,
    })
}

pub struct JobTaskWatchdog {
    jobs: JobStore,
    workers: SyncWorkerManager,
    task_timeout: Duration,
    /// A judge that keeps running on a demoted master would fail tasks whose
    /// reports this node can no longer accept, so silence starts counting from
    /// the last moment the master was active (see `clock_floor_ms`).
    monitor: MasterMonitor,
    /// Silence before this long is not a verdict on a session mismatch: a worker
    /// registers its session after it can already accept tasks, so a task can be
    /// accepted by a session the master has not recorded yet.
    session_grace: Duration,
    clock_floor_ms: AtomicI64,
}

impl JobTaskWatchdog {
    pub fn new(
        jobs: JobStore,
        workers: SyncWorkerManager,
        task_timeout: Duration,
        monitor: MasterMonitor,
        session_grace: Duration,
    ) -> Self {
        Self {
            jobs,
            workers,
            task_timeout,
            monitor,
            session_grace,
            clock_floor_ms: AtomicI64::new(0),
        }
    }

    fn live_workers(&self) -> LiveWorkers {
        let manager = self.workers.read();
        let mut live = LiveWorkers::default();
        for worker in manager.worker_map.workers().values() {
            live.insert(worker.address.clone(), worker.worker_session_id.clone());
        }
        live
    }
}

/// One task the sweep judged stuck, with the report time it was judged on, so
/// the failure can be re-checked before it is applied.
#[derive(Debug)]
struct StuckTask {
    job_id: String,
    task_id: String,
    observed_reports: u64,
    message: String,
}

/// Stuck tasks of every running job, plus how many were looked at.
///
/// Collected before anything is mutated: `update_progress` takes the same shard
/// locks this sweep holds while iterating.
fn collect_stuck(
    jobs: &curvine_runtime::sync::FastDashMap<String, JobContext>,
    live: &LiveWorkers,
    now_ms: i64,
    timeout_ms: i64,
    clock_floor_ms: i64,
    session_grace_ms: i64,
) -> (Vec<StuckTask>, usize) {
    let mut stuck = Vec::new();
    let mut scanned = 0;

    for entry in jobs.iter() {
        let job: &JobContext = entry.value();
        let state: JobTaskState = job.state.state();
        if state.is_finish() {
            continue;
        }

        for (task_id, detail) in job.tasks.iter() {
            scanned += 1;
            if let Some(reason) = stuck_reason_since(
                detail,
                live,
                now_ms,
                timeout_ms,
                clock_floor_ms,
                session_grace_ms,
            ) {
                stuck.push(StuckTask {
                    job_id: job.info.job_id.clone(),
                    task_id: task_id.clone(),
                    observed_reports: detail.reports,
                    message: reason.message(),
                });
            }
        }
    }

    (stuck, scanned)
}

impl LoopTask for JobTaskWatchdog {
    type Error = FsError;

    fn run(&self) -> Result<(), Self::Error> {
        let now = LocalTime::mills() as i64;
        // A demoted master cannot accept reports, so it must not judge tasks: a
        // demotion longer than the timeout would otherwise fail healthy jobs.
        // Remember when it was last active and start counting silence from there.
        if !self.monitor.is_active() {
            self.clock_floor_ms.store(now, Ordering::Relaxed);
            return Ok(());
        }
        let clock_floor_ms = self.clock_floor_ms.load(Ordering::Relaxed);
        let live = self.live_workers();
        let timeout_ms = self.task_timeout.as_millis() as i64;
        let session_grace_ms = self.session_grace.as_millis() as i64;
        let (stuck, scanned) = collect_stuck(
            &self.jobs,
            &live,
            now,
            timeout_ms,
            clock_floor_ms,
            session_grace_ms,
        );
        debug!(
            "job task watchdog: {} non-terminal task(s) across live jobs, {} stuck",
            scanned,
            stuck.len()
        );
        if stuck.is_empty() {
            return Ok(());
        }

        info!("job task watchdog: failing {} stuck task(s)", stuck.len());
        for task in stuck {
            let progress = JobTaskProgress {
                state: JobTaskState::Failed,
                loaded_size: 0,
                total_size: 0,
                update_time: now,
                message: task.message,
            };
            // A report may have arrived between the sweep and this call; failing
            // the task then would overwrite live progress and fail the job.
            match self.jobs.fail_task_if_stale(
                &task.job_id,
                &task.task_id,
                task.observed_reports,
                progress,
            ) {
                Ok(true) => {}
                Ok(false) => debug!(
                    "job task watchdog: task {} of job {} reported after the sweep, leaving it",
                    task.task_id, task.job_id
                ),
                Err(err) => warn!(
                    "job task watchdog: failing task {} of job {} failed: {}",
                    task.task_id, task.job_id, err
                ),
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate, stuck_reason, LiveWorkers, StuckReason};
    use crate::master::job::job_context::TaskDetail;
    use curvine_model::{JobTaskProgress, JobTaskState, LoadTaskInfo, WorkerAddress};
    use curvine_runtime::common::FastHashMap;

    const SESSION: &str = "session-1";

    fn worker(id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id: id,
            hostname: "localhost".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 8997,
            web_port: 0,
        }
    }

    /// Live workers, all on the same session unless the test says otherwise.
    fn live(ids: &[u32]) -> LiveWorkers {
        let mut set: LiveWorkers = FastHashMap::default();
        for id in ids {
            set.insert(worker(*id), SESSION.to_string());
        }
        set
    }

    fn live_with_session(id: u32, session: &str) -> LiveWorkers {
        let mut set: LiveWorkers = FastHashMap::default();
        set.insert(worker(id), session.to_string());
        set
    }

    #[allow(clippy::too_many_arguments)]
    fn check(
        state: JobTaskState,
        update_time: i64,
        dispatched_at: i64,
        session: &str,
        workers: &LiveWorkers,
        now_ms: i64,
        timeout_ms: i64,
    ) -> Option<StuckReason> {
        evaluate(
            &worker(1),
            state,
            update_time,
            dispatched_at,
            session,
            workers,
            now_ms,
            timeout_ms,
            0,
            0,
        )
    }

    #[test]
    fn a_demoted_master_does_not_count_its_own_downtime() {
        let workers = live_with_session(1, "current");
        // Dispatched long ago and silent, but the master was only active again at
        // 9_000: the demotion is not the worker's silence.
        assert!(evaluate(
            &worker(1),
            JobTaskState::Loading,
            2_000,
            2_000,
            "current",
            &workers,
            10_000,
            60_000,
            9_000,
            0,
        )
        .is_none());

        // Without the floor the same task is silent past the timeout.
        assert!(matches!(
            evaluate(
                &worker(1),
                JobTaskState::Loading,
                2_000,
                2_000,
                "current",
                &workers,
                70_000,
                60_000,
                0,
                0,
            ),
            Some(StuckReason::NoProgress { .. })
        ));
    }

    #[test]
    fn a_session_change_waits_for_the_grace_window() {
        let workers = live_with_session(1, "new-session");
        // The master recorded the old session (its heartbeat view lags the
        // process that accepted the task), so a fresh dispatch must not be
        // failed on the mismatch alone.
        assert!(evaluate(
            &worker(1),
            JobTaskState::Loading,
            10_000,
            10_000,
            "old-session",
            &workers,
            10_000,
            60_000,
            0,
            30_000,
        )
        .is_none());

        // Still silent past the grace window: the task really is gone.
        assert_eq!(
            evaluate(
                &worker(1),
                JobTaskState::Loading,
                10_000,
                10_000,
                "old-session",
                &workers,
                45_000,
                60_000,
                0,
                30_000,
            ),
            Some(StuckReason::WorkerLost)
        );
    }

    #[test]
    fn an_undispatched_task_is_never_stuck() {
        // A million-file plan spends hours in the dispatch loop; none of those
        // tasks may be failed for "no progress".
        assert!(check(
            JobTaskState::Pending,
            0,
            0,
            "",
            &live(&[]),
            10_000_000,
            1_000
        )
        .is_none());
    }

    #[test]
    fn an_evicted_worker_is_not_enough_to_fail_the_task() {
        // Heartbeat expiry evicts the worker from the live map, but a partition
        // does not wipe its task table: a task that is still reporting must not be
        // failed just because the master cannot see the worker.
        assert!(check(
            JobTaskState::Loading,
            9_000,
            1_000,
            SESSION,
            &live(&[2]),
            10_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn an_evicted_worker_with_a_silent_task_is_stuck() {
        assert_eq!(
            check(
                JobTaskState::Loading,
                1_000,
                1_000,
                SESSION,
                &live(&[2]),
                100_000,
                60_000
            ),
            Some(StuckReason::NoProgress {
                idle_ms: 99_000,
                timeout_ms: 60_000,
            })
        );
    }

    #[test]
    fn a_progressing_task_is_not_stuck() {
        assert!(check(
            JobTaskState::Loading,
            9_500,
            1_000,
            SESSION,
            &live(&[1]),
            10_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn a_silent_task_is_stuck_once_the_timeout_passes() {
        assert_eq!(
            check(
                JobTaskState::Loading,
                2_000,
                1_000,
                SESSION,
                &live(&[1]),
                100_000,
                60_000
            ),
            Some(StuckReason::NoProgress {
                idle_ms: 98_000,
                timeout_ms: 60_000,
            })
        );
    }

    #[test]
    fn a_finished_task_is_never_stuck() {
        assert!(check(
            JobTaskState::Completed,
            1_000,
            1_000,
            SESSION,
            &live(&[]),
            100_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn a_retried_task_waits_from_its_dispatch_time() {
        // A retried task gets a fresh `dispatched_at`; the old dispatch must not
        // make it look stale immediately.
        assert!(check(
            JobTaskState::Pending,
            0,
            99_500,
            SESSION,
            &live(&[1]),
            100_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn a_task_from_a_previous_worker_session_is_stuck() {
        // Same address, new session: the restarted worker never had this task, so
        // nothing will report it even though the address looks healthy.
        assert_eq!(
            check(
                JobTaskState::Loading,
                1_000,
                1_000,
                "old-session",
                &live_with_session(1, "new-session"),
                6_000,
                60_000
            ),
            Some(StuckReason::WorkerLost)
        );
    }

    #[test]
    fn a_task_from_the_current_session_is_not_stuck() {
        assert!(check(
            JobTaskState::Loading,
            6_000,
            6_000,
            "new-session",
            &live_with_session(1, "new-session"),
            7_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn an_unknown_session_falls_back_to_the_timeout() {
        // Before the first registration the session is empty; the task must still
        // be judged by its progress rather than failed outright.
        assert!(check(
            JobTaskState::Loading,
            6_000,
            6_000,
            "",
            &live_with_session(1, "new-session"),
            7_000,
            60_000
        )
        .is_none());
    }

    /// The sweep itself, against a real store: a dispatched task whose worker
    /// session changed must come back as stuck, a healthy one must not, and a
    /// finished job must be skipped entirely.
    #[test]
    fn the_sweep_finds_tasks_of_a_previous_session() {
        use super::collect_stuck;
        use crate::master::job::JobContext;
        use curvine_config::ClientConf;
        use curvine_model::{JobTaskType, LoadJobCommand, MountInfo};
        use curvine_runtime::sync::FastDashMap;

        let job = |job_id: &str| {
            JobContext::with_conf(
                &LoadJobCommand::builder("s3://bucket".to_string()).build(),
                job_id.to_string(),
                "s3://bucket".to_string(),
                "/cv".to_string(),
                &MountInfo::default(),
                &ClientConf::default(),
                1,
                JobTaskType::Load,
            )
        };

        let jobs: FastDashMap<String, JobContext> = FastDashMap::default();

        let mut running = job("job-running");
        running.add_task_detail(
            "task-old".to_string(),
            TaskDetail {
                dispatched_at: 1_000,
                dispatched_session: "old-session".to_string(),
                ..TaskDetail::new(running_task(1, "task-old"))
            },
        );
        running.add_task_detail(
            "task-ok".to_string(),
            TaskDetail {
                dispatched_at: 9_900,
                dispatched_session: "current".to_string(),
                ..TaskDetail::new(running_task(1, "task-ok"))
            },
        );
        jobs.insert("job-running".to_string(), running);

        let mut finished = job("job-finished");
        finished.add_task_detail(
            "task-done".to_string(),
            TaskDetail {
                dispatched_at: 1_000,
                dispatched_session: "old-session".to_string(),
                ..TaskDetail::new(running_task(1, "task-done"))
            },
        );
        finished.update_state(JobTaskState::Completed, "done");
        jobs.insert("job-finished".to_string(), finished);

        let (stuck, scanned) = collect_stuck(
            &jobs,
            &live_with_session(1, "current"),
            10_000,
            60_000,
            0,
            0,
        );
        assert_eq!(scanned, 2, "the finished job must not be scanned");
        assert_eq!(
            stuck.len(),
            1,
            "only the task from the previous session is stuck: {:?}",
            stuck
        );
        assert_eq!(stuck[0].job_id, "job-running");
        assert_eq!(stuck[0].task_id, "task-old");
        assert!(stuck[0].message.contains("worker lost"));
    }

    fn running_task(worker_id: u32, task_id: &str) -> LoadTaskInfo {
        LoadTaskInfo {
            job: curvine_model::LoadJobInfo {
                job_id: "job".to_string(),
                source_path: "s3://bucket".to_string(),
                target_path: "/cv".to_string(),
                block_size: 64 * 1024 * 1024,
                replicas: 1,
                storage_type: Default::default(),
                ttl_ms: 0,
                ttl_action: Default::default(),
                mount_info: Default::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: task_id.to_string(),
            worker: worker(worker_id),
            source_path: "s3://bucket/file".to_string(),
            target_path: "/cv/file".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: None,
        }
    }

    #[test]
    fn the_detail_wrapper_reads_the_same_fields() {
        let task = running_task(1, "task-1");
        let mut detail = TaskDetail::new(task);
        assert!(stuck_reason(&detail, &live(&[]), 10_000, 1_000).is_none());
        detail.dispatched_at = 5_000;
        detail.dispatched_session = SESSION.to_string();
        detail.progress = JobTaskProgress {
            state: JobTaskState::Loading,
            update_time: 6_000,
            ..Default::default()
        };
        // A different session at the same address is a restart: the task table it
        // held is gone, so the report can never come.
        assert_eq!(
            stuck_reason(
                &detail,
                &live_with_session(1, "other-session"),
                10_000,
                60_000
            ),
            Some(StuckReason::WorkerLost)
        );
        assert!(stuck_reason(&detail, &live(&[1]), 10_000, 60_000).is_none());
    }
}
