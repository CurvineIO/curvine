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
use crate::master::job::JobStore;
use crate::master::SyncWorkerManager;
use curvine_error::FsError;
use curvine_model::{JobTaskProgress, JobTaskState, WorkerAddress};
use curvine_runtime::common::{FastHashMap, LocalTime};
use curvine_runtime::runtime::LoopTask;
use log::{info, warn};
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
pub fn stuck_reason(
    detail: &TaskDetail,
    live_workers: &LiveWorkers,
    now_ms: i64,
    timeout_ms: i64,
) -> Option<StuckReason> {
    evaluate(
        &detail.task.worker,
        detail.progress.state,
        detail.progress.update_time,
        detail.dispatched_at,
        live_workers,
        now_ms,
        timeout_ms,
    )
}

/// Live workers and the start time of the instance that answers for them.
///
/// The address alone is not enough: a restarted worker comes back on the same
/// address with an empty task table, so a task dispatched before that start time
/// is gone even though the address looks healthy.
pub type LiveWorkers = FastHashMap<WorkerAddress, u64>;

/// The decision itself, without a task fixture.
#[allow(clippy::too_many_arguments)]
fn evaluate(
    worker: &WorkerAddress,
    state: JobTaskState,
    update_time: i64,
    dispatched_at: i64,
    live_workers: &LiveWorkers,
    now_ms: i64,
    timeout_ms: i64,
) -> Option<StuckReason> {
    if state.is_finish() || dispatched_at == 0 {
        return None;
    }

    let Some(startup_time_ms) = live_workers.get(worker) else {
        return Some(StuckReason::WorkerLost);
    };

    // Dispatched to the previous incarnation of this worker: the restart wiped
    // its task table, so the report will never come.
    if dispatched_at < *startup_time_ms as i64 {
        return Some(StuckReason::WorkerLost);
    }

    let last_signal = update_time.max(dispatched_at);
    let idle_ms = now_ms.saturating_sub(last_signal);
    if idle_ms > timeout_ms {
        return Some(StuckReason::NoProgress {
            idle_ms,
            timeout_ms,
        });
    }

    None
}

pub struct JobTaskWatchdog {
    jobs: JobStore,
    workers: SyncWorkerManager,
    task_timeout: Duration,
}

impl JobTaskWatchdog {
    pub fn new(jobs: JobStore, workers: SyncWorkerManager, task_timeout: Duration) -> Self {
        Self {
            jobs,
            workers,
            task_timeout,
        }
    }

    fn live_workers(&self) -> LiveWorkers {
        let manager = self.workers.read();
        let mut live = LiveWorkers::default();
        for worker in manager.worker_map.workers().values() {
            live.insert(worker.address.clone(), worker.startup_time_ms);
        }
        live
    }

    /// Collect the stuck tasks of every running job, then fail them.
    ///
    /// Collected first so the store is not mutated while it is being iterated:
    /// `update_progress` takes the same shard locks.
    fn collect(&self, live: &LiveWorkers, now_ms: i64) -> Vec<(String, String, String)> {
        let timeout_ms = self.task_timeout.as_millis() as i64;
        let mut stuck = Vec::new();

        for entry in self.jobs.iter() {
            let job = entry.value();
            let state: JobTaskState = job.state.state();
            if state.is_finish() {
                continue;
            }

            for (task_id, detail) in job.tasks.iter() {
                if let Some(reason) = stuck_reason(detail, live, now_ms, timeout_ms) {
                    stuck.push((job.info.job_id.clone(), task_id.clone(), reason.message()));
                }
            }
        }

        stuck
    }
}

impl LoopTask for JobTaskWatchdog {
    type Error = FsError;

    fn run(&self) -> Result<(), Self::Error> {
        let live = self.live_workers();
        let now = LocalTime::mills() as i64;
        let stuck = self.collect(&live, now);
        if stuck.is_empty() {
            return Ok(());
        }

        info!("job task watchdog: failing {} stuck task(s)", stuck.len());
        for (job_id, task_id, message) in stuck {
            let progress = JobTaskProgress {
                state: JobTaskState::Failed,
                loaded_size: 0,
                total_size: 0,
                update_time: now,
                message,
            };
            if let Err(err) = self.jobs.update_progress(&job_id, &task_id, progress) {
                warn!(
                    "job task watchdog: failing task {} of job {} failed: {}",
                    task_id, job_id, err
                );
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
    use curvine_model::{JobTaskProgress, JobTaskState, WorkerAddress};
    use curvine_runtime::common::FastHashMap;

    fn worker(id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id: id,
            hostname: "localhost".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 8997,
            web_port: 0,
        }
    }

    /// Live workers, all started at `0` unless the test says otherwise.
    fn live(ids: &[u32]) -> LiveWorkers {
        let mut set: LiveWorkers = FastHashMap::default();
        for id in ids {
            set.insert(worker(*id), 0);
        }
        set
    }

    fn live_started_at(id: u32, startup_time_ms: u64) -> LiveWorkers {
        let mut set: LiveWorkers = FastHashMap::default();
        set.insert(worker(id), startup_time_ms);
        set
    }

    #[test]
    fn an_undispatched_task_is_never_stuck() {
        // A million-file plan spends hours in the dispatch loop; none of those
        // tasks may be failed for "no progress".
        assert!(evaluate(
            &worker(1),
            JobTaskState::Pending,
            0,
            0,
            &live(&[]),
            10_000_000,
            1_000
        )
        .is_none());
    }

    #[test]
    fn a_task_on_a_lost_worker_is_stuck() {
        assert_eq!(
            evaluate(
                &worker(1),
                JobTaskState::Loading,
                1_000,
                1_000,
                &live(&[2]),
                2_000,
                60_000
            ),
            Some(StuckReason::WorkerLost)
        );
    }

    #[test]
    fn a_progressing_task_is_not_stuck() {
        assert!(evaluate(
            &worker(1),
            JobTaskState::Loading,
            9_500,
            1_000,
            &live(&[1]),
            10_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn a_silent_task_is_stuck_once_the_timeout_passes() {
        assert_eq!(
            evaluate(
                &worker(1),
                JobTaskState::Loading,
                2_000,
                1_000,
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
        assert!(evaluate(
            &worker(1),
            JobTaskState::Completed,
            1_000,
            1_000,
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
        assert!(evaluate(
            &worker(1),
            JobTaskState::Pending,
            0,
            99_500,
            &live(&[1]),
            100_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn a_task_dispatched_before_the_worker_restarted_is_stuck() {
        // Same address, new incarnation: the restarted worker never had this
        // task, so nothing will report it even though the address is healthy.
        assert_eq!(
            evaluate(
                &worker(1),
                JobTaskState::Loading,
                1_000,
                1_000,
                &live_started_at(1, 5_000),
                6_000,
                60_000
            ),
            Some(StuckReason::WorkerLost)
        );
    }

    #[test]
    fn a_task_dispatched_after_the_worker_started_is_not_stuck() {
        assert!(evaluate(
            &worker(1),
            JobTaskState::Loading,
            6_000,
            6_000,
            &live_started_at(1, 5_000),
            7_000,
            60_000
        )
        .is_none());
    }

    #[test]
    fn the_detail_wrapper_reads_the_same_fields() {
        let task = curvine_model::LoadTaskInfo {
            job: curvine_model::LoadJobInfo {
                job_id: "job-1".to_string(),
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
            task_id: "task-1".to_string(),
            worker: worker(1),
            source_path: "s3://bucket/file".to_string(),
            target_path: "/cv/file".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: None,
        };
        let mut detail = TaskDetail::new(task);
        assert!(stuck_reason(&detail, &live(&[]), 10_000, 1_000).is_none());
        detail.dispatched_at = 5_000;
        detail.progress = JobTaskProgress {
            state: JobTaskState::Loading,
            update_time: 6_000,
            ..Default::default()
        };
        assert_eq!(
            stuck_reason(&detail, &live(&[2]), 10_000, 60_000),
            Some(StuckReason::WorkerLost)
        );
    }
}
