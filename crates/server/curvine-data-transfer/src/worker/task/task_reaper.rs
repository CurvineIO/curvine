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

//! Bounded retention of finished transfer task contexts.
//!
//! A task whose terminal report could not be delivered must stay queryable:
//! the scheduler's stale probe reads the terminal result back from the worker
//! when the report was lost. That recovery path has two ways to finish, and
//! only one of them is observed by the worker:
//!
//! * the Store refused the report (fencing failed), so this attempt no longer
//!   owns the task row. The probe queries the *current* attempt and worker
//!   session, so it will never ask this worker about this context. Keeping it
//!   can never recover anything: it is dropped immediately.
//! * the report could not be delivered, so the Store state is unknown. The
//!   context is retained for a bounded window that outlives the stale-probe
//!   deadline, after which the probe has had its chance and the context is
//!   reaped.
//!
//! Without the second bound the retained contexts accumulate for the lifetime
//! of the process, and each one carries the full `LoadTaskInfo` including the
//! encoded source block layout.

use std::sync::Arc;
use std::time::Duration;

use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::FastDashMap;

use crate::worker::task::{record_metric, record_task_evicted, TaskContext};

/// How long a finished context stays queryable for the scheduler stale probe.
///
/// The lower bound is not arbitrary: the probe only starts after the task row's
/// `stale_deadline_at`, which the last successful report set to
/// `task_stale_timeout` (60s by default). A probe that reaches an unreachable
/// worker defers the row and retries, each deferral extending the deadline by
/// another `task_stale_timeout`. A 10 minute default therefore leaves several
/// probe rounds even when a transfer service instance is briefly unavailable.
#[derive(Clone, Debug)]
pub struct TerminalRetention {
    ttl: Duration,
}
impl TerminalRetention {
    /// Smallest safe retention: two stale-probe rounds.
    pub const MIN_TTL: Duration = Duration::from_secs(120);

    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl: ttl.max(Self::MIN_TTL),
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    fn ttl_ms(&self) -> i64 {
        i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX)
    }
}

/// Why a finished context left the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictReason {
    /// The Store refused the terminal report: the attempt lost ownership.
    Fenced,
    /// The retention window elapsed without a probe collecting the result.
    Expired,
}

impl EvictReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::Fenced => "fenced",
            Self::Expired => "expired",
        }
    }
}

/// True when a finished context is past its retention window.
///
/// `update_time` is the last progress transition recorded by `TaskContext`,
/// so it is the moment the terminal state became visible to a probe. A zero
/// timestamp (never updated) is treated as fresh: it cannot be aged, and
/// dropping it would remove a probe answer that was never offered.
fn is_expired(context: &TaskContext, now_ms: i64, ttl_ms: i64) -> bool {
    let updated = context.update_time();
    updated > 0 && now_ms.saturating_sub(updated) >= ttl_ms
}

/// Removes finished contexts past `retention` and reports the live counters.
///
/// Returns the number of reaped contexts. `reason` is fixed to `Expired` here:
/// a fenced context is evicted synchronously by the runner that saw the
/// rejection, never by the reaper.
pub fn reap_finished_tasks(
    tasks: &FastDashMap<String, Arc<TaskContext>>,
    retention: &TerminalRetention,
) -> usize {
    let now_ms = LocalTime::mills() as i64;
    let ttl_ms = retention.ttl_ms();

    let mut expired = Vec::new();
    let mut retained = 0usize;
    let mut oldest_age = None::<i64>;
    for entry in tasks.iter() {
        let context = entry.value();
        if !context.get_state().is_finish() {
            continue;
        }
        let updated = context.update_time();
        let age = if updated > 0 {
            Some(now_ms.saturating_sub(updated).max(0))
        } else {
            None
        };
        if is_expired(context, now_ms, ttl_ms) {
            expired.push(entry.key().clone());
            continue;
        }
        retained += 1;
        if let Some(age) = age {
            oldest_age = Some(oldest_age.map_or(age, |current: i64| current.max(age)));
        }
    }

    let mut reaped = 0usize;
    for task_id in expired {
        // Re-check under the shard write lock: a submit may have replaced the
        // entry since the scan, and only the context we aged may be removed.
        let removed = tasks.remove_if(&task_id, |_, context| {
            context.get_state().is_finish() && is_expired(context, now_ms, ttl_ms)
        });
        if removed.is_some() {
            reaped += 1;
            record_task_evicted(EvictReason::Expired.label());
        } else {
            // A competing insert won the race; that context is live again.
            retained += 1;
        }
    }

    if reaped > 0 {
        // Reconcile from the store instead of decrementing per removal, so the
        // gauge cannot drift when a competing removal wins the race.
        let live = tasks.len();
        record_metric(|metrics| metrics.set_active(live));
    }

    record_metric(|metrics| metrics.set_terminal_retained(retained, oldest_age));
    if reaped > 0 {
        log::info!(
            "reaped {} expired finished transfer task contexts, {} still retained",
            reaped,
            retained
        );
    }
    reaped
}

#[cfg(test)]
mod tests {
    use super::{reap_finished_tasks, EvictReason, TerminalRetention};
    use crate::worker::task::TaskContext;
    use curvine_model::{
        JobTaskState, LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TtlAction, WorkerAddress,
    };
    use curvine_runtime::sync::FastDashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn task(task_id: &str) -> LoadTaskInfo {
        LoadTaskInfo {
            job: LoadJobInfo {
                job_id: "job".to_string(),
                source_path: "s".to_string(),
                target_path: "t".to_string(),
                block_size: 4096,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: MountInfo::default(),
                create_time: 0,
                overwrite: None,
                source_type: Default::default(),
                transfer_job_id: None,
            },
            task_id: task_id.to_string(),
            worker: WorkerAddress::default(),
            source_path: "s".to_string(),
            target_path: "t".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            estimated_size: 0,
            transfer_report: None,
        }
    }

    fn store_with(entries: &[(&str, JobTaskState)]) -> FastDashMap<String, Arc<TaskContext>> {
        let store = FastDashMap::default();
        for (task_id, state) in entries {
            let context = Arc::new(TaskContext::new(task(task_id)));
            context.update_state(*state, "test");
            store.insert((*task_id).to_string(), context);
        }
        store
    }

    /// The minimum safe retention must outlive two stale-probe rounds, so a
    /// configured value below it is clamped instead of honored.
    #[test]
    fn ttl_is_clamped_to_the_minimum() {
        assert_eq!(
            TerminalRetention::new(Duration::from_secs(1)).ttl(),
            TerminalRetention::MIN_TTL
        );
        assert_eq!(
            TerminalRetention::new(Duration::from_secs(600)).ttl(),
            Duration::from_secs(600)
        );
    }

    /// A retained context is what the scheduler probe reads, so an unexpired
    /// finished task must survive a reap pass.
    #[test]
    fn fresh_finished_task_is_retained() {
        let store = store_with(&[("done", JobTaskState::Completed)]);
        assert_eq!(
            reap_finished_tasks(&store, &TerminalRetention::new(Duration::from_secs(600))),
            0
        );
        assert!(store.contains_key("done"));
    }

    /// Running tasks are never reaped: they still owe a terminal report.
    #[test]
    fn running_task_is_never_reaped() {
        let store = store_with(&[("busy", JobTaskState::Loading)]);
        assert_eq!(
            reap_finished_tasks(&store, &TerminalRetention::new(TerminalRetention::MIN_TTL)),
            0
        );
        assert!(store.contains_key("busy"));
    }

    /// `update_time == 0` cannot be aged, so it is not silently discarded.
    #[test]
    fn age_predicate_ignores_contexts_without_a_transition() {
        let fresh = TaskContext::new(task("never-started"));
        assert!(!fresh.get_state().is_finish());
        // A context that never reached a transition has no timestamp to age.
        assert!(!super::is_expired(&fresh, i64::MAX, 1));

        let done = TaskContext::new(task("done"));
        done.update_state(JobTaskState::Completed, "done");
        // A finished context carrying a real timestamp does age out.
        assert!(super::is_expired(&done, done.progress().update_time + 1, 1));
    }

    /// A finished context past its window must actually leave the store: this is
    /// the bound that stops one fenced or undelivered report per task from
    /// accumulating for the lifetime of the process.
    #[test]
    fn finished_task_older_than_the_window_is_reaped() {
        let store = store_with(&[("old-done", JobTaskState::Completed)]);
        let now = curvine_runtime::common::LocalTime::mills() as i64;
        let ttl_ms = 600_000i64;

        // Freshly finished: still inside the window.
        assert!(!super::is_expired(
            store.get("old-done").unwrap().value(),
            now,
            ttl_ms
        ));

        // The terminal transition happened longer ago than the window allows.
        assert!(super::is_expired(
            store.get("old-done").unwrap().value(),
            now + ttl_ms,
            ttl_ms
        ));
    }

    /// The reaper must reuse the same predicate as the age check, so a context
    /// that is still inside the window keeps answering the stale probe.
    #[test]
    fn reap_keeps_contexts_inside_the_window() {
        let store = store_with(&[
            ("done", JobTaskState::Completed),
            ("failed", JobTaskState::Failed),
        ]);
        assert_eq!(
            reap_finished_tasks(&store, &TerminalRetention::new(Duration::from_secs(600))),
            0
        );
        assert_eq!(store.len(), 2);
    }

    /// Labels are the metric contract; renaming one silently breaks dashboards.
    #[test]
    fn eviction_reason_labels_are_stable() {
        assert_eq!(EvictReason::Fenced.label(), "fenced");
        assert_eq!(EvictReason::Expired.label(), "expired");
    }
}
