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

//! Live runtime configuration for the transfer role.
//!
//! `job.worker_max_concurrent_tasks` gates dispatch (it is the per-worker
//! in-flight ceiling used by [`crate::transfer::TransferScheduler`]). Copying it
//! into the scheduler at construction time is the exact anti-pattern this module
//! removes: the same knob is already runtime-writable in the worker, so a pushed
//! value would be reported by the config endpoint while the scheduler kept
//! dispatching on the value read at startup. The value therefore lives behind an
//! atomic that every dispatch decision re-reads, and the registry built here is
//! the only write path.

use curvine_config::{ClusterConf, RuntimeConfigRegistry, TransferConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::{FsError, FsResult};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Runtime key for the per-worker concurrency ceiling.
///
/// The key keeps the historical spelling already published by the worker
/// registry; the on-disk field it is backed by is
/// `job.worker_max_concurrent_tasks`.
pub const WORKER_MAX_CONCURRENT_TASKS_CONFIG: &str = "worker_max_concurrent_tasks";

/// Runtime key for the per-job planned-task ceiling.
///
/// Backed by `transfer.max_tasks_per_transfer`. Lowering it cannot invalidate an
/// already-persisted plan: the limit is enforced while planning, so a running
/// job keeps the task rows it already has.
pub const MAX_TASKS_PER_TRANSFER_CONFIG: &str = "max_tasks_per_transfer";

/// Runtime key for the collected-entry ceiling of one planning pass.
///
/// Backed by `transfer.max_plan_entries`. Collection is what dominates planning
/// memory, so this is the knob that decides how wide a source tree one job may
/// walk before failing with "entries exceeds N during listing".
pub const MAX_PLAN_ENTRIES_CONFIG: &str = "max_plan_entries";

/// Transfer-role knobs that are adjustable without a restart.
///
/// Cloning is not implemented on purpose: the state is shared by `Arc` so that
/// the scheduler, the registry and the writer all observe one atomic.
#[derive(Debug)]
pub struct TransferRuntimeConfig {
    /// Value from the configuration file, restored by `reset_worker_task_limit`.
    file_worker_task_limit: usize,
    /// Effective ceiling. Read on every dispatch decision (never cached).
    worker_task_limit: AtomicUsize,
    /// Value from the configuration file, restored by `reset_max_tasks_per_transfer`.
    file_max_tasks_per_transfer: usize,
    /// Effective per-job task cap. Read on every planning cap check.
    max_tasks_per_transfer: AtomicUsize,
    /// Value from the configuration file, restored by `reset_max_plan_entries`.
    file_max_plan_entries: usize,
    /// Effective collected-entry cap. Read when a planning pass starts.
    max_plan_entries: AtomicUsize,
}

impl TransferRuntimeConfig {
    /// Captures the configuration file's value as the reset target.
    pub fn from_conf(conf: &ClusterConf) -> Self {
        Self::with_limits(
            conf.job.worker_max_concurrent_tasks,
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.max_plan_entries,
        )
    }

    /// Builds the state from a bare limit, for callers that do not hold a
    /// `ClusterConf` (tests). The file value is the same limit.
    pub fn with_limit(limit: usize) -> Self {
        Self::with_limits(
            limit,
            TransferConf::DEFAULT_MAX_TASKS_PER_TRANSFER,
            TransferConf::DEFAULT_MAX_PLAN_ENTRIES,
        )
    }

    /// Builds the state from explicit limits; each is floored to 1 so a zero in
    /// the configuration file cannot park dispatch or block every plan.
    pub fn with_limits(
        worker_task_limit: usize,
        max_tasks_per_transfer: usize,
        max_plan_entries: usize,
    ) -> Self {
        Self {
            file_worker_task_limit: worker_task_limit,
            worker_task_limit: AtomicUsize::new(worker_task_limit.max(1)),
            file_max_tasks_per_transfer: max_tasks_per_transfer,
            max_tasks_per_transfer: AtomicUsize::new(max_tasks_per_transfer.max(1)),
            file_max_plan_entries: max_plan_entries,
            max_plan_entries: AtomicUsize::new(max_plan_entries.max(1)),
        }
    }

    /// Effective per-worker concurrency ceiling.
    pub fn worker_task_limit(&self) -> usize {
        self.worker_task_limit.load(Ordering::Acquire)
    }

    /// Effective per-job planned-task ceiling.
    pub fn max_tasks_per_transfer(&self) -> usize {
        self.max_tasks_per_transfer.load(Ordering::Acquire)
    }

    /// Applies a new per-job task cap and returns the previous one.
    pub fn update_max_tasks_per_transfer(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                MAX_TASKS_PER_TRANSFER_CONFIG
            )));
        }
        Ok(self.max_tasks_per_transfer.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's per-job task cap and returns it.
    pub fn reset_max_tasks_per_transfer(&self) -> usize {
        let restored = self.file_max_tasks_per_transfer.max(1);
        self.max_tasks_per_transfer
            .store(restored, Ordering::Release);
        restored
    }

    /// Effective collected-entry ceiling for one planning pass.
    pub fn max_plan_entries(&self) -> usize {
        self.max_plan_entries.load(Ordering::Acquire)
    }

    /// Applies a new collected-entry ceiling and returns the previous one.
    pub fn update_max_plan_entries(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                MAX_PLAN_ENTRIES_CONFIG
            )));
        }
        Ok(self.max_plan_entries.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's collected-entry ceiling and returns it.
    pub fn reset_max_plan_entries(&self) -> usize {
        let restored = self.file_max_plan_entries.max(1);
        self.max_plan_entries.store(restored, Ordering::Release);
        restored
    }

    /// Applies a new ceiling and returns the previous one.
    pub fn update_worker_task_limit(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                WORKER_MAX_CONCURRENT_TASKS_CONFIG
            )));
        }
        Ok(self.worker_task_limit.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's value and returns it.
    ///
    /// Returns the *restored* value, not the previous one: the registry hands
    /// this value straight back to the caller as the new effective value, and a
    /// `swap`-style previous value would make the endpoint report the value it
    /// just replaced.
    pub fn reset_worker_task_limit(&self) -> usize {
        let restored = self.file_worker_task_limit.max(1);
        self.worker_task_limit.store(restored, Ordering::Release);
        restored
    }
}

/// Registry of the transfer role's runtime-writable knobs.
///
/// Matches the worker registry's shape (`register_at` with the dotted config
/// path) so both roles report the same key against the same file field.
pub fn build_transfer_runtime_registry(
    state: Arc<TransferRuntimeConfig>,
    conf: &ClusterConf,
) -> RuntimeConfigRegistry {
    let file_worker_task_limit = conf.job.worker_max_concurrent_tasks;
    let file_max_tasks_per_transfer = conf.transfer.max_tasks_per_transfer;
    let file_max_plan_entries = conf.transfer.max_plan_entries;

    RuntimeConfigRegistry::builder()
        .register_at(
            WORKER_MAX_CONCURRENT_TASKS_CONFIG,
            // Backed by `JobConf::worker_max_concurrent_tasks`, i.e. `[job].*`.
            // Without this declaration the config view would treat the key as a
            // runtime-only knob and silently drop the field from the view.
            Some("job.worker_max_concurrent_tasks"),
            move || Value::from(file_worker_task_limit),
            {
                let state = state.clone();
                move || Value::from(state.worker_task_limit())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(WORKER_MAX_CONCURRENT_TASKS_CONFIG, raw)?;
                    let previous = match state.update_worker_task_limit(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_worker_task_limit()))
            },
        )
        .register_at(
            MAX_TASKS_PER_TRANSFER_CONFIG,
            // Backed by `TransferConf::max_tasks_per_transfer`, i.e. `[transfer].*`.
            Some("transfer.max_tasks_per_transfer"),
            move || Value::from(file_max_tasks_per_transfer),
            {
                let state = state.clone();
                move || Value::from(state.max_tasks_per_transfer())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(MAX_TASKS_PER_TRANSFER_CONFIG, raw)?;
                    let previous = match state.update_max_tasks_per_transfer(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_max_tasks_per_transfer()))
            },
        )
        .register_at(
            MAX_PLAN_ENTRIES_CONFIG,
            // Backed by `TransferConf::max_plan_entries`, i.e. `[transfer].*`.
            Some("transfer.max_plan_entries"),
            move || Value::from(file_max_plan_entries),
            {
                let state = state.clone();
                move || Value::from(state.max_plan_entries())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(MAX_PLAN_ENTRIES_CONFIG, raw)?;
                    let previous = match state.update_max_plan_entries(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_max_plan_entries()))
            },
        )
        .build()
}

fn parse_usize(config: &str, value: &str) -> CommonResult<usize> {
    let parsed: usize = match value.parse() {
        Ok(v) => v,
        Err(_) => {
            return err_box!(
                "invalid {} value {:?}: must be a positive integer",
                config,
                value
            );
        }
    };
    if parsed == 0 {
        return err_box!("{} must be greater than zero", config);
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_set_and_reset_move_the_live_value() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        assert_eq!(
            state.worker_task_limit(),
            conf.job.worker_max_concurrent_tasks
        );

        let updated = registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "7", None)
            .unwrap();
        assert_eq!(state.worker_task_limit(), 7);
        assert_eq!(
            updated.previous_value,
            Value::from(conf.job.worker_max_concurrent_tasks)
        );

        // A zero ceiling would park every worker forever: the write is refused
        // and the effective value stays where it was.
        assert!(registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "0", None)
            .is_err());
        assert!(registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "many", None)
            .is_err());
        assert_eq!(state.worker_task_limit(), 7);

        registry
            .reset(WORKER_MAX_CONCURRENT_TASKS_CONFIG, None)
            .unwrap();
        assert_eq!(
            state.worker_task_limit(),
            conf.job.worker_max_concurrent_tasks
        );
    }

    #[test]
    fn update_rejects_zero_and_reports_the_previous_value() {
        let state = TransferRuntimeConfig::with_limit(3);
        assert!(state.update_worker_task_limit(0).is_err());
        assert_eq!(state.worker_task_limit(), 3);
        assert_eq!(state.update_worker_task_limit(5).unwrap(), 3);
        assert_eq!(state.worker_task_limit(), 5);
        assert_eq!(state.reset_worker_task_limit(), 3);
        assert_eq!(state.worker_task_limit(), 3);
    }

    #[test]
    fn zero_file_value_still_yields_a_usable_ceiling() {
        // `with_limit(0)` mirrors a config file that says 0: the effective
        // ceiling is floored to 1 rather than disabling dispatch entirely.
        let state = TransferRuntimeConfig::with_limit(0);
        assert_eq!(state.worker_task_limit(), 1);
        assert_eq!(state.reset_worker_task_limit(), 1);
    }

    /// The task cap is a separate knob from the worker ceiling: updating one
    /// must not disturb the other.
    #[test]
    fn task_cap_and_worker_ceiling_are_independent() {
        let state = TransferRuntimeConfig::with_limits(4, 100_000, 500_000);
        assert_eq!(state.worker_task_limit(), 4);
        assert_eq!(state.max_tasks_per_transfer(), 100_000);

        assert_eq!(
            state.update_max_tasks_per_transfer(250_000).unwrap(),
            100_000
        );
        assert_eq!(state.max_tasks_per_transfer(), 250_000);
        assert_eq!(state.worker_task_limit(), 4);

        assert_eq!(state.reset_max_tasks_per_transfer(), 100_000);
    }

    /// The collected-entry ceiling is its own knob: it bounds how wide a source
    /// tree one job may walk and must not move the task cap with it.
    #[test]
    fn plan_entry_cap_is_independent_and_rejects_zero() {
        let state = TransferRuntimeConfig::with_limits(1, 100_000, 300_000);
        assert_eq!(state.max_plan_entries(), 300_000);

        assert_eq!(state.update_max_plan_entries(500_000).unwrap(), 300_000);
        assert_eq!(state.max_plan_entries(), 500_000);
        assert_eq!(state.max_tasks_per_transfer(), 100_000);

        assert!(state.update_max_plan_entries(0).is_err());
        assert_eq!(state.max_plan_entries(), 500_000);

        assert_eq!(state.reset_max_plan_entries(), 300_000);
    }

    /// Wide partitions need a raisable entry cap; it is exposed on the same
    /// registry the UpdateConfigs endpoint drives.
    #[test]
    fn registry_updates_the_plan_entry_cap() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        let original = conf.transfer.max_plan_entries;

        let updated = registry
            .set(MAX_PLAN_ENTRIES_CONFIG, "800000", None)
            .unwrap();
        assert_eq!(state.max_plan_entries(), 800_000);
        assert_eq!(updated.previous_value, Value::from(original));
        assert_eq!(
            registry.config_path(MAX_PLAN_ENTRIES_CONFIG),
            Some("transfer.max_plan_entries")
        );

        assert!(registry.set(MAX_PLAN_ENTRIES_CONFIG, "0", None).is_err());
        assert_eq!(state.max_plan_entries(), 800_000);

        registry.reset(MAX_PLAN_ENTRIES_CONFIG, None).unwrap();
        assert_eq!(state.max_plan_entries(), original);
    }

    /// A zero cap would reject every non-empty plan, so writes refuse it and the
    /// effective value stays where it was.
    #[test]
    fn task_cap_update_rejects_zero() {
        let state = TransferRuntimeConfig::with_limits(1, 10, 500_000);
        assert!(state.update_max_tasks_per_transfer(0).is_err());
        assert_eq!(state.max_tasks_per_transfer(), 10);
    }

    /// The cap is hot-updatable through the same registry the UpdateConfigs
    /// endpoint drives, and it is reported against its on-disk config path.
    #[test]
    fn registry_updates_the_task_cap() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        let original = conf.transfer.max_tasks_per_transfer;

        let updated = registry
            .set(MAX_TASKS_PER_TRANSFER_CONFIG, "200000", None)
            .unwrap();
        assert_eq!(state.max_tasks_per_transfer(), 200_000);
        assert_eq!(updated.previous_value, Value::from(original));
        assert_eq!(
            registry.config_path(MAX_TASKS_PER_TRANSFER_CONFIG),
            Some("transfer.max_tasks_per_transfer")
        );

        assert!(registry
            .set(MAX_TASKS_PER_TRANSFER_CONFIG, "0", None)
            .is_err());
        assert_eq!(state.max_tasks_per_transfer(), 200_000);

        registry.reset(MAX_TASKS_PER_TRANSFER_CONFIG, None).unwrap();
        assert_eq!(state.max_tasks_per_transfer(), original);
    }
}
