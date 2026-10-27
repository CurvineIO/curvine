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

use curvine_config::{ClusterConf, RuntimeConfigRegistry};
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
}

impl TransferRuntimeConfig {
    /// Captures the configuration file's value as the reset target.
    pub fn from_conf(conf: &ClusterConf) -> Self {
        Self::with_limit(conf.job.worker_max_concurrent_tasks)
    }

    /// Builds the state from a bare limit, for callers that do not hold a
    /// `ClusterConf` (tests). The file value is the same limit.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            file_worker_task_limit: limit,
            // A zero ceiling would park every worker forever; the configuration
            // value is floored here exactly like the previous constructor
            // argument was, and writes below reject zero outright.
            worker_task_limit: AtomicUsize::new(limit.max(1)),
        }
    }

    /// Effective per-worker concurrency ceiling.
    pub fn worker_task_limit(&self) -> usize {
        self.worker_task_limit.load(Ordering::Acquire)
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
}
