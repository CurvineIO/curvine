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

use crate::worker::task::TaskManager;
use curvine_config::{ClusterConf, RuntimeConfigRegistry, TransferConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::{ByteUnit, DurationUnit};
use serde_json::Value;
use std::sync::Arc;

pub const TRANSFER_INFLIGHT_BUDGET_CONFIG: &str = "transfer_inflight_budget_bytes";
pub const WORKER_MAX_CONCURRENT_TASKS_CONFIG: &str = "worker_max_concurrent_tasks";
pub const TASK_TIMEOUT_CONFIG: &str = "task_timeout";
pub const TASK_REPORT_INTERVAL_CONFIG: &str = "task_report_interval";

#[derive(Clone)]
struct WorkerFileSnapshot {
    transfer_inflight_budget_bytes: i64,
    worker_max_concurrent_tasks: usize,
    task_timeout_ms: u64,
    task_report_interval_ms: u64,
}

impl WorkerFileSnapshot {
    fn from_conf(conf: &ClusterConf) -> Self {
        Self {
            transfer_inflight_budget_bytes: TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
            worker_max_concurrent_tasks: conf.job.worker_max_concurrent_tasks,
            task_timeout_ms: conf.job.task_timeout.as_millis() as u64,
            task_report_interval_ms: conf.job.task_report_interval.as_millis() as u64,
        }
    }
}

pub fn build_worker_runtime_registry(
    task_manager: Arc<TaskManager>,
    conf: &ClusterConf,
) -> RuntimeConfigRegistry {
    let file = Arc::new(WorkerFileSnapshot::from_conf(conf));
    let tm = task_manager;

    RuntimeConfigRegistry::builder()
        .register(
            TRANSFER_INFLIGHT_BUDGET_CONFIG,
            {
                let file = file.clone();
                move || Value::from(file.transfer_inflight_budget_bytes)
            },
            {
                let tm = tm.clone();
                move || Value::from(tm.transfer_inflight_budget_bytes())
            },
            {
                let tm = tm.clone();
                move |raw| {
                    let bytes = parse_inflight_budget(raw)?;
                    let previous = match tm.update_transfer_inflight_budget_bytes(bytes) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(bytes)))
                }
            },
            {
                let tm = tm.clone();
                let file = file.clone();
                move || {
                    let v = file.transfer_inflight_budget_bytes;
                    if let Err(e) = tm.update_transfer_inflight_budget_bytes(v) {
                        return err_box!(e.to_string());
                    }
                    Ok(Value::from(v))
                }
            },
        )
        .register(
            WORKER_MAX_CONCURRENT_TASKS_CONFIG,
            {
                let file = file.clone();
                move || Value::from(file.worker_max_concurrent_tasks)
            },
            {
                let tm = tm.clone();
                move || Value::from(tm.worker_max_concurrent_tasks())
            },
            {
                let tm = tm.clone();
                move |raw| {
                    let target = parse_usize(WORKER_MAX_CONCURRENT_TASKS_CONFIG, raw)?;
                    let previous = match tm.update_worker_max_concurrent_tasks(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let tm = tm.clone();
                let file = file.clone();
                move || {
                    let v = file.worker_max_concurrent_tasks;
                    if let Err(e) = tm.update_worker_max_concurrent_tasks(v) {
                        return err_box!(e.to_string());
                    }
                    Ok(Value::from(v))
                }
            },
        )
        .register(
            TASK_TIMEOUT_CONFIG,
            {
                let file = file.clone();
                move || Value::from(format!("{}ms", file.task_timeout_ms))
            },
            {
                let tm = tm.clone();
                move || Value::from(format!("{}ms", tm.task_timeout_ms()))
            },
            {
                let tm = tm.clone();
                move |raw| {
                    let ms = parse_duration_ms(TASK_TIMEOUT_CONFIG, raw)?;
                    let previous = match tm.update_task_timeout(ms) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(ms)))
                }
            },
            {
                let tm = tm.clone();
                let file = file.clone();
                move || {
                    let v = file.task_timeout_ms;
                    if let Err(e) = tm.update_task_timeout(v) {
                        return err_box!(e.to_string());
                    }
                    Ok(Value::from(format!("{}ms", v)))
                }
            },
        )
        .register(
            TASK_REPORT_INTERVAL_CONFIG,
            {
                let file = file.clone();
                move || Value::from(format!("{}ms", file.task_report_interval_ms))
            },
            {
                let tm = tm.clone();
                move || Value::from(format!("{}ms", tm.task_report_interval_ms()))
            },
            {
                let tm = tm.clone();
                move |raw| {
                    let ms = parse_duration_ms(TASK_REPORT_INTERVAL_CONFIG, raw)?;
                    let previous = match tm.update_task_report_interval(ms) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(ms)))
                }
            },
            {
                let tm = tm.clone();
                let file = file.clone();
                move || {
                    let v = file.task_report_interval_ms;
                    if let Err(e) = tm.update_task_report_interval(v) {
                        return err_box!(e.to_string());
                    }
                    Ok(Value::from(format!("{}ms", v)))
                }
            },
        )
        .build()
}

fn parse_inflight_budget(value: &str) -> CommonResult<i64> {
    let bytes = match ByteUnit::from_str(value) {
        Ok(v) => v,
        Err(error) => {
            return err_box!(
                "invalid {} value {:?}: {}",
                TRANSFER_INFLIGHT_BUDGET_CONFIG,
                value,
                error
            );
        }
    };
    let bytes = match i64::try_from(bytes.as_byte()) {
        Ok(v) => v,
        Err(_) => {
            return err_box!(
                "{} exceeds the maximum supported byte value",
                TRANSFER_INFLIGHT_BUDGET_CONFIG
            );
        }
    };
    if bytes <= 0 {
        return err_box!(
            "{} must be greater than zero",
            TRANSFER_INFLIGHT_BUDGET_CONFIG
        );
    }
    Ok(bytes)
}

fn parse_duration_ms(config: &str, value: &str) -> CommonResult<u64> {
    let unit = match DurationUnit::from_str(value) {
        Ok(v) => v,
        Err(error) => return err_box!("invalid {} value {:?}: {}", config, value, error),
    };
    let ms = unit.as_millis();
    if ms == 0 {
        return err_box!("{} must be greater than zero", config);
    }
    Ok(ms)
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
    use curvine_runtime::runtime::Runtime;

    #[test]
    fn parse_helpers_reject_invalid_values() {
        assert!(parse_inflight_budget("100GB").is_ok());
        assert!(parse_usize(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "0").is_err());
        assert!(parse_duration_ms(TASK_TIMEOUT_CONFIG, "0").is_err());
    }

    #[test]
    fn registry_round_trip_matches_task_manager() {
        let conf = ClusterConf::default();
        let tm = Arc::new(
            TaskManager::with_rt(Arc::new(Runtime::single()), &conf, "registry-test").unwrap(),
        );
        let registry = build_worker_runtime_registry(tm.clone(), &conf);
        registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "64", None)
            .unwrap();
        assert_eq!(tm.worker_max_concurrent_tasks(), 64);
        registry
            .reset(WORKER_MAX_CONCURRENT_TASKS_CONFIG, None)
            .unwrap();
        assert_eq!(
            tm.worker_max_concurrent_tasks(),
            conf.job.worker_max_concurrent_tasks
        );
    }
}
