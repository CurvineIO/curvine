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

use std::sync::Arc;

use curvine_config::{MasterConf, RuntimeConfigEntryView, RuntimeConfigRegistry};
use curvine_core_error::{err_box, CommonResult};
use curvine_proto::RuntimeConfigEntryProto;
use curvine_rpc::handler::LimitConf;
use serde_json::Value;

use crate::master::space::DirSpaceManager;

pub const DIR_SPACE_ENABLED_KEY: &str = curvine_config::DIR_SPACE_ENABLED_RUNTIME_KEY;
pub const DIR_SPACE_TRACKED_PATHS_KEY: &str = curvine_config::DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY;

/// Per-connection in-flight request cap. The key is the configuration path so the
/// config view can attribute the value without a translation table.
pub const CONN_LIMIT_KEY: &str = "master.conn_limit";
/// Process-wide concurrent request cap. Same dotted path as the key.
pub const GLOBAL_LIMIT_KEY: &str = "master.global_limit";

/// Master-side runtime registry.
///
/// Exposes the dir-space switch, the explicit statistics-path list, and the two
/// RPC concurrency limits. Overrides are volatile: restart restores the
/// file-backed values.
///
/// The limits are registered against the live [`LimitConf`] the RPC server
/// applies, not against the `ClusterConf` snapshot, so an edit changes the value
/// traffic is throttled by. A config-backed closure would only move the number
/// the config endpoint prints.
pub fn build_master_runtime_registry(
    manager: Arc<DirSpaceManager>,
    limit: LimitConf,
    master_conf: &MasterConf,
) -> RuntimeConfigRegistry {
    let file_conn_limit = master_conf.conn_limit;
    let file_global_limit = master_conf.global_limit;
    let reset_conn = limit.clone();
    let reset_global = limit.clone();

    RuntimeConfigRegistry::builder()
        .register_at(
            DIR_SPACE_ENABLED_KEY,
            Some("master.dir_space.enabled"),
            {
                let manager = manager.clone();
                move || Value::from(manager.file_enabled())
            },
            {
                let manager = manager.clone();
                move || Value::from(manager.tracking_enabled())
            },
            {
                let manager = manager.clone();
                move |raw| {
                    let enabled = parse_enabled(raw)?;
                    let previous = manager.update_enabled(enabled);
                    Ok((Value::from(previous), Value::from(enabled)))
                }
            },
            {
                let manager = manager.clone();
                move || Ok(Value::from(manager.reset_enabled()))
            },
        )
        .register_at(
            DIR_SPACE_TRACKED_PATHS_KEY,
            Some("master.dir_space.tracked_paths"),
            {
                let manager = manager.clone();
                move || Value::from(manager.file_tracked_paths())
            },
            {
                let manager = manager.clone();
                move || Value::from(manager.explicit_paths())
            },
            {
                let manager = manager.clone();
                move |raw| {
                    let paths = parse_tracked_paths(raw)?;
                    let (previous, applied) = manager.update_tracked_paths(&paths)?;
                    Ok((Value::from(previous), Value::from(applied)))
                }
            },
            {
                let manager = manager.clone();
                move || Ok(Value::from(manager.reset_tracked_paths()))
            },
        )
        .register_at(
            CONN_LIMIT_KEY,
            Some(CONN_LIMIT_KEY),
            move || Value::from(file_conn_limit),
            {
                let limit = limit.clone();
                move || Value::from(limit.conn_limit())
            },
            {
                let limit = limit.clone();
                move |raw| {
                    let value = parse_positive_usize(raw, CONN_LIMIT_KEY)?;
                    let previous = limit.set_conn_limit(value);
                    Ok((Value::from(previous), Value::from(value)))
                }
            },
            move || {
                reset_conn.set_conn_limit(file_conn_limit);
                Ok(Value::from(file_conn_limit))
            },
        )
        .register_at(
            GLOBAL_LIMIT_KEY,
            Some(GLOBAL_LIMIT_KEY),
            move || Value::from(file_global_limit),
            {
                let limit = limit.clone();
                move || Value::from(limit.global_limit_size())
            },
            {
                let limit = limit.clone();
                move |raw| {
                    let value = parse_positive_usize(raw, GLOBAL_LIMIT_KEY)?;
                    let previous = limit.set_global_limit(value);
                    Ok((Value::from(previous), Value::from(value)))
                }
            },
            move || {
                reset_global.set_global_limit(file_global_limit);
                Ok(Value::from(file_global_limit))
            },
        )
        .build()
}

fn parse_enabled(raw: &str) -> CommonResult<bool> {
    match raw.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => err_box!("{} expects true or false", DIR_SPACE_ENABLED_KEY),
    }
}

/// Reject zero and non-numeric input. `MasterConf::validate` applies the same
/// rule at startup, so a runtime edit must not be able to introduce a value the
/// process would refuse to boot with.
fn parse_positive_usize(raw: &str, key: &str) -> CommonResult<usize> {
    let value: usize = raw.trim().parse().map_err(|_| {
        curvine_core_error::CommonError::from(format!("{} expects an integer", key))
    })?;
    if value == 0 {
        return err_box!("{} must be greater than zero", key);
    }
    Ok(value)
}

fn parse_tracked_paths(raw: &str) -> CommonResult<Vec<String>> {
    match serde_json::from_str(raw) {
        Ok(paths) => Ok(paths),
        Err(err) => err_box!(
            "{} expects a JSON string array: {}",
            DIR_SPACE_TRACKED_PATHS_KEY,
            err
        ),
    }
}

pub fn runtime_entry_to_proto(view: RuntimeConfigEntryView) -> RuntimeConfigEntryProto {
    RuntimeConfigEntryProto {
        key: view.key,
        file_value_json: value_to_json_string(&view.file_value),
        override_value: view.override_value,
        effective_value_json: value_to_json_string(&view.effective_value),
    }
}

pub fn value_to_json_string(value: &Value) -> String {
    value.to_string()
}

pub fn list_runtime_config_entries(
    registry: &RuntimeConfigRegistry,
    key: Option<String>,
) -> CommonResult<Vec<RuntimeConfigEntryView>> {
    match key.filter(|k| !k.is_empty()) {
        Some(k) => Ok(vec![registry.get(&k)?]),
        None => Ok(registry.list()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::err_box;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    fn test_registry() -> RuntimeConfigRegistry {
        let live = Arc::new(AtomicI64::new(5));
        let file = Arc::new(5i64);
        let live_apply = live.clone();
        let live_reset = live.clone();
        let file_reset = file.clone();
        RuntimeConfigRegistry::builder()
            .register(
                "demo_limit",
                move || Value::from(*file),
                move || Value::from(live.load(Ordering::Relaxed)),
                move |raw| {
                    let v: i64 = match raw.parse() {
                        Ok(v) => v,
                        Err(_) => return err_box!("bad int"),
                    };
                    let previous = live_apply.swap(v, Ordering::Relaxed);
                    Ok((Value::from(previous), Value::from(v)))
                },
                move || {
                    let v = *file_reset;
                    live_reset.store(v, Ordering::Relaxed);
                    Ok(Value::from(v))
                },
            )
            .build()
    }

    #[test]
    fn list_and_proto_conversion() {
        let registry = test_registry();
        let all = list_runtime_config_entries(&registry, None).unwrap();
        assert_eq!(all.len(), 1);
        let pb = runtime_entry_to_proto(all[0].clone());
        assert_eq!(pb.key, "demo_limit");
        assert_eq!(pb.file_value_json, "5");
        assert_eq!(pb.effective_value_json, "5");
    }

    #[test]
    fn list_single_key() {
        let registry = test_registry();
        registry.set("demo_limit", "9", Some("test")).unwrap();
        let one = list_runtime_config_entries(&registry, Some("demo_limit".to_string())).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].effective_value, 9);
    }
}
