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

use curvine_config::{TransferConf, TransferStoreType};

#[test]
fn postgres_store_urls_select_the_postgres_backend() {
    for store_url in [
        "postgres://transfer:secret@db.example:5432/curvine_transfer",
        "postgresql://transfer:secret@db.example:5432/curvine_transfer",
    ] {
        let mut conf = TransferConf {
            enabled: true,
            hostname: "transfer-01.example".to_string(),
            store_url: store_url.to_string(),
            ..Default::default()
        };

        conf.init().unwrap();

        assert_eq!(conf.effective_store_type(), TransferStoreType::Postgres);
    }
}

#[test]
fn byte_budget_and_estimate_are_constants_on_by_default() {
    // Byte-budget gating is on by default (10 GiB); the unknown-size estimate
    // is fixed at 256 MiB. Neither is configurable - no knobs to mis-set.
    assert_eq!(
        TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
        10 * 1024 * 1024 * 1024
    );
    assert_eq!(
        TransferConf::DEFAULT_TASK_ESTIMATED_BYTES,
        256 * 1024 * 1024
    );
    assert_eq!(
        TransferConf::DEFAULT_SCHEDULER_INFLIGHT_CACHE_TTL_MS,
        15_000
    );
}

#[test]
fn task_retention_defaults_to_24_hours() {
    let mut conf = TransferConf::default();
    conf.init().unwrap();
    assert_eq!(
        conf.task_retention_str,
        TransferConf::DEFAULT_TASK_RETENTION
    );
    assert_eq!(
        conf.task_retention,
        conf.terminal_retention.checked_div(7).unwrap()
    );
}

#[test]
fn task_retention_missing_key_falls_back_to_24h() {
    // CI smoke loads the dist config template, which has no task_retention
    // key: a bare #[serde(default)] would decode to "" and fail init with
    // "invalid duration string". The field default must be "24h".
    let mut conf: TransferConf = toml::from_str("").unwrap();
    conf.init().unwrap();
    assert_eq!(
        conf.task_retention_str,
        TransferConf::DEFAULT_TASK_RETENTION
    );
    assert_eq!(
        conf.task_retention,
        conf.terminal_retention.checked_div(7).unwrap()
    );
}

#[test]
fn task_retention_accepts_explicit_shorter_window() {
    let mut conf: TransferConf = toml::from_str(
        r#"
            terminal_retention_str = "168h"
            task_retention_str = "24h"
        "#,
    )
    .unwrap();
    conf.init().unwrap();
    assert_eq!(
        conf.task_retention,
        conf.terminal_retention.checked_div(7).unwrap()
    );
}

#[test]
fn task_retention_rejects_zero_and_overshoot() {
    let mut zero: TransferConf = toml::from_str(r#"task_retention_str = "0s""#).unwrap();
    assert!(zero.init().is_err());
    let mut over: TransferConf = toml::from_str(
        r#"
            terminal_retention_str = "24h"
            task_retention_str = "168h"
        "#,
    )
    .unwrap();
    assert!(over.init().is_err());
}

/// The per-job task cap must be settable from the configuration file.
///
/// It previously carried `#[serde(skip)]`, so a `max_tasks_per_transfer` entry
/// was silently ignored and planning always enforced the built-in default. A
/// job exceeding that hard-coded cap failed with "files exceeds <default>" and
/// no way to raise it.
#[test]
fn max_tasks_per_transfer_is_configurable() {
    let conf: TransferConf = toml::from_str(
        r#"
        enabled = true
        hostname = "transfer-01.example"
        max_tasks_per_transfer = 250000
        "#,
    )
    .unwrap();
    assert_eq!(conf.max_tasks_per_transfer, 250_000);
}

/// Omitting the key keeps the historical default so existing files behave as before.
#[test]
fn max_tasks_per_transfer_defaults_when_absent() {
    let conf: TransferConf = toml::from_str(
        r#"
        enabled = true
        hostname = "transfer-01.example"
        "#,
    )
    .unwrap();
    // Pinned to a literal on purpose: asserting the constant against itself
    // would not notice a default change.
    assert_eq!(conf.max_tasks_per_transfer, 500_000);
}

/// Zero would reject every non-empty plan, so validation refuses it.
#[test]
fn max_tasks_per_transfer_rejects_zero() {
    // `TransferConf::init` runs the validator that enforces the lower bound.
    let mut conf = TransferConf {
        enabled: true,
        hostname: "transfer-01.example".to_string(),
        max_tasks_per_transfer: 0,
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("max_tasks_per_transfer"),
        "unexpected error: {err}"
    );
}
