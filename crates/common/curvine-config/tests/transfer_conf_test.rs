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
fn worker_max_inflight_tasks_parses_with_default() {
    // The count gate stays configurable; when absent it defaults to the
    // worker semaphore limit (100).
    let conf: TransferConf = toml::from_str("worker_max_inflight_tasks = 2").unwrap();
    assert_eq!(conf.worker_max_inflight_tasks, 2);

    let conf: TransferConf = toml::from_str("").unwrap();
    assert_eq!(
        conf.worker_max_inflight_tasks,
        TransferConf::DEFAULT_WORKER_MAX_INFLIGHT_TASKS
    );
    assert_eq!(TransferConf::DEFAULT_WORKER_MAX_INFLIGHT_TASKS, 100);
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
fn worker_max_inflight_tasks_parses_inside_cluster_conf() {
    let raw = r#"
        [transfer]
        worker_max_inflight_tasks = 7
    "#;
    let conf: curvine_config::ClusterConf = toml::from_str(raw).unwrap();
    assert_eq!(conf.transfer.worker_max_inflight_tasks, 7);
}
