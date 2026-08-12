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

use super::{BlockStore, HeartbeatTask};
use crate::worker::storage::Dataset;
use curvine_config::{ClusterConf, WorkerConf};
use curvine_core_error::CommonResult;
use curvine_model::{BlockReportStatus, DeleteBlockCmd, WorkerCommand};
use curvine_runtime::runtime::GroupExecutor;
use dashmap::{DashMap, DashSet};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

fn create_store() -> CommonResult<BlockStore> {
    let conf = ClusterConf {
        format_worker: true,
        worker: WorkerConf {
            dir_reserved: "0".to_string(),
            data_dir: vec!["[MEM:1KB]../testing/heartbeat-task".to_string()],
            ..WorkerConf::default()
        },
        ..ClusterConf::default()
    };
    BlockStore::new("test", &conf)
}

#[test]
fn report_is_emitted_only_after_delete_completes() -> CommonResult<()> {
    let store = create_store()?;
    let executor = Arc::new(GroupExecutor::new("heartbeat-task-test", 1, 2));
    let reports = Arc::new(DashMap::new());
    let pending = Arc::new(DashSet::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    executor.spawn(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })?;
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    HeartbeatTask::delete_block_task(
        executor.clone(),
        store.clone(),
        vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
            blocks: vec![1],
        })],
        reports.clone(),
        pending.clone(),
    );

    // The master repeats DeleteBlock until it accepts the Deleted report.
    // A repeated command must not queue or count the same deletion twice.
    HeartbeatTask::delete_block_task(
        executor.clone(),
        store.clone(),
        vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
            blocks: vec![1],
        })],
        reports.clone(),
        pending.clone(),
    );

    assert!(reports.is_empty(), "report must wait for physical deletion");
    assert!(pending.contains(&1));
    assert_eq!(store.read()?.num_blocks_to_delete(), 1);
    release_tx.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while !reports.contains_key(&1) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }

    let report = reports.get(&1).expect("missing deleted report");
    assert_eq!(report.status, BlockReportStatus::Deleted);
    assert_eq!(store.read()?.num_blocks_to_delete(), 0);
    assert!(
        pending.contains(&1),
        "pending state is cleared after master ACK"
    );
    // GroupExecutor's Drop waits for its worker threads; leaking this test-only
    // executor lets the process shutdown reclaim the idle thread deterministically.
    std::mem::forget(executor);
    Ok(())
}
