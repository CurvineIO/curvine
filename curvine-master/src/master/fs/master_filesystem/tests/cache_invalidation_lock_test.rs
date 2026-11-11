// Copyright 2026 OPPO
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

// Cache invalidation serializes with file readers through the shared inode
// stripes: a held read lock blocks phase two until released.

#![cfg(feature = "fault-injection")]

use super::*;

#[test]
fn cache_invalidation_waits_for_the_file_inode_read_lock() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::{Duration, Instant};

    let fs = test_fs("lost-worker-inode-lock");
    let path = "/cache-lost.log";
    let block_id = replication_file(&fs, path);
    fs.set_attr(
        path,
        SetAttrOptsBuilder::new()
            .ufs_mtime(12345)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .unwrap();
    let inode_id = fs.file_status(path).unwrap().id;
    let data_dir = fs
        .fs_dir
        .view()
        .unwrap()
        .get_rocks_store()
        .db
        .conf()
        .data_dir
        .clone();
    let file_state = || {
        let dir = fs.fs_dir.view().unwrap();
        let inode = dir.store.get_inode(inode_id, None).unwrap().unwrap();
        let file = inode.as_file_ref().unwrap();
        (
            file.storage_policy.state,
            file.storage_policy.ufs_mtime,
            file.blocks.is_empty(),
        )
    };
    let runtime = FaultRuntime::process();
    let hook_id = "lost-worker-inode-lock-hook";
    let rule = FaultRuleBuilder::named("master.metadata.cache_invalidation_inode_lock_planned")
        .matches("data_dir", data_dir.as_str())
        .unwrap()
        .matches("worker_id", 100)
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(hook_id, rule).unwrap();

    let held = fs.inode_locks.read_inode(inode_id);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = done_tx.send(worker_fs.delete_locations(100));
    });
    let observed = catch_unwind(AssertUnwindSafe(|| {
        let deadline = Instant::now() + Duration::from_secs(5);
        let reached = loop {
            let fired = runtime.rule(hook_id).unwrap().unwrap().executions > 0;
            if fired || Instant::now() >= deadline {
                break fired;
            }
            std::thread::yield_now();
        };
        let early = done_rx.recv_timeout(Duration::from_millis(200));
        (reached, early, file_state())
    }));
    // Release the lock and rule even if an observation panics.
    drop(held);
    let removed = runtime.remove(hook_id);
    let (reached, early, before) =
        observed.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    let blocked = matches!(&early, Err(RecvTimeoutError::Timeout));
    let result = match early {
        Err(RecvTimeoutError::Timeout) => done_rx.recv_timeout(Duration::from_secs(5)),
        completed => completed,
    };
    // A timed-out operation must fail the test without an unbounded join.
    if result.is_ok() {
        worker.join().unwrap();
    }
    assert!(removed.unwrap());
    assert!(reached, "cleanup never reached its inode lock request");
    assert!(
        blocked,
        "cleanup completed or disconnected while the lock was held"
    );
    assert_eq!(before, (StorageState::Both, 12345, false));
    assert_eq!(
        result
            .expect("cleanup did not finish after release")
            .unwrap()
            .removed_block_ids,
        vec![block_id]
    );
    assert_eq!(file_state(), (StorageState::Ufs, 12345, true));
}
