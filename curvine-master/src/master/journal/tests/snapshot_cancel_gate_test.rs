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

//! Cancelling a snapshot caller must not release the admission fence.
//!
//! `create_snapshot` spawns an inner task that owns the commit-gate barrier,
//! queues `ApplyMsg::CreateSnapshot`, and waits for its reply. Aborting the
//! caller only drops the outer await: the inner task keeps the fence until the
//! apply worker finishes the snapshot.
//!
//! The interleaving is driven by a test-only gate inside the worker's
//! `CreateSnapshot` branch, so the test confirms "queued and in progress"
//! before cancelling. Blocking the worker with a cold namespace guard is no
//! longer a valid driver: the snapshot body reads the shared view and a closed
//! view makes it fail immediately instead of waiting.

use super::*;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_runtime::common::Utils;
use curvine_runtime::sync::channel::CallChannel;
use std::time::{Duration, Instant};

#[test]
fn cancelling_the_caller_keeps_the_fence_until_the_snapshot_applies() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-cancel-gate-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let gate = fs.metadata_commit_gate();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    // Hold the apply worker inside the snapshot branch before it starts.
    let (release, entered) = snapshot_apply_gate::install(&loader.meta_data_dir);

    let snapshot_loader = loader.clone();
    let snapshot = runtime.spawn(async move { snapshot_loader.create_snapshot().await });

    // The barrier closes admission before the message is queued.
    let deadline = Instant::now() + Duration::from_secs(5);
    while gate.try_enter().is_some() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        gate.try_enter().is_none(),
        "snapshot caller must close admission before queueing"
    );

    // Confirm the worker is parked inside the snapshot branch before queueing
    // the marker; otherwise the marker could be served first.
    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("the apply worker must reach the snapshot branch");

    // An acknowledged no-op queued behind the snapshot cannot finish while the
    // worker is parked in the snapshot branch.
    let (tx, rx) = CallChannel::channel();
    runtime
        .block_on(loader.apply(false, ApplyMsg::new_entry_with_ack(Default::default(), tx)))
        .unwrap();
    let mut marker = runtime.spawn(async move { rx.receive().await });
    assert!(
        runtime
            .block_on(async { tokio::time::timeout(Duration::from_millis(200), &mut marker).await })
            .is_err(),
        "the apply queue must be blocked behind the in-progress snapshot"
    );

    // Cancel the caller: the inner task must keep the fence.
    snapshot.abort();
    let _ = runtime.block_on(snapshot);
    assert!(
        gate.try_enter().is_none(),
        "a cancelled caller must not release the snapshot fence"
    );

    // Release the worker and let both the snapshot and the marker finish.
    drop(release);
    let marker_result = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(10), &mut marker).await })
        .expect("apply queue must resume after the snapshot completes")
        .expect("marker task must not panic");
    marker_result.expect("the queued no-op must succeed after the snapshot");

    let deadline = Instant::now() + Duration::from_secs(10);
    while gate.try_enter().is_none() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        gate.try_enter().is_some(),
        "the fence must reopen once the snapshot completed"
    );

    drop(fs);
    journal.shutdown();
}
