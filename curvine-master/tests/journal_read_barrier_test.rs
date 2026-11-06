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

use curvine_master::master::journal::JournalReadBarrier;
use std::sync::{Arc, Barrier};

#[test]
fn read_barrier_requires_explicit_catch_up() {
    let barrier = JournalReadBarrier::new();
    assert!(barrier.ensure_current().is_ok());
    barrier.begin_catch_up(10, 12);
    assert!(barrier.ensure_current().is_err());
    barrier.advance_applied(12);
    assert!(barrier.ensure_current().is_err());
    barrier.advance_catch_up(11);
    assert!(barrier.ensure_current().is_err());
    barrier.advance_catch_up(12);
    assert!(barrier.ensure_current().is_ok());
    barrier.require_catch_up(15);
    barrier.require_catch_up(14);
    barrier.advance_catch_up(14);
    assert!(!barrier.is_current());
    barrier.advance_catch_up(15);
    assert!(barrier.is_current());
    barrier.begin_catch_up(5, 6);
    assert!(!barrier.is_current());
}

#[test]
fn read_barrier_fence_rejects_until_catch_up() {
    let barrier = JournalReadBarrier::new();
    barrier.advance_catch_up(41);
    assert!(barrier.is_current());

    barrier.require_catch_up(100);
    barrier.fence();
    barrier.advance_catch_up(42);
    barrier.unfence();
    assert!(barrier.ensure_current().is_err());
    barrier.advance_catch_up(100);
    assert!(barrier.ensure_current().is_ok());

    barrier.fence();
    barrier.begin_catch_up(100, 100);
    assert!(barrier.ensure_current().is_err());
    barrier.unfence();
    assert!(barrier.ensure_current().is_ok());
}

#[test]
fn role_epoch_rejects_requests_from_before_a_completed_fence() {
    let barrier = JournalReadBarrier::new();
    let old_epoch = barrier.current_epoch().unwrap();
    barrier.fence();
    assert!(barrier.ensure_epoch(old_epoch).is_err());
    barrier.unfence();
    assert!(barrier.ensure_current().is_ok());
    assert!(barrier.ensure_epoch(old_epoch).is_err());
    let new_epoch = barrier.current_epoch().unwrap();
    assert_ne!(old_epoch, new_epoch);
    assert!(barrier.ensure_epoch(new_epoch).is_ok());
}

#[test]
fn concurrent_updates_cannot_mark_unapplied_entries_current() {
    for _ in 0..100 {
        let barrier = Arc::new(JournalReadBarrier::new());
        let start = Arc::new(Barrier::new(3));
        let a = barrier.clone();
        let a_start = start.clone();
        let require = std::thread::spawn(move || {
            a_start.wait();
            a.require_catch_up(100);
        });
        let b = barrier.clone();
        let b_start = start.clone();
        let advance = std::thread::spawn(move || {
            b_start.wait();
            b.advance_catch_up(50);
        });
        start.wait();
        require.join().unwrap();
        advance.join().unwrap();
        barrier.advance_applied(100);
        assert!(!barrier.is_current());
        barrier.advance_catch_up(100);
        assert!(barrier.is_current());
    }
}

#[test]
fn writer_gate_blocks_writes_until_catch_up_completes() {
    use curvine_master::master::journal::JournalWriter;
    use curvine_master::Master;

    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir("journal-writer-gate");
    let rt = conf.journal.create_runtime();
    let client = curvine_raft::raft::RaftClient::from_conf(rt, &conf.journal);
    let writer = JournalWriter::new(
        true,
        client,
        &conf.journal,
        curvine_master::master::journal::RecoveryGate::new(),
    )
    .unwrap();

    // Testing writer: barrier defaults to current and enqueue never blocks.
    assert!(writer.is_metadata_current());
    writer.ensure_metadata_current().unwrap();

    // Simulate a follower promotion race: catch-up starts behind commit.
    writer.begin_metadata_catch_up(5, 10);
    assert!(writer.ensure_metadata_current().is_err());
    writer.advance_metadata_catch_up(10);
    assert!(writer.is_metadata_current());
    writer.advance_metadata_applied(11);
    assert!(writer.is_metadata_current());
}
