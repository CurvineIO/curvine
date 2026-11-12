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

// Lifecycle tests for the S6-a namespace view holder: hot guards block cold
// exclusivity until released (including cross-thread), cold windows reject
// new views, timeouts and panics never reopen a half-mutated view, and every
// exit path restores access or fails closed.

use crate::master::journal::JournalWriter;
use crate::master::meta::fs_dir_handle::{ColdError, FsDirHandle, NamespaceError};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::FsDir;
use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
use crate::master::quota::eviction::EvictionConf;
use crate::master::Master;
use crate::master::MasterMonitor;
use curvine_config::ClusterConf;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::Utils;
use curvine_runtime::sync::StateCtl;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

/// Builds a real `FsDir` through the same construction path as the journal
/// system's `build_fs_parts`, in testing mode (no journal threads), and hands
/// the caller the journal writer for fence assertions.
fn build(name: &str) -> (FsDirHandle, Arc<JournalWriter>) {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("fs-dir-handle-{}-{}", name, Utils::rand_str(8)));
    let rt = conf.journal.create_runtime();
    let monitor = MasterMonitor::new(StateCtl::new(0), StateCtl::new(0));
    let client = RaftClient::from_conf(rt, &conf.journal);
    let writer =
        Arc::new(JournalWriter::new(true, client, &conf.journal, monitor.recovery()).unwrap());
    let ttl_bucket_list =
        Arc::new(TtlBucketList::new(conf.master.ttl_bucket_interval_ms() as i64).unwrap());
    let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(EvictionConf::from_conf(&conf)));
    let fs_dir = FsDir::new(&conf, writer.clone(), ttl_bucket_list, evictor).unwrap();
    (FsDirHandle::new(fs_dir), writer)
}

#[test]
fn existing_view_blocks_cold_until_released() {
    let (handle, _) = build("held-view");
    let held = handle.view().unwrap();
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(50)),
        Err(ColdError::DrainTimeout)
    ));
    // The timed-out cold window restored access to the untouched view.
    assert!(handle.view().is_ok());
    drop(held);
    let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    cold.view().store.ensure_available().unwrap();
    drop(cold);
    assert!(handle.view().is_ok());
}

#[test]
fn cross_thread_view_blocks_cold_until_release() {
    let (handle, _) = build("cross-thread");
    let handle = Arc::new(handle);
    let reader_handle = Arc::clone(&handle);
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let reader = std::thread::spawn(move || {
        let view = reader_handle.view().unwrap();
        held_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(view);
    });
    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // A live guard on another thread blocks the cold owner.
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(50)),
        Err(ColdError::DrainTimeout)
    ));
    release_tx.send(()).unwrap();
    reader.join().unwrap();
    // Only after the release does exclusivity succeed.
    let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    cold.view().store.ensure_available().unwrap();
    drop(cold);
    assert!(handle.view().is_ok());
}

#[test]
fn cold_window_rejects_new_views_and_second_owner() {
    let (handle, _) = build("cold-window");
    let cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    assert!(matches!(handle.view(), Err(NamespaceError::Busy)));
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(10)),
        Err(ColdError::Busy)
    ));
    drop(cold);
    assert!(handle.view().is_ok());
}

#[test]
fn invalid_timeout_touches_nothing() {
    let (handle, _) = build("invalid-timeout");
    assert!(matches!(
        handle.begin_cold(Duration::MAX),
        Err(ColdError::InvalidTimeout)
    ));
    // The old view is still readable: no slot was emptied by the overflow.
    assert!(handle.view().is_ok());
}

#[test]
fn cold_error_exit_restores_the_same_view() {
    let (handle, _) = build("error-exit");
    let next_id = handle.view().unwrap().last_inode_id() + 1;
    let result: Result<(), ()> = (|| {
        let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
        cold.view().update_last_inode_id(next_id).unwrap();
        Err(())
    })();
    assert!(result.is_err());
    assert_eq!(handle.view().unwrap().last_inode_id(), next_id);
}

#[test]
fn panic_in_cold_window_fails_closed() {
    let (handle, writer) = build("panic-fail-closed");
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
        cold.view();
        panic!("injected cold-window panic");
    }));
    assert!(result.is_err());
    // Fail-closed: reads are rejected and the journal fence engaged, so the
    // possibly half-mutated view is never handed back out.
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
    assert!(matches!(
        handle.begin_cold(Duration::from_secs(1)),
        Err(ColdError::Unavailable)
    ));
    assert!(writer.ensure_metadata_current().is_err());
}

#[test]
fn explicit_failure_is_not_a_busy_cold_window() {
    let (handle, writer) = build("explicit-fail-closed");
    handle
        .begin_cold(Duration::from_secs(1))
        .unwrap()
        .fail_closed();
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
    assert!(matches!(
        handle.begin_cold(Duration::from_secs(1)),
        Err(ColdError::Unavailable)
    ));
    assert!(writer.ensure_metadata_current().is_err());
}

#[test]
fn cold_guard_leaves_no_stale_strong_reference() {
    let (handle, _) = build("no-stale");
    let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    cold.view().store.ensure_available().unwrap();
    drop(cold);
    assert!(handle.view().is_ok());
    // A second cold run still proves unique ownership without waiting for
    // any timeout: no dropped guard leaked an `Arc`.
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(20)),
        Ok(_)
    ));
}

// ---------------------------------------------------------------------------
// Terminal failure capability.
//
// `FsDirHandle::fail_closed(writer)` is the entry a failed confirmation uses
// when it may not own the view. The reopen path is shared by a normal cold
// drop and a drain timeout; a terminal failure may land at any point of it.
// ---------------------------------------------------------------------------

/// Terminal from OPEN: one-way FAILED, view and cold rejected, parked view
/// retired, writer fenced, cursor invalidated, and a repeat call never revives.
#[test]
fn terminal_failure_from_open_retires_the_view_and_fences() {
    let (handle, writer) = build("terminal-open");
    let view = handle.view().unwrap();
    assert!(
        view.store.cursor_store_available(),
        "fixture: the cursor store starts available"
    );

    handle.fail_closed(&writer);

    assert!(handle.is_failed());
    assert!(
        !view.store.cursor_store_available(),
        "the retired view's cursor store must be invalidated"
    );
    drop(view);
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
    assert!(matches!(
        handle.begin_cold(Duration::from_secs(1)),
        Err(ColdError::Unavailable)
    ));
    assert!(
        handle.slot_is_empty(),
        "terminal must retract the parked view instead of leaving a late slot"
    );
    assert!(
        writer.ensure_metadata_current().is_err(),
        "terminal must fence the writer unconditionally"
    );

    // Idempotent: a second terminal call must not revive anything.
    handle.fail_closed(&writer);
    assert!(handle.is_failed());
    assert!(handle.slot_is_empty());
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
}

/// Terminal while another cold owner is draining: the drain must retire the
/// view it took and report the terminal state, never a new owner.
#[test]
fn terminal_during_drain_retires_the_taken_view() {
    let (handle, writer) = build("terminal-drain");
    let handle = Arc::new(handle);
    let writer = Arc::new(writer);
    let held = handle.view().unwrap();

    let drain_handle = Arc::clone(&handle);
    let drain =
        std::thread::spawn(move || drain_handle.begin_cold(Duration::from_secs(5)).map(|_| ()));

    // Wait until the drain is in the cold state *and* has taken the slot:
    // the state flips before the swap, so both must be observed.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !(handle.is_cold() && handle.slot_is_empty()) {
        assert!(
            std::time::Instant::now() < deadline,
            "the drain never took the view into the cold state"
        );
        std::thread::yield_now();
    }

    // The confirmation fails while the drain still holds the view.
    handle.fail_closed(&writer);
    drop(held);

    let outcome = drain.join().expect("drain thread must not panic");
    assert!(
        matches!(outcome, Err(ColdError::Unavailable)),
        "a draining cold acquisition must report the terminal state, got: {outcome:?}"
    );
    assert!(handle.is_failed());
    assert!(
        handle.slot_is_empty(),
        "no late view may be published after the terminal failure"
    );
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
}

/// Terminal injected at each fixed point of the reopen path, for both the
/// normal drop and the drain-timeout path. In every combination the final
/// state is FAILED with no late slot.
#[test]
fn terminal_at_every_reopen_step_leaves_no_late_slot() {
    const POINTS: [&str; 4] = ["before_store", "after_store", "before_cas", "after_cas"];
    for point in POINTS {
        // Path 1: normal drop publishes the taken view back.
        {
            let (handle, writer) = build(&format!("terminal-drop-{point}"));
            let handle = Arc::new(handle);
            let writer = Arc::new(writer);
            let hook_handle = Arc::clone(&handle);
            let hook_writer = Arc::clone(&writer);
            handle.set_reopen_hook(Box::new(move |fired| {
                if fired == point {
                    hook_handle.fail_closed(&hook_writer);
                }
            }));
            let cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
            drop(cold);
            assert!(
                handle.is_failed(),
                "drop path at {point}: the terminal must win"
            );
            assert!(
                handle.slot_is_empty(),
                "drop path at {point}: no late slot may remain"
            );
            assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
        }

        // Path 2: the drain times out and publishes the same view back.
        {
            let (handle, writer) = build(&format!("terminal-timeout-{point}"));
            let handle = Arc::new(handle);
            let writer = Arc::new(writer);
            let hook_handle = Arc::clone(&handle);
            let hook_writer = Arc::clone(&writer);
            handle.set_reopen_hook(Box::new(move |fired| {
                if fired == point {
                    hook_handle.fail_closed(&hook_writer);
                }
            }));
            let held = handle.view().unwrap();
            let outcome = handle.begin_cold(Duration::from_millis(20));
            // At `after_cas` the reopen CAS has already succeeded, so the
            // reopen linearizes before the terminal and the original
            // DrainTimeout is the honest result. Before that point the
            // terminal wins and must be reported.
            let expected = if point == "after_cas" {
                matches!(outcome, Err(ColdError::DrainTimeout))
            } else {
                matches!(outcome, Err(ColdError::Unavailable))
            };
            assert!(
                expected,
                "timeout path at {point}: unexpected outcome (terminal must win \
                 unless the reopen already completed)"
            );
            drop(held);
            assert!(handle.is_failed());
            assert!(
                handle.slot_is_empty(),
                "timeout path at {point}: no late slot may remain"
            );
            assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
        }
    }
}

/// Without a terminal failure the drain timeout still restores the same view
/// and reports the original DrainTimeout.
#[test]
fn drain_timeout_without_terminal_still_reopens_the_same_view() {
    let (handle, _) = build("timeout-no-terminal");
    let next_id = handle.view().unwrap().last_inode_id() + 1;
    {
        let held = handle.view().unwrap();
        assert_eq!(held.last_inode_id(), next_id - 1);
        assert!(matches!(
            handle.begin_cold(Duration::from_millis(20)),
            Err(ColdError::DrainTimeout)
        ));
    }
    assert!(!handle.is_failed());
    assert!(
        !handle.slot_is_empty(),
        "the same view must be back in the slot"
    );
    assert_eq!(handle.view().unwrap().last_inode_id(), next_id - 1);
}
