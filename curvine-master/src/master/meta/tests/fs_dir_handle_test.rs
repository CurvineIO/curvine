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
//
// Interleavings are real cross-thread handovers: the reopen thread announces
// the fixed point over a channel and blocks there, the driver thread performs
// the terminal failure, then releases the reopen thread. Assertions run after
// all threads are joined and the hook is cleared, so no failure unwinds
// through a live guard or leaks the FsDir behind a reference cycle.
// ---------------------------------------------------------------------------

/// Terminal from OPEN: one-way FAILED, view and cold rejected, parked view
/// retired, writer fenced, cursor invalidated, and a repeat call never revives.
#[test]
fn terminal_failure_from_open_retires_the_view_and_fences() {
    let (handle, writer) = build("terminal-open");
    let view = handle.view().unwrap();
    let cursor = view.store.cursor_store();
    assert!(
        cursor.get_applied_req(77).is_ok(),
        "fixture: the cursor store starts usable"
    );

    handle.fail_closed(&writer);

    assert!(handle.is_failed());
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
    let error = cursor
        .get_applied_req(77)
        .expect_err("the terminal must have invalidated the cursor store");
    assert!(
        error.to_string().contains("cursor store unavailable"),
        "unexpected cursor error: {error}"
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
    let held = handle.view().unwrap();
    let cursor = held.store.cursor_store();
    assert!(cursor.get_applied_req(78).is_ok());

    // The drain runs on its own thread and reports through a channel. `handle`
    // is owned here, so the thread borrows nothing from this frame beyond the
    // channel.
    let (drain_done_tx, drain_done_rx) = mpsc::channel();
    let drain_thread = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || {
            let outcome = handle.begin_cold(Duration::from_secs(5)).map(|_| ());
            let _ = drain_done_tx.send(outcome);
        })
    };

    // Wait until the drain is in the cold state *and* has taken the slot:
    // the state flips before the swap, so both must be observed. Whether the
    // window was really reached is asserted only after the resources are
    // settled, so a missed deadline cannot panic through live guards.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut drain_took_view = false;
    while std::time::Instant::now() < deadline {
        if handle.is_cold() && handle.slot_is_empty() {
            drain_took_view = true;
            break;
        }
        std::thread::yield_now();
    }

    // The confirmation fails while the drain still holds the view.
    handle.fail_closed(&writer);
    drop(held);

    let outcome = drain_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the drain thread must finish after the terminal");
    drain_thread
        .join()
        .expect("the drain thread must not panic");
    // Assertions only after the drain thread is finished and joined.
    assert!(
        drain_took_view,
        "fixture: the drain must really take the view into the cold state"
    );
    assert!(
        matches!(outcome, Err(ColdError::Unavailable)),
        "a draining cold acquisition must report the terminal state"
    );
    assert!(handle.is_failed());
    assert!(
        handle.slot_is_empty(),
        "no late view may be published after the terminal failure"
    );
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
    assert!(matches!(
        handle.begin_cold(Duration::from_secs(1)),
        Err(ColdError::Unavailable)
    ));
    let error = cursor
        .get_applied_req(78)
        .expect_err("the retired view's cursor store must be invalidated");
    assert!(error.to_string().contains("cursor store unavailable"));
    assert!(writer.ensure_metadata_current().is_err());
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

/// Terminal at each reopen point, driven by a real cross-thread handover: the
/// reopen thread parks at the fixed point, the driver thread performs the
/// terminal there and releases it. Every combination asserts the outcome per
/// linearization and the full final state after all threads are joined:
/// FAILED, no late slot, cursor invalidated, view and cold rejected, writer
/// fenced.
#[test]
fn terminal_at_every_reopen_point_asserts_final_state() {
    const POINTS: [&str; 4] = ["before_store", "after_store", "before_cas", "after_cas"];
    for point in POINTS {
        for path in ["drop", "timeout"] {
            let (handle, writer) = build(&format!("terminal-final-{path}-{point}"));
            let handle = Arc::new(handle);
            let (reached_tx, reached_rx) = mpsc::channel::<&'static str>();
            let (release_tx, release_rx) = mpsc::channel::<()>();
            handle.set_reopen_hook(point, reached_tx, release_rx);
            let cursor = handle.view().unwrap().store.cursor_store();
            assert!(cursor.get_applied_req(80).is_ok());

            let (reopen_done_tx, reopen_done_rx) =
                mpsc::channel::<Result<Result<(), ColdError>, String>>();
            let reopen = {
                let handle = Arc::clone(&handle);
                std::thread::spawn(move || {
                    let outcome = if path == "drop" {
                        match handle.begin_cold(Duration::from_secs(1)) {
                            Ok(cold) => {
                                drop(cold);
                                Ok(Ok(()))
                            }
                            Err(e) => Err(format!("begin_cold failed: {e}")),
                        }
                    } else {
                        let held = handle.view().unwrap();
                        let outcome = handle.begin_cold(Duration::from_millis(20));
                        drop(held);
                        Ok(outcome.map(|_| ()))
                    };
                    let _ = reopen_done_tx.send(outcome);
                })
            };

            // The reopen thread parks at the fixed point (or reports that it
            // never got there).
            let reached = reached_rx.recv_timeout(Duration::from_secs(5));

            // The driver performs the terminal from another real thread. Its
            // completion is observed through a bounded done channel; even a
            // panicking terminal must not leave the parked thread stuck, so
            // the release happens regardless of the terminal's fate.
            let (terminal_done_tx, terminal_done_rx) = mpsc::channel();
            let terminal = {
                let handle = Arc::clone(&handle);
                let writer = writer.clone();
                std::thread::spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle.fail_closed(&writer)
                    }));
                    let _ = terminal_done_tx.send(outcome.is_ok());
                })
            };
            let terminal_ok = terminal_done_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or(false);

            // Release the parked thread; if the point was never reached, drop
            // the release end so the hook falls through and the reopen thread
            // can still finish.
            match reached {
                Ok(_) => {
                    let _ = release_tx.send(());
                }
                Err(_) => drop(release_tx),
            }
            // Bounded completion observation of the reopen thread before
            // joining it.
            let outcome = match reopen_done_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(outcome) => outcome,
                Err(_) => Err("reopen thread did not finish in time".to_string()),
            };
            reopen.join().expect("reopen thread must not panic");
            terminal.join().expect("terminal thread must not panic");
            handle.clear_reopen_hook();

            // The fixed point must actually have been reached: a scenario
            // whose hook never fired cannot pass on its final state alone.
            assert!(
                matches!(reached, Ok(fired) if fired == point),
                "{path} at {point}: the reopen path never reached the fixed point"
            );
            assert!(
                terminal_ok,
                "{path} at {point}: the terminal thread must complete cleanly"
            );

            // Outcome per path and linearization. A drop itself never returns
            // an error, so its outcome is always "reopen ok": the terminal's
            // effect on a drop is asserted through the final state below. A
            // timeout reports its result: at `after_cas` the reopen CAS
            // already succeeded and the caller keeps the original
            // DrainTimeout; before that point the terminal wins.
            match (path, point) {
                ("drop", _) => assert!(
                    matches!(&outcome, Ok(Ok(()))),
                    "drop at {point}: reopen must complete"
                ),
                ("timeout", "after_cas") => assert!(
                    matches!(&outcome, Ok(Err(ColdError::DrainTimeout))),
                    "timeout at {point}: original DrainTimeout must be reported, got {outcome:?}"
                ),
                ("timeout", _) => assert!(
                    matches!(&outcome, Ok(Err(ColdError::Unavailable))),
                    "timeout at {point}: terminal must be reported, got {outcome:?}"
                ),
                _ => unreachable!(),
            }

            // Final state, asserted after all threads are joined and the hook
            // is cleared.
            assert!(handle.is_failed(), "{path} at {point}: must be FAILED");
            assert!(
                handle.slot_is_empty(),
                "{path} at {point}: no late slot may remain"
            );
            assert!(
                matches!(handle.view(), Err(NamespaceError::Unavailable)),
                "{path} at {point}: reads must be rejected"
            );
            assert!(
                matches!(
                    handle.begin_cold(Duration::from_millis(20)),
                    Err(ColdError::Unavailable)
                ),
                "{path} at {point}: cold windows must be rejected"
            );
            let error = cursor
                .get_applied_req(80)
                .expect_err("the cursor store must end invalidated");
            assert!(
                error.to_string().contains("cursor store unavailable"),
                "{path} at {point}: unexpected cursor error: {error}"
            );
            assert!(
                writer.ensure_metadata_current().is_err(),
                "{path} at {point}: the writer must stay fenced"
            );
        }
    }
}
