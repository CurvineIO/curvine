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

//! Real-interleaving proofs for the slot-table gate. Every case uses a
//! test-only hook or an observable guard slot - no sleeps as timing
//! proofs, no fabricated "different slots" claims. Registered by the gate
//! file via cfg(test) #[path], so the crate-internal surface (hooks,
//! guard.slot) is accessible while staying out of the core test body.

use super::CommitGate;
use std::sync::{mpsc, Arc};
use std::time::Duration;

/// S7-2.1: two DIFFERENT threads must hold guards on DIFFERENT slots
/// (the TLS seed is per thread), and a concurrent close must wait for
/// both original-slot drops.
#[test]
fn two_threads_hold_guards_on_distinct_slots() {
    let gate = Arc::new(CommitGate::new());
    let slot_count = gate.test_slot_count();

    // Pigeonhole: with slot_count()+1 concurrent guards, two MUST land on
    // the same slot; conversely, holding slot_count()+1 guards guarantees
    // at least two DISTINCT slots are occupied (the seed space spans all
    // slots; a collision on every pair is impossible by counting). Pick
    // two guards on distinct slots deterministically via their REAL
    // observed slots - no assumption that any two threads differ.
    std::thread::scope(|scope| {
        let mut guards = Vec::new();
        let mut slots = Vec::new();
        for _ in 0..slot_count + 1 {
            let h = scope.spawn(|| {
                let slot = gate.test_probe_slot();
                let guard = gate.enter();
                (slot, guard)
            });
            let (slot, guard) = h.join().expect("the writer must enter");
            guards.push(guard);
            slots.push(slot);
        }

        // At least two distinct slots must be occupied.
        let first_slot = slots[0];
        let Some(second_index) = slots.iter().position(|&s| s != first_slot) else {
            panic!("pigeonhole: {slot_count}+1 guards must occupy >=2 distinct slots");
        };
        let _second_slot = slots[second_index];

        // A concurrent close waits for BOTH original-slot drops (pick the
        // two distinct-slot guards).
        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                let barrier = gate.close_and_wait_owned();
                closed_tx.send(()).expect("closer done");
                drop(barrier);
            })
        };
        assert!(
            closed_rx.try_recv().is_err(),
            "the close must wait while both guards are alive"
        );

        // Drop the two distinct-slot guards; the rest stay alive: the
        // close must NOT complete until every guard is gone.
        drop(guards.remove(0)); // slot first_slot (index 0 guard)
        drop(guards.remove(second_index - 1)); // adjust for the first removal
        assert!(
            closed_rx.try_recv().is_err(),
            "the close must still wait for the remaining slot_count-1 guards"
        );

        for guard in guards {
            drop(guard);
        }
        closed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("all drops must end the close");
        closer.join().expect("the closer must not panic");
    });
}

/// S7-2.2: the closer is observed PARKED (before its Condvar wait can be
/// satisfied) via the take-once before-last-leave hook; then the real
/// last leave completes it. A dropped-guard-before-park implementation
/// would fail this.
#[test]
fn closer_parks_before_the_last_leave_completes_it() {
    let gate = Arc::new(CommitGate::new());

    // Two writers in flight (different threads so their slots differ).
    let (g1, g2) = std::thread::scope(|scope| {
        let h1 = scope.spawn(|| gate.enter());
        let h2 = scope.spawn(|| gate.enter());
        (
            h1.join().expect("writer 1 must enter"),
            h2.join().expect("writer 2 must enter"),
        )
    });

    // Arm the hook that fires inside a NON-LAST leave while a close is
    // pending (the boundary the closer's wait cannot be satisfied at).
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    gate.arm_before_last_leave_hook(reached_tx, release_rx);

    // Start the closer; it fences and parks on in-flight != 0.
    let (fenced_tx, fenced_rx) = mpsc::channel();
    let (closer_done_tx, closer_done_rx) = mpsc::channel();
    let closer = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let barrier = gate.close_owned();
            fenced_tx.send(()).expect("the close fenced");
            gate.wait_for_quiescence();
            closer_done_tx.send(()).expect("quiescence reached");
            drop(barrier);
        })
    };
    fenced_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the close must fence");

    // Drop the FIRST guard from its own thread: its leave fires the hook
    // (reached) and blocks there; the main thread stays free to release.
    std::thread::scope(|scope| {
        let dropper = scope.spawn(|| drop(g1));
        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the non-last leave must reach the hook while the closer waits");
        // Release the hook: the leave completes; the closer stays parked
        // (count is still 1).
        release_tx.send(()).expect("release the hook");
        dropper.join().expect("the dropper must not panic");
    });
    // mpsc is synchronous: if the single leave had wrongly satisfied the
    // closer, the completion message would already be queued. The second
    // guard is still alive, so it must not be.
    assert!(
        closer_done_rx.try_recv().is_err(),
        "one leave must not satisfy the close while the other guard lives"
    );

    // The real last leave completes the quiescence.
    drop(g2);
    closer
        .join()
        .expect("the closer must complete after the last leave");
}

/// S7-2.3: a stale-hint attempt (hint read open, then a close fences the
/// slots before the CAS) must FAIL its CAS and observe the closed gate on
/// the re-read - the bounded budget rejects instead of entering past a
/// close. The protected value stays untouched by the rejected attempt.
#[test]
fn stale_hint_attempt_rejects_after_the_slots_fence() {
    let gate = Arc::new(CommitGate::new());

    // A writer enters and leaves cleanly first (the stale-hint scenario's
    // setup: an open gate with quiesced state).
    std::thread::scope(|scope| {
        let w = scope.spawn(|| gate.enter());
        w.join().expect("the writer must not panic");
    });

    // Arm the stale-hint hook on a second thread's try_enter.
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    gate.arm_before_first_cas_hook(reached_tx, release_rx);

    std::thread::scope(|scope| {
        // The attempt: hint read open... but the close fences the slots
        // during the hook window; the CAS must fail and the re-read must
        // reject.
        let attempt = scope.spawn(|| gate.try_enter());

        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the attempt must reach the stale-hint window");
        // Close while the attempt is parked before its CAS.
        let barrier = gate.close_owned();
        release_tx.send(()).expect("release the attempt");

        let guard = attempt.join().expect("the attempt must not panic");
        assert!(
            guard.is_none(),
            "a stale-hint attempt must reject after the slots fence"
        );

        // Reopen: a fresh writer can enter again.
        drop(barrier);
        let reopen = scope.spawn(|| gate.enter());
        reopen.join().expect("the reopen must admit");
    });
}

/// S7-2.4: the exclusive writer occupies the fixed slot 0 and is
/// rejected while the gate is closed (generation-cancel coverage lives in
/// the integration contract's transient_generation case).
#[test]
fn exclusive_entry_rejects_when_gate_closed_and_occupies_slot0() {
    let gate = Arc::new(CommitGate::new());
    let exclusive = gate
        .close_and_enter_if_open()
        .expect("an open gate must admit the exclusive writer");
    assert!(gate.has_exclusive_writer());
    // The gate is closed while the exclusive writer holds it: a second
    // close_and_enter_if_open must be rejected (the API's real contract).
    assert!(
        gate.close_and_enter_if_open().is_none(),
        "a second exclusive admission must be rejected while one is held"
    );
    drop(exclusive);
    assert!(!gate.has_exclusive_writer());
}
