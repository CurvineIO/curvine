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

use crossbeam::utils::CachePadded;
use parking_lot::{Condvar, Mutex};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

const CLOSED: usize = 1usize << (usize::BITS - 1);
const IN_FLIGHT_MASK: usize = !CLOSED;

/// The exclusive snapshot writer always occupies this slot; ordinary writers
/// use their per-thread slot instead.
const EXCLUSIVE_SLOT: usize = 0;

/// Upper bound on the slot table; `available_parallelism` clamps into it.
const MAX_SLOTS: usize = 64;

/// A fixed table of independently padded per-slot in-flight counters.
///
/// Ordinary writers fight over their own slot's cache line instead of a
/// single shared one; the CLOSED bit lives in every slot word so a close
/// fences each slot through its own atomic modification order (design §6).
struct SlotTable {
    slots: Box<[CachePadded<AtomicUsize>]>,
    /// `slots.len() - 1`; `mask & seed` picks a slot.
    mask: usize,
}

impl SlotTable {
    /// One gate, one fixed table: `available_parallelism` clamped to
    /// 8..=64 and rounded up to a power of two (8 when unavailable). The
    /// table never resizes — thread migration or cgroup changes do not
    /// re-index it, and no per-RPC allocation happens.
    fn new() -> Self {
        let count = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(8)
            .clamp(8, MAX_SLOTS)
            .next_power_of_two();
        Self {
            slots: (0..count)
                .map(|_| CachePadded::new(AtomicUsize::new(0)))
                .collect(),
            mask: count - 1,
        }
    }

    #[inline]
    fn get(&self, slot: usize) -> &AtomicUsize {
        &self.slots[slot]
    }

    #[inline]
    fn len(&self) -> usize {
        self.slots.len()
    }
}

/// One seed per thread, drawn once: `seed & gate.mask` selects the slot.
/// `Relaxed` suffices — the value is only a hash input, and the slot's own
/// atomics carry every real ordering guarantee.
fn thread_seed() -> usize {
    thread_local! {
        static SEED: Cell<usize> = const { Cell::new(0) };
        static INITIALIZED: Cell<bool> = const { Cell::new(false) };
    }
    SEED.with(|seed| {
        if !INITIALIZED.with(Cell::get) {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            // Stride of 2 keeps consecutive thread seeds distinct after the
            // |1 (odd) pinning: 0,2,4,6 -> 1,3,5,7. A plain +1 would make
            // 2k and 2k+1 collide to the same seed. wrapping semantics: the
            // seed is only a hash input, so a wrap-around merely changes
            // the slot mapping, not correctness.
            seed.set(COUNTER.fetch_add(2, Ordering::Relaxed).wrapping_add(1) | 1);
            INITIALIZED.with(|flag| flag.set(true));
        }
        seed.get()
    })
}

/// Coordinates a bounded set of mutators with an occasional quiescent read.
///
/// The owner chooses the consistency domain by using a distinct gate per
/// domain. The primitive itself intentionally has no namespace-specific
/// behavior.
///
/// State layout (S7): one fixed slot table holds every ordinary writer's
/// count and its own CLOSED bit; the lifecycle keeps the close-reference
/// count, the exclusive flag, and the generation. `global_closed` is only a
/// fast-path hint — a slot's own CLOSED bit is the final authority.
pub struct CommitGate {
    /// `CLOSED` hint bit for `enter`/`try_enter` fast paths; derives from
    /// the lifecycle close-reference count inside the lifecycle lock.
    global_closed: CachePadded<AtomicUsize>,
    slots: CachePadded<SlotTable>,
    lifecycle: Mutex<CommitGateLifecycle>,
    changed: Condvar,
    /// Test-only one-shot hook parked where the last in-flight writer is
    /// about to complete its leave under a pending close: the closer has
    /// checked non-zero and is about to wait on the Condvar.
    #[cfg(test)]
    before_last_leave_hook:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    /// Test-only one-shot hook parked inside `try_enter` after the FIRST
    /// global-hint read and before the first slot CAS: the stale-hint
    /// window (hint open, close fencing the slots next).
    #[cfg(test)]
    before_first_cas_hook:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

#[derive(Default)]
struct CommitGateLifecycle {
    closed: usize,
    exclusive: bool,
    generation: u64,
}

impl CommitGate {
    pub fn new() -> Self {
        Self {
            global_closed: CachePadded::new(AtomicUsize::new(0)),
            slots: CachePadded::new(SlotTable::new()),
            lifecycle: Mutex::new(CommitGateLifecycle::default()),
            changed: Condvar::new(),
            #[cfg(test)]
            before_last_leave_hook: Mutex::new(None),
            #[cfg(test)]
            before_first_cas_hook: Mutex::new(None),
        }
    }

    /// Test-only: the slot a fresh admission would enter (same TLS seed
    /// path as `enter`), for cross-thread slot-distinctness proofs.
    #[cfg(test)]
    pub(crate) fn test_probe_slot(&self) -> usize {
        self.slot_for()
    }

    /// Test-only: the number of slots (retry budget bound for proofs).
    #[cfg(test)]
    pub(crate) fn test_slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Test-only one-shot arrival/continue hook parked at the LAST
    /// leave-check inside `leave` when the close is pending but the count
    /// is not yet zero — i.e. exactly before the closer's Condvar wait can
    /// be satisfied. Fired once (take-once), then the real leave continues.
    #[cfg(test)]
    pub(crate) fn arm_before_last_leave_hook(
        &self,
        reached: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self.before_last_leave_hook.lock() = Some((reached, release));
    }

    /// Test-only: arms the stale-hint-window hook (one CAS attempt).
    #[cfg(test)]
    pub(crate) fn arm_before_first_cas_hook(
        &self,
        reached: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self.before_first_cas_hook.lock() = Some((reached, release));
    }

    /// Test-only: consumes the armed stale-hint-window hook, if any.
    #[cfg(test)]
    pub(crate) fn consume_before_first_cas_hook(
        &self,
    ) -> Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)> {
        self.before_first_cas_hook.lock().take()
    }

    /// Test-only: consumes the armed before-last-leave hook, if any.
    #[cfg(test)]
    pub(crate) fn consume_before_last_leave_hook(
        &self,
    ) -> Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)> {
        self.before_last_leave_hook.lock().take()
    }

    #[inline]
    fn slot_for(&self) -> usize {
        thread_seed() & self.slots.mask
    }

    #[inline]
    fn global_hint(&self) -> usize {
        self.global_closed.load(Ordering::Acquire)
    }

    pub fn enter(&self) -> CommitGateGuard<'_> {
        loop {
            let slot = self.slot_for();
            if self.global_hint() & CLOSED != 0 {
                let mut lifecycle = self.lifecycle.lock();
                while self.global_hint() & CLOSED != 0 {
                    self.changed.wait(&mut lifecycle);
                }
                continue;
            }

            // The global hint is open, but the slot's own bit is the final
            // authority: the Acquire keeps a close that fenced this slot
            // before the hint flipped visible to this decision.
            let slot_state = self.slots.get(slot).load(Ordering::Acquire);
            if slot_state & CLOSED != 0 {
                // The slot fenced after the hint read: re-read the global
                // hint (it may already be set) and fall back to the wait.
                if self.global_hint() & CLOSED != 0 {
                    continue;
                }
                // Hint open but slot fenced in a transient close that has
                // not reopened yet: wait like a globally closed gate.
                let mut lifecycle = self.lifecycle.lock();
                while self.slots.get(slot).load(Ordering::Acquire) & CLOSED != 0 {
                    self.changed.wait(&mut lifecycle);
                }
                continue;
            }

            assert!(
                slot_state & IN_FLIGHT_MASK < IN_FLIGHT_MASK,
                "namespace commit gate in-flight overflow"
            );
            if self
                .slots
                .get(slot)
                .compare_exchange_weak(
                    slot_state,
                    slot_state + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return CommitGateGuard { gate: self, slot };
            }
        }
    }

    /// Attempts a non-blocking writer admission with a BOUNDED retry
    /// budget: at most one weak CAS per slot in the table (N = 8..64,
    /// no configuration). Every failed CAS re-reads the global hint with
    /// Acquire; a close observed mid-retry (or the budget exhausted)
    /// returns `None`.
    ///
    /// `None` can happen while the gate is open under sustained
    /// contention: the current production caller is the best-effort atime
    /// refresh, which simply skips that one refresh on `None`. A caller
    /// that cannot tolerate skipping must use the blocking `enter`.
    /// Successful admissions keep their linearization point at the CAS and
    /// are visible to every close barrier.
    pub fn try_enter(&self) -> Option<CommitGateGuard<'_>> {
        // Walk the slot table instead of spinning on one slot: the retry
        // budget is bounded by the fixed slot count, and moving on after a
        // lost race spreads contention instead of hammering one line.
        for offset in 0..self.slots.len() {
            let slot = (self.slot_for() + offset) & self.slots.mask;
            // The global hint first: a closed gate rejects without touching
            // the lifecycle Mutex or the Condvar.
            if self.global_hint() & CLOSED != 0 {
                return None;
            }
            #[cfg(test)]
            if offset == 0 {
                // Stale-hint window: the hint said open, the slot CAS has
                // not run yet. A close can fence the slots in exactly this
                // gap; the CAS below must then fail and the re-read must
                // reject.
                if let Some((reached, release)) = self.consume_before_first_cas_hook() {
                    let _ = reached.send(());
                    let _ = release.recv_timeout(std::time::Duration::from_secs(60));
                }
            }

            let mut slot_state = self.slots.get(slot).load(Ordering::Acquire);
            loop {
                if slot_state & CLOSED != 0 {
                    // The slot's own CLOSED bit is the final authority.
                    return None;
                }
                if self.global_hint() & CLOSED != 0 {
                    return None;
                }

                assert!(
                    slot_state & IN_FLIGHT_MASK < IN_FLIGHT_MASK,
                    "namespace commit gate in-flight overflow"
                );
                match self.slots.get(slot).compare_exchange_weak(
                    slot_state,
                    slot_state + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        return Some(CommitGateGuard { gate: self, slot });
                    }
                    Err(current) => {
                        // Budgeted: move to the next slot after one lost
                        // race on this one.
                        break;
                    }
                }
            }
        }
        None
    }

    /// Snapshot intent holds exactly one exclusive writer while the journal
    /// applier captures its immutable image. Ordinary closed barriers alone
    /// do not authorize creating an image from a missing operation path.
    pub fn has_exclusive_writer(&self) -> bool {
        self.lifecycle.lock().exclusive
    }

    pub fn close_and_wait(&self) -> CommitGateBarrier<'_> {
        self.close_and_wait0();
        CommitGateBarrier { gate: self }
    }

    /// Starts an exclusive operation only while the gate is open.
    ///
    /// The returned guard both keeps new writers out and contributes one
    /// in-flight writer. A role-change barrier therefore waits for an active
    /// snapshot rather than treating a closed gate as already quiescent.
    pub fn close_and_enter_if_open(&self) -> Option<CommitGateExclusiveGuard<'_>> {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.closed != 0 {
            return None;
        }

        lifecycle.closed = 1;
        let generation = lifecycle.generation;
        // Exclusive close publishes the hint first (spec §5): fast paths
        // see the fence before any slot state matters.
        self.global_closed.fetch_or(CLOSED, Ordering::AcqRel);
        self.fence_all_slots();
        while self.any_in_flight() {
            self.changed.wait(&mut lifecycle);
        }

        // A lifecycle fence may close and reopen while the snapshot waits.
        // The generation detects that transition even after its guard is gone.
        if lifecycle.closed != 1 || lifecycle.generation != generation {
            lifecycle.closed -= 1;
            if lifecycle.closed == 0 {
                self.open_all_slots();
                self.changed.notify_all();
            }
            return None;
        }

        // The exclusive writer occupies the fixed slot 0 with a real count:
        // ordinary barriers wait for it like any other in-flight writer.
        let previous = self
            .slots
            .get(EXCLUSIVE_SLOT)
            .fetch_add(1, Ordering::AcqRel);
        assert_eq!(
            previous, CLOSED,
            "exclusive namespace commit gate entered with active writers"
        );
        lifecycle.exclusive = true;
        Some(CommitGateExclusiveGuard { gate: self })
    }

    /// Closes the gate until the returned RAII guard is dropped.
    ///
    /// This variant owns the gate reference so a lifecycle controller can keep
    /// writes closed across asynchronous state transitions.
    pub fn close_and_wait_owned(self: &std::sync::Arc<Self>) -> CommitGateOwnedBarrier {
        let barrier = self.close_owned();
        self.wait_for_quiescence();
        barrier
    }

    /// Stops admitting new writers immediately. The caller must wait for
    /// quiescence before reading or replacing state protected by this gate.
    pub fn close_owned(self: &std::sync::Arc<Self>) -> CommitGateOwnedBarrier {
        self.close0();
        CommitGateOwnedBarrier { gate: self.clone() }
    }

    /// Safe as a restore barrier only while a closing guard is held.
    pub fn wait_for_quiescence(&self) {
        let mut lifecycle = self.lifecycle.lock();
        while self.any_in_flight() {
            self.changed.wait(&mut lifecycle);
        }
    }

    fn close_and_wait0(&self) {
        self.close0();
        self.wait_for_quiescence();
    }

    fn close0(&self) {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.generation = lifecycle
            .generation
            .checked_add(1)
            .expect("namespace commit gate generation overflow");
        lifecycle.closed = lifecycle
            .closed
            .checked_add(1)
            .expect("namespace commit gate close overflow");
        if lifecycle.closed == 1 {
            // Two-phase fence (S7): publish the global hint first, then
            // fence every slot. close_owned returns only after every slot
            // is fenced; the global hint alone is never "all slots empty".
            self.global_closed.fetch_or(CLOSED, Ordering::AcqRel);
            self.fence_all_slots();
        }
    }

    fn leave(&self, slot: usize) {
        let previous = self.slots.get(slot).fetch_sub(1, Ordering::Release);
        assert!(
            previous & IN_FLIGHT_MASK > 0,
            "namespace commit gate in-flight underflow"
        );
        if previous & CLOSED != 0 {
            #[cfg(test)]
            {
                // A writer left while a close is pending: a proof that
                // parks the closer must observe THIS boundary (the leave
                // is in flight, the Condvar wait is not yet satisfiable
                // unless this is the gate-wide last writer). One-shot.
                if let Some((reached, release)) = self.consume_before_last_leave_hook() {
                    let _ = reached.send(());
                    let _ = release.recv_timeout(std::time::Duration::from_secs(60));
                }
            }
            if previous & IN_FLIGHT_MASK == 1 {
                // The slot's last writer left while a close is pending:
                // notify under the lifecycle lock so a closer that just
                // observed 1 and is about to park cannot miss the wakeup.
                let _lifecycle = self.lifecycle.lock();
                self.changed.notify_all();
            }
        }
    }

    /// Fences every slot: the effective close point is when the LAST slot
    /// flips. `AcqRel` pairs with an admission's `Acquire` so a CAS that
    /// lost the race either keeps its count fenced-visible or fails.
    fn fence_all_slots(&self) {
        for slot in 0..self.slots.len() {
            self.slots.get(slot).fetch_or(CLOSED, Ordering::AcqRel);
        }
    }

    /// Clears the CLOSED bit of every slot while KEEPING each count: a
    /// close_owned may drop before the drain, and old writers that are still
    /// alive must keep their counts so no later barrier misses them. Never
    /// store(0).
    fn open_all_slots(&self) {
        for slot in 0..self.slots.len() {
            self.slots
                .get(slot)
                .fetch_and(IN_FLIGHT_MASK, Ordering::Release);
        }
        self.global_closed
            .fetch_and(IN_FLIGHT_MASK, Ordering::Release);
        self.changed.notify_all();
    }

    fn open_one(&self) {
        let mut lifecycle = self.lifecycle.lock();
        assert!(
            lifecycle.closed > 0,
            "namespace commit gate close underflow"
        );
        lifecycle.closed -= 1;
        if lifecycle.closed == 0 {
            self.open_all_slots();
        }
    }

    fn leave_exclusive(&self) {
        // The exclusive writer always occupies slot 0 (§5).
        let previous = self
            .slots
            .get(EXCLUSIVE_SLOT)
            .fetch_sub(1, Ordering::Release);
        assert!(
            previous & CLOSED != 0 && previous & IN_FLIGHT_MASK > 0,
            "exclusive namespace commit gate in-flight underflow"
        );

        let mut lifecycle = self.lifecycle.lock();
        lifecycle.exclusive = false;
        self.changed.notify_all();
        assert!(
            lifecycle.closed > 0,
            "namespace commit gate close underflow"
        );
        lifecycle.closed -= 1;
        if lifecycle.closed == 0 {
            self.open_all_slots();
        }
    }

    fn any_in_flight(&self) -> bool {
        (0..self.slots.len())
            .any(|slot| self.slots.get(slot).load(Ordering::Acquire) & IN_FLIGHT_MASK != 0)
    }
}

impl Default for CommitGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CommitGateGuard<'a> {
    gate: &'a CommitGate,
    /// The slot this admission actually entered; Drop subtracts from THIS
    /// slot regardless of which thread drops the guard.
    slot: usize,
}

#[cfg(test)]
impl CommitGateGuard<'_> {
    /// Test-only: the slot this admission actually entered (distinct-slot
    /// proofs).
    pub(crate) fn test_slot(&self) -> usize {
        self.slot
    }
}

impl Drop for CommitGateGuard<'_> {
    fn drop(&mut self) {
        self.gate.leave(self.slot);
    }
}

pub struct CommitGateBarrier<'a> {
    gate: &'a CommitGate,
}

pub struct CommitGateExclusiveGuard<'a> {
    gate: &'a CommitGate,
}

impl Drop for CommitGateExclusiveGuard<'_> {
    fn drop(&mut self) {
        self.gate.leave_exclusive();
    }
}

impl Drop for CommitGateBarrier<'_> {
    fn drop(&mut self) {
        self.gate.open_one();
    }
}

pub struct CommitGateOwnedBarrier {
    gate: std::sync::Arc<CommitGate>,
}

impl Drop for CommitGateOwnedBarrier {
    fn drop(&mut self) {
        self.gate.open_one();
    }
}

#[cfg(test)]
#[path = "tests/namespace_commit_gate_interleaving_test.rs"]
mod namespace_commit_gate_interleaving_test;
