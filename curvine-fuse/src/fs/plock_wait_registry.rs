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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

/// Identity of a POSIX advisory lock owner (FUSE client + kernel lock owner).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LockOwner {
    pub client_id: String,
    pub owner_id: u64,
}

impl LockOwner {
    pub fn new(client_id: impl Into<String>, owner_id: u64) -> Self {
        Self {
            client_id: client_id.into(),
            owner_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LockWaitInfo {
    pub request_unique: u64,
    pub pid: u32,
    pub nodeid: u64,
    pub path: String,
    pub start: u64,
    pub end: u64,
}

impl LockWaitInfo {
    pub fn new(
        request_unique: u64,
        pid: u32,
        nodeid: u64,
        path: impl Into<String>,
        start: u64,
        end: u64,
    ) -> Self {
        Self {
            request_unique,
            pid,
            nodeid,
            path: path.into(),
            start,
            end,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LockWaitEdge {
    pub waiter: LockOwner,
    pub blocker: LockOwner,
    pub info: LockWaitInfo,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlockDeadlockCycle {
    pub edges: Vec<LockWaitEdge>,
    pub victim: LockOwner,
    pub victim_request_unique: u64,
}

impl PlockDeadlockCycle {
    fn new(edges: Vec<LockWaitEdge>) -> Self {
        debug_assert!(!edges.is_empty());

        let mut victim_idx = 0;
        for (idx, edge) in edges.iter().enumerate().skip(1) {
            let current = &edges[victim_idx];
            // Use a total order over (request_unique, client_id, owner_id) so
            // the victim is a pure function of the cycle contents, independent
            // of traversal order.  Strict > ensures that equal keys keep the
            // first-encountered maximum, which is deterministic.
            if edge.info.request_unique > current.info.request_unique
                || (edge.info.request_unique == current.info.request_unique
                    && edge.waiter.client_id > current.waiter.client_id)
                || (edge.info.request_unique == current.info.request_unique
                    && edge.waiter.client_id == current.waiter.client_id
                    && edge.waiter.owner_id > current.waiter.owner_id)
            {
                victim_idx = idx;
            }
        }

        Self {
            victim: edges[victim_idx].waiter.clone(),
            victim_request_unique: edges[victim_idx].info.request_unique,
            edges,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PlockWaitDecision {
    Wait {
        edge: LockWaitEdge,
        cycle: Option<PlockDeadlockCycle>,
    },
    Deadlock {
        edge: LockWaitEdge,
        cycle: PlockDeadlockCycle,
    },
}

impl PlockWaitDecision {
    pub fn is_deadlock(&self) -> bool {
        matches!(self, Self::Deadlock { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WaitRecord {
    blocked_by: LockOwner,
    info: LockWaitInfo,
}

/// Tracks in-flight F_SETLKW waiters so circular wait chains return EDEADLK.
///
/// Each registration is keyed by `(LockOwner, request_unique)` so that
/// multiple concurrent `F_SETLKW` requests sharing the same owner do not
/// overwrite each other's edges.
pub(crate) struct PlockWaitRegistry {
    waiters: Mutex<HashMap<(LockOwner, u64), WaitRecord>>,
    change_tx: watch::Sender<u64>,
    change_rx: watch::Receiver<u64>,
}

impl Default for PlockWaitRegistry {
    fn default() -> Self {
        let (change_tx, change_rx) = watch::channel(0u64);
        Self {
            waiters: Mutex::default(),
            change_tx,
            change_rx,
        }
    }
}

impl PlockWaitRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn register(&self, waiter: LockOwner, request_unique: u64, blocked_by: LockOwner) {
        self.waiters
            .lock()
            .expect("plock wait registry poisoned")
            .insert(
                (waiter, request_unique),
                WaitRecord {
                    blocked_by,
                    info: Self::empty_wait_info(),
                },
            );
    }

    pub fn unregister(&self, waiter: &LockOwner, request_unique: u64) {
        let removed = self
            .waiters
            .lock()
            .expect("plock wait registry poisoned")
            .remove(&(waiter.clone(), request_unique))
            .is_some();
        if removed {
            self.notify_waiters();
        }
    }

    pub fn notify_waiters(&self) {
        // Increment the generation counter so every waiter (including those
        // that have not yet registered their `changed()` future) observes the
        // change when they next call `wait_for_change`.
        let _ = self.change_tx.send(self.change_tx.borrow().wrapping_add(1));
    }

    pub async fn wait_for_change(&self, timeout: Duration) {
        // Clone the receiver so the cloned copy starts at the current
        // generation. If `notify_waiters` incremented it between the caller's
        // Master sample and this call, `changed()` returns immediately.
        let mut rx = self.change_rx.clone();
        let _ = tokio::time::timeout(timeout, rx.changed()).await;
    }

    /// Returns true when following blocked-by edges from `blocked_by` reaches `waiter`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn would_deadlock(&self, waiter: &LockOwner, blocked_by: &LockOwner) -> bool {
        let map = self.waiters.lock().expect("plock wait registry poisoned");
        Self::cycle_closed_by(
            &map,
            LockWaitEdge {
                waiter: waiter.clone(),
                blocker: blocked_by.clone(),
                info: Self::empty_wait_info(),
            },
        )
        .is_some()
    }

    /// Find the first wait record whose owner matches `owner`, if any.
    fn find_record(
        map: &HashMap<(LockOwner, u64), WaitRecord>,
        owner: &LockOwner,
    ) -> Option<WaitRecord> {
        map.iter()
            .find(|((o, _), _)| o == owner)
            .map(|(_, r)| r.clone())
    }

    /// Atomically replace `waiter -> blocked_by` and detect a wait cycle. When a
    /// cycle is present, the waiter with the highest FUSE request order in that
    /// cycle is selected as the deterministic EDEADLK victim.
    pub fn register_blocked_by(
        &self,
        waiter: LockOwner,
        request_unique: u64,
        blocked_by: LockOwner,
        info: LockWaitInfo,
    ) -> PlockWaitDecision {
        let mut map = self.waiters.lock().expect("plock wait registry poisoned");
        // Drop any prior edge for this specific request before walking the
        // graph so a stale self-edge cannot create a false cycle, and so the
        // new blocker is published atomically with the deadlock check.
        map.remove(&(waiter.clone(), request_unique));

        let edge = LockWaitEdge {
            waiter: waiter.clone(),
            blocker: blocked_by.clone(),
            info: info.clone(),
        };
        let cycle = Self::cycle_closed_by(&map, edge.clone());
        map.insert(
            (waiter.clone(), request_unique),
            WaitRecord { blocked_by, info },
        );

        drop(map);

        match cycle {
            Some(cycle) if cycle.victim == waiter => PlockWaitDecision::Deadlock { edge, cycle },
            Some(cycle) => {
                // Wake the selected victim so it can return EDEADLK promptly
                // instead of waiting for the next timed retry.
                self.notify_waiters();
                PlockWaitDecision::Wait {
                    edge,
                    cycle: Some(cycle),
                }
            }
            None => PlockWaitDecision::Wait { edge, cycle: None },
        }
    }

    fn cycle_closed_by(
        map: &HashMap<(LockOwner, u64), WaitRecord>,
        new_edge: LockWaitEdge,
    ) -> Option<PlockDeadlockCycle> {
        let cycle_start = new_edge.waiter.clone();
        let mut current = new_edge.blocker.clone();
        let mut visited = HashSet::new();
        let mut edges = vec![new_edge];

        loop {
            if current == cycle_start {
                return Some(PlockDeadlockCycle::new(edges));
            }
            if !visited.insert(current.clone()) {
                return None;
            }
            match Self::find_record(map, &current) {
                Some(record) => {
                    edges.push(LockWaitEdge {
                        waiter: current.clone(),
                        blocker: record.blocked_by.clone(),
                        info: record.info.clone(),
                    });
                    current = record.blocked_by.clone();
                }
                None => return None,
            }
        }
    }

    fn empty_wait_info() -> LockWaitInfo {
        LockWaitInfo::new(0, 0, 0, "", 0, 0)
    }
}

pub(crate) struct PlockWaitGuard {
    registry: Arc<PlockWaitRegistry>,
    owner: LockOwner,
    request_unique: u64,
    info: LockWaitInfo,
}

impl PlockWaitGuard {
    pub fn new(registry: Arc<PlockWaitRegistry>, owner: LockOwner, info: LockWaitInfo) -> Self {
        Self {
            request_unique: info.request_unique,
            registry,
            owner,
            info,
        }
    }

    pub fn register_blocked_by(&self, blocked_by: LockOwner) -> PlockWaitDecision {
        self.registry.register_blocked_by(
            self.owner.clone(),
            self.request_unique,
            blocked_by,
            self.info.clone(),
        )
    }

    pub fn clear_blocked_by(&self) {
        self.registry.unregister(&self.owner, self.request_unique);
    }
}

impl Drop for PlockWaitGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.owner, self.request_unique);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait_info(request_unique: u64) -> LockWaitInfo {
        LockWaitInfo::new(request_unique, request_unique as u32, 1, "/locks", 0, 4)
    }

    #[test]
    fn detects_two_process_cycle() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);

        reg.register(a.clone(), 1, b.clone());
        assert!(reg.would_deadlock(&b, &a));
        assert!(!reg.would_deadlock(&a, &b));
    }

    #[test]
    fn no_deadlock_without_waiters() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);
        assert!(!reg.would_deadlock(&a, &b));
    }

    #[test]
    fn guard_unregisters_on_drop() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);
        {
            let guard = PlockWaitGuard::new(reg.clone(), a.clone(), wait_info(1));
            assert!(!guard.register_blocked_by(b.clone()).is_deadlock());
        }
        assert!(!reg.would_deadlock(&b, &a));
    }

    #[test]
    fn register_blocked_by_rejects_opposite_edge_after_first_insert() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);

        assert!(!reg
            .register_blocked_by(a.clone(), 1, b.clone(), wait_info(1))
            .is_deadlock());
        assert!(reg
            .register_blocked_by(b.clone(), 2, a.clone(), wait_info(2))
            .is_deadlock());
    }

    #[test]
    fn clearing_waiter_removes_stale_blocked_by_edge() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);

        assert!(!reg
            .register_blocked_by(a.clone(), 1, b.clone(), wait_info(1))
            .is_deadlock());
        reg.unregister(&a, 1);
        assert!(!reg.would_deadlock(&b, &a));
    }

    #[test]
    fn guard_clear_blocked_by_removes_wait_edge() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);
        let guard = PlockWaitGuard::new(reg.clone(), a.clone(), wait_info(1));

        assert!(!guard.register_blocked_by(b.clone()).is_deadlock());
        guard.clear_blocked_by();
        assert!(!reg.would_deadlock(&b, &a));
    }

    #[test]
    fn stores_direct_blocker_even_when_blocker_is_waiting() {
        let reg = PlockWaitRegistry::new();
        let holder = LockOwner::new("c", 1);
        let mid = LockOwner::new("c", 2);
        let waiter = LockOwner::new("c", 3);

        // Store the current Master conflict, not the chain root. fcntl17 can
        // first report child1 as child3's blocker, then child2 after child1
        // unlocks; collapsing child2 -> child3 to child1 hides the later cycle.
        assert!(!reg
            .register_blocked_by(mid.clone(), 1, holder.clone(), wait_info(1))
            .is_deadlock());
        assert!(!reg
            .register_blocked_by(waiter.clone(), 2, mid.clone(), wait_info(2))
            .is_deadlock());
        let map = reg.waiters.lock().unwrap();
        let record = PlockWaitRegistry::find_record(&map, &waiter);
        assert_eq!(record.map(|r| r.blocked_by), Some(mid));
    }

    #[test]
    fn opposite_waiter_edges_still_detect_cycle_with_direct_edges() {
        // Registry-level A↔B remains a cycle (fcntl17 needs this). The SETLKW
        // path re-samples Master after a transient cycle so fcntl34 unlock/re-
        // lock races do not surface EDEADLK to userspace.
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 10);
        let b = LockOwner::new("c", 20);

        assert!(!reg
            .register_blocked_by(a.clone(), 1, b.clone(), wait_info(1))
            .is_deadlock());
        assert!(reg
            .register_blocked_by(b.clone(), 2, a.clone(), wait_info(2))
            .is_deadlock());
    }

    #[test]
    fn reverse_registration_must_not_deadlock_earlier_request() {
        let reg = PlockWaitRegistry::new();
        let child2 = LockOwner::new("c", 2);
        let child3 = LockOwner::new("c", 3);

        // fcntl17 sends child2's SETLKW before child3's SETLKW. If the later
        // child3 request is polled first and registers child3 -> child2, the
        // earlier child2 request must not become the EDEADLK victim just because
        // it is the second registry mutation.
        assert!(!reg
            .register_blocked_by(child3.clone(), 3, child2.clone(), wait_info(3))
            .is_deadlock());
        let child2_decision =
            reg.register_blocked_by(child2.clone(), 2, child3.clone(), wait_info(2));
        match child2_decision {
            PlockWaitDecision::Wait {
                cycle: Some(cycle), ..
            } => {
                assert_eq!(cycle.victim, child3);
                assert_eq!(cycle.victim_request_unique, 3);
            }
            other => panic!("expected child2 to wait on child3 victim, got {other:?}"),
        }

        assert!(reg
            .register_blocked_by(child3.clone(), 3, child2.clone(), wait_info(3))
            .is_deadlock());
    }

    #[test]
    fn delayed_third_owner_unlock_still_finds_direct_cycle() {
        let reg = PlockWaitRegistry::new();
        let child1 = LockOwner::new("c", 1);
        let child2 = LockOwner::new("c", 2);
        let child3 = LockOwner::new("c", 3);

        assert!(!reg
            .register_blocked_by(child3.clone(), 3, child1.clone(), wait_info(3))
            .is_deadlock());
        assert!(!reg
            .register_blocked_by(child2.clone(), 2, child3.clone(), wait_info(2))
            .is_deadlock());

        let child3_decision =
            reg.register_blocked_by(child3.clone(), 3, child2.clone(), wait_info(3));
        match child3_decision {
            PlockWaitDecision::Deadlock { cycle, .. } => {
                assert_eq!(cycle.victim, child3);
                assert_eq!(cycle.victim_request_unique, 3);
            }
            other => panic!("expected child3 to be EDEADLK victim, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn notify_waiters_wakes_blocked_retry() {
        let reg = PlockWaitRegistry::new();
        let waiter = reg.clone();

        let task = tokio::spawn(async move {
            waiter.wait_for_change(Duration::from_secs(30)).await;
        });
        tokio::task::yield_now().await;
        reg.notify_waiters();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("waiter should wake before timeout")
            .expect("waiter task should finish");
    }
}
