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

/// Tracks in-flight F_SETLKW waiters so circular wait chains return EDEADLK.
#[derive(Default)]
pub(crate) struct PlockWaitRegistry {
    waiters: Mutex<HashMap<LockOwner, LockOwner>>,
}

impl PlockWaitRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn register(&self, waiter: LockOwner, blocked_by: LockOwner) {
        self.waiters
            .lock()
            .expect("plock wait registry poisoned")
            .insert(waiter, blocked_by);
    }

    pub fn unregister(&self, waiter: &LockOwner) {
        self.waiters
            .lock()
            .expect("plock wait registry poisoned")
            .remove(waiter);
    }

    /// Returns true when following blocked-by edges from `blocked_by` reaches `waiter`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn would_deadlock(&self, waiter: &LockOwner, blocked_by: &LockOwner) -> bool {
        let map = self.waiters.lock().expect("plock wait registry poisoned");
        Self::reaches_waiter(&map, waiter, blocked_by)
    }

    /// Atomically replace `waiter -> blocked_by` and detect a wait cycle.
    /// Returns true when the edge would close a cycle (EDEADLK).
    ///
    /// Edges are resolved to the root non-waiting owner (follow `blocked_by`'s
    /// chain). That keeps the graph pointing at presumed holders and avoids
    /// waiter↔waiter cycles that appear when an OFD/POSIX holder unlocks and
    /// immediately re-enters `F_SETLKW` while another waiter still has a stale
    /// edge to the previous holder (LTP `fcntl34` multi-thread OFD pattern).
    pub fn try_register_blocked_by(&self, waiter: LockOwner, blocked_by: LockOwner) -> bool {
        let mut map = self.waiters.lock().expect("plock wait registry poisoned");
        // Drop any prior edge for this waiter before walking the graph so a
        // stale self-edge cannot create a false cycle, and so the new blocker
        // is published atomically with the deadlock check.
        map.remove(&waiter);

        let root = match Self::root_blocker(&map, &waiter, &blocked_by) {
            Ok(root) => root,
            Err(()) => return true,
        };
        if Self::reaches_waiter(&map, &waiter, &root) {
            return true;
        }
        map.insert(waiter, root);
        false
    }

    /// Follow wait edges from `blocked_by` until a non-waiter (presumed holder).
    /// Returns `Err(())` when the existing graph already cycles through `waiter`.
    fn root_blocker(
        map: &HashMap<LockOwner, LockOwner>,
        waiter: &LockOwner,
        blocked_by: &LockOwner,
    ) -> Result<LockOwner, ()> {
        let mut root = blocked_by.clone();
        let mut seen = HashSet::new();
        while let Some(next) = map.get(&root).cloned() {
            if &root == waiter || &next == waiter {
                return Err(());
            }
            if !seen.insert(root.clone()) {
                // Cycle among other waiters; treat as deadlock.
                return Err(());
            }
            root = next;
        }
        Ok(root)
    }

    fn reaches_waiter(
        map: &HashMap<LockOwner, LockOwner>,
        waiter: &LockOwner,
        blocked_by: &LockOwner,
    ) -> bool {
        let mut current = blocked_by.clone();
        let mut visited = HashSet::new();
        loop {
            if &current == waiter {
                return true;
            }
            if !visited.insert(current.clone()) {
                return false;
            }
            match map.get(&current) {
                Some(next) => current = next.clone(),
                None => return false,
            }
        }
    }
}

pub(crate) struct PlockWaitGuard {
    registry: Arc<PlockWaitRegistry>,
    owner: LockOwner,
}

impl PlockWaitGuard {
    pub fn new(registry: Arc<PlockWaitRegistry>, owner: LockOwner) -> Self {
        Self { registry, owner }
    }

    pub fn register_blocked_by(&self, blocked_by: LockOwner) -> bool {
        self.registry
            .try_register_blocked_by(self.owner.clone(), blocked_by)
    }

    pub fn clear_blocked_by(&self) {
        self.registry.unregister(&self.owner);
    }
}

impl Drop for PlockWaitGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_two_process_cycle() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);

        reg.register(a.clone(), b.clone());
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
            let guard = PlockWaitGuard::new(reg.clone(), a.clone());
            assert!(!guard.register_blocked_by(b.clone()));
        }
        assert!(!reg.would_deadlock(&b, &a));
    }

    #[test]
    fn try_register_blocked_by_rejects_opposite_edge_after_first_insert() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);

        assert!(!reg.try_register_blocked_by(a.clone(), b.clone()));
        assert!(reg.try_register_blocked_by(b.clone(), a.clone()));
    }

    #[test]
    fn clearing_waiter_removes_stale_blocked_by_edge() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);

        assert!(!reg.try_register_blocked_by(a.clone(), b.clone()));
        reg.unregister(&a);
        assert!(!reg.would_deadlock(&b, &a));
    }

    #[test]
    fn guard_clear_blocked_by_removes_wait_edge() {
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 1);
        let b = LockOwner::new("c", 2);
        let guard = PlockWaitGuard::new(reg.clone(), a.clone());

        assert!(!guard.register_blocked_by(b.clone()));
        guard.clear_blocked_by();
        assert!(!reg.would_deadlock(&b, &a));
    }

    #[test]
    fn resolves_edge_through_waiter_chain_to_holder() {
        let reg = PlockWaitRegistry::new();
        let holder = LockOwner::new("c", 1);
        let mid = LockOwner::new("c", 2);
        let waiter = LockOwner::new("c", 3);

        // mid waits on holder; a new waiter blocked by mid should wait on holder.
        assert!(!reg.try_register_blocked_by(mid.clone(), holder.clone()));
        assert!(!reg.try_register_blocked_by(waiter.clone(), mid.clone()));
        let map = reg.waiters.lock().unwrap();
        assert_eq!(map.get(&waiter), Some(&holder));
    }

    #[test]
    fn opposite_waiter_edges_still_detect_cycle_after_root_resolve() {
        // Registry-level A↔B remains a cycle (fcntl17 needs this). The SETLKW
        // path re-samples Master after a transient cycle so fcntl34 unlock/re-
        // lock races do not surface EDEADLK to userspace.
        let reg = PlockWaitRegistry::new();
        let a = LockOwner::new("c", 10);
        let b = LockOwner::new("c", 20);

        assert!(!reg.try_register_blocked_by(a.clone(), b.clone()));
        assert!(reg.try_register_blocked_by(b.clone(), a.clone()));
    }
}
