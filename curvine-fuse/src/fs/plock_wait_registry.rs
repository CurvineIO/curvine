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
    pub fn would_deadlock(&self, waiter: &LockOwner, blocked_by: &LockOwner) -> bool {
        let map = self.waiters.lock().expect("plock wait registry poisoned");
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
        if self.registry.would_deadlock(&self.owner, &blocked_by) {
            return true;
        }
        self.registry.register(self.owner.clone(), blocked_by);
        false
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
}
