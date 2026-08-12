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

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

// The reentrancy guard only exists in debug builds. `std::sync::RwLock` is
// writer-preferring on Linux: once a writer is queued, a new `read()` blocks.
// A thread that already holds a read guard and re-acquires the same lock can
// deadlock. Release builds pay zero cost; debug/test builds panic loudly at the
// reentrant acquisition instead of silently wedging the whole lock.
#[cfg(debug_assertions)]
mod reentry_guard {
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicU64, Ordering};

    static LOCK_ID_SEQ: AtomicU64 = AtomicU64::new(1);

    thread_local! {
        static HELD_LOCKS: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn next_id() -> u64 {
        LOCK_ID_SEQ.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn assert_not_reentrant(id: u64, kind: &str) {
        HELD_LOCKS.with(|held| {
            if held.borrow().contains(&id) {
                panic!(
                    "ArcRwLock reentrant {kind} on lock {id}: current thread already holds a \
                     guard. std::sync::RwLock is writer-preferring, so a reentrant acquire can \
                     deadlock when a writer is queued. Resolve the path/data before re-locking."
                );
            }
        });
    }

    pub(super) fn mark_held(id: u64) {
        HELD_LOCKS.with(|held| held.borrow_mut().push(id));
    }

    pub(super) fn unmark_held(id: u64) {
        let _ = HELD_LOCKS.try_with(|held| {
            let mut held = held.borrow_mut();
            if let Some(pos) = held.iter().rposition(|x| *x == id) {
                held.remove(pos);
            }
        });
    }
}

pub struct ArcRwLock<T> {
    inner: Arc<RwLock<T>>,
    #[cfg(debug_assertions)]
    id: u64,
}

impl<T> ArcRwLock<T> {
    pub fn new(s: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(s)),
            #[cfg(debug_assertions)]
            id: reentry_guard::next_id(),
        }
    }

    pub fn read(&self) -> ArcRwLockReadGuard<'_, T> {
        #[cfg(debug_assertions)]
        reentry_guard::assert_not_reentrant(self.id, "read");
        let guard = self.inner.read().unwrap();
        #[cfg(debug_assertions)]
        reentry_guard::mark_held(self.id);
        ArcRwLockReadGuard {
            guard,
            #[cfg(debug_assertions)]
            id: self.id,
        }
    }

    pub fn write(&self) -> ArcRwLockWriteGuard<'_, T> {
        #[cfg(debug_assertions)]
        reentry_guard::assert_not_reentrant(self.id, "write");
        let guard = self.inner.write().unwrap();
        #[cfg(debug_assertions)]
        reentry_guard::mark_held(self.id);
        ArcRwLockWriteGuard {
            guard,
            #[cfg(debug_assertions)]
            id: self.id,
        }
    }
}

impl<T> Clone for ArcRwLock<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            #[cfg(debug_assertions)]
            id: self.id,
        }
    }
}

impl<T> Deref for ArcRwLock<T> {
    type Target = RwLock<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct ArcRwLockReadGuard<'a, T> {
    guard: RwLockReadGuard<'a, T>,
    #[cfg(debug_assertions)]
    id: u64,
}

impl<T> Deref for ArcRwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> Drop for ArcRwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        reentry_guard::unmark_held(self.id);
    }
}

pub struct ArcRwLockWriteGuard<'a, T> {
    guard: RwLockWriteGuard<'a, T>,
    #[cfg(debug_assertions)]
    id: u64,
}

impl<T> Deref for ArcRwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> DerefMut for ArcRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<T> Drop for ArcRwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        reentry_guard::unmark_held(self.id);
    }
}

pub struct ArcMutex<T>(Arc<Mutex<T>>);

impl<T> ArcMutex<T> {
    pub fn new(s: T) -> Self {
        Self(Arc::new(Mutex::new(s)))
    }

    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock().unwrap()
    }
}

impl<T> Clone for ArcMutex<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Deref for ArcMutex<T> {
    type Target = Mutex<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type SyncRwLock<T> = Arc<RwLock<T>>;

pub type SyncMutex<T> = Arc<Mutex<T>>;

pub struct Sync;

impl Sync {
    pub fn rw_lock<T>(source: T) -> SyncRwLock<T> {
        Arc::new(RwLock::new(source))
    }

    pub fn mutex<T>(source: T) -> SyncMutex<T> {
        Arc::new(Mutex::new(source))
    }
}
