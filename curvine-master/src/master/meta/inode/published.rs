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

//! A value published to lock-free readers and replaced by one writer at a time.
//!
//! Readers pin the thread's epoch and read through the guard. Pinning touches
//! only thread-local state, so a read writes no shared word. A writer swaps the
//! pointer and hands the old value to the collector, which frees it once every
//! reader pinned before the swap has left.
//!
//! This replaces `ArcSwap`, whose `store` walks the debt slots of every thread
//! that ever loaded, so its cost grows with the number of reader threads. In a
//! same-directory write that cost sits inside the one serial section: measured
//! with 16 reader threads, `ArcSwap::store` took 3.7us against 0.36us for an
//! epoch pointer swap (64 readers: 45.6us against 1.8us).

use crossbeam::epoch::{self, Atomic, Guard, Owned};
use std::ops::Deref;
use std::sync::atomic::Ordering;

pub(crate) struct Published<T> {
    slot: Atomic<T>,
    /// `None` uses the process-wide collector. Tests give each slot its own, so
    /// threads pinned by unrelated tests cannot hold its garbage back.
    #[cfg(test)]
    collector: Option<epoch::Collector>,
}

/// A reader's view of the published value, valid while the guard lives.
pub(crate) struct Snapshot<'a, T> {
    value: &'a T,
    _guard: Guard,
}

impl<T> Published<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            slot: Atomic::new(value),
            #[cfg(test)]
            collector: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_collector(value: T, collector: epoch::Collector) -> Self {
        Self {
            slot: Atomic::new(value),
            collector: Some(collector),
        }
    }

    fn pin(&self) -> Guard {
        #[cfg(test)]
        if let Some(collector) = &self.collector {
            return collector.register().pin();
        }
        epoch::pin()
    }

    pub(crate) fn load(&self) -> Snapshot<'_, T> {
        let guard = self.pin();
        let shared = self.slot.load(Ordering::Acquire, &guard);
        // SAFETY: `slot` is never null after `new`, and the value it pointed to
        // is only freed after every guard pinned before the swap has dropped.
        // The guard is stored beside the reference, so the value outlives it.
        let value = unsafe { &*(shared.as_raw()) };
        Snapshot {
            value,
            _guard: guard,
        }
    }

    /// Replaces the value. The caller serializes writers (the children write
    /// lock), so no compare-and-swap loop is needed.
    pub(crate) fn store(&self, value: T) {
        let guard = self.pin();
        let old = self.slot.swap(Owned::new(value), Ordering::AcqRel, &guard);
        // SAFETY: `old` was unlinked by the swap; readers that already hold it
        // are pinned, and deferred destruction waits for them.
        unsafe { guard.defer_destroy(old) };
    }
}

impl<T> Deref for Snapshot<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
    }
}

impl<T> Drop for Published<T> {
    fn drop(&mut self) {
        // SAFETY: `&mut self` means no reader can still reach the slot.
        unsafe {
            let guard = epoch::unprotected();
            let old = self.slot.load(Ordering::Relaxed, guard);
            if !old.is_null() {
                drop(old.into_owned());
            }
        }
    }
}

// SAFETY: the slot owns a `T` that readers access by shared reference from any
// thread, so it is as thread-safe as `T: Send + Sync`.
unsafe impl<T: Send + Sync> Send for Published<T> {}
unsafe impl<T: Send + Sync> Sync for Published<T> {}

#[cfg(test)]
#[path = "tests/published_test.rs"]
mod published_test;
