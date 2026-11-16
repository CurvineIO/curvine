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
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Fixed striped table of namespace object locks. Each stripe is one RwLock
/// guarding a set of objects; concurrent keys sharing a stripe serialise, so
/// the stripe count must stay well above the expected concurrent-lock width.
/// Compared with the previous per-key HashMap pool this removes the shard
/// read guard, SipHash lookup, Arc clone, and idle cleanup passes: locking is
/// one multiply and one RwLock acquire.
///
/// Deadlock-freedom contract: callers must sort requests by stripe index and
/// deduplicate before acquiring, so every acquisition path takes stripes in
/// ascending order (see `InodeLockManager::lock_many`).
///
/// Each stripe sits on its own cache line: an unpadded 8-byte RwLock packs
/// eight stripes per line, so unrelated objects contended on one line.
pub(crate) struct ObjectLockPool {
    stripes: Box<[CachePadded<RwLock<()>>]>,
    shift: u32,
}

impl ObjectLockPool {
    pub(crate) fn new(shard_count: usize) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        Self {
            stripes: (0..count)
                .map(|_| CachePadded::new(RwLock::new(())))
                .collect(),
            shift: 64 - count.trailing_zeros(),
        }
    }

    /// Map a key to a fixed stripe.
    #[inline]
    pub(crate) fn index(&self, key: u64) -> usize {
        if self.shift == 64 {
            0
        } else {
            (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
        }
    }

    /// Test-only non-blocking shared acquisition by pre-computed stripe index.
    #[cfg(test)]
    pub(crate) fn try_read_by_stripe(&self, stripe: usize) -> Option<RwLockReadGuard<'_, ()>> {
        self.stripes.get(stripe)?.try_read()
    }

    /// Test-only non-blocking exclusive observation by pre-computed stripe
    /// index: succeeds only when NO read or write holder is left on the
    /// stripe — a read lock alone would not prove release, read/read coexist.
    #[cfg(test)]
    pub(crate) fn try_write_by_stripe(&self, stripe: usize) -> Option<RwLockWriteGuard<'_, ()>> {
        self.stripes.get(stripe)?.try_write()
    }

    /// Acquire an already-computed stripe index. Callers that group keys by
    /// stripe first (`acquire_stripes`) avoid hashing twice per key.
    #[inline]
    pub(crate) fn lock_shared_at(&self, stripe: usize) -> RwLockReadGuard<'_, ()> {
        self.stripes[stripe].read()
    }

    #[inline]
    pub(crate) fn lock_exclusive_at(&self, stripe: usize) -> RwLockWriteGuard<'_, ()> {
        self.stripes[stripe].write()
    }

    #[cfg(test)]
    pub(crate) fn stripe_count(&self) -> usize {
        self.stripes.len()
    }
}

#[cfg(test)]
#[path = "tests/object_lock_pool_test.rs"]
mod tests;
