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

use super::*;

#[test]
fn keys_share_stripe_only_through_hash() {
    let pool = ObjectLockPool::new(4096);
    // Distinct keys map deterministically; the same key always lands on
    // the same stripe, which is the mutual-exclusion contract.
    assert_eq!(pool.index(42), pool.index(42));
    assert!(pool.index(1) < pool.stripe_count());
    assert!(pool.index(u64::MAX) < pool.stripe_count());
}

#[test]
fn stripe_distribution_and_rounding() {
    let single = ObjectLockPool::new(1);
    assert!([0, 1, u64::MAX].iter().all(|&key| single.index(key) == 0));
    assert_eq!(ObjectLockPool::new(6).stripe_count(), 8);
    let pool = ObjectLockPool::new(4096);
    let distinct = (0..4095)
        .filter(|&key| pool.index(key) != pool.index(key + 1))
        .count();
    assert!(distinct * 100 >= 4095 * 99);
}
