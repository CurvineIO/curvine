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
fn read_and_write_sets_acquire_one_guard_per_stripe() {
    let manager = BlockLocationLockManager::new(64, 8);
    let ids = [1, 2, 3, 1];
    let stripes = ids
        .iter()
        .map(|&id| manager.block_locks.index(id as u64))
        .collect::<std::collections::HashSet<_>>();
    let set = manager.read_blocks(&ids);
    assert_eq!(set.guards.len(), stripes.len());
    drop(set);

    let set = manager.write_workers_blocks(&[7, 9], &[4, 4, 5]);
    let workers = [7, 9]
        .iter()
        .map(|&id| manager.worker_locks.index(id))
        .collect::<std::collections::HashSet<_>>();
    let blocks = [4, 4, 5]
        .iter()
        .map(|&id| manager.block_locks.index(id))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(set.guards.len(), workers.len() + blocks.len());
}

#[test]
fn same_stripe_keys_merge_into_one_guard() {
    let pool = ObjectLockPool::new(8);
    let mut collision = None;
    let mut distinct = None;
    'outer: for a in 0..64u64 {
        for b in (a + 1)..64u64 {
            if pool.index(a) == pool.index(b) {
                collision = Some((a, b));
                for c in (b + 1)..64u64 {
                    if pool.index(c) != pool.index(a) {
                        distinct = Some(c);
                        break 'outer;
                    }
                }
            }
        }
    }
    let (a, b) = collision.expect("8 stripes must collide within 0..64");
    let c = distinct.expect("a distinct stripe must exist");
    let guards = acquire_stripes(&pool, &[a, b, c], BlockMode::Write);
    assert_eq!(
        guards.len(),
        2,
        "same-stripe keys must merge into one guard"
    );
    // Dropping the guards must not hang: guards cover stripe RwLocks taken
    // exactly once.
    drop(guards);
}
