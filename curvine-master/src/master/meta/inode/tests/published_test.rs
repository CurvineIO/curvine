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

//! Lifetime rules of the epoch-published value: a held snapshot keeps its value
//! across later stores, replaced values are freed, and dropping the slot frees
//! the current one. The drop counter makes a leak or a double free visible.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Counted {
    id: usize,
    drops: Arc<AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn counted(id: usize, drops: &Arc<AtomicUsize>) -> Counted {
    Counted {
        id,
        drops: drops.clone(),
    }
}

/// Collection is deferred until the epoch advances; pinning repeatedly on the
/// slot's own collector drives it without waiting on unrelated test threads.
fn settle<T>(slot: &Published<T>) {
    for _ in 0..256 {
        drop(slot.load());
        slot.pin().flush();
    }
}

fn slot_of(value: Counted) -> Published<Counted> {
    Published::with_collector(value, crossbeam::epoch::Collector::new())
}

#[test]
fn a_held_snapshot_keeps_its_value_across_stores() {
    let drops = Arc::new(AtomicUsize::new(0));
    let slot = slot_of(counted(1, &drops));
    let held = slot.load();
    slot.store(counted(2, &drops));
    slot.store(counted(3, &drops));
    settle(&slot);
    assert_eq!(held.id, 1, "the reader must keep seeing what it loaded");
    assert_eq!(slot.load().id, 3);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "a pinned reader blocks the free"
    );
}

#[test]
fn replaced_values_are_freed_once_readers_leave() {
    let drops = Arc::new(AtomicUsize::new(0));
    let slot = slot_of(counted(0, &drops));
    for id in 1..=50 {
        slot.store(counted(id, &drops));
    }
    settle(&slot);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        50,
        "every replaced value is freed once"
    );
    assert_eq!(slot.load().id, 50);
}

#[test]
fn dropping_the_slot_frees_the_current_value() {
    let drops = Arc::new(AtomicUsize::new(0));
    let collector = crossbeam::epoch::Collector::new();
    {
        let slot = Published::with_collector(counted(1, &drops), collector.clone());
        slot.store(counted(2, &drops));
        settle(&slot);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the replaced value is freed"
        );
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "dropping the slot frees the current one"
    );
}

#[test]
fn concurrent_readers_see_a_monotonic_value_while_a_writer_stores() {
    let slot = Arc::new(Published::new(0usize));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (slot, stop) = (slot.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut last = 0;
                while !stop.load(Ordering::Relaxed) {
                    let seen = *slot.load();
                    assert!(seen >= last, "went backwards: {last} -> {seen}");
                    last = seen;
                }
                last
            })
        })
        .collect();
    for value in 1..=20_000usize {
        slot.store(value);
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        assert!(reader.join().unwrap() <= 20_000);
    }
    assert_eq!(*slot.load(), 20_000);
}
