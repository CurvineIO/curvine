// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! Minimal consistent hash ring: `hash(job_id)` → instance address.
//!
//! 160 virtual nodes per instance keep the load spread even for the small
//! cluster sizes transfer runs at. Immutable snapshot semantics: readers hold
//! an `Arc<ConsistentHashRing>` and a membership change builds a fresh ring.

use std::collections::{BTreeMap, HashMap};

const VNODES: u32 = 160;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingEntry {
    /// host:port of the instance's RPC endpoint.
    pub addr: String,
    /// Instance start epoch; a changed epoch means the instance restarted and
    /// callers should drop pooled connections to it.
    pub start_epoch: u64,
}

#[derive(Debug, Default)]
pub struct ConsistentHashRing {
    ring: BTreeMap<u64, String>,
    entries: HashMap<String, RingEntry>,
}

impl ConsistentHashRing {
    pub fn build(endpoints: impl IntoIterator<Item = (String, String, u64)>) -> Self {
        let mut ring = BTreeMap::new();
        let mut entries = HashMap::new();
        for (id, addr, start_epoch) in endpoints {
            entries.insert(id.clone(), RingEntry { addr, start_epoch });
            for vnode in 0..VNODES {
                let key = hash_key(format!("{id}#v{vnode}"));
                ring.insert(key, id.clone());
            }
        }
        Self { ring, entries }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, instance_id: &str) -> Option<&RingEntry> {
        self.entries.get(instance_id)
    }

    pub fn all_addrs(&self) -> Vec<String> {
        let mut addrs: Vec<String> = self.entries.values().map(|e| e.addr.clone()).collect();
        addrs.sort();
        addrs
    }

    /// Owner of a routing key. Empty ring returns None (caller falls back).
    pub fn owner(&self, job_id: &str) -> Option<&RingEntry> {
        if self.ring.is_empty() {
            return None;
        }
        let key = hash_key(job_id);
        // Owner = last vnode <= key (clockwise predecessor); wrap to the
        // largest vnode when the key falls before the first one.
        let id = self
            .ring
            .range(..=key)
            .next_back()
            .or_else(|| self.ring.iter().next_back())
            .map(|(_, id)| id.clone())?;
        self.entries.get(&id)
    }
}

fn hash_key(value: impl AsRef<[u8]>) -> u64 {
    // FNV-1a rounds plus a murmur3-style finalizer: the finalizer's avalanche
    // keeps sequential job ids (job-0, job-1, ...) spread across the ring.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in value.as_ref() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    fn ring(ids: &[&str]) -> ConsistentHashRing {
        ConsistentHashRing::build(
            ids.iter()
                .map(|id| ((*id).to_string(), (*id).to_string(), 1u64)),
        )
    }

    #[test]
    fn empty_ring_has_no_owner() {
        assert!(ConsistentHashRing::default().owner("job-1").is_none());
    }

    #[test]
    fn owner_is_deterministic() {
        let ring = ring(&["a:9000", "b:9000", "c:9000"]);
        let first = ring.owner("job-abc").unwrap().addr.clone();
        for _ in 0..10 {
            assert_eq!(ring.owner("job-abc").unwrap().addr, first);
        }
    }

    #[test]
    fn ownership_is_spread_across_nodes() {
        let ring = ring(&["a:9000", "b:9000", "c:9000"]);
        let mut counts: StdHashMap<String, usize> = StdHashMap::new();
        for i in 0..300 {
            let owner = ring.owner(&format!("job-{i}")).unwrap().addr.clone();
            *counts.entry(owner).or_default() += 1;
        }
        assert_eq!(counts.len(), 3);
        // No node should grab more than ~60% of keys with 160 vnodes each.
        for count in counts.values() {
            assert!(*count < 180, "unbalanced ring: {counts:?}");
        }
    }

    #[test]
    fn removing_a_node_moves_only_its_keys() {
        let full = ring(&["a:9000", "b:9000", "c:9000"]);
        let without_a = ring(&["b:9000", "c:9000"]);
        let mut moved = 0;
        for i in 0..300 {
            let key = &format!("job-{i}");
            if full.owner(key).unwrap().addr != without_a.owner(key).unwrap().addr {
                moved += 1;
            }
        }
        // ~1/3 of keys lived on `a`; tolerate hashing skew but nothing more.
        assert!(moved < 180, "too many keys moved when a node left: {moved}");
    }

    #[test]
    fn entries_survive_roundtrip() {
        let ring = ring(&["h1:9000"]);
        assert_eq!(ring.owner("job").unwrap().start_epoch, 1);
        assert_eq!(ring.len(), 1);
    }
}
