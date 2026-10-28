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

//! Process-wide TTL cache for owner/group name <-> id resolution.
//!
//! FUSE metadata paths (READDIR in particular) resolve the owner and group of every entry, and
//! with a remote NSS backend (SSSD/LDAP) a single lookup can take 100 ms or more. Results are
//! cached so repeated names cost a hash-map lookup. The lock is only held for map operations,
//! never across the NSS call itself, so a slow lookup never blocks other resolutions.
//!
//! Staleness is bounded by the TTLs: an owner/group change in the directory service becomes
//! visible after at most [`HIT_TTL`] (found) or [`MISS_TTL`] (not found), like nscd/sssd.

// Only the Linux resolvers use the cache; the type is platform-independent so it is unit-tested
// everywhere.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// How long a resolved name/id stays cached.
pub(crate) const HIT_TTL: Duration = Duration::from_secs(60);
/// How long a "no such user/group" result stays cached. Shorter than [`HIT_TTL`] so a newly
/// created account shows up quickly, but long enough that a directory listing full of unknown
/// owners does not query NSS once per entry.
pub(crate) const MISS_TTL: Duration = Duration::from_secs(10);
/// Upper bound on entries per cache, so a scan over many distinct owners cannot grow it without
/// limit.
pub(crate) const MAX_ENTRIES: usize = 4096;

struct Entry<V> {
    value: Option<V>,
    at: Instant,
}

pub(crate) struct TtlCache<K, V> {
    map: RwLock<HashMap<K, Entry<V>>>,
    hit_ttl: Duration,
    miss_ttl: Duration,
    max_entries: usize,
}

impl<K: Eq + Hash, V: Clone> TtlCache<K, V> {
    pub(crate) fn new(hit_ttl: Duration, miss_ttl: Duration, max_entries: usize) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            hit_ttl,
            miss_ttl,
            max_entries,
        }
    }

    fn fresh(&self, entry: &Entry<V>, now: Instant) -> bool {
        let ttl = if entry.value.is_some() {
            self.hit_ttl
        } else {
            self.miss_ttl
        };
        now.saturating_duration_since(entry.at) < ttl
    }

    /// Returns the cached value for `key`, or runs `resolve` and caches its result.
    ///
    /// `resolve` returns `Ok(Some(v))` when found, `Ok(None)` when the name/id does not exist
    /// (cached for the miss TTL), and `Err(_)` for a transient failure, which is not cached and
    /// yields `None`. Concurrent misses on the same key may both call `resolve`; the results are
    /// equivalent, so that is preferred over holding a lock across a slow NSS call.
    pub(crate) fn get_or_resolve<Q, E>(
        &self,
        key: &Q,
        resolve: impl FnOnce() -> Result<Option<V>, E>,
    ) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ToOwned<Owned = K> + ?Sized,
    {
        self.get_or_resolve_at(key, Instant::now(), resolve)
    }

    fn get_or_resolve_at<Q, E>(
        &self,
        key: &Q,
        now: Instant,
        resolve: impl FnOnce() -> Result<Option<V>, E>,
    ) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ToOwned<Owned = K> + ?Sized,
    {
        {
            let map = self.map.read().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = map.get(key) {
                if self.fresh(entry, now) {
                    return entry.value.clone();
                }
            }
        }

        let value = resolve().ok()?;
        let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
        if map.len() >= self.max_entries && !map.contains_key(key) {
            map.retain(|_, e| self.fresh(e, now));
            if map.len() >= self.max_entries {
                map.clear();
            }
        }
        map.insert(
            key.to_owned(),
            Entry {
                value: value.clone(),
                at: now,
            },
        );
        value
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }
}

fn new_cache<K: Eq + Hash, V: Clone>() -> TtlCache<K, V> {
    TtlCache::new(HIT_TTL, MISS_TTL, MAX_ENTRIES)
}

pub(crate) static UID_BY_NAME: std::sync::LazyLock<TtlCache<String, u32>> =
    std::sync::LazyLock::new(new_cache);
pub(crate) static NAME_BY_UID: std::sync::LazyLock<TtlCache<u32, String>> =
    std::sync::LazyLock::new(new_cache);
pub(crate) static GID_BY_NAME: std::sync::LazyLock<TtlCache<String, u32>> =
    std::sync::LazyLock::new(new_cache);
pub(crate) static NAME_BY_GID: std::sync::LazyLock<TtlCache<u32, String>> =
    std::sync::LazyLock::new(new_cache);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    const SEC: Duration = Duration::from_secs(1);

    fn cache() -> TtlCache<String, u32> {
        TtlCache::new(10 * SEC, 2 * SEC, 4)
    }

    fn found(calls: &AtomicUsize, v: u32) -> impl FnOnce() -> Result<Option<u32>, ()> + '_ {
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(v))
        }
    }

    #[test]
    fn hit_does_not_call_resolver_until_ttl_expires() {
        let c = cache();
        let calls = AtomicUsize::new(0);
        let t0 = Instant::now();

        assert_eq!(
            c.get_or_resolve_at("alice", t0, found(&calls, 1000)),
            Some(1000)
        );
        assert_eq!(
            c.get_or_resolve_at("alice", t0 + 9 * SEC, found(&calls, 7)),
            Some(1000)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Expired: re-resolved, and the new value replaces the old one.
        assert_eq!(
            c.get_or_resolve_at("alice", t0 + 10 * SEC, found(&calls, 1001)),
            Some(1001)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn miss_is_cached_for_the_shorter_ttl() {
        let c = cache();
        let calls = AtomicUsize::new(0);
        let t0 = Instant::now();
        let missing = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(None)
        };

        assert_eq!(c.get_or_resolve_at("ghost", t0, missing), None);
        assert_eq!(c.get_or_resolve_at("ghost", t0 + SEC, missing), None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Miss TTL (2s) elapsed even though the hit TTL (10s) has not: the account now exists.
        assert_eq!(
            c.get_or_resolve_at("ghost", t0 + 2 * SEC, found(&calls, 42)),
            Some(42)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn transient_error_is_not_cached() {
        let c = cache();
        let calls = AtomicUsize::new(0);
        let t0 = Instant::now();
        let failing = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<Option<u32>, _>("EIO")
        };

        assert_eq!(c.get_or_resolve_at("bob", t0, failing), None);
        assert_eq!(c.len(), 0);
        // The next call retries instead of serving a cached miss.
        assert_eq!(c.get_or_resolve_at("bob", t0, found(&calls, 5)), Some(5));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn size_is_bounded() {
        let c = cache();
        let calls = AtomicUsize::new(0);
        let t0 = Instant::now();

        for i in 0..4u32 {
            c.get_or_resolve_at(&format!("u{i}"), t0, found(&calls, i));
        }
        assert_eq!(c.len(), 4);

        // Full of fresh entries: the cache is cleared rather than growing past the bound.
        c.get_or_resolve_at("u4", t0, found(&calls, 4));
        assert_eq!(c.len(), 1);

        // Expired entries are dropped first; fresh ones survive.
        for i in 5..8u32 {
            c.get_or_resolve_at(&format!("u{i}"), t0 + 9 * SEC, found(&calls, i));
        }
        assert_eq!(c.len(), 4);
        c.get_or_resolve_at("u8", t0 + 10 * SEC, found(&calls, 8));
        assert_eq!(c.len(), 4, "only the expired u4 is evicted");
        assert_eq!(
            c.get_or_resolve_at("u5", t0 + 10 * SEC, found(&calls, 0)),
            Some(5)
        );
    }

    #[test]
    fn updating_an_existing_key_at_capacity_keeps_other_entries() {
        let c = cache();
        let calls = AtomicUsize::new(0);
        let t0 = Instant::now();
        for i in 0..4u32 {
            c.get_or_resolve_at(&format!("u{i}"), t0, found(&calls, i));
        }
        c.get_or_resolve_at("u0", t0 + 10 * SEC, found(&calls, 100));
        assert_eq!(c.len(), 4);
    }

    #[test]
    fn concurrent_lookups_return_their_own_ids() {
        let c = Arc::new(TtlCache::<String, u32>::new(10 * SEC, 2 * SEC, MAX_ENTRIES));
        let handles: Vec<_> = (0..16u32)
            .map(|t| {
                let c = c.clone();
                thread::spawn(move || {
                    for round in 0..200u32 {
                        let id = (t * 1000) + (round % 50);
                        let name = format!("user{id}");
                        let got = c.get_or_resolve(&name, || Ok::<_, ()>(Some(id)));
                        assert_eq!(got, Some(id), "{name}");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(c.len(), 16 * 50);
    }

    #[test]
    fn numeric_keys_work() {
        let c = TtlCache::<u32, String>::new(10 * SEC, 2 * SEC, 4);
        let got = c.get_or_resolve(&0, || Ok::<_, ()>(Some("root".to_string())));
        assert_eq!(got.as_deref(), Some("root"));
        let got = c.get_or_resolve(&0, || Ok::<_, ()>(Some("changed".to_string())));
        assert_eq!(got.as_deref(), Some("root"));
    }
}
