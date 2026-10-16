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

//! Immutable dir-space generation store.
//!
//! The store publishes fully built [`DirSpaceSnapshot`] generations produced by
//! the scanner. A generation is assigned inside the publish critical section
//! and never mutated afterwards; readers clone an `Arc` out of the lock and
//! then read without any synchronization.
//!
//! Query selection: a validated tracked-point generation is only preferred
//! when it contains the queried inode and its generation is newer than the
//! current global generation; otherwise the query falls back to the global
//! snapshot. When no published snapshot contains the inode, [`SnapshotMiss`]
//! is returned — zero-valued results are never fabricated.

use super::types::{DirSpaceSnapshot, SnapshotSource};
use std::collections::HashMap;
use std::sync::Arc;

/// The reason a query could not be served from any published snapshot.
///
/// Returned only when the queried inode is not contained in any usable
/// snapshot (or none has been published yet). Callers translate this into the
/// wire-level `SnapshotNotReady` error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMiss {
    /// No usable snapshot has been published yet.
    NoSnapshot,
    /// A snapshot exists but does not contain the queried inode, e.g. the
    /// directory was created or moved in after the generation was built.
    NotInSnapshot,
}

/// Query outcome: the selected immutable generation plus its source.
#[derive(Debug, Clone)]
pub struct DirSpaceQueryResult {
    pub snapshot: Arc<DirSpaceSnapshot>,
    pub source: SnapshotSource,
}

/// Publishes immutable global and tracked-point dir-space generations.
///
/// Internals are guarded by a single `parking_lot::RwLock`; publish takes the
/// write lock, assigns the next monotonically increasing generation, and swaps
/// an `Arc`. Query takes the read lock, selects a snapshot and clones the
/// `Arc` — the snapshot contents are then read lock-free.
pub struct DirSpaceSnapshotStore {
    inner: parking_lot::RwLock<StoreInner>,
}

struct StoreInner {
    next_generation: u64,
    global: Option<Arc<DirSpaceSnapshot>>,
    points: HashMap<String, Arc<DirSpaceSnapshot>>,
}

impl DirSpaceSnapshotStore {
    pub fn new() -> Self {
        Self {
            inner: parking_lot::RwLock::new(StoreInner {
                next_generation: 1,
                global: None,
                points: HashMap::new(),
            }),
        }
    }

    /// Publishes a fully built global snapshot.
    ///
    /// The generation is assigned inside the write-lock critical section, so
    /// published generations are strictly monotonic across global and point
    /// publications. `snapshot.meta.generation` is overwritten here; any value
    /// carried on an unpublished scanner product is ignored.
    pub fn publish_global(&self, mut snapshot: DirSpaceSnapshot) -> Arc<DirSpaceSnapshot> {
        let mut inner = self.inner.write();
        snapshot.meta.generation = inner.next_generation;
        inner.next_generation += 1;
        let snapshot = Arc::new(snapshot);
        inner.global = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Publishes a fully built tracked-point snapshot for `root_path`.
    ///
    /// The caller (manager) is responsible for path normalization and for
    /// verifying that `snapshot.root_id` matches the current resolution of
    /// `root_path`; the store only accepts already-validated point paths.
    /// Republishing over the same path atomically replaces the old generation.
    pub fn publish_point(
        &self,
        root_path: &str,
        mut snapshot: DirSpaceSnapshot,
    ) -> Arc<DirSpaceSnapshot> {
        let mut inner = self.inner.write();
        snapshot.meta.generation = inner.next_generation;
        inner.next_generation += 1;
        let snapshot = Arc::new(snapshot);
        inner.points.insert(root_path.to_string(), snapshot.clone());
        snapshot
    }

    /// Drops the global snapshot so queries stop serving it.
    pub fn clear_global(&self) -> bool {
        self.inner.write().global.take().is_some()
    }

    /// Removes a tracked point so its last generation is never selected again.
    /// Returns whether a point existed for the path.
    pub fn remove_point(&self, root_path: &str) -> bool {
        self.inner.write().points.remove(root_path).is_some()
    }

    /// Returns the current global snapshot, if one has been published.
    pub fn global(&self) -> Option<Arc<DirSpaceSnapshot>> {
        self.inner.read().global.clone()
    }

    /// Returns the published point snapshot for `root_path`, if any.
    pub fn point(&self, root_path: &str) -> Option<Arc<DirSpaceSnapshot>> {
        self.inner.read().points.get(root_path).cloned()
    }

    /// Snapshot of the currently published tracked points (path -> newest
    /// generation). Used by the manager to select point candidates and by
    /// metrics aggregation; the returned snapshots are immutable.
    pub fn points(&self) -> HashMap<String, Arc<DirSpaceSnapshot>> {
        self.inner.read().points.clone()
    }

    /// Resolves a query against the published snapshots.
    ///
    /// `validated_point_path` must be the normalized tracked-point path that
    /// was already verified (by the manager) to be the longest component-level
    /// prefix of the queried path and to still resolve to the point's root
    /// inode id. The store then prefers that point only when it actually
    /// contains `dir_id` and is newer than the global generation; otherwise it
    /// falls back to global. When neither snapshot contains the inode, a
    /// [`SnapshotMiss`] is returned.
    pub fn query(
        &self,
        dir_id: i64,
        validated_point_path: Option<&str>,
    ) -> Result<DirSpaceQueryResult, SnapshotMiss> {
        let inner = self.inner.read();

        let point = validated_point_path
            .and_then(|path| inner.points.get(path))
            .map(Arc::clone);
        let global = inner.global.clone();

        // Prefer a validated point when it contains the inode and is newer
        // than global. Timestamps are display-only; generation decides ordering.
        if let Some(point) = &point {
            let newer_than_global = match &global {
                Some(global) => point.meta.generation > global.meta.generation,
                None => true,
            };
            if newer_than_global && point.entries.contains_key(&dir_id) {
                return Ok(DirSpaceQueryResult {
                    source: point.source,
                    snapshot: point.clone(),
                });
            }
        }

        if let Some(global) = global {
            if global.entries.contains_key(&dir_id) {
                return Ok(DirSpaceQueryResult {
                    source: global.source,
                    snapshot: global,
                });
            }
            // A global generation exists but does not cover this inode; do not
            // fall back to an older point generation, and never fabricate a
            // zero-valued result.
            return Err(SnapshotMiss::NotInSnapshot);
        }

        // No global yet: a validated point that contains the inode may serve
        // the query on its own.
        match point {
            Some(point) if point.entries.contains_key(&dir_id) => Ok(DirSpaceQueryResult {
                source: point.source,
                snapshot: point,
            }),
            _ => Err(SnapshotMiss::NoSnapshot),
        }
    }
}

impl Default for DirSpaceSnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ROOT_INODE_ID;
    use crate::master::space::types::{DirAgg, SnapshotMeta};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn snapshot(root_id: i64, entries: Vec<(i64, DirAgg)>) -> DirSpaceSnapshot {
        DirSpaceSnapshot {
            meta: SnapshotMeta {
                generation: 0, // assigned by the store on publish
                started_ms: 1_000,
                finished_ms: 2_000,
                inode_count: entries.len() as u64 + 1,
                edge_count: entries.len() as u64,
                anomaly_count: 0,
                degraded: false,
            },
            root_id,
            source: SnapshotSource::Global,
            entries: entries.into_iter().collect::<HashMap<_, _>>(),
        }
    }

    fn point_snapshot(root_id: i64, entries: Vec<(i64, DirAgg)>) -> DirSpaceSnapshot {
        let mut s = snapshot(root_id, entries);
        s.source = SnapshotSource::TrackedPoint;
        s
    }

    fn dir_agg(bytes: u64, files: u64, dirs: u64) -> DirAgg {
        DirAgg {
            logical_bytes: bytes,
            file_count: files,
            directory_count: dirs,
        }
    }

    #[test]
    fn query_without_any_snapshot_misses() {
        let store = DirSpaceSnapshotStore::new();
        assert!(matches!(
            store.query(ROOT_INODE_ID, None),
            Err(SnapshotMiss::NoSnapshot)
        ));
        assert!(store.global().is_none());
    }

    #[test]
    fn global_only_query() {
        let store = DirSpaceSnapshotStore::new();
        store.publish_global(snapshot(
            ROOT_INODE_ID,
            vec![(ROOT_INODE_ID, dir_agg(1, 1, 1))],
        ));

        let res = store.query(ROOT_INODE_ID, None).expect("must hit");
        assert_eq!(res.source, SnapshotSource::Global);
        assert_eq!(res.snapshot.meta.generation, 1);
        assert_eq!(res.snapshot.entries[&ROOT_INODE_ID].logical_bytes, 1);

        // Unknown inode in an existing global generation is a miss, not zero.
        assert!(matches!(
            store.query(999_999, None),
            Err(SnapshotMiss::NotInSnapshot)
        ));
    }

    #[test]
    fn newer_point_beats_global() {
        let store = DirSpaceSnapshotStore::new();
        store.publish_global(snapshot(
            ROOT_INODE_ID,
            vec![(ROOT_INODE_ID, dir_agg(1, 1, 1)), (42, dir_agg(1, 1, 1))],
        ));

        let point = point_snapshot(10, vec![(42, dir_agg(7, 3, 2)), (10, dir_agg(0, 0, 1))]);
        store.publish_point("/mnt/a", point);

        let res = store.query(42, Some("/mnt/a")).expect("point must hit");
        assert_eq!(res.source, SnapshotSource::TrackedPoint);
        assert_eq!(res.snapshot.meta.generation, 2);
        assert_eq!(res.snapshot.entries[&42].logical_bytes, 7);

        // Point that does not contain the inode falls back to global.
        let res = store
            .query(ROOT_INODE_ID, Some("/mnt/a"))
            .expect("global fallback");
        assert_eq!(res.source, SnapshotSource::Global);
        assert_eq!(res.snapshot.meta.generation, 1);
    }

    #[test]
    fn older_point_never_beats_global() {
        let store = DirSpaceSnapshotStore::new();
        let point = point_snapshot(10, vec![(42, dir_agg(7, 3, 2))]);
        store.publish_point("/mnt/a", point);
        store.publish_global(snapshot(ROOT_INODE_ID, vec![(42, dir_agg(1, 1, 1))]));

        // The point (generation 1) is older than global (generation 2): even
        // though it contains the inode, global wins.
        let res = store.query(42, Some("/mnt/a")).expect("global wins");
        assert_eq!(res.source, SnapshotSource::Global);
        assert_eq!(res.snapshot.meta.generation, 2);
    }

    #[test]
    fn point_replacement_and_removal() {
        let store = DirSpaceSnapshotStore::new();
        store.publish_point("/mnt/a", point_snapshot(10, vec![(42, dir_agg(1, 1, 1))]));
        store.publish_point("/mnt/a", point_snapshot(10, vec![(42, dir_agg(5, 2, 1))]));
        let res = store.query(42, Some("/mnt/a")).expect("must hit");
        assert_eq!(res.snapshot.entries[&42].logical_bytes, 5);

        assert!(store.remove_point("/mnt/a"));
        assert!(!store.remove_point("/mnt/a"));
        assert!(matches!(
            store.query(42, Some("/mnt/a")),
            Err(SnapshotMiss::NoSnapshot)
        ));
    }

    #[test]
    fn failed_round_keeps_previous_generation() {
        let store = DirSpaceSnapshotStore::new();
        let first = store.publish_global(snapshot(ROOT_INODE_ID, vec![(42, dir_agg(1, 1, 1))]));

        // A failed scan round simply never publishes; the previous generation
        // stays queryable and unchanged.
        assert!(store.global().is_some());
        let res = store.query(42, None).expect("old generation retained");
        assert_eq!(res.snapshot.meta.generation, first.meta.generation);
        assert!(Arc::ptr_eq(&res.snapshot, &first));
    }

    #[test]
    fn generations_are_monotonic_across_scopes() {
        let store = DirSpaceSnapshotStore::new();
        let mut expected = 1u64;
        for _ in 0..100u64 {
            let g = store.publish_global(snapshot(ROOT_INODE_ID, vec![(42, dir_agg(1, 1, 1))]));
            assert_eq!(g.meta.generation, expected);
            expected += 1;
            let p = store.publish_point("/mnt/a", point_snapshot(10, vec![(42, dir_agg(1, 1, 1))]));
            assert_eq!(p.meta.generation, expected);
            expected += 1;
        }
    }

    #[test]
    fn concurrent_publishers_and_readers_never_observe_torn_generations() {
        let store = Arc::new(DirSpaceSnapshotStore::new());
        let stop = Arc::new(AtomicU64::new(0));

        let mut writers = Vec::new();
        for w in 0..2 {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            writers.push(std::thread::spawn(move || {
                let mut published = 0u64;
                while stop.load(Ordering::Relaxed) == 0 && published < 100 {
                    let entries = vec![(42, dir_agg(published, published, 1))];
                    let snap = if w == 0 {
                        store.publish_global(snapshot(ROOT_INODE_ID, entries))
                    } else {
                        store.publish_point("/mnt/a", point_snapshot(10, entries))
                    };
                    published += 1;
                    assert!(snap.meta.generation >= published);
                }
                published
            }));
        }

        let mut readers = Vec::new();
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            readers.push(std::thread::spawn(move || {
                let mut queries = 0u64;
                while stop.load(Ordering::Relaxed) == 0 {
                    // A published generation is immutable: whichever snapshot
                    // the reader observes must be internally consistent, i.e.
                    // the bytes value must match one of the written values and
                    // meta/entries always come from the same generation.
                    if let Ok(res) = store.query(42, Some("/mnt/a")) {
                        let bytes = res.snapshot.entries[&42].logical_bytes;
                        let files = res.snapshot.entries[&42].file_count;
                        assert_eq!(bytes, files, "torn generation observed");
                        assert!(res.snapshot.meta.generation >= 1);
                    }
                    queries += 1;
                }
                queries
            }));
        }

        let published: u64 = writers.into_iter().map(|h| h.join().unwrap()).sum();
        stop.store(1, Ordering::Relaxed);
        let _queries: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();

        assert!(published >= 200);
        // After the final publish the newest generation must be visible.
        let res = store
            .query(42, Some("/mnt/a"))
            .expect("final state must hit");
        assert_eq!(
            res.snapshot.meta.generation,
            store.inner.read().next_generation - 1
        );
    }

    #[test]
    fn point_without_global_can_serve_query() {
        let store = DirSpaceSnapshotStore::new();
        store.publish_point("/mnt/a", point_snapshot(10, vec![(42, dir_agg(3, 1, 1))]));

        let res = store.query(42, Some("/mnt/a")).expect("point serves query");
        assert_eq!(res.source, SnapshotSource::TrackedPoint);

        // Inode not covered by the point and no global published yet.
        assert!(matches!(
            store.query(43, Some("/mnt/a")),
            Err(SnapshotMiss::NoSnapshot)
        ));
        assert!(matches!(
            store.query(43, None),
            Err(SnapshotMiss::NoSnapshot)
        ));
    }
}
