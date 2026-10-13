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

//! Global dir-space scanner over one RocksDB metadata snapshot.
//!
//! The scanner walks the `inodes` and `edges` column families of a single
//! consistent read view (see `InodeStoreSnapshot`, Task 2), turns each record
//! into a compact fact ([`InodeFact`]) and feeds the Task 1
//! [`SubtreeAggregator`] to produce one fully built, unpublished generation.
//!
//! Invariants (design doc §8):
//!
//! - The scanner never touches `fs_dir`, the in-memory tree, or any store
//!   clone beyond the single-round [`MetadataScanLease`] it is handed; its
//!   product never carries a generation (the store assigns one on publish).
//! - Deadline, master role and lease cancellation are checked cooperatively
//!   every [`CHECK_INTERVAL`] records (and at each pass start) so a pending
//!   Raft snapshot restore is observed promptly.
//! - Unrecoverable problems (corrupt inode bytes, unparseable keys, oversized
//!   records, resource limits, overflow) fail the whole round; ignorable
//!   problems (missing edge target, negative length) only mark the output
//!   degraded and count as anomalies.
//! - No partial result is ever returned: either a complete
//!   [`UnpublishedSnapshot`] or an error, so the publisher can keep the last
//!   good generation.

use std::collections::HashMap;
use std::time::Instant;

use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_rocksdb::RocksUtils;
use curvine_runtime::common::{LocalTime, SerdeUtils};

use super::agg::{AggregationOutput, SubtreeAggregator};
use super::types::{AggregationStats, DirAgg, SnapshotSource};
use crate::master::meta::inode::{InodeView, ROOT_INODE_ID};
use crate::master::meta::store::InodeStoreSnapshot;
use crate::master::space::scan_lease::MetadataScanLease;

/// Cooperative checks (deadline / role / lease cancellation) run every this
/// many records.
const CHECK_INTERVAL: u64 = 4096;

/// Maximum child inode ids fetched per RocksDB batch in a point BFS step.
pub(crate) const POINT_CHILD_BATCH_SIZE: usize = 1024;

/// Per-record size guard against corrupted or adversarial edge values.
const MAX_EDGE_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// Compact per-inode fact kept during a scan. Never a full `InodeView`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InodeFact {
    pub is_dir: bool,
    pub logical_len: i64,
}

/// Resource limits for one scan round.
#[derive(Debug, Clone, Copy)]
pub struct ScanLimits {
    pub max_inodes: u64,
    pub max_edges: u64,
    pub max_dirs: u64,
    pub max_depth: u32,
    /// Upper bound on the estimated resident memory of scan-time structures.
    pub memory_budget_bytes: u64,
    /// Upper bound on a single serialized inode record.
    pub max_inode_record_bytes: u64,
}

impl ScanLimits {
    /// Conservative estimate of the scan's own footprint: facts plus
    /// aggregator bookkeeping. Used to bound allocation growth between
    /// checks so the budget is never overshot by a wide margin.
    pub fn estimated_bytes(&self, inode_count: u64, dir_count: u64, edge_count: u64) -> u64 {
        inode_count
            .saturating_mul(std::mem::size_of::<InodeFact>() as u64)
            .saturating_add(dir_count.saturating_mul(48))
            .saturating_add(edge_count.saturating_mul(32))
    }
}

/// Cooperative cancellation and resource context for one scan round.
/// Not `Clone`/`Debug`: the per-round callback keeps it intentionally opaque.
pub struct ScanContext {
    /// Monotonic deadline (`Instant`-based) for the whole round.
    pub deadline: Instant,
    /// Returns the current master role; a scan aborts as soon as it turns
    /// `false` (role lost / standby).
    pub is_active: Box<dyn Fn() -> bool + Send + Sync>,
    /// Scope resource limits.
    pub limits: ScanLimits,
}

/// A fully built scanner product, not yet published.
///
/// `entries` maps every reachable directory inode id (root excluded) to its
/// recursive aggregate; the root aggregate is carried separately so the
/// publisher can merge it into a `DirSpaceSnapshot`.
#[derive(Debug, Clone)]
pub struct UnpublishedSnapshot {
    pub source: SnapshotSource,
    pub root_id: i64,
    pub root: DirAgg,
    pub entries: HashMap<i64, DirAgg>,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub inode_count: u64,
    pub edge_count: u64,
    pub stats: AggregationStats,
}

/// Scans one RocksDB metadata snapshot into a complete dir-space generation.
pub struct GlobalScanner;

/// BFS dir-space scanner rooted at one tracked directory inode.
pub struct PointScanner;

impl PointScanner {
    /// Scan the subtree reachable from `root_id` within one metadata snapshot.
    ///
    /// Path resolution happens before this call; the scanner only reads
    /// RocksDB via `lease`. Sibling subtrees and nodes outside the BFS
    /// closure are never visited.
    pub fn scan(
        lease: &MetadataScanLease,
        root_id: i64,
        context: &ScanContext,
    ) -> CommonResult<UnpublishedSnapshot> {
        let started_ms = now_ms();
        let reader = lease.snapshot_reader()?;
        let mut scan = PointScanState::new(root_id, context);
        scan.check_cooperative(lease)?;
        scan.run_bfs(&reader, lease)?;
        let UnpublishedScan {
            root,
            entries,
            stats,
            inode_count,
            edge_count,
        } = scan.build()?;

        drop(reader);

        Ok(UnpublishedSnapshot {
            source: SnapshotSource::TrackedPoint,
            root_id,
            root,
            entries,
            started_ms,
            finished_ms: now_ms(),
            inode_count,
            edge_count,
            stats,
        })
    }
}

struct PointScanState<'a> {
    root_id: i64,
    context: &'a ScanContext,
    aggregator: SubtreeAggregator,
    inode_count: u64,
    edge_count: u64,
    dir_count: u64,
    stats: AggregationStats,
}

impl<'a> PointScanState<'a> {
    fn new(root_id: i64, context: &'a ScanContext) -> Self {
        let limits = context.limits;
        Self {
            root_id,
            context,
            aggregator: SubtreeAggregator::new(root_id, limits.max_dirs, limits.max_depth),
            inode_count: 0,
            edge_count: 0,
            dir_count: 1,
            stats: AggregationStats::default(),
        }
    }

    fn check_cooperative(&self, lease: &MetadataScanLease) -> CommonResult<()> {
        if lease.is_cancelled() {
            return err_box!("dir-space scan aborted: scan lease cancelled by restore");
        }
        if !(self.context.is_active)() {
            return err_box!("dir-space scan aborted: master role lost");
        }
        if Instant::now() >= self.context.deadline {
            return err_box!("dir-space point scan timed out");
        }
        Ok(())
    }

    fn check_budget(&self) -> CommonResult<()> {
        let limits = self.context.limits;
        if self.inode_count > limits.max_inodes {
            return err_box!("dir-space MaxInodes limit {} exceeded", limits.max_inodes);
        }
        if self.edge_count > limits.max_edges {
            return err_box!("dir-space MaxEdges limit {} exceeded", limits.max_edges);
        }
        let estimated = limits.estimated_bytes(self.inode_count, self.dir_count, self.edge_count);
        if estimated > limits.memory_budget_bytes {
            return err_box!(
                "dir-space memory budget {} bytes exceeded (estimated {} bytes)",
                limits.memory_budget_bytes,
                estimated
            );
        }
        Ok(())
    }

    fn run_bfs(
        &mut self,
        reader: &InodeStoreSnapshot<'_>,
        lease: &MetadataScanLease,
    ) -> CommonResult<()> {
        use std::collections::VecDeque;

        let mut queue: VecDeque<(i64, u32)> = VecDeque::new();
        queue.push_back((self.root_id, 0));

        while let Some((parent_id, depth)) = queue.pop_front() {
            self.check_cooperative(lease)?;
            self.check_budget()?;

            let mut child_ids = Vec::new();
            for item in reader.scan_children(parent_id)? {
                let (_, value) = try_err!(item);
                if value.len() > MAX_EDGE_VALUE_BYTES {
                    return err_box!(
                        "dir-space edge value of {} bytes exceeds sanity limit",
                        value.len()
                    );
                }
                self.edge_count = match self.edge_count.checked_add(1) {
                    Some(v) => v,
                    None => return err_box!("dir-space edge count overflow"),
                };
                child_ids.push(i64_from_value(&value)?);
            }

            for chunk in child_ids.chunks(POINT_CHILD_BATCH_SIZE) {
                self.check_cooperative(lease)?;
                self.check_budget()?;
                let inodes = reader.get_inodes(chunk)?;
                for (&child_id, inode_opt) in chunk.iter().zip(inodes.iter()) {
                    self.inode_count = match self.inode_count.checked_add(1) {
                        Some(v) => v,
                        None => return err_box!("dir-space inode count overflow"),
                    };
                    let Some(view) = inode_opt else {
                        self.stats.record_anomaly(1);
                        continue;
                    };
                    if view.id() != child_id {
                        return err_box!(
                            "dir-space scan: inode key/value id mismatch (key {}, value {})",
                            child_id,
                            view.id()
                        );
                    }
                    let fact = inode_fact_from_view_ref(view);
                    if fact.is_dir {
                        if depth.saturating_add(1) >= self.context.limits.max_depth {
                            return err_box!(
                                "dir-space MaxDepth limit {} exceeded",
                                self.context.limits.max_depth
                            );
                        }
                        self.dir_count = match self.dir_count.checked_add(1) {
                            Some(v) => v,
                            None => return err_box!("dir-space dir count overflow"),
                        };
                        self.aggregator.add_dir(child_id, parent_id)?;
                        queue.push_back((child_id, depth.saturating_add(1)));
                    } else {
                        if fact.logical_len < 0 {
                            self.stats.record_anomaly(1);
                        }
                        self.aggregator.add_file(parent_id, fact.logical_len)?;
                    }
                }
            }
        }
        self.check_budget()
    }

    fn build(self) -> CommonResult<UnpublishedScan> {
        let AggregationOutput {
            root,
            entries,
            mut stats,
        } = self.aggregator.build()?;
        stats.anomaly_count = stats.anomaly_count.saturating_add(self.stats.anomaly_count);
        stats.degraded = stats.degraded || self.stats.degraded;
        Ok(UnpublishedScan {
            root,
            entries,
            stats,
            inode_count: self.inode_count,
            edge_count: self.edge_count,
        })
    }
}

impl GlobalScanner {
    /// Run one global scan round on `lease`'s consistent read view.
    ///
    /// Takes only the single-round lease and a context: the scanner holds no
    /// store clones and never touches `fs_dir`. The caller (manager) drops
    /// the lease afterwards so a pending restore can proceed.
    pub fn scan(
        lease: &MetadataScanLease,
        context: &ScanContext,
    ) -> CommonResult<UnpublishedSnapshot> {
        let started_ms = now_ms();
        let reader = lease.snapshot_reader()?;

        let mut scan = ScanState::new(context);
        scan.check_cooperative(lease)?;
        scan.scan_inodes(&reader, lease)?;
        scan.scan_edges(&reader, lease)?;
        let UnpublishedScan {
            root,
            entries,
            stats,
            inode_count,
            edge_count,
        } = scan.build()?;

        drop(reader);

        Ok(UnpublishedSnapshot {
            source: SnapshotSource::Global,
            root_id: ROOT_INODE_ID,
            root,
            entries,
            started_ms,
            finished_ms: now_ms(),
            inode_count,
            edge_count,
            stats,
        })
    }
}

/// Mutable scan-round state shared by the CF passes.
struct ScanState<'a> {
    context: &'a ScanContext,
    facts: HashMap<i64, InodeFact>,
    aggregator: SubtreeAggregator,
    inode_count: u64,
    edge_count: u64,
    dir_count: u64,
    stats: AggregationStats,
}

impl<'a> ScanState<'a> {
    fn new(context: &'a ScanContext) -> Self {
        let limits = context.limits;
        Self {
            context,
            facts: HashMap::new(),
            aggregator: SubtreeAggregator::new(ROOT_INODE_ID, limits.max_dirs, limits.max_depth),
            inode_count: 0,
            edge_count: 0,
            dir_count: 0,
            stats: AggregationStats::default(),
        }
    }

    /// Cooperative abort check: lease cancellation, master role and deadline.
    fn check_cooperative(&self, lease: &MetadataScanLease) -> CommonResult<()> {
        if lease.is_cancelled() {
            return err_box!("dir-space scan aborted: scan lease cancelled by restore");
        }
        if !(self.context.is_active)() {
            return err_box!("dir-space scan aborted: master role lost");
        }
        if Instant::now() >= self.context.deadline {
            return err_box!("dir-space global scan timed out");
        }
        Ok(())
    }

    fn check_budget(&self) -> CommonResult<()> {
        let limits = self.context.limits;
        if self.inode_count > limits.max_inodes {
            return err_box!("dir-space MaxInodes limit {} exceeded", limits.max_inodes);
        }
        if self.edge_count > limits.max_edges {
            return err_box!("dir-space MaxEdges limit {} exceeded", limits.max_edges);
        }
        let estimated = limits.estimated_bytes(self.inode_count, self.dir_count, self.edge_count);
        if estimated > limits.memory_budget_bytes {
            return err_box!(
                "dir-space memory budget {} bytes exceeded (estimated {} bytes)",
                limits.memory_budget_bytes,
                estimated
            );
        }
        Ok(())
    }

    fn scan_inodes(
        &mut self,
        reader: &InodeStoreSnapshot<'_>,
        lease: &MetadataScanLease,
    ) -> CommonResult<()> {
        self.check_cooperative(lease)?;
        let iter = reader.scan_inodes()?;
        let max_record = self.context.limits.max_inode_record_bytes;

        for item in iter {
            let (key, value) = try_err!(item);

            if value.len() as u64 > max_record {
                return err_box!(
                    "dir-space inode record of {} bytes exceeds limit {}",
                    value.len(),
                    max_record
                );
            }

            let id = inode_id_from_key(&key)?;
            let fact = inode_fact_from_view(id, &value)?;

            if fact.is_dir {
                self.dir_count = match self.dir_count.checked_add(1) {
                    Some(v) => v,
                    None => return err_box!("dir-space dir count overflow"),
                };
            }

            self.facts.insert(id, fact);
            self.inode_count = match self.inode_count.checked_add(1) {
                Some(v) => v,
                None => return err_box!("dir-space inode count overflow"),
            };

            if self.inode_count.is_multiple_of(CHECK_INTERVAL) {
                self.check_budget()?;
                self.check_cooperative(lease)?;
            }
        }

        self.check_budget()
    }

    fn scan_edges(
        &mut self,
        reader: &InodeStoreSnapshot<'_>,
        lease: &MetadataScanLease,
    ) -> CommonResult<()> {
        self.check_cooperative(lease)?;
        let iter = reader.scan_edges()?;

        for item in iter {
            let (key, value) = try_err!(item);

            if value.len() > MAX_EDGE_VALUE_BYTES {
                return err_box!(
                    "dir-space edge value of {} bytes exceeds sanity limit",
                    value.len()
                );
            }

            self.edge_count = match self.edge_count.checked_add(1) {
                Some(v) => v,
                None => return err_box!("dir-space edge count overflow"),
            };

            let parent_id = edge_parent_from_key(&key)?;
            let child_id = i64_from_value(&value)?;

            self.apply_edge(parent_id, child_id)?;

            if self.edge_count.is_multiple_of(CHECK_INTERVAL) {
                self.check_budget()?;
                self.check_cooperative(lease)?;
            }
        }

        self.check_budget()
    }

    fn apply_edge(&mut self, parent_id: i64, child_id: i64) -> CommonResult<()> {
        // Negative inode ids are historical corruption; skip degraded.
        if parent_id < 0 || child_id < 0 {
            self.stats.record_anomaly(1);
            return Ok(());
        }

        let Some(child_fact) = self.facts.get(&child_id).copied() else {
            // Edge to a missing inode: ignore, degrade.
            self.stats.record_anomaly(1);
            return Ok(());
        };

        if child_fact.is_dir {
            self.aggregator.add_dir(child_id, parent_id)?;
        } else {
            // Negative file lengths degrade to 0 and mark the round.
            if child_fact.logical_len < 0 {
                self.stats.record_anomaly(1);
            }
            // Hard links surface as multiple edges to the same inode; the
            // aggregator counts each edge (namespace semantics).
            self.aggregator
                .add_file(parent_id, child_fact.logical_len)?;
        }
        Ok(())
    }

    fn build(self) -> CommonResult<UnpublishedScan> {
        let AggregationOutput {
            root,
            entries,
            mut stats,
        } = self.aggregator.build()?;
        // Merge scan-pass anomalies (missing edge targets, negative lengths)
        // into the aggregator's anomalies so the published meta reports both.
        stats.anomaly_count = stats.anomaly_count.saturating_add(self.stats.anomaly_count);
        stats.degraded = stats.degraded || self.stats.degraded;
        Ok(UnpublishedScan {
            root,
            entries,
            stats,
            inode_count: self.inode_count,
            edge_count: self.edge_count,
        })
    }
}

/// Aggregation output plus pass counters, kept together while `build`
/// consumes the scan state.
struct UnpublishedScan {
    root: DirAgg,
    entries: HashMap<i64, DirAgg>,
    stats: AggregationStats,
    inode_count: u64,
    edge_count: u64,
}

fn now_ms() -> i64 {
    LocalTime::mills() as i64
}

/// Parse an inode record key (`i64 big-endian inode id`).
fn inode_id_from_key(key: &[u8]) -> CommonResult<i64> {
    if key.len() != 8 {
        return err_box!(
            "dir-space scan: unparseable inode key of {} bytes",
            key.len()
        );
    }
    RocksUtils::i64_from_bytes(key)
}

/// Parse an edge record key (`i64 big-endian parent id` + `child name`).
fn edge_parent_from_key(key: &[u8]) -> CommonResult<i64> {
    if key.len() < 8 {
        return err_box!(
            "dir-space scan: unparseable edge key of {} bytes",
            key.len()
        );
    }
    RocksUtils::i64_from_bytes(&key[..8])
}

fn i64_from_value(value: &[u8]) -> CommonResult<i64> {
    RocksUtils::i64_from_bytes(value)
}

/// Deserialize one inode record into a compact fact. Corruption or a
/// key/value id mismatch fails the whole round (never silently skipped).
fn inode_fact_from_view(id: i64, bytes: &[u8]) -> CommonResult<InodeFact> {
    let view: InodeView = SerdeUtils::deserialize(bytes)?;
    if view.id() != id {
        return err_box!(
            "dir-space scan: inode key/value id mismatch (key {}, value {})",
            id,
            view.id()
        );
    }
    Ok(inode_fact_from_view_ref(&view))
}

fn inode_fact_from_view_ref(view: &InodeView) -> InodeFact {
    match view {
        InodeView::Dir(_) => InodeFact {
            is_dir: true,
            logical_len: 0,
        },
        _ => InodeFact {
            is_dir: false,
            logical_len: inode_logical_len(view),
        },
    }
}

/// Logical length of a non-directory inode (negative lengths degrade to 0).
fn inode_logical_len(view: &InodeView) -> i64 {
    match view {
        InodeView::File(f) => f.len,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::meta::inode::{InodeDir, InodeFile};
    use crate::master::meta::store::{InodeStore, RocksInodeStore};
    use crate::master::meta::FsDir;
    use crate::master::Master;
    use curvine_rocksdb::DBConf;
    use curvine_runtime::common::Utils;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn limits() -> ScanLimits {
        ScanLimits {
            max_inodes: 1_000_000,
            max_edges: 1_000_000,
            max_dirs: 1_000_000,
            max_depth: 1024,
            memory_budget_bytes: 1 << 30,
            max_inode_record_bytes: 16 * 1024 * 1024,
        }
    }

    fn context(is_active: &Arc<AtomicBool>) -> ScanContext {
        let flag = Arc::clone(is_active);
        ScanContext {
            deadline: Instant::now() + Duration::from_secs(120),
            is_active: Box::new(move || flag.load(Ordering::SeqCst)),
            limits: limits(),
        }
    }

    fn new_store(name: &str) -> CommonResult<InodeStore> {
        Master::init_test_metrics();
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "dir-space-scanner/{}-{}",
            name,
            Utils::rand_str(6)
        )));
        let rocks = RocksInodeStore::new(conf, true)?;
        InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000)?))
    }

    fn write_batch(
        store: &InodeStore,
        inodes: &[InodeView],
        edges: &[(i64, String, i64)],
    ) -> CommonResult<()> {
        let mut batch = store.new_batch();
        for inode in inodes {
            batch.write_inode(inode)?;
        }
        for (parent, name, child) in edges {
            batch.add_child(*parent, name, *child)?;
        }
        batch.commit()
    }

    fn dir(name: &str, id: i64, parent: i64) -> InodeView {
        InodeView::new_dir(name.to_string(), InodeDir::new(id, parent))
    }

    fn file(name: &str, id: i64, parent: i64, len: i64) -> InodeView {
        let mut f = InodeFile::new(id, parent);
        f.len = len;
        InodeView::new_file(name.to_string(), f)
    }

    /// root ── a(dir 2001) ── a.txt(file 3001, len 100)
    ///     └── b(dir 2002) ── b.txt(file 3002, len 40), hard link "hard"
    fn seed_basic(store: &InodeStore) -> CommonResult<()> {
        let root = FsDir::create_root();
        let dir_a = dir("a", 2001, ROOT_INODE_ID);
        let dir_b = dir("b", 2002, ROOT_INODE_ID);
        let file_a = file("a.txt", 3001, 2001, 100);
        let file_b = file("b.txt", 3002, 2002, 40);

        write_batch(
            store,
            &[root, dir_a, dir_b, file_a, file_b],
            &[
                (ROOT_INODE_ID, "a".to_string(), 2001),
                (ROOT_INODE_ID, "b".to_string(), 2002),
                (2001, "a.txt".to_string(), 3001),
                (2002, "b.txt".to_string(), 3002),
                // Hard link: second edge to the same file inode 3002.
                (2002, "hard".to_string(), 3002),
            ],
        )
    }

    fn run_global(store: &InodeStore, ctx: &ScanContext) -> CommonResult<UnpublishedSnapshot> {
        let lease = store.begin_snapshot_scan()?;
        GlobalScanner::scan(&lease, ctx)
    }

    fn agg_of(snap: &UnpublishedSnapshot, id: i64) -> DirAgg {
        if id == ROOT_INODE_ID || id == snap.root_id {
            snap.root
        } else {
            *snap
                .entries
                .get(&id)
                .unwrap_or_else(|| panic!("entry {} missing", id))
        }
    }

    // -------------------------------------------------------------------
    // Step 1: real temp-RocksDB global scan
    // -------------------------------------------------------------------

    #[test]
    fn global_scan_basic_tree_counts_and_aggregates() -> CommonResult<()> {
        let store = new_store("basic-tree")?;
        seed_basic(&store)?;
        let active = Arc::new(AtomicBool::new(true));
        let snap = run_global(&store, &context(&active))?;

        // Root: 3 file edges (a.txt, b.txt, hard link), 100 + 40 + 40 bytes,
        // 3 directories (root itself, a, b).
        let root = agg_of(&snap, ROOT_INODE_ID);
        assert_eq!(root.file_count, 3);
        assert_eq!(root.logical_bytes, 180);
        assert_eq!(root.directory_count, 3);

        let dir_a = agg_of(&snap, 2001);
        assert_eq!(dir_a.file_count, 1);
        assert_eq!(dir_a.logical_bytes, 100);
        assert_eq!(dir_a.directory_count, 1);

        let dir_b = agg_of(&snap, 2002);
        assert_eq!(dir_b.file_count, 2); // b.txt + hard link edge
        assert_eq!(dir_b.logical_bytes, 80);
        assert_eq!(dir_b.directory_count, 1);

        assert_eq!(snap.inode_count, 5);
        assert_eq!(snap.edge_count, 5);
        assert_eq!(snap.source, SnapshotSource::Global);
        assert!(!snap.stats.degraded);
        assert_eq!(snap.stats.anomaly_count, 0);
        assert!(snap.finished_ms >= snap.started_ms);

        Ok(())
    }

    #[test]
    fn global_scan_symlink_counts_as_file_entry() -> CommonResult<()> {
        let store = new_store("symlink")?;
        let root = FsDir::create_root();
        let link = InodeView::new_file(
            "lnk".to_string(),
            InodeFile::with_link(4001, 0, "/a", 0o777, None, None),
        );
        write_batch(
            &store,
            &[root, link],
            &[(ROOT_INODE_ID, "lnk".to_string(), 4001)],
        )?;

        let active = Arc::new(AtomicBool::new(true));
        let snap = run_global(&store, &context(&active))?;
        let root_agg = agg_of(&snap, ROOT_INODE_ID);
        assert_eq!(root_agg.file_count, 1);
        assert_eq!(root_agg.logical_bytes, 0);
        assert_eq!(root_agg.directory_count, 1);
        Ok(())
    }

    #[test]
    fn deep_chain_within_depth_limit_aggregates_to_root() -> CommonResult<()> {
        let store = new_store("deep-chain")?;
        let depth = 100;
        let mut inodes = vec![FsDir::create_root()];
        let mut edges = Vec::new();
        let mut parent = ROOT_INODE_ID;
        for i in 0..depth {
            let id = 7000 + i;
            inodes.push(dir(&format!("d{}", i), id, parent));
            edges.push((parent, format!("d{}", i), id));
            parent = id;
        }
        inodes.push(file("leaf", 8000, parent, 5));
        edges.push((parent, "leaf".to_string(), 8000));
        write_batch(&store, &inodes, &edges)?;

        let active = Arc::new(AtomicBool::new(true));
        let snap = run_global(&store, &context(&active))?;
        assert_eq!(snap.root.file_count, 1);
        assert_eq!(snap.root.logical_bytes, 5);
        assert_eq!(snap.root.directory_count, (depth + 1) as u64);
        Ok(())
    }

    // -------------------------------------------------------------------
    // Step 2: error paths
    // -------------------------------------------------------------------

    #[test]
    fn missing_child_inode_is_degraded_anomaly() -> CommonResult<()> {
        let store = new_store("missing-child")?;
        let root = FsDir::create_root();
        // Edge to an inode that does not exist.
        write_batch(
            &store,
            &[root],
            &[(ROOT_INODE_ID, "ghost".to_string(), 9999)],
        )?;

        let active = Arc::new(AtomicBool::new(true));
        let snap = run_global(&store, &context(&active))?;
        assert!(snap.stats.degraded);
        assert_eq!(snap.stats.anomaly_count, 1);
        assert_eq!(snap.root.file_count, 0);
        Ok(())
    }

    #[test]
    fn negative_length_counts_as_zero_and_degrades() -> CommonResult<()> {
        let store = new_store("negative-length")?;
        let root = FsDir::create_root();
        let bad = file("neg", 5001, ROOT_INODE_ID, -7);
        write_batch(
            &store,
            &[root, bad],
            &[(ROOT_INODE_ID, "neg".to_string(), 5001)],
        )?;

        let active = Arc::new(AtomicBool::new(true));
        let snap = run_global(&store, &context(&active))?;
        assert!(snap.stats.degraded);
        assert_eq!(snap.stats.anomaly_count, 1);
        assert_eq!(snap.root.file_count, 1);
        assert_eq!(snap.root.logical_bytes, 0);
        Ok(())
    }

    #[test]
    fn corrupt_inode_bytes_fail_the_round() -> CommonResult<()> {
        let store = new_store("corrupt-inode")?;
        let root = FsDir::create_root();
        write_batch(&store, &[root], &[])?;

        // Inject garbage bytes under a valid inode key.
        let mut batch = store.new_batch();
        batch.put_cf_raw(
            RocksInodeStore::CF_INODES,
            RocksUtils::i64_to_bytes(6001),
            vec![0xffu8, 0x00, 0x13, 0x37],
        )?;
        batch.commit()?;

        // The injected record must actually be visible to a full CF scan.
        let visible = store.store().inodes_iter()?.count();
        assert!(
            visible >= 2,
            "injected record not visible, count={}",
            visible
        );

        let active = Arc::new(AtomicBool::new(true));
        let err = run_global(&store, &context(&active)).unwrap_err();
        let msg = err.to_string();
        // Deserialization of the garbage record fails the whole round. The
        // exact message comes from the serde layer ("invalid value: integer").
        assert!(
            msg.contains("invalid value") || msg.contains("deserialize"),
            "unexpected error: {}",
            msg
        );
        Ok(())
    }

    #[test]
    fn unparseable_inode_key_fails_the_round() -> CommonResult<()> {
        let store = new_store("bad-inode-key")?;
        let root = FsDir::create_root();
        write_batch(&store, &[root], &[])?;

        let good: InodeView = file("x", 6002, ROOT_INODE_ID, 1);
        let bytes = SerdeUtils::serialize(&good)?;
        // Key is 5 bytes: not a valid inode id key.
        let mut batch = store.new_batch();
        batch.put_cf_raw(RocksInodeStore::CF_INODES, b"short", bytes)?;
        batch.commit()?;

        let active = Arc::new(AtomicBool::new(true));
        let err = run_global(&store, &context(&active)).unwrap_err();
        assert!(err.to_string().contains("inode key"));
        Ok(())
    }

    #[test]
    fn inode_key_value_id_mismatch_fails_the_round() -> CommonResult<()> {
        let store = new_store("id-mismatch")?;
        let root = FsDir::create_root();
        write_batch(&store, &[root], &[])?;

        let inode: InodeView = file("x", 6002, ROOT_INODE_ID, 1);
        let bytes = SerdeUtils::serialize(&inode)?;
        // Stored under a different id than the inode itself carries.
        let mut batch = store.new_batch();
        batch.put_cf_raw(
            RocksInodeStore::CF_INODES,
            RocksUtils::i64_to_bytes(6003),
            bytes,
        )?;
        batch.commit()?;

        let active = Arc::new(AtomicBool::new(true));
        let err = run_global(&store, &context(&active)).unwrap_err();
        assert!(err.to_string().contains("mismatch"));
        Ok(())
    }

    #[test]
    fn unparseable_edge_key_fails_the_round() -> CommonResult<()> {
        let store = new_store("bad-edge-key")?;
        let root = FsDir::create_root();
        write_batch(&store, &[root], &[])?;

        let value = RocksUtils::i64_to_bytes(6002);
        let mut batch = store.new_batch();
        batch.put_cf_raw(RocksInodeStore::CF_EDGES, b"ed", value)?;
        batch.commit()?;

        let active = Arc::new(AtomicBool::new(true));
        let err = run_global(&store, &context(&active)).unwrap_err();
        assert!(err.to_string().contains("edge key"));
        Ok(())
    }

    #[test]
    fn deadline_aborts_without_result() -> CommonResult<()> {
        let store = new_store("deadline")?;
        seed_basic(&store)?;

        let ctx = ScanContext {
            deadline: Instant::now() - Duration::from_millis(1),
            is_active: Box::new(|| true),
            limits: limits(),
        };
        let err = run_global(&store, &ctx).unwrap_err();
        assert!(err.to_string().contains("timed out"));
        Ok(())
    }

    #[test]
    fn max_inodes_limit_aborts() -> CommonResult<()> {
        let store = new_store("max-inodes")?;
        seed_basic(&store)?;

        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                max_inodes: 2,
                ..limits()
            },
        };
        let err = run_global(&store, &ctx).unwrap_err();
        assert!(err.to_string().contains("MaxInodes"));
        Ok(())
    }

    #[test]
    fn max_edges_limit_aborts() -> CommonResult<()> {
        let store = new_store("max-edges")?;
        seed_basic(&store)?;

        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                max_edges: 2,
                ..limits()
            },
        };
        let err = run_global(&store, &ctx).unwrap_err();
        assert!(err.to_string().contains("MaxEdges"));
        Ok(())
    }

    #[test]
    fn max_dirs_limit_aborts() -> CommonResult<()> {
        let store = new_store("max-dirs")?;
        seed_basic(&store)?;

        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                max_dirs: 1,
                ..limits()
            },
        };
        let err = run_global(&store, &ctx).unwrap_err();
        assert!(err.to_string().contains("MaxDirs"));
        Ok(())
    }

    #[test]
    fn max_depth_limit_aborts() -> CommonResult<()> {
        let store = new_store("max-depth")?;
        seed_basic(&store)?;

        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                max_depth: 1,
                ..limits()
            },
        };
        let err = run_global(&store, &ctx).unwrap_err();
        assert!(err.to_string().contains("MaxDepth"));
        Ok(())
    }

    #[test]
    fn memory_budget_limit_aborts() -> CommonResult<()> {
        let store = new_store("memory-budget")?;
        seed_basic(&store)?;

        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                memory_budget_bytes: 1,
                ..limits()
            },
        };
        let err = run_global(&store, &ctx).unwrap_err();
        assert!(err.to_string().contains("memory budget"));
        Ok(())
    }

    #[test]
    fn role_lost_aborts_without_result() -> CommonResult<()> {
        let store = new_store("role-lost")?;
        seed_basic(&store)?;
        let active = Arc::new(AtomicBool::new(false));
        let err = run_global(&store, &context(&active)).unwrap_err();
        assert!(err.to_string().contains("role"));
        Ok(())
    }

    fn run_point(
        store: &InodeStore,
        root_id: i64,
        ctx: &ScanContext,
    ) -> CommonResult<UnpublishedSnapshot> {
        let lease = store.begin_snapshot_scan()?;
        PointScanner::scan(&lease, root_id, ctx)
    }

    #[test]
    fn tracked_point_scan_excludes_sibling_subtree() -> CommonResult<()> {
        let store = new_store("point-sibling")?;
        seed_basic(&store)?;

        let active = Arc::new(AtomicBool::new(true));
        let snap = run_point(&store, 2001, &context(&active))?;

        assert_eq!(snap.root_id, 2001);
        assert_eq!(snap.source, SnapshotSource::TrackedPoint);
        let root = agg_of(&snap, 2001);
        assert_eq!(root.file_count, 1);
        assert_eq!(root.logical_bytes, 100);
        assert!(!snap.entries.contains_key(&2002));
        assert!(!snap.entries.contains_key(&ROOT_INODE_ID));
        Ok(())
    }

    #[test]
    fn tracked_point_child_batch_boundary() -> CommonResult<()> {
        let store = new_store("point-batch")?;
        let root = FsDir::create_root();
        let dir_a = dir("a", 2001, ROOT_INODE_ID);
        let mut inodes = vec![root, dir_a];
        let mut edges = vec![(ROOT_INODE_ID, "a".to_string(), 2001)];

        for i in 0..1025 {
            let id = 10_000 + i;
            inodes.push(file(&format!("f{i}"), id, 2001, 1));
            edges.push((2001, format!("f{i}"), id));
        }
        write_batch(&store, &inodes, &edges)?;

        let active = Arc::new(AtomicBool::new(true));
        let snap = run_point(&store, 2001, &context(&active))?;
        assert_eq!(snap.root.file_count, 1025);
        Ok(())
    }

    #[test]
    fn tracked_point_max_inodes_limit_aborts() -> CommonResult<()> {
        let store = new_store("point-max-inodes")?;
        let root = FsDir::create_root();
        let dir_a = dir("a", 2001, ROOT_INODE_ID);
        write_batch(
            &store,
            &[
                root,
                dir_a,
                file("f1", 3001, 2001, 1),
                file("f2", 3002, 2001, 1),
            ],
            &[
                (ROOT_INODE_ID, "a".to_string(), 2001),
                (2001, "f1".to_string(), 3001),
                (2001, "f2".to_string(), 3002),
            ],
        )?;
        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                max_inodes: 1,
                ..limits()
            },
        };
        let err = run_point(&store, 2001, &ctx).unwrap_err();
        assert!(err.to_string().contains("MaxInodes"));
        Ok(())
    }

    #[test]
    fn tracked_point_max_depth_limit_aborts() -> CommonResult<()> {
        let store = new_store("point-max-depth")?;
        let depth = 5;
        let mut inodes = vec![FsDir::create_root()];
        let mut edges = Vec::new();
        let mut parent = ROOT_INODE_ID;
        for i in 0..depth {
            let id = 7000 + i;
            inodes.push(dir(&format!("d{i}"), id, parent));
            edges.push((parent, format!("d{i}"), id));
            parent = id;
        }
        write_batch(&store, &inodes, &edges)?;

        let ctx = ScanContext {
            deadline: Instant::now() + Duration::from_secs(60),
            is_active: Box::new(|| true),
            limits: ScanLimits {
                max_depth: 2,
                ..limits()
            },
        };
        let err = run_point(&store, ROOT_INODE_ID, &ctx).unwrap_err();
        assert!(err.to_string().contains("MaxDepth"));
        Ok(())
    }

    #[test]
    fn lease_cancelled_aborts_without_result() -> CommonResult<()> {
        let store = new_store("lease-cancel")?;
        seed_basic(&store)?;

        // Acquire the lease, then start a restore on the same control: the
        // epoch bump cancels the lease (restore itself times out on the
        // still-held lease and rolls back without touching the DB).
        let lease = store.begin_snapshot_scan()?;
        store
            .scan_control()
            .begin_restore(Duration::from_millis(1))
            .ok();
        assert!(lease.is_cancelled());

        let active = Arc::new(AtomicBool::new(true));
        let ctx = context(&active);
        let err = GlobalScanner::scan(&lease, &ctx).unwrap_err();
        assert!(err.to_string().contains("cancel"));
        Ok(())
    }
}
