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

use crate::master::meta::inode::InodeView;
use crate::master::Master;
use curvine_core_error::{err_box, CommonResult};
use log::{debug, info, warn};
use parking_lot::Mutex;
use roaring::RoaringTreemap;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct TtlBucket {
    pub interval_start_ms: i64,
    pub interval_duration_ms: i64,
    pub inodes: Mutex<RoaringTreemap>,
}

impl TtlBucket {
    pub fn new(interval_start_ms: i64, interval_duration_ms: i64) -> Self {
        Self {
            interval_start_ms,
            interval_duration_ms,
            inodes: Mutex::new(RoaringTreemap::new()),
        }
    }

    pub fn get_interval_end_ms(&self) -> i64 {
        self.interval_start_ms + self.interval_duration_ms
    }

    pub fn add(&self, inode_id: i64) {
        self.inodes.lock().insert(inode_id as u64);
    }

    pub fn remove(&self, inode_id: i64) -> bool {
        self.inodes.lock().remove(inode_id as u64)
    }

    pub fn is_expired_at(&self, current_time_ms: i64) -> bool {
        self.get_interval_end_ms() <= current_time_ms
    }

    pub fn len(&self) -> u64 {
        self.inodes.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn optimize(&self) {
        self.inodes.lock().optimize();
    }
}

impl PartialEq for TtlBucket {
    fn eq(&self, other: &Self) -> bool {
        self.interval_start_ms == other.interval_start_ms
    }
}

impl Eq for TtlBucket {}
impl PartialOrd for TtlBucket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TtlBucket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.interval_start_ms.cmp(&other.interval_start_ms)
    }
}

pub struct TtlBucketList {
    /// Sorted bucket mapping: interval_start_ms -> TtlBucket (sorted by time)
    buckets: Arc<Mutex<BTreeMap<i64, Arc<TtlBucket>>>>,
    /// Interval duration for each bucket in milliseconds
    interval_duration_ms: i64,
}

impl TtlBucketList {
    pub fn new(interval_duration_ms: i64) -> CommonResult<Self> {
        if interval_duration_ms <= 0 {
            return err_box!(
                "ttl bucket interval_duration_ms must be positive, actual {}",
                interval_duration_ms
            );
        }

        info!(
            "Creating TTL bucket list with sorted storage, interval duration: {}ms",
            interval_duration_ms
        );
        Ok(Self {
            buckets: Arc::new(Mutex::new(BTreeMap::new())),
            interval_duration_ms,
        })
    }

    pub fn total_inodes(&self) -> u64 {
        self.buckets
            .lock()
            .values()
            .map(|bucket| bucket.len())
            .sum()
    }

    pub fn buckets_len(&self) -> usize {
        self.buckets.lock().len()
    }

    fn get_bucket_interval_start(&self, timestamp_ms: i64) -> i64 {
        (timestamp_ms / self.interval_duration_ms) * self.interval_duration_ms
    }

    fn get_or_create_bucket(&self, expiration_ms: i64) -> Arc<TtlBucket> {
        let interval_start = self.get_bucket_interval_start(expiration_ms);
        let mut binding = self.buckets.lock();
        binding
            .entry(interval_start)
            .or_insert_with(|| {
                debug!(
                    "creating new TTL bucket for interval starting at {}ms",
                    interval_start
                );
                Arc::new(TtlBucket::new(interval_start, self.interval_duration_ms))
            })
            .clone()
    }

    pub fn add(&self, inode: &InodeView) {
        if inode.is_file_entry() {
            warn!(
                "ttl_bucket: skip add for unresolved FileEntry (inode_id={}, name='{}'); TTL index needs a resolved inode",
                inode.id(),
                inode.name()
            );
            return;
        }

        let expiration_ms = match inode.expiration_ms() {
            Ok(Some(expiration_ms)) => expiration_ms,
            Ok(None) => return,
            Err(e) => {
                warn!("ttl_bucket: skip add for inode {}: {}", inode.id(), e);
                if let Ok(metrics) = Master::get_metrics() {
                    metrics.ttl_skipped_inodes.inc();
                }
                return;
            }
        };
        {
            let bucket = self.get_or_create_bucket(expiration_ms);
            bucket.add(inode.id());

            debug!(
                "added inode {} to sorted TTL bucket list, expires at {}ms",
                inode.id(),
                expiration_ms
            );
        }
    }

    pub fn remove(&self, inode: &InodeView) -> bool {
        if inode.is_file_entry() {
            warn!(
                "ttl_bucket: skip remove for unresolved FileEntry (inode_id={}, name='{}')",
                inode.id(),
                inode.name()
            );
            return false;
        }

        let expiration_ms = match inode.expiration_ms() {
            Ok(Some(expiration_ms)) => expiration_ms,
            Ok(None) => return false,
            Err(e) => {
                warn!("ttl_bucket: skip remove for inode {}: {}", inode.id(), e);
                return false;
            }
        };
        let interval_start = self.get_bucket_interval_start(expiration_ms);
        if let Some(bucket) = self.buckets.lock().get(&interval_start) {
            let res = bucket.remove(inode.id());
            debug!("removed inode {} from sorted TTL bucket list", inode.id());
            res
        } else {
            false
        }
    }

    // Any bucket with start <= current - interval_duration has end <= current ⇒ expired
    pub fn get_expired_buckets_at(&self, current_time_ms: i64) -> Vec<Arc<TtlBucket>> {
        let max_expired_start = current_time_ms.saturating_sub(self.interval_duration_ms);

        let mut map = self.buckets.lock();

        let keys: Vec<i64> = map.range(..=max_expired_start).map(|(k, _)| *k).collect();

        let mut buckets = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(bucket) = map.remove(&k) {
                buckets.push(bucket);
            }
        }
        buckets
    }

    pub fn optimize_all(&self) {
        let buckets: Vec<Arc<TtlBucket>> =
            self.buckets.lock().values().cloned().collect();
        for bucket in buckets {
            bucket.optimize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::{InodeFile, InodeView};
    use curvine_model::TtlAction;

    const INTERVAL_MS: i64 = 1000;

    fn file_with_ttl(id: i64, mtime: i64, ttl_ms: i64) -> InodeView {
        let mut f = InodeFile::new(id, mtime);
        f.storage_policy.ttl_ms = ttl_ms;
        f.storage_policy.ttl_action = TtlAction::Delete;
        InodeView::new_file("t".to_string(), f)
    }

    fn file_no_ttl(id: i64, mtime: i64) -> InodeView {
        InodeView::new_file("t".to_string(), InodeFile::new(id, mtime))
    }

    fn ttl_bucket_list() -> TtlBucketList {
        TtlBucketList::new(INTERVAL_MS).expect("valid ttl bucket interval")
    }

    #[test]
    fn add_skips_when_ttl_disabled() {
        let list = ttl_bucket_list();
        list.add(&file_no_ttl(1, 0));
        assert_eq!(list.total_inodes(), 0);
        assert_eq!(list.buckets_len(), 0);
    }

    #[test]
    fn add_places_inode_in_quantized_bucket() {
        let list = ttl_bucket_list();
        // expiration = mtime + ttl_ms = 100 + 500 = 600 -> interval_start 0
        list.add(&file_with_ttl(42, 100, 500));
        assert_eq!(list.total_inodes(), 1);
        assert_eq!(list.buckets_len(), 1);
    }

    #[test]
    fn add_same_interval_merges_into_one_bucket() {
        let list = ttl_bucket_list();
        list.add(&file_with_ttl(1, 0, 400)); // exp 400 -> start 0
        list.add(&file_with_ttl(2, 50, 350)); // exp 400 -> start 0
        assert_eq!(list.total_inodes(), 2);
        assert_eq!(list.buckets_len(), 1);
    }

    #[test]
    fn add_different_intervals_use_multiple_buckets() {
        let list = ttl_bucket_list();
        list.add(&file_with_ttl(1, 0, 500)); // exp 500 -> start 0
        list.add(&file_with_ttl(2, 500, 1000)); // exp 1500 -> start 1000
        assert_eq!(list.total_inodes(), 2);
        assert_eq!(list.buckets_len(), 2);
    }

    #[test]
    fn remove_drops_inode_from_bucket() {
        let list = ttl_bucket_list();
        let f = file_with_ttl(7, 0, 100);
        list.add(&f);
        assert_eq!(list.total_inodes(), 1);
        assert!(list.remove(&f));
        assert_eq!(list.total_inodes(), 0);
    }

    #[test]
    fn remove_unknown_returns_false() {
        let list = ttl_bucket_list();
        let f = file_with_ttl(9, 0, 100);
        assert!(!list.remove(&f));
    }

    #[test]
    fn get_expired_buckets_at_drains_and_matches_time_rule() {
        let list = ttl_bucket_list();
        list.add(&file_with_ttl(1, 0, 500)); // exp 500, bucket [0, INTERVAL_MS)

        let now = 2000_i64;
        let drained = list.get_expired_buckets_at(now);
        assert_eq!(drained.len(), 1);
        let b = &drained[0];
        assert_eq!(b.interval_start_ms, 0);
        assert!(b.is_expired_at(now));
        assert_eq!(b.len(), 1);
        assert_eq!(list.buckets_len(), 0);
    }

    #[test]
    fn get_expired_buckets_at_empty_when_not_yet_expired() {
        let list = ttl_bucket_list();
        // exp = 0 + 10_000 = 10_000 -> interval_start 10_000
        list.add(&file_with_ttl(1, 0, 10_000));

        let drained = list.get_expired_buckets_at(5000);
        assert!(drained.is_empty());
        assert_eq!(list.buckets_len(), 1);
        assert_eq!(list.total_inodes(), 1);
    }

    /// RoaringTreemap must handle IDs above u32::MAX (InodeId is i64 with a
    /// 2^40-1 ceiling); this is exactly why we picked the treemap variant
    /// over the u32-only RoaringBitmap.
    #[test]
    fn add_handles_large_inode_ids() {
        let list = ttl_bucket_list();
        let big_id: i64 = (1i64 << 39) - 1;
        let f = file_with_ttl(big_id, 0, 500);
        list.add(&f);
        assert_eq!(list.total_inodes(), 1);
        assert!(list.remove(&f));
        assert_eq!(list.total_inodes(), 0);
    }

    /// `optimize_all` must be a cardinality-preserving recompression; if it
    /// ever dropped elements, TTL cleanup would silently miss inodes.
    #[test]
    fn optimize_all_preserves_cardinality() {
        let list = ttl_bucket_list();
        for id in 0..1024i64 {
            list.add(&file_with_ttl(id, 0, 500));
        }
        assert_eq!(list.total_inodes(), 1024);

        list.optimize_all();
        // Recompression must not change membership.
        assert_eq!(list.total_inodes(), 1024);

        // A spot-check that individual IDs are still findable via remove.
        let probe = file_with_ttl(700, 0, 500);
        assert!(list.remove(&probe));
        assert_eq!(list.total_inodes(), 1023);
    }

    /// Roaring iteration is ascending; the checker relies on this to feed
    /// RocksDB point-lookups in ID order for locality.
    #[test]
    fn drained_bucket_iterates_in_ascending_id_order() {
        let list = ttl_bucket_list();
        for id in [50i64, 3, 999, 7, 128] {
            list.add(&file_with_ttl(id, 0, 500));
        }

        let drained = list.get_expired_buckets_at(2000);
        assert_eq!(drained.len(), 1);
        let taken = std::mem::take(&mut *drained[0].inodes.lock());
        let ids: Vec<i64> = taken.into_iter().map(|v| v as i64).collect();
        assert_eq!(ids, vec![3, 7, 50, 128, 999]);
    }

    /// Memory footprint at realistic scale: 10M inode IDs across 1000 TTL
    /// buckets (~10k contiguous IDs per bucket, matching the production
    /// pattern of monotonic InodeId allocation + time-window bucketing).
    ///
    /// Measures `RoaringTreemap::serialized_size()` per bucket, which is a
    /// tight proxy for heap footprint: container bytes dominate, and the
    /// per-bucket BTreeMap node + Arc + Mutex overhead (~100 B x 1000 = 100 KB)
    /// is negligible at this scale.
    ///
    /// Reference: the previous `FastHashSet<i64>` implementation would need
    /// ~103 MB for the same workload (~10.3 B/inode = 8 B key + 1 B ctrl byte
    /// at 7/8 load factor).
    ///
    /// Run with `--nocapture` to see the actual numbers:
    ///   cargo test -p curvine-master --lib \
    ///       memory_footprint_10m_inodes_1k_buckets -- --nocapture
    #[test]
    fn memory_footprint_10m_inodes_1k_buckets() {
        const INTERVAL_MS: i64 = 3_600_000; // 1h TTL bucket interval
        const NUM_BUCKETS: i64 = 1_000;
        const IDS_PER_BUCKET: i64 = 10_000;
        const TOTAL_INODES: i64 = NUM_BUCKETS * IDS_PER_BUCKET; // 10_000_000

        let list =
            TtlBucketList::new(INTERVAL_MS).expect("valid ttl bucket interval");

        // Bucket i receives IDs [i*IDS_PER_BUCKET + 1, (i+1)*IDS_PER_BUCKET].
        // Drive `get_or_create_bucket` + `TtlBucket::add` directly to avoid
        // 10M `InodeView`/String allocations that would dominate runtime; the
        // public `TtlBucketList::add(&InodeView)` path is covered by the other
        // tests in this module, here we measure roaring-side memory only.
        for bucket_idx in 0..NUM_BUCKETS {
            let expiration_ms = bucket_idx * INTERVAL_MS + 1;
            let bucket = list.get_or_create_bucket(expiration_ms);
            for offset in 0..IDS_PER_BUCKET {
                bucket.add(bucket_idx * IDS_PER_BUCKET + offset + 1);
            }
        }

        assert_eq!(list.total_inodes(), TOTAL_INODES as u64);
        assert_eq!(list.buckets_len(), NUM_BUCKETS as usize);

        let raw_bytes: usize = {
            let map = list.buckets.lock();
            map.values()
                .map(|b| b.inodes.lock().serialized_size())
                .sum()
        };

        list.optimize_all();

        let optimized_bytes: usize = {
            let map = list.buckets.lock();
            map.values()
                .map(|b| b.inodes.lock().serialized_size())
                .sum()
        };

        // Legacy FastHashSet<i64> reference: ~10.3 B/inode.
        let hashset_bytes: usize = (TOTAL_INODES as f64 * 10.3) as usize;

        println!("=== TTL index footprint: 10M inodes / 1k buckets ===");
        println!(
            "roaring raw:         {:>10} B  ({:6.2} MB, {:.3} B/inode)",
            raw_bytes,
            raw_bytes as f64 / 1_048_576.0,
            raw_bytes as f64 / TOTAL_INODES as f64
        );
        println!(
            "roaring optimized:   {:>10} B  ({:6.2} MB, {:.4} B/inode)",
            optimized_bytes,
            optimized_bytes as f64 / 1_048_576.0,
            optimized_bytes as f64 / TOTAL_INODES as f64
        );
        println!(
            "legacy hashset ref:  {:>10} B  ({:6.2} MB, ~10.300 B/inode)",
            hashset_bytes,
            hashset_bytes as f64 / 1_048_576.0
        );
        println!(
            "compression vs ref:  raw {:.1}x, optimized {:.1}x",
            hashset_bytes as f64 / raw_bytes.max(1) as f64,
            hashset_bytes as f64 / optimized_bytes.max(1) as f64
        );

        // Guard rails (fail loudly if the compression story ever regresses).
        //
        // Raw: each 10k-ID bucket needs 1-2 bitmap containers (8 KB each,
        // some buckets straddle 2^16 chunk boundaries) => <16 MB total.
        // Optimized: 10k contiguous IDs collapse to run containers (4 B/run),
        // most buckets have 1-2 runs => total drops well under 1 MB.
        assert!(
            raw_bytes < 16 * 1024 * 1024,
            "raw roaring {} B exceeds 16 MB ceiling",
            raw_bytes
        );
        assert!(
            optimized_bytes < 1024 * 1024,
            "optimized roaring {} B exceeds 1 MB ceiling",
            optimized_bytes
        );
        assert!(
            hashset_bytes / optimized_bytes.max(1) >= 100,
            "compression vs hashset degraded below 100x (raw={}, opt={})",
            hashset_bytes,
            optimized_bytes
        );
    }

    /// `optimize_all` must not hold the global buckets mutex across the whole
    /// pass. This test can't measure lock-hold time directly, but it exercises
    /// the interleaving that the lock-narrowing change enables: a writer
    /// thread keeps inserting into brand-new buckets while the main thread
    /// runs `optimize_all` in a loop. If the global lock were still held for
    /// the full pass, the writer would stall behind each optimize round; the
    /// assertions guard against deadlock, panic, or dropped elements.
    #[test]
    fn optimize_all_tolerates_concurrent_writers() {
        use std::thread;

        const PRE_BUCKETS: i64 = 100;
        const POST_BUCKETS: i64 = 100;
        const IDS_PER_BUCKET: i64 = 1_000;

        let list = Arc::new(ttl_bucket_list());

        // Pre-populate so optimize_all has real container work to do.
        for bucket_idx in 0..PRE_BUCKETS {
            let expiration_ms = bucket_idx * INTERVAL_MS + 1;
            let bucket = list.get_or_create_bucket(expiration_ms);
            for offset in 0..IDS_PER_BUCKET {
                bucket.add(bucket_idx * IDS_PER_BUCKET + offset + 1);
            }
        }
        assert_eq!(
            list.total_inodes(),
            (PRE_BUCKETS * IDS_PER_BUCKET) as u64
        );

        // Writer touches brand-new buckets so it must acquire the global
        // `buckets` mutex (via get_or_create_bucket) while optimize_all is
        // walking the map -- the exact contention the change unblocks.
        let writer_list = Arc::clone(&list);
        let writer = thread::spawn(move || {
            for bucket_idx in PRE_BUCKETS..(PRE_BUCKETS + POST_BUCKETS) {
                let expiration_ms = bucket_idx * INTERVAL_MS + 1;
                let bucket = writer_list.get_or_create_bucket(expiration_ms);
                for offset in 0..IDS_PER_BUCKET {
                    bucket.add(bucket_idx * IDS_PER_BUCKET + offset + 1);
                }
            }
        });

        // Main thread: run several optimize rounds while the writer is active.
        for _ in 0..5 {
            list.optimize_all();
        }

        writer.join().expect("writer thread panicked");

        let total_buckets = PRE_BUCKETS + POST_BUCKETS;
        assert_eq!(
            list.total_inodes(),
            (total_buckets * IDS_PER_BUCKET) as u64,
            "optimize_all raced with concurrent add and lost elements"
        );
        assert_eq!(list.buckets_len(), total_buckets as usize);
    }
}
