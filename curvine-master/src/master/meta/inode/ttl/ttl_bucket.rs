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
use curvine_runtime::common::{FastHashMap, FastHashSet};
use log::{debug, info, warn};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct TtlBucket {
    pub interval_start_ms: i64,
    pub interval_duration_ms: i64,
    /// Only reachable through `add`/`remove`/`drain`/`extend`/`snapshot`, so a
    /// mutation cannot silently desync `TtlBucketList::positions`.
    inodes: Mutex<FastHashSet<i64>>,
}

impl TtlBucket {
    pub fn new(interval_start_ms: i64, interval_duration_ms: i64) -> Self {
        Self {
            interval_start_ms,
            interval_duration_ms,
            inodes: Mutex::new(FastHashSet::new()),
        }
    }

    pub fn get_interval_end_ms(&self) -> i64 {
        self.interval_start_ms + self.interval_duration_ms
    }

    pub fn add(&self, inode_id: i64) {
        self.inodes.lock().insert(inode_id);
    }

    pub fn remove(&self, inode_id: i64) -> bool {
        self.inodes.lock().remove(&inode_id)
    }

    /// Take every inode id out of the bucket. Callers that may bail out must
    /// put the drained bucket back (see `TtlBucketList::put_back`).
    pub fn drain(&self) -> FastHashSet<i64> {
        std::mem::take(&mut *self.inodes.lock())
    }

    pub fn extend(&self, ids: impl IntoIterator<Item = i64>) {
        self.inodes.lock().extend(ids);
    }

    pub fn snapshot(&self) -> Vec<i64> {
        self.inodes.lock().iter().copied().collect()
    }

    pub fn is_expired_at(&self, current_time_ms: i64) -> bool {
        self.get_interval_end_ms() <= current_time_ms
    }

    pub fn len(&self) -> usize {
        self.inodes.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

/// The bucket map plus the reverse index that keeps each inode in exactly one
/// bucket.
///
/// Buckets alone cannot answer "where does this inode sit right now": the bucket
/// follows `expiration_ms = mtime + ttl_ms`, and both terms move under an inode
/// that is already indexed (`complete_file` advances mtime, `set_attr` can change
/// or clear the TTL). Re-adding such an inode used to insert it a second time and
/// leave the previous entry behind, so one file could be counted twice and be
/// scanned for cleanup twice. `positions` is the authority: an add moves the
/// inode, a remove drops it from the bucket it actually occupies.
struct TtlBucketState {
    /// Sorted bucket mapping: interval_start_ms -> TtlBucket (sorted by time)
    buckets: BTreeMap<i64, Arc<TtlBucket>>,
    /// inode id -> interval_start_ms of the bucket that holds it
    positions: FastHashMap<i64, i64>,
}

impl TtlBucketState {
    fn bucket(&mut self, interval_start: i64, interval_duration_ms: i64) -> Arc<TtlBucket> {
        self.buckets
            .entry(interval_start)
            .or_insert_with(|| {
                debug!(
                    "creating new TTL bucket for interval starting at {}ms",
                    interval_start
                );
                Arc::new(TtlBucket::new(interval_start, interval_duration_ms))
            })
            .clone()
    }

    /// Drops `id` from the bucket it is indexed under, removing that bucket once
    /// it is empty. Reports whether the inode was indexed at all.
    fn detach(&mut self, id: i64) -> bool {
        let Some(interval_start) = self.positions.remove(&id) else {
            return false;
        };
        if let Some(bucket) = self.buckets.get(&interval_start).cloned() {
            bucket.remove(id);
            if bucket.is_empty() {
                self.buckets.remove(&interval_start);
            }
        }
        debug!("removed inode {} from sorted TTL bucket list", id);
        true
    }
}

pub struct TtlBucketList {
    state: Arc<Mutex<TtlBucketState>>,
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
            state: Arc::new(Mutex::new(TtlBucketState {
                buckets: BTreeMap::new(),
                positions: FastHashMap::default(),
            })),
            interval_duration_ms,
        })
    }

    /// Drop every staged TTL entry; used when the metadata store is rebuilt.
    pub fn clear(&self) {
        let mut state = self.state.lock();
        state.buckets.clear();
        state.positions.clear();
    }

    pub fn total_inodes(&self) -> u64 {
        self.state
            .lock()
            .buckets
            .values()
            .map(|bucket| bucket.len() as u64)
            .sum()
    }

    pub fn buckets_len(&self) -> usize {
        self.state.lock().buckets.len()
    }

    fn get_bucket_interval_start(&self, timestamp_ms: i64) -> i64 {
        (timestamp_ms / self.interval_duration_ms) * self.interval_duration_ms
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

        // A policy that cannot be read leaves the inode where it is: dropping the
        // entry would silently stop TTL cleanup for it.
        let expiration_ms = match inode.expiration_ms() {
            Ok(expiration_ms) => expiration_ms,
            Err(e) => {
                warn!("ttl_bucket: skip add for inode {}: {}", inode.id(), e);
                if let Ok(metrics) = Master::get_metrics() {
                    metrics.ttl_skipped_inodes.inc();
                }
                return;
            }
        };

        let mut state = self.state.lock();
        state.detach(inode.id());

        let Some(expiration_ms) = expiration_ms else {
            return;
        };

        let interval_start = self.get_bucket_interval_start(expiration_ms);
        state
            .bucket(interval_start, self.interval_duration_ms)
            .add(inode.id());
        state.positions.insert(inode.id(), interval_start);

        debug!(
            "added inode {} to sorted TTL bucket list, expires at {}ms",
            inode.id(),
            expiration_ms
        );
    }

    pub fn remove(&self, inode: &InodeView) -> bool {
        self.state.lock().detach(inode.id())
    }

    // Any bucket with start <= current - interval_duration has end <= current ⇒ expired
    pub fn get_expired_buckets_at(&self, current_time_ms: i64) -> Vec<Arc<TtlBucket>> {
        let max_expired_start = current_time_ms.saturating_sub(self.interval_duration_ms);

        let mut state = self.state.lock();

        let keys: Vec<i64> = state
            .buckets
            .range(..=max_expired_start)
            .map(|(k, _)| *k)
            .collect();

        let mut buckets = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(bucket) = state.buckets.remove(&k) {
                // The bucket is out of the map while its inodes are processed, so
                // they are not (re)indexed anywhere until `put_back` restores the
                // ones a pass did not consume.
                for id in bucket.inodes.lock().iter() {
                    state.positions.remove(id);
                }
                buckets.push(bucket);
            }
        }
        buckets
    }

    /// Reinsert a drained bucket after an interrupted cleanup pass (#1754).
    ///
    /// An inode that was re-indexed while the bucket was in flight already sits
    /// in its new bucket, so only the ids without a position are restored.
    pub fn put_back(&self, bucket: &TtlBucket) {
        let ids = bucket.snapshot();
        if ids.is_empty() {
            return;
        }

        let mut state = self.state.lock();
        let pending: Vec<i64> = ids
            .into_iter()
            .filter(|id| !state.positions.contains_key(id))
            .collect();
        if pending.is_empty() {
            return;
        }

        let interval_start = bucket.interval_start_ms;
        state
            .bucket(interval_start, self.interval_duration_ms)
            .inodes
            .lock()
            .extend(pending.iter().copied());
        for id in pending {
            state.positions.insert(id, interval_start);
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

    // `apply_complete_file` re-indexes the file after `file.complete()` advanced
    // its mtime, so one inode can be handed to `add` twice with two different
    // expirations. The second one must move it, not leave the first behind.
    #[test]
    fn re_indexing_moves_the_inode_to_its_new_bucket() {
        let list = ttl_bucket_list();
        list.add(&file_with_ttl(42, 100, 500)); // exp 600 -> bucket 0
        list.add(&file_with_ttl(42, 1200, 500)); // exp 1700 -> bucket 1000

        assert_eq!(list.buckets_len(), 1);
        assert_eq!(list.total_inodes(), 1);

        let drained = list.get_expired_buckets_at(5000);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].interval_start_ms, 1000);
        assert_eq!(drained[0].len(), 1);
    }

    // Re-indexing is also how a TTL is cleared (set_attr / complete with a new
    // storage policy): the inode must leave the index entirely.
    #[test]
    fn re_indexing_without_expiration_drops_the_inode() {
        let list = ttl_bucket_list();
        list.add(&file_with_ttl(42, 100, 500));
        list.add(&file_no_ttl(42, 100));

        assert_eq!(list.total_inodes(), 0);
        assert_eq!(list.buckets_len(), 0);
    }

    #[test]
    fn remove_drops_a_re_indexed_inode_from_its_current_bucket() {
        let list = ttl_bucket_list();
        list.add(&file_with_ttl(42, 100, 500));
        list.add(&file_with_ttl(42, 1200, 500));

        assert!(list.remove(&file_with_ttl(42, 1200, 500)));
        assert_eq!(list.total_inodes(), 0);
        assert_eq!(list.buckets_len(), 0);
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
}
