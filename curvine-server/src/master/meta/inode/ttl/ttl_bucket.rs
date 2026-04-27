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
use log::{debug, info, warn};
use orpc::common::FastHashSet;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct TtlBucket {
    pub interval_start_ms: i64,
    pub interval_duration_ms: i64,
    pub inodes: Mutex<FastHashSet<i64>>,
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

pub struct TtlBucketList {
    /// Sorted bucket mapping: interval_start_ms -> TtlBucket (sorted by time)
    buckets: Arc<Mutex<BTreeMap<i64, Arc<TtlBucket>>>>,
    /// Interval duration for each bucket in milliseconds
    interval_duration_ms: i64,
}

impl TtlBucketList {
    pub fn new(interval_duration_ms: i64) -> Self {
        if interval_duration_ms <= 0 {
            panic!("interval_duration_ms must be positive");
        }

        info!(
            "Creating TTL bucket list with sorted storage, interval duration: {}ms",
            interval_duration_ms
        );
        Self {
            buckets: Arc::new(Mutex::new(BTreeMap::new())),
            interval_duration_ms,
        }
    }

    pub fn total_inodes(&self) -> u64 {
        self.buckets
            .lock()
            .values()
            .map(|bucket| bucket.len() as u64)
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

        if let Some(expiration_ms) = inode.expiration_ms() {
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

        if let Some(expiration_ms) = inode.expiration_ms() {
            let interval_start = self.get_bucket_interval_start(expiration_ms);
            if let Some(bucket) = self.buckets.lock().get(&interval_start) {
                let res = bucket.remove(inode.id());
                debug!("removed inode {} from sorted TTL bucket list", inode.id());
                res
            } else {
                false
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::{InodeFile, InodeView};
    use curvine_common::state::TtlAction;

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

    #[test]
    fn add_skips_when_ttl_disabled() {
        let list = TtlBucketList::new(INTERVAL_MS);
        list.add(&file_no_ttl(1, 0));
        assert_eq!(list.total_inodes(), 0);
        assert_eq!(list.buckets_len(), 0);
    }

    #[test]
    fn add_places_inode_in_quantized_bucket() {
        let list = TtlBucketList::new(INTERVAL_MS);
        // expiration = mtime + ttl_ms = 100 + 500 = 600 -> interval_start 0
        list.add(&file_with_ttl(42, 100, 500));
        assert_eq!(list.total_inodes(), 1);
        assert_eq!(list.buckets_len(), 1);
    }

    #[test]
    fn add_same_interval_merges_into_one_bucket() {
        let list = TtlBucketList::new(INTERVAL_MS);
        list.add(&file_with_ttl(1, 0, 400)); // exp 400 -> start 0
        list.add(&file_with_ttl(2, 50, 350)); // exp 400 -> start 0
        assert_eq!(list.total_inodes(), 2);
        assert_eq!(list.buckets_len(), 1);
    }

    #[test]
    fn add_different_intervals_use_multiple_buckets() {
        let list = TtlBucketList::new(INTERVAL_MS);
        list.add(&file_with_ttl(1, 0, 500)); // exp 500 -> start 0
        list.add(&file_with_ttl(2, 500, 1000)); // exp 1500 -> start 1000
        assert_eq!(list.total_inodes(), 2);
        assert_eq!(list.buckets_len(), 2);
    }

    #[test]
    fn remove_drops_inode_from_bucket() {
        let list = TtlBucketList::new(INTERVAL_MS);
        let f = file_with_ttl(7, 0, 100);
        list.add(&f);
        assert_eq!(list.total_inodes(), 1);
        assert!(list.remove(&f));
        assert_eq!(list.total_inodes(), 0);
    }

    #[test]
    fn remove_unknown_returns_false() {
        let list = TtlBucketList::new(INTERVAL_MS);
        let f = file_with_ttl(9, 0, 100);
        assert!(!list.remove(&f));
    }

    #[test]
    fn get_expired_buckets_at_drains_and_matches_time_rule() {
        let list = TtlBucketList::new(INTERVAL_MS);
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
        let list = TtlBucketList::new(INTERVAL_MS);
        // exp = 0 + 10_000 = 10_000 -> interval_start 10_000
        list.add(&file_with_ttl(1, 0, 10_000));

        let drained = list.get_expired_buckets_at(5000);
        assert!(drained.is_empty());
        assert_eq!(list.buckets_len(), 1);
        assert_eq!(list.total_inodes(), 1);
    }
}
