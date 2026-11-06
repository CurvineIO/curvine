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

    pub fn clear(&self) {
        self.buckets.lock().clear();
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
            let interval_start = self.get_bucket_interval_start(expiration_ms);
            let mut buckets = self.buckets.lock();
            let bucket = buckets.entry(interval_start).or_insert_with(|| {
                Arc::new(TtlBucket::new(interval_start, self.interval_duration_ms))
            });
            bucket.add(inode.id());
        }
        debug!(
            "added inode {} to sorted TTL bucket list, expires at {}ms",
            inode.id(),
            expiration_ms
        );
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
        let buckets: Vec<Arc<TtlBucket>> = self.buckets.lock().values().cloned().collect();
        for bucket in buckets {
            bucket.optimize();
        }
    }

    /// Reinsert a drained bucket after an interrupted cleanup pass (#1754).
    pub fn put_back(&self, bucket: &TtlBucket) {
        let ids: Vec<u64> = bucket.inodes.lock().clone().iter().collect();
        if ids.is_empty() {
            return;
        }
        let interval_start = self.get_bucket_interval_start(bucket.interval_start_ms);
        let mut buckets = self.buckets.lock();
        let target = buckets
            .entry(interval_start)
            .or_insert_with(|| Arc::new(TtlBucket::new(interval_start, self.interval_duration_ms)));
        target.inodes.lock().extend(ids);
    }
}

#[cfg(test)]
#[path = "tests/ttl_bucket_test.rs"]
mod tests;
