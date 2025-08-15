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

use crate::master::meta::inode::ttl::ttl_types::TtlResult;
use dashmap::DashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// TTL Bucket Management Module
//
// This module provides time-based bucket management for TTL (Time-To-Live) operations.
// It organizes inodes into time intervals for efficient batch processing of expired items.
//
// Key Features:
// - Time-based bucket organization with configurable intervals
// - Efficient O(log n) bucket lookup using BTreeMap for sorted storage
// - Thread-safe operations with concurrent access support
// - Automatic cleanup of empty expired buckets
// - Fast inode-to-bucket mapping for quick removal operations
// - Statistics collection for monitoring and debugging

#[derive(Debug)]
pub struct TtlBucket {
    /// Start time of the interval (aligned to bucket boundaries) in milliseconds since epoch
    pub interval_start_ms: u64,
    /// Interval duration in milliseconds
    pub interval_duration_ms: u64,
    /// Map of inode ID to retry count (memory efficient)
    pub inode_retries: RwLock<HashMap<u64, u32>>,
}
impl TtlBucket {
    pub fn new(interval_start_ms: u64, interval_duration_ms: u64) -> Self {
        Self {
            interval_start_ms,
            interval_duration_ms,
            inode_retries: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_interval_end_ms(&self) -> u64 {
        self.interval_start_ms + self.interval_duration_ms
    }

    pub fn add_inode(&self, inode_id: u64, retry_count: u32) {
        self.inode_retries.write().insert(inode_id, retry_count);
    }

    pub fn remove_inode(&self, inode_id: u64) -> Option<u32> {
        self.inode_retries.write().remove(&inode_id)
    }

    pub fn is_expired_at(&self, current_time_ms: u64) -> bool {
        self.get_interval_end_ms() <= current_time_ms
    }

    pub fn len(&self) -> usize {
        self.inode_retries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn for_each_inode<F: FnMut(u64, u32)>(&self, mut f: F) {
        // Take a lightweight snapshot of (inode_id, retry_count) pairs, then release the lock
        let snapshot: Vec<(u64, u32)> = self
            .inode_retries
            .read()
            .iter()
            .map(|(inode_id, retry_count)| (*inode_id, *retry_count))
            .collect();
        // Process without holding the read lock to avoid lock upgrade issues
        for (inode_id, retry_count) in snapshot {
            f(inode_id, retry_count);
        }
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

#[derive(Debug)]
pub struct TtlBucketList {
    /// Sorted bucket mapping: interval_start_ms -> TtlBucket (sorted by time)
    buckets: Arc<RwLock<BTreeMap<u64, Arc<TtlBucket>>>>,
    /// Hash index for fast access (inode_id -> interval_start_ms)
    inode_to_bucket_index: Arc<DashMap<u64, u64>>,
    /// Interval duration for each bucket in milliseconds
    interval_duration_ms: u64,
    /// Total number of inodes across all buckets
    total_inodes: Arc<std::sync::atomic::AtomicU64>,
}
impl TtlBucketList {
    pub fn new(interval_duration_ms: u64) -> Self {
        info!(
            "Creating TTL bucket list with sorted storage, interval duration: {}ms",
            interval_duration_ms
        );
        Self {
            buckets: Arc::new(RwLock::new(BTreeMap::new())),
            inode_to_bucket_index: Arc::new(DashMap::new()),
            interval_duration_ms,
            total_inodes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn total_inodes(&self) -> u64 {
        self.total_inodes.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn buckets_len(&self) -> usize {
        self.buckets.read().len()
    }

    fn get_bucket_interval_start(&self, timestamp_ms: u64) -> u64 {
        (timestamp_ms / self.interval_duration_ms) * self.interval_duration_ms
    }

    fn get_or_create_bucket(&self, timestamp_ms: u64) -> TtlResult<Arc<TtlBucket>> {
        let interval_start = self.get_bucket_interval_start(timestamp_ms);

        if let Some(bucket) = self.buckets.read().get(&interval_start) {
            return Ok(bucket.clone());
        }

        let mut buckets = self.buckets.write();
        if let Some(bucket) = buckets.get(&interval_start) {
            return Ok(bucket.clone());
        }

        debug!(
            "Creating new TTL bucket for interval starting at {}ms",
            interval_start
        );
        let new_bucket = Arc::new(TtlBucket::new(interval_start, self.interval_duration_ms));
        buckets.insert(interval_start, new_bucket.clone());

        Ok(new_bucket)
    }

    pub fn add_inode(&self, inode_id: u64, expiration_ms: u64) -> TtlResult<()> {
        let interval_start = self.get_bucket_interval_start(expiration_ms);
        let bucket = self.get_or_create_bucket(expiration_ms)?;

        bucket.add_inode(inode_id, 0);
        self.inode_to_bucket_index.insert(inode_id, interval_start);
        self.total_inodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(
            "Added inode {} to sorted TTL bucket list, expires at {}ms",
            inode_id, expiration_ms
        );
        Ok(())
    }

    pub fn remove_inode(&self, inode_id: u64) -> TtlResult<Option<u32>> {
        let interval_start = match self.inode_to_bucket_index.get(&inode_id) {
            Some(entry) => *entry.value(),
            None => return Ok(None), // Inode not found
        };

        if let Some(bucket) = self.buckets.read().get(&interval_start) {
            let result = bucket.remove_inode(inode_id);
            if result.is_some() {
                // Remove from index
                self.inode_to_bucket_index.remove(&inode_id);

                // Update total count
                self.total_inodes
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

                debug!("Removed inode {} from sorted TTL bucket list", inode_id);
            }
            Ok(result)
        } else {
            Ok(None)
        }
    }

    /// Reschedule an inode into a future bucket at the provided timestamp (ms).
    /// This keeps total_inodes unchanged.
    pub fn reschedule_inode(
        &self,
        inode_id: u64,
        retry_count: u32,
        next_timestamp_ms: u64,
    ) -> TtlResult<()> {
        // Find current bucket via index (if any) and remove it
        if let Some(cur) = self.inode_to_bucket_index.get(&inode_id) {
            let cur_start = *cur.value();
            if let Some(bucket) = self.buckets.read().get(&cur_start) {
                let _ = bucket.remove_inode(inode_id);
            }
        }

        // Insert into new bucket and update index; total_inodes unchanged
        let new_bucket = self.get_or_create_bucket(next_timestamp_ms)?;
        let new_start = self.get_bucket_interval_start(next_timestamp_ms);
        new_bucket.add_inode(inode_id, retry_count);
        self.inode_to_bucket_index.insert(inode_id, new_start);
        Ok(())
    }

    pub fn get_expired_buckets_at(&self, current_time_ms: u64) -> Vec<Arc<TtlBucket>> {
        // Any bucket with start <= current - interval_duration has end <= current ⇒ expired
        let max_expired_start = current_time_ms.saturating_sub(self.interval_duration_ms);

        self.buckets
            .read()
            .range(..=max_expired_start)
            .map(|(_, bucket)| bucket.clone())
            .collect()
    }

    /// Remove an empty bucket from the bucket list
    pub fn remove_empty_bucket(&self, bucket: &TtlBucket) -> TtlResult<bool> {
        // Double-check that the bucket is actually empty before attempting removal
        if !bucket.is_empty() {
            warn!(
                "Attempted to remove non-empty bucket with interval_start_ms={}, {} inodes remaining",
                bucket.interval_start_ms,
                bucket.len()
            );
            return Ok(false);
        }

        // Remove the bucket directly using its interval_start_ms
        let removed = self.buckets.write().remove(&bucket.interval_start_ms);
        if removed.is_some() {
            debug!(
                "Removed empty TTL bucket with interval_start_ms={}",
                bucket.interval_start_ms
            );
            Ok(true)
        } else {
            debug!(
                "Bucket with interval_start_ms={} not found for removal",
                bucket.interval_start_ms
            );
            Ok(false)
        }
    }
}
