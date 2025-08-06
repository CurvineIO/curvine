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

use crate::master::meta::inode::ttl::ttl_types::{
    TtlAction, TtlConfig, TtlError, TtlInodeMetadata, TtlResult,
};
use dashmap::DashMap;
use log::{debug, info, warn};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

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
    /// Map of inode ID to retry count for thread-safe access
    pub inode_retries: Arc<DashMap<u64, u32>>,
}
impl TtlBucket {
    pub fn new(interval_start_ms: u64, interval_duration_ms: u64) -> Self {
        Self {
            interval_start_ms,
            interval_duration_ms,
            inode_retries: Arc::new(DashMap::new()),
        }
    }

    pub fn get_interval_end_ms(&self) -> u64 {
        self.interval_start_ms + self.interval_duration_ms
    }

    pub fn add_inode(&self, inode_id: u64, retry_count: u32) {
        self.inode_retries.insert(inode_id, retry_count);
    }

    pub fn remove_inode(&self, inode_id: u64) -> Option<u32> {
        self.inode_retries
            .remove(&inode_id)
            .map(|(_, retry_count)| retry_count)
    }

    pub fn is_expired_at(&self, current_time_ms: u64) -> bool {
        self.get_interval_end_ms() <= current_time_ms
    }

    pub fn get_all_inodes(&self) -> Vec<TtlInodeMetadata> {
        self.inode_retries
            .iter()
            .map(|entry| TtlInodeMetadata {
                inode_id: *entry.key(),
                retry_count: *entry.value(),
                ttl_config: TtlConfig::new(0, TtlAction::Delete),
                last_retry_time_ms: None,
            })
            .collect()
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

    fn get_bucket_interval_start(&self, timestamp_ms: u64) -> u64 {
        (timestamp_ms / self.interval_duration_ms) * self.interval_duration_ms
    }

    fn get_or_create_bucket(&self, timestamp_ms: u64) -> TtlResult<Arc<TtlBucket>> {
        let interval_start = self.get_bucket_interval_start(timestamp_ms);

        {
            let buckets = self
                .buckets
                .read()
                .map_err(|_| TtlError::ServiceError("Failed to acquire read lock".to_string()))?;
            if let Some(bucket) = buckets.get(&interval_start) {
                return Ok(bucket.clone());
            }
        }

        let mut buckets = self
            .buckets
            .write()
            .map_err(|_| TtlError::ServiceError("Failed to acquire write lock".to_string()))?;
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

    pub fn add_inode(&self, metadata: &TtlInodeMetadata) -> TtlResult<()> {
        let expiration_ms = metadata.ttl_config.expiration_time_ms();
        let interval_start = self.get_bucket_interval_start(expiration_ms);
        let bucket = self.get_or_create_bucket(expiration_ms)?;

        bucket.add_inode(metadata.inode_id, metadata.retry_count);
        self.inode_to_bucket_index
            .insert(metadata.inode_id, interval_start);
        self.total_inodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(
            "Added inode {} to sorted TTL bucket list, expires at {}ms",
            metadata.inode_id, expiration_ms
        );
        Ok(())
    }

    pub fn remove_inode(&self, inode_id: u64) -> TtlResult<Option<u32>> {
        let interval_start = match self.inode_to_bucket_index.get(&inode_id) {
            Some(entry) => *entry.value(),
            None => return Ok(None), // Inode not found
        };

        let buckets = self
            .buckets
            .read()
            .map_err(|_| TtlError::ServiceError("Failed to acquire read lock".to_string()))?;

        if let Some(bucket) = buckets.get(&interval_start) {
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

    pub fn get_expired_buckets_at(&self, current_time_ms: u64) -> Vec<Arc<TtlBucket>> {
        let buckets = match self.buckets.read() {
            Ok(buckets) => buckets,
            Err(_) => {
                warn!("Failed to acquire read lock for expired buckets lookup");
                return Vec::new();
            }
        };

        let max_expired_start = current_time_ms.saturating_sub(self.interval_duration_ms);

        let expired_buckets: Vec<Arc<TtlBucket>> = buckets
            .range(..=max_expired_start) // Only check potentially expired buckets
            .filter(|(_, bucket)| bucket.is_expired_at(current_time_ms))
            .map(|(_, bucket)| bucket.clone())
            .collect();

        if !expired_buckets.is_empty() {
            debug!(
                "Found {} expired TTL buckets at time {}ms",
                expired_buckets.len(),
                current_time_ms
            );
        }

        expired_buckets
    }
}
