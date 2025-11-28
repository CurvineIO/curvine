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

use crate::master::meta::inode::ttl::ttl_bucket::TtlBucket;
use crate::master::meta::inode::ttl::ttl_bucket::TtlBucketList;
use crate::master::meta::inode::ttl::ttl_executor::InodeTtlExecutor;
use crate::master::meta::inode::ttl::ttl_types::{
    TtlAction, TtlCleanupConfig, TtlCleanupResult, TtlResult,
};
use crate::master::meta::inode::InodeView;
use curvine_common::state::StoragePolicy;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// TTL Checker Module
//
// This module provides the core TTL (Time-To-Live) checking and cleanup functionality.
// It processes expired inodes using a bucket-based approach for efficient batch operations.
//
// Key Features:
// - Bucket-based expiration processing for efficient batch operations
// - Configurable retry logic with attempt limits
// - Support for different TTL actions (Delete, Move, Free)
// - Integration with inode storage and execution systems
// - Comprehensive cleanup result tracking and statistics

pub struct InodeTtlChecker {
    bucket_list: Arc<TtlBucketList>,
    action_executor: InodeTtlExecutor,
    config: TtlCleanupConfig,
}

impl InodeTtlChecker {
    /// Create checker with external bucket list (for integration with InodeStore)
    pub fn new(
        action_executor: InodeTtlExecutor,
        config: TtlCleanupConfig,
        bucket_list: Arc<TtlBucketList>,
    ) -> TtlResult<Self> {
        config.validate()?;
        info!(
            "Created inode ttl checker with external bucket list, config: {:?}",
            config
        );
        Ok(Self {
            bucket_list,
            action_executor,
            config,
        })
    }

    pub fn cleanup_once(&self) -> TtlResult<TtlCleanupResult> {
        let start_time = SystemTime::now();
        let mut result = TtlCleanupResult::new();
        let current_time_ms = start_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let expired_buckets = self.bucket_list.get_expired_buckets_at(current_time_ms);

        if expired_buckets.is_empty() {
            debug!("No expired buckets found");
            return Ok(result);
        }

        info!("Found {} expired buckets to process", expired_buckets.len());

        for bucket in expired_buckets {
            let bucket_result = self.process_expired_bucket(&bucket, current_time_ms)?;
            result.merge(bucket_result);
        }

        let duration = start_time.elapsed().unwrap_or_default();
        debug!(
            "Inode ttl cleanup completed: {} processed in {}ms",
            result.total_processed(),
            duration.as_millis()
        );

        Ok(result)
    }

    fn process_expired_bucket(
        &self,
        bucket: &TtlBucket,
        current_time_ms: u64,
    ) -> TtlResult<TtlCleanupResult> {
        let mut result = TtlCleanupResult::new();

        // Iterate entries without long-held locks using for_each_inode snapshot
        bucket.for_each_inode(|inode_id, retry_count| {
            if self.should_stop_retry(retry_count) {
                warn!("Stopping retry for inode {} due to max attempts", inode_id);
                // Count as processed even if we stop retrying
                result.failed_inodes.push(inode_id);
                result.failed_cleanups += 1;
                result.total_processed += 1;
                let _ = bucket.remove_inode(inode_id);
                return;
            }

            let action_result = self.execute_ttl_action(inode_id);
            result.merge(action_result);

            if result.successful_deletes.contains(&inode_id)
                || result.successful_frees.contains(&inode_id)
            {
                let _ = bucket.remove_inode(inode_id);
            } else if result.failed_inodes.contains(&inode_id) {
                // Exponential backoff with jitter and reschedule into a future bucket
                let updated_retry = retry_count.saturating_add(1);

                if self.should_stop_retry(updated_retry) {
                    warn!("Max retries reached for inode {}, giving up", inode_id);
                    let _ = bucket.remove_inode(inode_id);
                } else {
                    let next_ts =
                        self.compute_next_retry_ms(current_time_ms, inode_id, updated_retry);
                    let _ = self
                        .bucket_list
                        .reschedule_inode(inode_id, updated_retry, next_ts);
                    result.retry_inodes.push(inode_id);
                    info!(
                        "Scheduled retry for inode {} (attempt {}/{}) at {}",
                        inode_id, updated_retry, self.config.max_retry_count, next_ts
                    );
                }
            }
        });

        // After processing all inodes, check if bucket is now empty and remove it
        if bucket.is_empty() {
            let _ = self.bucket_list.remove_empty_bucket(bucket);
        } else {
            debug!(
                "Bucket processing completed, {} inodes remaining for retry",
                bucket.len()
            );
        }

        Ok(result)
    }

    fn should_stop_retry(&self, retry_count: u32) -> bool {
        if retry_count >= self.config.max_retry_count {
            debug!(
                "Retry count limit reached: {}/{}",
                retry_count, self.config.max_retry_count
            );
            return true;
        }
        false
    }

    fn execute_ttl_action(&self, inode_id: u64) -> TtlCleanupResult {
        let mut result = TtlCleanupResult::new();
        result.total_processed = 1;

        let inode_view = match self.action_executor.get_inode_from_store(inode_id) {
            Ok(Some(view)) => view,
            Ok(None) => {
                error!("Inode {} not found in store", inode_id);
                result.failed_inodes.push(inode_id);
                result.failed_cleanups = 1;
                return result;
            }
            Err(e) => {
                error!("Failed to get inode {} from store: {}", inode_id, e);
                result.failed_inodes.push(inode_id);
                result.failed_cleanups = 1;
                return result;
            }
        };

        let storage_policy = match &inode_view {
            InodeView::File(_, file) => &file.storage_policy,
            InodeView::Dir(_, dir) => &dir.storage_policy,
            InodeView::FileEntry(..) => &StoragePolicy::default(),
        };

        let action = &storage_policy.ttl_action;

        debug!(
            "Executing ttl action {:?} for inode {} based on StoragePolicy",
            action, inode_id
        );

        match action {
            TtlAction::Delete => match self.action_executor.delete_inode(inode_id) {
                Ok(()) => {
                    debug!("Successfully executed DELETE action for inode {}", inode_id);
                    result.successful_deletes.push(inode_id);
                    result.successful_cleanups = 1;
                }
                Err(e) => {
                    error!("Delete failed for inode {}: {}", inode_id, e);
                    result.failed_inodes.push(inode_id);
                    result.failed_cleanups = 1;
                }
            },
            TtlAction::Persist | TtlAction::Evict | TtlAction::Flush => {
                match self.action_executor.free_inode(inode_id) {
                    Ok(()) => {
                        info!(
                            "Successfully executed {:?} action for inode {}",
                            action, inode_id
                        );
                        result.successful_frees.push(inode_id);
                        result.successful_cleanups = 1;
                    }
                    Err(e) => {
                        error!("{:?} action failed for inode {}: {}", action, inode_id, e);
                        result.failed_inodes.push(inode_id);
                        result.failed_cleanups = 1;
                    }
                }
            }
            TtlAction::None => {
                warn!("No ttl action defined for inode {}, skipping", inode_id);
                result.skipped_inodes = 1;
            }
        }

        result
    }

    fn compute_next_retry_ms(&self, current_time_ms: u64, inode_id: u64, retry_count: u32) -> u64 {
        let base = self.config.retry_interval_ms.max(1);
        let pow = retry_count.min(16); // cap to avoid overflow
        let mut backoff = base.saturating_mul(1u64 << pow);
        backoff = backoff.min(self.config.max_retry_duration_ms);

        let basis = inode_id.rotate_left(pow) ^ current_time_ms;
        let jitter_pct: i64 = (basis % 21) as i64 - 10; // [-10, +10]
        let jitter_ms = ((backoff as i128 * jitter_pct as i128) / 100) as i64;
        let adjusted = (backoff as i64 + jitter_ms).max(0) as u64;
        current_time_ms.saturating_add(adjusted)
    }
}
