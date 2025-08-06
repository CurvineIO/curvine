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

use crate::master::meta::inode::ttl::ttl_bucket::{TtlBucket, TtlBucketList};
use crate::master::meta::inode::ttl::ttl_executor::InodeTtlExecutor;
use crate::master::meta::inode::ttl::ttl_types::{
    TtlAction, TtlCleanupConfig, TtlCleanupResult, TtlError, TtlInodeMetadata, TtlResult,
};
use crate::master::meta::inode::InodeView;
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
// - Configurable retry logic with timeout and attempt limits
// - Support for different TTL actions (Delete, Move, Free)
// - Integration with inode storage and execution systems
// - Comprehensive cleanup result tracking and statistics

pub struct InodeTtlChecker {
    bucket_list: Arc<TtlBucketList>,
    action_executor: InodeTtlExecutor,
    config: TtlCleanupConfig,
}

impl InodeTtlChecker {
    pub fn new(action_executor: InodeTtlExecutor, config: TtlCleanupConfig) -> TtlResult<Self> {
        config.validate()?;
        let bucket_list = Arc::new(TtlBucketList::new(config.bucket_interval_ms));
        info!("Created inode ttl checker with config: {:?}", config);
        Ok(Self {
            bucket_list,
            action_executor,
            config,
        })
    }

    pub fn create_inode(&self, metadata: TtlInodeMetadata) -> TtlResult<()> {
        self.bucket_list.add_inode(&metadata).map_err(|e| {
            TtlError::ServiceError(format!(
                "Failed to add inode {} to ttl tracking: {}",
                metadata.inode_id, e
            ))
        })?;
        debug!("Added inode {} to ttl tracking", metadata.inode_id);
        Ok(())
    }

    pub fn remove_inode(&self, inode_id: u64) -> TtlResult<bool> {
        self.bucket_list.remove_inode(inode_id).map_err(|e| {
            TtlError::ServiceError(format!(
                "Failed to remove inode {} from ttl tracking: {}",
                inode_id, e
            ))
        })?;
        debug!("Removed inode {} from ttl tracking", inode_id);
        Ok(true)
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

        let expired_inodes = bucket.get_all_inodes();

        for metadata in expired_inodes {
            let inode_id = metadata.inode_id;
            if self.should_stop_retry(&metadata, current_time_ms) {
                warn!(
                    "Stopping retry for inode {} due to timeout or max attempts",
                    inode_id
                );
                result.failed_inodes.push(inode_id);
                result.failed_cleanups += 1;
                result.total_processed += 1;
                bucket.remove_inode(inode_id);
                continue;
            }

            let action_result = self.execute_ttl_action(inode_id);
            result.merge(action_result);

            if result.successful_deletes.contains(&inode_id)
                || result.successful_frees.contains(&inode_id)
            {
                bucket.remove_inode(inode_id);
            } else if result.failed_inodes.contains(&inode_id) {
                let updated_metadata =
                    self.increment_retry_with_timeout_check(metadata, current_time_ms)?;

                if self.should_stop_retry(&updated_metadata, current_time_ms) {
                    warn!("Max retries reached for inode {}, giving up", inode_id);
                    bucket.remove_inode(inode_id);
                } else {
                    bucket.add_inode(inode_id, updated_metadata.retry_count);
                    result.retry_inodes.push(inode_id);
                    info!(
                        "Scheduled retry for inode {} (attempt {}/{})",
                        inode_id, updated_metadata.retry_count, self.config.max_retry_count
                    );
                }
            }
        }

        Ok(result)
    }

    fn should_stop_retry(&self, metadata: &TtlInodeMetadata, current_time_ms: u64) -> bool {
        if metadata.retry_count >= self.config.max_retry_count {
            debug!(
                "Retry count limit reached: {}/{}",
                metadata.retry_count, self.config.max_retry_count
            );
            return true;
        }

        if let Some(first_retry_time) = metadata.last_retry_time_ms {
            let retry_duration = current_time_ms.saturating_sub(first_retry_time);

            if retry_duration > self.config.max_retry_duration_ms {
                warn!(
                    "Global retry timeout reached for inode {}: duration={}ms, limit={}ms",
                    metadata.inode_id, retry_duration, self.config.max_retry_duration_ms
                );
                return true;
            }
        }

        false
    }

    fn increment_retry_with_timeout_check(
        &self,
        mut metadata: TtlInodeMetadata,
        current_time_ms: u64,
    ) -> TtlResult<TtlInodeMetadata> {
        metadata.retry_count += 1;

        if metadata.last_retry_time_ms.is_none() {
            metadata.last_retry_time_ms = Some(current_time_ms);
        }

        debug!(
            "Incremented retry count for inode {}: {}/{}",
            metadata.inode_id, metadata.retry_count, self.config.max_retry_count
        );

        Ok(metadata)
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
            InodeView::File(file) => &file.storage_policy,
            InodeView::Dir(dir) => &dir.storage_policy,
        };

        let action = &storage_policy.ttl_action;

        debug!(
            "Executing ttl action {:?} for inode {} based on StoragePolicy",
            action, inode_id
        );

        match action {
            TtlAction::Delete => match self.action_executor.delete_inode(inode_id) {
                Ok(()) => {
                    info!("Successfully executed DELETE action for inode {}", inode_id);
                    result.successful_deletes.push(inode_id);
                    result.successful_cleanups = 1;
                }
                Err(e) => {
                    error!("Delete failed for inode {}: {}", inode_id, e);
                    result.failed_inodes.push(inode_id);
                    result.failed_cleanups = 1;
                }
            },
            TtlAction::Move | TtlAction::Ufs => match self.action_executor.free_inode(inode_id) {
                Ok(()) => {
                    info!(
                        "Successfully executed MOVE/FREE action for inode {}",
                        inode_id
                    );
                    result.successful_frees.push(inode_id);
                    result.successful_cleanups = 1;
                }
                Err(e) => {
                    error!("Free failed for inode {}: {}", inode_id, e);
                    result.failed_inodes.push(inode_id);
                    result.failed_cleanups = 1;
                }
            },
            TtlAction::None => {
                warn!("No ttl action defined for inode {}, skipping", inode_id);
                result.skipped_inodes = 1;
            }
        }

        result
    }
}
