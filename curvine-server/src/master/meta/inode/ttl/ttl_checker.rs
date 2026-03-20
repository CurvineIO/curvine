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
use crate::master::meta::inode::ttl::InodeTtlExecutor;
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::LocalTime;
use std::sync::Arc;

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
}

impl InodeTtlChecker {
    /// Create checker with external bucket list (for integration with InodeStore)
    pub fn new(
        action_executor: InodeTtlExecutor,
        bucket_list: Arc<TtlBucketList>,
    ) -> FsResult<Self> {
        Ok(Self {
            action_executor,
            bucket_list,
        })
    }

    pub fn cleanup_once(&self) -> FsResult<i64> {
        let start_time = LocalTime::mills() as i64;
        let expired_buckets = self.bucket_list.get_expired_buckets_at(start_time);

        if expired_buckets.is_empty() {
            debug!("no expired buckets found");
            return Ok(0);
        }

        info!("found {} expired buckets to process", expired_buckets.len());

        let mut total_processed = 0i64;
        for bucket in expired_buckets {
            self.process_expired_bucket(&bucket)?;
            total_processed += bucket.len() as i64;
        }

        Ok(total_processed)
    }

    fn process_expired_bucket(&self, bucket: &TtlBucket) -> FsResult<()> {
        let inode_ids: Vec<i64> = bucket.inodes.lock().iter().copied().collect();
        for inode_id in inode_ids {
            match self.action_executor.execute_by_id(inode_id) {
                Err(e) => {
                    warn!(
                        "error processing inode {}: {} (dropped from TTL scan until re-indexed)",
                        inode_id, e
                    );
                }
                Ok((false, inode)) => {
                    self.bucket_list.add(&inode);
                }

                _ => (),
            }
        }

        Ok(())
    }
}
