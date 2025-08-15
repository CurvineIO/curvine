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

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::ttl::ttl_bucket::TtlBucketList;
use crate::master::meta::inode::ttl::ttl_checker::InodeTtlChecker;
use crate::master::meta::inode::ttl::ttl_executor::InodeTtlExecutor;
use crate::master::meta::inode::ttl::ttl_types::{TtlCleanupConfig, TtlCleanupResult, TtlResult};
use log::{debug, error};
use std::sync::Arc;
use std::time::Instant;

// TTL Manager Module
//
// This module provides the high-level management interface for TTL operations.
// It coordinates between different TTL components and provides a unified API.
//
// Key Features:
// - Complete TTL system orchestration
// - Configuration management from MasterConf
// - Integration with filesystem and journal systems
// - Direct TTL cleanup operations

pub struct InodeTtlManager {
    checker: Arc<InodeTtlChecker>,
}

impl InodeTtlManager {
    pub fn new(filesystem: MasterFilesystem, bucket_list: Arc<TtlBucketList>) -> TtlResult<Self> {
        let ttl_executor = InodeTtlExecutor::new(filesystem.clone());

        // Create cleanup configuration directly from master config
        let cleanup_config = TtlCleanupConfig {
            check_interval_ms: filesystem.conf.ttl_checker_interval_ms(),
            bucket_interval_ms: filesystem.conf.ttl_bucket_interval_ms(),
            max_retry_count: filesystem.conf.ttl_checker_retry_attempts,
            max_retry_duration_ms: filesystem.conf.ttl_max_retry_duration_ms(),
            retry_interval_ms: filesystem.conf.ttl_retry_interval_ms(),
        };

        let checker = Arc::new(InodeTtlChecker::new(
            ttl_executor,
            cleanup_config,
            bucket_list,
        )?);

        let manager = Self { checker };
        Ok(manager)
    }

    pub fn cleanup(&self) -> TtlResult<TtlCleanupResult> {
        let start_time = Instant::now();

        match self.checker.cleanup_once() {
            Ok(result) => {
                let duration = start_time.elapsed();

                debug!(
                    "Inode ttl cleanup completed: processed {} items in {}ms",
                    result.total_processed,
                    duration.as_millis()
                );

                Ok(result)
            }
            Err(e) => {
                let duration = start_time.elapsed();
                error!(
                    "Inode ttl cleanup failed after {}ms: {}",
                    duration.as_millis(),
                    e
                );
                Err(e)
            }
        }
    }
}
