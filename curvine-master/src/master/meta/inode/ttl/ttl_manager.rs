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
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::ttl::TtlResult;
use crate::master::meta::inode::ttl::{InodeTtlChecker, InodeTtlExecutor};
use curvine_common::FsResult;
use log::{error, info};
use orpc::common::TimeSpent;
use std::sync::Arc;

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
        let ttl_executor = InodeTtlExecutor::with_managers(filesystem.clone());

        let checker = Arc::new(InodeTtlChecker::new(ttl_executor, bucket_list)?);

        let manager = Self { checker };
        Ok(manager)
    }

    pub fn cleanup(&self) -> FsResult<i64> {
        let spend = TimeSpent::new();

        match self.checker.cleanup_once() {
            Ok(total_processed) => {
                info!(
                    "inode ttl cleanup completed: processed {} items in {} ms",
                    total_processed,
                    spend.used_ms()
                );

                Ok(total_processed)
            }
            Err(e) => {
                error!(
                    "inode ttl cleanup failed after {}ms: {}",
                    spend.used_ms(),
                    e
                );
                Err(e)
            }
        }
    }
}
