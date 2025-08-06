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

use crate::master::meta::inode::ttl::ttl_checker::InodeTtlChecker;
use crate::master::meta::inode::ttl_types::{
    TtlCleanupResult, TtlError, TtlInodeMetadata, TtlResult,
};
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Instant;

// TTL Service Module
//
// This module provides the high-level TTL cleanup service that orchestrates
// TTL operations across the system. It acts as a service layer between
// the scheduler and the core TTL components.
//
// Key Features:
// - Passive cleanup service design (triggered by scheduler)
// - Service lifecycle management (initialize/stop)
// - Configuration management for TTL operations
// - Integration with TTL checker for actual cleanup
// - Service status monitoring and control
// - Inode TTL tracking management
// - Error handling and logging

pub struct InodeTtlCleanupService {
    checker: Arc<InodeTtlChecker>,
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

impl InodeTtlCleanupService {
    pub fn new(checker: Arc<InodeTtlChecker>) -> Self {
        Self {
            checker,
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn initialize(&self) -> TtlResult<()> {
        if self.is_running.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(()); // Already initialized
        }
        info!("Initializing TTL cleanup service (passive mode)");
        self.is_running
            .store(true, std::sync::atomic::Ordering::Relaxed);

        info!("Inode ttl cleanup service initialized successfully");
        Ok(())
    }

    pub fn cleanup_once(&self) -> TtlResult<TtlCleanupResult> {
        if !self.is_running.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(TtlError::ServiceError(
                "TTL cleanup service is not initialized".to_string(),
            ));
        }

        let start_time = Instant::now();

        match self.checker.cleanup_once() {
            Ok(result) => {
                let duration = start_time.elapsed();

                debug!(
                    "TTL cleanup completed: processed {} items in {}ms",
                    result.total_processed,
                    duration.as_millis()
                );

                Ok(result)
            }
            Err(e) => {
                let duration = start_time.elapsed();
                error!("TTL cleanup failed after {}ms: {}", duration.as_millis(), e);
                Err(e)
            }
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn add_inode(&self, metadata: TtlInodeMetadata) -> TtlResult<()> {
        if !self.is_running() {
            return Err(TtlError::ServiceError(
                "TTL cleanup service is not running".to_string(),
            ));
        }
        self.checker.create_inode(metadata)
    }

    pub fn remove_inode(&self, inode_id: u64) -> TtlResult<bool> {
        if !self.is_running() {
            return Err(TtlError::ServiceError(
                "TTL cleanup service is not running".to_string(),
            ));
        }
        self.checker.remove_inode(inode_id)
    }
}
