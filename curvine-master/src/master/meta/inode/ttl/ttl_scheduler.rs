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

use crate::master::meta::inode::ttl::ttl_manager::InodeTtlManager;
use curvine_common::error::FsError;
use log::{error, info};
use orpc::common::TimeSpent;
use orpc::runtime::LoopTask;
use orpc::sync::AtomicCounter;
use std::sync::Arc;
// TTL Scheduler Module
//
// This module provides the scheduling infrastructure for TTL operations.
// It implements a heartbeat-based scheduler that periodically triggers TTL cleanup.
//
// Key Features:
// - Heartbeat-based TTL cleanup scheduling
// - Integration with LoopTask framework
// - Configurable execution intervals and timeouts
// - Execution monitoring and statistics
// - Error handling and recovery
// - Task lifecycle management

#[derive(Clone)]
pub struct TtlHeartbeatChecker {
    ttl_manager: Arc<InodeTtlManager>,
    config: TtlHeartbeatConfig,
    execution_count: Arc<AtomicCounter>,
}

impl TtlHeartbeatChecker {
    pub fn new(ttl_manager: Arc<InodeTtlManager>, config: TtlHeartbeatConfig) -> Self {
        Self {
            ttl_manager,
            config,
            execution_count: Arc::new(AtomicCounter::new(0)),
        }
    }
    pub fn get_task_name(&self) -> &str {
        &self.config.task_name
    }
    pub fn get_timeout_ms(&self) -> u64 {
        self.config.timeout_ms
    }
}

#[derive(Debug, Clone)]
pub struct TtlHeartbeatConfig {
    pub task_name: String,
    pub timeout_ms: u64,
}

impl Default for TtlHeartbeatConfig {
    fn default() -> Self {
        Self {
            task_name: "ttl-heartbeat".to_string(),
            timeout_ms: 300_000, // 5 minutes timeout
        }
    }
}

impl LoopTask for TtlHeartbeatChecker {
    type Error = FsError;
    fn run(&self) -> Result<(), Self::Error> {
        let spend = TimeSpent::new();
        let execution_id = self.execution_count.add_and_get(1);

        info!(
            "Starting inode ttl clean execution #{} for task '{}'",
            execution_id,
            self.get_task_name()
        );

        let result = self.ttl_manager.cleanup();
        let used_ms = spend.used_ms();
        if used_ms > self.get_timeout_ms() {
            error!(
                "Inode ttl execution #{} for task '{}' exceeded timeout of {}ms (actual: {}ms)",
                execution_id,
                self.get_task_name(),
                self.get_timeout_ms(),
                used_ms
            );
        }
        if let Err(e) = result {
            error!("TTL heartbeat execution failed: execution_id={}, task_name={}, duration={}ms, error={}",
                execution_id, self.get_task_name(), used_ms, e);
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        // TTL manager is always ready after creation, so we never terminate
        // unless the entire system is shutting down
        false
    }
}
