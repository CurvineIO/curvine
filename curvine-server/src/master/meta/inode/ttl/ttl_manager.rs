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
use crate::master::meta::inode::ttl::ttl_checker::InodeTtlChecker;
use crate::master::meta::inode::ttl::ttl_executor::InodeTtlExecutor;
use crate::master::meta::inode::ttl::ttl_service::InodeTtlCleanupService;
use crate::master::meta::inode::ttl::ttl_types::{
    TtlCleanupConfig, TtlCleanupResult, TtlError, TtlInodeMetadata, TtlResult,
};
use crate::master::meta::inode::ttl_types::TtlConfig;
use crate::master::meta::inode::InodeView;
use curvine_common::conf::MasterConf;
use curvine_common::FsResult;
use log::info;
use orpc::err_box;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// TTL Manager Module
//
// This module provides the high-level management interface for TTL operations.
// It coordinates between different TTL components and provides a unified API.
//
// Key Features:
// - Complete TTL system orchestration
// - Configuration management from MasterConf
// - Lifecycle management (initialization, shutdown)
// - Integration with filesystem and journal systems
// - Unified API for TTL operations (add, remove, cleanup)
// - Service status monitoring and control

pub struct InodeTtlManager {
    service: Arc<InodeTtlCleanupService>,
    is_initialized: Arc<AtomicBool>,
}
#[derive(Debug, Clone, Default)]
pub struct TtlManagerConfig {
    pub cleanup_config: TtlCleanupConfig,
}
impl InodeTtlManager {
    pub fn create(filesystem: MasterFilesystem, master_conf: Arc<MasterConf>) -> TtlResult<Self> {
        info!("Creating complete TTL system for filesystem");

        let ttl_executor = InodeTtlExecutor::new(filesystem.clone());
        let ttl_manager_config = Self::create_config_from_master_conf(&master_conf);

        let checker = Arc::new(InodeTtlChecker::new(
            ttl_executor,
            ttl_manager_config.cleanup_config.clone(),
        )?);

        let service = Arc::new(InodeTtlCleanupService::new(checker));

        let is_initialized = Arc::new(AtomicBool::new(false));

        let manager = Self {
            service,
            is_initialized,
        };

        info!("InodeTtlManager created successfully");
        Ok(manager)
    }
    fn create_config_from_master_conf(master_conf: &MasterConf) -> TtlManagerConfig {
        let cleanup_config = TtlCleanupConfig {
            check_interval_ms: master_conf.ttl_checker_interval_ms(),
            bucket_interval_ms: master_conf.ttl_bucket_interval_ms(),
            max_retry_count: master_conf.ttl_checker_retry_attempts,
            max_retry_duration_ms: 1800000, // 30 minutes
            retry_interval_ms: 5000,        // 5 seconds
            cleanup_timeout_ms: 30000,      // 30 seconds
        };

        TtlManagerConfig { cleanup_config }
    }

    pub fn initialize(&self) -> TtlResult<()> {
        self.service.initialize()?;
        self.is_initialized
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn add_inode(&self, metadata: TtlInodeMetadata) -> TtlResult<()> {
        self.ensure_initialized()?;
        self.service.add_inode(metadata)
    }

    pub fn cleanup(&self) -> TtlResult<TtlCleanupResult> {
        self.ensure_initialized()?;
        let result = self.service.cleanup_once()?;
        Ok(result)
    }

    pub fn remove_inode(&self, inode_id: u64) -> TtlResult<bool> {
        self.ensure_initialized()?;
        self.service.remove_inode(inode_id)
    }

    pub fn is_initialized(&self) -> bool {
        self.is_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn ensure_initialized(&self) -> TtlResult<()> {
        if !self.is_initialized() {
            Err(TtlError::ServiceError(
                "TTL Manager not initialized".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn register_file_ttl(&self, inode: &InodeView) -> FsResult<()> {
        if let Some(ttl_config) = inode.ttl_config() {
            self.register_ttl_tracking(inode.id() as u64, ttl_config)?;
        }
        Ok(())
    }

    fn register_ttl_tracking(&self, inode_id: u64, ttl_config: TtlConfig) -> FsResult<()> {
        let metadata = TtlInodeMetadata {
            inode_id,
            ttl_config: ttl_config.clone(),
            retry_count: 0,
            last_retry_time_ms: None,
        };
        match self.add_inode(metadata) {
            Ok(_) => {
                info!(
                    "Register TTL: ID={}, TTL={}ms, Action={:?} for tracking successful.",
                    inode_id, ttl_config.ttl_ms, ttl_config.action
                );
            }
            Err(e) => {
                return err_box!("TTL registration failed: Inode {}, Error: {}", inode_id, e);
            }
        }
        Ok(())
    }

    pub(crate) fn remove_ttl_tracking(&self, inode_id: u64) -> FsResult<()> {
        match self.remove_inode(inode_id) {
            Ok(_) => {
                // TTL removal successful, no additional processing needed
            }
            Err(e) => {
                return err_box!("TTL removal failed: Inode {}, Error: {}", inode_id, e);
            }
        }
        Ok(())
    }
}
