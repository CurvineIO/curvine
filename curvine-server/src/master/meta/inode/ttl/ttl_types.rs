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

pub use curvine_common::state::TtlAction;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

// TTL Types Module
//
// This module defines all the core data types and structures used throughout
// the TTL system. It provides the fundamental building blocks for TTL operations.
//
// Key Components:
// - TTL configuration and metadata structures
// - Error types and result handling
// - Cleanup configuration and validation
// - Result tracking and statistics
// - Type conversions and utilities
// - Serialization support for persistence

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlConfig {
    pub ttl_ms: u64,
    pub action: TtlAction,
    pub creation_time_ms: u64,
}

impl TtlConfig {
    pub fn new(ttl_ms: u64, action: TtlAction) -> Self {
        let creation_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            ttl_ms,
            action,
            creation_time_ms,
        }
    }

    pub fn from_storage_policy(
        storage_policy: &curvine_common::state::StoragePolicy,
    ) -> Option<Self> {
        if storage_policy.ttl_ms > 0 && storage_policy.ttl_action != TtlAction::None {
            Some(Self {
                ttl_ms: storage_policy.ttl_ms as u64,
                action: storage_policy.ttl_action,
                creation_time_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            })
        } else {
            None
        }
    }

    pub fn with_creation_time(ttl_ms: u64, action: TtlAction, creation_time_ms: u64) -> Self {
        Self {
            ttl_ms,
            action,
            creation_time_ms,
        }
    }

    pub fn expiry_time_ms(&self) -> u64 {
        self.creation_time_ms + self.ttl_ms
    }

    pub fn expiration_time_ms(&self) -> u64 {
        self.expiry_time_ms()
    }

    pub fn is_expired(&self) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now_ms >= self.expiry_time_ms()
    }

    pub fn is_expired_at(&self, time_ms: u64) -> bool {
        time_ms >= self.expiry_time_ms()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlInodeMetadata {
    pub inode_id: u64,
    pub ttl_config: TtlConfig,
    pub retry_count: u32,
    pub last_retry_time_ms: Option<u64>,
}

impl TtlInodeMetadata {
    pub fn new(inode_id: u64, ttl_config: TtlConfig) -> Self {
        Self {
            inode_id,
            ttl_config,
            retry_count: 0,
            last_retry_time_ms: None,
        }
    }
    pub fn has_ttl(&self) -> bool {
        self.ttl_config.action != TtlAction::None
    }

    pub fn is_expired(&self) -> bool {
        self.ttl_config.is_expired()
    }

    pub fn is_expired_at(&self, time_ms: u64) -> bool {
        self.ttl_config.is_expired_at(time_ms)
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.last_retry_time_ms = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
    }

    pub fn exceeds_max_retries(&self, max_retries: u32) -> bool {
        self.retry_count >= max_retries
    }

    pub fn expiry_time_ms(&self) -> u64 {
        self.ttl_config.expiry_time_ms()
    }
}

#[derive(Debug, Clone)]
pub struct TtlCleanupConfig {
    pub check_interval_ms: u64,
    pub max_retry_count: u32,
    pub max_retry_duration_ms: u64,
    pub retry_interval_ms: u64,
    pub bucket_interval_ms: u64,
    pub cleanup_timeout_ms: u64,
}

impl Default for TtlCleanupConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 60000, // 1 minute
            max_retry_count: 3,
            max_retry_duration_ms: 1800000, // 30 minutes
            retry_interval_ms: 5000,        // 5 seconds
            bucket_interval_ms: 3600000,    // 1 hour
            cleanup_timeout_ms: 30000,      // 30 seconds
        }
    }
}

impl TtlCleanupConfig {
    pub fn validate(&self) -> TtlResult<()> {
        if self.check_interval_ms == 0 {
            return Err(TtlError::ConfigError(
                "check_interval_ms must be greater than 0".to_string(),
            ));
        }

        if self.max_retry_count == 0 {
            return Err(TtlError::ConfigError(
                "max_retry_count must be greater than 0".to_string(),
            ));
        }

        if self.max_retry_duration_ms == 0 {
            return Err(TtlError::ConfigError(
                "max_retry_duration_ms must be greater than 0".to_string(),
            ));
        }

        if self.retry_interval_ms == 0 {
            return Err(TtlError::ConfigError(
                "retry_interval_ms must be greater than 0".to_string(),
            ));
        }

        if self.max_retry_duration_ms <= self.retry_interval_ms {
            return Err(TtlError::ConfigError(
                "max_retry_duration_ms must be greater than retry_interval_ms".to_string(),
            ));
        }

        if self.max_retry_duration_ms < 60000 {
            // 1 minute
            return Err(TtlError::ConfigError(
                "max_retry_duration_ms should be at least 1 minute".to_string(),
            ));
        }

        if self.max_retry_duration_ms > 86400000 {
            // 24 hours
            return Err(TtlError::ConfigError(
                "max_retry_duration_ms should not exceed 24 hours".to_string(),
            ));
        }

        if self.bucket_interval_ms == 0 {
            return Err(TtlError::ConfigError(
                "bucket_interval_ms must be greater than 0".to_string(),
            ));
        }

        if self.cleanup_timeout_ms == 0 {
            return Err(TtlError::ConfigError(
                "cleanup_timeout_ms must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TtlError {
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("IO error: {0}")]
    Io(String),
    #[error("Service error: {0}")]
    ServiceError(String),
    #[error("Persistence error: {0}")]
    PersistenceError(String),
    #[error("Action execution error: {0}")]
    ActionExecutionError(String),
    #[error("Filesystem error: {0}")]
    FsError(String),
    #[error("Config error: {0}")]
    ConfigError(String),
}
impl From<std::io::Error> for TtlError {
    fn from(err: std::io::Error) -> Self {
        TtlError::Io(err.to_string())
    }
}
impl From<curvine_common::error::FsError> for TtlError {
    fn from(err: curvine_common::error::FsError) -> Self {
        TtlError::FsError(err.to_string())
    }
}
pub type TtlResult<T> = Result<T, TtlError>;
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TtlCleanupResult {
    pub total_processed: u64,
    pub successful_cleanups: u64,
    pub failed_cleanups: u64,
    pub skipped_inodes: u64,
    pub processed_buckets: u64,
    pub successful_deletes: Vec<u64>,
    pub successful_frees: Vec<u64>,
    pub failed_inodes: Vec<u64>,
    pub retry_inodes: Vec<u64>,
    pub successful_operations: (),
}

impl TtlCleanupResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, other: TtlCleanupResult) {
        self.total_processed += other.total_processed;
        self.successful_cleanups += other.successful_cleanups;
        self.failed_cleanups += other.failed_cleanups;
        self.skipped_inodes += other.skipped_inodes;
        self.processed_buckets += other.processed_buckets;
        self.successful_deletes.extend(other.successful_deletes);
        self.successful_frees.extend(other.successful_frees);
        self.failed_inodes.extend(other.failed_inodes);
        self.retry_inodes.extend(other.retry_inodes);
    }

    pub fn total_processed(&self) -> u64 {
        self.total_processed
    }
}
