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

use curvine_common::executor::ScheduledExecutor;
use orpc::runtime::{LoopTask, RpcRuntime};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tracing;

use super::traits::FileSystemAdapter;
use super::types::{CacheState, CredentialEntry};

/// Core credential store implementation that works with any FileSystemAdapter
///
/// This struct contains all the common logic for credential management,
/// including caching, file parsing, and refresh mechanisms.
pub struct CredentialStoreCore<T: FileSystemAdapter> {
    fs_adapter: T,
    credentials_path: T::PathType,
    cache: Arc<RwLock<CacheState>>,
    cache_refresh_interval: Duration,
}

impl<T: FileSystemAdapter> CredentialStoreCore<T> {
    pub fn new(
        fs_adapter: T,
        credentials_path: T::PathType,
        cache_refresh_interval: Duration,
    ) -> Self {
        tracing::debug!(
            "Initializing credential store core with path: {}, refresh_interval: {:?}",
            fs_adapter.path_to_string(&credentials_path),
            cache_refresh_interval
        );

        Self {
            fs_adapter,
            credentials_path,
            cache: Arc::new(RwLock::new(CacheState::new())),
            cache_refresh_interval,
        }
    }

    /// Initialize the store by creating the credentials file if it doesn't exist
    pub async fn initialize(&self) -> Result<(), String> {
        tracing::debug!("Initializing credential store core");

        // Check if credentials file exists
        match self.fs_adapter.file_exists(&self.credentials_path).await {
            Ok(true) => {
                tracing::info!("Credentials file exists, loading initial cache");
                self.refresh_cache().await?;
            }
            Ok(false) => {
                tracing::info!("Credentials file does not exist, creating empty file");
                self.create_empty_credentials_file().await?;
            }
            Err(e) => return orpc::err_box!("Failed to check credentials file existence: {}", e),
        }

        Ok(())
    }

    /// Create an empty credentials file
    async fn create_empty_credentials_file(&self) -> Result<(), String> {
        // Ensure parent directory exists
        self.fs_adapter
            .ensure_parent_dir(&self.credentials_path)
            .await?;

        // Create empty file
        self.fs_adapter
            .create_empty_file(&self.credentials_path)
            .await?;

        tracing::info!(
            "Created empty credentials file at: {}",
            self.fs_adapter.path_to_string(&self.credentials_path)
        );
        Ok(())
    }

    /// Refresh the in-memory cache from the file system
    pub async fn refresh_cache(&self) -> Result<(), String> {
        tracing::debug!("Refreshing credentials cache from file system");

        // Check file modification time first
        let file_mtime = self
            .fs_adapter
            .get_file_mtime(&self.credentials_path)
            .await?;

        // Check if we need to refresh based on time or file modification
        let should_refresh = {
            let cache = self.cache.read().await;
            cache
                .last_refresh
                .is_none_or(|last| last.elapsed() >= self.cache_refresh_interval)
                || cache.last_modified.is_none_or(|last| file_mtime > last)
        };

        if !should_refresh {
            tracing::debug!("Cache is up to date, skipping refresh");
            return Ok(());
        }

        // Load credentials and update cache
        let credentials = self.load_credentials_from_file().await?;
        let size = {
            let mut cache = self.cache.write().await;
            cache.credentials.clear();
            for entry in credentials {
                if entry.is_valid() {
                    cache.credentials.insert(
                        entry.access_key.clone(),
                        entry.secret_key.expose().to_string(),
                    );
                }
            }
            cache.last_modified = Some(file_mtime);
            cache.last_refresh = Some(Instant::now());
            cache.credentials.len()
        };

        tracing::debug!("Refreshed cache with {} credentials", size);
        Ok(())
    }

    /// Load credentials from the file system
    async fn load_credentials_from_file(&self) -> Result<Vec<CredentialEntry>, String> {
        let content = self.fs_adapter.read_file(&self.credentials_path).await?;

        // Parse JSONL content
        let content_str = String::from_utf8(content)
            .map_err(|e| format!("Invalid UTF-8 in credentials file: {}", e))?;

        let mut credentials = Vec::new();

        for (line_num, line) in content_str.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            match serde_json::from_str::<CredentialEntry>(line) {
                Ok(entry) => credentials.push(entry),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse credential entry at line {}: {}",
                        line_num + 1,
                        e
                    );
                    // Continue parsing other lines instead of failing completely
                }
            }
        }

        tracing::debug!("Loaded {} credential entries from file", credentials.len());
        Ok(credentials)
    }

    /// Add a new credential entry
    pub async fn add_credential(&self, entry: CredentialEntry) -> Result<(), String> {
        tracing::info!("Adding new credential for access key: {}", entry.access_key);

        let json_line = serde_json::to_string(&entry)
            .map_err(|e| format!("Failed to serialize credential entry: {}", e))?;

        let line_with_newline = format!("{}\n", json_line);
        self.fs_adapter
            .append_file(&self.credentials_path, line_with_newline.as_bytes())
            .await?;

        if entry.is_valid() {
            let mut cache = self.cache.write().await;
            cache.credentials.insert(
                entry.access_key.clone(),
                entry.secret_key.expose().to_string(),
            );
            cache.last_refresh = Some(Instant::now());

            if let Ok(file_mtime) = self.fs_adapter.get_file_mtime(&self.credentials_path).await {
                cache.last_modified = Some(file_mtime);
            }
        }

        tracing::info!(
            "Successfully added credential for access key: {}",
            entry.access_key
        );
        Ok(())
    }

    pub async fn list_credentials(&self) -> Result<Vec<CredentialEntry>, String> {
        self.load_credentials_from_file().await
    }

    pub async fn get_cache_stats(&self) -> (usize, Option<SystemTime>, Duration) {
        let cache = self.cache.read().await;
        let cache_age = cache
            .last_refresh
            .map(|instant| instant.elapsed())
            .unwrap_or(Duration::ZERO);
        (cache.credentials.len(), cache.last_modified, cache_age)
    }

    pub async fn get_access_key(&self, accesskey: &str) -> Result<Option<String>, String> {
        tracing::debug!("Looking up access key: {}", accesskey);

        // Perform fast lookup from cache (hold read lock briefly)
        let result = {
            let cache = self.cache.read().await;
            cache.credentials.get(accesskey).cloned()
        };

        match &result {
            Some(_) => tracing::debug!("Access key found in cache: {}", accesskey),
            None => tracing::debug!("Access key not found: {}", accesskey),
        }

        Ok(result)
    }

    pub fn credentials_path(&self) -> String {
        self.fs_adapter.path_to_string(&self.credentials_path)
    }

    pub fn start_cache_refresh_task(
        self: Arc<Self>,
        runtime: Arc<orpc::runtime::AsyncRuntime>,
    ) -> Result<CacheRefreshTask<T>, String> {
        let task = CacheRefreshTask::new(Arc::clone(&self), runtime);
        let refresh_interval_ms = self.cache_refresh_interval.as_millis() as u64;

        let executor =
            ScheduledExecutor::new("credential-store-cache-refresh", refresh_interval_ms);
        executor
            .start(task.clone())
            .map_err(|e| format!("Failed to start cache refresh task: {}", e))?;

        tracing::info!(
            "Started cache refresh task with interval: {:?}",
            self.cache_refresh_interval
        );
        Ok(task)
    }
}

pub use crate::error::StorageError as CacheRefreshError;

#[derive(Clone)]
pub struct CacheRefreshTask<T: FileSystemAdapter> {
    terminated: Arc<std::sync::atomic::AtomicBool>,
    store: Arc<CredentialStoreCore<T>>,
    runtime: Arc<orpc::runtime::AsyncRuntime>,
}

impl<T: FileSystemAdapter> CacheRefreshTask<T> {
    pub fn new(
        store: Arc<CredentialStoreCore<T>>,
        runtime: Arc<orpc::runtime::AsyncRuntime>,
    ) -> Self {
        Self {
            store,
            terminated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime,
        }
    }

    pub fn terminate(&self) {
        self.terminated
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<T: FileSystemAdapter> LoopTask for CacheRefreshTask<T> {
    type Error = CacheRefreshError;

    fn run(&self) -> Result<(), Self::Error> {
        // Use the provided runtime to execute async refresh_cache
        let store = Arc::clone(&self.store);

        self.runtime.block_on(async {
            if let Err(e) = store.refresh_cache().await {
                tracing::warn!("Cache refresh failed: {}", e);
                return Err(CacheRefreshError::CacheError(format!(
                    "Cache refresh failed: {}",
                    e
                )));
            }
            Ok(())
        })
    }

    fn terminate(&self) -> bool {
        self.terminated.load(std::sync::atomic::Ordering::Relaxed)
    }
}
