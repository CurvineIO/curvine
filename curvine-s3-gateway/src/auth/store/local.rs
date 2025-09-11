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

use super::{
    adapters::{expand_path, LocalFileSystemAdapter},
    core::{CacheRefreshTask, CredentialStoreCore},
    traits::{AccesskeyStore, CredentialStore},
    types::CredentialEntry,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub const DEFAULT_LOCAL_CREDENTIALS_PATH: &str = "~/.curvine/credentials.jsonl";

pub struct LocalAccessKeyStore {
    core: Arc<CredentialStoreCore<LocalFileSystemAdapter>>,
}

impl LocalAccessKeyStore {
    pub fn new(
        credentials_path: Option<&str>,
        cache_refresh_interval: Option<Duration>,
    ) -> Result<Self, String> {
        let path_str = credentials_path.unwrap_or(DEFAULT_LOCAL_CREDENTIALS_PATH);
        let credentials_path = expand_path(path_str)
            .map_err(|e| format!("Failed to expand path '{}': {}", path_str, e))?;

        let fs_adapter = LocalFileSystemAdapter::new();
        let refresh_interval = cache_refresh_interval.unwrap_or(Duration::from_secs(30));

        let core = Arc::new(CredentialStoreCore::new(
            fs_adapter,
            credentials_path,
            refresh_interval,
        ));

        tracing::debug!(
            "Initializing local access key store with path: {}",
            core.credentials_path()
        );

        Ok(Self { core })
    }

    pub async fn initialize(&self) -> Result<(), String> {
        self.core.initialize().await
    }

    pub async fn refresh_cache(&self) -> Result<(), String> {
        self.core.refresh_cache().await
    }

    pub async fn add_credential(&self, entry: CredentialEntry) -> Result<(), String> {
        self.core.add_credential(entry).await
    }

    pub async fn list_credentials(&self) -> Result<Vec<CredentialEntry>, String> {
        self.core.list_credentials().await
    }

    pub async fn get_cache_stats(&self) -> (usize, Option<SystemTime>, Duration) {
        self.core.get_cache_stats().await
    }

    pub fn start_cache_refresh_task(
        self: Arc<Self>,
        runtime: Arc<orpc::runtime::AsyncRuntime>,
    ) -> Result<CacheRefreshTask<LocalFileSystemAdapter>, String> {
        self.core.clone().start_cache_refresh_task(runtime)
    }
}

#[async_trait]
impl AccesskeyStore for LocalAccessKeyStore {
    async fn get(&self, accesskey: &str) -> Result<Option<String>, String> {
        tracing::debug!("Looking up access key in local store: {}", accesskey);
        self.core.get_access_key(accesskey).await
    }
}

#[async_trait]
impl CredentialStore for LocalAccessKeyStore {
    async fn initialize(&self) -> Result<(), String> {
        LocalAccessKeyStore::initialize(self).await
    }

    async fn add_credential(&self, entry: CredentialEntry) -> Result<(), String> {
        LocalAccessKeyStore::add_credential(self, entry).await
    }

    async fn list_credentials(&self) -> Result<Vec<CredentialEntry>, String> {
        LocalAccessKeyStore::list_credentials(self).await
    }

    async fn get_cache_stats(&self) -> (usize, Option<std::time::SystemTime>, std::time::Duration) {
        LocalAccessKeyStore::get_cache_stats(self).await
    }

    async fn refresh_cache(&self) -> Result<(), String> {
        LocalAccessKeyStore::refresh_cache(self).await
    }

    fn store_type(&self) -> &'static str {
        "Local Filesystem"
    }

    fn credentials_path(&self) -> String {
        self.core.credentials_path()
    }
}
