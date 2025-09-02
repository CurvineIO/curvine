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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use super::traits::{AccesskeyStore, CredentialStore};
use crate::auth::secure_key::SecretKey;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialEntry {
    pub access_key: String,
    pub secret_key: SecretKey,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub created_at: DateTime<Utc>,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl CredentialEntry {
    pub fn new(
        access_key: String,
        secret_key: impl Into<SecretKey>,
        description: Option<String>,
    ) -> Self {
        Self {
            access_key,
            secret_key: secret_key.into(),
            created_at: Utc::now(),
            enabled: true,
            description,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Default)]
pub struct CacheState {
    pub credentials: HashMap<String, String>,
    pub last_modified: Option<SystemTime>,
    pub last_refresh: Option<Instant>,
}

impl CacheState {
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
            last_modified: None,
            last_refresh: Some(Instant::now()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub credentials_path: Option<String>,
    pub cache_refresh_interval: Duration,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            credentials_path: None,
            cache_refresh_interval: Duration::from_secs(30),
        }
    }
}

impl StoreConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_credentials_path(mut self, path: Option<String>) -> Self {
        self.credentials_path = path;
        self
    }

    pub fn with_cache_refresh_interval(mut self, interval: Duration) -> Self {
        self.cache_refresh_interval = interval;
        self
    }
}

#[derive(Clone)]
pub enum AccessKeyStoreEnum {
    Local(Arc<crate::auth::LocalAccessKeyStore>),
    Curvine(Arc<crate::auth::CurvineAccessKeyStore>),
}

impl AccessKeyStoreEnum {
    pub fn store_type(&self) -> &'static str {
        match self {
            AccessKeyStoreEnum::Local(_) => "Local Filesystem",
            AccessKeyStoreEnum::Curvine(_) => "Curvine Distributed",
        }
    }

    pub fn start_cache_refresh_task(
        &self,
        runtime: Arc<orpc::runtime::AsyncRuntime>,
    ) -> Result<(), String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store
                .clone()
                .start_cache_refresh_task(runtime)
                .map(|_| ())
                .map_err(|e| format!("Failed to start cache refresh task for local store: {}", e)),
            AccessKeyStoreEnum::Curvine(store) => store
                .clone()
                .start_cache_refresh_task(runtime)
                .map(|_| ())
                .map_err(|e| {
                    format!(
                        "Failed to start cache refresh task for Curvine store: {}",
                        e
                    )
                }),
        }
    }
}

use async_trait::async_trait;

#[async_trait]
impl AccesskeyStore for AccessKeyStoreEnum {
    async fn get(&self, accesskey: &str) -> Result<Option<String>, String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.get(accesskey).await,
            AccessKeyStoreEnum::Curvine(store) => store.get(accesskey).await,
        }
    }
}

#[async_trait]
impl CredentialStore for AccessKeyStoreEnum {
    async fn initialize(&self) -> Result<(), String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.initialize().await,
            AccessKeyStoreEnum::Curvine(store) => store.initialize().await,
        }
    }

    async fn add_credential(&self, entry: CredentialEntry) -> Result<(), String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.add_credential(entry).await,
            AccessKeyStoreEnum::Curvine(store) => store.add_credential(entry).await,
        }
    }

    async fn list_credentials(&self) -> Result<Vec<CredentialEntry>, String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.list_credentials().await,
            AccessKeyStoreEnum::Curvine(store) => store.list_credentials().await,
        }
    }

    async fn get_cache_stats(&self) -> (usize, Option<SystemTime>, Duration) {
        match self {
            AccessKeyStoreEnum::Local(store) => store.get_cache_stats().await,
            AccessKeyStoreEnum::Curvine(store) => store.get_cache_stats().await,
        }
    }

    async fn refresh_cache(&self) -> Result<(), String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.refresh_cache().await,
            AccessKeyStoreEnum::Curvine(store) => store.refresh_cache().await,
        }
    }

    fn store_type(&self) -> &'static str {
        self.store_type()
    }

    fn credentials_path(&self) -> String {
        match self {
            AccessKeyStoreEnum::Local(store) => store.credentials_path(),
            AccessKeyStoreEnum::Curvine(store) => store.credentials_path(),
        }
    }
}
