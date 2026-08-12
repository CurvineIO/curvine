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

use async_trait::async_trait;
use std::time::{Duration, SystemTime};

use super::types::CredentialEntry;

#[async_trait]
pub trait AccesskeyStore: Send + Sync {
    async fn get(&self, accesskey: &str) -> Result<Option<String>, String>;
}

#[async_trait]
pub trait CredentialStore: AccesskeyStore {
    async fn initialize(&self) -> Result<(), String>;
    async fn add_credential(&self, entry: CredentialEntry) -> Result<(), String>;
    async fn list_credentials(&self) -> Result<Vec<CredentialEntry>, String>;
    async fn get_cache_stats(&self) -> (usize, Option<SystemTime>, Duration);
    async fn refresh_cache(&self) -> Result<(), String>;
    fn store_type(&self) -> &'static str;
    fn credentials_path(&self) -> String;
}

#[async_trait]
pub trait FileSystemAdapter: Send + Sync + Clone + 'static {
    type PathType: Send + Sync + Clone;
    fn path_to_string(&self, path: &Self::PathType) -> String;
    async fn read_file(&self, path: &Self::PathType) -> Result<Vec<u8>, String>;
    async fn write_file(&self, path: &Self::PathType, content: &[u8]) -> Result<(), String>;
    async fn append_file(&self, path: &Self::PathType, content: &[u8]) -> Result<(), String>;
    async fn get_file_mtime(&self, path: &Self::PathType) -> Result<SystemTime, String>;
    async fn ensure_parent_dir(&self, path: &Self::PathType) -> Result<(), String>;
    async fn file_exists(&self, path: &Self::PathType) -> Result<bool, String>;
    async fn create_empty_file(&self, path: &Self::PathType) -> Result<(), String>;
}
