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

pub mod adapters;
pub mod core;
pub mod curvine;
pub mod local;
pub mod traits;
pub mod types;

pub use adapters::{expand_path, CurvineFileSystemAdapter, LocalFileSystemAdapter};
pub use core::{CacheRefreshError, CacheRefreshTask, CredentialStoreCore};
pub use curvine::CurvineAccessKeyStore;
pub use local::LocalAccessKeyStore;
pub use traits::{AccesskeyStore, CredentialStore, FileSystemAdapter};
pub use types::{AccessKeyStoreEnum, CacheState, CredentialEntry, StoreConfig};

pub const DEFAULT_CREDENTIALS_PATH: &str = "/system/auth/credentials.jsonl";
pub const DEFAULT_LOCAL_CREDENTIALS_PATH: &str = "~/.curvine/credentials.jsonl";
