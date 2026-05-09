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

use std::sync::Arc;

use async_trait::async_trait;
use lance_core::error::Result;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreProvider};
use lancedb_upstream::error::Error;
use lancedb_upstream::ObjectStoreRegistry;
use lancedb_upstream::Session;
use url::Url;

pub const CURVINE_SCHEME: &str = "curvine";

#[derive(Debug, Clone, Default)]
pub struct CurvineObjectStore;

#[derive(Debug, Clone, Default)]
pub struct CurvineObjectStoreProvider;

impl CurvineObjectStoreProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ObjectStoreProvider for CurvineObjectStoreProvider {
    async fn new_store(&self, base_path: Url, _params: &ObjectStoreParams) -> Result<ObjectStore> {
        Err(lance_core::Error::invalid_input(format!(
            "Curvine object store is not implemented, uri={base_path}"
        )))
    }
}

pub fn curvine_registry() -> Arc<ObjectStoreRegistry> {
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert(CURVINE_SCHEME, Arc::new(CurvineObjectStoreProvider::new()));
    registry
}

pub fn curvine_session() -> Arc<Session> {
    Arc::new(Session::new(0, 0, curvine_registry()))
}

pub fn unsupported_curvine_uri(uri: impl Into<String>) -> Error {
    Error::NotSupported {
        message: format!(
            "Curvine object store, uri={} is not implemented",
            uri.into()
        ),
    }
}
