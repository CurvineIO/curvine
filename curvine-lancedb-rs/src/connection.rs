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

use std::collections::HashMap;

use crate::error::unsupported;

pub use lancedb_upstream::connection::{CloneTableBuilder, OpenTableBuilder, TableNamesBuilder};
pub use lancedb_upstream::connection::{ConnectRequest, Connection, LanceFileVersion};

#[derive(Debug)]
enum ConnectBuilderInner {
    Upstream(Box<lancedb_upstream::connection::ConnectBuilder>),
    UnsupportedCurvineUri { uri: String },
}

#[derive(Debug)]
pub struct ConnectBuilder {
    inner: ConnectBuilderInner,
}

impl ConnectBuilder {
    pub fn new(uri: &str) -> Self {
        if is_curvine_uri(uri) {
            Self {
                inner: ConnectBuilderInner::UnsupportedCurvineUri {
                    uri: uri.to_string(),
                },
            }
        } else {
            Self {
                inner: ConnectBuilderInner::Upstream(Box::new(lancedb_upstream::connect(uri))),
            }
        }
    }

    fn map_upstream(
        self,
        f: impl FnOnce(
            lancedb_upstream::connection::ConnectBuilder,
        ) -> lancedb_upstream::connection::ConnectBuilder,
    ) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(f(*builder))),
            },
            ConnectBuilderInner::UnsupportedCurvineUri { uri } => Self {
                inner: ConnectBuilderInner::UnsupportedCurvineUri { uri },
            },
        }
    }

    pub fn database_options(
        self,
        database_options: &dyn lancedb_upstream::database::DatabaseOptions,
    ) -> Self {
        self.map_upstream(|builder| builder.database_options(database_options))
    }

    pub fn embedding_registry(
        self,
        registry: std::sync::Arc<dyn lancedb_upstream::embeddings::EmbeddingRegistry>,
    ) -> Self {
        self.map_upstream(|builder| builder.embedding_registry(registry))
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.map_upstream(|builder| builder.storage_option(key, value))
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.map_upstream(|builder| builder.storage_options(pairs))
    }

    pub fn read_consistency_interval(self, read_consistency_interval: std::time::Duration) -> Self {
        self.map_upstream(|builder| builder.read_consistency_interval(read_consistency_interval))
    }

    pub fn session(self, session: std::sync::Arc<lancedb_upstream::Session>) -> Self {
        self.map_upstream(|builder| builder.session(session))
    }

    #[cfg(feature = "remote")]
    pub fn api_key(self, api_key: &str) -> Self {
        self.map_upstream(|builder| builder.api_key(api_key))
    }

    #[cfg(feature = "remote")]
    pub fn region(self, region: &str) -> Self {
        self.map_upstream(|builder| builder.region(region))
    }

    #[cfg(feature = "remote")]
    pub fn host_override(self, host_override: &str) -> Self {
        self.map_upstream(|builder| builder.host_override(host_override))
    }

    #[cfg(feature = "remote")]
    pub fn client_config(self, config: lancedb_upstream::remote::ClientConfig) -> Self {
        self.map_upstream(|builder| builder.client_config(config))
    }

    pub async fn execute(self) -> lancedb_upstream::Result<Connection> {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => builder.execute().await,
            ConnectBuilderInner::UnsupportedCurvineUri { uri } => {
                Err(unsupported(format!("Curvine object store, uri={uri}")))
            }
        }
    }
}

pub fn connect(uri: &str) -> ConnectBuilder {
    ConnectBuilder::new(uri)
}

enum ConnectNamespaceBuilderInner {
    Upstream(lancedb_upstream::connection::ConnectNamespaceBuilder),
    UnsupportedCurvineUri { uri: String },
}

pub struct ConnectNamespaceBuilder {
    inner: ConnectNamespaceBuilderInner,
}

impl std::fmt::Debug for ConnectNamespaceBuilderInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Upstream(_) => f.write_str("Upstream(..)"),
            Self::UnsupportedCurvineUri { uri } => f
                .debug_struct("UnsupportedCurvineUri")
                .field("uri", uri)
                .finish(),
        }
    }
}

impl std::fmt::Debug for ConnectNamespaceBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectNamespaceBuilder")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ConnectNamespaceBuilder {
    fn new(ns_impl: &str, properties: HashMap<String, String>) -> Self {
        if let Some(uri) = find_curvine_uri(&properties) {
            Self {
                inner: ConnectNamespaceBuilderInner::UnsupportedCurvineUri { uri },
            }
        } else {
            Self {
                inner: ConnectNamespaceBuilderInner::Upstream(lancedb_upstream::connect_namespace(
                    ns_impl, properties,
                )),
            }
        }
    }

    fn map_upstream(
        self,
        f: impl FnOnce(
            lancedb_upstream::connection::ConnectNamespaceBuilder,
        ) -> lancedb_upstream::connection::ConnectNamespaceBuilder,
    ) -> Self {
        match self.inner {
            ConnectNamespaceBuilderInner::Upstream(builder) => Self {
                inner: ConnectNamespaceBuilderInner::Upstream(f(builder)),
            },
            ConnectNamespaceBuilderInner::UnsupportedCurvineUri { uri } => Self {
                inner: ConnectNamespaceBuilderInner::UnsupportedCurvineUri { uri },
            },
        }
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.map_upstream(|builder| builder.storage_option(key, value))
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.map_upstream(|builder| builder.storage_options(pairs))
    }

    pub fn read_consistency_interval(self, read_consistency_interval: std::time::Duration) -> Self {
        self.map_upstream(|builder| builder.read_consistency_interval(read_consistency_interval))
    }

    pub fn embedding_registry(
        self,
        registry: std::sync::Arc<dyn lancedb_upstream::embeddings::EmbeddingRegistry>,
    ) -> Self {
        self.map_upstream(|builder| builder.embedding_registry(registry))
    }

    pub fn session(self, session: std::sync::Arc<lancedb_upstream::Session>) -> Self {
        self.map_upstream(|builder| builder.session(session))
    }

    pub fn server_side_query(self, enabled: bool) -> Self {
        self.map_upstream(|builder| builder.server_side_query(enabled))
    }

    pub async fn execute(self) -> lancedb_upstream::Result<Connection> {
        match self.inner {
            ConnectNamespaceBuilderInner::Upstream(builder) => builder.execute().await,
            ConnectNamespaceBuilderInner::UnsupportedCurvineUri { uri } => {
                Err(unsupported(format!("Curvine object store, uri={uri}")))
            }
        }
    }
}

pub fn connect_namespace(
    ns_impl: &str,
    properties: HashMap<String, String>,
) -> ConnectNamespaceBuilder {
    ConnectNamespaceBuilder::new(ns_impl, properties)
}

fn is_curvine_uri(uri: &str) -> bool {
    uri.starts_with("curvine://")
}

fn find_curvine_uri(properties: &HashMap<String, String>) -> Option<String> {
    for key in ["root", "uri"] {
        if let Some(value) = properties.get(key) {
            if is_curvine_uri(value) {
                return Some(value.clone());
            }
        }
    }

    properties
        .values()
        .find(|value| is_curvine_uri(value))
        .cloned()
}
