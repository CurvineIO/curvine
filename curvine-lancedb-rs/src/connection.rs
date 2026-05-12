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

use crate::object_store::curvine_session;

pub use lancedb_upstream::connection::{CloneTableBuilder, OpenTableBuilder, TableNamesBuilder};
pub use lancedb_upstream::connection::{ConnectRequest, Connection, LanceFileVersion};

#[derive(Debug)]
enum ConnectBuilderInner {
    Upstream(Box<lancedb_upstream::connection::ConnectBuilder>),
}

#[derive(Debug)]
pub struct ConnectBuilder {
    inner: ConnectBuilderInner,
}

impl ConnectBuilder {
    pub fn new(uri: &str) -> Self {
        let builder = lancedb_upstream::connect(uri);
        let builder = if is_curvine_uri(uri) {
            builder.session(curvine_session())
        } else {
            builder
        };
        Self {
            inner: ConnectBuilderInner::Upstream(Box::new(builder)),
        }
    }

    fn map_builder(
        self,
        f: impl FnOnce(
            lancedb_upstream::connection::ConnectBuilder,
        ) -> lancedb_upstream::connection::ConnectBuilder,
    ) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => Self {
                inner: ConnectBuilderInner::Upstream(Box::new(f(*builder))),
            },
        }
    }

    pub fn database_options(
        self,
        database_options: &dyn lancedb_upstream::database::DatabaseOptions,
    ) -> Self {
        self.map_builder(|builder| builder.database_options(database_options))
    }

    pub fn embedding_registry(
        self,
        registry: std::sync::Arc<dyn lancedb_upstream::embeddings::EmbeddingRegistry>,
    ) -> Self {
        self.map_builder(|builder| builder.embedding_registry(registry))
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.map_builder(|builder| builder.storage_option(key, value))
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.map_builder(|builder| builder.storage_options(pairs))
    }

    pub fn read_consistency_interval(self, read_consistency_interval: std::time::Duration) -> Self {
        self.map_builder(|builder| builder.read_consistency_interval(read_consistency_interval))
    }

    pub fn session(self, session: std::sync::Arc<lancedb_upstream::Session>) -> Self {
        self.map_builder(|builder| builder.session(session))
    }

    #[cfg(feature = "remote")]
    pub fn api_key(self, api_key: &str) -> Self {
        self.map_builder(|builder| builder.api_key(api_key))
    }

    #[cfg(feature = "remote")]
    pub fn region(self, region: &str) -> Self {
        self.map_builder(|builder| builder.region(region))
    }

    #[cfg(feature = "remote")]
    pub fn host_override(self, host_override: &str) -> Self {
        self.map_builder(|builder| builder.host_override(host_override))
    }

    #[cfg(feature = "remote")]
    pub fn client_config(self, config: lancedb_upstream::remote::ClientConfig) -> Self {
        self.map_builder(|builder| builder.client_config(config))
    }

    pub async fn execute(self) -> lancedb_upstream::Result<Connection> {
        match self.inner {
            ConnectBuilderInner::Upstream(builder) => builder.execute().await,
        }
    }
}

pub fn connect(uri: &str) -> ConnectBuilder {
    ConnectBuilder::new(uri)
}

#[derive(Debug)]
struct ConnectNamespacePending {
    ns_impl: String,
    properties: HashMap<String, String>,
    storage_options: HashMap<String, String>,
    read_consistency_interval: Option<std::time::Duration>,
    embedding_registry: Option<std::sync::Arc<dyn lancedb_upstream::embeddings::EmbeddingRegistry>>,
    session: Option<std::sync::Arc<lancedb_upstream::Session>>,
    server_side_query: bool,
}

impl ConnectNamespaceBuilder {
    fn new(ns_impl: &str, properties: HashMap<String, String>) -> Self {
        Self {
            pending: ConnectNamespacePending {
                ns_impl: ns_impl.to_string(),
                properties,
                storage_options: HashMap::new(),
                read_consistency_interval: None,
                embedding_registry: None,
                session: None,
                server_side_query: false,
            },
        }
    }

    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.pending
            .storage_options
            .insert(key.into(), value.into());
        self
    }

    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (key, value) in pairs {
            self.pending
                .storage_options
                .insert(key.into(), value.into());
        }
        self
    }

    pub fn read_consistency_interval(
        mut self,
        read_consistency_interval: std::time::Duration,
    ) -> Self {
        self.pending.read_consistency_interval = Some(read_consistency_interval);
        self
    }

    pub fn embedding_registry(
        mut self,
        registry: std::sync::Arc<dyn lancedb_upstream::embeddings::EmbeddingRegistry>,
    ) -> Self {
        self.pending.embedding_registry = Some(registry);
        self
    }

    pub fn session(mut self, session: std::sync::Arc<lancedb_upstream::Session>) -> Self {
        self.pending.session = Some(session);
        self
    }

    pub fn server_side_query(mut self, enabled: bool) -> Self {
        self.pending.server_side_query = enabled;
        self
    }

    pub async fn execute(self) -> lancedb_upstream::Result<Connection> {
        let wants_curvine = find_curvine_uri(&self.pending.properties).is_some();
        let mut builder =
            lancedb_upstream::connect_namespace(&self.pending.ns_impl, self.pending.properties);

        for (key, value) in self.pending.storage_options {
            builder = builder.storage_option(key, value);
        }

        if let Some(read_consistency_interval) = self.pending.read_consistency_interval {
            builder = builder.read_consistency_interval(read_consistency_interval);
        }

        if let Some(embedding_registry) = self.pending.embedding_registry {
            builder = builder.embedding_registry(embedding_registry);
        }

        if wants_curvine {
            builder = builder.session(curvine_session());
        } else if let Some(session) = self.pending.session {
            builder = builder.session(session);
        }

        builder
            .server_side_query(self.pending.server_side_query)
            .execute()
            .await
    }
}

#[derive(Debug)]
pub struct ConnectNamespaceBuilder {
    pending: ConnectNamespacePending,
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
