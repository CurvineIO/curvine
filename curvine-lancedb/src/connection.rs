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
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Deref;
use std::path::MAIN_SEPARATOR;
use std::sync::Arc;
use std::time::Duration;

use crate::object_store::curvine_session;
use crate::safe_commit::SafeCommitDatabase;

use lance::dataset::builder::DatasetBuilder;
use lance::dataset::refs::Ref;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, ReadParams, WriteDestination};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
use lance::{Dataset, Result as LanceResult};
use lance_namespace::LanceNamespace;
use lance_table::io::commit::ConditionalPutCommitHandler;
use lancedb_upstream::connection::{
    CloneTableBuilder as UpstreamCloneTableBuilder, ConnectBuilder as UpstreamConnectBuilder,
};
use lancedb_upstream::database::{CloneTableRequest, Database, DatabaseOptions};
use lancedb_upstream::embeddings::{EmbeddingRegistry, MemoryRegistry};
use lancedb_upstream::error::{Error, Result};
#[cfg(feature = "remote")]
use lancedb_upstream::remote::ClientConfig;
use lancedb_upstream::{
    connect as upstream_connect, connect_namespace as upstream_connect_namespace,
    Connection as UpstreamConnection, Session, Table,
};
use url::Url;

pub use lancedb_upstream::connection::{
    ConnectRequest, LanceFileVersion, OpenTableBuilder, TableNamesBuilder,
};

const LANCE_FILE_EXTENSION: &str = "lance";

#[derive(Debug)]
enum ConnectBuilderInner {
    Upstream {
        builder: Box<UpstreamConnectBuilder>,
        options: ConnectionOptions,
    },
}

#[derive(Clone, Debug, Default)]
struct ConnectionOptions {
    uri: String,
    query_string: Option<String>,
    storage_options: HashMap<String, String>,
    session: Option<Arc<Session>>,
    embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
    namespace_backed: bool,
}

/// Builder for a LanceDB connection. For `curvine://` URIs, applies a default session and, after
/// `execute`, may wrap the inner database so `create_table` / `open_table` use a safe commit handler.
#[derive(Debug)]
pub struct ConnectBuilder {
    inner: ConnectBuilderInner,
}

/// Established connection. For `curvine://` listing URIs, the inner database may be wrapped for safe commits.
#[derive(Clone)]
pub struct Connection {
    upstream: UpstreamConnection,
    options: ConnectionOptions,
}

pub struct CloneTableBuilder {
    upstream: UpstreamCloneTableBuilder,
    connection: Connection,
    request: CloneTableRequest,
}

impl ConnectBuilder {
    pub fn new(uri: &str) -> Self {
        let builder = upstream_connect(uri);
        let session = is_curvine_uri(uri).then(curvine_session);
        let options = ConnectionOptions {
            uri: normalize_listing_uri(uri).unwrap_or_else(|| uri.to_string()),
            query_string: listing_query_string(uri),
            session: session.clone(),
            ..Default::default()
        };
        if is_curvine_uri(uri) {
            Self {
                inner: ConnectBuilderInner::Upstream {
                    builder: Box::new(builder.session(session.unwrap_or_else(curvine_session))),
                    options,
                },
            }
        } else {
            Self {
                inner: ConnectBuilderInner::Upstream {
                    builder: Box::new(builder),
                    options,
                },
            }
        }
    }

    fn map_upstream(
        self,
        f: impl FnOnce(UpstreamConnectBuilder) -> UpstreamConnectBuilder,
        update: impl FnOnce(&mut ConnectionOptions),
    ) -> Self {
        match self.inner {
            ConnectBuilderInner::Upstream {
                builder,
                mut options,
            } => {
                update(&mut options);
                Self {
                    inner: ConnectBuilderInner::Upstream {
                        builder: Box::new(f(*builder)),
                        options,
                    },
                }
            }
        }
    }

    pub fn database_options(self, database_options: &dyn DatabaseOptions) -> Self {
        self.map_upstream(|builder| builder.database_options(database_options), |_| {})
    }

    pub fn embedding_registry(self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        let reg = registry.clone();
        self.map_upstream(
            |builder| builder.embedding_registry(reg),
            |options| {
                options.embedding_registry = Some(registry);
            },
        )
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        let upstream_key = key.clone();
        let upstream_value = value.clone();
        self.map_upstream(
            |builder| builder.storage_option(upstream_key, upstream_value),
            |options| {
                options.storage_options.insert(key, value);
            },
        )
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let pairs = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<Vec<_>>();
        let upstream_pairs = pairs.clone();
        self.map_upstream(
            |builder| builder.storage_options(upstream_pairs),
            |options| {
                options.storage_options.extend(pairs);
            },
        )
    }

    pub fn read_consistency_interval(self, read_consistency_interval: Duration) -> Self {
        self.map_upstream(
            |builder| builder.read_consistency_interval(read_consistency_interval),
            |_| {},
        )
    }

    pub fn session(self, session: Arc<Session>) -> Self {
        let upstream_session = session.clone();
        self.map_upstream(
            |builder| builder.session(upstream_session),
            |options| {
                options.session = Some(session);
            },
        )
    }

    #[cfg(feature = "remote")]
    pub fn api_key(self, api_key: &str) -> Self {
        self.map_upstream(|builder| builder.api_key(api_key), |_| {})
    }

    #[cfg(feature = "remote")]
    pub fn region(self, region: &str) -> Self {
        self.map_upstream(|builder| builder.region(region), |_| {})
    }

    #[cfg(feature = "remote")]
    pub fn host_override(self, host_override: &str) -> Self {
        self.map_upstream(|builder| builder.host_override(host_override), |_| {})
    }

    #[cfg(feature = "remote")]
    pub fn client_config(self, config: ClientConfig) -> Self {
        self.map_upstream(|builder| builder.client_config(config), |_| {})
    }

    pub async fn execute(self) -> Result<Connection> {
        match self.inner {
            ConnectBuilderInner::Upstream { builder, options } => {
                let upstream = builder.execute().await?;
                let upstream = wrap_upstream_connection_for_curvine(upstream, &options);
                Ok(Connection { upstream, options })
            }
        }
    }
}

pub fn connect(uri: &str) -> ConnectBuilder {
    ConnectBuilder::new(uri)
}

fn wrap_upstream_connection_for_curvine(
    upstream: UpstreamConnection,
    options: &ConnectionOptions,
) -> UpstreamConnection {
    if !is_curvine_uri(&options.uri) {
        return upstream;
    }
    let embedding_registry = options
        .embedding_registry
        .clone()
        .unwrap_or_else(|| Arc::new(MemoryRegistry::new()));
    let inner_db = upstream.database().clone();
    let wrapped: Arc<dyn Database> = if options.namespace_backed {
        Arc::new(SafeCommitDatabase::new_namespace(inner_db))
    } else {
        Arc::new(SafeCommitDatabase::new(inner_db))
    };
    UpstreamConnection::new(wrapped, embedding_registry)
}

impl Connection {
    pub fn clone_table(
        &self,
        target_table_name: impl Into<String>,
        source_uri: impl Into<String>,
    ) -> CloneTableBuilder {
        let target_table_name = target_table_name.into();
        let source_uri = source_uri.into();
        CloneTableBuilder {
            upstream: self
                .upstream
                .clone_table(target_table_name.clone(), source_uri.clone()),
            connection: self.clone(),
            request: CloneTableRequest::new(target_table_name, source_uri),
        }
    }

    fn should_handle_curvine_clone(&self, request: &CloneTableRequest) -> bool {
        is_curvine_uri(&self.options.uri)
            && is_curvine_uri(&request.source_uri)
            && request.is_shallow
            && request.target_namespace.is_empty()
            && request.namespace_client.is_none()
            && !self.options.namespace_backed
    }

    async fn clone_curvine_table(&self, request: CloneTableRequest) -> Result<Table> {
        validate_table_name(&request.target_table_name)?;

        let session = self.options.session.clone().unwrap_or_else(curvine_session);
        let storage_params = self.object_store_params();
        let read_params = ReadParams {
            store_options: Some(storage_params.clone()),
            session: Some(session.clone()),
            commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
            ..Default::default()
        };
        let mut source_dataset = DatasetBuilder::from_uri(&request.source_uri)
            .with_read_params(read_params)
            .load()
            .await
            .map_err(Error::from)?;

        let (version_ref, version_number) = match (request.source_version, request.source_tag) {
            (Some(version), None) => (Ref::Version(None, Some(version)), version),
            (None, Some(tag)) => {
                let tag_contents = source_dataset.tags().get(&tag).await.map_err(Error::from)?;
                (Ref::Tag(tag), tag_contents.version)
            }
            (None, None) => {
                let version = source_dataset.version().version;
                (Ref::Version(None, Some(version)), version)
            }
            (Some(_), Some(_)) => {
                return Err(Error::InvalidInput {
                    message: "Cannot specify both source_version and source_tag".to_string(),
                });
            }
        };
        let target_uri = self.table_uri(&request.target_table_name)?;
        clone_dataset_with_session(
            &mut source_dataset,
            &target_uri,
            version_ref,
            version_number,
            storage_params,
            session,
        )
        .await
        .map_err(Error::from)?;

        self.open_table(request.target_table_name)
            .storage_options(self.options.storage_options.clone())
            .execute()
            .await
    }

    fn object_store_params(&self) -> ObjectStoreParams {
        ObjectStoreParams {
            storage_options_accessor: if self.options.storage_options.is_empty() {
                None
            } else {
                Some(Arc::new(StorageOptionsAccessor::with_static_options(
                    self.options.storage_options.clone(),
                )))
            },
            ..Default::default()
        }
    }

    fn table_uri(&self, name: &str) -> Result<String> {
        validate_table_name(name)?;

        let mut uri = self.options.uri.clone();
        if !(uri.ends_with('/') || uri.ends_with('\\')) {
            if uri.contains("://") {
                uri.push('/');
            } else {
                uri.push(MAIN_SEPARATOR);
            }
        }
        uri.push_str(name);
        uri.push('.');
        uri.push_str(LANCE_FILE_EXTENSION);
        if let Some(query) = &self.options.query_string {
            uri.push('?');
            uri.push_str(query);
        }
        Ok(uri)
    }
}

impl Deref for Connection {
    type Target = UpstreamConnection;

    fn deref(&self) -> &Self::Target {
        &self.upstream
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Connection")
            .field("uri", &self.options.uri)
            .finish()
    }
}

impl Display for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.upstream.fmt(f)
    }
}

impl CloneTableBuilder {
    pub fn source_version(mut self, version: u64) -> Self {
        self.upstream = self.upstream.source_version(version);
        self.request.source_version = Some(version);
        self
    }

    pub fn source_tag(mut self, tag: impl Into<String>) -> Self {
        let tag = tag.into();
        self.upstream = self.upstream.source_tag(tag.clone());
        self.request.source_tag = Some(tag);
        self
    }

    pub fn target_namespace(mut self, namespace: Vec<String>) -> Self {
        self.upstream = self.upstream.target_namespace(namespace.clone());
        self.request.target_namespace = namespace;
        self
    }

    pub fn is_shallow(mut self, is_shallow: bool) -> Self {
        self.upstream = self.upstream.is_shallow(is_shallow);
        self.request.is_shallow = is_shallow;
        self
    }

    pub fn namespace_client(mut self, client: Arc<dyn LanceNamespace>) -> Self {
        self.upstream = self.upstream.namespace_client(client.clone());
        self.request.namespace_client = Some(client);
        self
    }

    pub async fn execute(self) -> Result<Table> {
        if self.connection.should_handle_curvine_clone(&self.request) {
            self.connection.clone_curvine_table(self.request).await
        } else {
            self.upstream.execute().await
        }
    }
}

async fn clone_dataset_with_session(
    source_dataset: &mut Dataset,
    target_uri: &str,
    version: Ref,
    version_number: u64,
    storage_params: ObjectStoreParams,
    session: Arc<Session>,
) -> LanceResult<Dataset> {
    let ref_name = match &version {
        Ref::Version(branch, _) => branch.clone(),
        Ref::VersionNumber(_) => source_dataset.version().metadata.get("branch").cloned(),
        Ref::Tag(tag) => source_dataset.tags().get(tag).await?.branch,
    };
    let clone_op = Operation::Clone {
        is_shallow: true,
        ref_name,
        ref_version: version_number,
        ref_path: source_dataset.uri().to_string(),
        branch_name: None,
    };
    let transaction = Transaction::new(version_number, clone_op, None);

    CommitBuilder::new(WriteDestination::Uri(target_uri))
        .with_store_params(storage_params)
        .with_object_store(Arc::new(source_dataset.object_store().clone()))
        .with_commit_handler(Arc::new(ConditionalPutCommitHandler))
        .with_storage_format(
            source_dataset
                .manifest()
                .data_storage_format
                .lance_file_version()?,
        )
        .with_session(session)
        .execute(transaction)
        .await
}

fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidTableName {
            name: name.to_string(),
            reason: "Table names cannot be empty strings".to_string(),
        });
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
    {
        return Err(Error::InvalidTableName {
            name: name.to_string(),
            reason:
                "Table names can only contain alphanumeric characters, underscores, hyphens, and periods"
                    .to_string(),
        });
    }
    Ok(())
}

enum ConnectNamespaceBuilderInner {
    Pending {
        ns_impl: String,
        properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<Duration>,
        embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
        session: Option<Arc<Session>>,
        server_side_query: bool,
    },
}

/// Builder for namespace-backed connections. When `properties` contain a `curvine://` root, behavior
/// matches `connect` (URI query split, optional default session).
pub struct ConnectNamespaceBuilder {
    inner: ConnectNamespaceBuilderInner,
}

impl Debug for ConnectNamespaceBuilderInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => f
                .debug_struct("Pending")
                .field("ns_impl", ns_impl)
                .field("properties", properties)
                .field("storage_options", storage_options)
                .field("read_consistency_interval", read_consistency_interval)
                .field("embedding_registry", &embedding_registry.is_some())
                .field("session", &session.is_some())
                .field("server_side_query", server_side_query)
                .finish(),
        }
    }
}

impl Debug for ConnectNamespaceBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ConnectNamespaceBuilder")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ConnectNamespaceBuilder {
    fn new(ns_impl: &str, properties: HashMap<String, String>) -> Self {
        Self {
            inner: ConnectNamespaceBuilderInner::Pending {
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

    fn map_upstream(
        self,
        f: impl FnOnce(ConnectNamespaceBuilderInner) -> ConnectNamespaceBuilderInner,
    ) -> Self {
        Self {
            inner: f(self.inner),
        }
    }

    pub fn storage_option(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => {
                storage_options.insert(key.into(), value.into());
                ConnectNamespaceBuilderInner::Pending {
                    ns_impl,
                    properties,
                    storage_options,
                    read_consistency_interval,
                    embedding_registry,
                    session,
                    server_side_query,
                }
            }
        })
    }

    pub fn storage_options(
        self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                mut storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => {
                for (key, value) in pairs {
                    storage_options.insert(key.into(), value.into());
                }
                ConnectNamespaceBuilderInner::Pending {
                    ns_impl,
                    properties,
                    storage_options,
                    read_consistency_interval,
                    embedding_registry,
                    session,
                    server_side_query,
                }
            }
        })
    }

    pub fn read_consistency_interval(self, read_consistency_interval: Duration) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                embedding_registry,
                session,
                server_side_query,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval: Some(read_consistency_interval),
                embedding_registry,
                session,
                server_side_query,
            },
        })
    }

    pub fn embedding_registry(self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                session,
                server_side_query,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry: Some(registry),
                session,
                server_side_query,
            },
        })
    }

    pub fn session(self, session: Arc<Session>) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                server_side_query,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session: Some(session),
                server_side_query,
            },
        })
    }

    pub fn server_side_query(self, enabled: bool) -> Self {
        self.map_upstream(|inner| match inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                ..
            } => ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query: enabled,
            },
        })
    }

    pub async fn execute(self) -> Result<Connection> {
        match self.inner {
            ConnectNamespaceBuilderInner::Pending {
                ns_impl,
                properties,
                storage_options,
                read_consistency_interval,
                embedding_registry,
                session,
                server_side_query,
            } => {
                let curvine_root_raw = find_curvine_uri(&properties);
                let wants_curvine = curvine_root_raw.is_some();
                let mut properties_for_upstream = properties;
                for (key, value) in &storage_options {
                    properties_for_upstream
                        .entry(format!("storage.{key}"))
                        .or_insert_with(|| value.clone());
                }
                let mut builder = upstream_connect_namespace(&ns_impl, properties_for_upstream);

                let (listing_uri, listing_query) = match curvine_root_raw {
                    Some(raw) => (
                        normalize_listing_uri(&raw).unwrap_or_else(|| raw.clone()),
                        listing_query_string(&raw),
                    ),
                    None => (String::new(), None),
                };

                let mut options = ConnectionOptions {
                    uri: listing_uri,
                    query_string: listing_query,
                    session: session.clone(),
                    namespace_backed: true,
                    embedding_registry: embedding_registry.clone(),
                    ..Default::default()
                };

                for (key, value) in storage_options {
                    options.storage_options.insert(key.clone(), value.clone());
                    builder = builder.storage_option(key, value);
                }

                if let Some(read_consistency_interval) = read_consistency_interval {
                    builder = builder.read_consistency_interval(read_consistency_interval);
                }

                if let Some(embedding_registry) = embedding_registry {
                    builder = builder.embedding_registry(embedding_registry);
                }

                if let Some(session) = session {
                    builder = builder.session(session);
                } else if wants_curvine {
                    let session = curvine_session();
                    options.session = Some(session.clone());
                    builder = builder.session(session);
                }

                let upstream = builder
                    .server_side_query(server_side_query)
                    .execute()
                    .await?;
                let upstream = wrap_upstream_connection_for_curvine(upstream, &options);
                Ok(Connection { upstream, options })
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
    let t = uri.trim_start();
    match Url::parse(t) {
        Ok(u) => u.scheme().eq_ignore_ascii_case("curvine"),
        Err(_) => {
            const PREFIX: &[u8] = b"curvine://";
            let b = t.as_bytes();
            b.len() >= PREFIX.len() && b[..PREFIX.len()].eq_ignore_ascii_case(PREFIX)
        }
    }
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

fn normalize_listing_uri(uri: &str) -> Option<String> {
    let mut url = Url::parse(uri).ok()?;
    url.set_query(None);
    Some(url.to_string())
}

fn listing_query_string(uri: &str) -> Option<String> {
    Url::parse(uri)
        .ok()
        .and_then(|url| url.query().map(ToString::to_string))
}
