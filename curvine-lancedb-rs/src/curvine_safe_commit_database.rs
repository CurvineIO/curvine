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

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use lance::dataset::{ReadParams, WriteParams};
use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DescribeTableRequest, DropNamespaceRequest, DropNamespaceResponse,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};
use lance_table::io::commit::{CommitHandler, ConditionalPutCommitHandler};
use lancedb_upstream::arrow::arrow_schema::SchemaRef;
use lancedb_upstream::database::{
    CloneTableRequest, CreateTableMode, CreateTableRequest, Database, OpenTableRequest,
    ReadConsistency, TableNamesRequest,
};
use lancedb_upstream::error::{Error, Result};
use lancedb_upstream::table::BaseTable;

fn curvine_commit_handler() -> Arc<dyn CommitHandler> {
    Arc::new(ConditionalPutCommitHandler)
}

fn ensure_create_request_has_commit_handler(mut req: CreateTableRequest) -> CreateTableRequest {
    let wp = req
        .write_options
        .lance_write_params
        .get_or_insert_with(WriteParams::default);
    if wp.commit_handler.is_none() {
        wp.commit_handler = Some(curvine_commit_handler());
    }
    req
}

fn ensure_open_request_has_commit_handler(mut req: OpenTableRequest) -> OpenTableRequest {
    match &mut req.lance_read_params {
        Some(rp) => {
            if rp.commit_handler.is_none() {
                rp.commit_handler = Some(curvine_commit_handler());
            }
        }
        None => {
            let mut rp = ReadParams::default();
            if let Some(ics) = req.index_cache_size {
                #[allow(deprecated)]
                rp.index_cache_size(ics as usize);
            }
            rp.commit_handler = Some(curvine_commit_handler());
            req.lance_read_params = Some(rp);
        }
    }
    req
}

#[derive(Clone, Copy)]
pub(crate) enum SafeCommitScope {
    Listing,
    Namespace,
}

/// Wraps the upstream database. Before `create_table` / `open_table`, sets `ConditionalPutCommitHandler`
/// when the request did not set `commit_handler`.
pub(crate) struct CurvineSafeCommitDatabase {
    upstream: Arc<dyn Database>,
    scope: SafeCommitScope,
}

impl CurvineSafeCommitDatabase {
    pub(crate) fn new(upstream: Arc<dyn Database>) -> Self {
        Self {
            upstream,
            scope: SafeCommitScope::Listing,
        }
    }

    pub(crate) fn new_namespace(upstream: Arc<dyn Database>) -> Self {
        Self {
            upstream,
            scope: SafeCommitScope::Namespace,
        }
    }

    async fn open_existing_table_for_create(
        &self,
        name: String,
        namespace: Vec<String>,
        data_schema: SchemaRef,
        callback: impl FnOnce(OpenTableRequest) -> OpenTableRequest,
    ) -> Result<Arc<dyn BaseTable>> {
        let request = callback(OpenTableRequest {
            name,
            namespace,
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        });
        let table = self.open_table(request).await?;
        let table_schema = table.schema().await?;
        if table_schema.as_ref() != data_schema.as_ref() {
            return Err(Error::Schema {
                message: "Provided schema does not match existing table schema".to_string(),
            });
        }
        Ok(table)
    }

    async fn namespace_uses_managed_versioning(&self, req: &OpenTableRequest) -> Result<bool> {
        let namespace = self.upstream.namespace_client().await?;
        let mut table_id = req.namespace.clone();
        table_id.push(req.name.clone());
        let response = namespace
            .describe_table(DescribeTableRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
            .map_err(|source| Error::Runtime {
                message: format!("Failed to describe namespace table: {source}"),
            })?;
        Ok(response.managed_versioning == Some(true))
    }
}

impl fmt::Debug for CurvineSafeCommitDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.upstream.as_ref(), f)
    }
}

impl fmt::Display for CurvineSafeCommitDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.upstream.as_ref(), f)
    }
}

#[async_trait]
impl Database for CurvineSafeCommitDatabase {
    fn uri(&self) -> &str {
        self.upstream.uri()
    }

    async fn read_consistency(&self) -> Result<ReadConsistency> {
        self.upstream.read_consistency().await
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        self.upstream.list_namespaces(request).await
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        self.upstream.create_namespace(request).await
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        self.upstream.drop_namespace(request).await
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        self.upstream.describe_namespace(request).await
    }

    #[allow(deprecated)]
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        self.upstream.table_names(request).await
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.upstream.list_tables(request).await
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let CreateTableRequest {
            name,
            namespace,
            data,
            mode,
            write_options,
            location,
            namespace_client,
        } = request;

        if let CreateTableMode::ExistOk(callback) = mode {
            let data_schema = data.schema();
            let open_result = self
                .open_existing_table_for_create(
                    name.clone(),
                    namespace.clone(),
                    data_schema,
                    callback,
                )
                .await;
            return match open_result {
                Ok(table) => Ok(table),
                Err(Error::TableNotFound { .. }) => {
                    let request = CreateTableRequest {
                        name,
                        namespace,
                        data,
                        mode: CreateTableMode::Create,
                        write_options,
                        location,
                        namespace_client,
                    };
                    self.upstream
                        .create_table(ensure_create_request_has_commit_handler(request))
                        .await
                }
                Err(err) => Err(err),
            };
        }

        let request = CreateTableRequest {
            name,
            namespace,
            data,
            mode,
            write_options,
            location,
            namespace_client,
        };
        let request = ensure_create_request_has_commit_handler(request);
        self.upstream.create_table(request).await
    }

    async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        self.upstream.clone_table(request).await
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let request = match self.scope {
            SafeCommitScope::Listing => ensure_open_request_has_commit_handler(request),
            SafeCommitScope::Namespace => {
                if self.namespace_uses_managed_versioning(&request).await? {
                    request
                } else {
                    ensure_open_request_has_commit_handler(request)
                }
            }
        };
        self.upstream.open_table(request).await
    }

    async fn rename_table(
        &self,
        cur_name: &str,
        new_name: &str,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        self.upstream
            .rename_table(cur_name, new_name, cur_namespace, new_namespace)
            .await
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        self.upstream.drop_table(name, namespace).await
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        self.upstream.drop_all_tables(namespace).await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.upstream.as_any()
    }

    async fn namespace_client(&self) -> Result<Arc<dyn lance_namespace::LanceNamespace>> {
        self.upstream.namespace_client().await
    }
}
