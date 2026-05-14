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
use std::env;
use std::sync::Arc;

use curvine_common::conf::ClusterConf;
use lancedb::connect;
use lancedb::connect_namespace;
use lancedb::error::Error as LanceDbError;
use lancedb::object_store::{curvine_registry, curvine_session, CurvineObjectStoreProvider};
use lancedb::{ObjectStoreProvider, ObjectStoreRegistry, Session};
use tokio::sync::Mutex;
use url::Url;

static ENV_MUTEX: Mutex<()> = Mutex::const_new(());

#[test]
fn error_module_reexports_upstream_error_types() {
    let err = LanceDbError::NotSupported {
        message: "example".to_string(),
    };

    assert_eq!(err.to_string(), "LanceDBError: not supported: example");
}

#[test]
fn curvine_registry_registers_curvine_scheme() {
    let registry = curvine_registry();
    assert!(registry.get_provider("curvine").is_some());
}

#[test]
fn curvine_session_uses_registry_with_curvine_scheme() {
    let session = curvine_session();
    assert!(session.store_registry().get_provider("curvine").is_some());
}

#[test]
fn curvine_provider_extracts_lance_relative_base_path() {
    let provider = CurvineObjectStoreProvider::new();

    let path = provider
        .extract_path(&Url::parse("curvine:///tmp/lancedb/demo/table.lance").unwrap())
        .unwrap();
    assert_eq!(path.as_ref(), "tmp/lancedb/demo/table.lance");

    let path = provider
        .extract_path(&Url::parse("curvine://tenant/data/db").unwrap())
        .unwrap();
    assert_eq!(path.as_ref(), "tenant/data/db");
}

#[tokio::test]
async fn local_connect_passes_through_to_upstream() {
    let tmpdir = tempfile::tempdir().unwrap();

    let conn = connect(tmpdir.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();

    assert_eq!(conn.uri(), tmpdir.path().to_str().unwrap());
}

#[tokio::test]
async fn namespace_connect_stays_compatible() {
    let tmpdir = tempfile::tempdir().unwrap();
    let mut properties = HashMap::new();
    properties.insert(
        "root".to_string(),
        tmpdir.path().to_str().unwrap().to_string(),
    );

    let conn = connect_namespace("dir", properties)
        .execute()
        .await
        .unwrap();

    let names = conn.table_names().execute().await.unwrap();
    assert!(names.is_empty());
}

#[tokio::test]
async fn namespace_connect_clone_table_delegates_to_upstream_not_supported() {
    let tmpdir = tempfile::tempdir().unwrap();
    let mut properties = HashMap::new();
    properties.insert(
        "root".to_string(),
        tmpdir.path().to_str().unwrap().to_string(),
    );

    let conn = connect_namespace("dir", properties)
        .execute()
        .await
        .unwrap();
    let result = conn
        .clone_table("clone_t", "file:///tmp/source.lance")
        .execute()
        .await;

    assert!(
        matches!(result, Err(LanceDbError::NotSupported { .. })),
        "namespace connections must preserve upstream clone_table unsupported semantics, got {result:?}"
    );
}

#[tokio::test]
async fn facade_boundary_curvine_connect_fails_without_config() {
    let _guard = ENV_MUTEX.lock().await;
    let saved = env::var(ClusterConf::ENV_CONF_FILE).ok();
    env::remove_var(ClusterConf::ENV_CONF_FILE);

    let result = connect("curvine:///data/lancedb/demo").execute().await;

    if let Some(val) = saved {
        env::set_var(ClusterConf::ENV_CONF_FILE, val);
    }

    let err = match result {
        Ok(_) => panic!("expected connect to fail without Curvine configuration"),
        Err(err) => err,
    };

    let rendered = err.to_string();
    assert!(
        rendered.contains("Missing Curvine cluster configuration"),
        "unexpected error message: {rendered}"
    );
    assert!(rendered.contains("CURVINE_CONF_FILE"));
}

#[tokio::test]
async fn facade_boundary_curvine_namespace_connect_fails_without_config() {
    let _guard = ENV_MUTEX.lock().await;
    let saved = env::var(ClusterConf::ENV_CONF_FILE).ok();
    env::remove_var(ClusterConf::ENV_CONF_FILE);

    let mut properties = HashMap::new();
    properties.insert(
        "root".to_string(),
        "curvine:///data/lancedb/demo".to_string(),
    );

    let result = connect_namespace("dir", properties).execute().await;

    if let Some(val) = saved {
        env::set_var(ClusterConf::ENV_CONF_FILE, val);
    }

    let err = match result {
        Ok(_) => panic!("expected namespace connect to fail without Curvine configuration"),
        Err(err) => err,
    };

    let rendered = err.to_string();
    assert!(
        rendered.contains("Missing Curvine cluster configuration"),
        "unexpected error message: {rendered}"
    );
    assert!(rendered.contains("CURVINE_CONF_FILE"));
}

#[tokio::test]
async fn curvine_namespace_connect_preserves_explicit_session() {
    let _guard = ENV_MUTEX.lock().await;
    let saved = env::var(ClusterConf::ENV_CONF_FILE).ok();
    env::remove_var(ClusterConf::ENV_CONF_FILE);

    let custom = Arc::new(Session::new(0, 0, Arc::new(ObjectStoreRegistry::empty())));
    let mut properties = HashMap::new();
    properties.insert(
        "root".to_string(),
        "curvine:///data/lancedb/demo".to_string(),
    );

    let result = connect_namespace("dir", properties)
        .session(custom)
        .execute()
        .await;

    if let Some(val) = saved {
        env::set_var(ClusterConf::ENV_CONF_FILE, val);
    }

    let err = match result {
        Ok(_) => panic!("expected namespace connect to fail with custom empty registry"),
        Err(err) => err,
    };

    let rendered = err.to_string();
    assert!(
        rendered.contains("No object store provider found for scheme: 'curvine'"),
        "custom session should be preserved; unexpected error message: {rendered}"
    );
    assert!(
        !rendered.contains("Missing Curvine cluster configuration"),
        "fallback curvine_session unexpectedly replaced the explicit session: {rendered}"
    );
}
