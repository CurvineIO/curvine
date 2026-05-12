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

#[test]
fn error_module_reexports_upstream_error_types() {
    let err = lancedb::error::Error::NotSupported {
        message: "example".to_string(),
    };

    assert_eq!(err.to_string(), "LanceDBError: not supported: example");
}

#[test]
fn curvine_registry_registers_curvine_scheme() {
    let registry = lancedb::object_store::curvine_registry();
    assert!(registry.get_provider("curvine").is_some());
}

#[test]
fn curvine_session_uses_registry_with_curvine_scheme() {
    let session = lancedb::object_store::curvine_session();
    assert!(session.store_registry().get_provider("curvine").is_some());
}

#[tokio::test]
async fn local_connect_passes_through_to_upstream() {
    let tmpdir = tempfile::tempdir().unwrap();

    let conn = lancedb::connect(tmpdir.path().to_str().unwrap())
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

    let conn = lancedb::connect_namespace("dir", properties)
        .execute()
        .await
        .unwrap();

    let names = conn.table_names().execute().await.unwrap();
    assert!(names.is_empty());
}

#[tokio::test]
async fn curvine_uri_reports_explicit_unsupported_boundary() {
    let err = match lancedb::connect("curvine:///data/lancedb/demo")
        .execute()
        .await
    {
        Ok(_) => panic!("curvine:// should be an explicit unsupported boundary"),
        Err(err) => err,
    };

    let rendered = err.to_string();
    assert!(rendered.contains("curvine://"));
    assert!(rendered.contains("not implemented"));
}

#[tokio::test]
async fn curvine_namespace_uri_reports_explicit_unsupported_boundary() {
    let mut properties = HashMap::new();
    properties.insert(
        "root".to_string(),
        "curvine:///data/lancedb/demo".to_string(),
    );

    let err = match lancedb::connect_namespace("dir", properties)
        .execute()
        .await
    {
        Ok(_) => panic!("curvine namespace uri should be an explicit unsupported boundary"),
        Err(err) => err,
    };

    let rendered = err.to_string();
    assert!(rendered.contains("curvine://"));
    assert!(rendered.contains("not implemented"));
}
