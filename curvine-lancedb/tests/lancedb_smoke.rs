// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Phase 4B — live-cluster LanceDB smoke on `curvine://` (connect → table lifecycle → query).
//!
//! ```text
//! CURVINE_CONF_FILE=/path/to/cluster.toml \
//!   cargo test -p curvine-lancedb --test lancedb_smoke -- --ignored
//!
//! CURVINE_MASTER_ADDRS=host1:8995,host2:8995 \
//!   cargo test -p curvine-lancedb --test lancedb_smoke -- --ignored
//! ```

use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use curvine_common::conf::ClusterConf;
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::object_store::{CURVINE_CONF_FILE_KEY, CURVINE_MASTER_ADDRS_KEY};
use lancedb::query::ExecutableQuery;

#[tokio::test]
#[ignore = "live Curvine cluster + CURVINE_CONF_FILE; cargo test -p curvine-lancedb --test lancedb_smoke -- --ignored"]
async fn lancedb_curvine_smoke_connect_table_query_names() {
    let storage_option = match env::var("CURVINE_MASTER_ADDRS") {
        Ok(v) => (CURVINE_MASTER_ADDRS_KEY, v),
        Err(_) => match env::var(ClusterConf::ENV_CONF_FILE) {
            Ok(v) => (CURVINE_CONF_FILE_KEY, v),
            Err(_) => {
                eprintln!(
                    "Skipping live LanceDB smoke test: neither CURVINE_MASTER_ADDRS nor CURVINE_CONF_FILE is set"
                );
                return;
            }
        },
    };

    if storage_option.1.trim().is_empty() {
        eprintln!(
            "Skipping live LanceDB smoke test: {} is empty",
            storage_option.0
        );
        return;
    }

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table_name = format!("lancedb_smoke_{unique}");
    let db_uri = format!("curvine:///tmp/{table_name}");

    let conn = connect(&db_uri)
        .storage_option(storage_option.0, storage_option.1.clone())
        .execute()
        .await
        .expect("connect curvine://");

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10_i32, 20, 30]))],
    )
    .expect("record batch");

    conn.create_table(&table_name, batch)
        .storage_option(storage_option.0, storage_option.1.clone())
        .execute()
        .await
        .expect("create_table");

    let names = conn.table_names().execute().await.expect("table_names");
    assert!(
        names.contains(&table_name),
        "table_names must include {table_name}, got {names:?}"
    );

    let table = conn
        .open_table(&table_name)
        .execute()
        .await
        .expect("open_table");

    let n = table.count_rows(None).await.expect("count_rows");
    assert_eq!(n, 3, "count_rows");

    let stream = table.query().execute().await.expect("query execute");
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("query batches");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "query row count");
}
