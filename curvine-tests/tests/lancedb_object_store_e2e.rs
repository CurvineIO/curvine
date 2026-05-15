// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//! Phase 6 — LanceDB on Curvine E2E (ListingDatabase + `curvine://` object store).
//! 覆盖常用路径与最小向量索引路径；各用例使用独立 `curvine:///tmp/...` workspace，并通过
//! `storage_option(CURVINE_CONF_FILE_KEY, …)` 注入配置，避免依赖进程级环境变量。

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, UInt64Type};
use arrow_array::Array;
use arrow_array::{
    BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use common::{row_count, start_minicluster, unique_ns};
use curvine_common::conf::ClusterConf;
use futures::{stream::FuturesUnordered, TryStreamExt};
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use lancedb::connect;
use lancedb::database::{CreateTableMode, ReadConsistency};
use lancedb::error::Error as LanceDbError;
use lancedb::expr::{col, lit};
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::{Index, IndexType};
use lancedb::object_store::{CurvineObjectStoreProvider, CURVINE_CONF_FILE_KEY};
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::table::{AddDataMode, ColumnAlteration, NewColumnTransform, OptimizeAction};
use lancedb::{DistanceType, ObjectStoreProvider};
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, UpdateVersion};
use orpc::{CommonError, CommonResult};
use std::env;
use url::Url;

static ENV_MUTATION_LOCK: Mutex<()> = Mutex::new(());

fn int32_values(batch: &RecordBatch, column: &str) -> Vec<Option<i32>> {
    let array = batch[column].as_primitive::<Int32Type>();
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(array.value(i))
            }
        })
        .collect()
}

fn float32_values(batch: &RecordBatch, column: &str) -> Vec<f32> {
    let array = batch[column].as_primitive::<Float32Type>();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn float64_values(batch: &RecordBatch, column: &str) -> Vec<f64> {
    let array = batch[column].as_primitive::<Float64Type>();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn bool_values(batch: &RecordBatch, column: &str) -> Vec<bool> {
    let array = batch[column].as_boolean();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn string_values(batch: &RecordBatch, column: &str) -> Vec<String> {
    let array = batch[column].as_string::<i32>();
    (0..array.len())
        .map(|i| array.value(i).to_string())
        .collect()
}

fn uint64_values(batch: &RecordBatch, column: &str) -> Vec<u64> {
    let array = batch[column].as_primitive::<UInt64Type>();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn id_age_batch(offset: i32, age: i32, rows: i32) -> Box<dyn RecordBatchReader + Send> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(offset..(offset + rows))),
            Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
                age,
                rows as usize,
            ))),
        ],
    )
    .unwrap();
    Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

fn int32_batch(column: &str, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::Int32,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
}

fn collect_id_age_rows(batches: &[RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = int32_values(batch, "id");
        let ages = int32_values(batch, "age");
        for row in 0..batch.num_rows() {
            rows.push((
                ids[row].expect("id is non-nullable"),
                ages[row].expect("age is non-nullable"),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

#[test]
fn lancedb_on_curvine_minicluster_smoke_extended() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/lancedb_smoke_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![10_i32, 20, 30]))],
        )
        .unwrap();

        conn.create_table("smoke_tbl", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"smoke_tbl".to_string()));

        let table = conn
            .open_table("smoke_tbl")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        let stream = table
            .query()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 3);

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![40_i32, 50, 60]))],
        )
        .unwrap();
        table
            .add(batch)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            6
        );

        conn.drop_table("smoke_tbl", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let names_after = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!names_after.contains(&"smoke_tbl".to_string()));

        let reopen = conn
            .open_table("smoke_tbl")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await;
        assert!(reopen.is_err());

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_table_lifecycle_create_list_open_drop_errors() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/lifecycle_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        conn.create_table("life_t", batch.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let dup = conn
            .create_table("life_t", batch.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await;
        assert!(
            matches!(dup, Err(LanceDbError::TableAlreadyExists { .. })),
            "expected TableAlreadyExists, got {dup:?}"
        );

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"life_t".to_string()));

        let open_bad = conn
            .open_table("no_such_table")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await;
        assert!(
            matches!(open_bad, Err(LanceDbError::TableNotFound { .. })),
            "expected TableNotFound, got {open_bad:?}"
        );

        let drop_bad = conn.drop_table("no_such_table", &[]).await;
        assert!(
            drop_bad.is_ok() || matches!(drop_bad, Err(LanceDbError::TableNotFound { .. })),
            "drop missing table: expected Ok (idempotent) or TableNotFound, got {drop_bad:?}"
        );

        conn.drop_table("life_t", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        conn.create_table("life_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let t = conn
            .open_table("life_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            t.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_table_names_create_modes_and_drop_all_tables() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/table_modes_{ns}");
        let conn = connect(&db_uri)
            .read_consistency_interval(Duration::from_secs(0))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(matches!(
            conn.read_consistency()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            ReadConsistency::Strong
        ));

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        for name in ["tbl_a", "tbl_b", "tbl_c", "tbl_d"] {
            conn.create_empty_table(name, schema.clone())
                .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
                .execute()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?;
        }

        assert_eq!(
            conn.table_names()
                .limit(2)
                .execute()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            vec!["tbl_a".to_string(), "tbl_b".to_string()]
        );
        assert_eq!(
            conn.table_names()
                .start_after("tbl_b")
                .limit(2)
                .execute()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            vec!["tbl_c".to_string(), "tbl_d".to_string()]
        );

        let existing = conn
            .create_empty_table("tbl_a", schema.clone())
            .mode(CreateTableMode::exist_ok(|request| request))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            existing
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            0
        );

        conn.create_table("tbl_b", int32_batch("x", vec![9, 10]))
            .mode(CreateTableMode::Overwrite)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let overwritten = conn
            .open_table("tbl_b")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            overwritten
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        conn.drop_all_tables(&[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .is_empty());
        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(reopened
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .is_empty());

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_clone_table_shallow_clone_roundtrip() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/clone_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty = conn
            .create_empty_table("empty_t", schema.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            empty
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            0
        );
        empty
            .add(int32_batch("id", vec![1, 2, 3]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            empty
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        let source = conn
            .create_table("source_t", int32_batch("id", vec![10, 20, 30]))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let source_uri = source
            .uri()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let cloned = conn
            .clone_table("clone_t", source_uri)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            cloned
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );
        cloned
            .add(int32_batch("id", vec![40]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            cloned
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            4
        );
        assert_eq!(
            source
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"empty_t".to_string()));
        assert!(names.contains(&"source_t".to_string()));
        assert!(names.contains(&"clone_t".to_string()));

        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let reopened_clone = reopened
            .open_table("clone_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            reopened_clone
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            4
        );
        let reopened_source = reopened
            .open_table("source_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            reopened_source
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_clone_table_shallow_clone_source_refs() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/clone_refs_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let source = conn
            .create_table("source_t", int32_batch("id", vec![1, 2]))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let v1 = source
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        source
            .add(int32_batch("id", vec![3, 4]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let v2 = source
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        {
            let mut tags = source
                .tags()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?;
            tags.create("v1", v1)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?;
        }
        source
            .add(int32_batch("id", vec![5, 6]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            source
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            6
        );

        let source_uri = source
            .uri()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let cloned_v1 = conn
            .clone_table("clone_v1", source_uri.clone())
            .source_version(v1)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            cloned_v1
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        let cloned_tag = conn
            .clone_table("clone_tag", source_uri.clone())
            .source_tag("v1")
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            cloned_tag
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        let cloned_v2 = conn
            .clone_table("clone_v2", source_uri.clone())
            .source_version(v2)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            cloned_v2
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            4
        );

        let both_ref_err = conn
            .clone_table("clone_both_ref", source_uri)
            .source_version(v1)
            .source_tag("v1")
            .execute()
            .await;
        assert!(
            matches!(both_ref_err, Err(LanceDbError::InvalidInput { .. })),
            "clone_table with source_version and source_tag should fail before writing target, got {both_ref_err:?}"
        );
        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!names.contains(&"clone_both_ref".to_string()));

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_listing_database_unsupported_clone_and_rename_semantics() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/unsupported_listing_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let table = conn
            .create_table("source_t", int32_batch("id", vec![1, 2]))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let source_uri = table
            .uri()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let deep_clone = conn
            .clone_table("deep_clone_t", source_uri.clone())
            .is_shallow(false)
            .execute()
            .await;
        assert!(
            matches!(deep_clone, Err(LanceDbError::NotSupported { .. })),
            "deep clone should be explicitly unsupported by upstream ListingDatabase, got {deep_clone:?}"
        );

        let namespace_clone = conn
            .clone_table("namespace_clone_t", source_uri)
            .target_namespace(vec!["ns".to_string()])
            .execute()
            .await;
        assert!(
            matches!(namespace_clone, Err(LanceDbError::NotSupported { .. })),
            "clone into namespace should be explicitly unsupported by upstream ListingDatabase, got {namespace_clone:?}"
        );

        let rename = conn.rename_table("source_t", "renamed_t", &[], &[]).await;
        assert!(
            matches!(rename, Err(LanceDbError::NotSupported { .. })),
            "rename_table should be explicitly unsupported by LanceDB OSS ListingDatabase, got {rename:?}"
        );
        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"source_t".to_string()));
        assert!(!names.contains(&"deep_clone_t".to_string()));
        assert!(!names.contains(&"namespace_clone_t".to_string()));
        assert!(!names.contains(&"renamed_t".to_string()));

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_write_append_multiple_batches_and_schema_mismatch() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/write_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
            .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![3, 4, 5]))],
        )
        .unwrap();

        let table = conn
            .create_table("w_t", b1)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        table
            .add(b2)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            5
        );

        let bad_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let bad_batch =
            RecordBatch::try_new(bad_schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
                .unwrap();
        let bad_add = table.add(bad_batch).execute().await;
        assert!(
            bad_add.is_err(),
            "schema mismatch add should fail: {bad_add:?}"
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_write_empty_initial_table_has_zero_rows() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/empty_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();
        let r = conn
            .create_table("empty_t", empty)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            r.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            0
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_add_overwrite_replaces_existing_rows() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/overwrite_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = conn
            .create_table("overwrite_t", initial)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let replacement =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![9]))]).unwrap();
        table
            .add(replacement)
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );
        let batches: Vec<RecordBatch> = table
            .query()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        assert_eq!(ids, vec![9]);

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_update_delete_and_merge_insert_semantics() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/mutation_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let table = conn
            .create_table("mutation_t", id_age_batch(0, 0, 10))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let update = table
            .update()
            .only_if("id < 3")
            .column("age", "age + 10")
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(update.rows_updated, 3);
        assert!(update.version > 0);
        assert_eq!(
            table
                .count_rows(Some("age = 10".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        let delete = table
            .delete("id >= 8")
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(delete.num_deleted_rows, 2);
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            8
        );

        let mut insert_only = table.merge_insert(&["id"]);
        insert_only.when_not_matched_insert_all();
        let inserted = insert_only
            .execute(id_age_batch(5, 20, 10))
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(inserted.num_inserted_rows, 7);
        assert_eq!(inserted.num_updated_rows, 0);
        assert_eq!(inserted.num_deleted_rows, 0);
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            15
        );

        let mut update_only = table.merge_insert(&["id"]);
        update_only.when_matched_update_all(Some("target.age = 0".to_string()));
        let updated = update_only
            .execute(id_age_batch(3, 30, 6))
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(updated.num_updated_rows, 5);
        assert_eq!(updated.num_inserted_rows, 0);
        assert_eq!(
            table
                .count_rows(Some("age = 30".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            5
        );

        let mut replace_subset = table.merge_insert(&["id"]);
        replace_subset
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(Some("id >= 10".to_string()));
        let replaced = replace_subset
            .execute(id_age_batch(10, 40, 2))
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(replaced.num_updated_rows, 2);
        assert_eq!(replaced.num_deleted_rows, 3);
        assert_eq!(
            table
                .count_rows(Some("id >= 10".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );
        assert_eq!(
            table
                .count_rows(Some("age = 40".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );
        let batches: Vec<RecordBatch> = table
            .query()
            .select(Select::columns(&["id", "age"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let rows = collect_id_age_rows(&batches);
        assert_eq!(rows.len(), 12);
        assert!(rows.contains(&(0, 10)));
        assert!(rows.contains(&(3, 30)));
        assert!(rows.contains(&(10, 40)));
        assert!(rows.contains(&(11, 40)));
        assert!(!rows.iter().any(|(id, _)| *id >= 12));

        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("mutation_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            reopened
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            12
        );
        assert_eq!(
            reopened
                .count_rows(Some("age = 40".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn curvine_object_store_put_update_conditional_etag_semantics() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let mut opts = HashMap::new();
        opts.insert(CURVINE_CONF_FILE_KEY.to_string(), conf);
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                opts,
            ))),
            ..Default::default()
        };
        let provider = CurvineObjectStoreProvider::new();
        let store = provider
            .new_store(
                Url::parse(&format!("curvine:///tmp/put_update_{ns}")).unwrap(),
                &params,
            )
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let key = ObjectPath::parse(format!("obj_{ns}.bin")).unwrap();
        let created = store
            .inner
            .put_opts(
                &key,
                Vec::from(&b"v1"[..]).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(created.e_tag.is_some());

        let updated = store
            .inner
            .put_opts(
                &key,
                Vec::from(&b"v2"[..]).into(),
                PutOptions {
                    mode: PutMode::Update(created.clone().into()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_ne!(updated.e_tag, created.e_tag);
        assert_eq!(
            store
                .inner
                .get(&key)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                .bytes()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                .as_ref(),
            b"v2"
        );

        let stale = store
            .inner
            .put_opts(
                &key,
                Vec::from(&b"stale"[..]).into(),
                PutOptions {
                    mode: PutMode::Update(created.into()),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(stale, Err(ObjectStoreError::Precondition { .. })),
            "stale PutMode::Update must not overwrite the current object, got {stale:?}"
        );
        let missing = store
            .inner
            .put_opts(
                &ObjectPath::parse(format!("missing_{ns}.bin")).unwrap(),
                Vec::from(&b"missing"[..]).into(),
                PutOptions {
                    mode: PutMode::Update(updated.clone().into()),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(missing, Err(ObjectStoreError::Precondition { .. })),
            "conditional update of a missing object must report Precondition, got {missing:?}"
        );
        let version_only = store
            .inner
            .put_opts(
                &key,
                Vec::from(&b"version-only"[..]).into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: Some("1".to_string()),
                    }),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(version_only, Err(ObjectStoreError::Generic { .. })),
            "Curvine exposes e_tag but no object version, got {version_only:?}"
        );
        assert_eq!(
            store
                .inner
                .get(&key)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                .bytes()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                .as_ref(),
            b"v2"
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn curvine_object_store_put_update_concurrent_retries_preserve_all_writes() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        const WORKERS: usize = 5;
        const INCREMENTS: usize = 10;

        let mut opts = HashMap::new();
        opts.insert(CURVINE_CONF_FILE_KEY.to_string(), conf);
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                opts,
            ))),
            ..Default::default()
        };
        let provider = CurvineObjectStoreProvider::new();
        let store = provider
            .new_store(
                Url::parse(&format!("curvine:///tmp/put_update_race_{ns}")).unwrap(),
                &params,
            )
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .inner;
        let key = ObjectPath::parse(format!("counter_{ns}.txt")).unwrap();

        let mut tasks = (0..WORKERS)
            .map(|_| {
                let store = Arc::clone(&store);
                let key = key.clone();
                async move {
                    for _ in 0..INCREMENTS {
                        loop {
                            match store.get(&key).await {
                                Ok(result) => {
                                    let version = UpdateVersion {
                                        e_tag: result.meta.e_tag.clone(),
                                        version: result.meta.version.clone(),
                                    };
                                    let bytes = result.bytes().await?;
                                    let value = std::str::from_utf8(bytes.as_ref())
                                        .map_err(|source| ObjectStoreError::Generic {
                                            store: "curvine-test",
                                            source: Box::new(source),
                                        })?
                                        .parse::<usize>()
                                        .map_err(|source| ObjectStoreError::Generic {
                                            store: "curvine-test",
                                            source: Box::new(source),
                                        })?;
                                    let next = (value + 1).to_string();
                                    match store
                                        .put_opts(
                                            &key,
                                            next.into_bytes().into(),
                                            PutOptions {
                                                mode: PutMode::Update(version),
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                    {
                                        Ok(_) => break,
                                        Err(ObjectStoreError::Precondition { .. }) => continue,
                                        Err(err) => return Err(err),
                                    }
                                }
                                Err(ObjectStoreError::NotFound { .. }) => {
                                    match store
                                        .put_opts(
                                            &key,
                                            Vec::from(&b"1"[..]).into(),
                                            PutOptions {
                                                mode: PutMode::Create,
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                    {
                                        Ok(_) => break,
                                        Err(ObjectStoreError::AlreadyExists { .. }) => continue,
                                        Err(err) => return Err(err),
                                    }
                                }
                                Err(err) => return Err(err),
                            }
                        }
                    }
                    Ok::<(), ObjectStoreError>(())
                }
            })
            .collect::<FuturesUnordered<_>>();

        while tasks
            .try_next()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .is_some()
        {}

        let bytes = store
            .get(&key)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let final_value = std::str::from_utf8(bytes.as_ref())
            .map_err(|e| CommonError::from(e.to_string()))?
            .parse::<usize>()
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(final_value, WORKERS * INCREMENTS);

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_stale_table_handle_append_rebases_on_curvine() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/concurrent_append_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        conn.create_table("append_t", int32_batch("id", vec![0]))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let t1 = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("append_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let t2 = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("append_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let initial_v1 = t1
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let initial_v2 = t2
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(initial_v1, initial_v2);

        t1.add(int32_batch("id", vec![1]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        t2.add(int32_batch("id", vec![2]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("append_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            reopened
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );
        let batches: Vec<RecordBatch> = reopened
            .query()
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut ids = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1, 2]);

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_query_limit_select_filter() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/query_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("q_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let stream = table
            .query()
            .limit(2)
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 2);
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
        assert!(batches
            .iter()
            .all(|batch| batch.schema().field(0).name() == "id"));
        let limited_ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        assert_eq!(limited_ids.len(), 2);
        assert!(limited_ids.iter().all(|id| (1..=5).contains(id)));

        let stream = table
            .query()
            .only_if("score >= 30")
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 3);
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 4, 5]);

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_query_offset_dynamic_expr_and_plans() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/query_expr_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..6)),
                Arc::new(Int32Array::from(vec![0, 10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("query_expr_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let full_batches: Vec<RecordBatch> = table
            .query()
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let full_ids: Vec<i32> = full_batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        assert_eq!(full_ids.len(), 6);

        let batches: Vec<RecordBatch> = table
            .query()
            .offset(2)
            .limit(2)
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 2);
        let offset_ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        assert_eq!(offset_ids, full_ids[2..4]);

        let batches: Vec<RecordBatch> = table
            .query()
            .select(Select::dynamic(&[("double_score", "score * 2")]))
            .only_if("id >= 3")
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut doubled: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "double_score"))
            .flatten()
            .collect();
        doubled.sort_unstable();
        assert_eq!(doubled, vec![60, 80, 100]);

        let batches: Vec<RecordBatch> = table
            .query()
            .only_if_expr(col("score").gt(lit(20)))
            .select(Select::expr_projection(&[
                ("id", col("id")),
                ("score_plus_one", col("score") + lit(1)),
            ]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = int32_values(batch, "id");
            let scores = int32_values(batch, "score_plus_one");
            for row in 0..batch.num_rows() {
                rows.push((
                    ids[row].expect("id is non-nullable"),
                    scores[row].expect("score_plus_one is non-nullable"),
                ));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![(3, 31), (4, 41), (5, 51)]);

        let plan = table
            .query()
            .only_if("score >= 30")
            .explain_plan(true)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!plan.trim().is_empty(), "empty explain plan");
        let analyzed = table
            .query()
            .only_if("score >= 30")
            .analyze_plan()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!analyzed.trim().is_empty(), "empty analyzed plan");

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_take_offsets_row_ids_and_table_metadata() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/take_meta_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let table = conn
            .create_table("take_t", id_age_batch(0, 7, 6))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(table.name(), "take_t");
        assert_eq!(table.namespace(), &[] as &[String]);
        assert!(table.id().contains("take_t"));
        assert!(table
            .uri()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .contains("take_t"));
        let initial_options = table
            .initial_storage_options()
            .await
            .expect("storage options should be retained");
        assert_eq!(initial_options.get(CURVINE_CONF_FILE_KEY), Some(&conf));
        let latest_options = table
            .latest_storage_options()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .expect("latest storage options should be retained");
        assert_eq!(latest_options.get(CURVINE_CONF_FILE_KEY), Some(&conf));

        let offset_batches: Vec<RecordBatch> = table
            .take_offsets(vec![1, 4])
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut offset_ids: Vec<i32> = offset_batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        offset_ids.sort_unstable();
        assert_eq!(offset_ids, vec![1, 4]);

        let row_id_batches: Vec<RecordBatch> = table
            .query()
            .with_row_id()
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut target_row_ids = Vec::new();
        for batch in &row_id_batches {
            let ids = int32_values(batch, "id");
            let row_ids = uint64_values(batch, "_rowid");
            for row in 0..batch.num_rows() {
                if ids[row] == Some(2) || ids[row] == Some(5) {
                    target_row_ids.push(row_ids[row]);
                }
            }
        }
        assert_eq!(target_row_ids.len(), 2);

        let taken_by_row_id: Vec<RecordBatch> = table
            .take_row_ids(target_row_ids)
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut ids: Vec<i32> = taken_by_row_id
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 5]);

        let stats = table
            .stats()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(stats.num_rows, 6);
        assert!(stats.total_bytes > 0);
        assert_eq!(stats.num_indices, 0);

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_schema_mixed_types_roundtrip() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/mixed_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("opt_i", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float32Array::from(vec![1.5_f32, 2.5])),
                Arc::new(Float64Array::from(vec![10.1_f64, 20.2])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Int32Array::from(vec![Some(7), None])),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("mix_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let stream = table
            .query()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 2);
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = int32_values(batch, "id");
            let labels = string_values(batch, "label");
            let f32s = float32_values(batch, "f32");
            let f64s = float64_values(batch, "f64");
            let flags = bool_values(batch, "flag");
            let opt_is = int32_values(batch, "opt_i");
            for row in 0..batch.num_rows() {
                rows.push((
                    ids[row].expect("id is non-nullable"),
                    labels[row].clone(),
                    f32s[row],
                    f64s[row],
                    flags[row],
                    opt_is[row],
                ));
            }
        }
        rows.sort_by_key(|row| row.0);
        assert_eq!(
            rows,
            vec![
                (1, "a".to_string(), 1.5_f32, 10.1_f64, true, Some(7)),
                (2, "b".to_string(), 2.5_f32, 20.2_f64, false, None),
            ]
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_schema_evolution_and_versions_roundtrip() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/schema_version_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Int32, false),
            Field::new("drop_me", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(Int32Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("schema_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let v1 = table
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let add_columns = table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    "double_score".into(),
                    "score * 2".into(),
                )]),
                None,
            )
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(add_columns.version > v1);

        let alter_columns = table
            .alter_columns(&[ColumnAlteration::new("score".into()).rename("renamed_score".into())])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(alter_columns.version > add_columns.version);

        let drop_columns = table
            .drop_columns(&["drop_me"])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(drop_columns.version > alter_columns.version);

        let schema = table
            .schema()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(schema.field_with_name("renamed_score").is_ok());
        assert!(schema.field_with_name("double_score").is_ok());
        assert!(schema.field_with_name("drop_me").is_err());

        let batches: Vec<RecordBatch> = table
            .query()
            .select(Select::columns(&["id", "renamed_score", "double_score"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = int32_values(batch, "id");
            let scores = int32_values(batch, "renamed_score");
            let doubled = int32_values(batch, "double_score");
            for row in 0..batch.num_rows() {
                rows.push((
                    ids[row].expect("id is non-nullable"),
                    scores[row].expect("renamed_score is non-nullable"),
                    doubled[row].expect("double_score is non-nullable"),
                ));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![(1, 10, 20), (2, 20, 40), (3, 30, 60)]);

        let latest = table
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let versions = table
            .list_versions()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(versions.len() >= 4);
        table
            .checkout(v1)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );
        let old_schema = table
            .schema()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(old_schema.field_with_name("score").is_ok());
        assert!(old_schema.field_with_name("double_score").is_err());
        let checked_out_write = table.update().column("score", "score + 1").execute().await;
        assert!(
            checked_out_write.is_err(),
            "writes should fail while checked out to an old version"
        );

        table
            .checkout_latest()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .version()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            latest
        );
        table
            .checkout(v1)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        table
            .restore()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(
            table
                .version()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                > latest
        );
        let restored_schema = table
            .schema()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(restored_schema.field_with_name("score").is_ok());
        assert!(restored_schema.field_with_name("double_score").is_err());
        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("schema_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let reopened_schema = reopened
            .schema()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(reopened_schema.field_with_name("score").is_ok());
        assert!(reopened_schema.field_with_name("double_score").is_err());
        assert_eq!(
            reopened
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_tags_checkout_tag_and_reopen_persist() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/tags_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let table = conn
            .create_table("tag_t", int32_batch("id", vec![1]))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let v1 = table
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        table
            .add(int32_batch("id", vec![2]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let v2 = table
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let mut tags = table
            .tags()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(tags
            .list()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .is_empty());
        tags.create("initial", v1)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        tags.create("latest", v2)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            tags.get_version("initial")
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            v1
        );
        table
            .add(int32_batch("id", vec![3]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let v3 = table
            .version()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        tags.update("latest", v3)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        tags.delete("initial")
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let listed = tags
            .list()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed.get("latest").map(|tag| tag.version), Some(v3));
        drop(tags);

        table
            .checkout_tag("latest")
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .version()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            v3
        );
        table
            .checkout(v1)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );
        table
            .checkout_latest()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("tag_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let reopened_tags = reopened
            .tags()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            reopened_tags
                .get_version("latest")
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            v3
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_native_manifest_config_metadata_and_stats_persist() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/native_meta_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let table = conn
            .create_table("native_t", id_age_batch(0, 1, 5))
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        table
            .delete("id >= 3")
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let native = table.as_native().expect("curvine listing table is native");
        assert!(native
            .load_indices()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .is_empty());
        native
            .update_config(vec![
                ("curvine.phase".to_string(), "phase6".to_string()),
                ("curvine.keep".to_string(), "yes".to_string()),
            ])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        native
            .delete_config_keys(&["curvine.keep"])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let manifest = native
            .manifest()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            manifest.config.get("curvine.phase"),
            Some(&"phase6".to_string())
        );
        assert!(!manifest.config.contains_key("curvine.keep"));

        native
            .replace_schema_metadata(vec![("schema.owner".to_string(), "curvine".to_string())])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let field_id = native
            .manifest()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .schema
            .field("age")
            .ok_or_else(|| CommonError::from("missing age field"))?
            .id as u32;
        let mut field_metadata = HashMap::new();
        field_metadata.insert("field.role".to_string(), "metric".to_string());
        native
            .replace_field_metadata(vec![(field_id, field_metadata)])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("native_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let reopened_native = reopened.as_native().expect("reopened table is native");
        assert_eq!(
            reopened_native
                .count_deleted_rows()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );
        let reopened_manifest = reopened_native
            .manifest()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            reopened_manifest.config.get("curvine.phase"),
            Some(&"phase6".to_string())
        );
        assert_eq!(
            reopened_manifest.schema.metadata.get("schema.owner"),
            Some(&"curvine".to_string())
        );
        assert_eq!(
            reopened_manifest
                .schema
                .field("age")
                .ok_or_else(|| CommonError::from("missing reopened age field"))?
                .metadata
                .get("field.role"),
            Some(&"metric".to_string())
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_drop_recreate_and_listing_clean() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/droprec_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        conn.create_table("reuse_name", batch.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        conn.drop_table("reuse_name", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!names.contains(&"reuse_name".to_string()));

        conn.create_table("reuse_name", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let t = conn
            .open_table("reuse_name")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            t.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_two_tables_sequential_writes_isolation() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/iso_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let s1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let b1 =
            RecordBatch::try_new(s1.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let b2 =
            RecordBatch::try_new(s2.clone(), vec![Arc::new(Int32Array::from(vec![100]))]).unwrap();

        conn.create_table("t_a", b1)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        conn.create_table("t_b", b2)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let ta = conn
            .open_table("t_a")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let tb = conn
            .open_table("t_b")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let add_a = RecordBatch::try_new(s1, vec![Arc::new(Int32Array::from(vec![3]))]).unwrap();
        let add_b = RecordBatch::try_new(s2, vec![Arc::new(Int32Array::from(vec![200]))]).unwrap();
        ta.add(add_a)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        tb.add(add_b)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        assert_eq!(
            ta.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );
        assert_eq!(
            tb.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"t_a".to_string()) && names.contains(&"t_b".to_string()));

        conn.drop_table("t_a", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let tb_only = conn
            .open_table("t_b")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            tb_only
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_connect_curvine_without_conf_fails() -> CommonResult<()> {
    let _guard = ENV_MUTATION_LOCK.lock().expect("poisoned lock");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CommonError::from(e.to_string()))?;
    let ns = unique_ns();
    let key = ClusterConf::ENV_CONF_FILE;
    let saved = env::var(key).ok();
    let _restore = RestoreEnvVar::new(key, saved);

    env::remove_var(key);
    let err = rt.block_on(async move {
        connect(&format!("curvine:///tmp/noconf_{ns}"))
            .execute()
            .await
    });
    assert!(err.is_err(), "expected connect failure without conf");

    Ok(())
}

struct RestoreEnvVar {
    key: &'static str,
    prev: Option<String>,
}

impl RestoreEnvVar {
    fn new(key: &'static str, prev: Option<String>) -> Self {
        Self { key, prev }
    }
}

impl Drop for RestoreEnvVar {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => env::set_var(self.key, v),
            None => env::remove_var(self.key),
        }
    }
}

#[test]
fn lancedb_connect_unknown_scheme_fails() -> CommonResult<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CommonError::from(e.to_string()))?;
    let err = rt.block_on(async {
        connect("unknown-scheme-xyz://localhost/bucket/db")
            .execute()
            .await
    });
    assert!(
        err.is_err(),
        "expected failure for unknown object store scheme"
    );
    Ok(())
}

#[test]
fn lancedb_non_root_workspace_allows_dot_curvine_segment() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/lancedb_dot_curvine_{ns}/.curvine/user_ws");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("z", DataType::Int32, false)]));
        let name = format!("tbl_under_dot_curvine_{ns}");
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();

        conn.create_table(&name, batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&name));

        let t = conn
            .open_table(&name)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            t.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );

        conn.drop_table(&name, &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_scalar_and_fts_indices_persist_after_reopen() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/scalar_fts_idx_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let rows = 120;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..rows)),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("category_{}", row % 5)),
                )),
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row % 2 == 0)),
                )),
                Arc::new(StringArray::from_iter_values((0..rows).map(|row| {
                    match row % 3 {
                        0 => "cat",
                        1 => "dog",
                        _ => "fish",
                    }
                }))),
            ],
        )
        .map_err(|e| CommonError::from(e.to_string()))?;

        let table = conn
            .create_table("idx_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        table
            .create_index(&["id"], Index::Auto)
            .name("id_btree_idx".to_string())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        table
            .create_index(&["category"], Index::Bitmap(Default::default()))
            .name("category_bitmap_idx".to_string())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .name("text_fts_idx".to_string())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("idx_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let mut index_configs = reopened
            .list_indices()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        index_configs.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(index_configs.len(), 3);
        assert!(index_configs.iter().any(|index| {
            index.name == "id_btree_idx"
                && index.index_type == IndexType::BTree
                && index.columns == vec!["id".to_string()]
        }));
        assert!(index_configs.iter().any(|index| {
            index.name == "category_bitmap_idx"
                && index.index_type == IndexType::Bitmap
                && index.columns == vec!["category".to_string()]
        }));
        assert!(index_configs.iter().any(|index| {
            index.name == "text_fts_idx"
                && index.index_type == IndexType::FTS
                && index.columns == vec!["text".to_string()]
        }));

        for name in ["id_btree_idx", "category_bitmap_idx", "text_fts_idx"] {
            let stats = reopened
                .index_stats(name)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                .unwrap_or_else(|| panic!("missing index stats for {name}"));
            assert_eq!(stats.num_unindexed_rows, 0);
        }

        assert_eq!(
            reopened
                .count_rows(Some("id >= 10 AND id < 20".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            10
        );
        assert_eq!(
            reopened
                .count_rows(Some("category = 'category_3'".to_string()))
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            24
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_vector_column_search_create_index_and_query_again() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        const DIM: i32 = 8;
        const N: usize = 256;
        let item = Arc::new(Field::new("item", DataType::Float32, true));
        let mut flat: Vec<f32> = Vec::with_capacity(N * DIM as usize);
        for r in 0..N {
            for c in 0..DIM as usize {
                flat.push((r as f32) * 0.01 + (c as f32) * 0.001);
            }
        }
        let values = Float32Array::from(flat);
        let list = FixedSizeListArray::try_new(item.clone(), DIM, Arc::new(values), None)
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vec", DataType::FixedSizeList(item, DIM), false),
        ]));
        let ids = Int32Array::from_iter_values(0..N as i32);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(list)])
            .map_err(|e| CommonError::from(e.to_string()))?;

        let db_uri = format!("curvine:///tmp/vec_idx_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let table = conn
            .create_table("vec_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let qv: Vec<f32> = vec![0.0_f32, 0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007];
        let stream = table
            .vector_search(qv.clone())
            .map_err(|e| CommonError::from(e.to_string()))?
            .limit(10)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let before: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(
            row_count(&before) >= 1,
            "vector search before index should return rows"
        );
        assert_nearest_vector_result(&before)?;

        table
            .create_index(&["vec"], Index::IvfPq(IvfPqIndexBuilder::default()))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let index_configs = table
            .list_indices()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(index_configs.len(), 1);
        assert_eq!(index_configs[0].columns, vec!["vec".to_string()]);

        let stats = table
            .index_stats(&index_configs[0].name)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .expect("created vector index should have stats");
        assert_eq!(stats.distance_type, Some(DistanceType::L2));
        let table_stats = table
            .stats()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(table_stats.num_rows, N);
        assert_eq!(table_stats.num_indices, 1);

        let more_values =
            Float32Array::from_iter_values((0..DIM as usize).map(|c| 9.0_f32 + (c as f32) * 0.001));
        let more_list = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            DIM,
            Arc::new(more_values),
            None,
        )
        .map_err(|e| CommonError::from(e.to_string()))?;
        let more = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "vec",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        DIM,
                    ),
                    false,
                ),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![N as i32])),
                Arc::new(more_list),
            ],
        )
        .map_err(|e| CommonError::from(e.to_string()))?;
        table
            .add(more)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let unoptimized = table
            .index_stats(&index_configs[0].name)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .expect("index stats should remain available after append");
        assert!(unoptimized.num_unindexed_rows > 0);

        table
            .optimize(OptimizeAction::Index(Default::default()))
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let optimized = table
            .index_stats(&index_configs[0].name)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .expect("index stats should remain available after optimize");
        assert_eq!(optimized.num_unindexed_rows, 0);
        let reopened = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .open_table("vec_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let reopened_indices = reopened
            .list_indices()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(reopened_indices.iter().any(|index| {
            index.name == index_configs[0].name && index.columns == vec!["vec".to_string()]
        }));
        let reopened_stats = reopened
            .index_stats(&index_configs[0].name)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .expect("reopened table should see persisted index stats");
        assert_eq!(reopened_stats.num_unindexed_rows, 0);

        let stream = reopened
            .vector_search(qv.clone())
            .map_err(|e| CommonError::from(e.to_string()))?
            .limit(10)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let after: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(
            row_count(&after) >= 1,
            "vector search after index should return rows"
        );
        assert_nearest_vector_result(&after)?;

        table
            .drop_index(&index_configs[0].name)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .list_indices()
                .await
                .map_err(|e| CommonError::from(e.to_string()))?
                .len(),
            0
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

fn assert_nearest_vector_result(batches: &[RecordBatch]) -> CommonResult<()> {
    let first = batches
        .first()
        .ok_or_else(|| CommonError::from("vector search returned no batches"))?;
    assert_eq!(
        int32_values(first, "id").first().copied().flatten(),
        Some(0)
    );
    let distance = float32_values(first, "_distance")
        .first()
        .copied()
        .ok_or_else(|| CommonError::from("vector search returned no distance"))?;
    assert!(
        distance.abs() < 1.0e-6,
        "expected nearest vector distance close to zero, got {distance}"
    );
    Ok(())
}
