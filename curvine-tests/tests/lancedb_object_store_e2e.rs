// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use curvine_tests::Testing;
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::query::ExecutableQuery;
use orpc::{CommonError, CommonResult};

#[test]
fn lancedb_on_curvine_minicluster_e2e() -> CommonResult<()> {
    let testing = Testing::builder().default().workers(3).build()?;
    let _cluster = testing.start_cluster()?;
    let conf_path = testing.active_conf_path().to_string();

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table_name = format!("lancedb_e2e_{unique}");
    let db_uri = format!("curvine:///tmp/{table_name}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result: Result<(), CommonError> = rt.block_on(async move {
        std::env::set_var(curvine_common::conf::ClusterConf::ENV_CONF_FILE, &conf_path);

        let conn = connect(&db_uri)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![10_i32, 20, 30]))],
        )
        .unwrap();

        conn.create_table(&table_name, batch)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&table_name));

        let table = conn
            .open_table(&table_name)
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
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3);

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

        conn.drop_table(&table_name, &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let names_after = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!names_after.contains(&table_name));

        let reopen = conn.open_table(&table_name).execute().await;
        assert!(
            reopen.is_err(),
            "open_table should fail after drop_table, got {reopen:?}"
        );

        Ok(())
    });
    result?;

    Ok(())
}
