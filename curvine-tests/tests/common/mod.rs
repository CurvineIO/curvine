// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, path::Path};

use arrow_array::RecordBatch;
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use once_cell::sync::OnceCell;
use orpc::CommonResult;
use tokio::runtime::Runtime;

static MINICLUSTER: OnceCell<RunningMinicluster> = OnceCell::new();
static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);
const CURVINE_E2E_CONF_FILE: &str = "CURVINE_E2E_CONF_FILE";

pub fn unique_ns() -> u128 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    nanos.saturating_mul(1_000_000) + seq
}

pub struct RunningMinicluster {
    pub _testing: Option<Testing>,
    pub _cluster: Option<Arc<MiniCluster>>,
    pub conf_path: String,
}

fn minicluster() -> CommonResult<&'static RunningMinicluster> {
    MINICLUSTER.get_or_try_init(|| {
        if let Ok(conf_path) = env::var(CURVINE_E2E_CONF_FILE) {
            if !Path::new(&conf_path).is_file() {
                return Err(orpc::CommonError::from(format!(
                    "{CURVINE_E2E_CONF_FILE} must point to a Curvine cluster config file: {conf_path}"
                )));
            }
            return Ok(RunningMinicluster {
                _testing: None,
                _cluster: None,
                conf_path,
            });
        }

        // This E2E binary shares one live cluster. Each test must use a unique
        // workspace root and must not mutate cluster-global state.
        let testing = Testing::builder().default().workers(3).build()?;
        let cluster = testing.start_cluster()?;
        let conf_path = testing.active_conf_path().to_string();
        Ok(RunningMinicluster {
            _testing: Some(testing),
            _cluster: Some(cluster),
            conf_path,
        })
    })
}

pub fn start_minicluster() -> CommonResult<(&'static RunningMinicluster, Runtime)> {
    let cluster = minicluster()?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| orpc::CommonError::from(e.to_string()))?;
    Ok((cluster, rt))
}

pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}
