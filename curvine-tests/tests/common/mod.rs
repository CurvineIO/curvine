// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use once_cell::sync::OnceCell;
use orpc::CommonResult;
use tokio::runtime::Runtime;

static MINICLUSTER: OnceCell<RunningMinicluster> = OnceCell::new();
static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn unique_ns() -> u128 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    nanos.saturating_mul(1_000_000) + seq
}

pub struct RunningMinicluster {
    pub _testing: Testing,
    pub _cluster: Arc<MiniCluster>,
    pub conf_path: String,
}

fn minicluster() -> CommonResult<&'static RunningMinicluster> {
    MINICLUSTER.get_or_try_init(|| {
        // This E2E binary shares one live cluster. Each test must use a unique
        // workspace root and must not mutate cluster-global state.
        let testing = Testing::builder().default().workers(3).build()?;
        let cluster = testing.start_cluster()?;
        let conf_path = testing.active_conf_path().to_string();
        Ok(RunningMinicluster {
            _testing: testing,
            _cluster: cluster,
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
