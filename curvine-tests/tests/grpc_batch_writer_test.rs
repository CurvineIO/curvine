// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

fn round_trip(short_circuit: bool) -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(move |conf| {
            conf.client.short_circuit = short_circuit;
            conf.client.replicas = 1;
            conf.client.write_chunk_size_str = "64B".into();
        })
        .build()?;
    let _cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let info = fs.get_filesystem_info().await?;
        assert_eq!(info.live_workers.len(), 1);
        assert!(fs
            .fs_context()
            .is_local_worker(&info.live_workers[0].address));
        let small = "batch-small".to_string();
        let large = "batch-large".repeat(128);
        let entries = [
            (Path::from_str("/grpc-batch/small")?, small.as_str()),
            (Path::from_str("/grpc-batch/large")?, large.as_str()),
        ];
        // Multiple rounds exercise writer completion and subsequent stream reuse.
        for _ in 0..2 {
            fs.write_batch_string(&entries).await?;
            for (path, expected) in &entries {
                assert_eq!(fs.get_status(path).await?.len, expected.len() as i64);
                let mut reader = fs.open(path).await?;
                let mut actual = vec![0; expected.len()];
                assert_eq!(reader.read_full(&mut actual).await?, expected.len());
                assert_eq!(actual, expected.as_bytes());
                reader.complete().await?;
            }
        }
        Ok::<_, curvine_error::FsError>(())
    })?;
    Ok(())
}

#[test]
fn batch_strings_remote_round_trip() -> CommonResult<()> {
    round_trip(false)
}

#[test]
fn batch_strings_short_circuit_round_trip() -> CommonResult<()> {
    round_trip(true)
}
