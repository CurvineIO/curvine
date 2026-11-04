// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn remote_write_windows_flush_seek_and_commit() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.client.short_circuit = false;
            conf.client.replicas = 1;
        })
        .build()?;
    let _cluster = testing.start_cluster()?;
    let base = testing.get_active_cluster_conf()?;
    for window in [8, 16, 32] {
        let mut conf = base.clone();
        conf.client.write_pipeline_window = window;
        conf.client.write_chunk_size_str = "4KB".into();
        conf.client.init()?;
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
        rt.block_on(async move {
            let path = Path::from_str(format!("/grpc-pipeline-{window}"))?;
            let mut expected: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
            let mut writer = fs.create(&path, true).await?;
            writer.write(&expected).await?;
            writer.flush().await?;
            writer.seek(8190).await?;
            writer.write(b"pipeline-seek").await?;
            expected[8190..8190 + 13].copy_from_slice(b"pipeline-seek");
            writer.complete().await?;
            let mut reader = fs.open(&path).await?;
            let mut actual = vec![0; expected.len()];
            let read = reader.read_full(&mut actual).await?;
            assert_eq!(read, expected.len());
            assert_eq!(actual, expected);
            Ok::<_, curvine_error::FsError>(())
        })?;
    }
    Ok(())
}
