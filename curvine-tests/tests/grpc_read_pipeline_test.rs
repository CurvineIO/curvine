// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0.

use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn remote_read_bursts_seek_and_complete_without_stale_data() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.client.short_circuit = false;
            conf.client.replicas = 1;
            conf.client.enable_read_ahead = false;
        })
        .build()?;
    let _cluster = testing.start_cluster()?;
    let base = testing.get_active_cluster_conf()?;
    for window in [1, 4, 8, 32] {
        let mut conf = base.clone();
        conf.client.read_pipeline_window = window;
        conf.client.read_chunk_size_str = "4KB".into();
        conf.client.init()?;
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
        rt.block_on(async move {
            let path = Path::from_str(format!("/grpc-read-window-{window}"))?;
            let expected: Vec<u8> = (0..64 * 1024 + 117).map(|i| (i % 251) as u8).collect();
            let mut writer = fs.create(&path, true).await?;
            writer.write(&expected).await?;
            writer.complete().await?;
            let mut reader = fs.open(&path).await?;
            for offset in [0, 8203, 16, expected.len() - 37] {
                reader.seek(offset as i64).await?;
                let mut actual = vec![0; 17];
                assert_eq!(reader.read_full(&mut actual).await?, actual.len());
                assert_eq!(actual, expected[offset..offset + actual.len()]);
            }
            reader.complete().await?;
            let mut reader = fs.open(&path).await?;
            let mut actual = vec![0; expected.len()];
            assert_eq!(reader.read_full(&mut actual).await?, expected.len());
            assert_eq!(actual, expected);
            reader.complete().await?;
            Ok::<_, curvine_error::FsError>(())
        })?;
    }
    Ok(())
}
