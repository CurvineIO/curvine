// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0.

use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;
use std::time::Duration;

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

    // Use a single real-storage file for all automatic-window chunk sizes.
    // Its unaligned length also exercises the last, partial response.
    let expected: Vec<u8> = (0..10 * 1024 * 1024 + 317)
        .map(|i| (i ^ (i >> 8) ^ (i >> 16) ^ (i >> 24)) as u8)
        .collect();
    let path = Path::from_str("/grpc-read-auto")?;
    for (chunk_setting, chunk_size) in [
        ("128KB", 128 * 1024),
        ("1MB", 1024 * 1024),
        // Larger than the auto policy's byte budget: one request must still
        // be allowed, otherwise the first read would never make progress.
        ("5MB", 5 * 1024 * 1024),
    ] {
        let mut conf = base.clone();
        conf.client.read_pipeline_window = 0;
        conf.client.read_chunk_size_str = chunk_setting.into();
        // Drive the gRPC stream directly from the foreground reader so that
        // the buffering and smart-prefetch layers do not hide its behavior.
        conf.client.read_chunk_num = 1;
        conf.client.read_parallel = 1;
        conf.client.enable_smart_prefetch = false;
        conf.client.init()?;
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
        rt.block_on(async {
            if chunk_setting == "128KB" {
                let mut writer = fs.create(&path, true).await?;
                writer.write(&expected).await?;
                writer.complete().await?;
            }

            let mut reader = fs.open(&path).await?;
            let mut actual = vec![0x5a; expected.len() + 31];
            let read = tokio::time::timeout(Duration::from_secs(60), reader.read_full(&mut actual))
                .await
                .expect("automatic-window read did not make progress")?;
            assert_eq!(read, expected.len(), "chunk size {chunk_setting}");
            assert_eq!(
                &actual[..read],
                expected.as_slice(),
                "chunk size {chunk_setting}"
            );
            assert!(actual[read..].iter().all(|&byte| byte == 0x5a));
            assert_eq!(reader.read(&mut [0; 1]).await?, 0);
            assert_eq!(reader.pos(), expected.len() as i64);

            // Rewind after EOF, consume several chunks to make an adaptive
            // burst possible, then seek backward and across distant offsets.
            reader.seek(0).await?;
            let prefix_len = (chunk_size * 3 + 73).min(expected.len() - 1);
            let mut prefix = vec![0; prefix_len];
            assert_eq!(reader.read_full(&mut prefix).await?, prefix_len);
            assert_eq!(prefix, expected[..prefix_len]);
            for offset in [
                17,
                expected.len() - chunk_size / 2 - 57,
                expected.len() - 89,
                0,
            ] {
                reader.seek(offset as i64).await?;
                let len = (chunk_size + 193).min(expected.len() - offset);
                let mut sample = vec![0; len];
                assert_eq!(reader.read_full(&mut sample).await?, len);
                assert_eq!(
                    sample,
                    expected[offset..offset + len],
                    "offset {offset}, chunk size {chunk_setting}"
                );
            }
            reader.complete().await?;

            // Delayed consumption and an early close must leave subsequent
            // reads usable even if responses were already requested.
            let mut reader = fs.open(&path).await?;
            let mut offset = 0;
            for _ in 0..3 {
                let len = chunk_size.min(expected.len() - offset);
                let mut sample = vec![0; len];
                assert_eq!(reader.read_full(&mut sample).await?, len);
                assert_eq!(sample, expected[offset..offset + len]);
                offset += len;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            reader.complete().await?;

            let mut abandoned = fs.open(&path).await?;
            let len = (chunk_size * 6).min(expected.len() / 2);
            let mut sample = vec![0; len];
            assert_eq!(abandoned.read_full(&mut sample).await?, len);
            assert_eq!(sample, expected[..len]);
            drop(abandoned);

            let mut reopened = fs.open(&path).await?;
            let offset = expected.len() / 3;
            reopened.seek(offset as i64).await?;
            let mut sample = vec![0; 4099];
            assert_eq!(reopened.read_full(&mut sample).await?, sample.len());
            assert_eq!(sample, expected[offset..offset + sample.len()]);
            reopened.complete().await?;
            Ok::<_, curvine_error::FsError>(())
        })?;
    }
    Ok(())
}
