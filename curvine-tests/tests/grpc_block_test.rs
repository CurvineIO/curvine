// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_model::{CreateFileOpts, OpenFlags, ProtoUtils};
use curvine_proto::{master_service_client::MasterServiceClient, CreateFileRequest};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn grpc_remote_block_write_flush_and_seek_read() -> CommonResult<()> {
    let background_panic = Arc::new(AtomicBool::new(false));
    let panic_flag = background_panic.clone();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_flag.store(true, Ordering::SeqCst);
        previous_hook(info);
    }));
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(move |conf| {
            conf.client.short_circuit = false;
            conf.client.replicas = 1;
            conf.client.master_conn_pool_size = 2;
            conf.client.block_size_str = "64KB".into();
            conf.master.min_block_size = 64 * 1024;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let port = cluster.worker_conf[0].worker.rpc_port;
    let master_addr = cluster.master_conf[0].master_addr();
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let mut master = MasterServiceClient::connect(format!("http://{}", master_addr))
            .await
            .map_err(|error| curvine_error::FsError::common(error.to_string()))?;
        let create = CreateFileRequest {
            path: "/grpc-retry-dedup".into(),
            opts: ProtoUtils::create_opts_to_pb(CreateFileOpts::with_create(true), "test"),
            flags: OpenFlags::new_write_only().set_create(true).value(),
            first_block_client: None,
        };
        let request_id = curvine_runtime::common::Utils::req_id();
        let mut first = tonic::Request::new(create.clone());
        curvine_rpc::grpc::set_request_id(&mut first, request_id);
        let first = master
            .create_file(first)
            .await
            .map_err(curvine_error::grpc::from_status)?
            .into_inner();
        let mut retry = tonic::Request::new(create);
        curvine_rpc::grpc::set_request_id(&mut retry, request_id);
        let retry = master
            .create_file(retry)
            .await
            .map_err(curvine_error::grpc::from_status)?
            .into_inner();
        assert_eq!(first.file_status.id, retry.file_status.id);
        let worker_control = curvine_server::data_transfer::JobWorkerClient::connect_grpc(
            "127.0.0.1",
            port,
            Duration::from_secs(5),
        )
        .await?;
        let missing = worker_control
            .query_transfer_task("missing", 1, "missing", 1, "session")
            .await?;
        assert!(!missing.found);
        let path = Path::from_str("/grpc-block-roundtrip")?;
        let mut writer = fs.create(&path, true).await?;
        writer.write(b"abcdef").await?;
        writer.flush().await?;
        writer.seek(2).await?;
        writer.write(b"XY").await?;
        writer.complete().await?;

        let mut reader = fs.open(&path).await?;
        let mut result = [0; 6];
        let n = reader.read_full(&mut result).await?;
        assert_eq!(&result[..n], b"abXYef");
        reader.seek(2).await?;
        let mut tail = [0; 4];
        let n = reader.read_full(&mut tail).await?;
        assert_eq!(&tail[..n], b"XYef");

        // Exercise AddBlock after completing each full block, including its
        // typed response and the client's decoding of its located workers.
        let path = Path::from_str("/grpc-multiple-blocks")?;
        let payload: Vec<u8> = (0..150_000).map(|n| (n % 251) as u8).collect();
        let mut writer = fs.create(&path, true).await?;
        writer.write(&payload).await?;
        writer.complete().await?;
        let mut reader = fs.open(&path).await?;
        let mut readback = vec![0; payload.len()];
        assert_eq!(reader.read_full(&mut readback).await?, payload.len());
        assert_eq!(readback, payload);
        Ok::<_, curvine_error::FsError>(())
    })?;
    drop(rt);
    // Prefetch tasks can release their last filesystem/runtime references just
    // after the reader is dropped. Catch panics in those background tasks too.
    std::thread::sleep(std::time::Duration::from_millis(250));
    assert!(
        !background_panic.load(Ordering::SeqCst),
        "background task panicked"
    );
    Ok(())
}
