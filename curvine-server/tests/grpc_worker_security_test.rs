// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_model::{ExtendedBlock, FileType, ProtoUtils, StorageType, WorkerAddress};
use curvine_net::net::NetUtils;
use curvine_proto::worker_control_service_client::WorkerControlServiceClient;
use curvine_proto::BlockWriteRequest;
use curvine_proto::QueryTransferTaskRequest;
use curvine_rpc::grpc::reflection::pb::{
    server_reflection_client::ServerReflectionClient, server_reflection_request::MessageRequest,
    server_reflection_response::MessageResponse, ServerReflectionRequest,
};
use curvine_rpc::grpc::{GrpcConfig, TransportConfig};
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_server::worker::Worker;
use std::path::PathBuf;
use std::time::Duration;

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../crates/core/rpc/tests/fixtures/grpc_tls")
        .join(name)
        .display()
        .to_string()
}

fn start_worker(reflection: bool) -> (u16, GrpcConfig) {
    let mut conf = ClusterConf {
        format_worker: true,
        ..Default::default()
    };
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec![format!(
        "[MEM:10MB]../testing/grpc-security-{}",
        conf.worker.rpc_port
    )];
    conf.worker.grpc = GrpcConfig {
        tls_enabled: true,
        ca_cert_path: fixture("ca.pem"),
        server_cert_path: fixture("server.pem"),
        server_key_path: fixture("server.key"),
        require_client_auth: true,
        enable_reflection: reflection,
        ..Default::default()
    };
    let client = GrpcConfig {
        tls_enabled: true,
        ca_cert_path: fixture("ca.pem"),
        client_cert_path: fixture("client.pem"),
        client_key_path: fixture("client.key"),
        domain_name: "localhost".into(),
        ..Default::default()
    };
    let port = conf.worker.rpc_port;
    let worker = Worker::with_conf(conf).unwrap();
    std::thread::spawn(move || worker.start_standalone());
    (port, client)
}

async fn connect(
    port: u16,
    config: &GrpcConfig,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    let tls = config.client_tls("localhost").unwrap();
    let endpoint = TransportConfig {
        connect_timeout: Duration::from_secs(2),
        ..Default::default()
    }
    .endpoint(config.uri("127.0.0.1", port), tls.as_ref())
    .unwrap();
    endpoint.connect().await
}

async fn wait_ready(port: u16, config: &GrpcConfig) -> tonic::transport::Channel {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match connect(port, config).await {
            Ok(channel) => return channel,
            Err(error) if tokio::time::Instant::now() >= deadline => {
                panic!("worker did not start: {error}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

#[allow(clippy::result_large_err)]
async fn listed_services(channel: tonic::transport::Channel) -> Result<Vec<String>, tonic::Status> {
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut client = ServerReflectionClient::new(channel);
    let mut stream = client
        .server_reflection_info(futures::stream::iter([request]))
        .await?
        .into_inner();
    let response = stream
        .message()
        .await?
        .ok_or_else(|| tonic::Status::internal("no reflection response"))?;
    match response.message_response {
        Some(MessageResponse::ListServicesResponse(list)) => Ok(list
            .service
            .into_iter()
            .map(|service| service.name)
            .collect()),
        _ => Err(tonic::Status::internal("unexpected reflection response")),
    }
}

#[tokio::test]
async fn worker_requires_client_identity_and_filters_optional_reflection() {
    let (enabled_port, config) = start_worker(true);
    let channel = wait_ready(enabled_port, &config).await;
    let services = listed_services(channel.clone()).await.unwrap();
    assert_eq!(services.len(), 2);
    assert!(services.iter().any(|s| s == "proto.BlockDataService"));
    assert!(services.iter().any(|s| s == "proto.WorkerControlService"));

    let mut client = WorkerControlServiceClient::new(channel);
    let mut query = tonic::Request::new(QueryTransferTaskRequest {
        job_id: "missing".into(),
        run_id: 1,
        task_id: "missing".into(),
        attempt_id: 1,
        worker_session_id: "missing".into(),
    });
    curvine_rpc::grpc::set_request_id(&mut query, 1);
    let response = client
        .query_transfer_task(query)
        .await
        .unwrap()
        .into_inner();
    assert!(!response.found);

    let mut client_conf = ClusterConf::default();
    client_conf.client.grpc = config.clone();
    let context = FsContext::new(client_conf).unwrap();
    let address = WorkerAddress {
        worker_id: 1,
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        rpc_port: enabled_port as u32,
        web_port: 0,
    };
    let block_client = context.block_client(&address).await.unwrap();
    let block = ExtendedBlock::new(12345, 0, StorageType::Mem, FileType::File);
    let header = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        block_size: 1024,
        chunk_size: 256,
        short_circuit: false,
        ..Default::default()
    };
    let request = |status, seq| {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(99)
            .seq_id(seq)
            .proto_header(header.clone())
            .build()
    };
    block_client
        .rpc(request(RequestStatus::Open, 0))
        .await
        .unwrap();
    block_client
        .rpc(request(RequestStatus::Cancel, 1))
        .await
        .unwrap();

    let mut no_identity = config.clone();
    no_identity.client_cert_path.clear();
    no_identity.client_key_path.clear();
    let unauthenticated = tokio::time::timeout(Duration::from_secs(5), async {
        match connect(enabled_port, &no_identity).await {
            Ok(channel) => listed_services(channel).await.map(|_| ()),
            Err(error) => Err(tonic::Status::unavailable(error.to_string())),
        }
    })
    .await
    .expect("unauthenticated request timed out");
    assert!(
        unauthenticated.is_err(),
        "mTLS accepted a client without identity"
    );

    let (disabled_port, disabled_config) = start_worker(false);
    let disabled = wait_ready(disabled_port, &disabled_config).await;
    let error = listed_services(disabled).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unimplemented);
}
