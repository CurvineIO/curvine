// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use curvine_core_error::CommonResult;
use curvine_proto::{master_service_client::MasterServiceClient, GetFilesystemInfoRequest};
use curvine_rpc::grpc::reflection::pb::v1alpha::{
    server_reflection_client::ServerReflectionClient, server_reflection_request::MessageRequest,
    server_reflection_response::MessageResponse, ServerReflectionRequest,
};
use curvine_rpc::grpc::{GrpcConfig, TransportConfig};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::{path::PathBuf, time::Duration};

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../crates/core/rpc/tests/fixtures/grpc_tls")
        .join(name)
        .display()
        .to_string()
}

#[test]
fn master_requires_client_identity_and_exposes_only_master_reflection() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(0)
        .mutate_conf(|conf| {
            conf.master.grpc = GrpcConfig {
                tls_enabled: true,
                ca_cert_path: fixture("ca.pem"),
                server_cert_path: fixture("server.pem"),
                server_key_path: fixture("server.key"),
                require_client_auth: true,
                enable_reflection: true,
                ..Default::default()
            };
            conf.client.grpc = GrpcConfig {
                tls_enabled: true,
                ca_cert_path: fixture("ca.pem"),
                client_cert_path: fixture("client.pem"),
                client_key_path: fixture("client.key"),
                domain_name: "localhost".into(),
                ..Default::default()
            };
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let runtime = cluster.clone_client_rt();
    runtime
        .block_on(async {
            // Exercise the production client and its configured TLS path.
            cluster.new_fs().get_filesystem_info().await?;
            let addr = cluster.master_conf().master_addr();
            let config = &cluster.master_conf().client.grpc;
            let transport = TransportConfig {
                connect_timeout: Duration::from_secs(2),
                request_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            };
            let endpoint = transport.endpoint(
                config.uri(&addr.hostname, addr.port),
                config.client_tls(&addr.hostname)?.as_ref(),
            )?;
            let channel = endpoint.connect().await?;
            let mut reflection = ServerReflectionClient::new(channel);
            let request = ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            };
            let mut stream = reflection
                .server_reflection_info(futures::stream::iter([request]))
                .await?
                .into_inner();
            let response = stream.message().await?.expect("reflection response");
            let Some(MessageResponse::ListServicesResponse(list)) = response.message_response
            else {
                panic!("expected service list");
            };
            let names: Vec<_> = list.service.iter().map(|s| s.name.as_str()).collect();
            assert!(names.contains(&"proto.MasterService"));
            assert!(!names.contains(&"proto.WorkerControlService"));

            let mut no_identity = config.clone();
            no_identity.client_cert_path.clear();
            no_identity.client_key_path.clear();
            let endpoint = transport.endpoint(
                no_identity.uri(&addr.hostname, addr.port),
                no_identity.client_tls(&addr.hostname)?.as_ref(),
            )?;
            let denied = tokio::time::timeout(Duration::from_secs(5), async {
                match endpoint.connect().await {
                    Err(_) => true,
                    Ok(channel) => {
                        let mut request = tonic::Request::new(GetFilesystemInfoRequest::default());
                        curvine_rpc::grpc::set_request_id(&mut request, 1);
                        MasterServiceClient::new(channel)
                            .get_filesystem_info(request)
                            .await
                            .is_err()
                    }
                }
            })
            .await
            .expect("mTLS authentication must finish");
            assert!(denied, "master must reject clients without a certificate");
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?;
    Ok(())
}
