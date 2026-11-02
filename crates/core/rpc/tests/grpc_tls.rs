// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use curvine_rpc::grpc::{reflection, GrpcConfig, TransportConfig};
use std::time::Duration;
use tonic_health::pb::{health_client::HealthClient, HealthCheckRequest};

fn path(name: &str) -> String {
    format!(
        "{}/tests/fixtures/grpc_tls/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn config() -> GrpcConfig {
    GrpcConfig {
        tls_enabled: true,
        ca_cert_path: path("ca.pem"),
        client_ca_cert_path: String::new(),
        server_cert_path: path("server.pem"),
        server_key_path: path("server.key"),
        client_cert_path: path("client.pem"),
        client_key_path: path("client.key"),
        domain_name: "localhost".into(),
        require_client_auth: true,
        enable_reflection: true,
    }
}

#[test]
fn rejects_unsafe_or_incomplete_configuration() {
    let conf = GrpcConfig {
        require_client_auth: true,
        ..Default::default()
    };
    assert!(conf.server_tls().is_err());
    let conf = GrpcConfig {
        ca_cert_path: path("ca.pem"),
        ..Default::default()
    };
    assert!(conf.client_tls("localhost").is_err());
    let conf = GrpcConfig {
        client_key_path: String::new(),
        ..config()
    };
    assert!(conf.client_tls("localhost").is_err());
    let conf = GrpcConfig {
        server_key_path: String::new(),
        ..config()
    };
    assert!(conf.server_tls().is_err());
    let conf = GrpcConfig {
        require_client_auth: false,
        client_ca_cert_path: path("ca.pem"),
        ..config()
    };
    assert!(conf.server_tls().is_err());
    assert_eq!(GrpcConfig::default().uri("::1", 80), "http://[::1]:80");
    let valid = config();
    let client = valid.client_tls("localhost").unwrap().unwrap();
    let server = valid.server_tls().unwrap().unwrap();
    assert!(!format!("{client:?}").contains("identity_pem"));
    assert!(!format!("{server:?}").contains("private_key_pem"));
}

#[tokio::test]
async fn mutual_tls_and_reflection_require_a_trusted_identity() {
    let conf = config();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let (_reporter, health) = curvine_rpc::grpc::health_reporter();
    let reflection = reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(curvine_proto::FILE_DESCRIPTOR_SET)
        .with_service_name("proto.MasterService")
        .build()
        .unwrap();
    let mut server = TransportConfig::default()
        .server(conf.server_tls().unwrap().as_ref())
        .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        server
            .add_service(health)
            .add_service(reflection)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let transport = TransportConfig {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    };
    let endpoint = transport
        .endpoint(
            conf.uri("127.0.0.1", addr.port()),
            conf.client_tls("127.0.0.1").unwrap().as_ref(),
        )
        .unwrap();
    let channel = endpoint.connect().await.unwrap();
    HealthClient::new(channel.clone())
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();

    use reflection::pb::{
        server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
        ServerReflectionRequest,
    };
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let symbol_request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileContainingSymbol(
            "proto.MasterService".into(),
        )),
    };
    let mut stream = ServerReflectionClient::new(channel)
        .server_reflection_info(futures::stream::iter(vec![request, symbol_request]))
        .await
        .unwrap()
        .into_inner();
    let response = stream.message().await.unwrap().unwrap();
    let Some(MessageResponse::ListServicesResponse(list)) = response.message_response else {
        panic!("expected service list")
    };
    assert!(list.service.iter().any(|s| s.name == "proto.MasterService"));
    assert!(!list
        .service
        .iter()
        .any(|s| s.name == "proto.BlockDataService"));
    let response = stream.message().await.unwrap().unwrap();
    let Some(MessageResponse::FileDescriptorResponse(file)) = response.message_response else {
        panic!("expected service descriptor");
    };
    assert!(!file.file_descriptor_proto.is_empty());
    drop(stream);

    let missing_identity = GrpcConfig {
        client_cert_path: String::new(),
        client_key_path: String::new(),
        ..conf.clone()
    };
    let endpoint = transport
        .endpoint(
            missing_identity.uri("127.0.0.1", addr.port()),
            missing_identity.client_tls("127.0.0.1").unwrap().as_ref(),
        )
        .unwrap();
    let denied = match endpoint.connect().await {
        Err(_) => true,
        Ok(channel) => HealthClient::new(channel)
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .is_err(),
    };
    assert!(denied, "mTLS must reject a client without a certificate");
    let wrong_name = GrpcConfig {
        domain_name: "wrong.example".into(),
        ..conf
    };
    let endpoint = transport
        .endpoint(
            wrong_name.uri("127.0.0.1", addr.port()),
            wrong_name.client_tls("127.0.0.1").unwrap().as_ref(),
        )
        .unwrap();
    assert!(
        endpoint.connect().await.is_err(),
        "TLS must verify the server name"
    );
    let wrong_ca = GrpcConfig {
        ca_cert_path: path("wrong-ca.pem"),
        domain_name: "localhost".into(),
        ..config()
    };
    let endpoint = transport.endpoint(
        wrong_ca.uri("127.0.0.1", addr.port()),
        wrong_ca.client_tls("127.0.0.1").unwrap().as_ref(),
    );
    let rejected = match endpoint {
        Err(_) => true,
        Ok(endpoint) => endpoint.connect().await.is_err(),
    };
    assert!(rejected, "TLS must reject an untrusted server certificate");
    let _ = stop.send(());
    task.await.unwrap();
}

#[tokio::test]
async fn server_tls_without_client_auth_accepts_no_client_certificate() {
    let conf = GrpcConfig {
        require_client_auth: false,
        client_cert_path: String::new(),
        client_key_path: String::new(),
        ..config()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let (_reporter, health) = curvine_rpc::grpc::health_reporter();
    let mut server = TransportConfig::default()
        .server(conf.server_tls().unwrap().as_ref())
        .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        server
            .add_service(health)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let endpoint = TransportConfig::default()
        .endpoint(
            conf.uri("127.0.0.1", addr.port()),
            conf.client_tls("127.0.0.1").unwrap().as_ref(),
        )
        .unwrap();
    let channel = endpoint.connect().await.unwrap();
    HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();
    let _ = stop.send(());
    task.await.unwrap();
}

#[tokio::test]
async fn custom_incoming_socket_options_are_applied() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, accepted) =
        tokio::join!(tokio::net::TcpStream::connect(address), listener.accept(),);
    let _client = client.unwrap();
    let (stream, _) = accepted.unwrap();
    stream.set_nodelay(false).unwrap();
    let socket = socket2::SockRef::from(&stream);
    socket.set_keepalive(false).unwrap();

    TransportConfig::default()
        .configure_socket(&stream)
        .unwrap();
    assert!(stream.nodelay().unwrap());
    assert!(socket.keepalive().unwrap());

    TransportConfig {
        tcp_keepalive: None,
        ..Default::default()
    }
    .configure_socket(&stream)
    .unwrap();
    assert!(stream.nodelay().unwrap());
    assert!(!socket.keepalive().unwrap());
}
