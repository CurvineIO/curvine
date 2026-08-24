#![cfg(feature = "etcd")]

use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_service_discovery::{
    decode_endpoint_value, encode_endpoint_value, EtcdDiscoveryConfig, EtcdServiceResolver,
    RegistrationOptions, RegistrationStatus, ServiceEndpoint, ServiceKey, ServiceKind,
    ServiceRegistry, ServiceResolver, ServiceResolverHandle, ServiceStatus, ServiceWatchEvent,
};
use etcd_client::{Client, DeleteOptions, PutOptions};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

fn etcd_endpoint() -> Option<String> {
    std::env::var("CURVINE_ETCD_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn unique_prefix(test_name: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("/curvine-test/{test_name}-{}-{millis}", std::process::id())
}

fn component_info(component: &str) -> curvine_proto::ComponentInfoProto {
    curvine_proto::ComponentInfoProto {
        component: Some(component.to_string()),
        release_version: Some("0.2.0".to_string()),
        git_commit: Some("abcdef".to_string()),
        git_tag: Some(String::new()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: Vec::new(),
    }
}

fn endpoint(id: &str) -> ServiceEndpoint {
    ServiceEndpoint {
        kind: ServiceKind::try_new("mds").unwrap(),
        id: id.to_string(),
        host: "mds.default.svc".to_string(),
        rpc_port: 9100,
        web_port: None,
        component_info: component_info("mds"),
        start_time_ms: 1,
        status: ServiceStatus::Serving,
        metadata: None,
    }
}

fn registration_options() -> RegistrationOptions {
    RegistrationOptions {
        lease_ttl_secs: 3,
        keep_alive_interval_secs: 1,
        register_timeout_ms: 5000,
    }
}

async fn next_watch_event(handle: &mut ServiceResolverHandle) -> ServiceWatchEvent {
    timeout(Duration::from_secs(5), handle.next_event())
        .await
        .expect("watch event timed out")
        .expect("watch stream closed")
        .expect("watch event failed")
}

#[test]
fn etcd_resolver_lists_registered_endpoint() {
    let Some(etcd_addr) = etcd_endpoint() else {
        eprintln!("skip etcd integration test: CURVINE_ETCD_ADDR is not set");
        return;
    };

    let rt = Arc::new(AsyncRuntime::single());
    let resolver_rt = rt.clone();
    rt.block_on(async move {
        let prefix = unique_prefix("list");
        let config =
            EtcdDiscoveryConfig::new(vec![etcd_addr.clone()], prefix.clone(), "test-cluster");
        let resolver = EtcdServiceResolver::connect(config, resolver_rt)
            .await
            .unwrap();
        let kind = ServiceKind::try_new("mds").unwrap();
        let service_key = ServiceKey::new(&prefix, "test-cluster", kind.clone(), "mds-1").unwrap();
        let endpoint = endpoint("mds-1");
        let value = encode_endpoint_value(&endpoint).unwrap();
        let mut client = Client::connect([etcd_addr], None).await.unwrap();

        client
            .put(service_key.as_string(), value, Some(PutOptions::new()))
            .await
            .unwrap();
        let snapshot = resolver.list(kind).await.unwrap();
        assert_eq!(snapshot.endpoints, vec![endpoint]);

        client
            .delete(prefix, Some(DeleteOptions::new().with_prefix()))
            .await
            .unwrap();
    });
}

#[test]
fn etcd_resolver_watch_emits_initial_reset() {
    let Some(etcd_addr) = etcd_endpoint() else {
        eprintln!("skip etcd integration test: CURVINE_ETCD_ADDR is not set");
        return;
    };

    let rt = Arc::new(AsyncRuntime::single());
    let resolver_rt = rt.clone();
    rt.block_on(async move {
        let prefix = unique_prefix("watch-reset");
        let config =
            EtcdDiscoveryConfig::new(vec![etcd_addr.clone()], prefix.clone(), "test-cluster");
        let resolver = EtcdServiceResolver::connect(config, resolver_rt)
            .await
            .unwrap();
        let kind = ServiceKind::try_new("mds").unwrap();
        let service_key = ServiceKey::new(&prefix, "test-cluster", kind.clone(), "mds-1").unwrap();
        let endpoint = endpoint("mds-1");
        let value = encode_endpoint_value(&endpoint).unwrap();
        let mut client = Client::connect([etcd_addr], None).await.unwrap();

        client
            .put(service_key.as_string(), value, Some(PutOptions::new()))
            .await
            .unwrap();
        let mut handle = resolver.watch(kind).await.unwrap();
        let event = handle.next_event().await.unwrap().unwrap();
        match event {
            ServiceWatchEvent::Reset(snapshot) => assert_eq!(snapshot.endpoints, vec![endpoint]),
            other => panic!("unexpected event: {other:?}"),
        }

        client
            .delete(prefix, Some(DeleteOptions::new().with_prefix()))
            .await
            .unwrap();
    });
}

#[test]
fn etcd_resolver_watch_emits_incremental_changes() {
    let Some(etcd_addr) = etcd_endpoint() else {
        eprintln!("skip etcd integration test: CURVINE_ETCD_ADDR is not set");
        return;
    };

    let rt = Arc::new(AsyncRuntime::single());
    let resolver_rt = rt.clone();
    rt.block_on(async move {
        let prefix = unique_prefix("watch-incremental");
        let config =
            EtcdDiscoveryConfig::new(vec![etcd_addr.clone()], prefix.clone(), "test-cluster");
        let resolver = EtcdServiceResolver::connect(config, resolver_rt)
            .await
            .unwrap();
        let kind = ServiceKind::try_new("mds").unwrap();
        let service_key = ServiceKey::new(&prefix, "test-cluster", kind.clone(), "mds-1").unwrap();
        let mut endpoint = endpoint("mds-1");
        let mut client = Client::connect([etcd_addr], None).await.unwrap();
        let mut handle = resolver.watch(kind.clone()).await.unwrap();

        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Reset(snapshot) => assert!(snapshot.endpoints.is_empty()),
            other => panic!("unexpected event: {other:?}"),
        }

        client
            .put(
                service_key.as_string(),
                encode_endpoint_value(&endpoint).unwrap(),
                Some(PutOptions::new()),
            )
            .await
            .unwrap();
        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Added(added) => assert_eq!(added, endpoint),
            other => panic!("unexpected event: {other:?}"),
        }

        endpoint.status = ServiceStatus::Draining;
        client
            .put(
                service_key.as_string(),
                encode_endpoint_value(&endpoint).unwrap(),
                Some(PutOptions::new()),
            )
            .await
            .unwrap();
        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Updated(updated) => assert_eq!(updated, endpoint),
            other => panic!("unexpected event: {other:?}"),
        }

        client
            .delete(service_key.as_string(), Some(DeleteOptions::new()))
            .await
            .unwrap();
        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Removed { kind, id } => {
                assert_eq!(kind, ServiceKind::try_new("mds").unwrap());
                assert_eq!(id, "mds-1");
            }
            other => panic!("unexpected event: {other:?}"),
        }

        client
            .delete(prefix, Some(DeleteOptions::new().with_prefix()))
            .await
            .unwrap();
    });
}

#[test]
fn etcd_registry_registers_and_shutdown_revokes_endpoint() {
    let Some(etcd_addr) = etcd_endpoint() else {
        eprintln!("skip etcd integration test: CURVINE_ETCD_ADDR is not set");
        return;
    };

    let rt = Arc::new(AsyncRuntime::single());
    let resolver_rt = rt.clone();
    rt.block_on(async move {
        let prefix = unique_prefix("register-shutdown");
        let config =
            EtcdDiscoveryConfig::new(vec![etcd_addr.clone()], prefix.clone(), "test-cluster");
        let discovery = EtcdServiceResolver::connect(config, resolver_rt)
            .await
            .unwrap();
        let kind = ServiceKind::try_new("mds").unwrap();
        let endpoint = endpoint("mds-1");
        let mut handle = discovery.watch(kind.clone()).await.unwrap();

        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Reset(snapshot) => assert!(snapshot.endpoints.is_empty()),
            other => panic!("unexpected event: {other:?}"),
        }

        let guard = discovery
            .register(endpoint.clone(), registration_options())
            .await
            .unwrap();
        let snapshot = discovery.list(kind.clone()).await.unwrap();
        assert_eq!(snapshot.endpoints, vec![endpoint]);

        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Added(added) => assert_eq!(added.id, "mds-1"),
            other => panic!("unexpected event: {other:?}"),
        }

        guard.shutdown().await.unwrap();
        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Removed { kind, id } => {
                assert_eq!(kind, ServiceKind::try_new("mds").unwrap());
                assert_eq!(id, "mds-1");
            }
            other => panic!("unexpected event: {other:?}"),
        }

        client_delete_prefix(etcd_addr, prefix).await;
    });
}

#[test]
fn etcd_registry_update_preserves_lease_and_emits_updated() {
    let Some(etcd_addr) = etcd_endpoint() else {
        eprintln!("skip etcd integration test: CURVINE_ETCD_ADDR is not set");
        return;
    };

    let rt = Arc::new(AsyncRuntime::single());
    let resolver_rt = rt.clone();
    rt.block_on(async move {
        let prefix = unique_prefix("update-lease");
        let config =
            EtcdDiscoveryConfig::new(vec![etcd_addr.clone()], prefix.clone(), "test-cluster");
        let discovery = EtcdServiceResolver::connect(config, resolver_rt)
            .await
            .unwrap();
        let kind = ServiceKind::try_new("mds").unwrap();
        let service_key = ServiceKey::new(&prefix, "test-cluster", kind.clone(), "mds-1").unwrap();
        let endpoint = endpoint("mds-1");
        let guard = discovery
            .register(endpoint, registration_options())
            .await
            .unwrap();
        let mut handle = discovery.watch(kind).await.unwrap();

        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Reset(snapshot) => assert_eq!(snapshot.endpoints.len(), 1),
            other => panic!("unexpected event: {other:?}"),
        }

        let mut client = Client::connect([etcd_addr.clone()], None).await.unwrap();
        let response = client.get(service_key.as_string(), None).await.unwrap();
        let lease_before = response.kvs()[0].lease();
        assert_eq!(lease_before, guard.lease_id());

        guard.update_status(ServiceStatus::Draining).await.unwrap();

        let response = client.get(service_key.as_string(), None).await.unwrap();
        let kv = &response.kvs()[0];
        assert_eq!(kv.lease(), lease_before);
        let updated = decode_endpoint_value(kv.value()).unwrap();
        assert_eq!(updated.status, ServiceStatus::Draining);

        match next_watch_event(&mut handle).await {
            ServiceWatchEvent::Updated(updated) => {
                assert_eq!(updated.id, "mds-1");
                assert_eq!(updated.status, ServiceStatus::Draining);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        guard.shutdown().await.unwrap();
        client_delete_prefix(etcd_addr, prefix).await;
    });
}

#[test]
fn etcd_registry_reports_keepalive_lost_after_external_revoke() {
    let Some(etcd_addr) = etcd_endpoint() else {
        eprintln!("skip etcd integration test: CURVINE_ETCD_ADDR is not set");
        return;
    };

    let rt = Arc::new(AsyncRuntime::single());
    let resolver_rt = rt.clone();
    rt.block_on(async move {
        let prefix = unique_prefix("keepalive-lost");
        let config =
            EtcdDiscoveryConfig::new(vec![etcd_addr.clone()], prefix.clone(), "test-cluster");
        let discovery = EtcdServiceResolver::connect(config, resolver_rt)
            .await
            .unwrap();
        let guard = discovery
            .register(endpoint("mds-1"), registration_options())
            .await
            .unwrap();
        let mut status_rx = guard.subscribe_status();
        let mut client = Client::connect([etcd_addr.clone()], None).await.unwrap();

        client.lease_revoke(guard.lease_id()).await.unwrap();
        timeout(Duration::from_secs(5), async {
            loop {
                status_rx.changed().await.unwrap();
                if matches!(
                    &*status_rx.borrow(),
                    RegistrationStatus::KeepAliveLost { .. }
                ) {
                    break;
                }
            }
        })
        .await
        .expect("keepalive lost status timed out");

        let error = guard
            .update_status(ServiceStatus::Draining)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            curvine_service_discovery::DiscoveryError::RegistrationLost(_)
        ));
        client_delete_prefix(etcd_addr, prefix).await;
    });
}

async fn client_delete_prefix(etcd_addr: String, prefix: String) {
    let mut client = Client::connect([etcd_addr], None).await.unwrap();
    client
        .delete(prefix, Some(DeleteOptions::new().with_prefix()))
        .await
        .unwrap();
}
