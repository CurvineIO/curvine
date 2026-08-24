use crate::registry::RegistrationControl;
use crate::{
    DiscoveryError, DiscoveryResult, RegistrationGuard, RegistrationOptions, RegistrationStatus,
    ServiceEndpoint, ServiceKind, ServiceRegistry, ServiceResolver, ServiceResolverHandle,
    ServiceSnapshot, ServiceStatus, ServiceWatchEvent, SnapshotReader,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, watch, RwLock};

#[derive(Default, Clone)]
struct FakeDiscovery {
    store: Arc<RwLock<HashMap<ServiceKind, BTreeMap<String, ServiceEndpoint>>>>,
}

#[async_trait]
impl ServiceRegistry for FakeDiscovery {
    async fn register(
        &self,
        endpoint: ServiceEndpoint,
        options: RegistrationOptions,
    ) -> DiscoveryResult<RegistrationGuard> {
        options.validate()?;
        endpoint.validate()?;
        let kind = endpoint.kind.clone();
        let service_id = endpoint.id.clone();
        self.store
            .write()
            .await
            .entry(kind.clone())
            .or_default()
            .insert(service_id.clone(), endpoint);
        let (status_tx, status_rx) = watch::channel(RegistrationStatus::Registered);
        Ok(RegistrationGuard {
            kind: kind.clone(),
            service_id: service_id.clone(),
            lease_id: 1,
            status_rx,
            control: Arc::new(FakeRegistrationControl {
                store: self.store.clone(),
                kind,
                service_id,
                status_tx,
            }),
        })
    }
}

#[async_trait]
impl ServiceResolver for FakeDiscovery {
    async fn list(&self, kind: ServiceKind) -> DiscoveryResult<ServiceSnapshot> {
        let endpoints = self
            .store
            .read()
            .await
            .get(&kind)
            .map(|services| services.values().cloned().collect())
            .unwrap_or_default();
        Ok(ServiceSnapshot {
            kind,
            revision: 0,
            stale: false,
            last_update_ms: 0,
            endpoints,
        })
    }

    async fn watch(&self, kind: ServiceKind) -> DiscoveryResult<ServiceResolverHandle> {
        let snapshot = self.list(kind.clone()).await?;
        let reader = SnapshotReader::new(snapshot.clone(), true);
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ServiceWatchEvent::Reset(snapshot)))
            .await
            .map_err(|error| DiscoveryError::EtcdUnavailable(error.to_string()))?;
        Ok(ServiceResolverHandle::new(kind, reader, rx))
    }
}

struct FakeRegistrationControl {
    store: Arc<RwLock<HashMap<ServiceKind, BTreeMap<String, ServiceEndpoint>>>>,
    kind: ServiceKind,
    service_id: String,
    status_tx: watch::Sender<RegistrationStatus>,
}

#[async_trait]
impl RegistrationControl for FakeRegistrationControl {
    async fn update_endpoint(&self, endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
        self.store
            .write()
            .await
            .entry(self.kind.clone())
            .or_default()
            .insert(self.service_id.clone(), endpoint);
        Ok(())
    }

    async fn update_status(&self, status: ServiceStatus) -> DiscoveryResult<()> {
        let mut store = self.store.write().await;
        let endpoint = store
            .get_mut(&self.kind)
            .and_then(|services| services.get_mut(&self.service_id))
            .ok_or_else(|| DiscoveryError::RegistrationLost("endpoint missing".to_string()))?;
        endpoint.status = status;
        Ok(())
    }

    async fn shutdown(&self) -> DiscoveryResult<()> {
        if let Some(services) = self.store.write().await.get_mut(&self.kind) {
            services.remove(&self.service_id);
        }
        let _ = self.status_tx.send(RegistrationStatus::Revoked);
        Ok(())
    }
}

fn endpoint(kind: &str, id: &str) -> ServiceEndpoint {
    ServiceEndpoint {
        kind: ServiceKind::try_new(kind).unwrap(),
        id: id.to_string(),
        host: "mds.default.svc".to_string(),
        rpc_port: 9100,
        web_port: None,
        component_info: curvine_proto::ComponentInfoProto {
            component: Some(kind.to_string()),
            release_version: Some("0.2.0".to_string()),
            git_commit: Some("abcdef".to_string()),
            git_tag: Some(String::new()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: Vec::new(),
        },
        start_time_ms: 1,
        status: ServiceStatus::Serving,
        metadata: None,
    }
}

#[tokio::test]
async fn fake_registry_and_resolver_exercise_traits() {
    let discovery = FakeDiscovery::default();
    let kind = ServiceKind::try_new("mds").unwrap();
    let guard = discovery
        .register(endpoint("mds", "mds-1"), RegistrationOptions::default())
        .await
        .unwrap();

    let snapshot = discovery.list(kind.clone()).await.unwrap();
    assert_eq!(snapshot.endpoints.len(), 1);
    assert_eq!(snapshot.endpoints[0].id, "mds-1");

    guard.update_status(ServiceStatus::Draining).await.unwrap();
    let snapshot = discovery.list(kind.clone()).await.unwrap();
    assert_eq!(snapshot.endpoints[0].status, ServiceStatus::Draining);

    guard.shutdown().await.unwrap();
    let snapshot = discovery.list(kind).await.unwrap();
    assert!(snapshot.endpoints.is_empty());
}

#[tokio::test]
async fn fake_watch_returns_initial_reset() {
    let discovery = FakeDiscovery::default();
    let kind = ServiceKind::try_new("mds").unwrap();
    discovery
        .register(endpoint("mds", "mds-1"), RegistrationOptions::default())
        .await
        .unwrap();

    let mut handle = discovery.watch(kind).await.unwrap();
    let event = handle.next_event().await.unwrap().unwrap();

    match event {
        ServiceWatchEvent::Reset(snapshot) => assert_eq!(snapshot.endpoints.len(), 1),
        other => panic!("unexpected watch event: {other:?}"),
    }
}
