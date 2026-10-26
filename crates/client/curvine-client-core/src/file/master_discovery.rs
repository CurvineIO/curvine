// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_error::FsResult;
use curvine_net::net::NodeAddr;
use curvine_rpc::client::ClusterConnector;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
#[cfg(feature = "discovery-etcd")]
use curvine_service_discovery::EtcdDiscoveryConfig;
use curvine_service_discovery::{
    DiscoveryError, DiscoveryProvider, DiscoveryResult, ServiceKind, ServiceSnapshot,
    ServiceStatus, ServiceWatchEvent, SnapshotReader,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::{watch, RwLock};

type ProviderFuture =
    Pin<Box<dyn Future<Output = DiscoveryResult<DiscoveryProvider>> + Send + 'static>>;
type ProviderFactory = Arc<dyn Fn(Arc<Runtime>) -> ProviderFuture + Send + Sync + 'static>;

#[derive(Clone, Debug)]
enum Readiness {
    Starting { last_error: Option<String> },
    Ready,
    Stopped,
}

#[derive(Default)]
struct CacheState {
    reader: Option<SnapshotReader>,
    snapshot: Option<ServiceSnapshot>,
    connected: bool,
    last_error: Option<String>,
}

/// Client-side master membership backed by the resolver's local watch cache.
/// The provider and runtime stay on an owned supervisor thread; neither the
/// FsContext nor its Tokio tasks own a discovery task that can outlive it.
pub(crate) struct ClientDiscovery {
    cache: Arc<RwLock<CacheState>>,
    readiness: watch::Receiver<Readiness>,
    initial_ready_timeout: Duration,
    allow_stale_cache: bool,
    _supervisor: DiscoverySupervisor,
}

struct DiscoverySupervisor {
    stop: watch::Sender<bool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for DiscoverySupervisor {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let Some(thread) = self.thread.take() else {
            return;
        };

        if thread.is_finished() {
            let _ = thread.join();
        } else {
            // Joining a thread that drives the shared Tokio runtime can block
            // that runtime's only worker. Keep cleanup explicit while moving
            // the join itself off the caller/runtime thread.
            let _ = thread::Builder::new()
                .name("curvine-discovery-reaper".to_string())
                .spawn(move || {
                    let _ = thread.join();
                });
        }
    }
}

impl ClientDiscovery {
    #[cfg(feature = "discovery-etcd")]
    pub(crate) fn start_etcd(
        runtime: Arc<Runtime>,
        config: EtcdDiscoveryConfig,
        connector: Weak<ClusterConnector>,
        initial_ready_timeout: Duration,
    ) -> FsResult<Self> {
        config
            .validate()
            .map_err(|error| fs_error(format!("invalid discovery configuration: {error}")))?;

        let config_for_factory = config.clone();
        let provider_factory: ProviderFactory = Arc::new(move |runtime| {
            let config = config_for_factory.clone();
            Box::pin(async move { curvine_service_discovery::connect_etcd(config, runtime).await })
        });

        Self::start(
            runtime,
            provider_factory,
            connector,
            ServiceKind::try_new("master").map_err(|error| fs_error(error.to_string()))?,
            config.watch_reconnect_min_ms,
            config.watch_reconnect_max_ms,
            config.allow_stale_cache,
            initial_ready_timeout,
        )
    }

    fn start(
        runtime: Arc<Runtime>,
        provider_factory: ProviderFactory,
        connector: Weak<ClusterConnector>,
        kind: ServiceKind,
        reconnect_min_ms: u64,
        reconnect_max_ms: u64,
        allow_stale_cache: bool,
        initial_ready_timeout: Duration,
    ) -> FsResult<Self> {
        let cache = Arc::new(RwLock::new(CacheState::default()));
        let (readiness_tx, readiness_rx) = watch::channel(Readiness::Starting { last_error: None });
        let (stop_tx, stop_rx) = watch::channel(false);
        let thread_cache = cache.clone();

        let thread = match thread::Builder::new()
            .name("curvine-master-discovery".to_string())
            .spawn(move || {
                supervise(
                    runtime,
                    provider_factory,
                    connector,
                    kind,
                    reconnect_min_ms,
                    reconnect_max_ms,
                    thread_cache,
                    readiness_tx,
                    stop_rx,
                );
            }) {
            Ok(thread) => thread,
            Err(error) => {
                return Err(fs_error(format!(
                    "failed to start master discovery supervisor: {error}"
                )));
            }
        };

        Ok(Self {
            cache,
            readiness: readiness_rx,
            initial_ready_timeout,
            allow_stale_cache,
            _supervisor: DiscoverySupervisor {
                stop: stop_tx,
                thread: Some(thread),
            },
        })
    }

    /// Wait for a usable initial master snapshot, then consult only the local
    /// watch cache. This never performs an etcd request on an RPC path.
    pub(crate) async fn snapshot_for_rpc(&self) -> FsResult<ServiceSnapshot> {
        let mut readiness = self.readiness.clone();
        let wait_ready = async {
            loop {
                match readiness.borrow().clone() {
                    Readiness::Ready => return Ok(()),
                    Readiness::Stopped => {
                        return Err("master discovery supervisor stopped".to_string())
                    }
                    Readiness::Starting { .. } => {}
                }

                if readiness.changed().await.is_err() {
                    return Err("master discovery supervisor stopped".to_string());
                }
            }
        };

        match tokio::time::timeout(self.initial_ready_timeout, wait_ready).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(fs_error(format!(
                    "master discovery is unavailable: {error}; retry the request"
                )));
            }
            Err(_) => {
                let cache = self.cache.read().await;
                let detail =
                    cache
                        .last_error
                        .clone()
                        .or_else(|| match readiness.borrow().clone() {
                            Readiness::Starting { last_error } => last_error,
                            _ => None,
                        });
                let suffix = detail
                    .map(|message| format!("; last discovery error: {message}"))
                    .unwrap_or_default();
                return Err(fs_error(format!(
                    "master discovery has no usable initial snapshot after {:?}{suffix}; retry the request",
                    self.initial_ready_timeout
                )));
            }
        }

        let (snapshot, reader, connected, last_error) = {
            let cache = self.cache.read().await;
            let Some(snapshot) = cache.snapshot.clone() else {
                return Err(fs_error(
                    "master discovery reported readiness without a snapshot; retry the request",
                ));
            };
            let Some(reader) = cache.reader.clone() else {
                return Err(fs_error(
                    "master discovery reported readiness without a cache reader; retry the request",
                ));
            };
            (snapshot, reader, cache.connected, cache.last_error.clone())
        };

        if !connected && !self.allow_stale_cache {
            let detail = last_error
                .as_deref()
                .unwrap_or("master discovery watch is disconnected");
            return Err(fs_error(format!(
                "master discovery cache cannot be used while disconnected: {detail}; retry the request"
            )));
        }

        match reader.snapshot().await {
            Ok(reader_snapshot)
                if self.allow_stale_cache || (!snapshot.stale && !reader_snapshot.stale) =>
            {
                Ok(snapshot)
            }
            Ok(_) => Err(fs_error(
                "master discovery cache is stale and discovery.allow_stale_cache is false; retry the request",
            )),
            Err(DiscoveryError::StaleCache) => Err(fs_error(
                "master discovery cache is stale and discovery.allow_stale_cache is false; retry the request",
            )),
            Err(error) => Err(fs_error(format!(
                "master discovery cache is unavailable: {error}; retry the request"
            ))),
        }
    }

    pub(crate) async fn ensure_ready_for_rpc(&self) -> FsResult<()> {
        let snapshot = self.snapshot_for_rpc().await?;
        match snapshot_to_nodes(&snapshot) {
            Ok(nodes) if !nodes.is_empty() => Ok(()),
            Ok(_) => Err(fs_error(
                "master discovery has no serving master endpoints; retry the request",
            )),
            Err(error) => Err(fs_error(format!(
                "master discovery snapshot is unusable: {error}; retry the request"
            ))),
        }
    }
}

fn fs_error(message: impl AsRef<str>) -> curvine_error::FsError {
    curvine_error::FsError::common(message)
}

fn supervise(
    runtime: Arc<Runtime>,
    provider_factory: ProviderFactory,
    connector: Weak<ClusterConnector>,
    kind: ServiceKind,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
    cache: Arc<RwLock<CacheState>>,
    readiness: watch::Sender<Readiness>,
    mut stop: watch::Receiver<bool>,
) {
    let mut reconnect_delay_ms = reconnect_min_ms.max(1);
    let mut initial_ready = false;

    loop {
        if *stop.borrow() {
            break;
        }

        let provider_result = runtime.block_on(async {
            tokio::select! {
                _ = stopped(&mut stop) => None,
                provider = provider_factory(runtime.clone()) => Some(provider),
            }
        });
        let Some(provider_result) = provider_result else {
            break;
        };
        let provider = match provider_result {
            Ok(provider) => provider,
            Err(error) => {
                let message = error.to_string();
                record_disconnect(&runtime, &cache, message.clone());
                if !initial_ready {
                    readiness.send_replace(Readiness::Starting {
                        last_error: Some(message),
                    });
                }
                if !wait_or_stop(&runtime, &mut stop, reconnect_delay_ms) {
                    break;
                }
                reconnect_delay_ms = reconnect_delay_ms
                    .saturating_mul(2)
                    .min(reconnect_max_ms.max(reconnect_delay_ms));
                continue;
            }
        };

        let resolver = provider.resolver.clone();
        let watch_result = runtime.block_on(async {
            let handle = tokio::select! {
                _ = stopped(&mut stop) => return None,
                handle = resolver.watch(kind.clone()) => Some(handle),
            }?;
            let handle = match handle {
                Ok(handle) => handle,
                Err(error) => return Some(Err(error.to_string())),
            };
            let (reader, events) = handle.into_detached_parts();
            let initial_snapshot = match reader.snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => return Some(Err(error.to_string())),
            };
            Some(Ok((reader, events, initial_snapshot)))
        });

        let Some(watch_result) = watch_result else {
            drop(provider);
            break;
        };
        let (reader, mut events, mut snapshot) = match watch_result {
            Ok(watch) => watch,
            Err(message) => {
                record_disconnect(&runtime, &cache, message.clone());
                if !initial_ready {
                    readiness.send_replace(Readiness::Starting {
                        last_error: Some(message),
                    });
                }
                drop(provider);
                if !wait_or_stop(&runtime, &mut stop, reconnect_delay_ms) {
                    break;
                }
                reconnect_delay_ms = reconnect_delay_ms
                    .saturating_mul(2)
                    .min(reconnect_max_ms.max(reconnect_delay_ms));
                continue;
            }
        };

        let nodes = snapshot_to_nodes(&snapshot);
        let initial_error = match nodes {
            Ok(nodes) => {
                if let Err(error) = reconcile(&runtime, &connector, nodes.clone()) {
                    Some(error)
                } else if nodes.is_empty() {
                    Some("discovery snapshot has no serving masters".to_string())
                } else {
                    None
                }
            }
            Err(error) => {
                let _ = reconcile(&runtime, &connector, Vec::new());
                Some(error)
            }
        };
        update_cache(
            &runtime,
            &cache,
            Some(reader.clone()),
            Some(snapshot.clone()),
            true,
            initial_error.clone(),
        );
        if initial_error.is_none() {
            initial_ready = true;
            reconnect_delay_ms = reconnect_min_ms.max(1);
            readiness.send_replace(Readiness::Ready);
        } else if !initial_ready {
            readiness.send_replace(Readiness::Starting {
                last_error: initial_error,
            });
        }

        let message = loop {
            enum Event {
                Stop,
                Received(Option<DiscoveryResult<ServiceWatchEvent>>),
            }
            let event = runtime.block_on(async {
                tokio::select! {
                    _ = stopped(&mut stop) => Event::Stop,
                    event = events.recv() => Event::Received(event),
                }
            });
            match event {
                Event::Stop => {
                    readiness.send_replace(Readiness::Stopped);
                    update_cache(
                        &runtime,
                        &cache,
                        None,
                        None,
                        false,
                        Some("client discovery stopped".to_string()),
                    );
                    drop(events);
                    drop(provider);
                    return;
                }
                Event::Received(None) => {
                    break "master discovery watch closed".to_string();
                }
                Event::Received(Some(Err(error))) => {
                    break error.to_string();
                }
                Event::Received(Some(Ok(event))) => match apply_event(&snapshot, event) {
                    Ok(next_snapshot) => match snapshot_to_nodes(&next_snapshot) {
                        Ok(nodes) => {
                            if let Err(error) = reconcile(&runtime, &connector, nodes.clone()) {
                                set_cache_error(&runtime, &cache, error.clone());
                                if !initial_ready {
                                    readiness.send_replace(Readiness::Starting {
                                        last_error: Some(error),
                                    });
                                }
                                continue;
                            }

                            snapshot = next_snapshot;
                            let event_error = if nodes.is_empty() {
                                Some("discovery snapshot has no serving masters".to_string())
                            } else {
                                None
                            };
                            set_cache_snapshot(
                                &runtime,
                                &cache,
                                snapshot.clone(),
                                true,
                                event_error.clone(),
                            );
                            if event_error.is_none() && !initial_ready {
                                initial_ready = true;
                                reconnect_delay_ms = reconnect_min_ms.max(1);
                                readiness.send_replace(Readiness::Ready);
                            } else if !initial_ready {
                                readiness.send_replace(Readiness::Starting {
                                    last_error: event_error,
                                });
                            }
                        }
                        Err(error) => {
                            set_cache_error(&runtime, &cache, error.clone());
                            if !initial_ready {
                                readiness.send_replace(Readiness::Starting {
                                    last_error: Some(error),
                                });
                            }
                        }
                    },
                    Err(error) => {
                        set_cache_error(&runtime, &cache, error.clone());
                        if !initial_ready {
                            readiness.send_replace(Readiness::Starting {
                                last_error: Some(error),
                            });
                        }
                    }
                },
            }
        };

        record_disconnect(&runtime, &cache, message.clone());
        if !initial_ready {
            readiness.send_replace(Readiness::Starting {
                last_error: Some(message),
            });
        }
        drop(events);
        drop(provider);
        if !wait_or_stop(&runtime, &mut stop, reconnect_delay_ms) {
            break;
        }
        reconnect_delay_ms = reconnect_delay_ms
            .saturating_mul(2)
            .min(reconnect_max_ms.max(reconnect_delay_ms));
    }

    readiness.send_replace(Readiness::Stopped);
    update_cache(
        &runtime,
        &cache,
        None,
        None,
        false,
        Some("client discovery stopped".to_string()),
    );
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

fn wait_or_stop(runtime: &Runtime, stop: &mut watch::Receiver<bool>, delay_ms: u64) -> bool {
    runtime.block_on(async {
        tokio::select! {
            _ = stopped(stop) => false,
            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => true,
        }
    })
}

fn snapshot_to_nodes(snapshot: &ServiceSnapshot) -> Result<Vec<NodeAddr>, String> {
    if snapshot.kind.as_str() != "master" {
        return Err(format!(
            "expected discovery service kind 'master', got '{}'",
            snapshot.kind
        ));
    }

    let mut nodes = Vec::new();
    for endpoint in &snapshot.endpoints {
        if endpoint.kind.as_str() != "master" {
            return Err(format!(
                "master discovery snapshot contains service kind '{}'",
                endpoint.kind
            ));
        }
        endpoint
            .validate()
            .map_err(|error| format!("invalid master endpoint '{}': {error}", endpoint.id))?;
        if endpoint.status != ServiceStatus::Serving {
            continue;
        }
        let node_id = endpoint.id.parse::<u64>().map_err(|error| {
            format!(
                "master service id '{}' is not a stable Raft node id: {error}",
                endpoint.id
            )
        })?;
        if node_id == ClusterConnector::DEFAULT_LEADER_ID {
            return Err(format!(
                "master service id '{}' is reserved for the no-leader sentinel",
                endpoint.id
            ));
        }
        nodes.push(NodeAddr::new(
            node_id,
            endpoint.host.clone(),
            endpoint.rpc_port,
        ));
    }
    Ok(nodes)
}

fn apply_event(
    snapshot: &ServiceSnapshot,
    event: ServiceWatchEvent,
) -> Result<ServiceSnapshot, String> {
    let mut updated = snapshot.clone();
    match event {
        ServiceWatchEvent::Reset(snapshot) => {
            if snapshot.kind.as_str() != "master" {
                return Err(format!(
                    "master discovery received reset for service kind '{}'",
                    snapshot.kind
                ));
            }
            if snapshot.revision < updated.revision {
                return Err(format!(
                    "master discovery reset revision {} is older than current revision {}",
                    snapshot.revision, updated.revision
                ));
            }
            updated = snapshot;
        }
        ServiceWatchEvent::Added(endpoint) | ServiceWatchEvent::Updated(endpoint) => {
            if endpoint.kind.as_str() != "master" {
                return Err(format!(
                    "master discovery received endpoint with kind '{}'",
                    endpoint.kind
                ));
            }
            updated
                .endpoints
                .retain(|current| current.id != endpoint.id);
            updated.endpoints.push(endpoint);
        }
        ServiceWatchEvent::Removed { kind, id } => {
            if kind.as_str() != "master" {
                return Err(format!(
                    "master discovery received removal for service kind '{}'",
                    kind
                ));
            }
            updated.endpoints.retain(|endpoint| endpoint.id != id);
        }
    }
    Ok(updated)
}

fn reconcile(
    runtime: &Runtime,
    connector: &Weak<ClusterConnector>,
    nodes: Vec<NodeAddr>,
) -> Result<(), String> {
    let Some(connector) = connector.upgrade() else {
        return Err("client context was dropped".to_string());
    };
    // This is synchronous and never crosses an await. The supervisor thread's
    // Runtime Arc remains alive until after this strong connector reference is
    // dropped, so it cannot become the last runtime owner on a Tokio worker.
    let result = connector
        .reconcile_nodes(nodes)
        .map_err(|error| error.to_string());
    drop(connector);
    let _keep_runtime_owned_until_after_connector = runtime;
    result
}

fn update_cache(
    runtime: &Runtime,
    cache: &Arc<RwLock<CacheState>>,
    reader: Option<SnapshotReader>,
    snapshot: Option<ServiceSnapshot>,
    connected: bool,
    last_error: Option<String>,
) {
    runtime.block_on(async {
        let mut cache = cache.write().await;
        if reader.is_some() {
            cache.reader = reader;
        }
        if snapshot.is_some() {
            cache.snapshot = snapshot;
        }
        cache.connected = connected;
        cache.last_error = last_error;
    });
}

fn set_cache_snapshot(
    runtime: &Runtime,
    cache: &Arc<RwLock<CacheState>>,
    snapshot: ServiceSnapshot,
    connected: bool,
    last_error: Option<String>,
) {
    runtime.block_on(async {
        let mut cache = cache.write().await;
        cache.snapshot = Some(snapshot);
        cache.connected = connected;
        cache.last_error = last_error;
    });
}

fn set_cache_error(runtime: &Runtime, cache: &Arc<RwLock<CacheState>>, error: String) {
    runtime.block_on(async {
        let mut cache = cache.write().await;
        cache.connected = false;
        cache.last_error = Some(error);
    });
}

fn record_disconnect(runtime: &Runtime, cache: &Arc<RwLock<CacheState>>, message: String) {
    runtime.block_on(async {
        let mut cache = cache.write().await;
        cache.connected = false;
        cache.last_error = Some(message);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use curvine_proto::ComponentInfoProto;
    use curvine_rpc::client::ClientConf;
    use curvine_service_discovery::{
        DiscoveryProvider, RegistrationGuard, RegistrationOptions, ServiceEndpoint,
        ServiceRegistry, ServiceResolver, ServiceResolverHandle,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    struct TestRegistry;

    #[async_trait]
    impl ServiceRegistry for TestRegistry {
        async fn register(
            &self,
            _endpoint: ServiceEndpoint,
            _options: RegistrationOptions,
        ) -> DiscoveryResult<RegistrationGuard> {
            Err(DiscoveryError::ProviderUnavailable(
                "test registry is unused".to_string(),
            ))
        }
    }

    struct TestResolver {
        snapshot: ServiceSnapshot,
        allow_stale_cache: bool,
        events: Mutex<Option<mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>>>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for TestResolver {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ServiceResolver for TestResolver {
        async fn list(&self, kind: ServiceKind) -> DiscoveryResult<ServiceSnapshot> {
            if kind != self.snapshot.kind {
                return Err(DiscoveryError::InvalidServiceKind(kind.to_string()));
            }
            Ok(self.snapshot.clone())
        }

        async fn watch(&self, kind: ServiceKind) -> DiscoveryResult<ServiceResolverHandle> {
            if kind != self.snapshot.kind {
                return Err(DiscoveryError::InvalidServiceKind(kind.to_string()));
            }
            let events = self.events.lock().unwrap().take().ok_or_else(|| {
                DiscoveryError::EtcdUnavailable("watch already taken".to_string())
            })?;
            Ok(ServiceResolverHandle::new(
                kind,
                SnapshotReader::new(self.snapshot.clone(), self.allow_stale_cache),
                events,
            ))
        }
    }

    fn test_endpoint(id: &str, host: &str, port: u16) -> ServiceEndpoint {
        ServiceEndpoint {
            kind: ServiceKind::try_new("master").unwrap(),
            id: id.to_string(),
            host: host.to_string(),
            rpc_port: port,
            web_port: None,
            component_info: ComponentInfoProto {
                component: Some("master".to_string()),
                ..Default::default()
            },
            start_time_ms: 1,
            status: ServiceStatus::Serving,
            metadata: None,
        }
    }

    fn test_snapshot(endpoints: Vec<ServiceEndpoint>) -> ServiceSnapshot {
        ServiceSnapshot {
            kind: ServiceKind::try_new("master").unwrap(),
            revision: 1,
            stale: false,
            last_update_ms: 1,
            endpoints,
        }
    }

    fn start_test_discovery(
        snapshot: ServiceSnapshot,
        initial_peers: Vec<NodeAddr>,
        allow_stale_reader: bool,
        allow_stale_client: bool,
        initial_ready_timeout: Duration,
    ) -> (
        Arc<Runtime>,
        Arc<ClusterConnector>,
        ClientDiscovery,
        mpsc::Sender<DiscoveryResult<ServiceWatchEvent>>,
        Arc<AtomicBool>,
    ) {
        let runtime = Arc::new(Runtime::new("client-discovery-test", 2, 2));
        let connector = Arc::new(ClusterConnector::with_rt(
            ClientConf::default(),
            runtime.clone(),
        ));
        connector.reconcile_nodes(initial_peers).unwrap();
        let (event_tx, event_rx) = mpsc::channel(16);
        let dropped = Arc::new(AtomicBool::new(false));
        let resolver = Arc::new(TestResolver {
            snapshot,
            allow_stale_cache: allow_stale_reader,
            events: Mutex::new(Some(event_rx)),
            dropped: dropped.clone(),
        });
        let factory: ProviderFactory = Arc::new(move |_runtime| {
            let resolver: Arc<dyn ServiceResolver> = resolver.clone();
            Box::pin(async move {
                Ok(DiscoveryProvider {
                    registry: Arc::new(TestRegistry),
                    resolver,
                })
            })
        });
        let discovery = ClientDiscovery::start(
            runtime.clone(),
            factory,
            Arc::downgrade(&connector),
            ServiceKind::try_new("master").unwrap(),
            5,
            20,
            allow_stale_client,
            initial_ready_timeout,
        )
        .unwrap();
        (runtime, connector, discovery, event_tx, dropped)
    }

    async fn wait_for<F>(mut condition: F)
    where
        F: FnMut() -> bool,
    {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if condition() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    #[test]
    fn initial_empty_snapshot_has_no_static_fallback_and_watch_updates_reconcile_membership() {
        let (runtime, connector, discovery, events, _dropped) = start_test_discovery(
            test_snapshot(Vec::new()),
            vec![NodeAddr::new(99, "127.0.0.99", 8999)],
            false,
            false,
            Duration::from_millis(60),
        );
        runtime.block_on(async {
            wait_for(|| connector.peer_snapshot().is_empty()).await;
            let error = discovery.snapshot_for_rpc().await.unwrap_err();
            assert!(error.to_string().contains("initial snapshot"));
            assert!(connector.peer_snapshot().is_empty());

            events
                .send(Ok(ServiceWatchEvent::Added(test_endpoint(
                    "41",
                    "127.0.0.41",
                    9041,
                ))))
                .await
                .unwrap();
            discovery.snapshot_for_rpc().await.unwrap();
            assert_eq!(
                connector.peer_snapshot(),
                vec![NodeAddr::new(41, "127.0.0.41", 9041)]
            );

            connector.change_leader(41);
            events
                .send(Ok(ServiceWatchEvent::Updated(test_endpoint(
                    "41",
                    "127.0.0.42",
                    9142,
                ))))
                .await
                .unwrap();
            wait_for(|| connector.peer_snapshot() == vec![NodeAddr::new(41, "127.0.0.42", 9142)])
                .await;
            assert_eq!(connector.leader_id(), None);

            events
                .send(Ok(ServiceWatchEvent::Added(test_endpoint(
                    "42",
                    "127.0.0.43",
                    9143,
                ))))
                .await
                .unwrap();
            wait_for(|| connector.peer_snapshot().len() == 2).await;

            events
                .send(Ok(ServiceWatchEvent::Removed {
                    kind: ServiceKind::try_new("master").unwrap(),
                    id: "41".to_string(),
                }))
                .await
                .unwrap();
            wait_for(|| connector.peer_snapshot() == vec![NodeAddr::new(42, "127.0.0.43", 9143)])
                .await;
            assert!(discovery.ensure_ready_for_rpc().await.is_ok());
            assert_eq!(connector.peer_snapshot().len(), 1);
        });
        drop(discovery);
        drop(connector);
        drop(runtime);
    }

    #[test]
    fn disconnected_discovery_rejects_rpc_when_stale_cache_is_disabled() {
        let (runtime, connector, discovery, events, _dropped) = start_test_discovery(
            test_snapshot(vec![test_endpoint("51", "127.0.0.51", 9051)]),
            Vec::new(),
            false,
            false,
            Duration::from_millis(250),
        );
        runtime.block_on(async {
            discovery.snapshot_for_rpc().await.unwrap();
            events
                .send(Err(DiscoveryError::EtcdUnavailable(
                    "watch connection lost".to_string(),
                )))
                .await
                .unwrap();
            wait_for(|| {
                discovery
                    .cache
                    .try_read()
                    .is_ok_and(|cache| !cache.connected)
            })
            .await;
            let error = discovery.ensure_ready_for_rpc().await.unwrap_err();
            assert!(error.to_string().contains("disconnected"));
        });
        drop(discovery);
        drop(connector);
        drop(runtime);
    }

    #[test]
    fn stale_initial_snapshot_obeys_the_resolver_stale_cache_policy() {
        let mut stale_snapshot = test_snapshot(vec![test_endpoint("61", "127.0.0.61", 9061)]);
        stale_snapshot.stale = true;

        let (runtime, connector, discovery, _events, _dropped) = start_test_discovery(
            stale_snapshot.clone(),
            Vec::new(),
            false,
            false,
            Duration::from_millis(40),
        );
        runtime.block_on(async {
            let error = discovery.snapshot_for_rpc().await.unwrap_err();
            assert!(error.to_string().contains("initial snapshot"));
        });
        drop(discovery);
        drop(connector);
        drop(runtime);

        let (runtime, connector, discovery, _events, _dropped) = start_test_discovery(
            stale_snapshot,
            Vec::new(),
            true,
            true,
            Duration::from_millis(250),
        );
        runtime.block_on(async {
            assert!(discovery.snapshot_for_rpc().await.unwrap().stale);
            discovery.ensure_ready_for_rpc().await.unwrap();
            assert_eq!(connector.peer_snapshot().len(), 1);
        });
        drop(discovery);
        drop(connector);
        drop(runtime);
    }

    #[test]
    fn stale_rpc_gate_cannot_clobber_membership_from_a_newer_watch_update() {
        let old_endpoint = test_endpoint("71", "127.0.0.71", 9071);
        let (runtime, connector, discovery, events, _dropped) = start_test_discovery(
            test_snapshot(vec![old_endpoint.clone()]),
            Vec::new(),
            false,
            false,
            Duration::from_millis(250),
        );

        runtime.block_on(async {
            discovery.snapshot_for_rpc().await.unwrap();
            events
                .send(Ok(ServiceWatchEvent::Updated(test_endpoint(
                    "71",
                    "127.0.0.72",
                    9172,
                ))))
                .await
                .unwrap();
            let current_peer = NodeAddr::new(71, "127.0.0.72", 9172);
            wait_for(|| connector.peer_snapshot() == vec![current_peer.clone()]).await;

            // Model a caller that observed an older cache entry just as the
            // supervisor had already reconciled a newer watch update.
            {
                let mut cache = discovery.cache.write().await;
                cache.snapshot = Some(test_snapshot(vec![old_endpoint]));
                cache.connected = false;
            }
            assert!(discovery.ensure_ready_for_rpc().await.is_err());
            assert_eq!(connector.peer_snapshot(), vec![current_peer]);
        });
        drop(discovery);
        drop(connector);
        drop(runtime);
    }

    #[test]
    fn reset_events_cannot_roll_back_the_cached_revision() {
        let current = test_snapshot(Vec::new());
        let mut older = test_snapshot(Vec::new());
        older.revision = current.revision - 1;
        let error = apply_event(&current, ServiceWatchEvent::Reset(older)).unwrap_err();
        assert!(error.contains("older than current revision"));
    }

    #[test]
    fn dropping_client_discovery_stops_and_releases_its_provider() {
        let (runtime, connector, discovery, _events, provider_dropped) = start_test_discovery(
            test_snapshot(vec![test_endpoint("81", "127.0.0.81", 9081)]),
            Vec::new(),
            false,
            false,
            Duration::from_millis(250),
        );
        runtime.block_on(async {
            discovery.snapshot_for_rpc().await.unwrap();
        });

        drop(discovery);
        runtime.block_on(wait_for(|| provider_dropped.load(Ordering::SeqCst)));
        drop(connector);
        drop(runtime);
    }
}
