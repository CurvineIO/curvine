use crate::registry::RegistrationControl;
use crate::{
    decode_endpoint_value_for_key, encode_endpoint_value, DiscoveryError, DiscoveryResult,
    RegistrationGuard, RegistrationOptions, RegistrationStatus, ServiceEndpoint, ServiceKey,
    ServiceKind, ServiceRegistry, ServiceResolver, ServiceResolverHandle, ServiceSnapshot,
    ServiceStatus, ServiceWatchEvent, SnapshotReader,
};
use async_trait::async_trait;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, LeaseKeepAliveStream,
    LeaseKeeper, PutOptions, Txn, TxnOp, WatchOptions,
};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::timeout;

static WATCH_JITTER_SEED: AtomicU64 = AtomicU64::new(1);
const KEEPALIVE_FAILURE_THRESHOLD: u32 = 3;

#[derive(Debug, Clone)]
pub struct EtcdDiscoveryConfig {
    pub endpoints: Vec<String>,
    pub prefix: String,
    pub cluster_id: String,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub watch_reconnect_min_ms: u64,
    pub watch_reconnect_max_ms: u64,
    pub watch_reconnect_jitter_ratio: f64,
    pub allow_stale_cache: bool,
}

#[async_trait]
impl ServiceRegistry for EtcdServiceResolver {
    async fn register(
        &self,
        endpoint: ServiceEndpoint,
        options: RegistrationOptions,
    ) -> DiscoveryResult<RegistrationGuard> {
        endpoint.validate()?;
        options.validate()?;
        let lease_ttl_secs = i64::try_from(options.lease_ttl_secs).map_err(|_| {
            DiscoveryError::InvalidRegistrationOptions(
                "lease_ttl_secs exceeds i64::MAX".to_string(),
            )
        })?;

        let key = ServiceKey::new(
            &self.config.prefix,
            &self.config.cluster_id,
            endpoint.kind.clone(),
            &endpoint.id,
        )?
        .as_string();
        let value = encode_endpoint_value(&endpoint)?;
        let mut client = connect_client(&self.config).await?;
        let timeout_duration = Duration::from_millis(options.register_timeout_ms);
        let lease = with_registration_timeout(
            timeout_duration,
            client.lease_grant(lease_ttl_secs, None),
            "grant lease",
        )
        .await?;
        let lease_id = lease.id();

        let txn = Txn::new()
            .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(
                key.clone(),
                value,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let txn_response = match with_registration_timeout(
            timeout_duration,
            client.txn(txn),
            "put registration endpoint",
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                revoke_lease_best_effort(&mut client, lease_id).await;
                return Err(error);
            }
        };
        if !txn_response.succeeded() {
            revoke_lease_best_effort(&mut client, lease_id).await;
            return Err(DiscoveryError::RegistrationAlreadyExists(key.clone()));
        }

        let (keeper, stream) = match with_registration_timeout(
            timeout_duration,
            client.lease_keep_alive(lease_id),
            "create lease keepalive",
        )
        .await
        {
            Ok(keepalive) => keepalive,
            Err(error) => {
                revoke_lease_best_effort(&mut client, lease_id).await;
                return Err(error);
            }
        };

        let kind = endpoint.kind.clone();
        let service_id = endpoint.id.clone();
        let (status_tx, status_rx) = watch::channel(RegistrationStatus::Registered);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let control = Arc::new(EtcdRegistrationControl {
            key,
            lease_id,
            client: Mutex::new(client),
            endpoint: Mutex::new(endpoint),
            status_tx: status_tx.clone(),
            shutdown_tx,
        });

        self.rt.spawn(keepalive_loop(
            kind.clone(),
            service_id.clone(),
            options.keep_alive_interval_secs,
            self.config.request_timeout_ms,
            keeper,
            stream,
            status_tx,
            shutdown_rx,
        ));

        Ok(RegistrationGuard {
            kind,
            service_id,
            lease_id,
            status_rx,
            control,
            _lifecycle_owner: Some(self.rt.clone()),
        })
    }
}

struct EtcdRegistrationControl {
    key: String,
    lease_id: i64,
    client: Mutex<Client>,
    endpoint: Mutex<ServiceEndpoint>,
    status_tx: watch::Sender<RegistrationStatus>,
    shutdown_tx: watch::Sender<bool>,
}

#[async_trait]
impl RegistrationControl for EtcdRegistrationControl {
    async fn update_endpoint(&self, endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
        let value = encode_endpoint_value(&endpoint)?;
        let mut current = self.endpoint.lock().await;
        let mut client = self.client.lock().await;
        put_endpoint_if_owned(&mut client, &self.key, value, self.lease_id).await?;
        *current = endpoint;
        Ok(())
    }

    async fn update_status(&self, status: ServiceStatus) -> DiscoveryResult<()> {
        let mut endpoint = self.endpoint.lock().await;
        let mut updated = endpoint.clone();
        updated.status = status;
        let value = encode_endpoint_value(&updated)?;
        let mut client = self.client.lock().await;
        put_endpoint_if_owned(&mut client, &self.key, value, self.lease_id).await?;
        *endpoint = updated;
        Ok(())
    }

    async fn shutdown(&self) -> DiscoveryResult<()> {
        let _ = self.status_tx.send(RegistrationStatus::Revoking);
        let _ = self.shutdown_tx.send(true);
        let mut client = self.client.lock().await;
        client
            .lease_revoke(self.lease_id)
            .await
            .map_err(DiscoveryError::from)?;
        let _ = self.status_tx.send(RegistrationStatus::Revoked);
        Ok(())
    }
}

async fn put_endpoint_if_owned(
    client: &mut Client,
    key: &str,
    value: String,
    lease_id: i64,
) -> DiscoveryResult<()> {
    let txn = Txn::new()
        .when(vec![Compare::lease(key, CompareOp::Equal, lease_id)])
        .and_then(vec![TxnOp::put(
            key,
            value,
            Some(PutOptions::new().with_lease(lease_id)),
        )]);
    let response = client.txn(txn).await.map_err(DiscoveryError::from)?;
    if response.succeeded() {
        Ok(())
    } else {
        Err(DiscoveryError::RegistrationLost(format!(
            "registration no longer owns etcd key {key} with lease {lease_id}"
        )))
    }
}

impl Drop for EtcdRegistrationControl {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl EtcdDiscoveryConfig {
    pub fn new(
        endpoints: Vec<String>,
        prefix: impl Into<String>,
        cluster_id: impl Into<String>,
    ) -> Self {
        Self {
            endpoints,
            prefix: prefix.into(),
            cluster_id: cluster_id.into(),
            connect_timeout_ms: 3000,
            request_timeout_ms: 3000,
            watch_reconnect_min_ms: 1000,
            watch_reconnect_max_ms: 30000,
            watch_reconnect_jitter_ratio: 0.2,
            allow_stale_cache: true,
        }
    }

    pub fn validate(&self) -> DiscoveryResult<()> {
        if self.endpoints.is_empty()
            || self
                .endpoints
                .iter()
                .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(DiscoveryError::InvalidDiscoveryConfig(
                "etcd endpoints must not be empty".to_string(),
            ));
        }
        ServiceKey::service_prefix(
            &self.prefix,
            &self.cluster_id,
            &ServiceKind::try_new("probe")?,
        )?;
        if self.connect_timeout_ms == 0 {
            return Err(DiscoveryError::InvalidDiscoveryConfig(
                "connect_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.request_timeout_ms == 0 {
            return Err(DiscoveryError::InvalidDiscoveryConfig(
                "request_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.watch_reconnect_min_ms == 0 {
            return Err(DiscoveryError::InvalidDiscoveryConfig(
                "watch_reconnect_min_ms must be > 0".to_string(),
            ));
        }
        if self.watch_reconnect_max_ms < self.watch_reconnect_min_ms {
            return Err(DiscoveryError::InvalidDiscoveryConfig(
                "watch_reconnect_max_ms must be >= watch_reconnect_min_ms".to_string(),
            ));
        }
        if !(0.0..=1.0).contains(&self.watch_reconnect_jitter_ratio) {
            return Err(DiscoveryError::InvalidDiscoveryConfig(
                "watch_reconnect_jitter_ratio must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(())
    }

    fn connect_options(&self) -> ConnectOptions {
        ConnectOptions::new()
            .with_connect_timeout(Duration::from_millis(self.connect_timeout_ms))
            .with_timeout(Duration::from_millis(self.request_timeout_ms))
    }
}

impl From<(&curvine_config::DiscoveryConf, &str)> for EtcdDiscoveryConfig {
    fn from((conf, cluster_id): (&curvine_config::DiscoveryConf, &str)) -> Self {
        Self {
            endpoints: conf.endpoints.clone(),
            prefix: conf.prefix.clone(),
            cluster_id: cluster_id.to_string(),
            connect_timeout_ms: conf.connect_timeout_ms,
            request_timeout_ms: conf.request_timeout_ms,
            watch_reconnect_min_ms: conf.watch_reconnect_min_ms,
            watch_reconnect_max_ms: conf.watch_reconnect_max_ms,
            watch_reconnect_jitter_ratio: conf.watch_reconnect_jitter_ratio,
            allow_stale_cache: conf.allow_stale_cache,
        }
    }
}

pub struct EtcdServiceResolver {
    config: EtcdDiscoveryConfig,
    rt: Arc<Runtime>,
    client: Arc<Mutex<Client>>,
}

pub type EtcdServiceRegistry = EtcdServiceResolver;

impl EtcdServiceResolver {
    pub async fn connect(config: EtcdDiscoveryConfig, rt: Arc<Runtime>) -> DiscoveryResult<Self> {
        config.validate()?;
        let client = connect_client(&config).await?;
        Ok(Self {
            config,
            rt,
            client: Arc::new(Mutex::new(client)),
        })
    }

    async fn list_with_client(
        client: &mut Client,
        config: &EtcdDiscoveryConfig,
        kind: ServiceKind,
    ) -> DiscoveryResult<ServiceSnapshot> {
        let service_prefix = ServiceKey::service_prefix(&config.prefix, &config.cluster_id, &kind)?;
        let response = client
            .get(service_prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        let revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or(0);
        let mut endpoints = Vec::new();
        for kv in response.kvs() {
            let key = match kv.key_str() {
                Ok(key) => key,
                Err(error) => {
                    log::warn!("drop discovery endpoint with invalid utf-8 key: error={error}");
                    continue;
                }
            };
            match decode_endpoint_value_for_key(&config.prefix, key, kv.value()) {
                Ok(endpoint) if endpoint.kind == kind => endpoints.push(endpoint),
                Ok(_) => {}
                Err(error) => {
                    log::warn!(
                        "drop invalid discovery endpoint from list: key={key}, error={error}"
                    );
                }
            }
        }
        Ok(ServiceSnapshot {
            kind,
            revision,
            stale: false,
            last_update_ms: now_millis(),
            endpoints,
        })
    }
}

#[async_trait]
impl ServiceResolver for EtcdServiceResolver {
    async fn list(&self, kind: ServiceKind) -> DiscoveryResult<ServiceSnapshot> {
        let mut client = self.client.lock().await;
        Self::list_with_client(&mut client, &self.config, kind).await
    }

    async fn watch(&self, kind: ServiceKind) -> DiscoveryResult<ServiceResolverHandle> {
        let snapshot = self.list(kind.clone()).await?;
        let reader = SnapshotReader::with_lifecycle_owner(
            snapshot.clone(),
            self.config.allow_stale_cache,
            Some(self.rt.clone()),
        );
        let (tx, rx) = mpsc::channel(128);
        tx.send(Ok(ServiceWatchEvent::Reset(snapshot.clone())))
            .await
            .map_err(|error| DiscoveryError::EtcdUnavailable(error.to_string()))?;

        let config = self.config.clone();
        let reader_for_task = reader.clone();
        let watch_kind = kind.clone();
        let jitter_seed = WATCH_JITTER_SEED.fetch_add(1, Ordering::Relaxed);
        self.rt.spawn(async move {
            watch_loop(
                config,
                watch_kind,
                snapshot.revision + 1,
                reader_for_task,
                tx,
                jitter_seed,
            )
            .await;
        });

        Ok(ServiceResolverHandle::with_lifecycle_owner(
            kind,
            reader,
            rx,
            Some(self.rt.clone()),
        ))
    }
}

async fn connect_client(config: &EtcdDiscoveryConfig) -> DiscoveryResult<Client> {
    Client::connect(config.endpoints.clone(), Some(config.connect_options()))
        .await
        .map_err(DiscoveryError::from)
}

async fn with_registration_timeout<T, F>(
    duration: Duration,
    future: F,
    operation: &'static str,
) -> DiscoveryResult<T>
where
    F: Future<Output = Result<T, etcd_client::Error>>,
{
    timeout(duration, future)
        .await
        .map_err(|_| DiscoveryError::EtcdUnavailable(format!("{operation} timed out")))?
        .map_err(DiscoveryError::from)
}

async fn revoke_lease_best_effort(client: &mut Client, lease_id: i64) {
    if let Err(error) = client.lease_revoke(lease_id).await {
        log::warn!("failed to revoke discovery lease after register failure: lease_id={lease_id}, error={error}");
    }
}

#[allow(clippy::too_many_arguments)]
async fn keepalive_loop(
    kind: ServiceKind,
    service_id: String,
    keep_alive_interval_secs: u64,
    request_timeout_ms: u64,
    mut keeper: LeaseKeeper,
    mut stream: LeaseKeepAliveStream,
    status_tx: watch::Sender<RegistrationStatus>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(keep_alive_interval_secs));
    let request_timeout = Duration::from_millis(request_timeout_ms);
    let mut consecutive_failures = 0_u32;

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    result = keeper.keep_alive() => {
                        if let Err(error) = result {
                            if record_keepalive_failure(
                                &status_tx,
                                &kind,
                                &service_id,
                                &mut consecutive_failures,
                                error.to_string(),
                            ) {
                                return;
                            }
                            continue;
                        }
                    }
                }
                let keepalive_response = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                        continue;
                    }
                    result = timeout(request_timeout, stream.message()) => result,
                };
                match keepalive_response {
                    Ok(Ok(Some(response))) if response.ttl() > 0 => {
                        consecutive_failures = 0;
                    }
                    Ok(Ok(Some(response))) => {
                        mark_keepalive_lost(
                            &status_tx,
                            &kind,
                            &service_id,
                            format!("lease keepalive returned non-positive ttl: {}", response.ttl()),
                        );
                        return;
                    }
                    Ok(Ok(None)) => {
                        mark_keepalive_lost(
                            &status_tx,
                            &kind,
                            &service_id,
                            "lease keepalive stream closed".to_string(),
                        );
                        return;
                    }
                    Ok(Err(error)) => {
                        if record_keepalive_failure(
                            &status_tx,
                            &kind,
                            &service_id,
                            &mut consecutive_failures,
                            error.to_string(),
                        ) {
                            return;
                        }
                    }
                    Err(_) => {
                        if record_keepalive_failure(
                            &status_tx,
                            &kind,
                            &service_id,
                            &mut consecutive_failures,
                            "lease keepalive response timed out".to_string(),
                        ) {
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn record_keepalive_failure(
    status_tx: &watch::Sender<RegistrationStatus>,
    kind: &ServiceKind,
    service_id: &str,
    consecutive_failures: &mut u32,
    message: String,
) -> bool {
    *consecutive_failures = consecutive_failures.saturating_add(1);
    log::warn!(
        "etcd discovery registration keepalive failure: kind={}, service_id={}, failures={}, error={}",
        kind,
        service_id,
        *consecutive_failures,
        message
    );
    if *consecutive_failures < KEEPALIVE_FAILURE_THRESHOLD {
        false
    } else {
        mark_keepalive_lost(status_tx, kind, service_id, message);
        true
    }
}

fn mark_keepalive_lost(
    status_tx: &watch::Sender<RegistrationStatus>,
    kind: &ServiceKind,
    service_id: &str,
    message: String,
) {
    log::warn!(
        "etcd discovery registration keepalive lost: kind={}, service_id={}, error={}",
        kind,
        service_id,
        message
    );
    let _ = status_tx.send(RegistrationStatus::KeepAliveLost { message });
}

async fn watch_loop(
    config: EtcdDiscoveryConfig,
    kind: ServiceKind,
    mut start_revision: i64,
    reader: SnapshotReader,
    tx: mpsc::Sender<DiscoveryResult<ServiceWatchEvent>>,
    jitter_seed: u64,
) {
    let mut reconnect_attempt = 0_u32;

    loop {
        if tx.is_closed() {
            return;
        }

        match watch_once(
            config.clone(),
            kind.clone(),
            start_revision,
            reader.clone(),
            tx.clone(),
        )
        .await
        {
            Ok(()) => return,
            Err(DiscoveryError::WatchCompacted { revision }) => {
                reader.mark_stale().await;
                log::warn!(
                    "etcd discovery watch compacted; refreshing snapshot: kind={}, revision={}",
                    kind,
                    revision
                );
                match refresh_watch_snapshot(&config, kind.clone(), reader.clone(), tx.clone())
                    .await
                {
                    Ok(revision) => {
                        start_revision = revision + 1;
                        reconnect_attempt = 0;
                    }
                    Err(error) => {
                        if tx.is_closed() {
                            return;
                        }
                        reconnect_attempt = reconnect_attempt.saturating_add(1);
                        log::warn!(
                            "etcd discovery snapshot refresh failed; retrying: kind={}, attempt={}, error={}",
                            kind,
                            reconnect_attempt,
                            error
                        );
                        if !sleep_reconnect_delay(&config, reconnect_attempt, jitter_seed, &tx)
                            .await
                        {
                            return;
                        }
                    }
                }
            }
            Err(error) => {
                if tx.is_closed() {
                    return;
                }
                reader.mark_stale().await;
                let snapshot = reader.cached_snapshot().await;
                start_revision = snapshot.revision.saturating_add(1);
                reconnect_attempt = reconnect_attempt.saturating_add(1);
                log::warn!(
                    "etcd discovery watch interrupted; reconnecting: kind={}, revision={}, attempt={}, error={}",
                    kind,
                    start_revision,
                    reconnect_attempt,
                    error
                );
                if !sleep_reconnect_delay(&config, reconnect_attempt, jitter_seed, &tx).await {
                    return;
                }
            }
        }
    }
}

async fn refresh_watch_snapshot(
    config: &EtcdDiscoveryConfig,
    kind: ServiceKind,
    reader: SnapshotReader,
    tx: mpsc::Sender<DiscoveryResult<ServiceWatchEvent>>,
) -> DiscoveryResult<i64> {
    let mut client = connect_client(config).await?;
    let snapshot = EtcdServiceResolver::list_with_client(&mut client, config, kind).await?;
    let revision = snapshot.revision;
    reader.replace_snapshot(snapshot.clone()).await;
    tx.send(Ok(ServiceWatchEvent::Reset(snapshot)))
        .await
        .map_err(|_| DiscoveryError::EtcdUnavailable("watch receiver closed".to_string()))?;
    Ok(revision)
}

async fn sleep_reconnect_delay(
    config: &EtcdDiscoveryConfig,
    attempt: u32,
    jitter_seed: u64,
    tx: &mpsc::Sender<DiscoveryResult<ServiceWatchEvent>>,
) -> bool {
    tokio::select! {
        _ = tx.closed() => false,
        _ = tokio::time::sleep(Duration::from_millis(reconnect_delay_ms(
            config,
            attempt,
            jitter_seed,
        ))) => true,
    }
}

fn reconnect_delay_ms(config: &EtcdDiscoveryConfig, attempt: u32, jitter_seed: u64) -> u64 {
    let shift = attempt.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    let base = config
        .watch_reconnect_min_ms
        .saturating_mul(multiplier)
        .min(config.watch_reconnect_max_ms);
    let jitter_bound = (base as f64 * config.watch_reconnect_jitter_ratio).round() as u64;
    if jitter_bound == 0 {
        base
    } else {
        let jitter_denominator = jitter_bound.saturating_add(1);
        if jitter_denominator == 0 {
            return config.watch_reconnect_max_ms;
        }
        let jitter = now_millis()
            .wrapping_add(jitter_seed)
            .wrapping_add(u64::from(attempt))
            % jitter_denominator;
        base.saturating_add(jitter)
            .min(config.watch_reconnect_max_ms)
    }
}

async fn watch_once(
    config: EtcdDiscoveryConfig,
    kind: ServiceKind,
    start_revision: i64,
    reader: SnapshotReader,
    tx: mpsc::Sender<DiscoveryResult<ServiceWatchEvent>>,
) -> DiscoveryResult<()> {
    let service_prefix = ServiceKey::service_prefix(&config.prefix, &config.cluster_id, &kind)?;
    let mut client = connect_client(&config).await?;
    let mut stream = client
        .watch(
            service_prefix,
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(start_revision)
                    .with_progress_notify(),
            ),
        )
        .await?;
    let mut snapshot = reader.cached_snapshot().await;
    let mut watch_confirmed = false;

    loop {
        let response = tokio::select! {
            _ = tx.closed() => return Ok(()),
            message = stream.message() => message?,
        };
        let Some(response) = response else {
            break;
        };

        if response.canceled() {
            let compact_revision = response.compact_revision();
            if compact_revision > 0 {
                log::warn!(
                    "etcd discovery watch compacted: kind={}, revision={}",
                    kind,
                    compact_revision
                );
                return Err(DiscoveryError::WatchCompacted {
                    revision: compact_revision,
                });
            }
            log::warn!(
                "etcd discovery watch canceled: kind={}, reason={}",
                kind,
                response.cancel_reason()
            );
            return Err(DiscoveryError::EtcdUnavailable(format!(
                "watch canceled: {}",
                response.cancel_reason()
            )));
        }

        let events = response.events();
        if let Some(header) = response.header() {
            snapshot.revision = header.revision();
        }
        for event in events {
            match event.event_type() {
                EventType::Put => {
                    let Some(kv) = event.kv() else {
                        continue;
                    };
                    let key = match kv.key_str() {
                        Ok(key) => key,
                        Err(error) => {
                            log::warn!("drop discovery watch put event with invalid utf-8 key: error={error}");
                            continue;
                        }
                    };
                    let endpoint = match decode_endpoint_value_for_key(
                        &config.prefix,
                        key,
                        kv.value(),
                    ) {
                        Ok(endpoint) => endpoint,
                        Err(error) => {
                            log::warn!(
                                "drop invalid discovery endpoint from watch: key={key}, error={error}"
                            );
                            continue;
                        }
                    };
                    let watch_event = if upsert_endpoint(&mut snapshot.endpoints, endpoint.clone())
                    {
                        ServiceWatchEvent::Updated(endpoint)
                    } else {
                        ServiceWatchEvent::Added(endpoint)
                    };
                    snapshot.stale = false;
                    snapshot.last_update_ms = now_millis();
                    reader.replace_snapshot(snapshot.clone()).await;
                    if tx.send(Ok(watch_event)).await.is_err() {
                        return Ok(());
                    }
                }
                EventType::Delete => {
                    let Some(kv) = event.kv() else {
                        continue;
                    };
                    let key = match kv.key_str() {
                        Ok(key) => key,
                        Err(error) => {
                            log::warn!("drop discovery watch delete event with invalid utf-8 key: error={error}");
                            continue;
                        }
                    };
                    let parsed_key = match ServiceKey::parse(&config.prefix, key) {
                        Ok(parsed_key) => parsed_key,
                        Err(error) => {
                            log::warn!(
                                "drop invalid discovery delete event: key={key}, error={error}"
                            );
                            continue;
                        }
                    };
                    if parsed_key.kind != kind {
                        continue;
                    }
                    remove_endpoint(&mut snapshot.endpoints, &parsed_key.service_id);
                    snapshot.stale = false;
                    snapshot.last_update_ms = now_millis();
                    reader.replace_snapshot(snapshot.clone()).await;
                    if tx
                        .send(Ok(ServiceWatchEvent::Removed {
                            kind: parsed_key.kind,
                            id: parsed_key.service_id,
                        }))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
        }
        if !watch_confirmed {
            snapshot.stale = false;
            snapshot.last_update_ms = now_millis();
            reader.replace_snapshot(snapshot.clone()).await;
            watch_confirmed = true;
        }
    }

    log::warn!("etcd discovery watch stream closed: kind={kind}");
    Err(DiscoveryError::EtcdUnavailable(
        "watch stream closed".to_string(),
    ))
}

fn upsert_endpoint(endpoints: &mut Vec<ServiceEndpoint>, endpoint: ServiceEndpoint) -> bool {
    if let Some(existing) = endpoints
        .iter_mut()
        .find(|existing| existing.id == endpoint.id)
    {
        *existing = endpoint;
        true
    } else {
        endpoints.push(endpoint);
        false
    }
}

fn remove_endpoint(endpoints: &mut Vec<ServiceEndpoint>, service_id: &str) {
    endpoints.retain(|endpoint| endpoint.id != service_id);
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::ComponentInfoProto;

    fn endpoint(id: &str) -> ServiceEndpoint {
        ServiceEndpoint {
            kind: ServiceKind::try_new("mds").unwrap(),
            id: id.to_string(),
            host: "mds.default.svc".to_string(),
            rpc_port: 9100,
            web_port: None,
            component_info: ComponentInfoProto {
                component: Some("mds".to_string()),
                release_version: Some("0.2.0".to_string()),
                git_commit: Some("abcdef".to_string()),
                git_tag: Some(String::new()),
                git_branch: Some("main".to_string()),
                protocol_version: Some(1),
                min_protocol_version: Some(1),
                capabilities: Vec::new(),
            },
            start_time_ms: 1,
            status: crate::ServiceStatus::Serving,
            metadata: None,
        }
    }

    #[test]
    fn upsert_endpoint_reports_added_then_updated() {
        let mut endpoints = Vec::new();
        assert!(!upsert_endpoint(&mut endpoints, endpoint("mds-1")));
        assert!(upsert_endpoint(&mut endpoints, endpoint("mds-1")));
        assert_eq!(endpoints.len(), 1);
    }

    #[test]
    fn remove_endpoint_drops_matching_id() {
        let mut endpoints = vec![endpoint("mds-1"), endpoint("mds-2")];
        remove_endpoint(&mut endpoints, "mds-1");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].id, "mds-2");
    }

    #[test]
    fn reconnect_delay_uses_exponential_backoff_and_cap() {
        let mut config = EtcdDiscoveryConfig::new(
            vec!["http://127.0.0.1:2379".to_string()],
            "/curvine",
            "test-cluster",
        );
        config.watch_reconnect_min_ms = 100;
        config.watch_reconnect_max_ms = 500;
        config.watch_reconnect_jitter_ratio = 0.0;

        assert_eq!(reconnect_delay_ms(&config, 1, 0), 100);
        assert_eq!(reconnect_delay_ms(&config, 2, 0), 200);
        assert_eq!(reconnect_delay_ms(&config, 3, 0), 400);
        assert_eq!(reconnect_delay_ms(&config, 4, 0), 500);
        assert_eq!(reconnect_delay_ms(&config, 64, 0), 500);
    }

    #[test]
    fn reconnect_delay_handles_max_jitter_without_overflow() {
        let mut config = EtcdDiscoveryConfig::new(
            vec!["http://127.0.0.1:2379".to_string()],
            "/curvine",
            "test-cluster",
        );
        config.watch_reconnect_min_ms = u64::MAX;
        config.watch_reconnect_max_ms = u64::MAX;
        config.watch_reconnect_jitter_ratio = 1.0;

        assert_eq!(reconnect_delay_ms(&config, 1, 0), u64::MAX);
    }

    #[test]
    fn config_rejects_invalid_reconnect_values() {
        let mut config = EtcdDiscoveryConfig::new(
            vec!["http://127.0.0.1:2379".to_string()],
            "/curvine",
            "test-cluster",
        );
        config.watch_reconnect_min_ms = 0;
        assert!(matches!(
            config.validate(),
            Err(DiscoveryError::InvalidDiscoveryConfig(_))
        ));

        let mut config = EtcdDiscoveryConfig::new(
            vec!["http://127.0.0.1:2379".to_string()],
            "/curvine",
            "test-cluster",
        );
        config.watch_reconnect_min_ms = 1000;
        config.watch_reconnect_max_ms = 999;
        assert!(config.validate().is_err());

        let mut config = EtcdDiscoveryConfig::new(
            vec!["http://127.0.0.1:2379".to_string()],
            "/curvine",
            "test-cluster",
        );
        config.watch_reconnect_jitter_ratio = f64::NAN;
        assert!(config.validate().is_err());
    }
}
