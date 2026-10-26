use crate::{
    DiscoveryError, DiscoveryResult, RegistrationGuard, RegistrationOptions, RegistrationStatus,
    ServiceEndpoint, ServiceRegistry, ServiceStatus,
};
use std::cmp::min;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};

/// Retry bounds for the initial registration and subsequent lease recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationRetryPolicy {
    /// The startup path fails if registration is not acquired within this time.
    pub startup_timeout_ms: u64,
    /// Minimum delay between registration attempts.
    pub retry_min_ms: u64,
    /// Maximum delay between registration attempts.
    pub retry_max_ms: u64,
}

impl Default for RegistrationRetryPolicy {
    fn default() -> Self {
        Self {
            startup_timeout_ms: 10_000,
            retry_min_ms: 1_000,
            retry_max_ms: 30_000,
        }
    }
}

impl RegistrationRetryPolicy {
    fn validate(self) -> DiscoveryResult<()> {
        if self.startup_timeout_ms == 0 {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "startup_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.retry_min_ms == 0 {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "retry_min_ms must be > 0".to_string(),
            ));
        }
        if self.retry_max_ms < self.retry_min_ms {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "retry_max_ms must be >= retry_min_ms".to_string(),
            ));
        }
        Ok(())
    }

    fn retry_min(self) -> Duration {
        Duration::from_millis(self.retry_min_ms)
    }

    fn retry_max(self) -> Duration {
        Duration::from_millis(self.retry_max_ms)
    }

    #[cfg(feature = "etcd")]
    pub fn from_discovery_conf(conf: &curvine_config::DiscoveryConf) -> Self {
        Self {
            startup_timeout_ms: conf
                .connect_timeout_ms
                .saturating_add(conf.request_timeout_ms),
            retry_min_ms: conf.watch_reconnect_min_ms,
            retry_max_ms: conf.watch_reconnect_max_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationSupervisorStatus {
    Registered,
    Reconnecting { last_error: String },
    Stopping,
    Stopped,
}

/// Owns one service registration and re-acquires it after terminal lease loss.
///
/// The current guard is private to the supervisor task, so a retired guard
/// cannot outlive the lifecycle and revoke a later registration with the same
/// service identity. Call [`shutdown`](Self::shutdown) for graceful revocation;
/// dropping the handle requests cancellation but cannot await the revoke.
pub struct RegistrationSupervisor {
    stop_tx: watch::Sender<bool>,
    status_rx: watch::Receiver<RegistrationSupervisorStatus>,
    desired_status_tx: watch::Sender<ServiceStatus>,
    task: Option<JoinHandle<DiscoveryResult<()>>>,
    // Keep the provider alive outside the async supervisor task. Some
    // providers own their runtime, and dropping the last runtime handle from
    // inside that runtime's task can panic during cleanup.
    _provider_owner: Arc<dyn ServiceRegistry>,
}

struct SupervisorControl {
    stop_rx: watch::Receiver<bool>,
    desired_status_rx: watch::Receiver<ServiceStatus>,
}

impl RegistrationSupervisor {
    /// Registers once before returning, with bounded retries for startup.
    ///
    /// This async constructor must run on an active Tokio runtime because it
    /// starts the lease monitor task after the initial registration succeeds.
    pub async fn start(
        registry: Arc<dyn ServiceRegistry>,
        endpoint: ServiceEndpoint,
        registration_options: RegistrationOptions,
        retry_policy: RegistrationRetryPolicy,
    ) -> DiscoveryResult<Self> {
        endpoint.validate()?;
        registration_options.validate()?;
        retry_policy.validate()?;

        let guard = Self::register_with_startup_retry(
            registry.as_ref(),
            endpoint.clone(),
            registration_options.clone(),
            retry_policy,
        )
        .await?;

        let (stop_tx, stop_rx) = watch::channel(false);
        let (status_tx, status_rx) = watch::channel(RegistrationSupervisorStatus::Registered);
        let (desired_status_tx, desired_status_rx) = watch::channel(endpoint.status.clone());
        let task = tokio::spawn(supervise_registration(
            registry.clone(),
            endpoint,
            registration_options,
            retry_policy,
            guard,
            SupervisorControl {
                stop_rx,
                desired_status_rx,
            },
            status_tx,
        ));

        Ok(Self {
            stop_tx,
            status_rx,
            desired_status_tx,
            task: Some(task),
            _provider_owner: registry,
        })
    }

    pub fn status(&self) -> RegistrationSupervisorStatus {
        self.status_rx.borrow().clone()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<RegistrationSupervisorStatus> {
        self.status_rx.clone()
    }

    /// Change the status this service should eventually publish.
    ///
    /// The update is delivered locally without waiting for a provider request.
    /// The supervisor applies it with a bounded request timeout, reports
    /// `Reconnecting` while an update is failing, and retains the desired value
    /// for the next registration attempt if the current lease is lost.
    pub fn set_desired_status(&self, status: ServiceStatus) {
        self.desired_status_tx.send_replace(status);
    }

    /// Cancels recovery, marks the endpoint draining, and explicitly revokes it.
    /// Repeated calls are safe.
    pub async fn shutdown(&mut self) -> DiscoveryResult<()> {
        let _ = self.stop_tx.send(true);
        match self.task.take() {
            Some(task) => task
                .await
                .map_err(|error| DiscoveryError::RegistrationSupervisorFailed(error.to_string()))?,
            None => Ok(()),
        }
    }

    async fn register_with_startup_retry(
        registry: &dyn ServiceRegistry,
        endpoint: ServiceEndpoint,
        registration_options: RegistrationOptions,
        retry_policy: RegistrationRetryPolicy,
    ) -> DiscoveryResult<RegistrationGuard> {
        let startup_timeout = Duration::from_millis(retry_policy.startup_timeout_ms);
        let deadline = Instant::now().checked_add(startup_timeout).ok_or_else(|| {
            DiscoveryError::InvalidRegistrationOptions(
                "startup_timeout_ms is too large".to_string(),
            )
        })?;
        let register_timeout = Duration::from_millis(registration_options.register_timeout_ms);
        let mut delay = retry_policy.retry_min();
        let mut last_error = "registration attempt was cancelled".to_string();

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DiscoveryError::RegistrationStartupTimeout {
                    timeout_ms: retry_policy.startup_timeout_ms,
                    last_error,
                });
            }

            match timeout(
                min(register_timeout, remaining),
                registry.register(endpoint.clone(), registration_options.clone()),
            )
            .await
            {
                Ok(Ok(guard)) => return Ok(guard),
                Ok(Err(error)) => last_error = error.to_string(),
                Err(_) => last_error = "registration attempt timed out".to_string(),
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DiscoveryError::RegistrationStartupTimeout {
                    timeout_ms: retry_policy.startup_timeout_ms,
                    last_error,
                });
            }

            sleep(min(delay, remaining)).await;
            delay = next_delay(delay, retry_policy.retry_max());
        }
    }
}

impl Drop for RegistrationSupervisor {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
    }
}

async fn supervise_registration(
    registry: Arc<dyn ServiceRegistry>,
    mut endpoint: ServiceEndpoint,
    registration_options: RegistrationOptions,
    retry_policy: RegistrationRetryPolicy,
    mut guard: RegistrationGuard,
    control: SupervisorControl,
    status_tx: watch::Sender<RegistrationSupervisorStatus>,
) -> DiscoveryResult<()> {
    let SupervisorControl {
        mut stop_rx,
        mut desired_status_rx,
    } = control;
    let mut retry_delay = retry_policy.retry_min();
    // A timeout or provider error leaves the remote value uncertain: the
    // provider may have committed the write before its response was lost.
    let mut status_synced = true;

    loop {
        let mut registration_status = guard.subscribe_status();
        loop {
            if *stop_rx.borrow() {
                return shutdown_guard(guard, &status_tx).await;
            }

            let current_status = registration_status.borrow().clone();
            match current_status {
                RegistrationStatus::Registered => {
                    let desired_status = desired_status_rx.borrow().clone();
                    if status_synced && endpoint.status == desired_status {
                        if !matches!(
                            *status_tx.borrow(),
                            RegistrationSupervisorStatus::Registered
                        ) {
                            let _ = status_tx.send(RegistrationSupervisorStatus::Registered);
                            retry_delay = retry_policy.retry_min();
                        }
                        tokio::select! {
                            changed = stop_rx.changed() => {
                                if changed.is_err() || *stop_rx.borrow() {
                                    return shutdown_guard(guard, &status_tx).await;
                                }
                            }
                            changed = registration_status.changed() => {
                                if changed.is_err() {
                                    return shutdown_guard(guard, &status_tx).await;
                                }
                            }
                            changed = desired_status_rx.changed() => {
                                if changed.is_err() {
                                    return shutdown_guard(guard, &status_tx).await;
                                }
                            }
                        }
                        continue;
                    }

                    let register_timeout =
                        Duration::from_millis(registration_options.register_timeout_ms);
                    status_synced = false;
                    let update = tokio::select! {
                        changed = stop_rx.changed() => {
                            if changed.is_err() || *stop_rx.borrow() {
                                return shutdown_guard(guard, &status_tx).await;
                            }
                            continue;
                        }
                        changed = registration_status.changed() => {
                            if changed.is_err() {
                                return shutdown_guard(guard, &status_tx).await;
                            }
                            continue;
                        }
                        changed = desired_status_rx.changed() => {
                            if changed.is_err() {
                                return shutdown_guard(guard, &status_tx).await;
                            }
                            continue;
                        }
                        result = timeout(register_timeout, guard.update_status(desired_status.clone())) => result,
                    };

                    match update {
                        Ok(Ok(())) => {
                            endpoint.status = desired_status;
                            status_synced = true;
                            retry_delay = retry_policy.retry_min();
                            let _ = status_tx.send(RegistrationSupervisorStatus::Registered);
                        }
                        Ok(Err(error)) => {
                            let _ = status_tx.send(RegistrationSupervisorStatus::Reconnecting {
                                last_error: error.to_string(),
                            });
                            retry_delay = next_delay(retry_delay, retry_policy.retry_max());
                            tokio::select! {
                                changed = stop_rx.changed() => {
                                    if changed.is_err() || *stop_rx.borrow() {
                                        return shutdown_guard(guard, &status_tx).await;
                                    }
                                }
                                changed = registration_status.changed() => {
                                    if changed.is_err() {
                                        return shutdown_guard(guard, &status_tx).await;
                                    }
                                }
                                changed = desired_status_rx.changed() => {
                                    if changed.is_err() {
                                        return shutdown_guard(guard, &status_tx).await;
                                    }
                                }
                                _ = sleep(retry_delay) => {}
                            }
                        }
                        Err(_) => {
                            let _ = status_tx.send(RegistrationSupervisorStatus::Reconnecting {
                                last_error: "service status update timed out".to_string(),
                            });
                            retry_delay = next_delay(retry_delay, retry_policy.retry_max());
                            tokio::select! {
                                changed = stop_rx.changed() => {
                                    if changed.is_err() || *stop_rx.borrow() {
                                        return shutdown_guard(guard, &status_tx).await;
                                    }
                                }
                                changed = registration_status.changed() => {
                                    if changed.is_err() {
                                        return shutdown_guard(guard, &status_tx).await;
                                    }
                                }
                                changed = desired_status_rx.changed() => {
                                    if changed.is_err() {
                                        return shutdown_guard(guard, &status_tx).await;
                                    }
                                }
                                _ = sleep(retry_delay) => {}
                            }
                        }
                    }
                }
                RegistrationStatus::KeepAliveLost { message } => {
                    let _ = status_tx.send(RegistrationSupervisorStatus::Reconnecting {
                        last_error: message,
                    });
                    // A provider may still be able to revoke a lease whose
                    // keepalive stream was lost. If it cannot, lease expiry is
                    // bounded by the configured TTL and registration retries
                    // continue until the key becomes available.
                    let _ = guard.shutdown().await;
                    break;
                }
                RegistrationStatus::Revoking | RegistrationStatus::Revoked => {
                    return shutdown_guard(guard, &status_tx).await;
                }
            }
        }

        loop {
            if *stop_rx.borrow() {
                let _ = status_tx.send(RegistrationSupervisorStatus::Stopped);
                return Ok(());
            }

            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        let _ = status_tx.send(RegistrationSupervisorStatus::Stopped);
                        return Ok(());
                    }
                }
                changed = desired_status_rx.changed() => {
                    if changed.is_err() {
                        let _ = status_tx.send(RegistrationSupervisorStatus::Stopped);
                        return Ok(());
                    }
                }
                _ = sleep(retry_delay) => {}
            }

            let register_timeout = Duration::from_millis(registration_options.register_timeout_ms);
            endpoint.status = desired_status_rx.borrow().clone();
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        let _ = status_tx.send(RegistrationSupervisorStatus::Stopped);
                        return Ok(());
                    }
                }
                changed = desired_status_rx.changed() => {
                    if changed.is_err() {
                        let _ = status_tx.send(RegistrationSupervisorStatus::Stopped);
                        return Ok(());
                    }
                    continue;
                }
                result = timeout(
                    register_timeout,
                    registry.register(endpoint.clone(), registration_options.clone()),
                ) => {
                    match result {
                        Ok(Ok(new_guard)) => {
                            if *stop_rx.borrow() {
                                return shutdown_guard(new_guard, &status_tx).await;
                            }
                            guard = new_guard;
                            status_synced = true;
                            retry_delay = retry_policy.retry_min();
                            let _ = status_tx.send(RegistrationSupervisorStatus::Registered);
                            break;
                        }
                        Ok(Err(error)) => {
                            let last_error = error.to_string();
                            let _ = status_tx.send(RegistrationSupervisorStatus::Reconnecting {
                                last_error: last_error.clone(),
                            });
                            retry_delay = next_delay(retry_delay, retry_policy.retry_max());
                        }
                        Err(_) => {
                            let _ = status_tx.send(RegistrationSupervisorStatus::Reconnecting {
                                last_error: "registration attempt timed out".to_string(),
                            });
                            retry_delay = next_delay(retry_delay, retry_policy.retry_max());
                        }
                    }
                }
            }
        }
    }
}

async fn shutdown_guard(
    guard: RegistrationGuard,
    status_tx: &watch::Sender<RegistrationSupervisorStatus>,
) -> DiscoveryResult<()> {
    let _ = status_tx.send(RegistrationSupervisorStatus::Stopping);
    let _ = guard.update_status(ServiceStatus::Draining).await;
    let result = guard.shutdown().await;
    let _ = status_tx.send(RegistrationSupervisorStatus::Stopped);
    result
}

fn next_delay(current: Duration, maximum: Duration) -> Duration {
    min(current.saturating_mul(2), maximum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RegistrationControl, ServiceKind};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use tokio::sync::{watch, Mutex};

    #[derive(Default)]
    struct TestRegistryState {
        calls: AtomicUsize,
        next_lease_id: AtomicI64,
        fail_next: AtomicUsize,
        fail_status_updates: AtomicUsize,
        shutdowns: AtomicUsize,
        active: Mutex<HashMap<String, i64>>,
        endpoints: Mutex<HashMap<String, ServiceEndpoint>>,
        status_senders: Mutex<Vec<watch::Sender<RegistrationStatus>>>,
    }

    #[derive(Clone, Default)]
    struct TestRegistry(Arc<TestRegistryState>);

    impl TestRegistry {
        fn fail_next(&self, count: usize) {
            self.0.fail_next.store(count, Ordering::SeqCst);
        }

        fn fail_status_updates(&self, count: usize) {
            self.0.fail_status_updates.store(count, Ordering::SeqCst);
        }

        async fn lose_latest(&self) {
            let sender = self.0.status_senders.lock().await.last().unwrap().clone();
            let _ = sender.send(RegistrationStatus::KeepAliveLost {
                message: "test lease loss".to_string(),
            });
        }

        fn calls(&self) -> usize {
            self.0.calls.load(Ordering::SeqCst)
        }

        fn shutdowns(&self) -> usize {
            self.0.shutdowns.load(Ordering::SeqCst)
        }

        async fn active_lease_id(&self, endpoint: &ServiceEndpoint) -> Option<i64> {
            let key = format!("{}/{}", endpoint.kind, endpoint.id);
            self.0.active.lock().await.get(&key).copied()
        }

        async fn active_status(&self, endpoint: &ServiceEndpoint) -> Option<ServiceStatus> {
            let key = format!("{}/{}", endpoint.kind, endpoint.id);
            self.0
                .endpoints
                .lock()
                .await
                .get(&key)
                .map(|endpoint| endpoint.status.clone())
        }

        fn consume_failure(&self) -> bool {
            self.0
                .fail_next
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        }

        fn consume_status_update_failure(&self) -> bool {
            self.fail_status_updates
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        }
    }

    struct TestRegistrationControl {
        state: Arc<TestRegistryState>,
        key: String,
        lease_id: i64,
        status_tx: watch::Sender<RegistrationStatus>,
    }

    #[async_trait]
    impl RegistrationControl for TestRegistrationControl {
        async fn update_endpoint(&self, _endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
            Ok(())
        }

        async fn update_status(&self, status: ServiceStatus) -> DiscoveryResult<()> {
            if self.state.consume_status_update_failure() {
                return Err(DiscoveryError::EtcdUnavailable(
                    "injected status update failure".to_string(),
                ));
            }
            if let Some(endpoint) = self.state.endpoints.lock().await.get_mut(&self.key) {
                endpoint.status = status;
            }
            Ok(())
        }

        async fn shutdown(&self) -> DiscoveryResult<()> {
            self.state.shutdowns.fetch_add(1, Ordering::SeqCst);
            let mut active = self.state.active.lock().await;
            if active.get(&self.key) == Some(&self.lease_id) {
                active.remove(&self.key);
            }
            self.state.endpoints.lock().await.remove(&self.key);
            let _ = self.status_tx.send(RegistrationStatus::Revoked);
            Ok(())
        }
    }

    #[async_trait]
    impl ServiceRegistry for TestRegistry {
        async fn register(
            &self,
            endpoint: ServiceEndpoint,
            options: RegistrationOptions,
        ) -> DiscoveryResult<RegistrationGuard> {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            options.validate()?;
            endpoint.validate()?;
            if self.consume_failure() {
                return Err(DiscoveryError::EtcdUnavailable(
                    "injected registration failure".to_string(),
                ));
            }

            let key = format!("{}/{}", endpoint.kind, endpoint.id);
            let lease_id = self.0.next_lease_id.fetch_add(1, Ordering::SeqCst) + 1;
            let mut active = self.0.active.lock().await;
            if active.contains_key(&key) {
                return Err(DiscoveryError::RegistrationAlreadyExists(key));
            }
            active.insert(key.clone(), lease_id);
            drop(active);
            self.0
                .endpoints
                .lock()
                .await
                .insert(key.clone(), endpoint.clone());

            let (status_tx, status_rx) = watch::channel(RegistrationStatus::Registered);
            self.0.status_senders.lock().await.push(status_tx.clone());
            let control = Arc::new(TestRegistrationControl {
                state: self.0.clone(),
                key,
                lease_id,
                status_tx,
            });
            Ok(RegistrationGuard::new(
                endpoint.kind,
                endpoint.id,
                lease_id,
                status_rx,
                control,
            ))
        }
    }

    fn test_endpoint() -> ServiceEndpoint {
        ServiceEndpoint {
            kind: ServiceKind::try_new("worker").unwrap(),
            id: "worker-1-session-1".to_string(),
            host: "worker.default.svc".to_string(),
            rpc_port: 9100,
            web_port: None,
            component_info: curvine_proto::ComponentInfoProto {
                component: Some("worker".to_string()),
                release_version: Some("test".to_string()),
                git_commit: Some("test".to_string()),
                git_tag: Some(String::new()),
                git_branch: Some("test".to_string()),
                protocol_version: Some(1),
                min_protocol_version: Some(1),
                capabilities: Vec::new(),
            },
            start_time_ms: 1,
            status: ServiceStatus::Starting,
            metadata: None,
        }
    }

    fn test_retry_policy() -> RegistrationRetryPolicy {
        RegistrationRetryPolicy {
            startup_timeout_ms: 250,
            retry_min_ms: 5,
            retry_max_ms: 20,
        }
    }

    #[tokio::test]
    async fn supervisor_re_registers_after_terminal_lease_loss() {
        let registry = TestRegistry::default();
        let mut supervisor = RegistrationSupervisor::start(
            Arc::new(registry.clone()),
            test_endpoint(),
            RegistrationOptions::default(),
            test_retry_policy(),
        )
        .await
        .unwrap();
        registry.fail_next(1);
        registry.lose_latest().await;

        let mut status = supervisor.subscribe_status();
        timeout(Duration::from_secs(1), async {
            loop {
                if registry.calls() >= 3
                    && matches!(*status.borrow(), RegistrationSupervisorStatus::Registered)
                {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        assert_eq!(registry.calls(), 3);
        supervisor.shutdown().await.unwrap();
        supervisor.shutdown().await.unwrap();
        assert_eq!(registry.shutdowns(), 2);
        assert_eq!(supervisor.status(), RegistrationSupervisorStatus::Stopped);
    }

    #[tokio::test]
    async fn desired_status_updates_a_starting_registration_to_serving() {
        let registry = TestRegistry::default();
        let endpoint = test_endpoint();
        let mut supervisor = RegistrationSupervisor::start(
            Arc::new(registry.clone()),
            endpoint.clone(),
            RegistrationOptions::default(),
            test_retry_policy(),
        )
        .await
        .unwrap();

        assert_eq!(
            registry.active_status(&endpoint).await,
            Some(ServiceStatus::Starting)
        );
        supervisor.set_desired_status(ServiceStatus::Serving);

        timeout(Duration::from_secs(1), async {
            loop {
                if registry.active_status(&endpoint).await == Some(ServiceStatus::Serving) {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        supervisor.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn desired_status_retries_and_survives_lease_recovery() {
        let registry = TestRegistry::default();
        let endpoint = test_endpoint();
        let mut supervisor = RegistrationSupervisor::start(
            Arc::new(registry.clone()),
            endpoint.clone(),
            RegistrationOptions::default(),
            test_retry_policy(),
        )
        .await
        .unwrap();
        registry.fail_status_updates(usize::MAX);
        supervisor.set_desired_status(ServiceStatus::Serving);

        let mut status = supervisor.subscribe_status();
        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    *status.borrow(),
                    RegistrationSupervisorStatus::Reconnecting { .. }
                ) {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            registry.active_status(&endpoint).await,
            Some(ServiceStatus::Starting)
        );

        registry.fail_status_updates(0);
        timeout(Duration::from_secs(1), async {
            loop {
                if registry.active_status(&endpoint).await == Some(ServiceStatus::Serving)
                    && matches!(*status.borrow(), RegistrationSupervisorStatus::Registered)
                {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        registry.lose_latest().await;
        timeout(Duration::from_secs(1), async {
            loop {
                if registry.calls() >= 2
                    && registry.active_status(&endpoint).await == Some(ServiceStatus::Serving)
                    && matches!(*status.borrow(), RegistrationSupervisorStatus::Registered)
                {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        supervisor.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_retries_during_backoff() {
        let registry = TestRegistry::default();
        let mut supervisor = RegistrationSupervisor::start(
            Arc::new(registry.clone()),
            test_endpoint(),
            RegistrationOptions::default(),
            RegistrationRetryPolicy {
                startup_timeout_ms: 250,
                retry_min_ms: 200,
                retry_max_ms: 400,
            },
        )
        .await
        .unwrap();
        registry.fail_next(usize::MAX);
        registry.lose_latest().await;

        let mut status = supervisor.subscribe_status();
        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    *status.borrow(),
                    RegistrationSupervisorStatus::Reconnecting { .. }
                ) {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        let calls_before_shutdown = registry.calls();
        supervisor.shutdown().await.unwrap();
        sleep(Duration::from_millis(250)).await;

        assert_eq!(registry.calls(), calls_before_shutdown);
        assert_eq!(supervisor.status(), RegistrationSupervisorStatus::Stopped);
    }

    #[tokio::test]
    async fn startup_registration_failure_is_bounded() {
        let registry = TestRegistry::default();
        registry.fail_next(usize::MAX);
        let started = Instant::now();
        let error = RegistrationSupervisor::start(
            Arc::new(registry),
            test_endpoint(),
            RegistrationOptions::default(),
            RegistrationRetryPolicy {
                startup_timeout_ms: 30,
                retry_min_ms: 5,
                retry_max_ms: 10,
            },
        )
        .await
        .err()
        .expect("registration retries must time out");

        assert!(matches!(
            error,
            DiscoveryError::RegistrationStartupTimeout { .. }
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn stopped_supervisor_cannot_revoke_replacement_registration() {
        let registry = TestRegistry::default();
        let endpoint = test_endpoint();
        let mut stopped = RegistrationSupervisor::start(
            Arc::new(registry.clone()),
            endpoint.clone(),
            RegistrationOptions::default(),
            test_retry_policy(),
        )
        .await
        .unwrap();
        registry.lose_latest().await;

        let mut status = stopped.subscribe_status();
        timeout(Duration::from_secs(1), async {
            loop {
                if registry.calls() >= 2
                    && matches!(*status.borrow(), RegistrationSupervisorStatus::Registered)
                {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        stopped.shutdown().await.unwrap();
        let mut replacement = RegistrationSupervisor::start(
            Arc::new(registry.clone()),
            endpoint.clone(),
            RegistrationOptions::default(),
            test_retry_policy(),
        )
        .await
        .unwrap();
        let replacement_lease = registry.active_lease_id(&endpoint).await.unwrap();

        stopped.shutdown().await.unwrap();

        assert_eq!(
            registry.active_lease_id(&endpoint).await,
            Some(replacement_lease)
        );
        replacement.shutdown().await.unwrap();
    }

    #[test]
    fn retry_delay_doubles_and_caps() {
        assert_eq!(
            next_delay(Duration::from_millis(10), Duration::from_millis(25)),
            Duration::from_millis(20)
        );
        assert_eq!(
            next_delay(Duration::from_millis(20), Duration::from_millis(25)),
            Duration::from_millis(25)
        );
        assert_eq!(
            next_delay(
                Duration::from_millis(u64::MAX),
                Duration::from_millis(u64::MAX)
            ),
            Duration::from_millis(u64::MAX)
        );
    }
}
