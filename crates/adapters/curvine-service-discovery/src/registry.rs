use crate::{DiscoveryError, DiscoveryResult, ServiceEndpoint, ServiceKind, ServiceStatus};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::watch;

#[async_trait]
pub trait ServiceRegistry: Send + Sync {
    async fn register(
        &self,
        endpoint: ServiceEndpoint,
        options: RegistrationOptions,
    ) -> DiscoveryResult<RegistrationGuard>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationOptions {
    pub lease_ttl_secs: u64,
    pub keep_alive_interval_secs: u64,
    pub register_timeout_ms: u64,
}

impl Default for RegistrationOptions {
    fn default() -> Self {
        Self {
            lease_ttl_secs: 10,
            keep_alive_interval_secs: 3,
            register_timeout_ms: 5000,
        }
    }
}

impl RegistrationOptions {
    pub fn validate(&self) -> DiscoveryResult<()> {
        if self.lease_ttl_secs == 0 {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "lease_ttl_secs must be > 0".to_string(),
            ));
        }
        if self.keep_alive_interval_secs == 0 {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "keep_alive_interval_secs must be > 0".to_string(),
            ));
        }
        let min_lease_ttl_secs = self
            .keep_alive_interval_secs
            .checked_mul(3)
            .ok_or_else(|| {
                DiscoveryError::InvalidRegistrationOptions(
                    "keep_alive_interval_secs * 3 overflows u64".to_string(),
                )
            })?;
        if self.lease_ttl_secs < min_lease_ttl_secs {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "lease_ttl_secs must be >= keep_alive_interval_secs * 3".to_string(),
            ));
        }
        if self.register_timeout_ms == 0 {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "register_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.lease_ttl_secs > i64::MAX as u64 {
            return Err(DiscoveryError::InvalidRegistrationOptions(
                "lease_ttl_secs must fit in i64".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationStatus {
    Registered,
    KeepAliveLost { message: String },
    Revoking,
    Revoked,
}

#[async_trait]
pub trait RegistrationControl: Send + Sync {
    async fn update_endpoint(&self, endpoint: ServiceEndpoint) -> DiscoveryResult<()>;
    async fn update_status(&self, status: ServiceStatus) -> DiscoveryResult<()>;
    async fn shutdown(&self) -> DiscoveryResult<()>;
}

pub struct RegistrationGuard {
    pub(crate) kind: ServiceKind,
    pub(crate) service_id: String,
    pub(crate) lease_id: i64,
    pub(crate) status_rx: watch::Receiver<RegistrationStatus>,
    pub(crate) control: Arc<dyn RegistrationControl>,
    pub(crate) _lifecycle_owner: Option<Arc<dyn Send + Sync>>,
}

impl RegistrationGuard {
    pub fn new(
        kind: ServiceKind,
        service_id: impl Into<String>,
        lease_id: i64,
        status_rx: watch::Receiver<RegistrationStatus>,
        control: Arc<dyn RegistrationControl>,
    ) -> Self {
        Self::with_lifecycle_owner(kind, service_id, lease_id, status_rx, control, None)
    }

    pub fn with_lifecycle_owner(
        kind: ServiceKind,
        service_id: impl Into<String>,
        lease_id: i64,
        status_rx: watch::Receiver<RegistrationStatus>,
        control: Arc<dyn RegistrationControl>,
        lifecycle_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            service_id: service_id.into(),
            lease_id,
            status_rx,
            control,
            _lifecycle_owner: lifecycle_owner,
        }
    }

    pub fn kind(&self) -> &ServiceKind {
        &self.kind
    }

    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }

    pub fn subscribe_status(&self) -> watch::Receiver<RegistrationStatus> {
        self.status_rx.clone()
    }

    pub async fn update_endpoint(&self, endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
        self.ensure_registered()?;
        self.validate_endpoint_identity(&endpoint)?;
        endpoint.validate()?;
        self.control.update_endpoint(endpoint).await
    }

    pub async fn update_status(&self, status: ServiceStatus) -> DiscoveryResult<()> {
        self.ensure_registered()?;
        self.control.update_status(status).await
    }

    pub async fn shutdown(&self) -> DiscoveryResult<()> {
        if matches!(&*self.status_rx.borrow(), RegistrationStatus::Revoked) {
            return Ok(());
        }
        self.control.shutdown().await
    }

    fn ensure_registered(&self) -> DiscoveryResult<()> {
        match &*self.status_rx.borrow() {
            RegistrationStatus::KeepAliveLost { message } => {
                Err(DiscoveryError::RegistrationLost(message.clone()))
            }
            RegistrationStatus::Revoking => Err(DiscoveryError::RegistrationLost(
                "registration is revoking".to_string(),
            )),
            RegistrationStatus::Revoked => Err(DiscoveryError::RegistrationLost(
                "registration has been revoked".to_string(),
            )),
            _ => Ok(()),
        }
    }

    fn validate_endpoint_identity(&self, endpoint: &ServiceEndpoint) -> DiscoveryResult<()> {
        if endpoint.kind != self.kind || endpoint.id != self.service_id {
            return Err(DiscoveryError::InvalidEndpointValue(format!(
                "endpoint kind/id must match registration guard: expected {}/{}, got {}/{}",
                self.kind, self.service_id, endpoint.kind, endpoint.id
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::ComponentInfoProto;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoopControl;

    #[async_trait]
    impl RegistrationControl for NoopControl {
        async fn update_endpoint(&self, _endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
            Ok(())
        }

        async fn update_status(&self, _status: ServiceStatus) -> DiscoveryResult<()> {
            Ok(())
        }

        async fn shutdown(&self) -> DiscoveryResult<()> {
            Ok(())
        }
    }

    struct CountingControl {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RegistrationControl for CountingControl {
        async fn update_endpoint(&self, _endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
            Ok(())
        }

        async fn update_status(&self, _status: ServiceStatus) -> DiscoveryResult<()> {
            Ok(())
        }

        async fn shutdown(&self) -> DiscoveryResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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
            component_info: ComponentInfoProto {
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

    fn guard(status: RegistrationStatus) -> RegistrationGuard {
        let (_tx, rx) = watch::channel(status);
        RegistrationGuard {
            kind: ServiceKind::try_new("mds").unwrap(),
            service_id: "mds-1".to_string(),
            lease_id: 10,
            status_rx: rx,
            control: Arc::new(NoopControl),
            _lifecycle_owner: None,
        }
    }

    #[test]
    fn registration_options_validate_defaults_and_invalid_values() {
        assert!(RegistrationOptions::default().validate().is_ok());

        let options = RegistrationOptions {
            lease_ttl_secs: 8,
            ..Default::default()
        };
        assert!(options.validate().is_err());

        let options = RegistrationOptions {
            keep_alive_interval_secs: 0,
            ..Default::default()
        };
        assert!(options.validate().is_err());

        let options = RegistrationOptions {
            register_timeout_ms: 0,
            ..Default::default()
        };
        assert!(options.validate().is_err());

        let options = RegistrationOptions {
            lease_ttl_secs: u64::MAX,
            keep_alive_interval_secs: u64::MAX,
            ..Default::default()
        };
        assert!(options.validate().is_err());
    }

    #[tokio::test]
    async fn registration_guard_rejects_identity_mismatch() {
        let guard = guard(RegistrationStatus::Registered);
        let err = guard
            .update_endpoint(endpoint("mds", "other-id"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("kind/id"));
    }

    #[tokio::test]
    async fn registration_guard_rejects_keepalive_lost() {
        let guard = guard(RegistrationStatus::KeepAliveLost {
            message: "lost".to_string(),
        });
        let err = guard
            .update_status(ServiceStatus::Serving)
            .await
            .unwrap_err();
        assert!(matches!(err, DiscoveryError::RegistrationLost(_)));
    }

    #[tokio::test]
    async fn registration_guard_rejects_revoking() {
        let guard = guard(RegistrationStatus::Revoking);
        let err = guard
            .update_status(ServiceStatus::Serving)
            .await
            .unwrap_err();
        assert!(matches!(err, DiscoveryError::RegistrationLost(_)));
    }

    #[tokio::test]
    async fn registration_guard_shutdown_is_retryable_and_revoked_is_idempotent() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (status_tx, rx) = watch::channel(RegistrationStatus::Registered);
        let guard = RegistrationGuard::new(
            ServiceKind::try_new("mds").unwrap(),
            "mds-1",
            10,
            rx,
            Arc::new(CountingControl {
                calls: calls.clone(),
            }),
        );

        guard.shutdown().await.unwrap();
        guard.shutdown().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let _ = status_tx.send(RegistrationStatus::Revoked);
        guard.shutdown().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
