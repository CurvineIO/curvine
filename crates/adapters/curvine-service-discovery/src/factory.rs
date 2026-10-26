use crate::{DiscoveryError, DiscoveryResult, ServiceRegistry, ServiceResolver};
use std::sync::Arc;

/// Runtime handles for the capabilities supplied by a discovery provider.
///
/// The current runtime provider supplies both capabilities from one shared
/// client. Callers only receive a provider when both capabilities are ready.
#[derive(Clone)]
pub struct DiscoveryProvider {
    pub registry: Arc<dyn ServiceRegistry>,
    pub resolver: Arc<dyn ServiceResolver>,
}

impl DiscoveryProvider {
    pub fn from_shared<P>(provider: Arc<P>) -> Self
    where
        P: ServiceRegistry + ServiceResolver + 'static,
    {
        let registry: Arc<dyn ServiceRegistry> = provider.clone();
        let resolver: Arc<dyn ServiceResolver> = provider;
        Self { registry, resolver }
    }
}

/// Fails clearly when configuration selects a provider omitted from this build.
///
/// Callers should invoke this only when discovery is enabled. Disabled discovery
/// continues to use the existing static master configuration path.
pub fn ensure_provider_available(provider: &str) -> DiscoveryResult<()> {
    let provider = provider.trim().to_ascii_lowercase();
    match provider.as_str() {
        "etcd" if cfg!(feature = "etcd") => Ok(()),
        "etcd" => Err(DiscoveryError::ProviderUnavailable(
            "Etcd support is not compiled into this binary; rebuild with the discovery-etcd feature"
                .to_string(),
        )),
        _ => Err(DiscoveryError::InvalidDiscoveryConfig(format!(
            "unsupported discovery provider '{provider}'"
        ))),
    }
}

#[cfg(feature = "etcd")]
pub async fn connect_etcd(
    config: crate::EtcdDiscoveryConfig,
    rt: Arc<curvine_runtime::runtime::Runtime>,
) -> DiscoveryResult<DiscoveryProvider> {
    ensure_provider_available("etcd")?;
    let provider = Arc::new(crate::EtcdServiceResolver::connect(config, rt).await?);
    Ok(DiscoveryProvider::from_shared(provider))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_availability_matches_build_feature() {
        let result = ensure_provider_available(" ETCD ");

        #[cfg(feature = "etcd")]
        assert!(result.is_ok());

        #[cfg(not(feature = "etcd"))]
        assert!(result.unwrap_err().to_string().contains("discovery-etcd"));
    }

    #[test]
    fn unknown_provider_is_rejected() {
        let error = ensure_provider_available("raft").unwrap_err();
        assert!(error.to_string().contains("unsupported discovery provider"));
    }

    #[tokio::test]
    async fn shared_provider_exposes_registry_and_resolver() {
        let provider =
            DiscoveryProvider::from_shared(Arc::new(crate::fake::FakeDiscovery::default()));
        let snapshot = provider
            .resolver
            .list(crate::ServiceKind::try_new("mds").unwrap())
            .await
            .unwrap();

        assert!(snapshot.endpoints.is_empty());
    }
}
