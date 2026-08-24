use crate::endpoint::validate_service_id;
use crate::{DiscoveryError, DiscoveryResult, ServiceKind};

const SERVICES_SEGMENT: &str = "services";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceKey {
    pub prefix: String,
    pub cluster_id: String,
    pub kind: ServiceKind,
    pub service_id: String,
}

impl ServiceKey {
    pub fn new(
        prefix: impl AsRef<str>,
        cluster_id: impl Into<String>,
        kind: ServiceKind,
        service_id: impl Into<String>,
    ) -> DiscoveryResult<Self> {
        let prefix = normalize_prefix(prefix.as_ref())?;
        let cluster_id = cluster_id.into();
        validate_cluster_id(&cluster_id)?;
        let service_id = service_id.into();
        validate_service_id(&service_id)?;
        Ok(Self {
            prefix,
            cluster_id,
            kind,
            service_id,
        })
    }

    pub fn service_prefix(
        prefix: impl AsRef<str>,
        cluster_id: impl AsRef<str>,
        kind: &ServiceKind,
    ) -> DiscoveryResult<String> {
        let prefix = normalize_prefix(prefix.as_ref())?;
        validate_cluster_id(cluster_id.as_ref())?;
        Ok(format!(
            "{}/{}/{}/{}/",
            prefix,
            cluster_id.as_ref(),
            SERVICES_SEGMENT,
            kind
        ))
    }

    pub fn parse(prefix: impl AsRef<str>, key: impl AsRef<str>) -> DiscoveryResult<Self> {
        let prefix = normalize_prefix(prefix.as_ref())?;
        let key = key.as_ref();
        let rest = key.strip_prefix(&prefix).ok_or_else(|| {
            DiscoveryError::InvalidEndpointValue(format!(
                "key '{}' does not start with prefix '{}'",
                key, prefix
            ))
        })?;
        let rest = rest.strip_prefix('/').ok_or_else(|| {
            DiscoveryError::InvalidEndpointValue(format!(
                "key '{}' is not under prefix '{}'",
                key, prefix
            ))
        })?;
        let mut parts = rest.split('/');
        let cluster_id = parts.next().unwrap_or_default().to_string();
        let services = parts.next().unwrap_or_default();
        let kind = parts.next().unwrap_or_default();
        let service_id = parts.next().unwrap_or_default().to_string();
        if parts.next().is_some() || services != SERVICES_SEGMENT {
            return Err(DiscoveryError::InvalidEndpointValue(format!(
                "invalid service key '{}'",
                key
            )));
        }
        let kind = ServiceKind::try_new(kind)?;
        Self::new(prefix, cluster_id, kind, service_id)
    }

    pub fn as_string(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.prefix, self.cluster_id, SERVICES_SEGMENT, self.kind, self.service_id
        )
    }
}

impl std::fmt::Display for ServiceKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.as_string())
    }
}

pub fn normalize_prefix(prefix: &str) -> DiscoveryResult<String> {
    let prefix = prefix.trim();
    let prefix = if prefix == "/" {
        prefix.to_string()
    } else {
        prefix.trim_end_matches('/').to_string()
    };
    if prefix.starts_with('/') && prefix.len() > 1 && !prefix.contains("//") {
        Ok(prefix)
    } else {
        Err(DiscoveryError::InvalidEndpointValue(format!(
            "invalid key prefix '{}'",
            prefix
        )))
    }
}

pub fn validate_cluster_id(cluster_id: &str) -> DiscoveryResult<()> {
    if is_key_safe_lowercase(cluster_id) {
        Ok(())
    } else {
        Err(DiscoveryError::InvalidEndpointValue(format!(
            "invalid cluster_id '{}'",
            cluster_id
        )))
    }
}

fn is_key_safe_lowercase(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_service_key_and_prefix() {
        let kind = ServiceKind::try_new("mds").unwrap();
        let key = ServiceKey::new("/curvine/", "prod-cluster", kind.clone(), "mds-1").unwrap();

        assert_eq!(key.as_string(), "/curvine/prod-cluster/services/mds/mds-1");
        assert_eq!(
            ServiceKey::service_prefix("/curvine/", "prod-cluster", &kind).unwrap(),
            "/curvine/prod-cluster/services/mds/"
        );
    }

    #[test]
    fn parses_service_key() {
        let key = ServiceKey::parse("/curvine", "/curvine/prod/services/mds/mds-1").unwrap();

        assert_eq!(key.prefix, "/curvine");
        assert_eq!(key.cluster_id, "prod");
        assert_eq!(key.kind.as_str(), "mds");
        assert_eq!(key.service_id, "mds-1");
    }

    #[test]
    fn rejects_invalid_key_parts() {
        assert!(ServiceKey::new(
            "curvine",
            "prod",
            ServiceKind::try_new("mds").unwrap(),
            "mds-1"
        )
        .is_err());
        assert!(ServiceKey::new(
            "/curvine",
            "prod/1",
            ServiceKind::try_new("mds").unwrap(),
            "mds-1"
        )
        .is_err());
        assert!(ServiceKey::new(
            "/curvine",
            "prod",
            ServiceKind::try_new("mds").unwrap(),
            "mds/1"
        )
        .is_err());
        assert!(ServiceKey::parse("/curvine", "/other/prod/services/mds/mds-1").is_err());
        assert!(ServiceKey::parse("/curvine", "/curvine/prod/not-services/mds/mds-1").is_err());
    }
}
