use crate::{DiscoveryError, DiscoveryResult, ServiceKey};
use curvine_proto::ComponentInfoProto;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceKind(String);

impl ServiceKind {
    pub fn try_new(kind: impl Into<String>) -> DiscoveryResult<Self> {
        let kind = kind.into();
        if is_key_safe_lowercase(&kind) {
            Ok(Self(kind))
        } else {
            Err(DiscoveryError::InvalidServiceKind(kind))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for ServiceKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ServiceKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ServiceKindVisitor;

        impl Visitor<'_> for ServiceKindVisitor {
            type Value = ServiceKind;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a non-empty lowercase [a-z0-9_-] service kind")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                ServiceKind::try_new(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(ServiceKindVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum ServiceStatus {
    Starting,
    #[default]
    Serving,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceSnapshot {
    pub kind: ServiceKind,
    pub revision: i64,
    pub stale: bool,
    pub last_update_ms: u64,
    pub endpoints: Vec<ServiceEndpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceEndpoint {
    pub kind: ServiceKind,
    pub id: String,
    pub host: String,
    pub rpc_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_port: Option<u16>,
    /// Producers must serialize a complete [`ComponentInfoProto`], including
    /// `capabilities` as an empty array when no capability is advertised.
    /// Without a workspace-wide serde default on generated proto structs,
    /// JSON values missing repeated fields are rejected during decode.
    pub component_info: ComponentInfoProto,
    pub start_time_ms: u64,
    #[serde(default)]
    pub status: ServiceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,
}

impl ServiceEndpoint {
    pub fn validate(&self) -> DiscoveryResult<()> {
        validate_service_id(&self.id)?;
        if self.host.trim().is_empty() {
            return Err(DiscoveryError::InvalidEndpointValue(
                "host must not be empty".to_string(),
            ));
        }
        if self.host.trim() != self.host || self.host.chars().any(char::is_whitespace) {
            return Err(DiscoveryError::InvalidEndpointValue(
                "host must not contain whitespace".to_string(),
            ));
        }
        if self.rpc_port == 0 {
            return Err(DiscoveryError::InvalidEndpointValue(
                "rpc_port must be > 0".to_string(),
            ));
        }
        if self.web_port == Some(0) {
            return Err(DiscoveryError::InvalidEndpointValue(
                "web_port must be > 0 when present".to_string(),
            ));
        }
        match self.component_info.component.as_deref() {
            Some(component) if component == self.kind.as_str() => Ok(()),
            Some(component) => Err(DiscoveryError::InvalidEndpointValue(format!(
                "component_info.component '{}' does not match kind '{}'",
                component, self.kind
            ))),
            None => Err(DiscoveryError::InvalidEndpointValue(
                "component_info.component must be present".to_string(),
            )),
        }
    }
}

pub fn encode_endpoint_value(endpoint: &ServiceEndpoint) -> DiscoveryResult<String> {
    endpoint.validate()?;
    serde_json::to_string(endpoint).map_err(|error| {
        DiscoveryError::InvalidEndpointValue(format!("failed to encode endpoint JSON: {error}"))
    })
}

pub fn decode_endpoint_value(value: &[u8]) -> DiscoveryResult<ServiceEndpoint> {
    let endpoint: ServiceEndpoint = serde_json::from_slice(value).map_err(|error| {
        DiscoveryError::InvalidEndpointValue(format!("failed to decode endpoint JSON: {error}"))
    })?;
    endpoint.validate()?;
    Ok(endpoint)
}

pub fn decode_endpoint_value_for_key(
    prefix: impl AsRef<str>,
    key: impl AsRef<str>,
    value: &[u8],
) -> DiscoveryResult<ServiceEndpoint> {
    let parsed_key = ServiceKey::parse(prefix, key.as_ref())?;
    let endpoint = decode_endpoint_value(value)?;
    if endpoint.kind != parsed_key.kind || endpoint.id != parsed_key.service_id {
        return Err(DiscoveryError::KeyValueMismatch {
            key: key.as_ref().to_string(),
            value_kind: endpoint.kind.to_string(),
            value_id: endpoint.id,
        });
    }
    Ok(endpoint)
}

pub fn validate_service_id(id: &str) -> DiscoveryResult<()> {
    if !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        Ok(())
    } else {
        Err(DiscoveryError::InvalidServiceId(id.to_string()))
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

    fn component_info(component: &str) -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some(component.to_string()),
            release_version: Some("0.2.0".to_string()),
            git_commit: Some("abcdef".to_string()),
            git_tag: Some(String::new()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["metadata-read".to_string()],
        }
    }

    #[test]
    fn service_kind_rejects_invalid_values() {
        assert!(ServiceKind::try_new("mds").is_ok());
        assert!(ServiceKind::try_new("mds_1-prod").is_ok());
        assert!(ServiceKind::try_new("").is_err());
        assert!(ServiceKind::try_new("MDS").is_err());
        assert!(ServiceKind::try_new("mds/service").is_err());
    }

    #[test]
    fn service_kind_deserialize_uses_validation() {
        assert!(serde_json::from_str::<ServiceKind>("\"mds\"").is_ok());
        assert!(serde_json::from_str::<ServiceKind>("\"MDS\"").is_err());
    }

    #[test]
    fn service_status_defaults_to_serving_and_uses_lowercase_json() {
        let status = serde_json::from_str::<ServiceStatus>("\"draining\"").unwrap();
        assert_eq!(status, ServiceStatus::Draining);
        assert_eq!(ServiceStatus::default(), ServiceStatus::Serving);
        assert_eq!(
            serde_json::to_string(&ServiceStatus::Serving).unwrap(),
            "\"serving\""
        );
    }

    #[test]
    fn endpoint_validation_requires_component_to_match_kind() {
        let endpoint = ServiceEndpoint {
            kind: ServiceKind::try_new("mds").unwrap(),
            id: "mds-pod-9100-abcd".to_string(),
            host: "mds.default.svc".to_string(),
            rpc_port: 9100,
            web_port: None,
            component_info: component_info("mds"),
            start_time_ms: 1,
            status: ServiceStatus::Serving,
            metadata: None,
        };
        assert!(endpoint.validate().is_ok());

        let mut invalid = endpoint;
        invalid.component_info.component = Some("worker".to_string());
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn endpoint_validation_rejects_invalid_ports_and_service_id() {
        let mut endpoint = ServiceEndpoint {
            kind: ServiceKind::try_new("mds").unwrap(),
            id: "mds-pod-9100-abcd".to_string(),
            host: "mds.default.svc".to_string(),
            rpc_port: 9100,
            web_port: None,
            component_info: component_info("mds"),
            start_time_ms: 1,
            status: ServiceStatus::Serving,
            metadata: None,
        };

        endpoint.id = "mds/pod".to_string();
        assert!(endpoint.validate().is_err());

        endpoint.id = "mds-pod-9100-abcd".to_string();
        endpoint.rpc_port = 0;
        assert!(endpoint.validate().is_err());

        endpoint.rpc_port = 9100;
        endpoint.web_port = Some(0);
        assert!(endpoint.validate().is_err());

        endpoint.web_port = None;
        endpoint.host = " mds.default.svc".to_string();
        assert!(endpoint.validate().is_err());

        endpoint.host = "mds.default svc".to_string();
        assert!(endpoint.validate().is_err());
    }

    #[test]
    fn endpoint_status_defaults_when_missing() {
        let endpoint = serde_json::json!({
            "kind": "mds",
            "id": "mds-pod-9100-abcd",
            "host": "mds.default.svc",
            "rpc_port": 9100,
            "component_info": component_info("mds"),
            "start_time_ms": 1
        });
        let endpoint: ServiceEndpoint = serde_json::from_value(endpoint).unwrap();
        assert_eq!(endpoint.status, ServiceStatus::Serving);
    }

    #[test]
    fn endpoint_value_round_trips_and_validates_key() {
        let endpoint = ServiceEndpoint {
            kind: ServiceKind::try_new("mds").unwrap(),
            id: "mds-pod-9100-abcd".to_string(),
            host: "mds.default.svc".to_string(),
            rpc_port: 9100,
            web_port: Some(9101),
            component_info: component_info("mds"),
            start_time_ms: 1,
            status: ServiceStatus::Serving,
            metadata: None,
        };
        let value = encode_endpoint_value(&endpoint).unwrap();
        let decoded = decode_endpoint_value_for_key(
            "/curvine",
            "/curvine/prod/services/mds/mds-pod-9100-abcd",
            value.as_bytes(),
        )
        .unwrap();

        assert_eq!(decoded, endpoint);
    }

    #[test]
    fn endpoint_value_rejects_key_value_mismatch() {
        let endpoint = ServiceEndpoint {
            kind: ServiceKind::try_new("mds").unwrap(),
            id: "mds-pod-9100-abcd".to_string(),
            host: "mds.default.svc".to_string(),
            rpc_port: 9100,
            web_port: None,
            component_info: component_info("mds"),
            start_time_ms: 1,
            status: ServiceStatus::Serving,
            metadata: None,
        };
        let value = encode_endpoint_value(&endpoint).unwrap();

        let err = decode_endpoint_value_for_key(
            "/curvine",
            "/curvine/prod/services/mds/other-id",
            value.as_bytes(),
        )
        .unwrap_err();
        assert!(matches!(err, DiscoveryError::KeyValueMismatch { .. }));
    }

    #[test]
    fn endpoint_decode_rejects_component_info_missing_capabilities() {
        let endpoint = serde_json::json!({
            "kind": "mds",
            "id": "mds-pod-9100-abcd",
            "host": "mds.default.svc",
            "rpc_port": 9100,
            "component_info": {
                "component": "mds",
                "release_version": "0.2.0",
                "git_commit": "abcdef",
                "git_tag": "",
                "git_branch": "main",
                "protocol_version": 1,
                "min_protocol_version": 1
            },
            "start_time_ms": 1
        });

        assert!(serde_json::from_value::<ServiceEndpoint>(endpoint).is_err());
    }
}
