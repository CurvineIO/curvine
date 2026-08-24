use curvine_error::FsError;

pub type DiscoveryResult<T> = Result<T, DiscoveryError>;

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("invalid service kind: {0}")]
    InvalidServiceKind(String),
    #[error("invalid service id: {0}")]
    InvalidServiceId(String),
    #[error("invalid endpoint value: {0}")]
    InvalidEndpointValue(String),
    #[error(
        "endpoint key/value mismatch: key={key}, value_kind={value_kind}, value_id={value_id}"
    )]
    KeyValueMismatch {
        key: String,
        value_kind: String,
        value_id: String,
    },
    #[error("invalid registration options: {0}")]
    InvalidRegistrationOptions(String),
    #[error("etcd unavailable: {0}")]
    EtcdUnavailable(String),
    #[error("watch revision has been compacted: {revision}")]
    WatchCompacted { revision: i64 },
    #[error("resolver cache is stale")]
    StaleCache,
    #[error("service registration lost: {0}")]
    RegistrationLost(String),
}

#[cfg(feature = "etcd")]
impl From<etcd_client::Error> for DiscoveryError {
    fn from(value: etcd_client::Error) -> Self {
        DiscoveryError::EtcdUnavailable(value.to_string())
    }
}

impl From<DiscoveryError> for FsError {
    fn from(value: DiscoveryError) -> Self {
        FsError::from(curvine_core_error::CommonError::from(value))
    }
}
