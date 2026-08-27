mod endpoint;
mod error;
#[cfg(feature = "etcd")]
mod etcd_provider;
#[cfg(test)]
mod fake;
mod key;
mod registry;
mod resolver;

pub use self::endpoint::{
    decode_endpoint_value, decode_endpoint_value_for_key, encode_endpoint_value, ServiceEndpoint,
    ServiceKind, ServiceSnapshot, ServiceStatus,
};
pub use self::error::{DiscoveryError, DiscoveryResult};
#[cfg(feature = "etcd")]
pub use self::etcd_provider::{EtcdDiscoveryConfig, EtcdServiceRegistry, EtcdServiceResolver};
pub use self::key::{normalize_prefix, validate_cluster_id, ServiceKey};
pub use self::registry::{
    RegistrationControl, RegistrationGuard, RegistrationOptions, RegistrationStatus,
    ServiceRegistry,
};
pub use self::resolver::{
    ServiceResolver, ServiceResolverHandle, ServiceWatch, ServiceWatchEvent, SnapshotReader,
};
