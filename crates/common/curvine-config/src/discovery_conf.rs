use crate::FsResult;
use curvine_core_error::err_box;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DiscoveryConf {
    pub enabled: bool,
    pub provider: String,
    pub endpoints: Vec<String>,
    pub prefix: String,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub watch_reconnect_min_ms: u64,
    pub watch_reconnect_max_ms: u64,
    pub watch_reconnect_jitter_ratio: f64,
    pub allow_stale_cache: bool,
}

impl DiscoveryConf {
    pub const PROVIDER_ETCD: &'static str = "etcd";
    pub const DEFAULT_PREFIX: &'static str = "/curvine";
    pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 3000;
    pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 3000;
    pub const DEFAULT_WATCH_RECONNECT_MIN_MS: u64 = 1000;
    pub const DEFAULT_WATCH_RECONNECT_MAX_MS: u64 = 30000;
    pub const DEFAULT_WATCH_RECONNECT_JITTER_RATIO: f64 = 0.2;

    pub fn init(&mut self, cluster_id: &str) -> FsResult<()> {
        self.normalize();

        if !self.enabled {
            return Ok(());
        }

        if self.provider != Self::PROVIDER_ETCD {
            return err_box!(
                "discovery.provider must be '{}' when discovery is enabled, got '{}'",
                Self::PROVIDER_ETCD,
                self.provider
            );
        }

        if self.endpoints.is_empty() {
            return err_box!("discovery.endpoints must not be empty when discovery is enabled");
        }

        if self.endpoints.iter().any(|endpoint| endpoint.is_empty()) {
            return err_box!("discovery.endpoints must not contain empty endpoint");
        }

        if !Self::is_valid_prefix(&self.prefix) {
            return err_box!(
                "discovery.prefix must be an absolute key prefix like '/curvine', got '{}'",
                self.prefix
            );
        }

        if !Self::is_key_safe_lowercase(cluster_id) {
            return err_box!(
                "cluster.cluster_id must be non-empty and contain only [a-z0-9_-] when discovery is enabled, got '{}'",
                cluster_id
            );
        }

        if self.connect_timeout_ms == 0 {
            return err_box!("discovery.connect_timeout_ms must be > 0");
        }

        if self.request_timeout_ms == 0 {
            return err_box!("discovery.request_timeout_ms must be > 0");
        }

        if self.watch_reconnect_min_ms == 0 {
            return err_box!("discovery.watch_reconnect_min_ms must be > 0");
        }

        if self.watch_reconnect_max_ms < self.watch_reconnect_min_ms {
            return err_box!(
                "discovery.watch_reconnect_max_ms must be >= discovery.watch_reconnect_min_ms"
            );
        }

        if !(0.0..=1.0).contains(&self.watch_reconnect_jitter_ratio) {
            return err_box!("discovery.watch_reconnect_jitter_ratio must be between 0.0 and 1.0");
        }

        Ok(())
    }

    fn normalize(&mut self) {
        self.provider = self.provider.trim().to_ascii_lowercase();
        self.prefix = normalize_prefix(&self.prefix);
        self.endpoints = self
            .endpoints
            .iter()
            .map(|endpoint| endpoint.trim().to_string())
            .collect();
    }

    fn is_valid_prefix(prefix: &str) -> bool {
        prefix.starts_with('/')
            && prefix.len() > 1
            && !prefix.ends_with('/')
            && !prefix.contains("//")
    }

    fn is_key_safe_lowercase(value: &str) -> bool {
        !value.is_empty()
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
            })
    }
}

impl Default for DiscoveryConf {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: Self::PROVIDER_ETCD.to_string(),
            endpoints: Vec::new(),
            prefix: Self::DEFAULT_PREFIX.to_string(),
            connect_timeout_ms: Self::DEFAULT_CONNECT_TIMEOUT_MS,
            request_timeout_ms: Self::DEFAULT_REQUEST_TIMEOUT_MS,
            watch_reconnect_min_ms: Self::DEFAULT_WATCH_RECONNECT_MIN_MS,
            watch_reconnect_max_ms: Self::DEFAULT_WATCH_RECONNECT_MAX_MS,
            watch_reconnect_jitter_ratio: Self::DEFAULT_WATCH_RECONNECT_JITTER_RATIO,
            allow_stale_cache: true,
        }
    }
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim();
    if trimmed == "/" {
        trimmed.to_string()
    } else {
        trimmed.trim_end_matches('/').to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_discovery_conf_is_disabled_etcd() {
        let conf = DiscoveryConf::default();

        assert!(!conf.enabled);
        assert_eq!(conf.provider, DiscoveryConf::PROVIDER_ETCD);
        assert!(conf.endpoints.is_empty());
        assert_eq!(conf.prefix, DiscoveryConf::DEFAULT_PREFIX);
        assert!(conf.allow_stale_cache);
    }

    #[test]
    fn disabled_discovery_ignores_unknown_provider_and_empty_endpoints() {
        let mut conf = DiscoveryConf {
            provider: "unknown".to_string(),
            ..Default::default()
        };

        assert!(!conf.enabled);
        conf.init("bad/cluster").unwrap();
    }

    #[test]
    fn enabled_discovery_accepts_valid_etcd_config() {
        let mut conf = DiscoveryConf {
            enabled: true,
            provider: " ETCD ".to_string(),
            endpoints: vec![" http://etcd-0:2379 ".to_string()],
            prefix: " /curvine/ ".to_string(),
            ..Default::default()
        };

        conf.init("prod-cluster_1").unwrap();

        assert_eq!(conf.provider, DiscoveryConf::PROVIDER_ETCD);
        assert_eq!(conf.endpoints, vec!["http://etcd-0:2379"]);
        assert_eq!(conf.prefix, DiscoveryConf::DEFAULT_PREFIX);
    }

    #[test]
    fn enabled_discovery_rejects_invalid_values() {
        let base = DiscoveryConf {
            enabled: true,
            endpoints: vec!["http://etcd-0:2379".to_string()],
            ..Default::default()
        };

        let mut conf = DiscoveryConf {
            provider: "static".to_string(),
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("provider"));

        let mut conf = DiscoveryConf {
            endpoints: Vec::new(),
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("endpoints"));

        let mut conf = DiscoveryConf {
            prefix: "curvine".to_string(),
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("prefix"));

        let mut conf = DiscoveryConf {
            prefix: "/curvine//services".to_string(),
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("prefix"));

        let mut conf = DiscoveryConf {
            connect_timeout_ms: 0,
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("connect_timeout_ms"));

        let mut conf = DiscoveryConf {
            request_timeout_ms: 0,
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("request_timeout_ms"));

        let mut conf = DiscoveryConf {
            watch_reconnect_min_ms: 0,
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("watch_reconnect_min_ms"));

        let mut conf = DiscoveryConf {
            watch_reconnect_min_ms: 1000,
            watch_reconnect_max_ms: 999,
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("watch_reconnect_max_ms"));

        let mut conf = DiscoveryConf {
            watch_reconnect_jitter_ratio: 1.01,
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("watch_reconnect_jitter_ratio"));

        let mut conf = DiscoveryConf {
            watch_reconnect_jitter_ratio: f64::NAN,
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("watch_reconnect_jitter_ratio"));

        let mut conf = DiscoveryConf {
            endpoints: vec!["http://etcd-0:2379".to_string(), "   ".to_string()],
            ..base.clone()
        };
        assert!(conf
            .init("curvine")
            .unwrap_err()
            .to_string()
            .contains("empty endpoint"));

        let mut conf = base;
        assert!(conf
            .init("bad/cluster")
            .unwrap_err()
            .to_string()
            .contains("cluster_id"));
    }
}
