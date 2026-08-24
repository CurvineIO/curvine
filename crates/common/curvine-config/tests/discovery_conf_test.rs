use curvine_config::{ClusterConf, DiscoveryConf};

#[test]
fn cluster_conf_defaults_discovery_to_disabled() {
    let conf = ClusterConf::default();

    assert_eq!(conf.discovery, DiscoveryConf::default());
    assert!(!conf.discovery.enabled);
}

#[test]
fn cluster_conf_deserializes_discovery_section() {
    let conf: ClusterConf = toml::from_str(
        r#"
cluster_id = "prod-cluster"

[discovery]
enabled = true
provider = "etcd"
endpoints = ["http://etcd-0:2379", "http://etcd-1:2379"]
prefix = "/curvine-prod"
connect_timeout_ms = 2500
request_timeout_ms = 3500
watch_reconnect_min_ms = 500
watch_reconnect_max_ms = 10000
watch_reconnect_jitter_ratio = 0.15
allow_stale_cache = false
"#,
    )
    .expect("cluster config with discovery section should deserialize");

    assert!(conf.discovery.enabled);
    assert_eq!(conf.discovery.provider, DiscoveryConf::PROVIDER_ETCD);
    assert_eq!(conf.discovery.endpoints.len(), 2);
    assert_eq!(conf.discovery.prefix, "/curvine-prod");
    assert_eq!(conf.discovery.connect_timeout_ms, 2500);
    assert_eq!(conf.discovery.request_timeout_ms, 3500);
    assert_eq!(conf.discovery.watch_reconnect_min_ms, 500);
    assert_eq!(conf.discovery.watch_reconnect_max_ms, 10000);
    assert_eq!(conf.discovery.watch_reconnect_jitter_ratio, 0.15);
    assert!(!conf.discovery.allow_stale_cache);
}

#[test]
fn discovery_init_validates_only_when_enabled() {
    let mut disabled = DiscoveryConf {
        provider: "unknown".to_string(),
        ..Default::default()
    };
    disabled
        .init("bad/cluster")
        .expect("disabled discovery should not validate provider or cluster id");

    let mut enabled = DiscoveryConf {
        enabled: true,
        endpoints: vec!["http://etcd-0:2379".to_string()],
        ..Default::default()
    };
    enabled
        .init("prod-cluster")
        .expect("valid enabled discovery config should initialize");
}
