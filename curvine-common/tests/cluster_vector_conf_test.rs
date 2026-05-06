use curvine_common::conf::ClusterConf;

#[test]
fn cluster_conf_roundtrips_vector_subsystem_defaults() {
    let mut conf = ClusterConf::default();
    conf.vector.worker_private_vector_root = "/tmp/vector-worker-root".to_string();
    conf.vector.default_shard_count = 16;
    conf.vector.segment_target_size_mb = 128;

    let ser = toml::to_string(&conf).expect("serialize ClusterConf");
    let parsed: ClusterConf = toml::from_str(&ser).expect("deserialize ClusterConf");

    assert_eq!(
        parsed.vector.worker_private_vector_root,
        "/tmp/vector-worker-root"
    );
    assert_eq!(parsed.vector.default_shard_count, 16);
    assert_eq!(parsed.vector.segment_target_size_mb, 128);
}
