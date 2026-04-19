#[cfg(test)]
mod test {
    use orpc::io::SpdkConf;

    #[test]
    fn spdk_conf_toml_deserialization_with_targets() {
        let toml_str = r#"
            enabled = true
            app_name = "curvine"
            hugepage = "2048MB"
            reactor_mask = "0x3"
            shm_id = -1
            mem_channel = 0
            io_queue_depth = 256
            io_queue_requests = 512
            io_timeout = "60s"
            io_retry_count = 4
            keep_alive_timeout = "10s"
            dma_pool_size = "64MB"
            [[targets]]
            trtype = "rdma"
            traddr = "192.168.1.100"
            trsvcid = 4420
            subnqn = "nqn.2024-01.io.curvine:subsystem1"
            [[targets]]
            trtype = "rdma"
            traddr = "192.168.1.101"
            trsvcid = 4420
            subnqn = "nqn.2024-01.io.curvine:subsystem2"
        "#;

        let conf: SpdkConf = toml::from_str(toml_str).unwrap();
        assert!(conf.enabled);
        assert_eq!(conf.app_name, "curvine");
        assert_eq!(conf.hugepage_str, "2048MB");
        assert_eq!(conf.reactor_mask, "0x3");
        assert_eq!(conf.shm_id, -1);
        assert_eq!(conf.io_queue_depth, 256);
        assert_eq!(conf.targets.len(), 2);
        assert_eq!(conf.targets[0].traddr, "192.168.1.100");
        assert_eq!(conf.targets[0].trtype, "rdma");
        assert_eq!(conf.targets[1].traddr, "192.168.1.101");
        assert_eq!(conf.targets[1].subnqn, "nqn.2024-01.io.curvine:subsystem2");
    }

    #[test]
    fn minimal_toml_test() {
        let toml_str = "enabled = true\n";
        let conf: orpc::io::SpdkConf = toml::from_str(toml_str).unwrap();
        println!("parsed: {:?}", conf.enabled);
    }
}
