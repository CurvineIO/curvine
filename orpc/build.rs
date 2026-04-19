fn main() {
    #[cfg(feature = "spdk")]
    link_spdk();
}

#[cfg(feature = "spdk")]
fn link_spdk() {
    let spdk_dir = std::env::var("SPDK_DIR").ok();
    let lib_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/build/lib", d))
        .unwrap_or_else(|| "/usr/local/lib".to_string());
    println!("cargo:rustc-link-search=native={}", lib_dir);

    // Compile C helper for version-safe opts setters
    let include_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/include", d))
        .unwrap_or_else(|| "/usr/local/include".to_string());
    let dpdk_include = spdk_dir
        .as_ref()
        .map(|d| format!("{}/dpdk/build/include", d))
        .unwrap_or_else(|| "/usr/local/include/dpdk".to_string());
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let helper_src = format!("{}/csrc/spdk_opts_helper.c", manifest_dir);
    cc::Build::new()
        .file(&helper_src)
        .include(&include_dir)
        .include(&dpdk_include)
        .compile("spdk_opts_helper");
    println!("cargo:rerun-if-changed={}", helper_src);

    // DPDK lib subdirs
    if let Some(ref dir) = spdk_dir {
        println!("cargo:rustc-link-search=native={}/dpdk/build/lib", dir);
        println!("cargo:rustc-link-search=native={}/build/lib/dpdk", dir);
        println!("cargo:rustc-link-search=native={}/dpdk/lib", dir);
    }

    // Link SPDK libs (static)
    let whole = ["spdk_nvme", "spdk_sock", "spdk_sock_posix"];
    let mut libs = vec![
        "spdk_trace",
        "spdk_json",
        "spdk_jsonrpc",
        "spdk_rpc",
        "spdk_log",
        "spdk_util",
        "spdk_env_dpdk",
        "spdk_env_dpdk_rpc",
        "spdk_init",
        "spdk_thread",
        "spdk_dma",
    ];

    #[cfg(feature = "spdk-rdma")]
    libs.extend(["spdk_rdma_provider", "spdk_rdma_utils"]);

    for lib in &whole {
        let path = format!("{}/lib{}.a", lib_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static:+whole-archive={}", lib);
        }
    }
    for lib in &libs {
        let path = format!("{}/lib{}.a", lib_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    // DPDK libs
    let dpdk_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/dpdk/build/lib", d))
        .unwrap_or_else(|| "/usr/local/lib".to_string());
    let dpdk_libs = [
        "rte_eal",
        "rte_mempool",
        "rte_ring",
        "rte_mbuf",
        "rte_bus_pci",
        "rte_pci",
        "rte_kvargs",
        "rte_telemetry",
        "rte_power",
        "rte_ethdev",
        "rte_net",
        "rte_vhost",
        "rte_cryptodev",
        "rte_compressdev",
        "rte_dmadev",
    ];
    for lib in &dpdk_libs {
        let path = format!("{}/lib{}.a", dpdk_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    // Extra DPDK libs
    let extra = [
        "rte_log",
        "rte_argparse",
        "rte_bus_vdev",
        "rte_cmdline",
        "rte_hash",
        "rte_mempool_ring",
        "rte_meter",
        "rte_rcu",
        "rte_stack",
        "rte_timer",
    ];
    for lib in &extra {
        let path = format!("{}/lib{}.a", dpdk_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    // SPDK keyring
    let keyring = format!("{}/libspdk_keyring.a", lib_dir);
    if std::path::Path::new(&keyring).exists() {
        println!("cargo:rustc-link-lib=static=spdk_keyring");
    }

    // System libs
    for lib in &[
        "pthread", "dl", "rt", "numa", "uuid", "fuse3", "isal", "ssl", "crypto",
    ] {
        println!("cargo:rustc-link-lib=dylib={}", lib);
    }

    // RDMA libs (spdk-rdma only)
    #[cfg(feature = "spdk-rdma")]
    for lib in &["ibverbs", "rdmacm"] {
        println!("cargo:rustc-link-lib=dylib={}", lib);
    }

    println!("cargo:rerun-if-env-changed=SPDK_DIR");
}
