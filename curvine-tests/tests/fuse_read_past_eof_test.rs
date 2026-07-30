use curvine_common::fs::Path;
use curvine_common::fs::Reader;
use curvine_common::fs::Writer;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;
use std::sync::Arc;

/// LTP ftest always read()s before checking file_max. Reading past EOF must
/// succeed with a short/empty result (POSIX), not fail with EIO.
#[test]
fn fuse_read_past_eof_returns_empty() {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size = 128 * 1024 * 1024;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();

    rt.block_on(async {
        let path = Path::from_str("/fuse_read_past_eof.data").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write(b"hello").await.unwrap();
        writer.flush().await.unwrap();

        let mut reader = fs.open(&path).await.unwrap();
        // Past EOF: fuse_read must not error.
        let chunks = reader.fuse_read(1024 * 1024, 2048).await.expect("past EOF");
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(n, 0, "past-EOF fuse_read must return 0 bytes");
        drop(reader);
        writer.complete().await.unwrap();

        // Truncate-to-empty then read at a positive offset (ftest pattern).
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.flush().await.unwrap();
        let mut reader = fs.open(&path).await.unwrap();
        let chunks = reader
            .fuse_read(0xEB800, 2048)
            .await
            .expect("past EOF after truncate");
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(n, 0, "past-EOF after truncate must return 0 bytes");

        writer.complete().await.unwrap();
        println!("fuse_read_past_eof_returns_empty: OK");
    });
}
