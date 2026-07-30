use curvine_common::fs::Path;
use curvine_common::fs::Reader;
use curvine_common::fs::Writer;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;
use std::sync::Arc;

/// Sparse extend then read unwritten hole after flush — must return zeros, not EIO.
#[test]
fn sparse_hole_flush_then_read_zeros() {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size = 128 * 1024 * 1024;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();

    rt.block_on(async {
        let path = Path::from_str("/sparse_hole_flush.data").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();

        let hole = 64 * 1024i64;
        let chunk = vec![0xABu8; 4096];
        writer.seek(hole).await.unwrap();
        writer.write(&chunk).await.unwrap();
        writer.flush().await.expect("flush");

        let mut reader = fs.open(&path).await.unwrap();
        // Read inside the hole
        reader.seek(0).await.unwrap();
        let mut buf = vec![0xffu8; 4096];
        let n = reader.read_full(&mut buf).await.expect("read hole");
        assert_eq!(n, 4096, "short hole read");
        assert!(
            buf.iter().all(|&b| b == 0),
            "hole must be zeros, got {:?}",
            &buf[..16]
        );

        // Read the written chunk
        reader.seek(hole).await.unwrap();
        let mut buf2 = vec![0u8; 4096];
        let n2 = reader.read_full(&mut buf2).await.expect("read data");
        assert_eq!(n2, 4096);
        assert_eq!(&buf2[..], &chunk[..]);

        writer.complete().await.unwrap();
        println!("sparse_hole_flush_then_read_zeros: OK");
    });
}
