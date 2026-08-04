use curvine_fs_api::Path;
use curvine_fs_api::Reader;
use curvine_fs_api::Writer;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn ftest_like_truncate_flush_read_write() {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size = 128 * 1024 * 1024;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();

    rt.block_on(async {
        let path = Path::from_str("/ftest_flush_stress.data").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();
        // empty flush like dirty-read after truncate
        writer.flush().await.expect("flush empty");
        let mut reader = fs.open(&path).await.unwrap();
        let chunks = reader.fuse_read(0xba800, 2048).await.expect("past eof");
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 0);
        drop(reader);

        // write at high offset (sparse), flush, read hole and data
        writer.seek(0xba800).await.unwrap();
        let data = vec![0x5Au8; 2048];
        writer.write(&data).await.unwrap();
        writer.flush().await.expect("flush after sparse write");

        let mut reader = fs.open(&path).await.unwrap();
        reader.seek(0xd000).await.unwrap();
        let mut hole = vec![0xffu8; 2048];
        let n = reader.read_full(&mut hole).await.expect("hole read");
        assert_eq!(n, 2048, "hole should be full zeros within file");
        assert!(hole.iter().all(|&b| b == 0), "hole zeros");

        reader.seek(0xba800).await.unwrap();
        let mut got = vec![0u8; 2048];
        let n = reader.read_full(&mut got).await.expect("data read");
        assert_eq!(n, 2048);
        assert_eq!(got, data);
        drop(reader);

        // continue writes after flush (lease open) — must not hit block length mismatch
        writer.seek(0xd000).await.unwrap();
        let data2 = vec![0xA5u8; 2048];
        writer.write(&data2).await.expect("write after flush");
        writer.flush().await.expect("second flush");
        writer.complete().await.unwrap();
        println!("ftest_like_truncate_flush_read_write: OK");
    });
}
