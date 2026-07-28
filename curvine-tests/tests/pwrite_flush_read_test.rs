use curvine_common::fs::Path;
use curvine_common::fs::Reader;
use curvine_common::fs::Writer;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;
use std::sync::Arc;

/// Mimic LTP pwrite01 / FUSE dirty-read: sparse pwrites, flush (master write
/// lease stays open), then open a reader and read back published data.
#[test]
fn pwrite_flush_then_read() {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size = 128 * 1024 * 1024;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();

    rt.block_on(async {
        let path = Path::from_str("/pwrite_flush_read.data").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();

        let k1 = 1024usize;
        let buf0 = vec![0u8; k1];
        let buf1 = vec![1u8; k1];
        let buf2 = vec![2u8; k1];
        let buf3 = vec![3u8; k1];

        writer.seek(0).await.unwrap();
        writer.write(&buf0).await.unwrap();
        writer.seek((2 * k1) as i64).await.unwrap();
        writer.write(&buf2).await.unwrap();
        writer.seek((3 * k1) as i64).await.unwrap();
        writer.write(&buf3).await.unwrap();
        writer.seek(k1 as i64).await.unwrap();
        writer.write(&buf1).await.unwrap();

        writer.flush().await.expect("flush before read");

        let mut reader = fs.open(&path).await.expect("open reader after flush");
        let expected = [&buf0[..], &buf1[..], &buf2[..], &buf3[..]];
        for (i, exp) in expected.iter().enumerate() {
            reader.seek((i * k1) as i64).await.unwrap();
            let mut buf = vec![0u8; k1];
            let n = reader.read_full(&mut buf).await.unwrap();
            assert_eq!(n, k1, "short read at chunk {}", i);
            assert_eq!(&buf[..], *exp, "mismatch at chunk {}", i);
        }
        drop(reader);

        // Flush must leave the master write lease open so later writes work.
        let buf4 = vec![4u8; k1];
        writer.seek((4 * k1) as i64).await.unwrap();
        writer.write(&buf4).await.unwrap();
        writer.flush().await.expect("flush after continued write");

        let mut reader = fs
            .open(&path)
            .await
            .expect("reopen reader after second flush");
        reader.seek((4 * k1) as i64).await.unwrap();
        let mut buf = vec![0u8; k1];
        let n = reader.read_full(&mut buf).await.unwrap();
        assert_eq!(n, k1, "short read at chunk 4");
        assert_eq!(&buf[..], &buf4[..], "mismatch at chunk 4");

        writer.complete().await.unwrap();
        println!("pwrite_flush_then_read: all chunks OK");
    });
}
