use crate::common::Utils;
use crate::io::block_io::BlockIO;
use crate::io::spdk_bdev::SpdkBdev;
use crate::io::spdk_env::{NvmeTarget, SpdkConf, SpdkEnv, SpdkEnvState};
use crate::sys::DataSlice;
use bytes::BytesMut;
use std::sync::Once;

static INIT: Once = Once::new();

/// Initialize SPDK env once for all tests.
fn get_spdk_env() -> &'static SpdkEnv {
    INIT.call_once(|| {
        let conf = test_spdk_conf();
        println!("conf: {:?}", conf);
        SpdkEnv::init_global(conf).expect("Failed to init global SPDK env");
    });
    SpdkEnv::global().expect("SPDK env not initialized")
}

fn test_spdk_conf() -> SpdkConf {
    let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap_or("127.0.0.1".to_string());
    let trsvcid = std::env::var("SPDK_TARGET_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4420);
    let subnqn =
        std::env::var("SPDK_TARGET_NQN").unwrap_or("nqn.2024-01.io.curvine:test".to_string());
    let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or("tcp".to_string());

    SpdkConf {
        enabled: true,
        app_name: "curvine-test".to_string(),
        hugepage_mb: std::env::var("SPDK_HUGEPAGE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256),
        reactor_mask: std::env::var("SPDK_REACTOR_MASK").unwrap_or("0x1".to_string()),

        targets: vec![NvmeTarget {
            traddr,
            trsvcid,
            subnqn,
            trtype,
            adrfam: "ipv4".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn first_bdev_name() -> String {
    let env = get_spdk_env();
    assert!(!env.bdev_names().is_empty(), "No bdevs available");
    env.bdev_names()[0].clone()
}

fn zero_region(bdev_name: &str, offset: i64, len: usize) {
    let zeros = vec![0u8; len];
    let mut writer = SpdkBdev::open_write(bdev_name, offset, 0).unwrap();
    writer.write_all(&zeros).unwrap();
    writer.flush().unwrap();
}

/// Full SPDK lifecycle test (init => discovery => I/O => concurrent => shutdown).
// All phases in one test because SPDK global init runs once, and shutdown is destructive.
#[test]
fn spdk_full_lifecycle() {
    let env = get_spdk_env();
    assert_eq!(env.state(), SpdkEnvState::Initialized);
    assert!(env.is_initialized());
    assert!(!env.bdev_names().is_empty());
    println!("Discovered bdevs: {:?}", env.bdev_names());
    let bdev_name = first_bdev_name();

    // Phase 2: open for write
    {
        let bdev = SpdkBdev::open_write(&bdev_name, 0, 0).expect("open for write");
        assert_eq!(bdev.pos(), 0);
        assert!(bdev.len() > 0);
        assert_eq!(bdev.path(), bdev_name);
        println!("pass open file for write test");
    }

    // Phase 3: open for read
    {
        let bdev = SpdkBdev::open_read(&bdev_name, 0, 0).expect("open for read");
        assert_eq!(bdev.pos(), 0);
        assert!(bdev.len() > 0);
        println!("pass open file for read test");
    }

    // Phase 4: open with offset
    {
        let offset = 4096u64;
        let bdev = SpdkBdev::open_read(&bdev_name, offset, 0).expect("open with offset");
        assert_eq!(bdev.pos(), offset as i64);
        println!("pass open file with offset test");
    }

    // Phase 5: write/read roundtrip
    {
        let test_data = b"Hello SPDK over NVMe-oF/RDMA!";
        let aligned_len = ((test_data.len() + 511) / 512) * 512;
        let mut write_buf = vec![0u8; aligned_len];
        write_buf[..test_data.len()].copy_from_slice(test_data);
        zero_region(&bdev_name, 0, aligned_len);

        let mut bdev = SpdkBdev::open_write(&bdev_name, 0, 0).unwrap();
        bdev.write_all(&write_buf).unwrap();
        bdev.flush().unwrap();
        assert_eq!(bdev.pos(), aligned_len as i64);
        bdev.seek(0).unwrap();
        let mut read_buf = vec![0u8; aligned_len];
        bdev.read_all(&mut read_buf).unwrap();
        assert_eq!(&read_buf[..test_data.len()], test_data);
        assert_eq!(bdev.pos(), aligned_len as i64);
        println!("pass write/read round-trip test");
    }

    // Phase 6: write_region/read_region
    {
        let block_size = 512;
        let data = Utils::rand_str(block_size);
        let region = DataSlice::buffer(BytesMut::from(data.as_bytes()));

        let mut bdev = SpdkBdev::open_write(&bdev_name, 0, 0).unwrap();
        bdev.write_region(&region).unwrap();
        bdev.flush().unwrap();
        assert_eq!(bdev.pos(), block_size as i64);
        bdev.seek(0).unwrap();
        let result = bdev.read_region(false, block_size as i32).unwrap();
        assert_eq!(result.len(), block_size);
        assert_eq!(result.as_slice(), data.as_bytes());
        println!("pass write_region/read_region test");
    }

    // Phase 7: concurrent I/O through poller
    {
        use std::sync::{Arc, Barrier};

        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));
        let aligned_len = 4096usize;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let name = bdev_name.clone();
                let b = barrier.clone();
                std::thread::spawn(move || {
                    let offset = (i * aligned_len * 2) as i64;
                    let mut bdev = SpdkBdev::open_write(&name, offset, 0).unwrap();
                    let pattern = vec![(i as u8).wrapping_add(0x41); aligned_len];

                    b.wait();
                    bdev.write_all(&pattern).unwrap();
                    bdev.flush().unwrap();
                    bdev.seek(offset).unwrap();
                    let mut read_buf = vec![0u8; aligned_len];
                    bdev.read_all(&mut read_buf).unwrap();
                    assert_eq!(read_buf, pattern, "Thread {} data corruption", i);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("Concurrent poller I/O thread panicked");
        }
        println!("pass concurrent poller I/O test (8 threads)");
    }

    // Phase 8: shutdown (must be last — destructive)
    {
        env.shutdown();
        assert_eq!(env.state(), SpdkEnvState::ShutDown);
        assert!(
            SpdkEnv::global().is_none(),
            "global() should return None after shutdown"
        );
        let result = SpdkBdev::open_write(&bdev_name, 0, 0);
        assert!(result.is_err(), "Should not open bdev after shutdown");
        println!("pass shutdown test");
    }
}
