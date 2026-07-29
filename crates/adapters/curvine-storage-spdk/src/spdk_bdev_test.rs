use crate::spdk_bdev::SpdkBdev;
use crate::spdk_env::{QpairPool, SpdkEnv, SpdkEnvState};
use crate::spdk_ffi;
use bytes::BytesMut;
use orpc::common::Utils;
use orpc::io::{BlockIO, NvmeTarget, SpdkConf};
use orpc::sys::DataSlice;
use std::collections::HashMap;
use std::sync::Once;
use std::sync::{atomic::AtomicBool, Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
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
    let iova_mode = std::env::var("SPDK_IOVA_MODE").unwrap_or_else(|_| "va".to_string());

    SpdkConf {
        enabled: true,
        app_name: "curvine-test".to_string(),
        hugepage_mb: std::env::var("SPDK_HUGEPAGE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256),
        reactor_mask: std::env::var("SPDK_REACTOR_MASK").unwrap_or("0x1".to_string()),
        keep_alive_timeout_ms: 500,
        iova_mode,

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
                thread::spawn(move || {
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

    // Phase 7b: admin polling prevents KATO disconnect
    {
        let conf = test_spdk_conf();
        thread::sleep(Duration::from_millis(conf.keep_alive_timeout_ms * 3));
        let env = get_spdk_env();
        assert!(
            !env.bdev_names().is_empty(),
            "Controller disconnected after idle past KATO"
        );
        let mut bdev = SpdkBdev::open_read(&bdev_name, 0, 0).unwrap();
        let mut buf = vec![0u8; 512];
        bdev.read_all(&mut buf).unwrap();
        println!("pass admin polling prevents KATO disconnect test");
    }

    // Phase 7c: qpair pool - release frees when pool is full
    {
        let env = get_spdk_env();
        let bdev = env.bdevs().first().expect("no bdevs");
        let ctrlr = bdev.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr;

        let p = QpairPool {
            inner: Mutex::new(HashMap::new()),
            ctrl_state: Mutex::new(HashMap::new()),
            notify: Condvar::new(),
            max_per_ctrlr: 2,
            shutdown: AtomicBool::new(false),
        };
        p.register_limit(ctrlr as usize, 4);

        // Allocate real qpairs
        let q1 = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        assert!(!q1.is_null());
        let q2 = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        assert!(!q2.is_null());
        let q3 = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        assert!(!q3.is_null());

        // Fill pool to capacity (max_per_ctrlr=2) via direct push
        {
            let mut pool = p.inner.lock().unwrap();
            pool.entry(ctrlr as usize).or_default().push(q1);
            pool.entry(ctrlr as usize).or_default().push(q2);
        }

        // Reserve a slot so release() has a balanced reservation to release
        assert!(p.try_reserve(ctrlr as usize)); // active=1

        // release() with pool full (2 >= max_per_ctrlr=2) -> free q3 via FFI
        p.release(ctrlr, q3); // release_reservation -> active 1->0

        // Pool unchanged at 2 (q3 was freed, not pushed)
        let pool = p.inner.lock().unwrap();
        assert_eq!(pool.get(&(ctrlr as usize)).map_or(0, |s| s.len()), 2);
        drop(pool);

        // Clean up: free cached q1, q2 before leaving scope
        p.drain_all(); // frees q1, q2

        println!("pass release_pool_full_frees_qpair");
    }

    // Phase 7d: acquire rolls back active count on contention
    {
        let env = get_spdk_env();
        let bdev = env.bdevs().first().expect("no bdevs");
        let ctrlr = bdev.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr;

        let p = Arc::new(QpairPool {
            inner: Mutex::new(HashMap::new()),
            ctrl_state: Mutex::new(HashMap::new()),
            notify: Condvar::new(),
            max_per_ctrlr: 16,
            shutdown: AtomicBool::new(false),
        });
        p.register_limit(ctrlr as usize, 1);

        // First acquire → active=1 (at capacity)
        let q1 = p.acquire(ctrlr).expect("first acquire");
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 1);

        // Second acquire in another thread -> blocks (at capacity)
        let p2 = Arc::clone(&p);
        let ctrlr_for_thread = ctrlr as usize;
        let handle = thread::spawn(move || {
            let ctrlr_ptr = ctrlr_for_thread as *mut spdk_ffi::spdk_nvme_ctrlr;
            let q = p2.acquire(ctrlr_ptr).expect("second acquire failed");
            q as usize
        });

        // Give the thread time to enter the slow path
        thread::sleep(Duration::from_millis(50));

        // Release first qpair -> unblocks the second acquire
        p.release(ctrlr, q1);

        let q2_ptr = handle.join().expect("second acquire thread panicked")
            as *mut spdk_ffi::spdk_nvme_qpair;

        // Active should be 1 (second acquire succeeded, first was released)
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 1);

        // Release second qpair -> active=0
        p.release(ctrlr, q2_ptr);
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 0);

        println!("pass acquire_contention_active_count_correct");
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
