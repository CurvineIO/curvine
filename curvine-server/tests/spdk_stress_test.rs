#![cfg(feature = "spdk")]

use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse,
};
use curvine_common::state::{ExtendedBlock, FileType, StorageType};
use curvine_common::utils::ProtoUtils;
use curvine_server::worker::Worker;
use orpc::common::Utils;
use orpc::io::net::NetUtils;
use orpc::io::{NvmeTarget, SpdkConf};
use orpc::message::{Builder, RequestStatus};
use orpc::sys::DataSlice::Buffer;
use orpc::CommonResult;
use prost::bytes::BytesMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

static WORKER_CONF: OnceLock<ClusterConf> = OnceLock::new();

const DEFAULT_CHUNK_SIZE: i32 = 4096;
const DEFAULT_NUM_CHUNKS: i32 = 50;

// ============================================================================
// Configuration
// ============================================================================

fn get_worker() -> &'static ClusterConf {
    WORKER_CONF.get_or_init(|| {
        let mut conf = ClusterConf::default();

        conf.worker.rpc_port = NetUtils::hold_available_port();
        conf.worker.web_port = NetUtils::hold_available_port();
        conf.worker.data_dir = vec!["[SPDK]/tmp/curvine-spdk-stress".into()];

        let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
        let trsvcid: u16 = std::env::var("SPDK_TARGET_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4420);
        let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or_else(|_| "tcp".into());

        conf.worker.spdk = SpdkConf {
            enabled: true,
            app_name: "curvine-spdk-stress".into(),
            hugepage_mb: 64,
            reactor_mask: "0x2".into(),
            targets: vec![NvmeTarget {
                traddr,
                trsvcid,
                subnqn: "nqn.2024-01.io.curvine:test".into(),
                trtype,
                adrfam: "ipv4".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        conf.client.init().unwrap();
        let server = Worker::with_conf(conf.clone()).unwrap();
        thread::spawn(move || server.start_standalone());
        thread::sleep(Duration::from_secs(2));
        conf
    })
}

// ============================================================================
// I/O Helpers
// ============================================================================

fn write_block(
    id: i64,
    chunk_size: i32,
    num_chunks: i32,
    conf: &ClusterConf,
) -> CommonResult<(u64, Duration)> {
    let block_size = (chunk_size as i64) * (num_chunks as i64);
    let block = ExtendedBlock::new(id, block_size, StorageType::Spdk, FileType::File);

    let open_req = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block.clone()),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "stress".into(),
        chunk_size,
        pipeline_stream: vec![],
    };

    let client = conf.worker_sync_client()?;

    let req_id = Utils::req_id();
    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(-1)
        .proto_header(open_req)
        .build();

    let response: BlockWriteResponse = client.rpc_check(msg)?.parse_header()?;
    assert!(
        response.path.is_none(),
        "SPDK blocks must not short-circuit"
    );

    let mut checksum: u64 = 0;
    let start = Instant::now();

    for seq in 0..num_chunks {
        let data = BytesMut::from(Utils::rand_str(chunk_size as usize).as_str());
        checksum += Utils::crc32(&data) as u64;

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq)
            .data(Buffer(data))
            .build();

        let _ = client.rpc_check(msg)?;
    }

    let complete_req = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "stress".into(),
        chunk_size,
        pipeline_stream: vec![],
    };

    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(num_chunks)
        .proto_header(complete_req)
        .build();

    let _ = client.rpc_check(msg)?;
    let elapsed = start.elapsed();

    Ok((checksum, elapsed))
}

fn read_block(
    id: i64,
    chunk_size: i32,
    num_chunks: i32,
    conf: &ClusterConf,
) -> CommonResult<(u64, Duration)> {
    let total_len = (chunk_size as i64) * (num_chunks as i64);

    let open_req = BlockReadRequest {
        id,
        off: 0,
        len: total_len,
        chunk_size,
        short_circuit: false,
        ..Default::default()
    };

    let req_id = Utils::req_id();
    let msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(-1)
        .request(RequestStatus::Open)
        .proto_header(open_req)
        .build();

    let client = conf.worker_sync_client()?;
    let response: BlockReadResponse = client.rpc_check(msg)?.parse_header()?;

    let mut checksum: u64 = 0;
    let mut bytes_read: i64 = 0;
    let start = Instant::now();

    while bytes_read < response.len {
        let seq = (bytes_read / chunk_size as i64) as i32;
        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .req_id(req_id)
            .seq_id(seq)
            .request(RequestStatus::Running)
            .build();

        let rep = client.rpc_check(msg)?;
        if rep.data_len() == 0 {
            break;
        }

        bytes_read += rep.data_len() as i64;
        checksum += Utils::crc32(rep.data_bytes().unwrap()) as u64;
    }

    let msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(num_chunks)
        .build();

    let _ = client.rpc_check(msg)?;
    let elapsed = start.elapsed();

    Ok((checksum, elapsed))
}

// ============================================================================
// Tests
// ============================================================================

/// Test 1: Concurrent write/read with integrity check
#[test]
fn test_concurrent_io() -> CommonResult<()> {
    let conf = get_worker().clone();
    let num_threads = 2;
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let num_chunks = DEFAULT_NUM_CHUNKS;
    let block_bytes = (chunk_size * num_chunks) as u64;

    eprintln!("=== Concurrent I/O Test ===");
    eprintln!("Threads: {}, Block: {} KB", num_threads, block_bytes / 1024);

    let barrier = Arc::new(Barrier::new(num_threads));
    let errors = Arc::new(AtomicU64::new(0));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let barrier = barrier.clone();
            let conf = conf.clone();
            let errors = errors.clone();

            thread::spawn(move || {
                barrier.wait();

                let block_id = Utils::req_id().abs();
                match write_block(block_id, chunk_size, num_chunks, &conf) {
                    Ok((write_ck, write_dur)) => {
                        eprintln!(
                            "  Thread {}: wrote {} KB in {:?}",
                            block_id,
                            block_bytes / 1024,
                            write_dur
                        );

                        if let Ok((read_ck, _read_dur)) =
                            read_block(block_id, chunk_size, num_chunks, &conf)
                        {
                            if write_ck != read_ck {
                                eprintln!("  Thread {}: CHECKSUM MISMATCH!", tid);
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  Thread {}: ERROR: {}", tid, e);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panic");
    }

    let elapsed = wall_start.elapsed();
    let total_errors = errors.load(Ordering::Relaxed);

    eprintln!(
        "Time: {:.3}s, Errors: {}",
        elapsed.as_secs_f64(),
        total_errors
    );
    assert_eq!(total_errors, 0, "Test had {} errors", total_errors);

    eprintln!("=== PASSED ===");
    Ok(())
}

/// Test 2: I/O size sweep
#[test]
fn test_io_size_sweep() -> CommonResult<()> {
    let conf = get_worker().clone();
    let block_id = Utils::req_id().abs();

    let sizes = vec![("4K", 4096, 25), ("64K", 65536, 4)];

    eprintln!("=== I/O Size Sweep ===");

    for (name, chunk, num) in sizes {
        let block_bytes = (chunk * num) as u64;
        eprintln!("Profile {}: {} KB", name, block_bytes / 1024);

        let (ck, dur) = write_block(block_id, chunk as i32, num as i32, &conf)?;
        let _ = read_block(block_id, chunk as i32, num as i32, &conf)?;

        eprintln!("  Write: {} KB in {:?}", block_bytes / 1024, dur);
        eprintln!("  Checksum: {}", ck);
    }

    eprintln!("=== PASSED ===");
    Ok(())
}

/// Test 3: Sustained throughput
#[test]
fn test_sustained() -> CommonResult<()> {
    let conf = get_worker().clone();
    let chunk_size = 65536;
    let num_chunks = 4;
    let num_blocks = 5;

    eprintln!("=== Sustained Throughput ===");
    eprintln!(
        "Write {} blocks ({} KB each)",
        num_blocks,
        (chunk_size * num_chunks) / 1024
    );

    let mut blocks = vec![];
    let start = Instant::now();

    for _ in 0..num_blocks {
        let id = Utils::req_id().abs();
        blocks.push(id);
        let (_ck, dur) = write_block(id, chunk_size, num_chunks, &conf)?;
        eprintln!("  Wrote block {}: {} ms", id, dur.as_millis());
    }

    let write_elapsed = start.elapsed();
    let write_mb = (num_blocks * chunk_size * num_chunks) as f64 / (1024.0 * 1024.0);
    eprintln!(
        "Write: {:.1} MB in {:.2}s = {:.1} MB/s",
        write_mb,
        write_elapsed.as_secs_f64(),
        write_mb / write_elapsed.as_secs_f64()
    );

    let read_start = Instant::now();
    for id in &blocks {
        let _ = read_block(*id, chunk_size, num_chunks, &conf)?;
    }
    let read_elapsed = read_start.elapsed();
    eprintln!(
        "Read: {:.1} MB in {:.2}s = {:.1} MB/s",
        write_mb,
        read_elapsed.as_secs_f64(),
        write_mb / read_elapsed.as_secs_f64()
    );

    eprintln!("=== PASSED ===");
    Ok(())
}

/// Test 4: Concurrency ramp
#[test]
fn test_concurrency_ramp() -> CommonResult<()> {
    let conf = get_worker().clone();
    let levels = vec![1, 2, 4];

    eprintln!("=== Concurrency Ramp ===");

    for level in levels {
        let barrier = Arc::new(Barrier::new(level));
        let start = Instant::now();
        let block_bytes = (DEFAULT_CHUNK_SIZE * DEFAULT_NUM_CHUNKS) as u64;

        let handles: Vec<_> = (0..level)
            .map(|_| {
                let barrier = barrier.clone();
                let conf = conf.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let id = Utils::req_id().abs();
                    write_block(id, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)
                })
            })
            .collect();

        let mut total_ck = 0u64;
        for h in handles {
            if let Ok((ck, _)) = h.join().expect("panic") {
                total_ck += ck;
            }
        }

        let elapsed = start.elapsed();
        let mbps = (level as f64 * block_bytes as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();

        eprintln!(
            "{} threads: {:.1} MB/s (checksum: {})",
            level, mbps, total_ck
        );
    }

    eprintln!("=== PASSED ===");
    Ok(())
}

/// Test 5: Mixed read/write
#[test]
fn test_mixed_read_write() -> CommonResult<()> {
    let conf = get_worker().clone();

    eprintln!("=== Mixed Read/Write ===");

    let seed_id = Utils::req_id().abs();
    let (seed_ck, _) = write_block(seed_id, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)?;
    eprintln!("Seeded block {} with checksum {}", seed_id, seed_ck);

    let barrier = Arc::new(Barrier::new(2));
    let errors = Arc::new(AtomicU64::new(0));

    let w_handle = {
        let barrier = barrier.clone();
        let conf = conf.clone();
        thread::spawn(move || {
            barrier.wait();
            let id = Utils::req_id().abs();
            write_block(id, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)
        })
    };

    let r_handle = {
        let barrier = barrier.clone();
        let conf = conf.clone();
        thread::spawn(move || {
            barrier.wait();
            read_block(seed_id, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)
        })
    };

    let write_result = w_handle.join().expect("writer panic");
    let read_result = r_handle.join().expect("reader panic");

    if write_result.is_err() || read_result.is_err() {
        errors.fetch_add(1, Ordering::Relaxed);
    }

    let total_errors = errors.load(Ordering::Relaxed);
    assert_eq!(total_errors, 0, "Test had {} errors", total_errors);

    eprintln!("=== PASSED ===");
    Ok(())
}

/// Test 6: Read seek on non-zero offset
#[test]
fn test_read_seek_nonzero() -> CommonResult<()> {
    let conf = get_worker().clone();

    eprintln!("=== Read Seek Test ===");

    let id_a = Utils::req_id().abs();
    let (ck_a, _) = write_block(id_a, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)?;
    eprintln!("Block A: id={}, checksum={}", id_a, ck_a);

    let id_b = Utils::req_id().abs();
    let (ck_b, _) = write_block(id_b, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)?;
    eprintln!("Block B: id={}, checksum={}", id_b, ck_b);

    let (ck_b_read, _) = read_block(id_b, DEFAULT_CHUNK_SIZE, DEFAULT_NUM_CHUNKS, &conf)?;
    assert_eq!(ck_b, ck_b_read, "Read checksum must match write");

    eprintln!("=== PASSED ===");
    Ok(())
}
