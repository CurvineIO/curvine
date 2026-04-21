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
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice::Buffer;
use orpc::CommonResult;
use prost::bytes::BytesMut;
use std::thread;
const CHUNK_SIZE: i32 = 4096; // NVMe-aligned chunk size
const LOOP_NUM: i32 = 50;

/// Build an SPDK config from environment variables.
/// Start a worker with SPDK-backed storage.
fn start_spdk_worker() -> ClusterConf {
    common::require_spdk_test_env();
    let mut conf = ClusterConf::default();
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.web_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec!["[SPDK]/tmp/curvine-spdk-test".to_owned()];
    let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap();
    let trsvcid: u16 = std::env::var("SPDK_TARGET_PORT")
        .unwrap()
        .parse()
        .expect("SPDK_TARGET_PORT must be a valid u16");
    let subnqn = std::env::var("SPDK_TARGET_NQN").unwrap();
    let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or_else(|_| "tcp".into());

    conf.worker.spdk = SpdkConf {
        enabled: true,
        app_name: "curvine-spdk-test".to_string(),
        hugepage_mb: std::env::var("SPDK_HUGEPAGE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64),
        reactor_mask: std::env::var("SPDK_REACTOR_MASK").unwrap_or_else(|_| "0x2".to_string()),
        targets: vec![NvmeTarget {
            traddr,
            trsvcid,
            subnqn,
            trtype,
            adrfam: "ipv4".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };

    conf.client.init().unwrap();
    let server = Worker::with_conf(conf.clone()).unwrap();
    thread::spawn(move || server.start_standalone());
    thread::sleep(std::time::Duration::from_secs(2));
    conf
}
fn spdk_block_write(id: i64, conf: &ClusterConf) -> CommonResult<u64> {
    let block_size = (CHUNK_SIZE * LOOP_NUM) as i64;
    let block = ExtendedBlock::new(id, block_size, StorageType::Spdk, FileType::File);

    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "spdk-test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
    };

    let req_id = Utils::req_id();
    let mut seq_id = -1;
    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(seq_id)
        .proto_header(request)
        .build();

    let client = conf.worker_sync_client()?;
    let response: BlockWriteResponse = client.rpc(msg)?.parse_header()?;
    assert_eq!(response.off, 0);
    assert!(
        response.path.is_none(),
        "SPDK blocks must not return a short-circuit path"
    );
    assert_eq!(
        StorageType::from(response.storage_type),
        StorageType::Spdk,
        "Response storage_type should be Spdk"
    );

    seq_id += 1;
    let mut checksum: u64 = 0;
    for _ in 0..LOOP_NUM {
        let bytes = BytesMut::from(Utils::rand_str(CHUNK_SIZE as usize).as_str());
        checksum += Utils::crc32(&bytes) as u64;
        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .data(Buffer(bytes))
            .build();
        let _ = client.rpc(msg)?;
        seq_id += 1;
    }

    let block = ExtendedBlock::new(id, block_size, StorageType::Spdk, FileType::File);
    let complete_request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "spdk-test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
    };
    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(seq_id)
        .proto_header(complete_request)
        .build();
    let _: Message = client.rpc_check(msg)?;
    Ok(checksum)
}
fn spdk_block_read(id: i64, conf: &ClusterConf) -> CommonResult<u64> {
    let request = BlockReadRequest {
        id,
        off: 0,
        len: (CHUNK_SIZE * LOOP_NUM) as i64,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        ..Default::default()
    };

    let req_id = Utils::req_id();
    let mut seq_id = -1;
    let msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(seq_id)
        .request(RequestStatus::Open)
        .proto_header(request)
        .build();

    let client = conf.worker_sync_client()?;
    seq_id += 1;
    let rep: BlockReadResponse = client.rpc_check(msg)?.parse_header()?;
    assert!(
        rep.path.is_none(),
        "SPDK blocks must not return a short-circuit path"
    );
    assert_eq!(
        StorageType::from(rep.storage_type),
        StorageType::Spdk,
        "Response storage_type should be Spdk"
    );

    let mut start = 0;
    let mut check_sum: u64 = 0;
    while start < rep.len {
        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .req_id(req_id)
            .seq_id(seq_id)
            .request(RequestStatus::Running)
            .build();
        seq_id += 1;
        let rep = client.rpc_check(msg)?;
        if rep.data_len() == 0 {
            break;
        } else {
            start += rep.data_len() as i64;
            check_sum += Utils::crc32(rep.data_bytes().unwrap()) as u64;
        }
    }
    let msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(seq_id)
        .build();
    let _: Message = client.rpc(msg)?;
    Ok(check_sum)
}
// =========================================================================
// Tests
// =========================================================================

#[test]
fn test_spdk_worker_end_to_end() -> CommonResult<()> {
    let conf = start_spdk_worker();

    // Sub-test 1: write/read roundtrip with checksum
    eprintln!("--- sub-test: write/read roundtrip ---");
    {
        let block_id = Utils::req_id().abs();
        let write_ck = spdk_block_write(block_id, &conf)?;
        let read_ck = spdk_block_read(block_id, &conf)?;
        assert_eq!(
            write_ck, read_ck,
            "SPDK block write/read checksum mismatch: write={}, read={}",
            write_ck, read_ck
        );
        eprintln!("  PASSED (checksum={})", write_ck);
    }

    // Sub-test 2: short-circuit denied for SPDK blocks
    eprintln!("--- sub-test: short-circuit denied ---");
    {
        let block_id = Utils::req_id().abs();
        let block_size = (CHUNK_SIZE * 10) as i64;
        let block = ExtendedBlock::new(block_id, block_size, StorageType::Spdk, FileType::File);

        let request = BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(block),
            off: 0,
            block_size,
            short_circuit: true,
            client_name: "spdk-test".to_string(),
            chunk_size: CHUNK_SIZE,
            pipeline_stream: Vec::new(),
        };

        let req_id = Utils::req_id();
        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(0)
            .proto_header(request)
            .build();

        let client = conf.worker_sync_client()?;
        let response: BlockWriteResponse = client.rpc(msg)?.parse_header()?;
        assert!(
            response.path.is_none(),
            "SPDK blocks must deny short-circuit: path should be None, got {:?}",
            response.path
        );
        eprintln!("  PASSED");
    }

    // Sub-test 3: multiple blocks coexist on the same bdev
    eprintln!("--- sub-test: multiple blocks coexist ---");
    {
        let mut blocks = Vec::new();
        for _ in 0..3 {
            let block_id = Utils::req_id().abs();
            let write_ck = spdk_block_write(block_id, &conf)?;
            blocks.push((block_id, write_ck));
        }
        for (i, (block_id, write_ck)) in blocks.iter().enumerate() {
            let read_ck = spdk_block_read(*block_id, &conf)?;
            assert_eq!(
                *write_ck, read_ck,
                "Block {} (iter {}) checksum mismatch: write={}, read={}",
                block_id, i, write_ck, read_ck
            );
        }
        eprintln!("  PASSED (3 blocks written then all read back)");
    }
    eprintln!("=== All SPDK sub-tests passed ===");
    Ok(())
}
