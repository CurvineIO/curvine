// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse,
    BlocksBatchCommitRequest, BlocksBatchWriteRequest, BlocksBatchWriteResponse, DataHeaderProto,
    FileWriteData, FilesBatchWriteRequest,
};
use curvine_common::state::{ExtendedBlock, FileAllocOpts, FileType, StorageType};
use curvine_common::utils::ProtoUtils;
use curvine_server::worker::Worker;
use orpc::common::Utils;
use orpc::io::net::NetUtils;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice::Buffer;
use orpc::CommonResult;
use prost::bytes::BytesMut;
use std::thread;

#[cfg(feature = "fault-injection")]
use curvine_fault::{
    FaultController, FaultHttpController, FaultRuleBuilder, FaultRuntime, FaultTestSession,
    FaultValue,
};
#[cfg(feature = "fault-injection")]
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
#[cfg(feature = "fault-injection")]
use std::time::Duration;

const CHUNK_SIZE: i32 = 1024;
const LOOP_NUM: i32 = 100;

#[cfg(feature = "fault-injection")]
static WORKER_FAULT_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(feature = "fault-injection")]
fn worker_fault_test_serial() -> MutexGuard<'static, ()> {
    WORKER_FAULT_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// Test the worker interface function.
fn start_worker() -> ClusterConf {
    let mut conf = ClusterConf::default();
    // Use hold_available_port so the socket stays bound until RpcServer::run() claims it,
    // preventing TOCTOU races when nextest runs tests in parallel.
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.web_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec![format!(
        "[MEM:10MB]../testing/worker-test-{}",
        Utils::req_id().abs()
    )];
    conf.client.init().unwrap();

    let server = Worker::with_conf(conf.clone()).unwrap();
    thread::spawn(move || server.start_standalone());
    conf
}

#[cfg(feature = "fault-injection")]
fn start_worker_with_faults() -> ClusterConf {
    const TOKEN_ENV: &str = "CURVINE_WORKER_TEST_FAULT_TOKEN";
    std::env::set_var(TOKEN_ENV, "worker-test-secret");

    let mut conf = ClusterConf::default();
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.web_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec![format!(
        "[MEM:10MB]../testing/worker-fault-test-{}",
        Utils::req_id().abs()
    )];
    conf.fault_injection.enabled = true;
    conf.fault_injection.auth_token_env = TOKEN_ENV.to_string();
    conf.client.init().unwrap();

    let server = Worker::with_conf(conf.clone()).unwrap();
    let faults = FaultRuntime::process();
    faults
        .clear()
        .expect("fault-enabled Worker test must start from a clean Runtime");
    // Use Worker::block_on_start so the Web control plane is bound; the older
    // start_standalone path only starts the RPC server.
    thread::spawn(move || {
        if let Err(error) = server.block_on_start() {
            log::error!("fault-enabled worker failed to start: {error}");
            std::process::abort();
        }
    });
    conf
}

#[test]
fn test_worker_block_write_and_read_with_checksum_validation() -> CommonResult<()> {
    let conf = start_worker();

    let block_id = Utils::req_id().abs();
    let write_ck = block_write(block_id, &conf)?;
    let read_ck = block_read(block_id, &conf)?;

    assert_eq!(write_ck, read_ck);
    Ok(())
}

#[test]
fn test_worker_complete_oversized_block_aborts_pending_block() -> CommonResult<()> {
    let conf = start_worker();
    let client = conf.worker_sync_client()?;
    let block_id = Utils::req_id().abs();
    let block_size = CHUNK_SIZE as i64;
    let block = ExtendedBlock::new(block_id, block_size + 1, StorageType::Disk, FileType::File);
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
    };
    let req_id = Utils::req_id();

    let open = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(request.clone())
        .build();
    let _: BlockWriteResponse = client.rpc_check(open)?.parse_header()?;

    let complete = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(1)
        .proto_header(request)
        .build();
    let err = client.rpc_check(complete).unwrap_err();
    assert!(err.to_string().contains("Invalid block length"));

    let read_client = conf.worker_sync_client()?;
    let read = BlockReadRequest {
        id: block_id,
        off: 0,
        len: 1,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        ..Default::default()
    };
    let read_open = Builder::new()
        .code(RpcCode::ReadBlock)
        .request(RequestStatus::Open)
        .req_id(Utils::req_id())
        .seq_id(0)
        .proto_header(read)
        .build();
    let err = read_client.rpc_check(read_open).unwrap_err();
    assert!(
        err.to_string()
            .contains(&format!("Block {} not found", block_id)),
        "unexpected read error: {err}"
    );

    Ok(())
}

#[test]
fn test_worker_batch_short_circuit_complete_uses_open_context_without_server_file(
) -> CommonResult<()> {
    let conf = start_worker();
    let client = conf.worker_sync_client()?;
    let block_size = CHUNK_SIZE as i64;
    let blocks = [
        ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File),
        ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File),
    ];

    let open_req_id = Utils::req_id();
    let open = BlocksBatchWriteRequest {
        blocks: blocks
            .iter()
            .cloned()
            .map(ProtoUtils::extend_block_to_pb)
            .collect(),
        off: 0,
        block_size,
        req_id: open_req_id,
        seq_id: 0,
        chunk_size: CHUNK_SIZE,
        short_circuit: true,
        client_name: "test".to_string(),
    };
    let open_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Open)
        .req_id(open_req_id)
        .seq_id(0)
        .proto_header(open)
        .build();
    let response: BlocksBatchWriteResponse = client.rpc_check(open_msg)?.parse_header()?;
    assert_eq!(response.responses.len(), blocks.len());
    assert!(response
        .responses
        .iter()
        .all(|response| response.path.is_some()));

    let complete = BlocksBatchCommitRequest {
        blocks: blocks
            .iter()
            .cloned()
            .map(ProtoUtils::extend_block_to_pb)
            .collect(),
        off: 0,
        block_size,
        req_id: open_req_id,
        seq_id: 1,
        cancel: false,
    };
    let complete_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Complete)
        .req_id(complete.req_id)
        .seq_id(1)
        .proto_header(complete)
        .build();
    let _: Message = client.rpc_check(complete_msg)?;

    Ok(())
}

#[test]
fn test_worker_batch_short_circuit_complete_requires_open_connection() -> CommonResult<()> {
    let conf = start_worker();
    let open_client = conf.worker_sync_client()?;
    let complete_client = conf.worker_sync_client()?;
    let block_size = CHUNK_SIZE as i64;
    let blocks = [
        ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File),
        ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File),
    ];

    let req_id = Utils::req_id();
    let open = BlocksBatchWriteRequest {
        blocks: blocks
            .iter()
            .cloned()
            .map(ProtoUtils::extend_block_to_pb)
            .collect(),
        off: 0,
        block_size,
        req_id,
        seq_id: 0,
        chunk_size: CHUNK_SIZE,
        short_circuit: true,
        client_name: "test".to_string(),
    };
    let open_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(open)
        .build();
    let _: BlocksBatchWriteResponse = open_client.rpc_check(open_msg)?.parse_header()?;

    let complete = BlocksBatchCommitRequest {
        blocks: blocks
            .iter()
            .cloned()
            .map(ProtoUtils::extend_block_to_pb)
            .collect(),
        off: 0,
        block_size,
        req_id,
        seq_id: 1,
        cancel: false,
    };
    let complete_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(1)
        .proto_header(complete)
        .build();
    assert!(complete_client.rpc_check(complete_msg).is_err());

    Ok(())
}

#[test]
fn test_worker_batch_remote_write_complete_and_read_back() -> CommonResult<()> {
    let conf = start_worker();
    let client = conf.worker_sync_client()?;
    let block_size = CHUNK_SIZE as i64;
    let req_id = Utils::req_id();
    let mut blocks = [
        ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File),
        ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File),
    ];

    let open = BlocksBatchWriteRequest {
        blocks: blocks
            .iter()
            .cloned()
            .map(ProtoUtils::extend_block_to_pb)
            .collect(),
        off: 0,
        block_size,
        req_id,
        seq_id: 0,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        client_name: "test".to_string(),
    };
    let open_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(open)
        .build();
    let _: BlocksBatchWriteResponse = client.rpc_check(open_msg)?.parse_header()?;

    let contents = ["batch-block-a", "batch-block-b"];
    let write = FilesBatchWriteRequest {
        files: contents
            .iter()
            .map(|content| FileWriteData {
                path: String::new(),
                content: content.as_bytes().to_vec(),
            })
            .collect(),
        req_id,
        seq_id: 1,
    };
    let write_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Running)
        .req_id(req_id)
        .seq_id(1)
        .proto_header(write)
        .build();
    let _: Message = client.rpc_check(write_msg)?;

    let non_flush = DataHeaderProto {
        offset: 0,
        flush: false,
        is_last: false,
    };
    let non_flush_msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Running)
        .req_id(req_id)
        .seq_id(2)
        .proto_header(non_flush)
        .build();
    assert!(client.rpc_check(non_flush_msg).is_err());

    let flush = DataHeaderProto {
        offset: 0,
        flush: true,
        is_last: false,
    };
    let flush_msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Running)
        .req_id(req_id)
        .seq_id(3)
        .proto_header(flush)
        .build();
    let _: Message = client.rpc_check(flush_msg)?;

    for (block, content) in blocks.iter_mut().zip(contents) {
        block.len = content.len() as i64;
    }

    let complete = BlocksBatchCommitRequest {
        blocks: blocks
            .iter()
            .cloned()
            .map(ProtoUtils::extend_block_to_pb)
            .collect(),
        off: 0,
        block_size,
        req_id,
        seq_id: 4,
        cancel: false,
    };
    let complete_msg = Builder::new()
        .code(RpcCode::WriteBlocksBatch)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(4)
        .proto_header(complete)
        .build();
    let _: Message = client.rpc_check(complete_msg)?;
    for (block, content) in blocks.iter().zip(contents) {
        assert_eq!(
            block_read_bytes(block.id, content.len() as i64, &conf)?,
            content.as_bytes()
        );
    }

    Ok(())
}

#[cfg(feature = "fault-injection")]
#[test]
// Host-integration coverage: the generic crate tests cannot prove that a
// Worker mounts the HTTP router and evaluates a real RPC point in one process.
fn test_worker_fault_http_control_plane_e2e() -> CommonResult<()> {
    let _serial = worker_fault_test_serial();
    let conf = start_worker_with_faults();
    let token = std::env::var("CURVINE_WORKER_TEST_FAULT_TOKEN")
        .expect("start_worker_with_faults sets the fault token env");
    let base = format!("http://{}:{}", conf.worker.hostname, conf.worker.web_port);
    let open_req_id = Utils::req_id();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let session = rt.block_on(async {
        let controller = Arc::new(
            FaultHttpController::new(&base, &token)
                .map_err(|e| orpc::CommonError::from(e.to_string()))?,
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            match controller.status().await {
                Ok(status) => {
                    assert!(status
                        .points
                        .iter()
                        .any(|point| point.name == "worker.rpc.before_dispatch"));
                    break;
                }
                Err(error) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = error;
                }
                Err(error) => {
                    return Err(orpc::CommonError::from(format!(
                        "worker fault HTTP never became ready at {base}: {error}"
                    )));
                }
            }
        }

        let mut session = FaultTestSession::new();
        session.add_target("worker", controller);
        session
            .preflight()
            .await
            .map_err(|e| orpc::CommonError::from(e.to_string()))?;
        let rule = FaultRuleBuilder::named("worker.rpc.before_dispatch")
            .matches("req_id", open_req_id)
            .and_then(|builder| builder.matches("rpc_code", RpcCode::WriteBlock as i32))
            .and_then(|builder| builder.matches("request_status", i8::from(RequestStatus::Open)))
            .and_then(|builder| builder.times(1))
            .and_then(|builder| builder.return_error("worker HTTP control-plane failure"))
            .map_err(|e| orpc::CommonError::from(e.to_string()))?;
        session
            .configure("worker", "http-fail-write-open", rule)
            .await
            .map_err(|e| orpc::CommonError::from(e.to_string()))?;

        Ok::<FaultTestSession, orpc::CommonError>(session)
    })?;

    let block_size = (CHUNK_SIZE * LOOP_NUM) as i64;
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(ExtendedBlock::new(
            Utils::req_id().abs(),
            block_size,
            StorageType::Disk,
            FileType::File,
        )),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "fault-http-test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
    };
    let open = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(open_req_id)
        .seq_id(0)
        .proto_header(request)
        .build();
    assert!(conf.worker_sync_client()?.rpc_check(open).is_err());

    rt.block_on(async {
        let rule = session
            .wait_for_executions("worker", "http-fail-write-open", 1, Duration::from_secs(5))
            .await
            .map_err(|e| orpc::CommonError::from(e.to_string()))?;
        assert_eq!(rule.executions, 1);
        assert_eq!(
            rule.last_context.as_ref().unwrap().get("rpc_code"),
            Some(&FaultValue::I64(RpcCode::WriteBlock as i64))
        );
        assert_eq!(
            rule.last_context.as_ref().unwrap().get("request_status"),
            Some(&FaultValue::I64(RequestStatus::Open as i8 as i64))
        );
        session
            .cleanup()
            .await
            .map_err(|e| orpc::CommonError::from(e.to_string()))?;
        Ok::<(), orpc::CommonError>(())
    })?;

    let healthy_block = Utils::req_id().abs();
    let write_ck = block_write(healthy_block, &conf)?;
    let read_ck = block_read(healthy_block, &conf)?;
    assert_eq!(write_ck, read_ck);
    Ok(())
}

fn block_write(id: i64, conf: &ClusterConf) -> CommonResult<u64> {
    let block_size = (CHUNK_SIZE * LOOP_NUM) as i64;
    let block = ExtendedBlock::new(id, block_size, StorageType::Disk, FileType::File);
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "test".to_string(),
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
    seq_id += 1;

    let mut checksum: u64 = 0;
    for _ in 0..LOOP_NUM {
        let bytes = BytesMut::from(Utils::rand_str(CHUNK_SIZE as usize).as_str());
        checksum += Utils::crc32(&bytes) as u64;

        // write data
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

    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(seq_id)
        .build();

    let _: Message = client.rpc(msg)?;

    Ok(checksum)
}

#[test]
fn test_worker_short_circuit_open_resizes_block_file() -> CommonResult<()> {
    let conf = start_worker();
    let block_id = Utils::req_id().abs();
    let block_size = CHUNK_SIZE as i64;
    let target_len = block_size / 2;
    let mut block = ExtendedBlock::new(block_id, target_len, StorageType::Disk, FileType::File);
    block.alloc_opts = Some(FileAllocOpts::with_truncate(target_len));

    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block.clone()),
        off: 0,
        block_size,
        short_circuit: true,
        client_name: "test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
    };

    let req_id = Utils::req_id();
    let open = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(request)
        .build();

    let client = conf.worker_sync_client()?;
    let response: BlockWriteResponse = client.rpc(open)?.parse_header()?;
    let path = response
        .path
        .as_ref()
        .expect("short-circuit write should return local path");
    assert_eq!(
        std::fs::metadata(path)?.len(),
        target_len as u64,
        "short-circuit open must apply alloc_opts resize on worker before client writes"
    );

    block.len = target_len;
    let complete_block = ProtoUtils::extend_block_to_pb(block);
    let complete = BlockWriteRequest {
        block: complete_block,
        off: 0,
        block_size,
        short_circuit: true,
        client_name: "test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
    };
    let complete_msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(1)
        .proto_header(complete)
        .build();
    let _: Message = client.rpc(complete_msg)?;

    Ok(())
}

fn block_read(id: i64, conf: &ClusterConf) -> CommonResult<u64> {
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
    println!("read-reap: {:#?}", rep);

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
        println!("rep {}", rep.data.len());
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

fn block_read_bytes(id: i64, len: i64, conf: &ClusterConf) -> CommonResult<Vec<u8>> {
    let request = BlockReadRequest {
        id,
        off: 0,
        len,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        ..Default::default()
    };

    let req_id = Utils::req_id();
    let client = conf.worker_sync_client()?;
    let open = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(0)
        .request(RequestStatus::Open)
        .proto_header(request)
        .build();
    let _: BlockReadResponse = client.rpc_check(open)?.parse_header()?;

    let read = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(1)
        .request(RequestStatus::Running)
        .build();
    let response = client.rpc_check(read)?;
    let data = response.data.as_slice().to_vec();

    let complete = Builder::new()
        .code(RpcCode::ReadBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(2)
        .build();
    let _: Message = client.rpc_check(complete)?;

    Ok(data)
}
