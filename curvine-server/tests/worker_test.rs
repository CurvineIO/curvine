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

use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice::Buffer;
use curvine_model::ProtoUtils;
use curvine_model::{ExtendedBlock, FileAllocOpts, FileType, StorageType};
use curvine_net::net::NetUtils;
use curvine_proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse,
    BlocksBatchCommitRequest, BlocksBatchWriteRequest, BlocksBatchWriteResponse,
    ComponentInfoProto, DataHeaderProto, FileWriteData, FilesBatchWriteRequest,
};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use curvine_runtime::common::Utils;
use curvine_server::worker::Worker;
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

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires strace syscall injection and permission to trace child processes"]
fn test_worker_io_uring_setup_denied_falls_back_over_rpc() -> CommonResult<()> {
    const CHILD_DIR: &str = "CURVINE_TEST_IO_URING_FALLBACK_DIR";
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        return io_uring_fallback_rpc_roundtrip(std::path::Path::new(&dir));
    }

    // Isolate the worker's process-wide configuration and logger. Deny only
    // ring setup in this child, including on the executor's startup thread.
    let dir = tempfile::tempdir()?;
    let output = std::process::Command::new("strace")
        .args([
            "-f",
            "-qq",
            "-e",
            "trace=io_uring_setup",
            "-e",
            "inject=io_uring_setup:error=EPERM",
            "--",
        ])
        .arg(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            "test_worker_io_uring_setup_denied_falls_back_over_rpc",
            "--nocapture",
        ])
        .env(CHILD_DIR, dir.path())
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trace = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "worker RPC test failed:\n{stdout}\n{trace}"
    );
    assert!(
        trace.lines().any(|line| line.contains("io_uring_setup(")
            && line.contains("EPERM")
            && line.contains("(INJECTED)")),
        "the kernel setup denial was not injected:\n{trace}"
    );

    let logs = std::fs::read_to_string(dir.path().join("worker.log"))?;
    let selection: Vec<_> = logs
        .lines()
        .filter(|line| line.contains("Worker remote file I/O:"))
        .collect();
    assert_eq!(
        selection.len(),
        1,
        "expected one startup backend diagnostic: {selection:?}"
    );
    assert!(
        selection[0].contains("requested=io_uring, effective=local_file; fallback:"),
        "{}",
        selection[0]
    );
    assert!(
        selection[0].contains("(os error 1)"),
        "expected the EPERM setup reason: {}",
        selection[0]
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn io_uring_tcp_conf(dir: &std::path::Path) -> CommonResult<ClusterConf> {
    let mut conf = ClusterConf::default();
    conf.worker.hostname = "127.0.0.1".to_string();
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.web_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec![format!("[DISK:10MB]{}", dir.join("blocks").display())];
    conf.worker.enable_io_uring = true;
    conf.worker.enable_send_file = false;
    conf.worker.enable_splice = false;
    conf.worker.log.log_dir = dir.to_string_lossy().into_owned();
    conf.worker.log.file_name = "worker.log".to_string();
    conf.client.short_circuit = false;
    conf.client.enable_read_ahead = false;
    conf.client.init()?;
    Ok(conf)
}

#[cfg(target_os = "linux")]
fn io_uring_fallback_rpc_roundtrip(dir: &std::path::Path) -> CommonResult<()> {
    let conf = io_uring_tcp_conf(dir)?;

    // This guard flushes the test log before the child process exits.
    let _logger = curvine_runtime::common::Logger::new(conf.worker.log.clone());
    let worker = Worker::with_conf(conf.clone())?;
    thread::spawn(move || worker.start_standalone());

    let payload = b"curvine\x00io_uring\xfffallback";
    let block = ExtendedBlock::new(
        Utils::req_id().abs(),
        payload.len() as i64,
        StorageType::Disk,
        FileType::File,
    );
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block.clone()),
        block_size: CHUNK_SIZE as i64,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        ..Default::default()
    };
    let client = conf.worker_sync_client()?;
    let req_id = Utils::req_id();
    let open = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(request.clone())
        .build();
    let _: BlockWriteResponse = client.rpc_check(open)?.parse_header()?;
    let write = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Running)
        .req_id(req_id)
        .seq_id(1)
        .proto_header(DataHeaderProto {
            offset: 0,
            flush: true,
            is_last: false,
        })
        .data(Buffer(BytesMut::from(payload.as_slice())))
        .build();
    client.rpc_check(write)?;
    let complete = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(2)
        .proto_header(request)
        .build();
    client.rpc_check(complete)?;
    assert_eq!(block_read_bytes(block.id, block.len, &conf)?, payload);
    Ok(())
}

#[cfg(target_os = "linux")]
mod real_io_uring {
    use super::*;
    use curvine_proto::{BlocksBatchCommitResponse, FilesBatchWriteResponse};
    use curvine_rpc::client::RpcClient;
    use curvine_runtime::common::Logger;
    use std::path::Path;
    use std::process::Command;
    use std::time::{Duration, Instant};

    fn isolated_worker(
        test: &str,
        restart: bool,
        workload: impl FnOnce(&ClusterConf, bool) -> CommonResult<()>,
    ) -> CommonResult<()> {
        const CHILD_DIR: &str = "CURVINE_TEST_REAL_IO_URING_DIR";
        const CHILD_RESTART: &str = "CURVINE_TEST_REAL_IO_URING_RESTART";
        if let Some(dir) = std::env::var_os(CHILD_DIR) {
            let restarting = std::env::var(CHILD_RESTART).as_deref() == Ok("1");
            let mut conf = io_uring_tcp_conf(Path::new(&dir))?;
            conf.format_worker = !restarting;
            conf.worker.io_threads = 2;
            conf.worker.worker_threads = 2;
            conf.client.io_threads = 1;
            conf.client.worker_threads = 1;
            let _logger = Logger::new(conf.worker.log.clone());
            let worker = Worker::with_conf(conf.clone())?;
            thread::spawn(move || worker.start_standalone());
            return workload(&conf, restarting);
        }

        // Worker configuration and counters are process globals. On restart
        // reuse only the disk, not the old worker's runtime or metrics.
        let dir = tempfile::tempdir()?;
        let test = format!("real_io_uring::{test}");
        let run = |restarting| -> CommonResult<()> {
            let output = Command::new(std::env::current_exe()?)
                .args(["--ignored", "--exact", &test, "--nocapture"])
                .env(CHILD_DIR, dir.path())
                .env(CHILD_RESTART, if restarting { "1" } else { "0" })
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            if !output.status.success() || !stdout.lines().any(|line| line == "running 1 test") {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let log =
                    std::fs::read_to_string(dir.path().join("worker.log")).unwrap_or_default();
                let backend = log
                    .lines()
                    .rev()
                    .find(|line| line.contains("Worker remote file I/O:"))
                    .unwrap_or("no backend startup diagnostic");
                panic!(
                    "{test} (restart={restarting}) failed: {}\n{backend}\n{stdout}\n{stderr}",
                    output.status
                );
            }
            Ok(())
        };
        run(false)?;
        if restart {
            run(true)?;
        }
        Ok(())
    }

    fn file_block(id: i64, len: usize) -> ExtendedBlock {
        ExtendedBlock::new(id, len as i64, StorageType::Disk, FileType::File)
    }

    fn write_request(block: &ExtendedBlock) -> BlockWriteRequest {
        BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(block.clone()),
            block_size: CHUNK_SIZE as i64,
            chunk_size: CHUNK_SIZE,
            short_circuit: false,
            ..Default::default()
        }
    }

    fn single_control(
        req_id: i64,
        seq_id: i32,
        status: RequestStatus,
        request: &BlockWriteRequest,
    ) -> Message {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request.clone())
            .build()
    }

    fn running(req_id: i64, seq_id: i32, offset: i64, flush: bool, data: &[u8]) -> Message {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(DataHeaderProto {
                offset,
                flush,
                is_last: false,
            })
            .data(Buffer(BytesMut::from(data)))
            .build()
    }

    fn assert_stale_terminals(
        conf: &ClusterConf,
        seq_id: i32,
        frame: impl Fn(i32, RequestStatus) -> Message,
    ) -> CommonResult<()> {
        let stale = conf.worker_sync_client()?;
        for (seq_id, status) in [
            (seq_id, RequestStatus::Complete),
            (seq_id + 1, RequestStatus::Cancel),
        ] {
            // Check a worker error response, not merely a transport error.
            let reply = stale.rpc(frame(seq_id, status))?;
            assert!(
                !reply.is_success(),
                "stale {status:?} without Open was accepted"
            );
        }
        Ok(())
    }

    fn assert_ring_read(
        conf: &ClusterConf,
        block_id: i64,
        expected: &[u8],
        ring_bytes: usize,
    ) -> CommonResult<()> {
        let metrics = Worker::get_metrics()?;
        let before = metrics.io_uring_read_bytes.get();
        let data = block_read_bytes(block_id, expected.len() as i64, conf)?;
        assert_eq!(data, expected, "block {block_id} bytes");
        assert_eq!(
            metrics.io_uring_read_bytes.get() - before,
            if cfg!(feature = "fault-injection") {
                0
            } else {
                ring_bytes as i64
            },
            "ReadBlock of {block_id}: only eligible finalized reads should reach the ring"
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires Linux with working io_uring (fallback is a test failure)"]
    fn write_block_over_tcp() -> CommonResult<()> {
        isolated_worker("write_block_over_tcp", false, |conf, _| {
            let original = b"single\x00\xffblock-tail";
            let replacement = b"\x80NEW\x00";
            let offset = 2;
            let mut expected = original.to_vec();
            expected[offset..offset + replacement.len()].copy_from_slice(replacement);
            let block = file_block(1, original.len());
            let request = write_request(&block);
            let req_id = Utils::req_id();
            let control = |seq, status| single_control(req_id, seq, status, &request);
            let writer = conf.worker_sync_client()?;
            let open: BlockWriteResponse = writer
                .rpc_check(control(0, RequestStatus::Open))?
                .parse_header()?;
            assert!(open.path.is_none());

            let metrics = Worker::get_metrics()?;
            let before = metrics.io_uring_write_bytes.get();
            writer.rpc_check(running(req_id, 1, 0, false, original))?;
            assert_eq!(
                metrics.io_uring_write_bytes.get() - before,
                original.len() as i64,
                "WriteBlock Running must complete its payload through io_uring before any flush"
            );
            assert_ring_read(conf, block.id, original, 0)?;
            writer.rpc_check(running(req_id, 2, 0, true, &[]))?;

            let before = metrics.io_uring_write_bytes.get();
            writer.rpc_check(running(req_id, 3, offset as i64, false, replacement))?;
            assert_eq!(
                metrics.io_uring_write_bytes.get() - before,
                replacement.len() as i64,
                "the backward overwrite must also use io_uring"
            );
            writer.rpc_check(running(req_id, 4, 0, true, &[]))?;
            assert_stale_terminals(conf, 5, control)?;
            assert_ring_read(conf, block.id, &expected, 0)?;
            writer.rpc_check(control(5, RequestStatus::Complete))?;
            // A new connection can only acknowledge this Complete as a replay
            // once the replica is finalized, not just visible as staging.
            conf.worker_sync_client()?
                .rpc_check(control(5, RequestStatus::Complete))?;
            assert_ring_read(conf, block.id, &expected, expected.len())?;
            let mut with_read_ahead = conf.clone();
            with_read_ahead.client.enable_read_ahead = true;
            assert_ring_read(&with_read_ahead, block.id, &expected, 0)
        })
    }

    #[test]
    #[ignore = "requires Linux with working io_uring (fallback is a test failure)"]
    fn write_blocks_batch_over_tcp() -> CommonResult<()> {
        isolated_worker("write_blocks_batch_over_tcp", false, |conf, _| {
            let contents: [&[u8]; 3] = [b"first\xff", b"middle\0\xc0", b"last-sibling"];
            let metrics = Worker::get_metrics()?;
            for (ids, fail_middle) in [([1, 2, 3], false), ([4, 5, 6], true)] {
                let mut blocks = ids.map(|id| file_block(id, 0));
                let req_id = Utils::req_id();
                let writer = conf.worker_sync_client()?;
                let frame = |seq, status| {
                    Builder::new()
                        .code(RpcCode::WriteBlocksBatch)
                        .request(status)
                        .req_id(req_id)
                        .seq_id(seq)
                };
                let block_protos = |blocks: &[ExtendedBlock]| {
                    blocks
                        .iter()
                        .cloned()
                        .map(ProtoUtils::extend_block_to_pb)
                        .collect()
                };
                let open: BlocksBatchWriteResponse = writer
                    .rpc_check(
                        frame(0, RequestStatus::Open)
                            .proto_header(BlocksBatchWriteRequest {
                                blocks: block_protos(&blocks),
                                block_size: CHUNK_SIZE as i64,
                                chunk_size: CHUNK_SIZE,
                                short_circuit: false,
                                req_id,
                                seq_id: 0,
                                ..Default::default()
                            })
                            .build(),
                    )?
                    .parse_header()?;
                assert_eq!(open.responses.len(), blocks.len());
                assert!(open
                    .responses
                    .iter()
                    .all(|response| response.path.is_none()));

                let write = |seq_id, contents: &[&[u8]]| {
                    frame(seq_id, RequestStatus::Running)
                        .proto_header(FilesBatchWriteRequest {
                            files: contents
                                .iter()
                                .map(|data| FileWriteData {
                                    path: String::new(),
                                    content: data.to_vec(),
                                })
                                .collect(),
                            req_id,
                            seq_id,
                            component_info: None,
                        })
                        .build()
                };
                let before = metrics.io_uring_write_bytes.get();
                let written: FilesBatchWriteResponse =
                    writer.rpc_check(write(1, &contents))?.parse_header()?;
                assert_eq!(written.results, [true; 3]);
                assert_eq!(
                    metrics.io_uring_write_bytes.get() - before,
                    contents.iter().map(|data| data.len() as i64).sum::<i64>(),
                    "every WriteBlocksBatch Running payload must complete through io_uring before flush"
                );
                for (block, data) in blocks.iter_mut().zip(contents) {
                    assert_ring_read(conf, block.id, data, 0)?;
                    block.len = data.len() as i64;
                }
                // A batch flush uses WriteBlock/Running on the same connection.
                writer.rpc_check(running(req_id, 2, 0, true, &[]))?;
                let terminal = |seq, status| {
                    frame(seq, status)
                        .proto_header(BlocksBatchCommitRequest {
                            blocks: block_protos(&blocks),
                            block_size: CHUNK_SIZE as i64,
                            req_id,
                            seq_id: seq,
                            cancel: status == RequestStatus::Cancel,
                            ..Default::default()
                        })
                        .build()
                };
                if fail_middle {
                    // Slot 0 can append. Slot 1's append would exceed its
                    // reservation; slot 2 must never reach the ring.
                    let extra = b"discard this append";
                    let oversized = vec![0xa5; CHUNK_SIZE as usize];
                    let before = metrics.io_uring_write_bytes.get();
                    let reply = writer.rpc(write(3, &[extra, &oversized, b"never written"]))?;
                    assert!(
                        !reply.is_success() && reply.to_error_msg().contains("exceeds block size"),
                        "expected middle block write-range error, got {reply:?}"
                    );
                    assert_eq!(
                        metrics.io_uring_write_bytes.get() - before,
                        extra.len() as i64,
                        "only the first slot may complete a ring write in the failed Running frame"
                    );
                    let reply = writer.rpc(terminal(4, RequestStatus::Complete))?;
                    assert!(!reply.is_success(), "the failed batch must not commit");
                    assert_stale_terminals(conf, 4, terminal)?;
                    // The rolled-back writer may acknowledge Cancel as a
                    // replay, but it no longer owns the retained checkpoints.
                    writer.rpc_check(terminal(5, RequestStatus::Cancel))?;
                    for (block, data) in blocks.iter().zip(contents) {
                        let mut checkpoint = data.to_vec();
                        checkpoint.push(0); // No unflushed byte may survive the rollback.
                        assert_ring_read(conf, block.id, &checkpoint, 0)?;
                    }
                } else {
                    assert_stale_terminals(conf, 3, terminal)?;
                    let committed: BlocksBatchCommitResponse = writer
                        .rpc_check(terminal(3, RequestStatus::Complete))?
                        .parse_header()?;
                    assert_eq!(committed.results, [true; 3]);
                    let observer = conf.worker_sync_client()?;
                    for (block, data) in blocks.iter().zip(contents) {
                        // A read alone could still be served by staging. A
                        // single-block terminal replay requires a finalized replica.
                        observer.rpc_check(single_control(
                            Utils::req_id(),
                            0,
                            RequestStatus::Complete,
                            &write_request(block),
                        ))?;
                        assert_ring_read(conf, block.id, data, data.len())?;
                    }
                }
            }
            Ok(())
        })
    }

    #[test]
    #[ignore = "requires Linux with working io_uring (fallback is a test failure)"]
    fn buffered_complete_and_abandoned_write_after_restart_over_tcp() -> CommonResult<()> {
        isolated_worker(
            "buffered_complete_and_abandoned_write_after_restart_over_tcp",
            true,
            |conf, restarting| {
                let payload = b"buffered-complete\0\xff";
                if restarting {
                    assert_ring_read(conf, 1, payload, payload.len())?;
                    // No Complete was sent for block 2. Its private inode must not
                    // be found after a process stop, even if EOF cleanup did not run.
                    if let Ok(bytes) = block_read_bytes(2, payload.len() as i64, conf) {
                        assert_eq!(bytes, vec![0; payload.len()]);
                    }
                    return Ok(());
                }
                let metrics = Worker::get_metrics()?;
                let before = metrics.io_uring_write_bytes.get();
                for id in [1, 2] {
                    let block = file_block(id, payload.len());
                    let request = write_request(&block);
                    let writer = conf.worker_sync_client()?;
                    let req_id = Utils::req_id();
                    let control = |seq, status| single_control(req_id, seq, status, &request);
                    writer.rpc_check(control(0, RequestStatus::Open))?;
                    writer.rpc_check(running(req_id, 1, 0, false, payload))?;
                    if id == 1 {
                        writer.rpc_check(control(2, RequestStatus::Complete))?;
                    } else {
                        assert_stale_terminals(conf, 2, control)?;
                    }
                }
                assert_eq!(
                    metrics.io_uring_write_bytes.get() - before,
                    (payload.len() * 2) as i64
                );
                assert_ring_read(conf, 1, payload, payload.len())
            },
        )
    }

    #[test]
    #[ignore = "requires Linux with working io_uring (fallback is a test failure)"]
    fn async_write_disconnect_and_restart_over_tcp() -> CommonResult<()> {
        isolated_worker(
            "async_write_disconnect_and_restart_over_tcp",
            true,
            |conf, restarting| {
                let checkpoint = b"flushed\0\xffprefix";
                let tail = b"not-flushed";
                let block = file_block(1, checkpoint.len());
                let live = [checkpoint.as_slice(), tail.as_slice()].concat();
                let mut recovered = checkpoint.to_vec();
                recovered.resize(live.len(), 0);
                if restarting {
                    return assert_ring_read(conf, block.id, &recovered, 0);
                }

                let request = write_request(&block);
                let req_id = Utils::req_id();
                let control = |seq, status| single_control(req_id, seq, status, &request);
                let metrics = Worker::get_metrics()?;
                let before = metrics.io_uring_write_bytes.get();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let writer = rt.block_on(async {
                    let writer =
                        RpcClient::with_raw(&conf.worker_addr(), &conf.client_rpc_conf()).await?;
                    for frame in [
                        control(0, RequestStatus::Open),
                        running(req_id, 1, 0, false, checkpoint),
                        running(req_id, 2, 0, true, &[]),
                        running(req_id, 3, checkpoint.len() as i64, false, tail),
                    ] {
                        let reply = writer.timeout_rpc(Duration::from_secs(10), frame).await?;
                        assert!(reply.is_success(), "async writer: {}", reply.to_error_msg());
                    }
                    Ok::<_, curvine_io::IOError>(writer)
                })?;
                assert_eq!(
                    metrics.io_uring_write_bytes.get() - before,
                    live.len() as i64,
                    "both async Running payloads must complete through io_uring"
                );
                assert_ring_read(conf, block.id, &live, 0)?;
                drop(writer);

                // EOF is handled asynchronously; wait for reads to switch from
                // the private live view to the durable checkpoint, without a Complete.
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let bytes = block_read_bytes(block.id, live.len() as i64, conf)?;
                    if bytes == recovered {
                        break;
                    }
                    assert_eq!(bytes, live, "disconnect exposed a partial generation");
                    assert!(
                        Instant::now() < deadline,
                        "unflushed tail remained after disconnect"
                    );
                    thread::sleep(Duration::from_millis(10));
                }
                assert_stale_terminals(conf, 4, control)?;
                assert_ring_read(conf, block.id, &recovered, 0)
            },
        )
    }
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
        component_info: None,
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
fn test_worker_stale_write_terminals_cannot_mutate_pending_file() -> CommonResult<()> {
    let conf = start_worker();
    let published = b"published";
    let pending = b"rewritten"; // Same length, so the old replica cannot justify a replay.
    let block = ExtendedBlock::new(
        Utils::req_id().abs(),
        published.len() as i64,
        StorageType::Disk,
        FileType::File,
    );
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block.clone()),
        block_size: CHUNK_SIZE as i64,
        chunk_size: CHUNK_SIZE,
        ..Default::default()
    };
    let control = |req_id, seq_id, status| {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request.clone())
            .build()
    };
    let write_and_flush = |req_id, data: &[u8]| {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(1)
            .proto_header(DataHeaderProto {
                offset: 0,
                flush: true,
                is_last: false,
            })
            .data(Buffer(BytesMut::from(data)))
            .build()
    };

    let check_stale_terminals = |req_id| -> CommonResult<()> {
        let stale = conf.worker_sync_client()?;
        for (seq_id, status) in [(2, RequestStatus::Complete), (3, RequestStatus::Cancel)] {
            let err = stale
                .rpc_check(control(req_id, seq_id, status))
                .unwrap_err();
            assert!(
                err.to_string().contains("requires a successful Open"),
                "unexpected stale {status:?} error: {err}"
            );
        }
        Ok(())
    };

    let writer = conf.worker_sync_client()?;
    let req_id = Utils::req_id();
    writer.rpc_check(control(req_id, 0, RequestStatus::Open))?;
    writer.rpc_check(write_and_flush(req_id, published))?;
    check_stale_terminals(req_id)?;
    writer.rpc_check(control(req_id, 2, RequestStatus::Complete))?;
    // After handler retirement, a compatible terminal on a finalized block
    // still gets an idempotent acknowledgment.
    writer.rpc_check(control(req_id, 2, RequestStatus::Complete))?;
    writer.rpc_check(control(req_id, 3, RequestStatus::Cancel))?;
    assert_eq!(block_read_bytes(block.id, block.len, &conf)?, published);

    let rewrite_id = Utils::req_id();
    writer.rpc_check(control(rewrite_id, 0, RequestStatus::Open))?;
    writer.rpc_check(write_and_flush(rewrite_id, pending))?;
    check_stale_terminals(rewrite_id)?;
    drop(writer);
    check_stale_terminals(rewrite_id)?;

    // The disconnected writer's checkpoint was not published. A new writer
    // that opens it can still complete it, proving Cancel did not delete it.
    assert_eq!(block_read_bytes(block.id, block.len, &conf)?, published);
    let writer = conf.worker_sync_client()?;
    let req_id = Utils::req_id();
    writer.rpc_check(control(req_id, 0, RequestStatus::Open))?;
    writer.rpc_check(control(req_id, 1, RequestStatus::Complete))?;
    assert_eq!(block_read_bytes(block.id, block.len, &conf)?, pending);
    Ok(())
}

#[test]
fn test_worker_cancel_replay_on_absent_block_requires_empty_body() -> CommonResult<()> {
    let conf = start_worker();
    let block = ExtendedBlock::new(Utils::req_id().abs(), 0, StorageType::Disk, FileType::File);
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        block_size: CHUNK_SIZE as i64,
        chunk_size: CHUNK_SIZE,
        ..Default::default()
    };
    let req_id = Utils::req_id();
    let frame = |seq_id, status| {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request.clone())
            .build()
    };
    let client = conf.worker_sync_client()?;
    client.rpc_check(frame(0, RequestStatus::Open))?;
    client.rpc_check(frame(1, RequestStatus::Cancel))?;
    client.rpc_check(frame(1, RequestStatus::Cancel))?;
    assert!(!client.rpc(frame(2, RequestStatus::Complete))?.is_success());

    let mut cancel_with_data = frame(3, RequestStatus::Cancel);
    cancel_with_data.data = Buffer(BytesMut::from(&b"unexpected"[..]));
    assert!(!client.rpc(cancel_with_data)?.is_success());
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
        component_info: None,
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
        component_info: None,
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
        component_info: None,
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
        component_info: None,
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
        component_info: None,
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
        component_info: None,
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
        component_info: None,
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

#[test]
fn test_worker_running_flush_only_no_complete() -> CommonResult<()> {
    // Regression test for GH#1673: a flush-only Running frame must
    // reach file.flush() even when no Complete frame follows.
    let (conf, tmp_dir) = start_worker_with_tempdir();
    let client = conf.worker_sync_client()?;
    let block_size = CHUNK_SIZE as i64;
    let req_id = Utils::req_id();

    // --- Open a block ---
    let block = ExtendedBlock::new(req_id, block_size, StorageType::Disk, FileType::File);
    let open = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
        component_info: None,
    };
    let open_msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(open)
        .build();
    let _: BlockWriteResponse = client.rpc_check(open_msg)?.parse_header()?;

    // --- Write data with a normal Running frame ---
    let write_data = b"hello-worker-flush-test";
    let write_msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Running)
        .req_id(req_id)
        .seq_id(1)
        .data(curvine_io::DataSlice::Buffer(BytesMut::from(
            &write_data[..],
        )))
        .build();
    let _: Message = client.rpc_check(write_msg)?;

    // --- Send flush-only Running frame (the bug scenario) ---
    let flush_header = DataHeaderProto {
        offset: 0,
        flush: true,
        is_last: false,
    };
    let flush_msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Running)
        .req_id(req_id)
        .seq_id(2)
        .proto_header(flush_header)
        .build();
    // This must succeed; the fix calls file.flush()? here.
    let _: Message = client.rpc_check(flush_msg)?;

    // --- Verify data persists without a Complete frame ---
    // Read the block back via RPC; if flush() was called the data is on disk.
    let read_req = BlockReadRequest {
        id: req_id,
        off: 0,
        len: block_size,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        ..Default::default()
    };
    let read_req_msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(0)
        .request(RequestStatus::Open)
        .proto_header(read_req)
        .build();
    let _: BlockReadResponse = client.rpc_check(read_req_msg)?.parse_header()?;

    let read_data_msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(1)
        .request(RequestStatus::Running)
        .build();
    let response = client.rpc_check(read_data_msg)?;
    let data = response.data.as_slice();

    // The data written in step 3 must be present even without a Complete frame.
    // If the fix is absent, this assertion will fail because flush() was never
    // called for the flush-only frame, and the in-buffer data may not be
    // persisted to the on-disk store.
    assert!(
        !data.is_empty(),
        "flush-only Running frame did not persist data; data likely lost because file.flush() was not called"
    );
    assert_eq!(
        data.get(..write_data.len()),
        Some(write_data.as_slice()),
        "flushed data content mismatch"
    );
    std::fs::remove_dir_all(&tmp_dir).expect("failed to clean up worker test temp directory");
    Ok(())
}

fn start_worker_with_tempdir() -> (ClusterConf, std::path::PathBuf) {
    let tmp_dir = std::env::temp_dir().join(format!(
        "curvine-test-worker-{}-{}",
        std::process::id(),
        Utils::req_id().abs()
    ));
    std::fs::create_dir_all(&tmp_dir).expect("failed to create worker test temp directory");

    let mut conf = ClusterConf::default();
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.web_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec![tmp_dir.to_string_lossy().to_string()];
    conf.client.init().unwrap();

    let server = Worker::with_conf(conf.clone()).unwrap();
    thread::spawn(move || server.start_standalone());
    (conf, tmp_dir)
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
                .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?,
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
                    return Err(curvine_core_error::CommonError::from(format!(
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
            .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?;
        let rule = FaultRuleBuilder::named("worker.rpc.before_dispatch")
            .matches("req_id", open_req_id)
            .and_then(|builder| builder.matches("rpc_code", RpcCode::WriteBlock as i32))
            .and_then(|builder| builder.matches("request_status", i8::from(RequestStatus::Open)))
            .and_then(|builder| builder.times(1))
            .and_then(|builder| builder.return_error("worker HTTP control-plane failure"))
            .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?;
        session
            .configure("worker", "http-fail-write-open", rule)
            .await
            .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?;

        Ok::<FaultTestSession, curvine_core_error::CommonError>(session)
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
        component_info: None,
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
            .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?;
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
            .map_err(|e| curvine_core_error::CommonError::from(e.to_string()))?;
        Ok::<(), curvine_core_error::CommonError>(())
    })?;

    let healthy_block = Utils::req_id().abs();
    let write_ck = block_write(healthy_block, &conf)?;
    let read_ck = block_read(healthy_block, &conf)?;
    assert_eq!(write_ck, read_ck);
    Ok(())
}

fn old_client_component_info() -> ComponentInfoProto {
    ComponentInfoProto {
        component: Some("client".to_string()),
        release_version: Some("0.1.0".to_string()),
        git_commit: Some("abc".to_string()),
        git_tag: Some(String::new()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: vec![],
    }
}

fn start_worker_with_compatibility(mode: &str, min_client_version: &str) -> ClusterConf {
    let mut conf = ClusterConf::default();
    conf.worker.rpc_port = NetUtils::hold_available_port();
    conf.worker.web_port = NetUtils::hold_available_port();
    conf.worker.data_dir = vec![format!(
        "[MEM:10MB]../testing/worker-compat-{}-{}",
        mode,
        Utils::req_id().abs()
    )];
    conf.worker.compatibility.mode = mode.to_string();
    conf.worker.compatibility.min_client_version = min_client_version.to_string();
    conf.client.init().unwrap();

    let server = Worker::with_conf(conf.clone()).unwrap();
    thread::spawn(move || server.start_standalone());
    conf
}

#[test]
fn test_worker_enforce_mode_rejects_old_client_on_first_open_frame() -> CommonResult<()> {
    // 旧 client + 协议不匹配: a worker explicitly configured to enforce
    // min_client_version rejects an older client on its first data-plane open
    // frame with an explicit error carrying both versions.
    let conf = start_worker_with_compatibility("enforce", "99.0.0");
    let client = conf.worker_sync_client()?;

    let block_id = Utils::req_id().abs();
    let block = ExtendedBlock::new(
        block_id,
        CHUNK_SIZE as i64,
        StorageType::Disk,
        FileType::File,
    );
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size: CHUNK_SIZE as i64,
        short_circuit: false,
        client_name: "old-client".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
        component_info: Some(old_client_component_info()),
    };
    let open = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(Utils::req_id())
        .seq_id(0)
        .proto_header(request)
        .build();

    let err = client.rpc_check(open).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("0.1.0"),
        "error should name the peer version: {msg}"
    );
    assert!(
        msg.contains("99.0.0"),
        "error should name the minimum: {msg}"
    );
    assert!(
        msg.contains("diagnose"),
        "error should suggest diagnose mode: {msg}"
    );
    Ok(())
}

#[test]
fn test_worker_diagnose_mode_allows_old_client_on_data_plane() -> CommonResult<()> {
    // Acceptance: 默认不拒绝. Even with an explicit (low) min_client_version,
    // a diagnose-mode worker warns but allows the old client's open frame.
    let conf = start_worker_with_compatibility("diagnose", "99.0.0");
    let client = conf.worker_sync_client()?;

    let block_id = Utils::req_id().abs();
    let block = ExtendedBlock::new(
        block_id,
        CHUNK_SIZE as i64,
        StorageType::Disk,
        FileType::File,
    );
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size: CHUNK_SIZE as i64,
        short_circuit: false,
        client_name: "old-client".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: Vec::new(),
        component_info: Some(old_client_component_info()),
    };
    let req_id = Utils::req_id();
    let open = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(0)
        .proto_header(request)
        .build();
    // The open frame must succeed: diagnose warns but never rejects.
    let _: BlockWriteResponse = client.rpc_check(open)?.parse_header()?;
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
        component_info: None,
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
        component_info: None,
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
        component_info: None,
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
        enable_read_ahead: conf.client.enable_read_ahead,
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
