// Copyright 2026 OPPO.
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

use super::*;
use crate::worker::block::{
    staged_checkpoint_totals_for_benchmark, staged_fast_complete_totals_for_benchmark,
};
use crate::worker::storage::FileLayout;
use bytes::Bytes;
use curvine_core_error::err_box;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, ProtoUtils, StorageType};
use curvine_net::net::InetAddr;
use curvine_proto::{BlockWriteRequest, BlockWriteResponse, DataHeaderProto};
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use serde_json::{json, Value};
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

#[cfg(not(feature = "fault-injection"))]
#[path = "read_benchmark.rs"]
mod read_benchmark;

const TEST_NAME: &str = "worker::worker_server::write_benchmark::worker_write_ablation";
const CHILD_ENV: &str = "CURVINE_WRITE_BENCH_CHILD_VARIANT";
const ROOT_ENV: &str = "CURVINE_WRITE_BENCH_DIR";
const BLOCKS_ENV: &str = "CURVINE_WRITE_BENCH_BLOCKS";
const REPS_ENV: &str = "CURVINE_WRITE_BENCH_REPS";
const VERIFY_ENV: &str = "CURVINE_WRITE_BENCH_VERIFY";
const FLUSH_ENV: &str = "CURVINE_WRITE_BENCH_EXPLICIT_FLUSH";
const HOST: &str = "127.0.0.1";
const BLOCK_BYTES: usize = 1 << 20;
const CONCURRENCIES: [usize; 2] = [1, 16];
const WORKER_IO_THREADS: usize = 4;
const WORKER_BLOCKING_THREADS: usize = 16;
const CLIENT_IO_THREADS: usize = 4;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(60);
const RPC_TIMEOUT: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const READY_FILE: &str = "rpc-port";
const STATS_FILE: &str = "worker.json";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Variant {
    LocalFile,
    StagedPwrite,
    IoUring,
}

impl Variant {
    const ALL: [Self; 3] = [Self::LocalFile, Self::StagedPwrite, Self::IoUring];

    fn name(self) -> &'static str {
        match self {
            Self::LocalFile => "local_file",
            Self::StagedPwrite => "staged_pwrite",
            Self::IoUring => "io_uring",
        }
    }
}

#[derive(Clone, Copy)]
struct Workload {
    blocks: usize,
    verify_bytes: bool,
    explicit_flush: bool,
}

#[test]
#[ignore = "worker write ablation; requires ext4 and working io_uring on Linux"]
fn worker_write_ablation() -> CommonResult<()> {
    let blocks = env_count(BLOCKS_ENV, 512)?;
    if blocks < CONCURRENCIES[1] || blocks as u64 > i64::MAX as u64 / BLOCK_BYTES as u64 {
        return err_box!(
            "{} must be at least 16 and fit in an i64 byte count",
            BLOCKS_ENV
        );
    }
    let workload = Workload {
        blocks,
        verify_bytes: env_flag(VERIFY_ENV, true)?,
        explicit_flush: env_flag(FLUSH_ENV, false)?,
    };
    match env::var(CHILD_ENV) {
        Ok(name) => {
            let variant = Variant::ALL
                .into_iter()
                .find(|variant| variant.name() == name)
                .ok_or_else(|| format!("unknown worker variant {name:?}"))?;
            return serve_trial(variant, workload);
        }
        Err(env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }

    let repetitions = env_count(REPS_ENV, 3)?;
    let root = bench_root()?;
    let tick_rate = clock_tick_rate()?;
    // Libtest may have started printing the test name without a newline.
    println!();
    for rep in 0..repetitions {
        for clients in CONCURRENCIES {
            for order in 0..Variant::ALL.len() {
                let variant = Variant::ALL[(rep % Variant::ALL.len() + order) % Variant::ALL.len()];
                let row = run_trial(
                    &root,
                    variant,
                    rep + 1,
                    order + 1,
                    clients,
                    workload,
                    tick_rate,
                )
                .map_err(|e| {
                    format!(
                        "{} repetition {} with {clients} clients: {e}",
                        variant.name(),
                        rep + 1
                    )
                })?;
                println!("WORKER_WRITE_BENCH {row}");
            }
        }
    }
    Ok(())
}

fn env_count(name: &str, default: usize) -> CommonResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|&n| n > 0)
            .ok_or_else(|| format!("{name} must be a positive integer, got {value:?}").into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_flag(name: &str, default: bool) -> CommonResult<bool> {
    match env::var(name).as_deref() {
        Ok("1") => Ok(true),
        Ok("0") => Ok(false),
        Err(env::VarError::NotPresent) => Ok(default),
        _ => err_box!("{} must be 0 or 1", name),
    }
}

fn bench_root() -> CommonResult<PathBuf> {
    let root = env::var_os(ROOT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../target"));
    let root = root
        .canonicalize()
        .map_err(|e| format!("benchmark directory {} ({ROOT_ENV}): {e}", root.display()))?;
    // The magic number alone cannot distinguish ext4 from ext2/ext3. The
    // directory fd's mount ID also handles bind mounts and escaped paths.
    let dir = fs::File::open(&root)?;
    let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", dir.as_raw_fd()))?;
    let mount_id = fdinfo
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or("missing benchmark directory mount ID")?;
    let mounts = fs::read("/proc/self/mountinfo")?;
    let mounts = String::from_utf8_lossy(&mounts);
    let fs_type = mounts
        .lines()
        .find(|line| line.split_ascii_whitespace().next() == Some(mount_id))
        .and_then(|line| line.split_once(" - "))
        .and_then(|(_, fields)| fields.split_ascii_whitespace().next())
        .ok_or("missing benchmark directory filesystem type")?;
    if fs_type != "ext4" {
        return err_box!(
            "{} is on {fs_type}; set {ROOT_ENV} to an existing writable ext4 directory",
            root.display()
        );
    }
    Ok(root)
}

struct TrialDir(PathBuf);

impl TrialDir {
    fn new(root: &Path) -> CommonResult<Self> {
        let path = root.join(format!("worker-write-ablation-{}", Utils::uuid()));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TrialDir {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_dir_all(&self.0) {
            eprintln!(
                "could not remove benchmark directory {}: {e}",
                self.0.display()
            );
        }
    }
}

struct TrialWorker(Child);

impl TrialWorker {
    fn start(dir: &Path, variant: Variant) -> CommonResult<Self> {
        let child = Command::new(env::current_exe()?)
            .args(["--ignored", "--exact", TEST_NAME, "--nocapture"])
            .current_dir(dir)
            .env(CHILD_ENV, variant.name())
            .env_remove(RpcServer::<WorkerService>::ORPC_BIND_HOSTNAME)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(Self(child))
    }

    fn wait_for_port(&mut self, dir: &Path) -> CommonResult<u16> {
        let ready = dir.join(READY_FILE);
        let start = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait()? {
                return err_box!("worker exited before readiness: {}", status);
            }
            match fs::read_to_string(&ready) {
                Ok(port) => return Ok(port.parse()?),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            if start.elapsed() >= CONTROL_TIMEOUT {
                return err_box!("timed out waiting for worker RPC readiness");
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn finish(&mut self, dir: &Path) -> CommonResult<Value> {
        self.0
            .stdin
            .take()
            .ok_or("missing worker stdin")?
            .write_all(b"\x01")?;
        let start = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait()? {
                if !status.success() {
                    return err_box!("worker verification failed: {}", status);
                }
                return Ok(serde_json::from_slice(&fs::read(dir.join(STATS_FILE))?)?);
            }
            if start.elapsed() >= CONTROL_TIMEOUT {
                return err_box!("timed out waiting for worker counters and verification");
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for TrialWorker {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn run_trial(
    root: &Path,
    variant: Variant,
    repetition: usize,
    order: usize,
    clients: usize,
    workload: Workload,
    tick_rate: u64,
) -> CommonResult<Value> {
    let dir = TrialDir::new(root)?;
    let mut worker = TrialWorker::start(&dir.0, variant)?;
    let port = worker.wait_for_port(&dir.0)?;
    let pid = worker.0.id();
    let run = run_clients(pid, port, clients, workload)?;
    let worker_stats = worker.finish(&dir.0)?;
    let completed = run.complete_rpc_ns.len();
    Ok(json!({
        "variant": variant.name(),
        "repetition": repetition,
        "order": order,
        "filesystem": "ext4",
        "explicit_flush": workload.explicit_flush,
        "clients": clients,
        "client_io_threads": CLIENT_IO_THREADS,
        "worker_io_threads": WORKER_IO_THREADS,
        "worker_blocking_threads": WORKER_BLOCKING_THREADS,
        "blocks_completed": completed,
        "block_bytes": BLOCK_BYTES,
        "bytes_completed": completed as u64 * BLOCK_BYTES as u64,
        "wall_ns": run.elapsed.as_nanos() as u64,
        "mib_per_sec": completed as f64 / run.elapsed.as_secs_f64(),
        "complete_rpc_ns": run.complete_rpc_ns,
        "worker_pid": pid,
        "clk_tck": tick_rate,
        "worker_user_ticks": run.user_ticks,
        "worker_system_ticks": run.system_ticks,
        "worker_cpu_seconds": (run.user_ticks as f64 + run.system_ticks as f64) / tick_rate as f64,
        "worker": worker_stats,
    }))
}

fn serve_trial(variant: Variant, workload: Workload) -> CommonResult<()> {
    let data = env::current_dir()?.join("data");
    // Never format a pre-existing directory, even if launched with the child marker by hand.
    fs::create_dir(&data)?;
    // RpcServer's held-port helper binds a wildcard address. Reserve a loopback
    // port instead, releasing it immediately before RpcServer binds it.
    let reservation = TcpListener::bind((HOST, 0))?;
    let port = reservation.local_addr()?.port();
    let mut conf = ClusterConf {
        format_master: false,
        format_worker: true,
        ..Default::default()
    };
    // WorkerService constructs internal master clients even for this
    // standalone workload. Never probe any real local cluster.
    conf.master.hostname = HOST.to_owned();
    conf.master.rpc_port = 0;
    conf.client.master_addrs = vec![InetAddr::new(HOST, 0)];
    conf.worker.hostname = HOST.to_owned();
    conf.worker.rpc_port = port;
    conf.worker.io_threads = WORKER_IO_THREADS;
    conf.worker.worker_threads = WORKER_BLOCKING_THREADS;
    conf.worker.data_dir = vec![data
        .to_str()
        .ok_or("worker data path must be UTF-8")?
        .to_owned()];
    conf.worker.enable_io_uring = variant == Variant::IoUring;
    conf.worker.enable_splice = false;
    conf.worker.enable_send_file = false;
    conf.worker.block_checksum = false;
    conf.worker.block_checksum_verify = false;
    conf.worker.block_checksum_scan_on_start = false;
    conf.worker.io_slow_threshold = "5m".to_owned();
    conf.worker.log.level = "WARN".to_owned();
    conf.worker.log.log_dir = Logger::TARGET_STDERR.to_owned();
    conf.client.short_circuit = false;
    conf.client.heartbeat_interval_ms = 0;
    conf.client.init()?;

    let _logger = Logger::new(conf.worker.log.clone());
    log::set_max_level(log::LevelFilter::Warn);
    let server_conf = conf.worker_server_conf();
    let rt = Arc::new(server_conf.create_runtime());
    let mut service = WorkerService::with_conf(&conf, rt.clone(), Utils::uuid())?;
    if variant == Variant::StagedPwrite {
        service.store.use_staged_pwrite_for_benchmark();
    }
    let store = service.store.clone();
    CLUSTER_CONF.get_or_init(|| conf);
    let metrics = WORKER_METRICS.get_or_try_init(|| WorkerMetrics::new(store.clone()))?;
    if metrics.io_uring_write_bytes.get() != 0
        || staged_checkpoint_totals_for_benchmark() != (0, 0)
        || staged_fast_complete_totals_for_benchmark() != (0, 0)
    {
        return err_box!("worker write counters were not fresh at startup");
    }
    let server = RpcServer::with_rt(rt.clone(), server_conf, service);
    drop(reservation);
    let mut state = server.start();
    rt.block_on(async { tokio::time::timeout(CONTROL_TIMEOUT, state.wait_startup()).await })??;
    fs::write("rpc-port.tmp", port.to_string())?;
    fs::rename("rpc-port.tmp", READY_FILE)?;

    // Verify only after the parent's wall timer and final CPU sample.
    // EOF without the byte means the parent abandoned the trial.
    let mut done = [0];
    io::stdin().lock().read_exact(&mut done)?;
    if done != [1] {
        return err_box!("invalid benchmark completion signal");
    }
    let stats = worker_stats(&store, metrics, variant, workload)?;
    fs::write(STATS_FILE, serde_json::to_vec(&stats)?)?;
    Ok(())
}

fn worker_stats(
    store: &BlockStore,
    metrics: &WorkerMetrics,
    variant: Variant,
    workload: Workload,
) -> CommonResult<Value> {
    let blocks = workload.blocks;
    // No background writer or warm-up shares this fresh process and directory.
    let dir_id = store.get_block(1)?.dir_id();
    let label = dir_id.to_string();
    let labels = &[label.as_str()];
    let writes = metrics.write_count.with_label_values(labels).get();
    let bytes = metrics.write_bytes.with_label_values(labels).get();
    let write_time_us = metrics.write_time_us.with_label_values(labels).get();
    let ring_bytes = metrics.io_uring_write_bytes.get();
    let (checkpoint_count, checkpoint_ns) = staged_checkpoint_totals_for_benchmark();
    let (fast_complete_count, fast_complete_ns) = staged_fast_complete_totals_for_benchmark();
    let expected_bytes = blocks as i64 * BLOCK_BYTES as i64;
    let expected_ring = if variant == Variant::IoUring {
        expected_bytes
    } else {
        0
    };
    let (expected_checkpoints, expected_fast_completes) = match (variant, workload.explicit_flush) {
        (Variant::LocalFile, _) => (0, 0),
        (_, true) => (blocks as u64, 0),
        (_, false) => (0, blocks as u64),
    };
    if writes != blocks as i64 || bytes != expected_bytes {
        return err_box!(format!(
            "worker wrote {writes} chunks / {bytes} bytes; expected {blocks} / {expected_bytes}"
        ));
    }
    if ring_bytes != expected_ring {
        return err_box!(format!(
            "completed ring bytes {ring_bytes}, expected {expected_ring}; check for backend fallback"
        ));
    }
    if checkpoint_count != expected_checkpoints || (expected_checkpoints == 0 && checkpoint_ns != 0)
    {
        return err_box!(format!(
            "staged checkpoints {checkpoint_count} ({checkpoint_ns} ns), expected {expected_checkpoints}"
        ));
    }

    if fast_complete_count != expected_fast_completes
        || (expected_fast_completes == 0 && fast_complete_ns != 0)
    {
        return err_box!(format!(
            "staged fast completes {fast_complete_count} ({fast_complete_ns} ns), expected {expected_fast_completes}"
        ));
    }

    let expected_payload = workload.verify_bytes.then(payload);
    for id in 1..=blocks {
        let meta = store.get_block(id as i64)?;
        if !meta.is_final() || meta.len() != BLOCK_BYTES as i64 {
            return err_box!("block {} is not finalized at the expected length", id);
        }
        if let Some(expected) = &expected_payload {
            let (_, dir) = store.read()?.layout_for(&meta)?;
            let actual = fs::read(FileLayout::block_path(&dir, &meta)?)?;
            if actual.as_slice() != expected.as_ref() {
                return err_box!("finalized block {} does not match the payload", id);
            }
        }
    }
    Ok(json!({
        "dir_id": dir_id,
        "write_count": writes,
        "write_bytes": bytes,
        "write_time_us": write_time_us,
        "io_uring_write_bytes": ring_bytes,
        "staged_checkpoint_count": checkpoint_count,
        "staged_checkpoint_ns": checkpoint_ns,
        "staged_fast_complete_count": fast_complete_count,
        "staged_fast_complete_ns": fast_complete_ns,
        "finalized_block_count": blocks,
        "verified_block_count": if workload.verify_bytes { blocks } else { 0 },
    }))
}

struct ClientRun {
    elapsed: Duration,
    user_ticks: u64,
    system_ticks: u64,
    complete_rpc_ns: Vec<u64>,
}

fn run_clients(pid: u32, port: u16, clients: usize, workload: Workload) -> CommonResult<ClientRun> {
    let blocks = workload.blocks;
    let explicit_flush = workload.explicit_flush;
    let rt = AsyncRuntime::new("write-bench-client", CLIENT_IO_THREADS, 1);
    rt.block_on(async {
        let addr = InetAddr::new(HOST, port);
        let conf = ClientConf::default();
        let mut connections = Vec::with_capacity(clients);
        for _ in 0..clients {
            connections.push(Arc::new(RpcClient::with_raw(&addr, &conf).await?));
        }
        let data = payload();
        let gate = Arc::new(Barrier::new(clients + 1));
        let mut tasks = JoinSet::new();
        // Keep connections alive through the second CPU sample. Each raw
        // connection is used serially by only its own task.
        for (lane, connection) in connections.iter().enumerate() {
            let client = connection.clone();
            let data = data.clone();
            let gate = gate.clone();
            let mut complete_ns = Vec::with_capacity(blocks.div_ceil(clients));
            tasks.spawn(async move {
                gate.wait().await;
                for id in ((lane + 1)..=blocks).step_by(clients) {
                    complete_ns.push(write_block(&client, id as i64, &data, explicit_flush).await?);
                }
                Ok::<_, CommonError>(complete_ns)
            });
        }
        let mut complete_rpc_ns = Vec::with_capacity(blocks);
        let before = cpu_ticks(pid)?;
        let start = Instant::now();
        gate.wait().await;
        while let Some(result) = tasks.join_next().await {
            complete_rpc_ns.extend(result??);
        }
        let elapsed = start.elapsed();
        let after = cpu_ticks(pid)?;
        if complete_rpc_ns.len() != blocks {
            return err_box!(
                "client completed {} of {blocks} blocks",
                complete_rpc_ns.len()
            );
        }
        Ok(ClientRun {
            elapsed,
            user_ticks: after
                .0
                .checked_sub(before.0)
                .ok_or("worker user ticks decreased")?,
            system_ticks: after
                .1
                .checked_sub(before.1)
                .ok_or("worker system ticks decreased")?,
            complete_rpc_ns,
        })
    })
}

fn payload() -> Bytes {
    // A deterministic nonzero pattern; clones do not copy 1 MiB on every RPC.
    (0..BLOCK_BYTES)
        .map(|i| (i ^ (i >> 8) ^ (i >> 16)) as u8)
        .collect::<Vec<_>>()
        .into()
}

async fn write_block(
    client: &RpcClient,
    id: i64,
    data: &Bytes,
    explicit_flush: bool,
) -> CommonResult<u64> {
    let block = ExtendedBlock::new(id, BLOCK_BYTES as i64, StorageType::Disk, FileType::File);
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        block_size: BLOCK_BYTES as i64,
        chunk_size: BLOCK_BYTES as i32,
        short_circuit: false,
        ..Default::default()
    };
    let frame = |seq, status| {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(id)
            .seq_id(seq)
    };
    let open = frame(0, RequestStatus::Open)
        .proto_header_ref(&request)
        .build();
    let opened: BlockWriteResponse = check_rpc(client, open).await?.parse_header()?;
    if opened.id != id || opened.path.is_some() {
        return err_box!(
            "unexpected remote Open response for block {}: {:?}",
            id,
            opened
        );
    }
    let running = frame(1, RequestStatus::Running)
        .proto_header(DataHeaderProto {
            offset: 0,
            flush: false,
            is_last: false,
        })
        .data(DataSlice::bytes(data.clone()))
        .build();
    check_rpc(client, running).await?;
    let complete_seq = if explicit_flush {
        // Match BlockClient::write_flush with a separate, payload-free RPC.
        let flush = frame(2, RequestStatus::Running)
            .proto_header(DataHeaderProto {
                offset: BLOCK_BYTES as i64,
                flush: true,
                is_last: false,
            })
            .build();
        check_rpc(client, flush).await?;
        3
    } else {
        2
    };
    let complete = frame(complete_seq, RequestStatus::Complete)
        .proto_header(request)
        .build();
    let start = Instant::now();
    check_rpc(client, complete).await?;
    Ok(start.elapsed().as_nanos() as u64)
}

async fn check_rpc(client: &RpcClient, message: Message) -> CommonResult<Message> {
    let id = message.req_id();
    let status = message.request_status();
    let response = client
        .timeout_rpc(RPC_TIMEOUT, message)
        .await
        .map_err(|e| format!("block {id} {status:?}: {e}"))?;
    if !response.is_success() {
        return err_box!("block {id} {status:?}: {}", response.to_error_msg());
    }
    Ok(response)
}

fn clock_tick_rate() -> CommonResult<u64> {
    let output = Command::new("getconf").arg("CLK_TCK").output()?;
    if !output.status.success() {
        return err_box!(
            "getconf CLK_TCK failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let hz = String::from_utf8(output.stdout)?.trim().parse::<u64>()?;
    if hz == 0 {
        return err_box!("getconf CLK_TCK returned zero");
    }
    Ok(hz)
}

fn cpu_ticks(pid: u32) -> CommonResult<(u64, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    // comm can contain spaces and parentheses. After its final ')' the
    // fields start at 3 (state); utime and stime are fields 14 and 15.
    let (_, fields) = stat.rsplit_once(')').ok_or("invalid /proc stat comm")?;
    let mut fields = fields.split_ascii_whitespace();
    let user = fields.nth(11).ok_or("missing /proc stat utime")?.parse()?;
    let system = fields.next().ok_or("missing /proc stat stime")?.parse()?;
    Ok((user, system))
}
