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
use curvine_error::FsResult;
use curvine_proto::{BlockReadRequest, BlockReadResponse};
use curvine_rpc::handler::MessageHandler;
use std::sync::atomic::{AtomicU64, Ordering};

const READ_TEST_NAME: &str =
    "worker::worker_server::write_benchmark::read_benchmark::worker_read_ablation";
const PREOPENED_TEST_NAME: &str =
    "worker::worker_server::write_benchmark::read_benchmark::worker_read_preopened";
const PREOPENED_BYTES: usize = 64 << 20;
const PREOPENED_CLIENTS: [usize; 3] = [1, 16, 64];
const READ_CHILD_ENV: &str = "CURVINE_READ_BENCH_CHILD";
const READ_BLOCKS_ENV: &str = "CURVINE_READ_BENCH_BLOCKS";
const READ_REPS_ENV: &str = "CURVINE_READ_BENCH_REPS";
const READ_COLD_ENV: &str = "CURVINE_READ_BENCH_COLD";
const WARM_CHUNK_BYTES: usize = 128 * 1024;
const RANDOM_CHUNK_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadArm {
    LocalFile,
    BlockingRing,
    AsyncRing,
}

impl ReadArm {
    const ALL: [Self; 3] = [Self::LocalFile, Self::BlockingRing, Self::AsyncRing];

    fn name(self) -> &'static str {
        match self {
            Self::LocalFile => "local_file",
            Self::BlockingRing => "blocking_ring",
            Self::AsyncRing => "async_ring",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadPattern {
    WarmSequential,
    ColdAdvisedRandom,
    PreopenedSequential,
}

impl ReadPattern {
    const ALL: [Self; 2] = [Self::WarmSequential, Self::ColdAdvisedRandom];

    fn name(self) -> &'static str {
        match self {
            Self::WarmSequential => "warm_sequential",
            Self::ColdAdvisedRandom => "cold_advised_random",
            Self::PreopenedSequential => "preopened_cold_advised_sequential",
        }
    }

    fn chunk_bytes(self) -> usize {
        match self {
            Self::WarmSequential => WARM_CHUNK_BYTES,
            Self::ColdAdvisedRandom | Self::PreopenedSequential => RANDOM_CHUNK_BYTES,
        }
    }

    fn block_bytes(self, blocks: usize) -> usize {
        if self == Self::PreopenedSequential {
            PREOPENED_BYTES / blocks
        } else {
            BLOCK_BYTES
        }
    }

    fn chunks_per_block(self, block_bytes: usize) -> usize {
        match self {
            Self::WarmSequential | Self::PreopenedSequential => block_bytes / self.chunk_bytes(),
            Self::ColdAdvisedRandom => 1,
        }
    }

    fn plan(self, blocks: usize, mut seed: u64) -> Vec<ReadJob> {
        let mut jobs: Vec<_> = (1..=blocks as i64)
            .map(|id| ReadJob { id, offset: 0 })
            .collect();
        if self == Self::ColdAdvisedRandom {
            // Visit every file once after cache advice, with the same shuffled
            // block order and page-aligned offsets in each arm of the repetition.
            for i in (1..jobs.len()).rev() {
                let j = (next_random(&mut seed) % (i as u64 + 1)) as usize;
                jobs.swap(i, j);
            }
            for job in &mut jobs {
                let page = next_random(&mut seed) % (BLOCK_BYTES / RANDOM_CHUNK_BYTES) as u64;
                job.offset = page as usize * RANDOM_CHUNK_BYTES;
            }
        }
        jobs
    }
}

fn next_random(seed: &mut u64) -> u64 {
    // SplitMix64; keep the workloads reproducible without a rand dependency.
    *seed = seed.wrapping_add(0x9e3779b97f4a7c15);
    let z = (*seed ^ (*seed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[derive(Clone, Copy)]
struct ReadJob {
    id: i64,
    offset: usize,
}

#[test]
#[ignore = "worker read ablation; requires ext4 and working io_uring on Linux"]
fn worker_read_ablation() -> CommonResult<()> {
    let blocks = env_count(READ_BLOCKS_ENV, 256)?;
    if blocks < CONCURRENCIES[1] || blocks as u64 > i64::MAX as u64 / BLOCK_BYTES as u64 {
        return err_box!(
            "{} must be at least {} and fit in an i64 byte count",
            READ_BLOCKS_ENV,
            CONCURRENCIES[1]
        );
    }
    let cold = env_flag(READ_COLD_ENV, true)?;
    let patterns = if cold {
        &ReadPattern::ALL[..]
    } else {
        &ReadPattern::ALL[..1]
    };
    read_matrix(READ_TEST_NAME, patterns, &CONCURRENCIES, Some(blocks))
}

// Keep the number of samples and the total working set fixed. After one
// sync + DONTNEED per trial file, each lane walks its own block from offset
// zero, one new 4 KiB page per headerless RPC. Kernel readahead is possible.
#[test]
#[ignore = "pre-opened worker read ablation; requires ext4, python3 and working Linux io_uring"]
fn worker_read_preopened() -> CommonResult<()> {
    read_matrix(
        PREOPENED_TEST_NAME,
        &[ReadPattern::PreopenedSequential],
        &PREOPENED_CLIENTS,
        None, // One distinct block and TCP connection per client.
    )
}

fn read_matrix(
    test_name: &str,
    patterns: &[ReadPattern],
    concurrencies: &[usize],
    fixed_blocks: Option<usize>,
) -> CommonResult<()> {
    match env::var(READ_CHILD_ENV) {
        Ok(profile) => {
            let (arm, pattern) = profile
                .split_once('/')
                .ok_or("invalid read worker profile")?;
            let arm = ReadArm::ALL
                .into_iter()
                .find(|value| value.name() == arm)
                .ok_or_else(|| format!("unknown read arm {arm:?}"))?;
            let pattern = patterns
                .iter()
                .copied()
                .find(|value| value.name() == pattern)
                .ok_or_else(|| format!("unknown read pattern {pattern:?}"))?;
            let blocks = env_count(READ_BLOCKS_ENV, 256)?;
            if fixed_blocks.is_none() && !concurrencies.contains(&blocks) {
                return err_box!("unexpected pre-opened block count: {}", blocks);
            }
            return serve_read_trial(arm, pattern, blocks);
        }
        Err(env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }

    let root = bench_root()?;
    let repetitions = env_count(READ_REPS_ENV, 3)?;
    let tick_rate = clock_tick_rate()?;
    println!();
    for repetition in 1..=repetitions {
        for &pattern in patterns {
            for &clients in concurrencies {
                let jobs = pattern.plan(fixed_blocks.unwrap_or(clients), repetition as u64);
                let arms = ReadArm::ALL;
                let rotation = (repetition - 1) % arms.len();
                for order in 0..arms.len() {
                    let arm = arms[(rotation + order) % arms.len()];
                    let trial = ReadTrial {
                        arm,
                        pattern,
                        clients,
                        repetition,
                        order: order + 1,
                    };
                    let row = trial.run(test_name, &root, &jobs, tick_rate).map_err(|e| {
                        format!(
                            "{} {} repetition {repetition} with {clients} clients: {e}",
                            arm.name(),
                            pattern.name()
                        )
                    })?;
                    println!("WORKER_READ_BENCH {row}");
                }
            }
        }
    }
    Ok(())
}

struct ReadTrial {
    arm: ReadArm,
    pattern: ReadPattern,
    clients: usize,
    repetition: usize,
    order: usize,
}

impl ReadTrial {
    fn run(
        &self,
        test_name: &str,
        root: &Path,
        jobs: &[ReadJob],
        tick_rate: u64,
    ) -> CommonResult<Value> {
        let dir = TrialDir::new(root)?;
        let profile = format!("{}/{}", self.arm.name(), self.pattern.name());
        let mut worker = TrialWorker(
            Command::new(env::current_exe()?)
                .args(["--ignored", "--exact", test_name, "--nocapture"])
                .current_dir(&dir.0)
                .env(READ_CHILD_ENV, profile)
                .env(READ_BLOCKS_ENV, jobs.len().to_string())
                .env_remove(RpcServer::<ReadService>::ORPC_BIND_HOSTNAME)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let port = worker.wait_for_port(&dir.0)?;
        let pid = worker.0.id();
        let mut run = run_read_clients(pid, port, self.clients, self.pattern, jobs)?;
        let stats = worker.finish(&dir.0)?;
        run.running_ns.sort_unstable();
        let count = run.running_ns.len();
        let bytes = count as u64 * self.pattern.chunk_bytes() as u64;
        let total_ns: u64 = run.running_ns.iter().sum();
        let percentile = |p: u64| run.running_ns[((count as u64 * p).div_ceil(100) - 1) as usize];
        let seconds = run.elapsed.as_secs_f64();
        Ok(json!({
            "variant": self.arm.name(),
            "workload": self.pattern.name(),
            "repetition": self.repetition,
            "order": self.order,
            "filesystem": "ext4",
            "clients": self.clients,
            "client_io_threads": CLIENT_IO_THREADS,
            "worker_io_threads": WORKER_IO_THREADS,
            "worker_blocking_threads": WORKER_BLOCKING_THREADS,
            "blocks_seeded": jobs.len(),
            "block_bytes": self.pattern.block_bytes(jobs.len()),
            "chunk_bytes": self.pattern.chunk_bytes(),
            "running_rpc_count": count,
            "running_rpc_total_ns": total_ns,
            "running_rpc_p95_ns": percentile(95),
            "running_rpc_p99_ns": percentile(99),
            "bytes_verified": bytes,
            "wall_ns": run.elapsed.as_nanos() as u64,
            "mib_per_sec": bytes as f64 / BLOCK_BYTES as f64 / seconds,
            "running_rpcs_per_sec": count as f64 / seconds,
            "worker_pid": pid,
            "clk_tck": tick_rate,
            "worker_user_ticks": run.user_ticks,
            "worker_system_ticks": run.system_ticks,
            "worker_cpu_seconds": (run.user_ticks as f64 + run.system_ticks as f64) / tick_rate as f64,
            "worker_proc_read_bytes": run.proc_read_bytes,
            "worker": stats,
        }))
    }
}

#[derive(Default)]
struct ReadDispatchCounts {
    blocking: AtomicU64,
    asynchronous: AtomicU64,
}

struct ReadService {
    worker: WorkerService,
    counts: Arc<ReadDispatchCounts>,
}

impl HandlerService for ReadService {
    type Item = ReadBenchHandler;

    fn has_conn_state(&self) -> bool {
        self.worker.has_conn_state()
    }

    fn get_message_handler(&self, conn_info: Option<ConnState>) -> Self::Item {
        ReadBenchHandler {
            handler: self.worker.get_message_handler(conn_info),
            counts: self.counts.clone(),
        }
    }
}

struct ReadBenchHandler {
    handler: WorkerHandler,
    counts: Arc<ReadDispatchCounts>,
}

fn headerless_running(msg: &Message) -> bool {
    RpcCode::from(msg.code()) == RpcCode::ReadBlock
        && msg.request_status() == RequestStatus::Running
        && msg.header_len() == 0
        && msg.data.is_empty()
}

impl MessageHandler for ReadBenchHandler {
    type Error = <WorkerHandler as MessageHandler>::Error;

    fn is_sync(&self, msg: &Message) -> bool {
        self.handler.is_sync(msg)
    }

    fn get_rt(&self, msg: &Message) -> Option<&Runtime> {
        self.handler.get_rt(msg)
    }

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        self.record(headerless_running(msg), self.handler.handle(msg)?, false)
    }

    async fn async_handle(&self, msg: Message) -> FsResult<Message> {
        let running = headerless_running(&msg);
        self.record(running, self.handler.async_handle(msg).await?, true)
    }
}

impl ReadBenchHandler {
    fn record(&self, running: bool, reply: Message, async_dispatch: bool) -> FsResult<Message> {
        if running && reply.is_success() {
            let counter = match (async_dispatch, &reply.data) {
                (false, DataSlice::Buffer(_)) => &self.counts.blocking,
                (true, DataSlice::Bytes(_)) => &self.counts.asynchronous,
                _ => return err_box!("unexpected read response buffer (async={})", async_dispatch),
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        Ok(reply)
    }
}

fn serve_read_trial(arm: ReadArm, pattern: ReadPattern, blocks: usize) -> CommonResult<()> {
    let data = env::current_dir()?.join("data");
    // Only format a directory created by this process.
    fs::create_dir(&data)?;
    let reservation = TcpListener::bind((HOST, 0))?;
    let port = reservation.local_addr()?.port();
    let mut conf = ClusterConf {
        format_master: false,
        format_worker: true,
        ..Default::default()
    };
    conf.master.hostname = HOST.to_owned();
    conf.master.rpc_port = 0;
    conf.client.master_addrs = vec![InetAddr::new(HOST, 0)];
    conf.client.heartbeat_interval_ms = 0;
    conf.client.enable_read_ahead = false;
    conf.client.short_circuit = false;
    conf.client.init()?;
    conf.worker.hostname = HOST.to_owned();
    conf.worker.rpc_port = port;
    conf.worker.io_threads = WORKER_IO_THREADS;
    conf.worker.worker_threads = WORKER_BLOCKING_THREADS;
    conf.worker.data_dir = vec![data
        .to_str()
        .ok_or("worker data path must be UTF-8")?
        .to_owned()];
    conf.worker.enable_io_uring = arm != ReadArm::LocalFile;
    conf.worker.enable_splice = false;
    conf.worker.enable_send_file = false;
    conf.worker.block_checksum = false;
    conf.worker.block_checksum_verify = false;
    conf.worker.block_checksum_scan_on_start = false;
    conf.worker.io_slow_threshold = "5m".to_owned();
    conf.worker.log.level = "WARN".to_owned();
    conf.worker.log.log_dir = Logger::TARGET_STDERR.to_owned();

    let _logger = Logger::new(conf.worker.log.clone());
    log::set_max_level(log::LevelFilter::Warn);
    let server_conf = conf.worker_server_conf();
    let rt = Arc::new(server_conf.create_runtime());
    let mut worker = WorkerService::with_conf(&conf, rt.clone(), Utils::uuid())?;
    worker.store.blocking_ring_read_control = arm == ReadArm::BlockingRing;
    CLUSTER_CONF.get_or_init(|| conf);
    seed_blocks(&worker.store, blocks, pattern)?;
    let metrics = WORKER_METRICS.get_or_try_init(|| WorkerMetrics::new(worker.store.clone()))?;
    let counts = Arc::<ReadDispatchCounts>::default();
    let service = ReadService {
        worker,
        counts: counts.clone(),
    };
    let server = RpcServer::with_rt(rt.clone(), server_conf, service);
    drop(reservation);
    let mut state = server.start();
    rt.block_on(async { tokio::time::timeout(CONTROL_TIMEOUT, state.wait_startup()).await })??;
    fs::write("rpc-port.tmp", port.to_string())?;
    fs::rename("rpc-port.tmp", READY_FILE)?;

    // The parent samples wall time and CPU before asking us to check the counters.
    let mut done = [0];
    io::stdin().lock().read_exact(&mut done)?;
    if done != [1] {
        return err_box!("invalid read benchmark completion signal");
    }
    let stats = read_worker_stats(metrics, &counts, arm, pattern, blocks)?;
    fs::write(STATS_FILE, serde_json::to_vec(&stats)?)?;
    Ok(())
}

// A bijective word pattern: a swapped file, repeated page or wrong byte offset
// cannot pass just because all clients received a full-length response.
fn preopened_payload(id: i64, len: usize) -> Bytes {
    let mut data = vec![0; len];
    for (i, word) in data.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        let key = ((id as u64) << 32) | i as u64;
        *word = key.wrapping_mul(0x9e3779b97f4a7c15).to_le_bytes();
    }
    Bytes::from(data)
}

fn seed_blocks(store: &BlockStore, blocks: usize, pattern: ReadPattern) -> CommonResult<()> {
    let block_bytes = pattern.block_bytes(blocks);
    let common = (pattern != ReadPattern::PreopenedSequential).then(payload);
    let mut paths = Vec::with_capacity(blocks);
    for id in 1..=blocks as i64 {
        let block = ExtendedBlock::new(id, block_bytes as i64, StorageType::Disk, FileType::File);
        let meta = store.open_block(&block)?;
        // Use the same synchronous file writer even when the remote read backend uses a ring.
        let mut writer = store.open_writer(&meta, 0)?;
        let data = common
            .clone()
            .unwrap_or_else(|| preopened_payload(id, block_bytes));
        writer.write_region(&DataSlice::bytes(data))?;
        writer.flush()?;
        drop(writer);
        let meta = store.finalize_block(&block)?;
        let (_, dir) = store.read()?.layout_for(&meta)?;
        let path = FileLayout::block_path(&dir, &meta)?;
        let file = fs::OpenOptions::new().write(true).open(&path)?;
        if !meta.is_final() || file.metadata()?.len() != block_bytes as u64 {
            return err_box!(
                "seed block {} is not a finalized {}-byte file",
                id,
                block_bytes
            );
        }
        // DONTNEED may leave dirty pages resident. Flush these files, never the mount.
        file.sync_data()?;
        paths.push(path);
    }
    match pattern {
        ReadPattern::WarmSequential => {
            let mut buffer = vec![0; BLOCK_BYTES];
            for path in paths {
                fs::File::open(path)?.read_exact(&mut buffer)?;
            }
            Ok(())
        }
        ReadPattern::ColdAdvisedRandom | ReadPattern::PreopenedSequential => advise_cold(&paths),
    }
}

fn advise_cold(paths: &[PathBuf]) -> CommonResult<()> {
    // Use the explicit list of finalized trial files, not a directory walk or a
    // host-wide cache drop. A successful advisory call is not proof of eviction.
    const MANIFEST: &str = "read-bench-files.json";
    const SCRIPT: &str = r#"
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as manifest:
    paths = json.load(manifest)
for path in paths:
    with open(path, "rb", buffering=0) as block:
        os.posix_fadvise(block.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
"#;
    fs::write(MANIFEST, serde_json::to_vec(paths)?)?;
    let status = Command::new("python3")
        .args(["-c", SCRIPT, MANIFEST])
        .status()
        .map_err(|e| format!("could not run python3 for POSIX_FADV_DONTNEED: {e}"))?;
    if !status.success() {
        return err_box!("python3 POSIX_FADV_DONTNEED failed: {}", status);
    }
    Ok(())
}

fn read_worker_stats(
    metrics: &WorkerMetrics,
    counts: &ReadDispatchCounts,
    arm: ReadArm,
    pattern: ReadPattern,
    blocks: usize,
) -> CommonResult<Value> {
    let expected_count =
        blocks as i64 * pattern.chunks_per_block(pattern.block_bytes(blocks)) as i64;
    let expected_bytes = expected_count * pattern.chunk_bytes() as i64;
    let expected_ring = if arm == ReadArm::LocalFile {
        0
    } else {
        expected_bytes
    };
    let ring_bytes = metrics.io_uring_read_bytes.get();
    if ring_bytes != expected_ring {
        return err_box!(format!(
            "completed ring read bytes {ring_bytes}, expected {expected_ring}; check for backend fallback"
        ));
    }
    let read_count = metrics.read_count.get();
    let read_bytes = metrics.read_bytes.get();
    if (read_count, read_bytes) != (expected_count, expected_bytes) {
        return err_box!(format!(
            "worker read {read_count} chunks / {read_bytes} bytes; expected {expected_count} / {expected_bytes}"
        ));
    }
    let expected_async = if arm == ReadArm::AsyncRing {
        expected_count as u64
    } else {
        0
    };
    let expected_blocking = expected_count as u64 - expected_async;
    let blocking = counts.blocking.load(Ordering::Relaxed);
    let asynchronous = counts.asynchronous.load(Ordering::Relaxed);
    if (blocking, asynchronous) != (expected_blocking, expected_async) {
        return err_box!(format!(
            "headerless Running dispatches: blocking={blocking}, async={asynchronous}; expected blocking={expected_blocking}, async={expected_async}"
        ));
    }
    Ok(json!({
        "read_count": read_count,
        "read_bytes": read_bytes,
        "read_time_us": metrics.read_time_us.get(),
        "io_uring_read_bytes": ring_bytes,
        "running_blocking_dispatches": blocking,
        "running_async_dispatches": asynchronous,
    }))
}

struct ReadRun {
    elapsed: Duration,
    user_ticks: u64,
    system_ticks: u64,
    proc_read_bytes: Option<u64>,
    running_ns: Vec<u64>,
}

fn run_read_clients(
    pid: u32,
    port: u16,
    clients: usize,
    pattern: ReadPattern,
    jobs: &[ReadJob],
) -> CommonResult<ReadRun> {
    let rt = AsyncRuntime::new("read-bench-client", CLIENT_IO_THREADS, 1);
    rt.block_on(async {
        let addr = InetAddr::new(HOST, port);
        let conf = ClientConf::default();
        let mut connections = Vec::with_capacity(clients);
        for _ in 0..clients {
            connections.push(Arc::new(RpcClient::with_raw(&addr, &conf).await?));
        }
        let preopened = pattern == ReadPattern::PreopenedSequential;
        let block_bytes = pattern.block_bytes(jobs.len());
        let chunks = pattern.chunks_per_block(block_bytes);
        let expected = if preopened {
            if clients != jobs.len() {
                return err_box!("pre-opened reads need one block per connection");
            }
            let mut buffers = Vec::with_capacity(clients);
            for (client, &job) in connections.iter().zip(jobs) {
                open_read(client, job, pattern, block_bytes).await?;
                buffers.push(preopened_payload(job.id, block_bytes));
            }
            buffers
        } else {
            vec![payload(); clients]
        };
        let gate = Arc::new(Barrier::new(clients + 1));
        let mut tasks = JoinSet::new();
        // Keep connections open through the final CPU sample, with one session at a time per lane.
        for (lane, connection) in connections.iter().enumerate() {
            let client = connection.clone();
            let expected = expected[lane].clone();
            let gate = gate.clone();
            let start = lane * jobs.len() / clients;
            let end = (lane + 1) * jobs.len() / clients;
            let lane_jobs = jobs[start..end].to_vec();
            let mut running_ns = Vec::with_capacity(lane_jobs.len() * chunks);
            tasks.spawn(async move {
                gate.wait().await;
                for job in lane_jobs {
                    if !preopened {
                        open_read(&client, job, pattern, block_bytes).await?;
                    }
                    read_chunks(&client, job, pattern, &expected, &mut running_ns).await?;
                    if !preopened {
                        complete_read(&client, job.id, chunks).await?;
                    }
                }
                Ok::<_, CommonError>(running_ns)
            });
        }
        let expected_count = jobs.len() * chunks;
        let mut running_ns = Vec::with_capacity(expected_count);
        let io_before = proc_read_bytes(pid);
        let before = cpu_ticks(pid)?;
        let start = Instant::now();
        gate.wait().await;
        while let Some(result) = tasks.join_next().await {
            running_ns.extend(result??);
        }
        // Client verification is in the rate, not in the per-RPC latencies.
        // Only the original session workloads include Open/Complete in the rate.
        let elapsed = start.elapsed();
        let after = cpu_ticks(pid)?;
        let io_delta = io_before
            .zip(proc_read_bytes(pid))
            .and_then(|(start, end)| end.checked_sub(start));
        if running_ns.len() != expected_count {
            return err_box!(
                "completed {} of {expected_count} Running reads",
                running_ns.len()
            );
        }
        if preopened {
            for (client, job) in connections.iter().zip(jobs) {
                complete_read(client, job.id, chunks).await?;
            }
        }
        Ok(ReadRun {
            elapsed,
            user_ticks: after
                .0
                .checked_sub(before.0)
                .ok_or("worker user ticks decreased")?,
            system_ticks: after
                .1
                .checked_sub(before.1)
                .ok_or("worker system ticks decreased")?,
            proc_read_bytes: io_delta,
            running_ns,
        })
    })
}

fn read_frame(id: i64, seq: i32, status: RequestStatus) -> Builder {
    Builder::new()
        .code(RpcCode::ReadBlock)
        .request(status)
        .req_id(id)
        .seq_id(seq)
}

async fn open_read(
    client: &RpcClient,
    job: ReadJob,
    pattern: ReadPattern,
    block_bytes: usize,
) -> CommonResult<()> {
    let id = job.id;
    let open = read_frame(id, 0, RequestStatus::Open)
        .proto_header(BlockReadRequest {
            id,
            off: job.offset as i64,
            len: block_bytes as i64,
            chunk_size: pattern.chunk_bytes() as i32,
            short_circuit: false,
            enable_read_ahead: false,
            read_ahead_len: 0,
            drop_cache_len: 0,
            component_info: None,
        })
        .build();
    let opened: BlockReadResponse = check_rpc(client, open).await?.parse_header()?;
    if opened.id != id || opened.len != block_bytes as i64 || opened.path.is_some() {
        return err_box!("unexpected remote read Open for block {}: {:?}", id, opened);
    }
    Ok(())
}

async fn read_chunks(
    client: &RpcClient,
    job: ReadJob,
    pattern: ReadPattern,
    expected: &Bytes,
    running_ns: &mut Vec<u64>,
) -> CommonResult<()> {
    let id = job.id;
    let chunk_bytes = pattern.chunk_bytes();
    for chunk in 0..pattern.chunks_per_block(expected.len()) {
        let off = job.offset + chunk * chunk_bytes;
        // The starting offset is on Open. A seek header here would force the legacy path.
        let running = read_frame(id, chunk as i32 + 1, RequestStatus::Running).build();
        let start = Instant::now();
        let reply = check_rpc(client, running).await?;
        let ns = start.elapsed().as_nanos() as u64;
        if reply.data.as_slice() != &expected[off..off + chunk_bytes] {
            return err_box!(
                "block {id} offset {off}: Running payload mismatch ({} bytes)",
                reply.data.len()
            );
        }
        running_ns.push(ns);
    }
    Ok(())
}

async fn complete_read(client: &RpcClient, id: i64, chunks: usize) -> CommonResult<()> {
    check_rpc(
        client,
        read_frame(id, chunks as i32 + 1, RequestStatus::Complete).build(),
    )
    .await?;
    Ok(())
}

fn proc_read_bytes(pid: u32) -> Option<u64> {
    // Kernel accounting is useful context, but not a cache-residency or device-level measurement.
    fs::read_to_string(format!("/proc/{pid}/io"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("read_bytes:"))?
        .trim()
        .parse()
        .ok()
}
