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

//! Drive [`InflightBytesGate`] against a controllable HTTP byte source.
//!
//! This is the evidence harness for the in-flight-byte admission control: the
//! dummy S3 server (`state/2026-11-19-curvine-inflight-bytes/server/dummy_s3.py`)
//! exposes a tunable knee (latency, per-request rate, server slots), and this
//! binary either sweeps fixed byte budgets or lets the probe policy find one.
//!
//! Build (debug is enough for a loopback test):
//!   cargo run -p curvine-data-transfer --example inflight_probe -- \
//!     --url http://127.0.0.1:19000/test --files 32 --file-size-mb 64 \
//!     --targets 16,32,64,128,256 --duration-secs 20 --out results/
//!
//! Every run writes a timeline CSV (200 ms samples) and one summary row, so
//! intermediate attempts remain reviewable.

use curvine_data_transfer::worker::task::{InflightBytesGate, ProbePolicy};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MIB: f64 = 1024.0 * 1024.0;

#[derive(Debug, Clone)]
struct Args {
    url: String,
    key_prefix: String,
    key_width: usize,
    files: usize,
    start_index: usize,
    file_size: i64,
    chunk: i64,
    concurrent: usize,
    task_bytes: i64,
    cap_task_bytes: bool,
    /// Emulates `worker_max_concurrent_tasks`: tasks that may exist at once.
    /// 0 = unlimited (the bench default, so the gate alone is visible).
    task_limit: usize,
    /// Reserve in-flight bytes per range request instead of per task. This is
    /// the "homogeneous work unit" model: the budget granularity becomes the
    /// read chunk, not the whole object's pipeline.
    gate_per_range: bool,
    targets: Vec<i64>,
    probe: bool,
    probe_window: Duration,
    probe_min: i64,
    probe_max: i64,
    duration: Duration,
    sample: Duration,
    out: PathBuf,
}

impl Args {
    fn parse() -> Self {
        let mut args = Args {
            url: "http://127.0.0.1:19000/test".to_string(),
            key_prefix: "file-".to_string(),
            key_width: 5,
            files: 32,
            start_index: 0,
            file_size: 0,
            chunk: 4 * 1024 * 1024,
            concurrent: 4,
            task_bytes: 0,
            // Production prices a task by its source stat; keep the bench on
            // the same model unless asked for the uncapped pipeline cost.
            cap_task_bytes: true,
            task_limit: 0,
            gate_per_range: false,
            targets: vec![],
            probe: false,
            probe_window: Duration::from_secs(2),
            probe_min: 16 * 1024 * 1024,
            probe_max: 1024 * 1024 * 1024,
            duration: Duration::from_secs(20),
            sample: Duration::from_millis(200),
            out: PathBuf::from("inflight-probe-results"),
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut value = || {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {arg}"))
            };
            match arg.as_str() {
                "--url" => args.url = value(),
                "--key-prefix" => args.key_prefix = value(),
                "--key-width" => args.key_width = value().parse().unwrap(),
                "--files" => args.files = value().parse().unwrap(),
                "--start-index" => args.start_index = value().parse().unwrap(),
                "--file-size-mb" => args.file_size = (value().parse::<f64>().unwrap() * MIB) as i64,
                "--chunk-mb" => args.chunk = (value().parse::<f64>().unwrap() * MIB) as i64,
                "--concurrent" => args.concurrent = value().parse().unwrap(),
                "--task-mb" => args.task_bytes = (value().parse::<f64>().unwrap() * MIB) as i64,
                "--cap-task-bytes" => args.cap_task_bytes = true,
                "--no-cap-task-bytes" => args.cap_task_bytes = false,
                "--task-limit" => args.task_limit = value().parse().unwrap(),
                "--gate-per-range" => args.gate_per_range = true,
                "--targets-mb" => {
                    args.targets = value()
                        .split(',')
                        .map(|v| (v.parse::<f64>().unwrap() * MIB) as i64)
                        .collect()
                }
                "--probe" => args.probe = true,
                "--probe-window-secs" => {
                    args.probe_window = Duration::from_secs(value().parse().unwrap())
                }
                "--probe-min-mb" => args.probe_min = (value().parse::<f64>().unwrap() * MIB) as i64,
                "--probe-max-mb" => args.probe_max = (value().parse::<f64>().unwrap() * MIB) as i64,
                "--duration-secs" => args.duration = Duration::from_secs(value().parse().unwrap()),
                "--sample-ms" => args.sample = Duration::from_millis(value().parse().unwrap()),
                "--out" => args.out = PathBuf::from(value()),
                other => panic!("unknown argument {other}"),
            }
        }
        if args.task_bytes == 0 {
            args.task_bytes = args.chunk * args.concurrent as i64;
        }
        if args.targets.is_empty() && !args.probe {
            args.targets = vec![
                16 * 1024 * 1024,
                32 * 1024 * 1024,
                64 * 1024 * 1024,
                128 * 1024 * 1024,
                256 * 1024 * 1024,
            ];
        }
        args
    }

    fn key(&self, i: usize) -> String {
        format!("{}{:0width$}", self.key_prefix, i, width = self.key_width)
    }
}

#[derive(Default)]
struct RunStats {
    completed_bytes: AtomicU64,
    completed_ranges: AtomicU64,
    requests_started: AtomicU64,
    inflight_requests: AtomicI64,
    peak_inflight_requests: AtomicI64,
    errors: AtomicU64,
    latencies_ms: Mutex<Vec<f64>>,
}

impl RunStats {
    fn observe(&self, ms: f64) {
        let mut lat = self.latencies_ms.lock().unwrap();
        if lat.len() < 200_000 {
            lat.push(ms);
        }
    }

    fn percentile(&self, p: f64) -> f64 {
        let mut lat = self.latencies_ms.lock().unwrap().clone();
        if lat.is_empty() {
            return 0.0;
        }
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (((p / 100.0) * (lat.len() - 1) as f64).round() as usize).min(lat.len() - 1);
        lat[idx]
    }

    fn snapshot_latency(&self) -> (f64, f64) {
        (self.percentile(50.0), self.percentile(95.0))
    }
}

#[derive(serde::Deserialize, Default)]
struct ServerStats {
    active: i64,
    peak_active: i64,
    service_ms_p50: Option<f64>,
    service_ms_p95: Option<f64>,
    queue_ms_p95: Option<f64>,
    bytes_total: i64,
}

/// `scheme://host:port` part of the dataset URL (the path carries the dataset
/// spec, which may itself contain `/`, e.g. `rate=25MB/s`).
fn server_root(url: &str) -> String {
    match url.find("://") {
        Some(scheme) => {
            let rest = &url[scheme + 3..];
            match rest.find('/') {
                Some(slash) => url[..scheme + 3 + slash].to_string(),
                None => url.to_string(),
            }
        }
        None => url.to_string(),
    }
}

async fn server_stats(client: &reqwest::Client, base: &str) -> ServerStats {
    let url = format!("{}/__stats", server_root(base));
    match client.get(url).send().await {
        Ok(resp) => resp.json().await.unwrap_or_default(),
        Err(_) => ServerStats::default(),
    }
}

async fn reset_server(client: &reqwest::Client, base: &str) {
    let url = format!("{}/__reset", server_root(base));
    let _ = client.post(url).send().await;
}

async fn fetch_range(
    client: &reqwest::Client,
    url: String,
    start: i64,
    end: i64,
    stats: Arc<RunStats>,
    gate: InflightBytesGate,
    reserve: Option<i64>,
) -> Result<u64, String> {
    let _permit = match reserve {
        Some(bytes) => Some(gate.acquire(bytes).await),
        None => None,
    };
    stats.requests_started.fetch_add(1, Ordering::Relaxed);
    let now_inflight = stats.inflight_requests.fetch_add(1, Ordering::AcqRel) + 1;
    stats
        .peak_inflight_requests
        .fetch_max(now_inflight, Ordering::Relaxed);
    let started = Instant::now();

    let result = async {
        let resp = client
            .get(&url)
            .header("Range", format!("bytes={}-{}", start, end))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!(
                "HTTP {} for {} [{}, {})",
                resp.status(),
                url,
                start,
                end
            ));
        }
        let mut resp = resp;
        let mut total = 0u64;
        while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
            total += chunk.len() as u64;
        }
        Ok(total)
    }
    .await;

    stats.inflight_requests.fetch_sub(1, Ordering::AcqRel);
    stats.observe(started.elapsed().as_secs_f64() * 1000.0);
    match result {
        Ok(total) => {
            stats.completed_bytes.fetch_add(total, Ordering::Relaxed);
            stats.completed_ranges.fetch_add(1, Ordering::Relaxed);
            gate.record_completed(total as i64);
            Ok(total)
        }
        Err(e) => {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            Err(e)
        }
    }
}

async fn head_size(client: &reqwest::Client, url: &str) -> Result<i64, String> {
    let resp = client.head(url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HEAD {} -> {}", url, resp.status()));
    }
    // `Response::content_length()` reports the remaining *body* length, which is
    // 0 for a HEAD response; the object size only exists in the header.
    resp.headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or_else(|| format!("HEAD {} returned no Content-Length", url))
}

async fn run_task(
    client: reqwest::Client,
    args: Arc<Args>,
    gate: InflightBytesGate,
    stats: Arc<RunStats>,
    task: usize,
) {
    let url = format!("{}/{}", args.url.trim_end_matches('/'), args.key(task));
    let file_size = if args.file_size > 0 {
        args.file_size
    } else {
        match head_size(&client, &url).await {
            Ok(size) => size,
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("HEAD failed for {}: {}", url, e);
                return;
            }
        }
    };
    // The whole task counts as in-flight while it runs, like a load task in the
    // worker: its read pipeline can hold chunk*concurrent bytes at any moment.
    // `--cap-task-bytes` mirrors the worker's priced admission: it stats the
    // source and prices a 4 KiB object at 4 KiB instead of the whole pipeline.
    let task_bytes = if args.cap_task_bytes {
        args.task_bytes.min(file_size).max(1)
    } else {
        args.task_bytes
    };
    let _permit = if args.gate_per_range {
        None
    } else {
        Some(gate.acquire(task_bytes).await)
    };

    let ranges: Vec<(i64, i64)> = (0..file_size)
        .step_by(args.chunk as usize)
        .map(|start| {
            let end = (start + args.chunk).min(file_size);
            (start, end)
        })
        .collect();

    let outcome = stream::iter(ranges)
        .map(|(start, end)| {
            fetch_range(
                &client,
                url.clone(),
                start,
                end.saturating_sub(1),
                stats.clone(),
                gate.clone(),
                args.gate_per_range.then_some(end - start),
            )
        })
        .buffer_unordered(args.concurrent)
        .try_collect::<Vec<u64>>()
        .await;

    if let Err(e) = outcome {
        if stats.errors.load(Ordering::Relaxed) < 5 {
            eprintln!("task {} failed: {}", task, e);
        }
    }
}

struct RunOutcome {
    label: String,
    target: i64,
    settled_target: i64,
    avg_mbps: f64,
    p50_ms: f64,
    p95_ms: f64,
    server_peak_active: i64,
    server_p95_ms: f64,
    peak_inflight_requests: i64,
    server_bytes: i64,
    client_bytes: u64,
    errors: u64,
    gate_peak_bytes: i64,
    gate_waiters_max: usize,
    gate_waits_total: u64,
}

#[allow(clippy::too_many_arguments)]
async fn run_once(
    label: &str,
    gate: InflightBytesGate,
    args: Arc<Args>,
    client: reqwest::Client,
    probe: Option<ProbePolicy>,
) -> RunOutcome {
    reset_server(&client, &args.url).await;
    let stats = Arc::new(RunStats::default());
    let start = Instant::now();

    let limiter =
        (args.task_limit > 0).then(|| Arc::new(tokio::sync::Semaphore::new(args.task_limit)));
    let mut handles = Vec::new();
    for i in args.start_index..args.start_index + args.files {
        let ctx = (client.clone(), args.clone(), gate.clone(), stats.clone());
        let limiter = limiter.clone();
        handles.push(tokio::spawn(async move {
            let _slot = match limiter {
                Some(limiter) => Some(limiter.acquire_owned().await.expect("task limiter")),
                None => None,
            };
            run_task(ctx.0, ctx.1, ctx.2, ctx.3, i).await;
        }));
    }

    let timeline_path = args.out.join(format!("timeline_{}.csv", label));
    let mut timeline = std::fs::File::create(&timeline_path).expect("create timeline csv");
    use std::io::Write;
    writeln!(
        timeline,
        "t_s,target_mib,used_mib,active_tasks,waiters,inflight_requests,completed_mib,inst_mbps,srv_active,srv_peak,srv_p50_ms,srv_p95_ms,srv_queue_p95_ms"
    )
    .unwrap();

    let mut last_completed = 0u64;
    let mut last_t = Instant::now();
    let mut gate_peak = 0i64;
    let mut waiter_max = 0usize;
    let mut probe_task = probe.map(|policy| {
        let gate = gate.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(policy.window).await;
                if gate.probe_tick(policy.window).is_none() && !gate.is_probing() {
                    break;
                }
            }
        })
    });

    loop {
        tokio::time::sleep(args.sample).await;
        if start.elapsed() >= args.duration {
            break;
        }
        let now = Instant::now();
        let completed = stats.completed_bytes.load(Ordering::Relaxed);
        let inst = (completed - last_completed) as f64 / now.duration_since(last_t).as_secs_f64();
        last_completed = completed;
        last_t = now;

        let snap = gate.snapshot();
        gate_peak = gate_peak.max(snap.used_bytes);
        waiter_max = waiter_max.max(snap.waiters);
        let srv = server_stats(&client, &args.url).await;
        writeln!(
            timeline,
            "{:.3},{:.2},{:.2},{},{},{},{:.2},{:.2},{},{},{:.1},{:.1},{:.1}",
            start.elapsed().as_secs_f64(),
            snap.target_bytes as f64 / MIB,
            snap.used_bytes as f64 / MIB,
            snap.active_permits,
            snap.waiters,
            stats.inflight_requests.load(Ordering::Relaxed),
            completed as f64 / MIB,
            inst / MIB,
            srv.active,
            srv.peak_active,
            srv.service_ms_p50.unwrap_or(0.0),
            srv.service_ms_p95.unwrap_or(0.0),
            srv.queue_ms_p95.unwrap_or(0.0),
        )
        .unwrap();
    }

    for handle in &handles {
        handle.abort();
    }
    if let Some(task) = probe_task.take() {
        task.abort();
    }
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let client_bytes = stats.completed_bytes.load(Ordering::Relaxed);
    let (p50, p95) = stats.snapshot_latency();
    let srv = server_stats(&client, &args.url).await;
    let snap = gate.snapshot();

    RunOutcome {
        label: label.to_string(),
        target: gate.target_bytes(),
        settled_target: snap.target_bytes,
        avg_mbps: client_bytes as f64 / elapsed / MIB,
        p50_ms: p50,
        p95_ms: p95,
        server_peak_active: srv.peak_active,
        server_p95_ms: srv.service_ms_p95.unwrap_or(0.0),
        peak_inflight_requests: stats.peak_inflight_requests.load(Ordering::Relaxed),
        server_bytes: srv.bytes_total,
        client_bytes,
        errors: stats.errors.load(Ordering::Relaxed),
        gate_peak_bytes: gate_peak,
        gate_waiters_max: waiter_max,
        gate_waits_total: snap.waits_total,
    }
}

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());
    std::fs::create_dir_all(&args.out).expect("create out dir");
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(256)
        .tcp_nodelay(true)
        .build()
        .expect("http client");

    println!(
        "target={} files={} file={} chunk={:.1}MiB concurrent={} task={:.1}MiB duration={:?} probe={}",
        args.url,
        args.files,
        if args.file_size > 0 {
            format!("{:.1}MiB", args.file_size as f64 / MIB)
        } else {
            "per-object".to_string()
        },
        args.chunk as f64 / MIB,
        args.concurrent,
        args.task_bytes as f64 / MIB,
        args.duration,
        args.probe,
    );

    let summary_path = args.out.join("summary.csv");
    let write_header = !summary_path.exists();
    let mut summary = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&summary_path)
        .expect("open summary");
    {
        use std::io::Write;
        if write_header {
            writeln!(
                summary,
                "label,target_mib,settled_target_mib,avg_mbps,p50_ms,p95_ms,server_peak_active,server_p95_ms,peak_inflight_requests,server_mib,client_mib,errors,gate_peak_mib,gate_waiters_max,gate_waits_total"
            )
            .unwrap();
        }
    }

    if args.probe {
        let policy = ProbePolicy::new(args.probe_min, args.probe_max, args.probe_window);
        let gate = InflightBytesGate::with_probe(policy);
        let outcome = run_once("probe", gate, args.clone(), client, Some(policy)).await;
        record(&mut summary, &outcome);
    } else {
        for target in args.targets.clone() {
            let gate = InflightBytesGate::new(target);
            let label = format!("fixed_{}mib", target / (1024 * 1024));
            let outcome = run_once(&label, gate, args.clone(), client.clone(), None).await;
            record(&mut summary, &outcome);
        }
    }
}

fn record(summary: &mut std::fs::File, o: &RunOutcome) {
    use std::io::Write;
    writeln!(
        summary,
        "{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{:.2},{},{:.2},{:.2},{},{:.2},{},{}",
        o.label,
        o.target as f64 / MIB,
        o.settled_target as f64 / MIB,
        o.avg_mbps,
        o.p50_ms,
        o.p95_ms,
        o.server_peak_active,
        o.server_p95_ms,
        o.peak_inflight_requests,
        o.server_bytes as f64 / MIB,
        o.client_bytes as f64 / MIB,
        o.errors,
        o.gate_peak_bytes as f64 / MIB,
        o.gate_waiters_max,
        o.gate_waits_total,
    )
    .unwrap();
    summary.flush().unwrap();
    println!(
        "run {:<14} target={:>7.1}MiB settled={:>7.1}MiB avg={:>8.2}MiB/s p50={:>6.1}ms p95={:>7.1}ms srv_peak={} srv_p95={:>6.1}ms gate_peak={:.1}MiB waits={} errors={}",
        o.label,
        o.target as f64 / MIB,
        o.settled_target as f64 / MIB,
        o.avg_mbps,
        o.p50_ms,
        o.p95_ms,
        o.server_peak_active,
        o.server_p95_ms,
        o.gate_peak_bytes as f64 / MIB,
        o.gate_waits_total,
        o.errors,
    );
}
