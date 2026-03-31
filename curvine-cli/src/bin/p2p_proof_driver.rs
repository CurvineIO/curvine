use bytes::BytesMut;
use clap::{Parser, ValueEnum};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use orpc::common::{Logger, Utils};
use orpc::io::net::InetAddr;
use orpc::runtime::RpcRuntime;
use orpc::{err_box, CommonResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

#[derive(Copy, Clone, Eq, PartialEq, ValueEnum)]
enum Role {
    Provider,
    Consumer,
    Mixed,
}

#[derive(Parser)]
#[command(name = "p2p-proof-driver")]
struct Args {
    #[arg(long)]
    role: Role,
    #[arg(long)]
    conf: String,
    #[arg(long)]
    master_addrs: String,
    #[arg(long)]
    remote_path: String,
    #[arg(long)]
    input_file: Option<PathBuf>,
    #[arg(long)]
    output_file: PathBuf,
    #[arg(long)]
    bootstrap_file: Option<PathBuf>,
    #[arg(long)]
    peer_file: Option<PathBuf>,
    #[arg(long)]
    bootstrap_host: Option<String>,
    #[arg(long, default_value_t = 1)]
    warm_reads: u32,
    #[arg(long, default_value_t = 180)]
    hold_secs: u64,
    #[arg(long, default_value_t = 120)]
    bootstrap_wait_secs: u64,
    #[arg(long, default_value_t = 2)]
    report_interval_secs: u64,
    #[arg(long, default_value_t = 1)]
    consumer_reads: u32,
    #[arg(long, default_value_t = 0)]
    consumer_read_interval_ms: u64,
    #[arg(long, default_value_t = 0)]
    consumer_startup_wait_ms: u64,
    #[arg(long)]
    client_id: Option<String>,
    #[arg(long)]
    manifest_dir: Option<PathBuf>,
    #[arg(long)]
    read_manifest_dir: Option<PathBuf>,
    #[arg(long)]
    write_manifest_dir: Option<PathBuf>,
    #[arg(long, default_value = "64KB,1MB,16MB")]
    file_size_spec: String,
    #[arg(long, default_value_t = 16)]
    file_count: u32,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    #[arg(long, default_value_t = 70)]
    read_ratio: u8,
    #[arg(long, default_value_t = 80)]
    hotset_ratio: u8,
    #[arg(long, default_value_t = 0)]
    op_interval_ms: u64,
    #[arg(long, default_value_t = false)]
    warm_written: bool,
    #[arg(long, default_value_t = 0)]
    post_run_hold_secs: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct ManifestEntry {
    path: String,
    size_bytes: usize,
    sha256: String,
    owner: String,
    write_seq: u64,
    created_at_ms: u64,
}

#[derive(Clone, Serialize)]
struct LatencySummary {
    count: usize,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Serialize)]
struct WorkloadSummary {
    role: String,
    client_id: String,
    duration_secs: u64,
    size_spec: Vec<usize>,
    file_count: u32,
    read_ratio: u8,
    hotset_ratio: u8,
    total_ops: u64,
    read_ops: u64,
    write_ops: u64,
    read_ok: u64,
    write_ok: u64,
    read_errors: u64,
    write_errors: u64,
    first_read_error: Option<String>,
    first_write_error: Option<String>,
    sha_mismatches: u64,
    bytes_read: u64,
    bytes_written: u64,
    manifest_entries_seen: u64,
    p2p_recv_delta: u64,
    p2p_sent_delta: u64,
    cached_chunks_delta: u64,
    read_latency: LatencySummary,
    write_latency: LatencySummary,
}

struct WorkloadRng {
    state: u64,
}

impl WorkloadRng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn next_index(&mut self, len: usize) -> usize {
        if len <= 1 {
            return 0;
        }
        (self.next_u64() % len as u64) as usize
    }

    fn next_percent(&mut self) -> u8 {
        (self.next_u64() % 100) as u8
    }
}

fn parse_master_addrs(raw: &str) -> CommonResult<Vec<InetAddr>> {
    let mut addrs = Vec::new();
    for node in raw.split(',') {
        let pair: Vec<&str> = node.trim().split(':').collect();
        if pair.len() != 2 {
            return err_box!(
                "invalid master_addrs format: '{}', expected 'host1:port1,host2:port2'",
                raw
            );
        }
        let host = pair[0].to_string();
        let port = pair[1]
            .parse::<u16>()
            .map_err(|_| format!("invalid master port '{}'", pair[1]))?;
        addrs.push(InetAddr::new(host, port));
    }
    Ok(addrs)
}

fn load_conf(args: &Args) -> CommonResult<ClusterConf> {
    let mut conf = ClusterConf::from(&args.conf)?;
    conf.client.master_addrs = parse_master_addrs(&args.master_addrs)?;
    conf.client.init()?;
    Ok(conf)
}

fn parent_of(remote: &str) -> String {
    match remote.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(idx) => remote[..idx].to_string(),
    }
}

fn normalize_remote_dir(remote: &str) -> String {
    if remote == "/" {
        return "/".to_string();
    }
    remote.trim_end_matches('/').to_string()
}

fn parse_size_spec(spec: &str) -> CommonResult<Vec<usize>> {
    let mut sizes = Vec::new();
    for token in spec
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        sizes.push(parse_size_token(token)?);
    }
    if sizes.is_empty() {
        return err_box!("file_size_spec is empty");
    }
    Ok(sizes)
}

fn parse_size_token(token: &str) -> CommonResult<usize> {
    let normalized = token.trim().to_ascii_uppercase();
    let split_at = normalized
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(normalized.len());
    if split_at == 0 {
        return err_box!("invalid size token: {}", token);
    }
    let value = normalized[..split_at]
        .parse::<u64>()
        .map_err(|_| format!("invalid size token: {}", token))?;
    let unit = normalized[split_at..].trim();
    let multiplier = match unit {
        "" | "B" => 1u64,
        "K" | "KB" => 1024u64,
        "M" | "MB" => 1024u64 * 1024,
        "G" | "GB" => 1024u64 * 1024 * 1024,
        _ => return err_box!("unsupported size unit in token: {}", token),
    };
    let size = value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size token overflow: {}", token))?;
    usize::try_from(size).map_err(|_| format!("size token overflow: {}", token).into())
}

fn choose_manifest_index(total: usize, hotset_ratio: u8, rng: &mut WorkloadRng) -> usize {
    if total <= 1 {
        return 0;
    }
    let hotset_ratio = hotset_ratio.clamp(1, 100) as usize;
    let hotset_len = total
        .saturating_mul(hotset_ratio)
        .div_ceil(100)
        .clamp(1, total);
    let start = total.saturating_sub(hotset_len);
    start + rng.next_index(hotset_len)
}

fn sha256_hex(data: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(data);
    digest
        .finalize()
        .iter()
        .map(|value| format!("{:02x}", value))
        .collect()
}

fn summarize_latencies(mut latencies: Vec<f64>) -> LatencySummary {
    if latencies.is_empty() {
        return LatencySummary {
            count: 0,
            avg_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
        };
    }
    latencies.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let count = latencies.len();
    let avg_ms = latencies.iter().sum::<f64>() / count as f64;
    let percentile = |ratio: f64| {
        let index = ((count.saturating_sub(1)) as f64 * ratio).round() as usize;
        latencies[index]
    };
    LatencySummary {
        count,
        avg_ms,
        p50_ms: percentile(0.50),
        p95_ms: percentile(0.95),
        p99_ms: percentile(0.99),
        max_ms: *latencies.last().unwrap_or(&0.0),
    }
}

fn client_id(args: &Args) -> String {
    args.client_id
        .clone()
        .unwrap_or_else(|| format!("{}-{}", std::process::id(), Utils::uuid()))
}

fn manifest_file(manifest_dir: &FsPath, client_id: &str) -> PathBuf {
    manifest_dir.join(format!("{}.jsonl", client_id))
}

fn resolve_manifest_dirs(args: &Args) -> CommonResult<(PathBuf, PathBuf)> {
    let read_manifest_dir = args
        .read_manifest_dir
        .clone()
        .or_else(|| args.manifest_dir.clone())
        .or_else(|| args.write_manifest_dir.clone())
        .ok_or_else(|| "mixed role requires --manifest-dir or --read-manifest-dir".to_string())?;
    let write_manifest_dir = args
        .write_manifest_dir
        .clone()
        .or_else(|| args.manifest_dir.clone())
        .or_else(|| args.read_manifest_dir.clone())
        .ok_or_else(|| "mixed role requires --manifest-dir or --write-manifest-dir".to_string())?;
    Ok((read_manifest_dir, write_manifest_dir))
}

fn append_manifest_entry(
    manifest_dir: &FsPath,
    client_id: &str,
    entry: &ManifestEntry,
) -> CommonResult<()> {
    fs::create_dir_all(manifest_dir)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(manifest_file(manifest_dir, client_id))?;
    serde_json::to_writer(&mut file, entry)?;
    writeln!(file)?;
    Ok(())
}

fn load_manifest_entries(manifest_dir: &FsPath) -> CommonResult<Vec<ManifestEntry>> {
    if !manifest_dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries: Vec<ManifestEntry> = Vec::new();
    for item in fs::read_dir(manifest_dir)? {
        let path = item?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        let content = fs::read_to_string(path)?;
        for line in content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            entries.push(serde_json::from_str(line)?);
        }
    }
    entries.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.owner.cmp(&right.owner))
            .then_with(|| left.write_seq.cmp(&right.write_seq))
    });
    Ok(entries)
}

fn payload_seed(client_id: &str, write_seq: u64, size: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    client_id.hash(&mut hasher);
    write_seq.hash(&mut hasher);
    size.hash(&mut hasher);
    hasher.finish()
}

fn build_payload(client_id: &str, write_seq: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    let mut rng = WorkloadRng::new(payload_seed(client_id, write_seq, size));
    let mut offset = 0usize;
    while offset < payload.len() {
        let bytes = rng.next_u64().to_le_bytes();
        let end = (offset + bytes.len()).min(payload.len());
        payload[offset..end].copy_from_slice(&bytes[..end - offset]);
        offset = end;
    }
    payload
}

fn write_workload_summary(path: &PathBuf, summary: &WorkloadSummary) -> CommonResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(summary)?)?;
    Ok(())
}

async fn read_all(
    fs: &UnifiedFileSystem,
    path: &Path,
    metric_prefix: Option<&str>,
) -> CommonResult<Vec<u8>> {
    let status_start = Instant::now();
    let status = fs.get_status(path).await?;
    if let Some(prefix) = metric_prefix {
        println!(
            "{}_STATUS_MS={:.3}",
            prefix,
            status_start.elapsed().as_secs_f64() * 1000.0
        );
    }
    let open_start = Instant::now();
    let mut reader = fs.open(path).await?;
    if let Some(prefix) = metric_prefix {
        println!(
            "{}_OPEN_MS={:.3}",
            prefix,
            open_start.elapsed().as_secs_f64() * 1000.0
        );
    }
    let mut buf = BytesMut::zeroed(status.len as usize);
    let read_start = Instant::now();
    let size = reader.read_full(&mut buf).await?;
    if let Some(prefix) = metric_prefix {
        println!(
            "{}_READ_MS={:.3}",
            prefix,
            read_start.elapsed().as_secs_f64() * 1000.0
        );
    }
    let complete_start = Instant::now();
    reader.complete().await?;
    if let Some(prefix) = metric_prefix {
        println!(
            "{}_COMPLETE_MS={:.3}",
            prefix,
            complete_start.elapsed().as_secs_f64() * 1000.0
        );
    }
    buf.truncate(size);
    Ok(buf.to_vec())
}

async fn write_all(fs: &UnifiedFileSystem, path: &Path, payload: &[u8]) -> CommonResult<()> {
    let mut writer = fs.create(path, true).await?;
    writer.write(payload).await?;
    writer.complete().await?;
    Ok(())
}

async fn hold_after_workload(fs: &UnifiedFileSystem, hold_secs: u64, report_interval_secs: u64) {
    if hold_secs == 0 {
        return;
    }
    let report_interval = Duration::from_secs(report_interval_secs.max(1));
    let deadline = Instant::now() + Duration::from_secs(hold_secs);
    while Instant::now() < deadline {
        let _ = fs.cv().metrics_report().await;
        sleep(report_interval).await;
    }
}

async fn wait_bootstrap_addr(fs: &UnifiedFileSystem, wait_secs: u64) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(wait_secs.max(1));
    while Instant::now() < deadline {
        if let Some(addr) = fs.p2p_bootstrap_peer_addr() {
            return Some(addr);
        }
        sleep(Duration::from_millis(100)).await;
    }
    None
}

fn rewrite_bootstrap_host(raw: &str, host: Option<&str>) -> String {
    let Some(host) = host else {
        return raw.to_string();
    };
    let mut parts: Vec<String> = raw
        .split('/')
        .map(std::string::ToString::to_string)
        .collect();
    if parts.len() > 3 && matches!(parts[1].as_str(), "ip4" | "ip6" | "dns" | "dns4" | "dns6") {
        parts[2] = host.to_string();
        return parts.join("/");
    }
    raw.to_string()
}

fn peer_id_from_bootstrap_addr(addr: &str) -> Option<&str> {
    addr.rsplit_once("/p2p/").map(|(_, peer_id)| peer_id)
}

async fn publish_bootstrap_identity(
    fs: &UnifiedFileSystem,
    args: &Args,
    prefix: &str,
) -> CommonResult<Option<(String, String)>> {
    if args.bootstrap_file.is_none() && args.peer_file.is_none() {
        return Ok(None);
    }
    let bootstrap_addr_raw = wait_bootstrap_addr(fs, args.bootstrap_wait_secs)
        .await
        .ok_or_else(|| "bootstrap address not ready".to_string())?;
    let bootstrap_addr =
        rewrite_bootstrap_host(&bootstrap_addr_raw, args.bootstrap_host.as_deref());
    let peer_id = fs
        .p2p_peer_id()
        .or_else(|| peer_id_from_bootstrap_addr(&bootstrap_addr).map(str::to_string))
        .ok_or_else(|| format!("invalid bootstrap address: {}", bootstrap_addr))?;
    if let Some(bootstrap_file) = args.bootstrap_file.as_ref() {
        fs::write(bootstrap_file, bootstrap_addr.as_bytes())?;
    }
    if let Some(peer_file) = args.peer_file.as_ref() {
        fs::write(peer_file, peer_id.as_bytes())?;
    }
    println!("{}_PEER_ID={}", prefix, peer_id);
    println!("{}_BOOTSTRAP_ADDR={}", prefix, bootstrap_addr);
    Ok(Some((peer_id, bootstrap_addr)))
}

async fn run_provider(fs: UnifiedFileSystem, args: &Args) -> CommonResult<()> {
    let input_file = args
        .input_file
        .as_ref()
        .ok_or_else(|| "provider requires --input-file".to_string())?;
    if !fs.p2p_enabled() {
        return err_box!("p2p service is not enabled");
    }
    if args.bootstrap_file.is_none() || args.peer_file.is_none() {
        return err_box!("provider requires --bootstrap-file and --peer-file");
    }

    let parent = Path::from_str(parent_of(&args.remote_path).as_str())?;
    let remote = Path::from_str(&args.remote_path)?;
    let payload = fs::read(input_file)?;

    let _ = fs.delete(&parent, true).await;
    fs.mkdir(&parent, true).await?;
    write_all(&fs, &remote, payload.as_slice()).await?;

    let mut warmed = Vec::new();
    for _ in 0..args.warm_reads.max(1) {
        warmed = read_all(&fs, &remote, None).await?;
    }
    fs::write(&args.output_file, warmed)?;

    let _ = publish_bootstrap_identity(&fs, args, "PROVIDER").await?;
    println!(
        "PROVIDER_CACHE_CHUNKS={}",
        fs.p2p_stats_snapshot()
            .map(|snapshot| snapshot.cached_chunks_count)
            .unwrap_or(0)
    );

    hold_after_workload(&fs, args.hold_secs, args.report_interval_secs).await;
    let _ = fs.cv().metrics_report().await;
    println!("PROVIDER_STATE_OK=1");
    Ok(())
}

async fn run_consumer(fs: UnifiedFileSystem, args: &Args) -> CommonResult<()> {
    let remote = Path::from_str(&args.remote_path)?;
    let read_times = args.consumer_reads.max(1);
    let read_interval = Duration::from_millis(args.consumer_read_interval_ms);
    let startup_wait = Duration::from_millis(args.consumer_startup_wait_ms);
    if !startup_wait.is_zero() {
        sleep(startup_wait).await;
    }
    let before = fs.p2p_stats_snapshot();
    let mut payload = Vec::new();
    let read_begin = Instant::now();
    for index in 0..read_times {
        let read_start = Instant::now();
        let metric_prefix = format!("CONSUMER_READ_{}", index + 1);
        payload = read_all(&fs, &remote, Some(metric_prefix.as_str())).await?;
        println!(
            "CONSUMER_READ_{}_MS={:.3}",
            index + 1,
            read_start.elapsed().as_secs_f64() * 1000.0
        );
        if index + 1 < read_times && !read_interval.is_zero() {
            sleep(read_interval).await;
        }
    }
    let read_elapsed_ms = read_begin.elapsed().as_secs_f64() * 1000.0;
    std::fs::write(&args.output_file, payload)?;
    let after = fs.p2p_stats_snapshot();

    let _ = fs.cv().metrics_report().await;

    let recv_delta = after
        .as_ref()
        .zip(before.as_ref())
        .map(|(after, before)| after.bytes_recv.saturating_sub(before.bytes_recv))
        .unwrap_or(0);
    let cache_delta = after
        .as_ref()
        .zip(before.as_ref())
        .map(|(after, before)| {
            after
                .cached_chunks_count
                .saturating_sub(before.cached_chunks_count)
        })
        .unwrap_or(0);
    let p2p_enabled = u8::from(fs.p2p_enabled());

    println!("CONSUMER_P2P_ENABLED={}", p2p_enabled);
    println!("CONSUMER_P2P_RECV_DELTA={}", recv_delta);
    println!("CONSUMER_CACHE_CHUNKS_DELTA={}", cache_delta);
    println!("CONSUMER_READS={}", read_times);
    println!("CONSUMER_READ_ELAPSED_MS={:.3}", read_elapsed_ms);
    println!("CONSUMER_STATE_OK=1");
    Ok(())
}

async fn run_mixed(fs: UnifiedFileSystem, args: &Args) -> CommonResult<()> {
    if !fs.p2p_enabled() {
        return err_box!("mixed role requires p2p to be enabled");
    }
    let (read_manifest_dir, write_manifest_dir) = resolve_manifest_dirs(args)?;
    let client_id = client_id(args);
    let size_spec = parse_size_spec(&args.file_size_spec)?;
    let remote_dir = normalize_remote_dir(&args.remote_path);
    let remote_root = Path::from_str(&remote_dir)?;
    let _ = fs.mkdir(&remote_root, true).await;
    let client_root = Path::from_str(format!("{}/{}", remote_dir, client_id))?;
    let _ = fs.mkdir(&client_root, true).await;
    let _ = publish_bootstrap_identity(&fs, args, "MIXED").await?;

    let before = fs
        .p2p_stats_snapshot()
        .ok_or_else(|| "mixed role requires p2p snapshot".to_string())?;
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs.max(1));
    let op_interval = Duration::from_millis(args.op_interval_ms);
    let read_ratio = args.read_ratio.clamp(0, 100);
    let hotset_ratio = args.hotset_ratio.clamp(1, 100);
    let mut rng = WorkloadRng::new(payload_seed(
        &client_id,
        args.duration_secs,
        size_spec.len(),
    ));
    let mut write_seq = 0u64;
    let mut total_ops = 0u64;
    let mut read_ops = 0u64;
    let mut write_ops = 0u64;
    let mut read_ok = 0u64;
    let mut write_ok = 0u64;
    let mut read_errors = 0u64;
    let mut write_errors = 0u64;
    let mut sha_mismatches = 0u64;
    let mut bytes_read = 0u64;
    let mut bytes_written = 0u64;
    let mut first_read_error = None;
    let mut first_write_error = None;
    let mut manifest_entries_seen = 0u64;
    let mut read_latencies = Vec::new();
    let mut write_latencies = Vec::new();

    while Instant::now() < deadline {
        let manifest = load_manifest_entries(read_manifest_dir.as_path())?;
        manifest_entries_seen = manifest_entries_seen.max(manifest.len() as u64);
        let should_read = !manifest.is_empty() && rng.next_percent() < read_ratio;
        if should_read {
            let entry = &manifest[choose_manifest_index(manifest.len(), hotset_ratio, &mut rng)];
            let path = Path::from_str(&entry.path)?;
            let start = Instant::now();
            read_ops += 1;
            match read_all(&fs, &path, None).await {
                Ok(payload) => {
                    let actual = sha256_hex(&payload);
                    bytes_read = bytes_read.saturating_add(payload.len() as u64);
                    if actual == entry.sha256 {
                        read_ok = read_ok.saturating_add(1);
                    } else {
                        sha_mismatches = sha_mismatches.saturating_add(1);
                        read_errors = read_errors.saturating_add(1);
                    }
                }
                Err(err) => {
                    if first_read_error.is_none() {
                        first_read_error = Some(err.to_string());
                    }
                    read_errors = read_errors.saturating_add(1);
                }
            }
            read_latencies.push(start.elapsed().as_secs_f64() * 1000.0);
        } else {
            let slot = write_seq % u64::from(args.file_count.max(1));
            let size = size_spec[rng.next_index(size_spec.len())];
            let payload = build_payload(&client_id, write_seq, size);
            let sha256 = sha256_hex(&payload);
            let remote_path = format!(
                "{}/{}/slot-{:04}-seq-{:08}.bin",
                remote_dir, client_id, slot, write_seq
            );
            let path = Path::from_str(&remote_path)?;
            let start = Instant::now();
            write_ops = write_ops.saturating_add(1);
            match write_all(&fs, &path, &payload).await {
                Ok(()) => {
                    if args.warm_written {
                        match read_all(&fs, &path, None).await {
                            Ok(warmed) => {
                                let warmed_sha256 = sha256_hex(&warmed);
                                if warmed_sha256 != sha256 {
                                    if first_write_error.is_none() {
                                        first_write_error = Some(format!(
                                            "warm_written checksum mismatch for {}: {} != {}",
                                            remote_path, warmed_sha256, sha256
                                        ));
                                    }
                                    write_errors = write_errors.saturating_add(1);
                                    write_latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                                    total_ops = total_ops.saturating_add(1);
                                    if !op_interval.is_zero() {
                                        sleep(op_interval).await;
                                    }
                                    continue;
                                }
                            }
                            Err(err) => {
                                if first_write_error.is_none() {
                                    first_write_error = Some(err.to_string());
                                }
                                write_errors = write_errors.saturating_add(1);
                                write_latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                                total_ops = total_ops.saturating_add(1);
                                if !op_interval.is_zero() {
                                    sleep(op_interval).await;
                                }
                                continue;
                            }
                        }
                    }
                    let entry = ManifestEntry {
                        path: remote_path,
                        size_bytes: size,
                        sha256,
                        owner: client_id.clone(),
                        write_seq,
                        created_at_ms: orpc::common::LocalTime::mills(),
                    };
                    append_manifest_entry(write_manifest_dir.as_path(), &client_id, &entry)?;
                    write_ok = write_ok.saturating_add(1);
                    bytes_written = bytes_written.saturating_add(size as u64);
                    write_seq = write_seq.saturating_add(1);
                }
                Err(err) => {
                    if first_write_error.is_none() {
                        first_write_error = Some(err.to_string());
                    }
                    write_errors = write_errors.saturating_add(1);
                }
            }
            write_latencies.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        total_ops = total_ops.saturating_add(1);
        if !op_interval.is_zero() {
            sleep(op_interval).await;
        }
    }

    let _ = fs.cv().metrics_report().await;
    let after = fs
        .p2p_stats_snapshot()
        .ok_or_else(|| "mixed role requires p2p snapshot".to_string())?;
    let summary = WorkloadSummary {
        role: "mixed".to_string(),
        client_id: client_id.clone(),
        duration_secs: args.duration_secs.max(1),
        size_spec,
        file_count: args.file_count.max(1),
        read_ratio,
        hotset_ratio,
        total_ops,
        read_ops,
        write_ops,
        read_ok,
        write_ok,
        read_errors,
        write_errors,
        first_read_error,
        first_write_error,
        sha_mismatches,
        bytes_read,
        bytes_written,
        manifest_entries_seen,
        p2p_recv_delta: after.bytes_recv.saturating_sub(before.bytes_recv),
        p2p_sent_delta: after.bytes_sent.saturating_sub(before.bytes_sent),
        cached_chunks_delta: after
            .cached_chunks_count
            .saturating_sub(before.cached_chunks_count) as u64,
        read_latency: summarize_latencies(read_latencies),
        write_latency: summarize_latencies(write_latencies),
    };
    write_workload_summary(&args.output_file, &summary)?;

    println!("MIXED_CLIENT_ID={}", summary.client_id);
    println!("MIXED_TOTAL_OPS={}", summary.total_ops);
    println!("MIXED_READ_OK={}", summary.read_ok);
    println!("MIXED_WRITE_OK={}", summary.write_ok);
    println!("MIXED_SHA_MISMATCHES={}", summary.sha_mismatches);
    println!("MIXED_P2P_RECV_DELTA={}", summary.p2p_recv_delta);
    println!("MIXED_P2P_SENT_DELTA={}", summary.p2p_sent_delta);
    if let Some(first_write_error) = summary.first_write_error.as_ref() {
        println!("MIXED_FIRST_WRITE_ERROR={}", first_write_error);
    }
    if let Some(first_read_error) = summary.first_read_error.as_ref() {
        println!("MIXED_FIRST_READ_ERROR={}", first_read_error);
    }
    hold_after_workload(&fs, args.post_run_hold_secs, args.report_interval_secs).await;
    let _ = fs.cv().metrics_report().await;
    println!("MIXED_STATE_OK=1");
    Ok(())
}

fn main() -> CommonResult<()> {
    let args = Args::parse();
    Utils::set_panic_exit_hook();
    let conf = load_conf(&args)?;
    Logger::init(conf.log.clone());

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = UnifiedFileSystem::with_rt(conf, rt.clone())?;
    rt.block_on(async move {
        let res = match args.role {
            Role::Provider => run_provider(fs, &args).await,
            Role::Consumer => run_consumer(fs, &args).await,
            Role::Mixed => run_mixed(fs, &args).await,
        };
        if let Err(e) = &res {
            eprintln!("{}", e);
        }
        res
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mixed_args() -> Args {
        Args {
            role: Role::Mixed,
            conf: "conf".to_string(),
            master_addrs: "127.0.0.1:8995".to_string(),
            remote_path: "/tmp".to_string(),
            input_file: None,
            output_file: PathBuf::from("out.json"),
            bootstrap_file: None,
            peer_file: None,
            bootstrap_host: None,
            warm_reads: 1,
            hold_secs: 180,
            bootstrap_wait_secs: 120,
            report_interval_secs: 2,
            consumer_reads: 1,
            consumer_read_interval_ms: 0,
            consumer_startup_wait_ms: 0,
            client_id: Some("client-1".to_string()),
            manifest_dir: Some(PathBuf::from("/tmp/manifest")),
            read_manifest_dir: None,
            write_manifest_dir: None,
            file_size_spec: "64KB,1MB,16MB".to_string(),
            file_count: 16,
            duration_secs: 60,
            read_ratio: 70,
            hotset_ratio: 80,
            op_interval_ms: 0,
            warm_written: false,
            post_run_hold_secs: 0,
        }
    }

    #[test]
    fn parse_size_spec_accepts_mixed_units() {
        let parsed = parse_size_spec("4KB, 1MB, 2GB").expect("size spec should parse");
        assert_eq!(parsed, vec![4 * 1024, 1024 * 1024, 2 * 1024 * 1024 * 1024]);
    }

    #[test]
    fn choose_manifest_index_stays_inside_hotset_tail() {
        let mut rng = WorkloadRng::new(7);
        for _ in 0..128 {
            let index = choose_manifest_index(10, 30, &mut rng);
            assert!(index >= 7);
            assert!(index < 10);
        }
    }

    #[test]
    fn manifest_dirs_default_to_manifest_dir() {
        let args = mixed_args();
        let (read_dir, write_dir) =
            resolve_manifest_dirs(&args).expect("manifest dirs should resolve");
        assert_eq!(read_dir, PathBuf::from("/tmp/manifest"));
        assert_eq!(write_dir, PathBuf::from("/tmp/manifest"));
    }

    #[test]
    fn manifest_dirs_allow_split_read_and_write_paths() {
        let mut args = mixed_args();
        args.read_manifest_dir = Some(PathBuf::from("/tmp/read-manifest"));
        args.write_manifest_dir = Some(PathBuf::from("/tmp/write-manifest"));
        let (read_dir, write_dir) =
            resolve_manifest_dirs(&args).expect("manifest dirs should resolve");
        assert_eq!(read_dir, PathBuf::from("/tmp/read-manifest"));
        assert_eq!(write_dir, PathBuf::from("/tmp/write-manifest"));
    }

    #[test]
    fn post_run_hold_defaults_to_zero() {
        let args = Args::parse_from([
            "p2p-proof-driver",
            "--role",
            "mixed",
            "--conf",
            "conf",
            "--master-addrs",
            "127.0.0.1:8995",
            "--remote-path",
            "/tmp",
            "--output-file",
            "out.json",
        ]);
        assert_eq!(args.post_run_hold_secs, 0);
    }

    #[test]
    fn post_run_hold_can_be_configured() {
        let args = Args::parse_from([
            "p2p-proof-driver",
            "--role",
            "mixed",
            "--conf",
            "conf",
            "--master-addrs",
            "127.0.0.1:8995",
            "--remote-path",
            "/tmp",
            "--output-file",
            "out.json",
            "--post-run-hold-secs",
            "12",
        ]);
        assert_eq!(args.post_run_hold_secs, 12);
    }
}
