use bytes::BytesMut;
use clap::{Parser, ValueEnum};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use orpc::common::{Logger, Utils};
use orpc::io::net::InetAddr;
use orpc::runtime::RpcRuntime;
use orpc::{err_box, CommonResult};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

#[derive(Copy, Clone, Eq, PartialEq, ValueEnum)]
enum Role {
    Provider,
    Consumer,
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

async fn read_all(fs: &UnifiedFileSystem, path: &Path) -> CommonResult<Vec<u8>> {
    let status = fs.get_status(path).await?;
    let mut reader = fs.open(path).await?;
    let mut buf = BytesMut::zeroed(status.len as usize);
    let size = reader.read_full(&mut buf).await?;
    reader.complete().await?;
    buf.truncate(size);
    Ok(buf.to_vec())
}

async fn write_all(fs: &UnifiedFileSystem, path: &Path, payload: &[u8]) -> CommonResult<()> {
    let mut writer = fs.create(path, true).await?;
    writer.write(payload).await?;
    writer.complete().await?;
    Ok(())
}

async fn wait_bootstrap_addr(
    service: Arc<curvine_client::p2p::P2pService>,
    wait_secs: u64,
) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(wait_secs.max(1));
    while Instant::now() < deadline {
        if let Some(addr) = service.bootstrap_peer_addr() {
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

async fn run_provider(fs: UnifiedFileSystem, args: &Args) -> CommonResult<()> {
    let input_file = args
        .input_file
        .as_ref()
        .ok_or_else(|| "provider requires --input-file".to_string())?;
    let bootstrap_file = args
        .bootstrap_file
        .as_ref()
        .ok_or_else(|| "provider requires --bootstrap-file".to_string())?;
    let peer_file = args
        .peer_file
        .as_ref()
        .ok_or_else(|| "provider requires --peer-file".to_string())?;
    let p2p_service = fs
        .fs_context()
        .p2p_service()
        .ok_or_else(|| "p2p service is not enabled".to_string())?;

    let parent = Path::from_str(parent_of(&args.remote_path).as_str())?;
    let remote = Path::from_str(&args.remote_path)?;
    let payload = std::fs::read(input_file)?;

    let _ = fs.delete(&parent, true).await;
    fs.mkdir(&parent, true).await?;
    write_all(&fs, &remote, payload.as_slice()).await?;

    let mut warmed = Vec::new();
    for _ in 0..args.warm_reads.max(1) {
        warmed = read_all(&fs, &remote).await?;
    }
    std::fs::write(&args.output_file, warmed)?;

    let bootstrap_addr_raw = wait_bootstrap_addr(p2p_service.clone(), args.bootstrap_wait_secs)
        .await
        .ok_or_else(|| "provider bootstrap address not ready".to_string())?;
    let bootstrap_addr =
        rewrite_bootstrap_host(&bootstrap_addr_raw, args.bootstrap_host.as_deref());
    let peer_id = peer_id_from_bootstrap_addr(&bootstrap_addr)
        .ok_or_else(|| format!("invalid bootstrap address: {}", bootstrap_addr))?
        .to_string();
    std::fs::write(bootstrap_file, bootstrap_addr.as_bytes())?;
    std::fs::write(peer_file, peer_id.as_bytes())?;

    println!("PROVIDER_PEER_ID={}", peer_id);
    println!("PROVIDER_BOOTSTRAP_ADDR={}", bootstrap_addr);
    println!(
        "PROVIDER_CACHE_CHUNKS={}",
        p2p_service.snapshot().cached_chunks_count
    );

    let report_interval = Duration::from_secs(args.report_interval_secs.max(1));
    let deadline = Instant::now() + Duration::from_secs(args.hold_secs.max(1));
    while Instant::now() < deadline {
        fs.cv().sync_p2p_metrics();
        let _ = fs.cv().metrics_report().await;
        sleep(report_interval).await;
    }

    fs.cv().sync_p2p_metrics();
    let _ = fs.cv().metrics_report().await;
    println!("PROVIDER_STATE_OK=1");
    Ok(())
}

async fn run_consumer(fs: UnifiedFileSystem, args: &Args) -> CommonResult<()> {
    let p2p_service = fs
        .fs_context()
        .p2p_service()
        .ok_or_else(|| "p2p service is not enabled".to_string())?;
    let remote = Path::from_str(&args.remote_path)?;
    let read_times = args.consumer_reads.max(1);
    let read_interval = Duration::from_millis(args.consumer_read_interval_ms);
    let before = p2p_service.snapshot();
    let mut payload = Vec::new();
    for index in 0..read_times {
        payload = read_all(&fs, &remote).await?;
        if index + 1 < read_times && !read_interval.is_zero() {
            sleep(read_interval).await;
        }
    }
    std::fs::write(&args.output_file, payload)?;
    let after = p2p_service.snapshot();

    fs.cv().sync_p2p_metrics();
    let _ = fs.cv().metrics_report().await;

    println!(
        "CONSUMER_P2P_RECV_DELTA={}",
        after.bytes_recv.saturating_sub(before.bytes_recv)
    );
    println!(
        "CONSUMER_CACHE_CHUNKS_DELTA={}",
        after
            .cached_chunks_count
            .saturating_sub(before.cached_chunks_count)
    );
    println!("CONSUMER_READS={}", read_times);
    println!("CONSUMER_STATE_OK=1");
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
        };
        if let Err(e) = &res {
            eprintln!("{}", e);
        }
        res
    })
}
