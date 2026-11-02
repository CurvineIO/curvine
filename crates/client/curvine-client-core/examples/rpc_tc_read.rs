// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0.
// A transport-neutral Curvine remote-read workload. See scripts/perf/rpc-tc.

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::runtime::RpcRuntime;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn setting(name: &str, default: usize) -> CommonResult<usize> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn cpu_ticks() -> CommonResult<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    // comm can contain spaces and parentheses; fields after its last ')' start
    // with state (field 3). utime and stime are fields 14 and 15.
    let tail = stat.rsplit_once(')').ok_or("invalid /proc/self/stat")?.1;
    let fields: Vec<_> = tail.split_whitespace().collect();
    Ok(fields.get(11).ok_or("missing utime")?.parse::<u64>()?
        + fields.get(12).ok_or("missing stime")?.parse::<u64>()?)
}

fn main() -> CommonResult<()> {
    let source_hash = option_env!("RPC_TC_SOURCE_SHA256").unwrap_or("unverified");
    if source_hash != "unverified"
        && (source_hash.len() != 64 || !source_hash.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err("invalid build-time workload digest".into());
    }
    let conf_path = env::var("RPC_TC_CONF")?;
    let marker_dir = PathBuf::from(env::var("RPC_TC_MEASURE_DIR")?);
    let file_bytes = setting("RPC_TC_FILE_BYTES", 64 * 1024 * 1024)?;
    let chunk_bytes = setting("RPC_TC_CHUNK_BYTES", 1024 * 1024)?;
    let reads = setting("RPC_TC_READS", 256)?;
    let warmup = setting("RPC_TC_WARMUP_READS", 16)?;
    let concurrency = setting("RPC_TC_CONCURRENCY", 1)?;
    if file_bytes == 0
        || chunk_bytes == 0
        || reads == 0
        || concurrency == 0
        || file_bytes % chunk_bytes != 0
        || file_bytes > i64::MAX as usize
    {
        return Err("file and chunk sizes must be positive, file size must be a multiple of chunk size, and reads must be positive".into());
    }
    let ticks = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()?;
    if !ticks.status.success() {
        return Err("getconf CLK_TCK failed".into());
    }
    let ticks_per_second: f64 = std::str::from_utf8(&ticks.stdout)?.trim().parse()?;
    if !ticks_per_second.is_finite() || ticks_per_second <= 0.0 {
        return Err("invalid clock tick rate".into());
    }

    let mut conf = ClusterConf::from(conf_path)?;
    conf.client.short_circuit = false;
    conf.client.enable_read_ahead = false;
    conf.client.read_chunk_size = chunk_bytes;
    conf.client.block_size = file_bytes as i64;
    conf.client.replicas = 1;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(conf, rt.clone())?;
    rt.block_on(async move {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if fs.get_filesystem_info().await.is_ok_and(|info| !info.live_workers.is_empty()) { break; }
            if Instant::now() >= deadline { return Err("worker registration timed out".into()); }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let path = Path::from_str(format!("/rpc-tc-benchmark-{}", std::process::id()))?;
        let mut writer = fs.create(&path, true).await?;
        let pattern: Vec<u8> = (0..chunk_bytes).map(|i| (i % 251) as u8).collect();
        for _ in 0..(file_bytes / chunk_bytes) { writer.write(&pattern).await?; }
        writer.complete().await?;
        let mut readers = Vec::with_capacity(concurrency);
        let chunks = file_bytes / chunk_bytes;
        for _ in 0..concurrency {
            let mut reader = fs.open(&path).await?;
            let mut buf = vec![0; chunk_bytes];
            for index in 0..warmup {
                if index != 0 && index % chunks == 0 { reader.seek(0).await?; }
                let got = reader.read_full(&mut buf).await?;
                if got != chunk_bytes || buf != pattern { return Err("warmup read failed validation".into()); }
            }
            reader.seek(0).await?;
            readers.push((reader, buf));
        }
        fs::write(marker_dir.join("ready"), b"ready\n")?;
        let deadline = Instant::now() + Duration::from_secs(120);
        while !marker_dir.join("go").exists() {
            if Instant::now() >= deadline { return Err("measurement start timed out".into()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let cpu_start = cpu_ticks()?;
        let start = Instant::now();
        let requests = readers.into_iter().map(|(mut reader, mut buf)| {
            let pattern = &pattern;
            async move {
                let mut samples = Vec::with_capacity(reads);
                for index in 0..reads {
                    if index != 0 && index % chunks == 0 { reader.seek(0).await?; }
                    let t = Instant::now();
                    let got = reader.read_full(&mut buf).await?;
                    samples.push(t.elapsed().as_secs_f64() * 1000.0);
                    if got != chunk_bytes || buf.first() != pattern.first() || buf.last() != pattern.last() {
                        return Err("measured read failed validation".into());
                    }
                }
                Ok::<Vec<f64>, curvine_core_error::CommonError>(samples)
            }
        });
        let samples: Vec<f64> = futures::future::try_join_all(requests).await?.into_iter().flatten().collect();
        let elapsed = start.elapsed().as_secs_f64();
        let cpu_seconds = (cpu_ticks()? - cpu_start) as f64 / ticks_per_second;
        let latency = samples.iter().map(|v| format!("{:.9}", v)).collect::<Vec<_>>().join(",");
        let bytes = chunk_bytes.checked_mul(reads).and_then(|n| n.checked_mul(concurrency)).ok_or("byte count overflow")?;
        let output = format!("{{\"bytes\":{},\"elapsed_seconds\":{:.9},\"client_cpu_seconds\":{:.9},\"latency_ms\":[{}],\"workload\":{{\"file_bytes\":{},\"chunk_bytes\":{},\"reads\":{},\"warmup_reads\":{},\"concurrency\":{},\"source_sha256\":\"{}\",\"short_circuit\":false,\"read_ahead\":false}}}}\n", bytes, elapsed, cpu_seconds, latency, file_bytes, chunk_bytes, reads, warmup, concurrency, source_hash);
        fs::write(marker_dir.join("client.json"), output)?;
        Ok::<(), curvine_core_error::CommonError>(())
    })?;
    Ok(())
}
