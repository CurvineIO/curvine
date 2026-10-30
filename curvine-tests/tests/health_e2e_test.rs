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

//! End-to-end: a broken data directory must become visible, must stop the node
//! from taking writes, and must come back without restarting the worker.
//!
//! This exercises the real chain: probe loop -> health state -> heartbeat ->
//! master worker view -> client allocation.

use curvine_client::file::CurvineFileSystem;
use curvine_error::FsResult;
use curvine_fs_api::{Path, Writer};
use curvine_model::{FaultKind, StorageInfo, WorkerInfo};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_tests::Testing;
use std::path::Path as StdPath;
use std::sync::Arc;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(30);

/// The client plus its runtime: every helper drives the runtime itself so the
/// test thread owns the polling loop.
struct Ctx {
    rt: Arc<AsyncRuntime>,
    fs: CurvineFileSystem,
}

impl Ctx {
    fn write(&self, path: &str) -> FsResult<()> {
        self.rt.block_on(async {
            let mut writer = self.fs.create(&Path::from_str(path)?, true).await?;
            writer.write(Utils::rand_str(4096).as_bytes()).await?;
            writer.complete().await?;
            Ok(())
        })
    }

    /// Master view of live workers: the same data `cv node check` reads.
    fn workers(&self) -> Vec<WorkerInfo> {
        self.rt
            .block_on(async { self.fs.get_filesystem_info().await })
            .map(|info| info.live_workers)
            .unwrap_or_default()
    }
}

#[test]
fn broken_data_dir_is_reported_and_recovers() -> FsResult<()> {
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_worker_conf(|_, conf| {
            // Keep the test fast: the defaults (5s interval, 3 failures) need
            // ~20s per direction.
            conf.worker.health.check_interval = "200ms".to_string();
            conf.worker.health.consecutive_failures = 2;
            conf.worker.health.consecutive_successes = 2;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let worker_web_ports: Vec<u16> = cluster
        .worker_conf
        .iter()
        .map(|conf| conf.worker.web_port)
        .collect();
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = Ctx {
        fs: testing.get_fs(Some(rt.clone()), Some(conf))?,
        rt,
    };

    ctx.write("/health-e2e-before.log")?;

    // Break one node's data directory: what the field EIO looked like to the
    // worker, only deterministic.
    let (broken_worker, dir, park) = detach_data_dir(&ctx)?;
    println!("broken worker {broken_worker} dir {dir}");

    // 1) The node reports itself unable to take writes, with the reason.
    let worker = wait_for_worker(&ctx, &broken_worker, |w| !w.accepting_allocations())?;
    let dir_info = unhealthy_dir(&worker).expect("unhealthy dir must be reported");
    assert_eq!(dir_info.fault_kind, FaultKind::PathMissing);
    assert!(
        dir_info.unhealthy_since_ms > 0,
        "the node must report when it went unhealthy"
    );
    assert!(
        !worker.schedulable(),
        "master must stop routing to an unhealthy node"
    );

    // 2) The fault is queryable by state and by error type: this is what the
    // dashboards and `cv node check` consumers filter on.
    let metrics = wait_for_metrics(&worker_web_ports, "kind=\"path_missing\"")?;
    assert_eq!(
        gauge_value(&metrics, "worker_unhealthy_dirs"),
        Some(1.0),
        "unhealthy dir count must be exported"
    );
    assert_eq!(
        gauge_value(&metrics, "worker_accepting_allocations"),
        Some(0.0),
        "the worker must advertise that it takes no writes"
    );
    assert!(
        metrics.contains("health_transition_total{"),
        "state transitions must be countable"
    );
    assert!(
        metrics.contains("dir_health_state{"),
        "per-disk health state must be exported"
    );

    // 3) The cluster stays writable: the healthy node absorbs the writes.
    write_until_success(&ctx, "/health-e2e-during.log")?;

    // 4) Restoring the directory brings the node back without a restart.
    std::fs::rename(&park, &dir)?;
    let worker = wait_for_worker(&ctx, &broken_worker, |w| w.accepting_allocations())?;
    assert!(
        unhealthy_dir(&worker).is_none(),
        "the recovered node must report a healthy directory"
    );
    ctx.write("/health-e2e-after.log")?;

    Ok(())
}

/// Writes retry until a healthy node is picked: the master can keep routing to
/// a node for up to one heartbeat after it turned unhealthy.
fn write_until_success(ctx: &Ctx, path: &str) -> FsResult<()> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        match ctx.write(path) {
            Ok(()) => return Ok(()),
            Err(e) => {
                // Never blame capacity for a broken disk.
                assert!(
                    !e.to_string().contains("Not enough Disk storage capacity"),
                    "a broken disk must not be reported as insufficient capacity: {e}"
                );
                if Instant::now() >= deadline {
                    return Err(e);
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn wait_for_worker(
    ctx: &Ctx,
    worker_id: &str,
    ready: impl Fn(&WorkerInfo) -> bool,
) -> FsResult<WorkerInfo> {
    let deadline = Instant::now() + TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Some(worker) = ctx
            .workers()
            .into_iter()
            .find(|w| w.rpc_addr() == worker_id)
        {
            if ready(&worker) {
                return Ok(worker);
            }
            last = format!(
                "accepting_allocations={} dirs={:?}",
                worker.accepting_allocations(),
                worker
                    .storage_map
                    .values()
                    .map(|d| (
                        d.dir_path.clone(),
                        d.health_state.as_str_name(),
                        d.fault_kind.as_str_name()
                    ))
                    .collect::<Vec<_>>()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(curvine_error::FsError::common(format!(
        "worker {worker_id} did not reach the expected health state in time: {last}"
    )))
}

/// Scrape each worker's `/metrics` until one reports `needle`.
fn wait_for_metrics(ports: &[u16], needle: &str) -> FsResult<String> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        for port in ports {
            if let Some(text) = scrape(*port) {
                if text.contains(needle) {
                    return Ok(text);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(curvine_error::FsError::common(format!(
                "no worker exported {needle} within the timeout"
            )));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Value of a (possibly labelled) gauge sample, e.g. `worker_unhealthy_dirs{...} 1`.
fn gauge_value(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .filter(|line| line.starts_with(name))
        .find_map(|line| line.split_whitespace().last()?.parse().ok())
}

fn scrape(port: u16) -> Option<String> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

fn unhealthy_dir(worker: &WorkerInfo) -> Option<&StorageInfo> {
    worker.storage_map.values().find(|dir| !dir.is_usable())
}

/// Moves one live worker's data directory aside and returns
/// `(worker_addr, original_dir, parked_dir)`.
fn detach_data_dir(ctx: &Ctx) -> FsResult<(String, String, String)> {
    let worker = ctx
        .workers()
        .into_iter()
        .find(|worker| {
            worker
                .storage_map
                .values()
                .any(|dir| StdPath::new(&dir.dir_path).is_dir())
        })
        .ok_or_else(|| curvine_error::FsError::common("no live worker with a usable data dir"))?;

    let dir = worker
        .storage_map
        .values()
        .map(|dir| dir.dir_path.clone())
        .find(|path| StdPath::new(path).is_dir())
        .ok_or_else(|| curvine_error::FsError::common("worker reported no existing data dir"))?;

    let park = format!("{dir}.health-e2e-parked");
    std::fs::rename(&dir, &park)?;
    Ok((worker.rpc_addr(), dir, park))
}
