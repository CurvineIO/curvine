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

//! Failpoint crash matrix.
//!
//! For every durable/visible boundary on the metadata write path this test
//! aborts a real master process at that boundary, restarts it, and checks:
//! - the process actually died at the injected point;
//! - the restarted master serves metadata again;
//! - state the crash could not have made durable is absent, state that was
//!   acknowledged is present;
//! - offline journal replay finishes without a fatal error, keeps inode ids
//!   unique, and sees the same committed baseline.
//!
//! Post-startup points are armed through the master's fault HTTP control plane.
//! Startup points (recovery, reset, replay, snapshot install) run before the
//! HTTP control plane exists, so they are armed through
//! `CURVINE_FAULT_CRASH_POINT`, which the process fault runtime loads at first
//! evaluation.
//!
//! Ignored because it spawns, aborts and restarts real processes. Run with:
//!   cargo test -p curvine-server --features fault-injection \
//!     --test failpoint_crash_test -- --ignored --nocapture
//!
//! Set `CURVINE_FAILPOINT_TEST_FILTER=<substring>` (or `=<exact name>`) to
//! iterate over a subset while debugging.

#![cfg(all(target_os = "linux", feature = "fault-injection"))]

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_fault::{FaultController, FaultHttpController, FaultRuleBuilder};
use curvine_fs_api::{Path as FsPath, RpcCode};
use curvine_proto::GetFilesystemInfoRequest;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::raft::storage::{PeerStorage, RocksLogStorage};
use curvine_raft::raft::{RaftClient, RaftNode};
use curvine_rpc::client::RpcClient;
use curvine_rpc::message::{MessageBuilder, INIT_SEQ_ID};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_server::master::journal::{JournalLoader, JournalSystem};
use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TOKEN_ENV: &str = "CURVINE_FAILPOINT_TOKEN";
const TOKEN: &str = "failpoint-crash-matrix";
const BASELINE: &str = "/fp/base";
const CONFIRM: &str = "/fp/confirm";
const TARGET: &str = "/fp/target";
const AFTER_RESTART: &str = "/fp/after-restart";
const NO_SNAPSHOT_ENTRIES: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expected {
    /// The crash happened before the entry could leave this process.
    Absent,
    /// The entry was acknowledged to its sender before the crash.
    Present,
    /// Crash timing leaves the outcome open; only consistency is asserted.
    Either,
}

struct Point {
    name: &'static str,
    expected: Expected,
    snapshot_entries: u64,
}

const POST_STARTUP: &[Point] = &[
    Point {
        name: "master.journal.before_enqueue",
        expected: Expected::Absent,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.journal.after_enqueue",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.journal.before_flush",
        expected: Expected::Absent,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.journal.after_flush",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.apply.before_batch",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.apply.before_watermark",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.apply.after_batch",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.apply.before_ack",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "master.apply.after_ack",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.propose.before_append",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.propose.after_append",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.storage.before_append",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.storage.after_append",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.storage.before_commit",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.storage.after_commit",
        expected: Expected::Either,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.storage.before_apply_dispatch",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.storage.after_apply_dispatch",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.commit.before_apply",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
    Point {
        name: "raft.commit.after_apply",
        expected: Expected::Present,
        snapshot_entries: NO_SNAPSHOT_ENTRIES,
    },
];

const SNAPSHOT: &[Point] = &[
    Point {
        name: "master.snapshot.before_checkpoint",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "master.snapshot.after_checkpoint",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "master.snapshot.create.before",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "master.snapshot.create.after",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
];

/// Follower snapshot creation and leader-side registration need a real quorum:
/// a single-voter leader creates snapshots through the journal writer instead
/// of the follower job, and never reports one to itself.
const MULTI: &[Point] = &[
    Point {
        name: "raft.snapshot.create.before",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.create.after_app_snapshot",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.create.after_log_snapshot",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.create.before_compact",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.create.after_compact",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.register.before",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.register.after",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.register.before_compact",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
    Point {
        name: "raft.snapshot.register.after_compact",
        expected: Expected::Either,
        snapshot_entries: 1,
    },
];

/// Linked and reachable only when a member downloads a snapshot from a peer;
/// the startup snapshot group covers the local install path.
const CATALOG_ONLY: &[&str] = &[
    "raft.snapshot.install.before_fetch",
    "raft.snapshot.install.after_fetch",
    "raft.snapshot.install.before_log_apply",
    "raft.snapshot.install.after_log_apply",
    "raft.snapshot.install.before_app_apply",
    "raft.snapshot.install.after_app_apply",
];

struct StartupPoint {
    name: &'static str,
    with_snapshot: bool,
}

const STARTUP: &[StartupPoint] = &[
    // A single-voter group reports term/vote/commit changes through the light
    // ready path, so an ordinary write does not evaluate set_hard_state. The
    // election Ready at startup does, which is where this point is injected.
    StartupPoint {
        name: "raft.storage.before_hard_state",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.storage.after_hard_state",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.recovery.before_install",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.recovery.no_snapshot.before_reset",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.recovery.no_snapshot.after_reset",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.recovery.before_scan",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.recovery.after_scan",
        with_snapshot: false,
    },
    StartupPoint {
        name: "master.rebuild.before_reset",
        with_snapshot: false,
    },
    StartupPoint {
        name: "master.rebuild.after_reset",
        with_snapshot: false,
    },
    StartupPoint {
        name: "master.replay.before_scan",
        with_snapshot: false,
    },
    StartupPoint {
        name: "master.replay.after_scan",
        with_snapshot: false,
    },
    StartupPoint {
        name: "master.replay.before_entry",
        with_snapshot: false,
    },
    StartupPoint {
        name: "master.replay.after_entry",
        with_snapshot: false,
    },
    StartupPoint {
        name: "raft.recovery.snapshot.before_apply",
        with_snapshot: true,
    },
    StartupPoint {
        name: "raft.recovery.snapshot.after_apply",
        with_snapshot: true,
    },
    StartupPoint {
        name: "master.snapshot.apply.before",
        with_snapshot: true,
    },
    StartupPoint {
        name: "master.snapshot.apply.after",
        with_snapshot: true,
    },
];

struct PortPool {
    held: Vec<TcpListener>,
    ports: Vec<u16>,
}

impl PortPool {
    fn new(count: usize) -> Self {
        let held = (0..count)
            .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
            .collect::<Vec<_>>();
        let ports = held
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();
        Self { held, ports }
    }

    fn release(&mut self) {
        self.held.clear();
    }
}

struct ClusterEnv {
    root: PathBuf,
    conf_path: PathBuf,
    conf: ClusterConf,
    web_port: u16,
    log_path: PathBuf,
}

impl Drop for ClusterEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl ClusterEnv {
    fn new(snapshot_entries: u64) -> Self {
        let mut pool = PortPool::new(3);
        let rpc_port = pool.ports[0];
        let web_port = pool.ports[1];
        let raft_port = pool.ports[2];
        pool.release();

        let root = std::env::temp_dir().join(format!(
            "curvine-failpoint-{}-{rpc_port}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("master/logs")).unwrap();

        let conf_path = root.join("master/curvine-cluster.toml");
        fs::write(
            &conf_path,
            master_toml(&root, rpc_port, web_port, raft_port, snapshot_entries),
        )
        .unwrap();

        let conf = ClusterConf::from(conf_path.to_str().unwrap()).unwrap();
        Self {
            log_path: root.join("master/logs/master.log"),
            root,
            conf_path,
            conf,
            web_port,
        }
    }

    fn spawn(&self, crash_point: Option<&str>, append: bool) -> Node {
        let log = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(&self.log_path)
            .unwrap();
        let err = log.try_clone().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_curvine-server"));
        command
            .args(["--service", "master", "--conf"])
            .arg(&self.conf_path)
            .env_remove("CURVINE_MASTER_HOSTNAME")
            .env_remove("CURVINE_JOURNAL_HOSTNAME")
            .env_remove("CURVINE_WORKER_HOSTNAME")
            .env_remove("CURVINE_CLIENT_HOSTNAME")
            .env_remove("CURVINE_TRANSFER_HOSTNAME")
            .env_remove("CURVINE_CONF_FILE")
            .env(TOKEN_ENV, TOKEN)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err));
        match crash_point {
            Some(point) => {
                command.env("CURVINE_FAULT_CRASH_POINT", point);
            }
            None => {
                command.env_remove("CURVINE_FAULT_CRASH_POINT");
            }
        }
        let child = command.spawn().unwrap();
        Node {
            child,
            web_port: self.web_port,
            log_path: self.log_path.clone(),
        }
    }

    fn offline_check(&self) -> OfflineState {
        let conf = ClusterConf::from(self.conf_path.to_str().unwrap()).unwrap();
        offline_state(&conf)
    }

    fn fatal_logs(&self) -> Vec<String> {
        fatal_logs(&self.log_path)
    }
}

fn offline_state(conf: &ClusterConf) -> OfflineState {
    let js = match JournalSystem::from_conf(conf) {
        Ok(js) => js,
        Err(error) if error.to_string().contains("invalid master data") => {
            println!(
                "note: discarding the invalid derived metadata dir {}",
                conf.db_conf().data_dir
            );
            fs::remove_dir_all(&conf.db_conf().data_dir).unwrap();
            JournalSystem::from_conf(conf).unwrap()
        }
        Err(error) => panic!("offline JournalSystem failed: {error}"),
    };
    let applied = install_snapshot(conf, &js).expect("offline snapshot install/replay");
    let master_fs = js.fs();
    let baseline = master_fs.exists(BASELINE).expect("offline baseline lookup");
    let target = master_fs.exists(TARGET).unwrap_or(false);
    let statuses = master_fs.list_status("/fp").expect("offline /fp listing");
    let mut ids = HashSet::new();
    let mut duplicate = false;
    for status in statuses {
        if !ids.insert(status.id) {
            duplicate = true;
        }
    }
    let hash = master_fs.sum_hash().expect("offline state hash");
    let last_inode_id = master_fs.last_inode_id();
    js.shutdown();
    OfflineState {
        applied,
        baseline,
        target,
        duplicate,
        hash,
        last_inode_id,
    }
}

fn fatal_logs(path: &Path) -> Vec<String> {
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .map(str::trim)
        .filter(|line| {
            [
                "journal replay gap",
                "fatal journal apply",
                "metadata invariant broken",
                "failed to apply journal",
                "journal is compacted to index",
                "no entries are on disk",
                "snapshot application failed",
                "refusing to advance Ready",
                "panicked at",
            ]
            .iter()
            .any(|key| line.contains(key))
        })
        .take(10)
        .map(str::to_string)
        .collect()
}

struct Node {
    child: Child,
    web_port: u16,
    log_path: PathBuf,
}

impl Node {
    fn kill_now(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn wait_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() >= deadline {
                self.kill_now();
                panic!(
                    "process did not crash within {timeout:?}; log tail:\n{}",
                    tail(&self.log_path, 80)
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn assert_crashed_at_failpoint(&mut self, point: &str) {
        let status = self.wait_exit(Duration::from_secs(25));
        assert!(
            !status.success(),
            "point {point} was reached but the process exited successfully"
        );
        let text = fs::read_to_string(&self.log_path).unwrap_or_default();
        assert!(
            text.contains("[curvine-fault] crash action executed"),
            "point {point} did not execute the injected crash; log tail:\n{}",
            tail(&self.log_path, 80)
        );
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct OfflineState {
    applied: u64,
    baseline: bool,
    target: bool,
    duplicate: bool,
    hash: u128,
    last_inode_id: i64,
}

fn master_toml(
    root: &Path,
    rpc_port: u16,
    web_port: u16,
    raft_port: u16,
    snapshot_entries: u64,
) -> String {
    let meta_dir = toml_path(&root.join("master/meta"));
    let journal_dir = toml_path(&root.join("master/journal"));
    let log_dir = toml_path(&root.join("master/logs"));
    format!(
        r#"# Generated by failpoint_crash_test; one single-voter raft group.
format_master = false
cluster_id = "failpoint"

[fault_injection]
enabled = true
auth_token_env = "{TOKEN_ENV}"

[master]
hostname = "127.0.0.1"
rpc_port = {rpc_port}
web_port = {web_port}
meta_dir = "{meta_dir}"
io_threads = 2
worker_threads = 4
heartbeat_interval = "500ms"
log = {{ level = "info", log_dir = "{log_dir}", file_name = "master.log" }}

[journal]
enable = true
hostname = "127.0.0.1"
rpc_port = {raft_port}
journal_dir = "{journal_dir}"
group_name = "failpoint"
io_threads = 1
worker_threads = 2
raft_tick_interval_ms = 100
raft_heartbeat_tick = 1
raft_election_tick = 10
retry_interval_secs = 1
snapshot_entries = {snapshot_entries}
snapshot_min_interval = "0ms"
journal_addrs = [{{ id = 1, hostname = "127.0.0.1", port = {raft_port} }}]

[client]
hostname = "127.0.0.1"
short_circuit = false
conn_retry_max_duration_ms = 0
conn_timeout_ms = 300
rpc_retry_max_duration_ms = 0
rpc_retry_min_sleep_ms = 10
rpc_retry_max_sleep_ms = 50
rpc_timeout_ms = 2000
master_addrs = [{{ hostname = "127.0.0.1", port = {rpc_port} }}]
"#
    )
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn tail(path: &Path, lines: usize) -> String {
    let text = fs::read_to_string(path).unwrap_or_default();
    let all = text.lines().collect::<Vec<_>>();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn can_connect(port: u16) -> bool {
    TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(300),
    )
    .is_ok()
}

fn wait_port(port: u16, what: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if can_connect(port) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{what} never listened on 127.0.0.1:{port} within {timeout:?}");
}

fn wait_node_web(node: &mut Node, what: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = node.child.try_wait().unwrap() {
            panic!(
                "{what} exited with {status} before listening; log tail:\n{}",
                tail(&node.log_path, 120)
            );
        }
        if can_connect(node.web_port) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{what} never listened within {timeout:?}; log tail:\n{}",
        tail(&node.log_path, 120)
    );
}

fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(500),
    )
    .ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text.split_whitespace().nth(1)?.parse::<u16>().ok()?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Some((status, body))
}

fn metric_value(body: &str, name: &str) -> Option<i64> {
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((head, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let key = head.split('{').next().unwrap_or(head).trim();
        if key == name {
            return value.trim().parse().ok();
        }
    }
    None
}

fn watermarks(port: u16) -> Option<(i64, i64)> {
    let (status, body) = http_get(port, "/metrics")?;
    if status != 200 {
        return None;
    }
    Some((
        metric_value(&body, "journal_applied")?,
        metric_value(&body, "journal_committed")?,
    ))
}

async fn leader_ready(conf: &ClusterConf) -> bool {
    let Ok(rpc) = RpcClient::with_raw(&conf.client.master_addrs[0], &conf.client_rpc_conf()).await
    else {
        return false;
    };
    let request = MessageBuilder::new_rpc(RpcCode::GetFilesystemInfo)
        .seq_id(INIT_SEQ_ID)
        .proto_header(GetFilesystemInfoRequest::default())
        .build();
    matches!(
        rpc.timeout_rpc(Duration::from_secs(2), request).await,
        Ok(response) if response.is_success()
    )
}

async fn wait_leader(env: &ClusterEnv, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if leader_ready(&env.conf).await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "master never became leader; log tail:\n{}",
            tail(&env.log_path, 80)
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn wait_ready(env: &ClusterEnv) {
    wait_port(env.web_port, "master web", Duration::from_secs(60));
    wait_leader(env, Duration::from_secs(60)).await;
}

async fn wait_applied(env: &ClusterEnv, above: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if watermarks(env.web_port).is_some_and(|(applied, _)| applied > above) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "applied watermark never passed {above}; log tail:\n{}",
            tail(&env.log_path, 80)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn new_fs(env: &ClusterEnv, rt: &Arc<Runtime>) -> CurvineFileSystem {
    CurvineFileSystem::with_rt(env.conf.clone(), rt.clone()).unwrap()
}

async fn retry_mkdir(fs: &CurvineFileSystem, path: &str) {
    let path = FsPath::from_str(path).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match fs.mkdir(&path, true).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                println!("mkdir retry: {error}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("mkdir failed: {error}"),
        }
    }
}

async fn prepare_baseline(env: &ClusterEnv, rt: &Arc<Runtime>) -> CurvineFileSystem {
    wait_ready(env).await;
    let fs = new_fs(env, rt);
    let before = watermarks(env.web_port)
        .map(|(applied, _)| applied)
        .unwrap_or(0);
    retry_mkdir(&fs, BASELINE).await;
    // The handler returns before its journal entry is committed. A later entry
    // can only be applied after the baseline, so wait until the confirm write
    // advances the watermark: that proves the baseline is committed before we
    // kill the process.
    tokio::time::sleep(Duration::from_millis(50)).await;
    retry_mkdir(&fs, CONFIRM).await;
    wait_applied(env, before + 2, Duration::from_secs(10)).await;
    let baseline = FsPath::from_str(BASELINE).unwrap();
    assert!(
        fs.get_status(&baseline).await.is_ok(),
        "baseline is not visible before the injected crash"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    fs
}

fn wait_log_contains(path: &Path, needle: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if fs::read_to_string(path)
            .unwrap_or_default()
            .contains(needle)
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

async fn configure_crash_at(web_port: u16, point: &str) {
    let controller =
        FaultHttpController::new(format!("http://127.0.0.1:{web_port}"), TOKEN).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match controller.status().await {
            Ok(status) if status.points.iter().any(|view| view.name == point) => break,
            Ok(_) if Instant::now() < deadline => {}
            Ok(_) => panic!("fault point {point} is not linked into the master binary"),
            Err(error) if Instant::now() < deadline => {
                println!("fault control not ready: {error}");
            }
            Err(error) => panic!("fault control never became ready: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    controller
        .preflight_clean()
        .await
        .expect("a fresh master must start without fault rules");
    let rule = FaultRuleBuilder::named(point)
        .times(1)
        .unwrap()
        .crash()
        .unwrap();
    controller.configure("crash-matrix", rule).await.unwrap();
}

async fn configure_crash(env: &ClusterEnv, point: &str) {
    configure_crash_at(env.web_port, point).await;
}

async fn crash_on_target(env: &ClusterEnv, fs: &CurvineFileSystem, point: &str) {
    configure_crash(env, point).await;
    let target = FsPath::from_str(TARGET).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), fs.mkdir(&target, true)).await;
}

async fn verify_after_restart(
    env: &ClusterEnv,
    rt: &Arc<Runtime>,
    expected: Expected,
    point: &str,
) {
    wait_ready(env).await;
    let fs = new_fs(env, rt);
    let baseline = FsPath::from_str(BASELINE).unwrap();
    let target = FsPath::from_str(TARGET).unwrap();
    assert!(
        fs.get_status(&baseline).await.is_ok(),
        "{point}: committed baseline disappeared after restart"
    );
    retry_mkdir(&fs, AFTER_RESTART).await;

    let found = match fs.get_status(&target).await {
        Ok(_) => true,
        Err(error) if error.to_string().contains("not found") => false,
        Err(error) => panic!("{point}: target lookup failed after restart: {error}"),
    };
    match expected {
        Expected::Absent => assert!(
            !found,
            "{point}: the entry was never handed to the journal but survived the crash"
        ),
        Expected::Present => assert!(
            found,
            "{point}: an acknowledged entry was lost after restart"
        ),
        Expected::Either => {}
    }
    println!("{point}: target_present_after_restart={found}");
}

fn check_offline(env: &ClusterEnv, point: &str, failed: &mut Vec<String>) {
    let fatal = env.fatal_logs();
    if !fatal.is_empty() {
        failed.push(format!("{point}: fatal master log lines: {fatal:?}"));
        return;
    }
    let state = env.offline_check();
    if !state.baseline {
        failed.push(format!(
            "{point}: offline replay lost the committed baseline (applied={}, hash={:x})",
            state.applied, state.hash
        ));
    }
    if state.duplicate {
        failed.push(format!(
            "{point}: offline replay produced duplicate inode ids (applied={}, hash={:x}, last_inode_id={})",
            state.applied, state.hash, state.last_inode_id
        ));
    }
    println!(
        "{point}: offline applied={} hash={:x} baseline={} target={} last_inode_id={}",
        state.applied, state.hash, state.baseline, state.target, state.last_inode_id
    );
}

fn selected(name: &str) -> bool {
    std::env::var("CURVINE_FAILPOINT_TEST_FILTER")
        .map(|filter| {
            if filter.is_empty() {
                true
            } else if let Some(exact) = filter.strip_prefix('=') {
                name == exact
            } else {
                name.contains(&filter)
            }
        })
        .unwrap_or(true)
}

fn install_snapshot(conf: &ClusterConf, js: &JournalSystem) -> CommonResult<u64> {
    let rt = conf.journal.create_runtime();
    let id = conf.journal.node_id()?;
    let client = RaftClient::from_conf(rt.clone(), &conf.journal);
    let storage = PeerStorage::new(
        rt.clone(),
        id,
        js.log_store(),
        js.journal_loader(),
        client,
        &conf.journal,
    );
    let applied = rt.block_on(
        RaftNode::<RocksLogStorage, JournalLoader>::install_snapshot(&storage, vec![1]),
    )?;
    Ok(applied)
}

struct MultiClusterEnv {
    root: PathBuf,
    conf_paths: Vec<PathBuf>,
    confs: Vec<ClusterConf>,
    web_ports: Vec<u16>,
    log_paths: Vec<PathBuf>,
}

impl Drop for MultiClusterEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl MultiClusterEnv {
    fn new(snapshot_entries: u64) -> Self {
        let mut pool = PortPool::new(9);
        let rpc_ports = pool.ports[0..3].to_vec();
        let web_ports = pool.ports[3..6].to_vec();
        let raft_ports = pool.ports[6..9].to_vec();
        pool.release();

        let root = std::env::temp_dir().join(format!(
            "curvine-failpoint-multi-{}-{}",
            std::process::id(),
            rpc_ports[0]
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();

        let mut conf_paths = Vec::new();
        let mut confs = Vec::new();
        let mut log_paths = Vec::new();
        for index in 0..3 {
            let dir = root.join(format!("master-{index}"));
            fs::create_dir_all(dir.join("logs")).unwrap();
            let conf_path = dir.join("curvine-cluster.toml");
            fs::write(
                &conf_path,
                multi_master_toml(
                    &dir,
                    index,
                    &rpc_ports,
                    &web_ports,
                    &raft_ports,
                    snapshot_entries,
                ),
            )
            .unwrap();
            confs.push(ClusterConf::from(conf_path.to_str().unwrap()).unwrap());
            log_paths.push(dir.join("logs/master.log"));
            conf_paths.push(conf_path);
        }
        Self {
            root,
            conf_paths,
            confs,
            web_ports,
            log_paths,
        }
    }

    fn spawn(&self, index: usize, crash_point: Option<&str>, append: bool) -> Node {
        let log_path = &self.log_paths[index];
        let log = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(log_path)
            .unwrap();
        let err = log.try_clone().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_curvine-server"));
        command
            .args(["--service", "master", "--conf"])
            .arg(&self.conf_paths[index])
            .env_remove("CURVINE_MASTER_HOSTNAME")
            .env_remove("CURVINE_JOURNAL_HOSTNAME")
            .env_remove("CURVINE_WORKER_HOSTNAME")
            .env_remove("CURVINE_CLIENT_HOSTNAME")
            .env_remove("CURVINE_TRANSFER_HOSTNAME")
            .env_remove("CURVINE_CONF_FILE")
            .env(TOKEN_ENV, TOKEN)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err));
        match crash_point {
            Some(point) => {
                command.env("CURVINE_FAULT_CRASH_POINT", point);
            }
            None => {
                command.env_remove("CURVINE_FAULT_CRASH_POINT");
            }
        }
        Node {
            child: command.spawn().unwrap(),
            web_port: self.web_ports[index],
            log_path: log_path.clone(),
        }
    }

    fn stop_all(&self, nodes: &mut [Node]) {
        for node in nodes.iter_mut() {
            node.kill_now();
        }
    }
}

fn multi_master_toml(
    dir: &Path,
    index: usize,
    rpc_ports: &[u16],
    web_ports: &[u16],
    raft_ports: &[u16],
    snapshot_entries: u64,
) -> String {
    let meta_dir = toml_path(&dir.join("meta"));
    let journal_dir = toml_path(&dir.join("journal"));
    let log_dir = toml_path(&dir.join("logs"));
    let peers = raft_ports
        .iter()
        .enumerate()
        .map(|(id, port)| {
            format!(
                "{{ id = {}, hostname = \"127.0.0.1\", port = {port} }}",
                id + 1
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let masters = rpc_ports
        .iter()
        .map(|port| format!("{{ hostname = \"127.0.0.1\", port = {port} }}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"# Generated by failpoint_crash_test; three voters.
format_master = false
cluster_id = "failpoint-multi"

[fault_injection]
enabled = true
auth_token_env = "{TOKEN_ENV}"

[master]
hostname = "127.0.0.1"
rpc_port = {rpc_port}
web_port = {web_port}
meta_dir = "{meta_dir}"
io_threads = 2
worker_threads = 4
heartbeat_interval = "500ms"
log = {{ level = "info", log_dir = "{log_dir}", file_name = "master.log" }}

[journal]
enable = true
hostname = "127.0.0.1"
rpc_port = {raft_port}
journal_dir = "{journal_dir}"
group_name = "failpoint-multi"
io_threads = 1
worker_threads = 2
raft_tick_interval_ms = 100
raft_heartbeat_tick = 1
raft_election_tick = 10
retry_interval_secs = 1
snapshot_entries = {snapshot_entries}
snapshot_min_interval = "0ms"
snapshot_interval = "3600s"
leader_create_snapshot = false
# A leader may register a snapshot whose files live on a peer's checkpoint
# directory. The default retention (3) can purge that directory before the
# leader restarts, which strands the leader; keep test checkpoints longer so
# this matrix only measures crash-boundary behavior.
retain_checkpoint_num = 100
journal_addrs = [{peers}]

[client]
hostname = "127.0.0.1"
short_circuit = false
conn_retry_max_duration_ms = 0
conn_timeout_ms = 300
rpc_retry_max_duration_ms = 0
rpc_retry_min_sleep_ms = 10
rpc_retry_max_sleep_ms = 50
rpc_timeout_ms = 3000
master_addrs = [{masters}]
"#,
        rpc_port = rpc_ports[index],
        web_port = web_ports[index],
        raft_port = raft_ports[index],
    )
}

async fn find_leader_index(confs: &[ClusterConf]) -> Option<usize> {
    for (index, conf) in confs.iter().enumerate() {
        let Ok(rpc) =
            RpcClient::with_raw(&conf.client.master_addrs[index], &conf.client_rpc_conf()).await
        else {
            continue;
        };
        let request = MessageBuilder::new_rpc(RpcCode::GetFilesystemInfo)
            .seq_id(INIT_SEQ_ID)
            .proto_header(GetFilesystemInfoRequest::default())
            .build();
        if let Ok(response) = rpc.timeout_rpc(Duration::from_secs(2), request).await {
            if response.is_success() {
                return Some(index);
            }
        }
    }
    None
}

async fn wait_multi_leader(env: &MultiClusterEnv, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(index) = find_leader_index(&env.confs).await {
            return index;
        }
        assert!(
            Instant::now() < deadline,
            "three-node cluster never elected a leader"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn wait_converged(web_ports: &[u16], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut streak = 0;
    let mut last = String::from("<none>");
    while Instant::now() < deadline {
        let samples: Vec<_> = web_ports.iter().map(|port| watermarks(*port)).collect();
        if samples.iter().all(|sample| sample.is_some()) {
            let applied: Vec<i64> = samples.iter().map(|sample| sample.unwrap().0).collect();
            last = format!("{applied:?}");
            if applied.windows(2).all(|window| window[0] == window[1]) {
                streak += 1;
                if streak >= 4 {
                    return;
                }
            } else {
                streak = 0;
            }
        } else {
            streak = 0;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!("replicas did not converge; last applied indexes: {last}");
}

async fn prepare_multi_baseline(env: &MultiClusterEnv, rt: &Arc<Runtime>) -> CurvineFileSystem {
    for port in &env.web_ports {
        wait_port(*port, "master web", Duration::from_secs(60));
    }
    let leader = wait_multi_leader(env, Duration::from_secs(60)).await;
    let fs = CurvineFileSystem::with_rt(env.confs[0].clone(), rt.clone()).unwrap();
    let before = watermarks(env.web_ports[leader])
        .map(|(applied, _)| applied)
        .unwrap_or(0);
    retry_mkdir(&fs, BASELINE).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while watermarks(env.web_ports[leader]).map(|(applied, _)| applied) <= Some(before) {
        assert!(
            Instant::now() < deadline,
            "baseline was never applied by the leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_converged(&env.web_ports, Duration::from_secs(90)).await;
    // A follower can skip snapshot creation when the baseline entries arrive in
    // a Ready that also carries a soft-state change. A few spaced warmup writes
    // give it later Readys to cross the entry threshold.
    for index in 0..3 {
        let path = FsPath::from_str(&format!("/fp/warmup-{index}")).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), fs.mkdir(&path, true)).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let _ = wait_log_contains(
        &env.log_paths[leader],
        "register snapshot",
        Duration::from_secs(10),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    fs
}

async fn multi_trigger(fs: &CurvineFileSystem, web_ports: &[u16]) {
    // Several spaced writes: a follower that is still generating its previous
    // snapshot skips one opportunity, then catches the next entry that pushes
    // the op-id delta past `snapshot_entries`.
    for index in 0..6 {
        let path = FsPath::from_str(&format!("/fp/trigger-{index}")).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), fs.mkdir(&path, true)).await;
        println!("trigger {index}: {result:?}");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    for (index, port) in web_ports.iter().enumerate() {
        println!(
            "node {index} watermark after trigger: {:?}",
            watermarks(*port)
        );
    }
}

async fn verify_multi_after_restart(
    env: &MultiClusterEnv,
    rt: &Arc<Runtime>,
    target: usize,
    point: &str,
    node: &mut Node,
) {
    let _ = target;
    wait_node_web(node, "restarted master web", Duration::from_secs(60));
    wait_multi_leader(env, Duration::from_secs(60)).await;
    wait_converged(&env.web_ports, Duration::from_secs(90)).await;
    let fs = CurvineFileSystem::with_rt(env.confs[0].clone(), rt.clone()).unwrap();
    let baseline = FsPath::from_str(BASELINE).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match fs.get_status(&baseline).await {
            Ok(_) => break,
            Err(error) if Instant::now() < deadline => {
                println!("{point}: baseline lookup retry: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("{point}: baseline lost after restart: {error}"),
        }
    }
    retry_mkdir(&fs, AFTER_RESTART).await;
}

#[test]
#[ignore = "crash matrix: aborts a real master at every failpoint"]
fn failpoint_catalog_links_every_matrix_point() {
    let env = ClusterEnv::new(NO_SNAPSHOT_ENTRIES);
    let mut node = env.spawn(None, false);
    wait_port(env.web_port, "master web", Duration::from_secs(60));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let names = rt.block_on(async {
        let controller =
            FaultHttpController::new(format!("http://127.0.0.1:{}", env.web_port), TOKEN).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match controller.status().await {
                Ok(status) if !status.points.is_empty() => break status.points,
                Ok(_) if Instant::now() < deadline => {}
                Err(_) if Instant::now() < deadline => {}
                Ok(_) => panic!("fault catalog stayed empty"),
                Err(error) => panic!("fault control never became ready: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    node.kill_now();

    let linked = names
        .into_iter()
        .map(|view| view.name)
        .collect::<HashSet<_>>();
    let expected = POST_STARTUP
        .iter()
        .map(|point| point.name)
        .chain(SNAPSHOT.iter().map(|point| point.name))
        .chain(MULTI.iter().map(|point| point.name))
        .chain(STARTUP.iter().map(|point| point.name))
        .chain(CATALOG_ONLY.iter().copied())
        .collect::<Vec<_>>();
    let missing = expected
        .iter()
        .filter(|name| !linked.contains(**name))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "failpoints missing from the linked catalog: {missing:?}"
    );
    println!("failpoint catalog: {} points linked", linked.len());
}

#[test]
#[ignore = "crash matrix: aborts a real master at every post-startup failpoint"]
fn failpoint_crash_matrix_post_startup() {
    let rt = Arc::new(Runtime::new("failpoint-post-startup", 2, 4));
    let mut failed = Vec::new();

    for point in POST_STARTUP.iter().filter(|point| selected(point.name)) {
        println!("\n=== {} ({:?}) ===", point.name, point.expected);
        let env = ClusterEnv::new(point.snapshot_entries);

        let mut node = env.spawn(None, false);
        rt.block_on(async {
            let fs = prepare_baseline(&env, &rt).await;
            crash_on_target(&env, &fs, point.name).await;
        });
        node.assert_crashed_at_failpoint(point.name);

        let mut node = env.spawn(None, true);
        let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(verify_after_restart(&env, &rt, point.expected, point.name));
        }));
        if verified.is_err() {
            failed.push(format!("{}: post-restart verification failed", point.name));
        }
        node.kill_now();
        if verified.is_ok() {
            check_offline(&env, point.name, &mut failed);
        }
    }

    assert!(
        failed.is_empty(),
        "failpoint crash matrix failures:\n{}",
        failed.join("\n")
    );
}

#[test]
#[ignore = "crash matrix: aborts a real master while it creates/installs snapshots"]
fn failpoint_crash_matrix_snapshots() {
    let rt = Arc::new(Runtime::new("failpoint-snapshots", 2, 4));
    let mut failed = Vec::new();

    for point in SNAPSHOT.iter().filter(|point| selected(point.name)) {
        println!("\n=== {} ({:?}) ===", point.name, point.expected);
        let env = ClusterEnv::new(point.snapshot_entries);

        let mut node = env.spawn(None, false);
        rt.block_on(async {
            let fs = prepare_baseline(&env, &rt).await;
            // The baseline write itself triggers one snapshot; let it settle so
            // the armed rule fires on the target write instead of on stale work.
            crash_on_target(&env, &fs, point.name).await;
        });
        node.assert_crashed_at_failpoint(point.name);

        let mut node = env.spawn(None, true);
        let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(verify_after_restart(&env, &rt, point.expected, point.name));
        }));
        if verified.is_err() {
            failed.push(format!("{}: post-restart verification failed", point.name));
        }
        node.kill_now();
        if verified.is_ok() {
            check_offline(&env, point.name, &mut failed);
        }
    }

    assert!(
        failed.is_empty(),
        "snapshot crash matrix failures:\n{}",
        failed.join("\n")
    );
}

#[test]
#[ignore = "crash matrix: aborts a real master during startup recovery"]
fn failpoint_crash_matrix_startup() {
    let rt = Arc::new(Runtime::new("failpoint-startup", 2, 4));
    let mut failed = Vec::new();

    for point in STARTUP.iter().filter(|point| selected(point.name)) {
        println!(
            "\n=== {} (snapshot={}) ===",
            point.name, point.with_snapshot
        );
        let entries = if point.with_snapshot {
            1
        } else {
            NO_SNAPSHOT_ENTRIES
        };
        let env = ClusterEnv::new(entries);

        // Prepare a durable baseline plus, for the install group, a registered
        // snapshot on disk. Then stop without a clean shutdown.
        let mut node = env.spawn(None, false);
        rt.block_on(async {
            prepare_baseline(&env, &rt).await;
        });
        if point.with_snapshot {
            assert!(
                wait_log_contains(
                    &env.log_path,
                    "create leader snapshot, dir=",
                    Duration::from_secs(20)
                ),
                "{}: no snapshot was persisted before the restart; log tail:\n{}",
                point.name,
                tail(&env.log_path, 80)
            );
            std::thread::sleep(Duration::from_millis(300));
        }
        node.kill_now();

        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut node = env.spawn(Some(point.name), true);
            node.assert_crashed_at_failpoint(point.name);
        }));
        if let Err(payload) = crashed {
            let reason = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<panic>".to_string());
            failed.push(format!(
                "{}: startup crash injection failed: {reason}",
                point.name
            ));
            continue;
        }

        let mut node = env.spawn(None, true);
        let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async {
                wait_ready(&env).await;
                let fs = new_fs(&env, &rt);
                let baseline = FsPath::from_str(BASELINE).unwrap();
                assert!(
                    fs.get_status(&baseline).await.is_ok(),
                    "{}: baseline was lost after startup recovery",
                    point.name
                );
                retry_mkdir(&fs, AFTER_RESTART).await;
            });
        }));
        if verified.is_err() {
            failed.push(format!("{}: post-startup recovery failed", point.name));
        }
        node.kill_now();
        if verified.is_ok() {
            check_offline(&env, point.name, &mut failed);
        }
    }

    assert!(
        failed.is_empty(),
        "startup crash matrix failures:\n{}",
        failed.join("\n")
    );
}

#[test]
#[ignore = "crash matrix: three-master follower snapshot creation and leader registration"]
fn failpoint_crash_matrix_multi_node_snapshots() {
    let rt = Arc::new(Runtime::new("failpoint-multi-snapshots", 2, 4));
    let mut failed = Vec::new();

    for point in MULTI.iter().filter(|point| selected(point.name)) {
        println!("\n=== {} ({:?}) ===", point.name, point.expected);
        let env = MultiClusterEnv::new(point.snapshot_entries);
        let mut nodes: Vec<Node> = (0..3).map(|index| env.spawn(index, None, false)).collect();

        let fs = rt.block_on(async { prepare_multi_baseline(&env, &rt).await });
        let leader = rt.block_on(async { wait_multi_leader(&env, Duration::from_secs(30)).await });
        let target = if point.name.starts_with("raft.snapshot.create.") {
            (leader + 1) % 3
        } else {
            leader
        };
        println!("leader={leader} target={target}");

        if point.name.starts_with("raft.snapshot.create.") {
            // Best effort: the spaced trigger writes also produce opportunities
            // if the baseline snapshot job was skipped or still running.
            let _ = wait_log_contains(
                &env.log_paths[target],
                "report snapshot",
                Duration::from_secs(5),
            );
            std::thread::sleep(Duration::from_millis(300));
        }

        rt.block_on(async {
            configure_crash_at(env.web_ports[target], point.name).await;
            multi_trigger(&fs, &env.web_ports).await;
        });

        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            nodes[target].assert_crashed_at_failpoint(point.name);
        }));
        if let Err(payload) = crashed {
            let reason = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<panic>".to_string());
            failed.push(format!("{}: crash injection failed: {reason}", point.name));
            env.stop_all(&mut nodes);
            continue;
        }

        nodes[target] = env.spawn(target, None, true);
        let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let node = &mut nodes[target];
            rt.block_on(verify_multi_after_restart(
                &env, &rt, target, point.name, node,
            ));
        }));
        if verified.is_err() {
            failed.push(format!("{}: post-restart verification failed", point.name));
        }
        env.stop_all(&mut nodes);

        if verified.is_ok() {
            for (index, path) in env.log_paths.iter().enumerate() {
                let fatal = fatal_logs(path);
                if !fatal.is_empty() {
                    failed.push(format!(
                        "{}: node {index} logged fatal lines: {fatal:?}",
                        point.name
                    ));
                }
            }
        }
    }

    assert!(
        failed.is_empty(),
        "multi-node snapshot crash matrix failures:\n{}",
        failed.join("\n")
    );
}
