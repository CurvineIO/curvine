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

//! End-to-end proof that fs_mode UFS replay survives a hard leader kill.
//!
//! Three real master processes form one Raft group and one worker serves the
//! blocks. The fs_mode mount points at `<root>/blocked/ufs`, where
//! `<root>/blocked` is a **regular file**: every UFS side effect fails, so the
//! replayer can only retry and `ufs_applied` never catches `applied`.
//!
//! * The leader keeps `journal_ufs_applied` below `journal_applied`, which proves
//!   the exports are still owed and nothing has reached the disk.
//! * Data is written through a plain `CurvineFileSystem` client, i.e. only
//!   metadata reaches the cluster; UFS effects belong to the replayer alone.
//! * The leader is then hard killed and the UFS is unblocked. The **newly
//!   elected** leader catching `journal_ufs_applied` up to `journal_applied`
//!   proves that leader drained the backlog, not the one that died.
//! * Finally the bytes on the local disk are compared with what was written.
//!
//! `testing = true` is deliberately absent: it disables the background apply
//! worker and the UFS replayer.

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_fs_api::Path as FsPath;
use curvine_model::{MountOptions, WriteType};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MASTERS: usize = 3;

const CV_MOUNT: &str = "/ufs-failover";
const CV_DATA: &str = "/ufs-failover/data";
const ALPHA: &str = "alpha: ufs replay survives a leader kill\n";
const BETA: &str = "beta: renamed while the ufs was blocked\n";
const DOOMED: &str = "doomed: written and deleted before the replay ran\n";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Binds `count` ephemeral ports at the same time so they are guaranteed to be
/// distinct, then hands them back. The sockets are dropped before any child is
/// spawned: a Windows bind is exclusive, so a port held here would make the
/// child fail to start.
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

struct Node {
    name: String,
    web_port: u16,
    child: Child,
    log_path: PathBuf,
}

impl Node {
    fn kill_now(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn exited(&mut self) -> Option<String> {
        match self.child.try_wait() {
            Ok(Some(status)) => Some(format!("{} exited with {}", self.name, status)),
            _ => None,
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill_now();
    }
}

/// TOML basic strings treat `\` as an escape, and opendal's fs backend needs the
/// object path to be absolute without a leading slash, so every Windows path is
/// rendered with forward slashes.
fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// `file://E:/...` (no third slash): `Path::from_str` keeps the whole tail as the
/// object path, which is exactly what opendal's `root("/")` operator expects.
fn ufs_uri(dir: &Path) -> String {
    format!("file://{}", toml_path(dir))
}

fn raft_peers(ports: &[u16]) -> String {
    ports
        .iter()
        .enumerate()
        .map(|(i, port)| {
            format!(
                "{{ id = {}, hostname = \"127.0.0.1\", port = {} }}",
                i + 1,
                port
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn master_addrs(ports: &[u16]) -> String {
    ports
        .iter()
        .map(|port| format!("{{ hostname = \"127.0.0.1\", port = {} }}", port))
        .collect::<Vec<_>>()
        .join(", ")
}

fn master_toml(
    dir: &Path,
    rpc_port: u16,
    web_port: u16,
    raft_port: u16,
    raft_ports: &[u16],
    master_ports: &[u16],
) -> String {
    let meta_dir = toml_path(&dir.join("meta"));
    let journal_dir = toml_path(&dir.join("journal"));
    let log_dir = toml_path(&dir.join("logs"));
    format!(
        r#"# Generated by ufs_replay_failover_test; three of these form one cluster.
format_master = true
cluster_name = "ufs-replay-failover"

[master]
hostname = "127.0.0.1"
rpc_port = {rpc_port}
web_port = {web_port}
meta_dir = "{meta_dir}"
io_threads = 4
worker_threads = 8
log = {{ level = "info", log_dir = "{log_dir}", file_name = "master.log" }}

[journal]
enable = true
hostname = "127.0.0.1"
rpc_port = {raft_port}
journal_dir = "{journal_dir}"
group_name = "ufs-replay-failover"
io_threads = 1
worker_threads = 2
raft_tick_interval_ms = 100
retry_interval_secs = 1
ufs_copy_timeout = "180s"
journal_addrs = [{peers}]

[client]
hostname = "127.0.0.1"
short_circuit = false
master_addrs = [{masters}]
"#,
        peers = raft_peers(raft_ports),
        masters = master_addrs(master_ports),
    )
}

fn worker_toml(dir: &Path, rpc_port: u16, web_port: u16, master_ports: &[u16]) -> String {
    let data_dir = toml_path(&dir.join("data"));
    let log_dir = toml_path(&dir.join("logs"));
    format!(
        r#"# Generated by ufs_replay_failover_test.
format_worker = true
cluster_name = "ufs-replay-failover"

[worker]
hostname = "127.0.0.1"
rpc_port = {rpc_port}
web_port = {web_port}
data_dir = ["[DISK:20GB]{data_dir}"]
log = {{ level = "info", log_dir = "{log_dir}", file_name = "worker.log" }}

[client]
hostname = "127.0.0.1"
short_circuit = false
master_addrs = [{masters}]
"#,
        masters = master_addrs(master_ports),
    )
}

fn spawn_node(name: String, service: &str, conf: &Path, log_path: &Path, web_port: u16) -> Node {
    let out = fs::File::create(log_path).unwrap();
    let err = out.try_clone().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_curvine-server"))
        .args(["--service", service, "--conf", conf.to_str().unwrap()])
        // The generated configuration must win over any hostname exported by
        // the developer shell, otherwise `check_master_hostname` rejects it.
        .env_remove("CURVINE_MASTER_HOSTNAME")
        .env_remove("CURVINE_WORKER_HOSTNAME")
        .env_remove("CURVINE_CLIENT_HOSTNAME")
        .env_remove("CURVINE_TRANSFER_HOSTNAME")
        .env_remove("CURVINE_CONF_FILE")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    Node {
        name,
        web_port,
        child,
        log_path: log_path.to_path_buf(),
    }
}

fn tail(path: &Path, lines: usize) -> String {
    let text = fs::read_to_string(path).unwrap_or_default();
    let all = text.lines().collect::<Vec<_>>();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn dump(nodes: &[Node]) -> String {
    nodes
        .iter()
        .map(|node| {
            format!(
                "--- {} log ({}) ---\n{}",
                node.name,
                node.log_path.display(),
                tail(&node.log_path, 60)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
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
        thread::sleep(Duration::from_millis(100));
    }
    panic!("{what} never listened on 127.0.0.1:{port} within {timeout:?}");
}

/// Minimal HTTP/1.1 GET over a raw socket: the test only needs a status code and
/// a body, and pulling in a client crate for that would widen the dev-deps.
fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(500),
    )
    .ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(20)))
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

fn wait_probe(port: u16, path: &str, want: u16, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    let mut last = String::from("<no response>");
    while Instant::now() < deadline {
        match http_get(port, path) {
            Some((status, body)) => {
                if status == want {
                    return body;
                }
                last = format!("{} {}", status, body.trim());
            }
            None => last = "<no response>".to_string(),
        }
        thread::sleep(Duration::from_millis(300));
    }
    panic!("GET {path} on 127.0.0.1:{port} never reached {want} within {timeout:?}; last: {last}");
}

/// `only` restricts the search to the nodes still alive after the kill.
fn wait_leader(nodes: &mut [Node], only: &[usize], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    let mut last = String::new();
    while Instant::now() < deadline {
        let mut round = String::new();
        for &index in only {
            if let Some(exit) = nodes[index].exited() {
                panic!("{exit}\n{}", dump(nodes));
            }
            let status = http_get(nodes[index].web_port, "/readyz")
                .map(|(status, _)| status)
                .unwrap_or(0);
            if status == 200 {
                return index;
            }
            round.push_str(&format!("{}={} ", nodes[index].name, status));
        }
        last = round;
        thread::sleep(Duration::from_millis(200));
    }
    panic!(
        "no active leader within {timeout:?}; last readyz: {last}\n{}",
        dump(nodes)
    );
}

/// Prometheus text format, e.g. `journal_applied{cluster_id="x"} 42`.
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

/// Wait until the UFS watermark holds at the metadata watermark for several
/// consecutive samples. A fresh leader briefly reports "caught up" right after
/// its catch-up pass while raft is still committing the backlog the previous
/// leader left behind, so one sample can be a false positive.
fn wait_ufs_caught_up(port: u16, min_applied: i64, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    let mut streak = 0;
    let mut last = String::from("<no metrics>");
    while Instant::now() < deadline {
        if let Some((200, body)) = http_get(port, "/metrics") {
            let applied = metric_value(&body, "journal_applied");
            let ufs_applied = metric_value(&body, "journal_ufs_applied");
            if let (Some(applied), Some(ufs_applied)) = (applied, ufs_applied) {
                last = format!("applied={applied} ufs_applied={ufs_applied}");
                if applied >= min_applied && ufs_applied >= applied {
                    streak += 1;
                    if streak >= 4 {
                        return body;
                    }
                } else {
                    streak = 0;
                }
            }
        }
        thread::sleep(Duration::from_millis(500));
    }
    panic!(
        "the UFS watermark did not reach the metadata watermark within {timeout:?} (needs applied >= {min_applied}); last: {last}"
    );
}

/// Wait until the UFS watermark stays behind the metadata watermark for several
/// consecutive samples, i.e. the replayer still owes side effects.
fn wait_ufs_behind(port: u16, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    let mut streak = 0;
    let mut last = String::from("<no metrics>");
    while Instant::now() < deadline {
        if let Some((200, body)) = http_get(port, "/metrics") {
            let applied = metric_value(&body, "journal_applied");
            let ufs_applied = metric_value(&body, "journal_ufs_applied");
            if let (Some(applied), Some(ufs_applied)) = (applied, ufs_applied) {
                last = format!("applied={applied} ufs_applied={ufs_applied}");
                if ufs_applied < applied {
                    streak += 1;
                    if streak >= 3 {
                        return body;
                    }
                } else {
                    streak = 0;
                }
            }
        }
        thread::sleep(Duration::from_millis(500));
    }
    panic!("the UFS replay never fell behind within {timeout:?}; last: {last}");
}

/// Wait until the writes just issued are durable in raft, not merely queued.
///
/// A close RPC only drops its entry into the journal writer's in-memory batch,
/// which is proposed at most `writer_flush_batch_ms` later (10ms by default), so
/// killing the leader right after the write returns loses entries raft never
/// saw. Wait for the writer queue to drain, raft to commit past the pre-write
/// watermark, and apply to catch up — for several samples in a row, so a batch
/// still sitting in its flush window cannot pass the check.
fn wait_writes_committed(port: u16, applied_before: i64, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    let mut streak = 0;
    let mut last = String::from("<no metrics>");
    while Instant::now() < deadline {
        if let Some((200, body)) = http_get(port, "/metrics") {
            let applied = metric_value(&body, "journal_applied");
            let committed = metric_value(&body, "journal_committed");
            let queued = metric_value(&body, "journal_queue_len");
            if let (Some(applied), Some(committed), Some(queued)) = (applied, committed, queued) {
                last = format!("applied={applied} committed={committed} queued={queued}");
                if applied > applied_before && queued == 0 && applied == committed {
                    streak += 1;
                    if streak >= 3 {
                        return body;
                    }
                } else {
                    streak = 0;
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "the writes were not committed within {timeout:?} \
         (needs applied > {applied_before}, journal queue empty, apply caught up); last: {last}"
    );
}

fn cv(path: &str) -> FsPath {
    FsPath::from_str(path).unwrap()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[test]
fn fs_mode_ufs_replay_completes_on_the_new_leader_after_a_leader_kill() {
    // 3 x (master rpc, master web, journal rpc) + worker rpc + worker web.
    let mut pool = PortPool::new(3 * MASTERS + 2);
    let master_rpc = pool.ports[0..MASTERS].to_vec();
    let master_web = pool.ports[MASTERS..2 * MASTERS].to_vec();
    let raft = pool.ports[2 * MASTERS..3 * MASTERS].to_vec();
    let worker_rpc = pool.ports[3 * MASTERS];
    let worker_web = pool.ports[3 * MASTERS + 1];
    // Every port was bound at once, which is what makes them distinct; the
    // sockets have to be closed before the child processes bind them.
    pool.release();

    let root = std::env::temp_dir().join(format!(
        "curvine-ufs-replay-failover-{}-{}",
        std::process::id(),
        raft[0]
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();

    // The mount root sits below a regular file, so the replayer cannot create
    // anything until the test removes that file and creates the directory.
    let blocker = root.join("blocked");
    let ufs_root = blocker.join("ufs");
    let ufs_data = ufs_root.join("data");
    fs::write(&blocker, b"a regular file where a directory is expected").unwrap();
    assert!(!ufs_root.exists());

    // ---- configurations ---------------------------------------------------
    let mut master_confs = Vec::new();
    for index in 0..MASTERS {
        let dir = root.join(format!("master-{index}"));
        fs::create_dir_all(dir.join("logs")).unwrap();
        let conf = dir.join("curvine-cluster.toml");
        fs::write(
            &conf,
            master_toml(
                &dir,
                master_rpc[index],
                master_web[index],
                raft[index],
                &raft,
                &master_rpc,
            ),
        )
        .unwrap();
        master_confs.push(conf);
    }

    let worker_dir = root.join("worker");
    fs::create_dir_all(worker_dir.join("logs")).unwrap();
    let worker_conf = worker_dir.join("curvine-cluster.toml");
    fs::write(
        &worker_conf,
        worker_toml(&worker_dir, worker_rpc, worker_web, &master_rpc),
    )
    .unwrap();

    // ---- three masters, one raft group ------------------------------------
    let mut nodes = master_confs
        .iter()
        .enumerate()
        .map(|(index, conf)| {
            spawn_node(
                format!("master-{index}"),
                "master",
                conf,
                &root.join(format!("master-{index}")).join("logs/master.log"),
                master_web[index],
            )
        })
        .collect::<Vec<_>>();

    // A master only binds the metrics/probe port once its raft role is known.
    for port in master_web.iter() {
        wait_port(*port, "master metrics", Duration::from_secs(120));
    }

    let mut candidate = (0..MASTERS).collect::<Vec<_>>();
    let leader = wait_leader(&mut nodes, &candidate, Duration::from_secs(120));
    println!("leader before the kill: master-{leader}");
    wait_ufs_caught_up(master_web[leader], 0, Duration::from_secs(60));

    // ---- client, then the worker (it needs a reachable master to join) ----
    let client_conf = ClusterConf::from(master_confs[0].to_str().unwrap()).unwrap();
    let rt: Arc<Runtime> = Arc::new(client_conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(client_conf.clone(), rt.clone()).unwrap();

    // One worker is enough: it holds the only replica of every block.
    nodes.push(spawn_node(
        "worker-0".to_string(),
        "worker",
        &worker_conf,
        &worker_dir.join("logs/worker.log"),
        worker_web,
    ));

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let info = rt.block_on(fs.get_filesystem_info()).unwrap();
        let live = info.live_workers.len();
        if live >= 1 {
            println!("worker registered, live_workers={live}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no worker registered within 120s\n{}",
            dump(&nodes)
        );
        thread::sleep(Duration::from_millis(500));
    }

    // ---- mount a local directory as an fs_mode UFS ------------------------
    let ufs_path = FsPath::from_str(ufs_uri(&ufs_root)).unwrap();
    assert_eq!(ufs_path.scheme(), Some("file"));
    assert_eq!(ufs_path.path(), toml_path(&ufs_root));
    let opts = MountOptions::builder()
        .write_type(WriteType::FsMode)
        .build();
    rt.block_on(fs.mount(&ufs_path, &cv(CV_MOUNT), opts))
        .unwrap();
    println!("mounted {} -> {CV_MOUNT}", ufs_path.full_path());

    // A mkdir below the mount root is journaled after the mount entry, so the
    // replayer resolves it to the blocked UFS path and can never finish it.
    assert!(rt.block_on(fs.mkdir(&cv(CV_DATA), true)).unwrap());

    wait_ufs_behind(master_web[leader], Duration::from_secs(120));

    // ---- data written while the replay is blocked -------------------------
    // Snapshot the watermark first: the writes below only reach the journal
    // writer's batch, and the kill must not outrun raft.
    let applied_before = metric_value(
        &wait_probe(master_web[leader], "/metrics", 200, Duration::from_secs(60)),
        "journal_applied",
    )
    .expect("journal_applied");
    rt.block_on(async {
        fs.write_string(&cv("/ufs-failover/data/alpha.txt"), ALPHA)
            .await
            .unwrap();
        fs.write_string(&cv("/ufs-failover/data/beta.txt"), BETA)
            .await
            .unwrap();
        assert!(fs
            .rename(
                &cv("/ufs-failover/data/beta.txt"),
                &cv("/ufs-failover/data/beta-renamed.txt"),
            )
            .await
            .unwrap());
        fs.write_string(&cv("/ufs-failover/data/doomed.txt"), DOOMED)
            .await
            .unwrap();
        fs.delete(&cv("/ufs-failover/data/doomed.txt"), false)
            .await
            .unwrap();
    });

    // The RPCs above may still sit in the journal writer's flush window; raft
    // has to own them before the leader can be killed.
    wait_writes_committed(master_web[leader], applied_before, Duration::from_secs(60));

    // Nothing of the above may have reached the disk: the mount root is still
    // unreachable behind the blocker file.
    assert!(blocker.is_file(), "the UFS must still be blocked");
    assert!(
        !ufs_root.exists(),
        "the leader must not have exported anything while the replay could not run"
    );

    // The backlog is real, not a lucky no-op: the metadata watermark moved on
    // while the UFS watermark stayed behind.
    let metrics = wait_probe(master_web[leader], "/metrics", 200, Duration::from_secs(60));
    let applied = metric_value(&metrics, "journal_applied").expect("journal_applied");
    let ufs_applied = metric_value(&metrics, "journal_ufs_applied").expect("journal_ufs_applied");
    println!("before the kill: applied={applied} ufs_applied={ufs_applied}");
    assert!(
        ufs_applied < applied,
        "the UFS watermark must lag the metadata watermark (applied={applied}, ufs_applied={ufs_applied})"
    );

    // ---- hard kill the leader, then unblock the UFS -----------------------
    nodes[leader].kill_now();
    println!("killed master-{leader}");
    fs::remove_file(&blocker).unwrap();
    fs::create_dir(&blocker).unwrap();
    // Leave `<root>/blocked/ufs` absent so the replayer has to create the mount
    // root itself.

    candidate.retain(|index| *index != leader);
    let new_leader = wait_leader(&mut nodes, &candidate, Duration::from_secs(120));
    println!("leader after the kill: master-{new_leader}");
    assert_ne!(new_leader, leader, "the killed master cannot lead again");

    // The watermark transition on the surviving leader is the contract that
    // matters: this process had to replay the whole backlog itself. A fresh
    // leader also reports "caught up" for a moment right after its catch-up
    // pass while raft is still committing the entries the killed leader left
    // behind, so require the metadata watermark to reach what the old leader
    // had applied and the UFS watermark to hold there, several samples in a
    // row.
    let metrics = wait_ufs_caught_up(master_web[new_leader], applied, Duration::from_secs(300));
    let applied_after = metric_value(&metrics, "journal_applied").expect("journal_applied");
    let ufs_applied = metric_value(&metrics, "journal_ufs_applied").expect("journal_ufs_applied");
    println!("after the replay: applied={applied_after} ufs_applied={ufs_applied}");
    assert!(
        applied_after >= applied,
        "the new leader must have applied every entry the old leader had (old={applied}, new={applied_after})"
    );
    assert!(
        ufs_applied >= applied_after,
        "the new leader must have replayed up to the metadata watermark (applied={applied_after}, ufs_applied={ufs_applied})"
    );

    // ---- the bytes on the local disk --------------------------------------
    assert_eq!(
        fs::read_to_string(ufs_data.join("alpha.txt")).unwrap(),
        ALPHA,
        "alpha.txt must be exported byte for byte"
    );
    assert_eq!(
        fs::read_to_string(ufs_data.join("beta-renamed.txt")).unwrap(),
        BETA,
        "the rename must be replayed on the UFS"
    );
    assert!(
        !ufs_data.join("beta.txt").exists(),
        "the pre-rename name must not survive on the UFS"
    );
    assert!(
        !ufs_data.join("doomed.txt").exists(),
        "the delete must be replayed on the UFS"
    );

    // ---- and the metadata agrees -----------------------------------------
    let mut names = rt.block_on(async {
        fs.list_status(&cv(CV_DATA))
            .await
            .unwrap()
            .into_iter()
            .map(|status| status.name)
            .collect::<Vec<_>>()
    });
    names.sort();
    assert_eq!(names, vec!["alpha.txt", "beta-renamed.txt"]);
    assert_eq!(
        rt.block_on(fs.read_string(&cv("/ufs-failover/data/alpha.txt")))
            .unwrap(),
        ALPHA
    );
    assert_eq!(
        rt.block_on(fs.read_string(&cv("/ufs-failover/data/beta-renamed.txt")))
            .unwrap(),
        BETA
    );

    println!("UFS replay verified on disk at {}", ufs_data.display());

    drop(nodes);
    let _ = fs::remove_dir_all(&root);
}
