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

use std::fs;
use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn available_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect::<Vec<_>>();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

fn can_connect(port: u16) -> bool {
    TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(200),
    )
    .is_ok()
}

fn wait_for_listener(child: &mut Child, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if can_connect(port) {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("master exited before Raft listener started: {status}; stderr: {stderr}");
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("Raft listener {port} did not start within 30 seconds");
}

#[test]
fn slow_member_recovery_keeps_raft_liveness_open_while_master_rpc_is_unready() {
    let ports = available_ports(5);
    let root = std::env::temp_dir().join(format!(
        "curvine-master-recovery-liveness-{}-{}",
        std::process::id(),
        ports[0]
    ));
    let meta_dir = root.join("meta");
    let journal_dir = root.join("journal");
    let log_dir = root.join("logs");
    fs::create_dir_all(&root).unwrap();
    fs::create_dir_all(&log_dir).unwrap();

    let master_port = ports[0];
    let raft_port = ports[1];
    let peer2_port = ports[2];
    let peer3_port = ports[3];
    let web_port = ports[4];
    let conf_path = root.join("curvine-cluster.toml");
    let conf = format!(
        r#"
format_master = false
testing = true

[master]
hostname = "127.0.0.1"
rpc_port = {master_port}
web_port = {web_port}
meta_dir = "{}"
log = {{ level = "info", log_dir = "{}", file_name = "master.log" }}

[journal]
enable = true
hostname = "127.0.0.1"
rpc_port = {raft_port}
journal_dir = "{}"
recover_from_peers = 1
io_threads = 1
worker_threads = 2
raft_tick_interval_ms = 100
journal_addrs = [
  {{ id = 1, hostname = "127.0.0.1", port = {raft_port} }},
  {{ id = 2, hostname = "127.0.0.1", port = {peer2_port} }},
  {{ id = 3, hostname = "127.0.0.1", port = {peer3_port} }},
]

[client]
master_addrs = [{{ hostname = "127.0.0.1", port = {master_port} }}]
"#,
        meta_dir.display(),
        log_dir.display(),
        journal_dir.display(),
    );
    fs::write(&conf_path, conf).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_curvine-server"))
        .args(["--service", "master", "--conf", conf_path.to_str().unwrap()])
        .env_remove("CURVINE_MASTER_HOSTNAME")
        .env_remove("CURVINE_JOURNAL_HOSTNAME")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);

    wait_for_listener(&mut child.0, raft_port);
    assert!(
        !can_connect(master_port),
        "Master RPC must remain unready while recovery has no healthy quorum"
    );

    // Model a slow snapshot/catch-up interval: the process must remain alive and
    // the Raft listener used by startup/liveness probes must stay reachable.
    thread::sleep(Duration::from_secs(2));
    assert!(child.0.try_wait().unwrap().is_none());
    assert!(can_connect(raft_port));
    assert!(!can_connect(master_port));

    let manifest = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../curvine-docker/deploy/example/curvine-master.yaml"),
    )
    .unwrap();
    assert!(manifest.contains("readinessProbe:\n            tcpSocket:\n              port: 8995"));
    assert!(manifest.contains("startupProbe:\n            tcpSocket:\n              port: 8996"));
    assert!(manifest.contains("livenessProbe:\n            tcpSocket:\n              port: 8996"));

    drop(child);
    fs::remove_dir_all(root).unwrap();
}
