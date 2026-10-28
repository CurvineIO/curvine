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

//! `DownloadJob` runs over real TCP against an in-process snapshot owner. The
//! owner gates every `Open` request, so the observed concurrency is
//! deterministic instead of timing dependent.

use curvine_net::net::ConnState;
use curvine_raft::conf::JournalConf;
use curvine_raft::proto::raft::{SnapshotDownloadRequest, SnapshotFileInfo, SnapshotFileList};
use curvine_raft::raft::snapshot::{DownloadJob, SnapshotDownloadHandler};
use curvine_raft::raft::{RaftClient, RaftError, RaftPeer};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const CHUNK_SIZE: usize = 4096;
const GATE_TIMEOUT: Duration = Duration::from_secs(10);
const START_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct GateState {
    open: usize,
    max_open: usize,
    released: bool,
}

/// Blocks every owner `Open` until the test releases the gate and records how
/// many connections were transferring at the same time.
struct Gate {
    state: Mutex<GateState>,
    cv: Condvar,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState::default()),
            cv: Condvar::new(),
        })
    }

    fn enter(&self) {
        let mut state = self.state.lock().unwrap();
        state.open += 1;
        state.max_open = state.max_open.max(state.open);

        let deadline = Instant::now() + GATE_TIMEOUT;
        while !state.released {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let (guard, _) = self.cv.wait_timeout(state, left).unwrap();
            state = guard;
        }
        state.open -= 1;
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.cv.notify_all();
    }

    fn max_open(&self) -> usize {
        self.state.lock().unwrap().max_open
    }
}

/// One handler per connection, exactly like `RaftService`: the owner keeps a
/// single reader per connection.
struct OwnerService {
    gate: Arc<Gate>,
    fail_path: Option<String>,
}

impl HandlerService for OwnerService {
    type Item = OwnerHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        OwnerHandler {
            download: Mutex::new(SnapshotDownloadHandler::new(CHUNK_SIZE)),
            gate: self.gate.clone(),
            fail_path: self.fail_path.clone(),
        }
    }
}

struct OwnerHandler {
    download: Mutex<SnapshotDownloadHandler>,
    gate: Arc<Gate>,
    fail_path: Option<String>,
}

impl MessageHandler for OwnerHandler {
    type Error = RaftError;

    fn handle(&self, msg: &Message) -> Result<Message, RaftError> {
        if msg.request_status() == RequestStatus::Open {
            self.gate.enter();

            if let Some(fail) = &self.fail_path {
                let req: SnapshotDownloadRequest = msg.parse_header()?;
                if req.snapshot_file.path == *fail {
                    let err = RaftError::from(format!("injected transfer failure: {fail}"));
                    return Ok(msg.error_ext(&err));
                }
            }
        }

        let mut handler = self.download.lock().unwrap();
        handler.handle(msg)
    }
}

fn start_owner(gate: Arc<Gate>, fail_path: Option<String>) -> JournalConf {
    let mut conf = JournalConf::with_test();
    conf.hostname = "127.0.0.1".to_string();
    conf.journal_addrs = vec![RaftPeer::from_addr(&conf.hostname, conf.rpc_port)];

    let server = RpcServer::new(
        ServerConf::with_hostname(conf.hostname.clone(), conf.rpc_port),
        OwnerService { gate, fail_path },
    );
    thread::spawn(move || server.block_on_start());

    let addr = format!("{}:{}", conf.hostname, conf.rpc_port);
    let deadline = Instant::now() + START_TIMEOUT;
    while Instant::now() < deadline {
        if TcpStream::connect(&addr).is_ok() {
            return conf;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("snapshot owner did not start on {addr}");
}

fn source_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut data = vec![0u8; len];
    for byte in data.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (state >> 33) as u8;
    }
    data
}

fn write_source(dir: &str, sizes: &[(&str, usize)]) -> SnapshotFileList {
    let mut files = Vec::with_capacity(sizes.len());
    for (name, len) in sizes {
        let path = Path::new(dir).join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, source_bytes(*len)).unwrap();
        files.push(SnapshotFileInfo {
            path: name.to_string(),
            mtime: 0,
            ctime: 0,
            len: *len as u64,
        });
    }

    SnapshotFileList {
        dir: dir.to_string(),
        files,
    }
}

fn wait_for_opens(gate: &Gate, expected: usize) {
    let deadline = Instant::now() + GATE_TIMEOUT;
    while Instant::now() < deadline && gate.max_open() < expected {
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_contents(dir: &str, sizes: &[(&str, usize)]) {
    for (name, len) in sizes {
        let downloaded = fs::read(Path::new(dir).join(name))
            .unwrap_or_else(|e| panic!("downloaded file {name} is missing: {e}"));
        assert_eq!(
            downloaded,
            source_bytes(*len),
            "content mismatch for {name}"
        );
    }
}

fn cleanup(dirs: &[&str]) {
    for dir in dirs {
        let _ = fs::remove_dir_all(dir);
    }
}

/// Every file gets its own connection, so with `parallel = 3` and three files
/// all three transfers must be in flight at the same moment.
#[test]
fn download_job_transfers_files_in_parallel() {
    let sizes: &[(&str, usize)] = &[
        ("checkpoint.db", CHUNK_SIZE * 3),
        ("journal/000001.db", CHUNK_SIZE * 2 + 17),
        ("journal/000002.db", CHUNK_SIZE),
    ];

    let src = Utils::test_sub_dir(format!("snapshot-parallel-src-{}", Utils::rand_id()));
    let manifest = write_source(&src, sizes);

    let gate = Gate::new();
    let mut conf = start_owner(gate.clone(), None);
    conf.snapshot_download_parallel = 3;
    conf.snapshot_read_chunk_size = CHUNK_SIZE;

    let dest = Utils::test_sub_dir(format!("snapshot-parallel-dst-{}", Utils::rand_id()));
    let node_id = conf.journal_addrs[0].id;
    let rt = conf.create_runtime();
    let client = RaftClient::from_conf(rt.clone(), &conf);
    let mut job = DownloadJob::new(node_id, dest.clone(), manifest, &conf, client);

    let handle = thread::spawn(move || rt.block_on(async move { job.run().await }));

    wait_for_opens(&gate, 3);
    gate.release();

    let downloaded = handle
        .join()
        .unwrap()
        .expect("parallel download must succeed");
    assert_eq!(
        gate.max_open(),
        3,
        "three files were never in flight together"
    );
    assert_eq!(downloaded, dest);
    assert_contents(&dest, sizes);

    cleanup(&[&src, &dest]);
}

/// `snapshot_download_parallel = 1` keeps the old serial transfer: the second
/// connection must not open while the first one is still running.
#[test]
fn download_job_parallel_one_is_serial() {
    let sizes: &[(&str, usize)] = &[
        ("checkpoint.db", CHUNK_SIZE * 2),
        ("journal/000001.db", CHUNK_SIZE),
        ("journal/000002.db", CHUNK_SIZE),
    ];

    let src = Utils::test_sub_dir(format!("snapshot-serial-src-{}", Utils::rand_id()));
    let manifest = write_source(&src, sizes);

    let gate = Gate::new();
    let mut conf = start_owner(gate.clone(), None);
    conf.snapshot_download_parallel = 1;
    conf.snapshot_read_chunk_size = CHUNK_SIZE;

    let dest = Utils::test_sub_dir(format!("snapshot-serial-dst-{}", Utils::rand_id()));
    let node_id = conf.journal_addrs[0].id;
    let rt = conf.create_runtime();
    let client = RaftClient::from_conf(rt.clone(), &conf);
    let mut job = DownloadJob::new(node_id, dest.clone(), manifest, &conf, client);

    let handle = thread::spawn(move || rt.block_on(async move { job.run().await }));

    wait_for_opens(&gate, 1);
    thread::sleep(Duration::from_millis(300));
    let observed = gate.max_open();
    gate.release();

    handle
        .join()
        .unwrap()
        .expect("serial download must succeed");
    assert_eq!(observed, 1, "parallel = 1 opened {observed} connections");
    assert_contents(&dest, sizes);

    cleanup(&[&src, &dest]);
}

/// A directory that cannot be created must fail the job before any transfer
/// starts, so `run()` never deletes the directory under a running task.
#[test]
fn download_job_setup_failure_starts_no_transfer() {
    let sizes: &[(&str, usize)] = &[
        ("checkpoint.db", CHUNK_SIZE),
        ("blocked/sub/x.db", CHUNK_SIZE),
    ];

    let src = Utils::test_sub_dir(format!("snapshot-setup-src-{}", Utils::rand_id()));
    let manifest = write_source(&src, sizes);

    let gate = Gate::new();
    let mut conf = start_owner(gate.clone(), None);
    conf.snapshot_download_parallel = 2;
    conf.snapshot_read_chunk_size = CHUNK_SIZE;

    let dest = Utils::test_sub_dir(format!("snapshot-setup-dst-{}", Utils::rand_id()));
    // A regular file where the manifest needs a parent directory.
    fs::write(Path::new(&dest).join("blocked"), b"x").unwrap();

    let node_id = conf.journal_addrs[0].id;
    let rt = conf.create_runtime();
    let client = RaftClient::from_conf(rt.clone(), &conf);
    let mut job = DownloadJob::new(node_id, dest.clone(), manifest, &conf, client);

    rt.block_on(async move { job.run().await })
        .expect_err("an unusable write path must fail the job");

    assert_eq!(
        gate.max_open(),
        0,
        "a setup failure must not start a transfer"
    );
    assert!(
        !Path::new(&dest).exists(),
        "the failed snapshot directory must be removed"
    );

    cleanup(&[&src]);
}

/// A failing transfer must abort the remaining work and leave no partial
/// snapshot directory behind.
#[test]
fn download_job_failure_removes_partial_directory() {
    let sizes: &[(&str, usize)] = &[
        ("checkpoint.db", CHUNK_SIZE),
        ("journal/000001.db", CHUNK_SIZE * 4),
        ("journal/000002.db", CHUNK_SIZE * 4),
        ("journal/000003.db", CHUNK_SIZE * 4),
    ];

    let src = Utils::test_sub_dir(format!("snapshot-fail-src-{}", Utils::rand_id()));
    let manifest = write_source(&src, sizes);

    let gate = Gate::new();
    let mut conf = start_owner(gate.clone(), Some("journal/000002.db".to_string()));
    conf.snapshot_download_parallel = 2;
    conf.snapshot_read_chunk_size = CHUNK_SIZE;

    let dest = Utils::test_sub_dir(format!("snapshot-fail-dst-{}", Utils::rand_id()));
    let node_id = conf.journal_addrs[0].id;
    let rt = conf.create_runtime();
    let client = RaftClient::from_conf(rt.clone(), &conf);
    let mut job = DownloadJob::new(node_id, dest.clone(), manifest, &conf, client);

    let handle = thread::spawn(move || rt.block_on(async move { job.run().await }));

    wait_for_opens(&gate, 2);
    gate.release();

    let err = handle
        .join()
        .unwrap()
        .expect_err("an injected transfer failure must fail the job");
    assert!(
        err.to_string().contains("injected transfer failure"),
        "unexpected error: {err}"
    );
    assert!(
        !Path::new(&dest).exists(),
        "the partial snapshot directory must be removed"
    );

    cleanup(&[&src]);
}
