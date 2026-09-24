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

//! Real TCP + RocksDB + checkpoint download across three isolated processes.
//! No Kubernetes, remote hosts, fixed ports or shared application stores.
#![cfg(target_os = "linux")]

use curvine_raft::conf::JournalConf;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_raft::raft::{
    RaftClient, RaftError, RaftJournal, RaftPeer, RaftResult, RaftUtils, RoleMonitor,
};
use curvine_raft::rocksdb::DBEngine;
use curvine_raft::utils::SerdeUtils;
use curvine_runtime::common::{Logger, Utils};
use curvine_runtime::runtime::RpcRuntime;
use prost::Message;
use raft::eraftpb::Entry;
use raft::{StateRole, Storage};
use serde_json::{json, Value};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct DiskApp {
    db: Arc<Mutex<DBEngine>>,
    log: RocksLogStorage,
    id: u64,
    root: PathBuf,
}

impl DiskApp {
    fn entry(&self, entry: Entry) -> RaftResult<()> {
        let db = self.db.lock().unwrap();
        if !entry.data.is_empty() {
            let (k, v): (String, String) = SerdeUtils::deserialize(&entry.data)?;
            db.put(k.as_bytes(), v.as_bytes())?;
        }
        let applied = AppliedIndex {
            index: entry.index,
            term: entry.term,
            ..Default::default()
        };
        let state = FsmState {
            applied: applied.clone(),
            ufs_applied: applied,
        };
        db.put(b"fsm", state.encode_to_vec())?;
        Ok(())
    }

    fn values(&self) -> Vec<Option<String>> {
        let db = self.db.lock().unwrap();
        (0..32)
            .map(|i| {
                db.get(format!("k{i}"))
                    .unwrap()
                    .map(|v| String::from_utf8(v).unwrap())
            })
            .collect()
    }
}

impl AppStorage for DiskApp {
    async fn apply(&self, _: bool, msg: ApplyMsg) -> RaftResult<()> {
        if let ApplyMsg::Scan(applied) = msg {
            let commit = self.log.hard_state().commit;
            if applied.index < commit {
                for e in self.log.scan_entries(applied.index + 1, commit + 1)? {
                    self.entry(e)?;
                }
            }
            return Ok(());
        }
        let (entry, ack) = msg.into_entry_with_ack()?;
        let result = self.entry(entry);
        if let Some(tx) = ack {
            let _ = tx.send(result);
            Ok(())
        } else {
            result
        }
    }
    fn get_fsm_state(&self) -> FsmState {
        self.db
            .lock()
            .unwrap()
            .get(b"fsm")
            .unwrap()
            .map(|v| FsmState::decode(v.as_slice()).unwrap())
            .unwrap_or_default()
    }
    async fn role_change(&self, _: StateRole) -> RaftResult<()> {
        Ok(())
    }
    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        let state = self.get_fsm_state();
        let db = self.db.lock().unwrap();
        let dir = db.create_checkpoint(state.applied.index)?;
        RaftUtils::create_file_snapshot(dir, self.id, state)
    }
    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        if let Some(files) = snapshot.files_data {
            if self.root.join("fail-snapshot").exists() {
                return Err(RaftError::other(
                    "injected application snapshot failure".into(),
                ));
            }
            // Pause after journal snapshot persistence but before application
            // restore, to crash at a real recovery boundary.
            if self.root.join("pause-snapshot").exists() {
                fs::write(self.root.join("snapshot-waiting"), b"")?;
                while self.root.join("pause-snapshot").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            let mut db = self.db.lock().unwrap();
            RaftUtils::apply_rocks_snapshot(&mut db, &files)?;
            db.put(b"fsm", snapshot.fsm_state.encode_to_vec())?;
        }
        Ok(())
    }
    fn snapshot_dir(&self, id: u64) -> RaftResult<String> {
        Ok(self.db.lock().unwrap().get_checkpoint_path(id))
    }
}

fn atomic_json(path: &Path, value: Value) {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec(&value).unwrap()).unwrap();
    fs::rename(tmp, path).unwrap();
}

#[test]
#[ignore = "child process entrypoint; launched only by three_process_empty_member_recovery"]
fn recovery_process() {
    let Ok(root) = std::env::var("CURVINE_RECOVERY_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let conf: JournalConf =
        serde_json::from_slice(&fs::read(root.join("conf.json")).unwrap()).unwrap();
    let id: u64 = std::env::var("CURVINE_RECOVERY_TEST_ID")
        .unwrap()
        .parse()
        .unwrap();
    Logger::default();
    let rt = conf.create_runtime();
    let log = RocksLogStorage::from_conf(&conf, false);
    let app = DiskApp {
        db: Arc::new(Mutex::new(
            DBEngine::from_dir(root.join("meta").to_str().unwrap(), false).unwrap(),
        )),
        log: log.clone(),
        id,
        root: root.clone(),
    };
    let monitor = RoleMonitor::new();
    let ctl = monitor.read_ctl();
    let journal = RaftJournal::new(rt.clone(), log.clone(), app.clone(), conf.clone(), monitor);
    rt.block_on(journal.run()).unwrap();
    let client = RaftClient::from_conf(rt.clone(), &conf);
    let mut done = 0;
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if let Ok(bytes) = fs::read(root.join("command.json")) {
            let cmd: Value = serde_json::from_slice(&bytes).unwrap();
            let seq = cmd["seq"].as_u64().unwrap();
            if seq > done {
                if cmd["kind"] == "write" {
                    for i in cmd["start"].as_u64().unwrap()..cmd["end"].as_u64().unwrap() {
                        let bytes =
                            SerdeUtils::serialize(&(format!("k{i}"), format!("v{i}"))).unwrap();
                        rt.block_on(client.send_propose(bytes)).unwrap();
                    }
                } else if cmd["kind"] == "snapshot" {
                    let snapshot = rt.block_on(app.create_snapshot()).unwrap();
                    let compact = snapshot.fsm_state.compact();
                    log.create_snapshot(snapshot).unwrap();
                    log.compact(compact).unwrap();
                }
                done = seq;
            }
        }
        let hs = log.hard_state();
        let snapshot = log
            .latest_snapshot()
            .unwrap()
            .map(|s| SnapshotData::decode(s.data.as_ref()).unwrap());
        atomic_json(
            &root.join("status.json"),
            json!({
                "role": ctl.state::<i8>(), "term": hs.term, "commit": hs.commit,
                "applied": app.get_fsm_state().applied.index, "first": log.first_index().unwrap(),
                "snapshot": snapshot.map(|s| json!({"node":s.node_id, "dir":s.data_dir()})),
                "values": app.values(), "done": done,
            }),
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

struct Cluster {
    root: PathBuf,
    conf: Vec<JournalConf>,
    children: Vec<Option<Child>>,
    seq: u64,
}

impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("curvine-1718-rpc-{}", Utils::rand_id()));
        fs::create_dir_all(&root).unwrap();
        // Hold all reservations until every address has been chosen.
        let ports: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers: Vec<_> = ports
            .iter()
            .enumerate()
            .map(|(i, l)| RaftPeer::new(i as u64 + 1, "127.0.0.1", l.local_addr().unwrap().port()))
            .collect();
        let conf = peers
            .iter()
            .enumerate()
            .map(|(i, peer)| JournalConf {
                hostname: "127.0.0.1".into(),
                rpc_port: peer.port,
                journal_addrs: peers.clone(),
                journal_dir: root
                    .join(format!("n{}/journal", i + 1))
                    .to_str()
                    .unwrap()
                    .into(),
                io_threads: 1,
                worker_threads: 2,
                raft_tick_interval_ms: 50,
                raft_heartbeat_tick: 2,
                raft_election_tick: 40,
                raft_min_election_ticks: 40,
                raft_max_election_ticks: 60,
                snapshot_interval: "1h".into(),
                snapshot_entries: 1_000_000,
                conn_timeout_ms: 500,
                io_timeout_ms: 2000,
                ..Default::default()
            })
            .collect();
        Self {
            root,
            conf,
            children: (0..3).map(|_| None).collect(),
            seq: 0,
        }
    }
    fn dir(&self, i: usize) -> PathBuf {
        self.root.join(format!("n{}", i + 1))
    }
    fn start(&mut self, i: usize, recovery: bool) {
        let root = self.dir(i);
        fs::create_dir_all(&root).unwrap();
        let _ = fs::remove_file(root.join("status.json"));
        let _ = fs::remove_file(root.join("command.json"));
        self.conf[i].recover_from_peers = recovery;
        fs::write(
            root.join("conf.json"),
            serde_json::to_vec(&self.conf[i]).unwrap(),
        )
        .unwrap();
        let out = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("process.log"))
            .unwrap();
        self.children[i] = Some(
            Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "recovery_process", "--nocapture"])
                .env("CURVINE_RECOVERY_TEST_ROOT", &root)
                .env("CURVINE_RECOVERY_TEST_ID", (i + 1).to_string())
                .stdout(Stdio::from(out.try_clone().unwrap()))
                .stderr(Stdio::from(out))
                .spawn()
                .unwrap(),
        );
    }
    fn stop(&mut self, i: usize) {
        if let Some(mut child) = self.children[i].take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    fn status(&self, i: usize) -> Value {
        fs::read(self.dir(i).join("status.json"))
            .ok()
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or(Value::Null)
    }
    fn wait(&mut self, label: &str, predicate: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(35);
        while Instant::now() < deadline {
            for child in self.children.iter_mut().flatten() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited; evidence: {}",
                    self.root.display()
                );
            }
            if predicate(self) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!(
            "{label} timed out; status={:?}; evidence={}",
            (0..3).map(|i| self.status(i)).collect::<Vec<_>>(),
            self.root.display()
        );
    }
    fn command(&mut self, node: usize, kind: &str, start: u64, end: u64) {
        self.seq += 1;
        let seq = self.seq;
        atomic_json(
            &self.dir(node).join("command.json"),
            json!({"seq":seq,"kind":kind,"start":start,"end":end}),
        );
        self.wait(kind, |c| c.status(node)["done"] == seq);
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..3 {
            self.stop(i);
        }
        if !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

#[test]
fn three_process_empty_member_recovery() {
    let mut c = Cluster::new();
    for i in 0..3 {
        c.start(i, false);
    }
    c.wait("elect leader", |c| {
        (0..3).filter(|&i| c.status(i)["role"] == 1).count() == 1
    });
    let leader = (0..3).find(|&i| c.status(i)["role"] == 1).unwrap();
    let victim = (leader + 1) % 3;
    c.command(leader, "write", 0, 24);
    c.wait("initial catchup", |c| {
        (0..3).all(|i| c.status(i)["values"][23] == "v23")
    });
    let expected = json!((0..32)
        .map(|i| (i < 24).then(|| format!("v{i}")))
        .collect::<Vec<_>>());
    c.wait("all initial values", |c| {
        (0..3).all(|i| c.status(i)["values"] == expected)
    });
    let term = c.status(leader)["term"].clone();
    let commit = c.status(leader)["commit"].clone();
    for compact in [false, true] {
        if compact {
            c.command(leader, "snapshot", 0, 0);
            assert!(
                c.status(leader)["first"].as_u64().unwrap() > 1,
                "must exercise the snapshot path"
            );
        }
        c.stop(victim);
        // Only the test victim's dedicated directories are removed.
        fs::remove_dir_all(c.dir(victim).join("meta")).unwrap();
        fs::remove_dir_all(c.dir(victim).join("journal")).unwrap();
        if compact {
            fs::write(c.dir(victim).join("pause-snapshot"), b"").unwrap();
        }
        c.start(victim, true);
        if compact {
            c.wait("snapshot apply paused", |c| {
                c.dir(victim).join("snapshot-waiting").exists() && c.status(victim)["role"] == 0
            });
            assert!(c
                .dir(victim)
                .join("journal/member-recovery-in-progress")
                .exists());
            assert_eq!(
                c.status(victim)["applied"],
                0,
                "must not publish in-memory snapshot progress"
            );
            c.stop(victim);
            fs::remove_file(c.dir(victim).join("pause-snapshot")).unwrap();
            // The persisted marker, not the flag, must keep this restart from
            // voting/campaigning until the local snapshot is installed.
            c.start(victim, false);
        }
        c.wait("empty member recovery", |c| {
            let state = c.status(victim);
            state["role"] == 2
                && state["applied"] == commit
                && state["commit"] == commit
                && state["values"] == expected
        });
        assert!(!c
            .dir(victim)
            .join("journal/member-recovery-in-progress")
            .exists());
        assert_eq!(c.status(leader)["role"], 1);
        assert_eq!(
            c.status(leader)["term"],
            term,
            "leader must not change to trigger recovery"
        );
        if compact {
            let snapshot = c.status(victim)["snapshot"].clone();
            assert_eq!(snapshot["node"], (victim + 1) as u64);
            assert!(snapshot["dir"]
                .as_str()
                .unwrap()
                .starts_with(c.dir(victim).to_str().unwrap()));
        }
        // Real process restart using the recovered local stores, recovery disabled.
        c.stop(victim);
        c.start(victim, false);
        c.wait("normal restart", |c| {
            c.status(victim)["role"] == 2
                && c.status(victim)["applied"] == commit
                && c.status(victim)["values"] == expected
        });
    }
    // Fail the actual application restore after the log snapshot was saved.
    // Raft must stop without publishing Follower readiness or deleting the
    // recovery marker. A same-version restart retries the local checkpoint.
    c.stop(victim);
    fs::remove_dir_all(c.dir(victim).join("meta")).unwrap();
    fs::remove_dir_all(c.dir(victim).join("journal")).unwrap();
    fs::write(c.dir(victim).join("fail-snapshot"), b"").unwrap();
    c.start(victim, true);
    c.wait("failed snapshot stops Raft", |c| {
        c.status(victim)["role"] == 3
    });
    assert_eq!(c.status(victim)["applied"], 0);
    assert!(c
        .dir(victim)
        .join("journal/member-recovery-in-progress")
        .exists());
    let log = fs::read_to_string(c.dir(victim).join("process.log")).unwrap();
    assert!(log.contains("refusing to advance Ready"));
    c.stop(victim);
    fs::remove_file(c.dir(victim).join("fail-snapshot")).unwrap();
    c.start(victim, false);
    c.wait("retry failed snapshot", |c| {
        c.status(victim)["role"] == 2
            && c.status(victim)["applied"] == commit
            && c.status(victim)["values"] == expected
    });
    assert!(!c
        .dir(victim)
        .join("journal/member-recovery-in-progress")
        .exists());
    c.command(leader, "write", 24, 32);
    c.wait("writes after recovery", |c| {
        let expected = json!((0..32).map(|i| format!("v{i}")).collect::<Vec<_>>());
        (0..3).all(|i| c.status(i)["values"] == expected)
    });
    assert_eq!(c.status(leader)["term"], term);
}
