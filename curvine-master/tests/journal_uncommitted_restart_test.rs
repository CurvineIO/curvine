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

use curvine_config::{ClusterConf, RaftPeer};
use curvine_master::master::journal::{JournalBatch, JournalSystem};
use curvine_master::master::Master;
use curvine_raft::proto::raft::AppliedIndex;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use prost::Message;
use raft::eraftpb::Entry;
use std::process::Command;

// The testing writer does not commit to Raft. Local rows flushed before
// journal commit must not become visible after restart. This currently fails:
// the reconstructed tree contains the uncommitted row.
#[test]
fn restart_does_not_expose_uncommitted_metadata() {
    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_CRASH_PROBE_DIR") {
        let conf = probe_conf(&base, None);
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.mkdir("/uncommitted", false).unwrap();
        // Checkpoint creation flushes the live RocksDB but does not register a
        // Raft snapshot, forcing the no-snapshot restart path under test.
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        // Exit without Rust destructors, after ensuring RocksDB data reached
        // disk.
        std::process::exit(0);
    }

    let probe_base = std::env::temp_dir().join(format!(
        "journal-crash-probe-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let probe_base = probe_base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "--ignored",
            "restart_does_not_expose_uncommitted_metadata",
        ])
        .env("CURVINE_CRASH_PROBE_DIR", &probe_base)
        .status()
        .unwrap();
    assert!(status.success(), "crash probe failed: {status}");

    let conf = probe_conf(&probe_base, None);
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let applied = loader.get_fsm_state().applied;
    assert_eq!(applied.index, 0, "no committed journal entry exists yet");
    assert!(
        !journal.fs().exists("/uncommitted").unwrap(),
        "uncommitted metadata must not be visible after restart"
    );
    conf.journal
        .create_runtime()
        .block_on(loader.apply(true, ApplyMsg::new_scan(applied)))
        .unwrap();
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&probe_base);
}

// Simulate a committed baseline by persisting a directory and a matching
// cursor/placeholder Raft entry, then flush a rename without committing it.
// The placeholder is not a replayable mkdir or a real multi-node commit.
#[test]
fn restart_does_not_expose_uncommitted_rename_after_committed_cursor() {
    use curvine_model::RenameFlags;

    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_RENAME_CRASH_PROBE_DIR") {
        let conf = probe_conf(&base, None);
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.mkdir("/committed", false).unwrap();
        // The rebuild reconstructs the namespace from the committed log, so
        // the baseline mkdir must be part of the committed entry.
        let mut batch = JournalBatch::new(1);
        for entry in fs.fs_dir.read().take_entries() {
            batch.push(entry);
        }
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            })
            .unwrap();
        fs.fs_dir
            .read()
            .store_journal_applied(
                &AppliedIndex {
                    term: 1,
                    index: 1,
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .unwrap();
        fs.rename("/committed", "/uncommitted", RenameFlags::empty())
            .unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::process::exit(0);
    }

    let base = std::env::temp_dir().join(format!(
        "journal-rename-crash-probe-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "restart_does_not_expose_uncommitted_rename_after_committed_cursor",
        ])
        .env("CURVINE_RENAME_CRASH_PROBE_DIR", &base)
        .status()
        .unwrap();
    assert!(status.success(), "crash probe failed: {status}");

    let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
    let loader = journal.journal_loader();
    // The rebuild resets the cursor; the committed baseline comes back with
    // the log scan while the uncommitted rename stays gone.
    assert_eq!(loader.get_fsm_state().applied.index, 0);
    assert!(
        !journal.fs().exists("/uncommitted").unwrap(),
        "uncommitted rename must not be visible"
    );
    let applied = loader.get_fsm_state().applied;
    probe_conf(&base, None)
        .journal
        .create_runtime()
        .block_on(loader.apply(true, ApplyMsg::new_scan(applied)))
        .unwrap();
    assert_eq!(loader.get_fsm_state().applied.index, 1);
    assert!(
        journal.fs().exists("/committed").unwrap(),
        "baseline path must survive restart"
    );
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

// This starts the real single-node Raft candidate and scans its committed log,
// but uses a testing writer and does not simulate a multi-node election.
#[test]
fn raft_start_does_not_expose_uncommitted_rename() {
    use curvine_model::RenameFlags;

    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_RAFT_RENAME_PROBE_DIR") {
        let port: u16 = std::env::var("CURVINE_RAFT_RENAME_PROBE_PORT")
            .unwrap()
            .parse()
            .unwrap();
        let journal = JournalSystem::from_conf(&probe_conf(&base, Some(port))).unwrap();
        let fs = journal.fs();
        fs.mkdir("/committed", false).unwrap();
        let mut batch = JournalBatch::new(1);
        for entry in fs.fs_dir.read().take_entries() {
            batch.push(entry);
        }
        assert!(batch.len() > 0, "the committed fixture must contain mkdir");
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            })
            .unwrap();
        fs.fs_dir
            .read()
            .store_journal_applied(
                &AppliedIndex {
                    term: 1,
                    index: 1,
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .unwrap();
        fs.rename("/committed", "/uncommitted", RenameFlags::empty())
            .unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::process::exit(0);
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let base = std::env::temp_dir().join(format!(
        "raft-rename-crash-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "raft_start_does_not_expose_uncommitted_rename"])
        .env("CURVINE_RAFT_RENAME_PROBE_DIR", &base)
        .env("CURVINE_RAFT_RENAME_PROBE_PORT", port.to_string())
        .status()
        .unwrap();
    assert!(status.success(), "crash probe failed: {status}");
    let journal = JournalSystem::from_conf(&probe_conf(&base, Some(port))).unwrap();
    // The rebuild resets the cursor; the committed baseline returns with the
    // Raft startup replay.
    assert_eq!(journal.journal_loader().get_fsm_state().applied.index, 0);
    let fs = journal.fs();
    let _listener = journal.start_blocking().unwrap();
    assert!(
        !fs.exists("/uncommitted").unwrap(),
        "uncommitted rename must not be visible after Raft startup"
    );
    assert!(fs.exists("/committed").unwrap());
}

// A registered snapshot can replace speculative local rows before log scan.
// The writer and commit below are test fixtures, not a multi-node election.
#[test]
fn raft_start_restores_registered_snapshot_before_replay() {
    use curvine_model::RenameFlags;
    use curvine_raft::raft::storage::{LogStorage, RocksLogStorage};

    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_RAFT_SNAPSHOT_PROBE_DIR") {
        let port: u16 = std::env::var("CURVINE_RAFT_SNAPSHOT_PROBE_PORT")
            .unwrap()
            .parse()
            .unwrap();
        let conf = probe_conf(&base, Some(port));
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        if std::env::var_os("CURVINE_RAFT_SNAPSHOT_VERIFY").is_some() {
            let loader = journal.journal_loader();
            let _listener = journal.start_blocking().unwrap();
            assert!(fs.exists("/committed").unwrap());
            assert!(!fs.exists("/uncommitted").unwrap());
            assert!(
                fs.exists("/suffix").unwrap(),
                "committed suffix must be replayed"
            );
            assert_eq!(loader.get_fsm_state().applied.index, 2);
            return;
        }
        fs.mkdir("/committed", false).unwrap();
        let mut batch = JournalBatch::new(1);
        for entry in fs.fs_dir.read().take_entries() {
            batch.push(entry);
        }
        assert!(batch.len() > 0);
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            })
            .unwrap();
        let loader = journal.journal_loader();
        let rt = conf.journal.create_runtime();
        // Advance the test fixture's applied watermark without replaying mkdir
        // over the already materialized baseline.
        rt.block_on(loader.apply(
            true,
            ApplyMsg::new_entry(Entry {
                term: 1,
                index: 1,
                ..Default::default()
            }),
        ))
        .unwrap();
        let snapshot = rt.block_on(loader.create_snapshot()).unwrap();
        drop(loader);
        drop(fs);
        journal.shutdown();
        let log = RocksLogStorage::from_conf(&conf.journal, false);
        log.create_snapshot(snapshot).unwrap();
        drop(log);
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.mkdir("/suffix", false).unwrap();
        let mut suffix = JournalBatch::new(2);
        for entry in fs.fs_dir.read().take_entries() {
            suffix.push(entry);
        }
        assert!(suffix.len() > 0);
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 2,
                data: suffix.encode().unwrap(),
                ..Default::default()
            })
            .unwrap();
        fs.rename("/committed", "/uncommitted", RenameFlags::empty())
            .unwrap();
        fs.fs_dir.read().create_checkpoint(2).unwrap();
        std::process::exit(0);
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let base = std::env::temp_dir().join(format!(
        "raft-snapshot-crash-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "raft_start_restores_registered_snapshot_before_replay",
        ])
        .env("CURVINE_RAFT_SNAPSHOT_PROBE_DIR", &base)
        .env("CURVINE_RAFT_SNAPSHOT_PROBE_PORT", port.to_string())
        .status()
        .unwrap();
    assert!(status.success(), "snapshot crash probe failed: {status}");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "raft_start_restores_registered_snapshot_before_replay",
        ])
        .env("CURVINE_RAFT_SNAPSHOT_PROBE_DIR", &base)
        .env("CURVINE_RAFT_SNAPSHOT_PROBE_PORT", port.to_string())
        .env("CURVINE_RAFT_SNAPSHOT_VERIFY", "1")
        .status()
        .unwrap();
    assert!(status.success(), "snapshot restart probe failed: {status}");
    std::fs::remove_dir_all(&base).unwrap();
}

#[test]
fn restart_rejects_corrupt_applied_cursor() {
    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_CORRUPT_CURSOR_DIR") {
        let conf = probe_conf(&base, None);
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.fs_dir.read().store_journal_applied(&[0xff]).unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::process::exit(0);
    }
    let base = std::env::temp_dir().join(format!(
        "journal-corrupt-cursor-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "restart_rejects_corrupt_applied_cursor"])
        .env("CURVINE_CORRUPT_CURSOR_DIR", &base)
        .status()
        .unwrap();
    assert!(status.success(), "cursor probe failed: {status}");
    // Peer recovery still validates the cursor before the Raft recovery
    // state is loaded; run it on the untouched fixture first. No network or
    // peer recovery is exercised.
    let mut recovery_conf = probe_conf(&base, Some(16001));
    recovery_conf.journal.journal_addrs = vec![
        RaftPeer::new(1, "127.0.0.1", 16001),
        RaftPeer::new(2, "127.0.0.1", 16002),
        RaftPeer::new(3, "127.0.0.1", 16003),
    ];
    recovery_conf.journal.recover_from_peers = Some(1);
    let result = JournalSystem::from_conf(&recovery_conf);
    assert!(
        matches!(result, Err(ref error) if error.to_string().contains("invalid journal applied cursor"))
    );
    // Plain startup rebuilds from the committed log instead of trusting the
    // corrupt cursor, so the corrupt value can no longer mislead replay.
    let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
    assert_eq!(journal.journal_loader().get_fsm_state().applied.index, 0);
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn restart_rejects_cursor_ahead_of_raft_commit() {
    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_AHEAD_CURSOR_DIR") {
        let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
        let fs = journal.fs();
        let applied = AppliedIndex {
            term: 1,
            index: 3,
            ..Default::default()
        };
        fs.fs_dir
            .read()
            .store_journal_applied(&applied.encode_to_vec())
            .unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::process::exit(0);
    }
    let base = std::env::temp_dir().join(format!(
        "journal-ahead-cursor-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "restart_rejects_cursor_ahead_of_raft_commit"])
        .env("CURVINE_AHEAD_CURSOR_DIR", &base)
        .status()
        .unwrap();
    assert!(status.success(), "cursor probe failed: {status}");
    // The no-snapshot rebuild discards the ahead cursor and replays the
    // committed log; the bogus value cannot make replay skip entries.
    let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
    assert_eq!(journal.journal_loader().get_fsm_state().applied.index, 0);
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn restart_rejects_cursor_with_wrong_term() {
    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_WRONG_TERM_CURSOR_DIR") {
        let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 1,
                ..Default::default()
            })
            .unwrap();
        let fs = journal.fs();
        fs.fs_dir
            .read()
            .store_journal_applied(
                &AppliedIndex {
                    term: 2,
                    index: 1,
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::process::exit(0);
    }
    let base = std::env::temp_dir()
        .join(format!(
            "journal-wrong-term-{}-{}",
            std::process::id(),
            Utils::rand_str(6)
        ))
        .to_str()
        .unwrap()
        .to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "restart_rejects_cursor_with_wrong_term"])
        .env("CURVINE_WRONG_TERM_CURSOR_DIR", &base)
        .status()
        .unwrap();
    assert!(status.success());
    // The no-snapshot rebuild discards the term-mismatched cursor and
    // replays the committed log with term validation per entry.
    let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
    assert_eq!(journal.journal_loader().get_fsm_state().applied.index, 0);
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn restart_loads_cursor_at_raft_commit() {
    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_VALID_CURSOR_DIR") {
        let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 1,
                ..Default::default()
            })
            .unwrap();
        let fs = journal.fs();
        let applied = AppliedIndex {
            term: 1,
            index: 1,
            ..Default::default()
        };
        fs.fs_dir
            .read()
            .store_journal_applied(&applied.encode_to_vec())
            .unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::process::exit(0);
    }
    let base = std::env::temp_dir().join(format!(
        "journal-valid-cursor-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "restart_loads_cursor_at_raft_commit"])
        .env("CURVINE_VALID_CURSOR_DIR", &base)
        .status()
        .unwrap();
    assert!(status.success(), "cursor probe failed: {status}");
    let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
    let loader = journal.journal_loader();
    // The rebuild resets the cursor to 0; the committed state comes back with
    // the log scan, not from the pre-crash cursor row.
    assert_eq!(loader.get_fsm_state().applied.index, 0);
    let applied = loader.get_fsm_state().applied;
    probe_conf(&base, None)
        .journal
        .create_runtime()
        .block_on(loader.apply(true, ApplyMsg::new_scan(applied)))
        .unwrap();
    assert_eq!(loader.get_fsm_state().applied.index, 1);
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

// A snapshot marker may be committed after its checkpoint was created. Its
// applied index can therefore exceed the cursor stored in that checkpoint.
#[test]
fn restored_snapshot_cursor_survives_restart() {
    use curvine_raft::proto::raft::FsmState;
    use curvine_raft::raft::RaftUtils;

    Master::init_test_metrics();
    if let Ok(base) = std::env::var("CURVINE_SNAPSHOT_CURSOR_DIR") {
        let conf = probe_conf(&base, None);
        let journal = JournalSystem::from_conf(&conf).unwrap();
        for index in 1..=2 {
            journal
                .append_committed_entry_for_test(Entry {
                    term: 1,
                    index,
                    ..Default::default()
                })
                .unwrap();
        }
        let fs = journal.fs();
        let applied = AppliedIndex {
            term: 1,
            index: 1,
            ..Default::default()
        };
        fs.fs_dir
            .read()
            .store_journal_applied(&applied.encode_to_vec())
            .unwrap();
        let checkpoint = fs.fs_dir.read().create_checkpoint(1).unwrap();
        let state = FsmState {
            applied: AppliedIndex {
                term: 1,
                index: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let snapshot = RaftUtils::create_file_snapshot(&checkpoint, 1, state).unwrap();
        // Register the snapshot with the Raft log store, the way a real
        // follower installation does. Without the registration the restart
        // cannot tell restored rows from uncommitted ones and the rebuild
        // would discard them.
        {
            use curvine_raft::raft::storage::LogStorage;
            journal
                .raft_journal_for_test()
                .log_store()
                .register_snapshot(snapshot.clone())
                .unwrap();
        }
        conf.journal
            .create_runtime()
            .block_on(journal.journal_loader().apply_snapshot(snapshot))
            .unwrap();
        // No extra checkpoint: a successful snapshot apply must flush its cursor.
        std::process::exit(0);
    }
    let base = std::env::temp_dir().join(format!(
        "journal-snapshot-cursor-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let base = base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "restored_snapshot_cursor_survives_restart"])
        .env("CURVINE_SNAPSHOT_CURSOR_DIR", &base)
        .status()
        .unwrap();
    assert!(status.success(), "snapshot probe failed: {status}");
    let journal = JournalSystem::from_conf(&probe_conf(&base, None)).unwrap();
    assert_eq!(journal.journal_loader().get_fsm_state().applied.index, 2);
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

// Exercise a direct journal scan over materialized rows. This does not start
// Raft and does not establish safety of the uncommitted row.
#[test]
fn direct_scan_replays_committed_entry_onto_materialized_rows() {
    Master::init_test_metrics();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    if let Ok(base) = std::env::var("CURVINE_COMMITTED_PROBE_DIR") {
        let conf = probe_conf(&base, Some(port));
        let journal = JournalSystem::from_conf(&conf).unwrap();
        let fs = journal.fs();
        fs.mkdir("/committed", false).unwrap();
        let mut batch = JournalBatch::new(1);
        for entry in fs.fs_dir.read().take_entries() {
            batch.push(entry);
        }
        journal
            .append_committed_entry_for_test(Entry {
                term: 1,
                index: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            })
            .unwrap();
        fs.mkdir("/uncommitted", false).unwrap();
        fs.fs_dir.read().create_checkpoint(1).unwrap();
        std::fs::write(format!("{base}/raft-port"), port.to_string()).unwrap();
        std::process::exit(0);
    }

    let probe_base = std::env::temp_dir().join(format!(
        "journal-committed-probe-{}-{}",
        std::process::id(),
        Utils::rand_str(6)
    ));
    let probe_base = probe_base.to_str().unwrap().to_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "direct_scan_replays_committed_entry_onto_materialized_rows",
        ])
        .env("CURVINE_COMMITTED_PROBE_DIR", &probe_base)
        .status()
        .unwrap();
    assert!(status.success(), "crash probe failed: {status}");

    let port_file = format!("{probe_base}/raft-port");
    let probe_port: u16 = std::fs::read_to_string(&port_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let conf = probe_conf(&probe_base, Some(probe_port));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    let applied = loader.get_fsm_state().applied;
    assert_eq!(
        applied.index, 0,
        "the cursor sits below the committed index"
    );
    let rt = conf.journal.create_runtime();
    rt.block_on(loader.apply(true, ApplyMsg::new_scan(applied)))
        .unwrap();
    assert!(
        loader.get_fsm_state().applied.index >= 1,
        "the committed suffix must be applied"
    );
    assert!(journal.fs().exists("/committed").unwrap());
    assert!(
        !journal.fs().exists("/uncommitted").unwrap(),
        "the rebuild must drop the uncommitted row"
    );
    journal.shutdown();
    let _ = std::fs::remove_dir_all(&probe_base);
}

fn probe_conf(base: &str, raft_port: Option<u16>) -> ClusterConf {
    let mut conf = ClusterConf {
        testing: true,
        format_master: false,
        ..Default::default()
    };
    conf.journal.enable = true;
    conf.master.meta_dir = format!("{base}/meta");
    conf.journal.journal_dir = format!("{base}/journal");
    if let Some(port) = raft_port {
        conf.journal.hostname = "127.0.0.1".into();
        conf.journal.rpc_port = port;
        conf.journal.journal_addrs = vec![RaftPeer::new(1, "127.0.0.1", port)];
    }
    conf
}
