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

mod data_dir_tests {
    use super::super::*;
    use curvine_config::{JournalConf, MasterConf};
    use curvine_raft::raft::RaftPeer;
    use curvine_runtime::common::Utils;

    fn non_format_master_conf(name: &str, multi_master: bool) -> ClusterConf {
        let mut journal = JournalConf::with_test();
        journal.enable = false;
        journal.journal_dir = Utils::test_sub_dir(format!("master-journal-test/journal-{}", name));
        if multi_master {
            journal
                .journal_addrs
                .push(RaftPeer::new(2, "localhost", journal.rpc_port + 1));
        }

        ClusterConf {
            format_master: false,
            testing: true,
            master: MasterConf {
                meta_dir: Utils::test_sub_dir(format!("master-journal-test/meta-{}", name)),
                ..Default::default()
            },
            journal,
            ..Default::default()
        }
    }

    #[test]
    fn require_existing_master_data_allows_clean_empty_non_format_dirs() -> FsResult<()> {
        for multi_master in [false, true] {
            let name = format!(
                "clean-empty-non-format-{}-{}",
                if multi_master { "ha" } else { "single" },
                Utils::rand_str(6)
            );
            let conf = non_format_master_conf(&name, multi_master);
            let _ = fs::remove_dir_all(&conf.master.meta_dir);
            let _ = fs::remove_dir_all(&conf.journal.journal_dir);

            JournalSystem::require_existing_master_data(&conf)?;

            assert!(Path::new(&conf.master.meta_dir).is_dir());
            assert!(Path::new(&conf.journal.journal_dir).is_dir());
        }

        Ok(())
    }

    #[test]
    fn require_existing_master_data_refuses_dirty_non_format_dirs() -> FsResult<()> {
        for multi_master in [false, true] {
            let name = format!(
                "dirty-non-format-{}-{}",
                if multi_master { "ha" } else { "single" },
                Utils::rand_str(6)
            );
            let conf = non_format_master_conf(&name, multi_master);
            let _ = fs::remove_dir_all(&conf.master.meta_dir);
            let _ = fs::remove_dir_all(&conf.journal.journal_dir);
            fs::create_dir_all(&conf.master.meta_dir)?;
            fs::create_dir_all(&conf.journal.journal_dir)?;
            fs::write(
                Path::new(&conf.journal.journal_dir).join("orphaned-file"),
                "not rocksdb",
            )?;

            let err = JournalSystem::require_existing_master_data(&conf)
                .expect_err("dirty master data directory must be refused");
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("format_master=false")
                    && err_msg.contains("inconsistent or invalid master data directories"),
                "unexpected error: {}",
                err_msg
            );
        }

        Ok(())
    }

    #[test]
    fn journal_recovery_marker_is_allowed_but_unknown_entries_are_rejected() -> FsResult<()> {
        let name = format!("marker-{}", Utils::rand_str(6));
        let mut conf = non_format_master_conf(&name, true);
        conf.journal.enable = true;
        conf.journal.recover_from_peers = Some(1);
        let _ = fs::remove_dir_all(&conf.master.meta_dir);
        let _ = fs::remove_dir_all(&conf.journal.journal_dir);
        fs::create_dir_all(conf.db_conf().data_dir)?;
        fs::create_dir_all(conf.journal.db_conf().data_dir)?;
        fs::write(conf.journal.recovery_marker(), b"")?;

        JournalSystem::require_existing_master_data(&conf)?;

        fs::write(
            Path::new(&conf.journal.journal_dir).join("unexpected"),
            b"not allowed",
        )?;
        assert!(JournalSystem::require_existing_master_data(&conf).is_err());
        let _ = fs::remove_dir_all(&conf.master.meta_dir);
        let _ = fs::remove_dir_all(&conf.journal.journal_dir);
        Ok(())
    }

    #[test]
    fn recovery_marker_is_not_allowed_in_the_meta_directory() -> FsResult<()> {
        let name = format!("meta-marker-{}", Utils::rand_str(6));
        let mut conf = non_format_master_conf(&name, true);
        conf.journal.enable = true;
        conf.journal.recover_from_peers = Some(1);
        let _ = fs::remove_dir_all(&conf.master.meta_dir);
        let _ = fs::remove_dir_all(&conf.journal.journal_dir);
        fs::create_dir_all(conf.db_conf().data_dir)?;
        fs::create_dir_all(conf.journal.db_conf().data_dir)?;
        fs::write(
            Path::new(&conf.master.meta_dir).join("member-recovery-in-progress"),
            b"",
        )?;

        assert!(JournalSystem::require_existing_master_data(&conf).is_err());
        let _ = fs::remove_dir_all(&conf.master.meta_dir);
        let _ = fs::remove_dir_all(&conf.journal.journal_dir);
        Ok(())
    }
}

mod recovery_metrics_tests {
    use super::super::*;
    use crate::master::Master;
    use curvine_config::{JournalConf, MasterConf};
    use curvine_raft::proto::raft::AppliedIndex;
    use curvine_raft::raft::storage::PeerStorage;
    use curvine_runtime::common::Utils;
    use raft::eraftpb::HardState;

    #[test]
    fn recovery_refuses_formatting_or_disabled_journal() -> FsResult<()> {
        let root = std::env::temp_dir().join(format!("curvine-1718-config-{}", Utils::rand_id()));
        fs::create_dir_all(&root)?;
        for (format_master, enable) in [(true, true), (false, false)] {
            for recover_from_peers in [true, false] {
                let conf = ClusterConf {
                    format_master,
                    journal: JournalConf {
                        recover_from_peers: recover_from_peers.then_some(1),
                        journal_dir: root.to_str().unwrap().into(),
                        enable,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                let marker = conf.journal.recovery_marker();
                if !recover_from_peers {
                    fs::write(&marker, b"")?;
                }
                let Err(error) = JournalSystem::from_conf(&conf) else {
                    panic!("unsafe recovery configuration was accepted")
                };
                assert!(error.to_string().contains("requires format_master=false"));
                if !recover_from_peers {
                    assert!(marker.exists(), "must not format away the recovery marker");
                    fs::remove_file(marker)?;
                }
            }
        }
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn snapshot_restore_and_hard_state_only_changes_refresh_journal_metrics() -> FsResult<()> {
        let root = std::env::temp_dir().join(format!("curvine-1718-metrics-{}", Utils::rand_id()));
        let conf = ClusterConf {
            testing: true,
            format_master: true,
            master: MasterConf {
                meta_dir: root.join("meta").to_str().unwrap().into(),
                ..Default::default()
            },
            journal: JournalConf {
                journal_dir: root.join("journal").to_str().unwrap().into(),
                io_threads: 1,
                worker_threads: 2,
                ..JournalConf::with_test()
            },
            ..Default::default()
        };
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let log = js.raft_journal.log_store().clone();
        log.append(
            &(1..=13)
                .map(|index| Entry {
                    index,
                    term: 30,
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        )?;
        let peer = PeerStorage::new(
            js.rt.clone(),
            log,
            loader.clone(),
            RaftClient::from_conf(js.rt.clone(), &conf.journal),
            &conf.journal,
        );
        peer.set_hard_state(&HardState {
            term: 30,
            vote: 1,
            commit: 12,
        })?;
        let mut snapshot = js.rt.block_on(loader.create_snapshot())?;
        snapshot.snapshot_id = 10;
        snapshot.fsm_state.applied = AppliedIndex {
            index: 10,
            term: 30,
            ..Default::default()
        };
        snapshot.fsm_state.ufs_applied = AppliedIndex {
            index: 7,
            term: 29,
            ..Default::default()
        };
        let metrics = Master::get_metrics()?;
        metrics.journal_applied.set(0);
        metrics.journal_ufs_applied.set(0);
        metrics.journal_committed.set(0);
        metrics.journal_term.set(0);
        // Restore a real RocksDB checkpoint, with no subsequent business write.
        js.rt.block_on(loader.apply_snapshot(snapshot))?;
        assert_eq!(metrics.journal_applied.get(), 10);
        assert_eq!(metrics.journal_ufs_applied.get(), 7);
        assert_eq!(metrics.journal_committed.get(), 12);
        assert_eq!(metrics.journal_term.get(), 30);
        assert_eq!(
            loader.get_fsm_state().applied.index,
            10,
            "metrics must not pretend applied == committed"
        );
        peer.set_hard_state(&HardState {
            term: 31,
            vote: 2,
            commit: 12,
        })?;
        assert_eq!(metrics.journal_term.get(), 31);
        assert_eq!(metrics.journal_applied.get(), 10);
        peer.set_hard_state_commit(13)?;
        assert_eq!(metrics.journal_committed.get(), 13);
        assert_eq!(metrics.journal_applied.get(), 10);
        js.rt
            .block_on(loader.apply_snapshot(SnapshotData::default()))?;
        assert_eq!(
            metrics.journal_applied.get(),
            10,
            "placeholder must not reset restored gauges"
        );
        drop(peer);
        drop(loader);
        js.shutdown();
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
