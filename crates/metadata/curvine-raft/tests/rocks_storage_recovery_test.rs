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

use curvine_core_error::CommonResult;
use curvine_raft::raft::storage::RocksStorageCore;
use curvine_raft::rocksdb::{DBConf, DBEngine, RocksUtils};
use curvine_runtime::common::{FileUtils, Utils};
use prost::Message;
use raft::eraftpb::{Entry, HardState, Snapshot};

fn test_conf(name: &str) -> DBConf {
    DBConf::new(Utils::test_sub_dir(format!(
        "rocks-storage-recovery-{}-{}",
        name,
        Utils::rand_str(6)
    )))
}

fn assert_reseed_required(error: impl ToString, commit: u64, lower: u64, upper: u64) {
    let message = error.to_string();
    assert!(
        message.contains(&format!("hard_state.commit={commit}")),
        "missing commit in error: {message}"
    );
    assert!(
        message.contains(&format!("[{lower}, {upper}]")),
        "missing durable range in error: {message}"
    );
    assert!(
        message.contains("Restore a consistent master meta+journal pair from a healthy voter"),
        "missing operator hint in error: {message}"
    );
}

#[test]
fn hard_state_can_precede_entries_within_one_ready() -> CommonResult<()> {
    let conf = test_conf("commit-before-append");
    let mut storage = RocksStorageCore::new(conf.clone(), true);
    storage.set_hard_state(HardState {
        term: 7,
        vote: 1,
        commit: 1,
    })?;
    storage.append(&[Entry {
        term: 7,
        index: 1,
        ..Default::default()
    }])?;
    drop(storage);

    let mut reopened = RocksStorageCore::new(conf, false);
    assert_eq!(reopened.init_state()?.hard_state.commit, 1);
    Ok(())
}

#[test]
fn startup_rejects_a_hard_state_beyond_the_durable_log_tail() {
    let conf = test_conf("startup");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    let db = DBEngine::new(
        conf.clone()
            .add_cf(RocksStorageCore::CF_ENTRIES)
            .add_cf(RocksStorageCore::CF_META),
        true,
    )
    .unwrap();
    db.put_cf(
        RocksStorageCore::CF_META,
        RocksStorageCore::STATE_KEY,
        HardState {
            term: 7,
            vote: 1,
            commit: 42,
        }
        .encode_to_vec(),
    )
    .unwrap();
    drop(db);

    let error = RocksStorageCore::new(conf, false).init_state().unwrap_err();
    assert_reseed_required(error, 42, 0, 0);
}

#[test]
fn startup_rejects_a_hard_state_below_the_compacted_prefix() {
    let conf = test_conf("startup-lower-bound");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    let db = DBEngine::new(
        conf.clone()
            .add_cf(RocksStorageCore::CF_ENTRIES)
            .add_cf(RocksStorageCore::CF_META),
        true,
    )
    .unwrap();
    db.put_cf(
        RocksStorageCore::CF_META,
        RocksStorageCore::INDEX_KEY,
        RocksUtils::u64_u64_to_bytes(101, 110),
    )
    .unwrap();
    db.put_cf(
        RocksStorageCore::CF_META,
        RocksStorageCore::STATE_KEY,
        HardState {
            term: 7,
            vote: 1,
            commit: 99,
        }
        .encode_to_vec(),
    )
    .unwrap();
    drop(db);

    let error = RocksStorageCore::new(conf, false).init_state().unwrap_err();
    assert_reseed_required(error, 99, 100, 110);
}

#[test]
fn snapshot_only_storage_restarts_at_the_snapshot_index() {
    let conf = test_conf("snapshot-only");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let mut storage = RocksStorageCore::new(conf.clone(), true);
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 100;
        snapshot.mut_metadata().term = 7;
        snapshot.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];
        storage.apply_snapshot(snapshot).unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    let state = storage.init_state().unwrap();
    assert_eq!(state.hard_state.commit, 100);
    assert_eq!(state.conf_state.voters, vec![1, 2, 3]);
    assert_eq!(storage.first_index(), 101);
    assert_eq!(storage.last_index(), 100);
}

#[test]
fn snapshot_install_compacts_the_durable_log_range() {
    let conf = test_conf("snapshot-compacts-range");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let mut storage = RocksStorageCore::new(conf.clone(), true);
        let entries: Vec<_> = (1..=3)
            .map(|index| Entry {
                term: 7,
                index,
                ..Default::default()
            })
            .collect();
        storage.append(&entries).unwrap();

        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 3;
        snapshot.mut_metadata().term = 7;
        storage.apply_snapshot(snapshot).unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    assert_eq!(storage.init_state().unwrap().hard_state.commit, 3);
    assert_eq!(storage.first_index(), 4);
    assert_eq!(storage.last_index(), 3);
}

#[test]
fn startup_rejects_corrupt_persisted_raft_metadata() {
    let cases = [
        (
            "index-range",
            RocksStorageCore::INDEX_KEY,
            "failed to decode raft index range",
        ),
        (
            "hard-state",
            RocksStorageCore::STATE_KEY,
            "failed to decode raft hard state",
        ),
        (
            "snapshot",
            RocksStorageCore::SNAP_KEY,
            "failed to decode raft snapshot",
        ),
        (
            "conf-state",
            RocksStorageCore::CONF_STATE_KEY,
            "failed to decode raft conf state",
        ),
    ];

    for (name, key, expected) in cases {
        let conf = test_conf(name);
        FileUtils::delete_path(&conf.base_dir, true).unwrap();
        let db = DBEngine::new(
            conf.clone()
                .add_cf(RocksStorageCore::CF_ENTRIES)
                .add_cf(RocksStorageCore::CF_META),
            true,
        )
        .unwrap();
        db.put_cf(RocksStorageCore::CF_META, key, [0xff]).unwrap();
        drop(db);

        let error = RocksStorageCore::new(conf, false)
            .init_state()
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{name}: {error}");
        assert!(
            error.contains("corrupted local directory"),
            "{name}: {error}"
        );
    }
}
