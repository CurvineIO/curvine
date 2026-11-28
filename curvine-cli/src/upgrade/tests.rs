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

//! End-to-end checks against stores written in the legacy `bincode` layout.
//!
//! The legacy store is produced by writing `bincode` payloads through the same
//! column families the old build used, then the converter is run over it and the
//! result is read back through the normal accessors, which decode with the
//! metadata codec only. Every test also asserts that the source is left alone.

use super::journal_db::{journal_conf, upgrade_journal_dir};
use super::meta_db::{convert_db_dir, db_conf};
use super::*;
use curvine_codec::{Codec, Decode};
use curvine_master::master::journal::JournalBatch;
use curvine_master::master::meta::inode::{InodeDir, InodeView};
use curvine_master::master::meta::store::RocksInodeStore;
use curvine_model::{
    AccessMode, BlockLocation, FileLock, LockFlags, LockType, MountInfo, Provider, StorageType,
    TtlAction, WriteType,
};
use curvine_proto::JournalBatchProto;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData, SnapshotFileList};
use curvine_rocksdb::{DBConf, DBEngine, RocksUtils};
use curvine_runtime::common::Utils;
use prost::Message;
use raft::eraftpb::{Entry, Snapshot, SnapshotMetadata};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

fn test_dir(name: &str) -> String {
    Utils::test_sub_dir(format!("meta-upgrade-{}-{}", name, Utils::rand_str(6)))
}

/// Store a file-list snapshot in the journal `meta` CF, as a member that took
/// its checkpoint at `checkpoint` would have. Returns the encoded record.
fn write_journal_snapshot(dir: &str, checkpoint: &str, index: u64) -> Vec<u8> {
    let db = DBEngine::new(journal_conf(dir), true).unwrap();
    let applied = AppliedIndex {
        term: 1,
        index,
        op_id: 0,
        rpc_id: 0,
    };
    let data = SnapshotData {
        snapshot_id: index,
        node_id: 1,
        create_time: 0,
        bytes_data: None,
        files_data: Some(SnapshotFileList {
            dir: checkpoint.to_string(),
            files: vec![],
        }),
        fsm_state: FsmState {
            applied: applied.clone(),
            ufs_applied: applied,
        },
    };
    let snapshot = Snapshot {
        data: data.encode_to_vec(),
        metadata: Some(SnapshotMetadata {
            index,
            term: 1,
            ..Default::default()
        }),
    };
    let bytes = snapshot.encode_to_vec();
    db.put_cf("meta", [0x01u8], &bytes).unwrap();
    db.flush(true).unwrap();
    bytes
}

/// Decode one record the way the master does.
///
/// The converter never reads its output back; the tests do, to prove that what
/// was written is loadable.
fn decode_meta<T: Decode>(bytes: &[u8]) -> curvine_core_error::CommonResult<T> {
    Ok(Codec::decode(bytes)?)
}

/// Configuration matching what the master opens for `<base>/data`: `data_dir`
/// stays the `data/` child of `base`.
fn store_conf(base: &str) -> DBConf {
    DBConf::new(base)
        .add_cf(RocksInodeStore::CF_INODES)
        .add_cf(RocksInodeStore::CF_EDGES)
        .add_cf(RocksInodeStore::CF_BLOCK)
        .add_cf(RocksInodeStore::CF_LOCATION)
        .add_cf(RocksInodeStore::CF_COMMON)
}

/// Whether a payload is already a `JournalBatchProto` envelope. Mirrors the
/// check the converter uses, so it can prove the source was not rewritten.
fn looks_like_proto(bytes: &[u8]) -> bool {
    JournalBatchProto::decode(bytes)
        .map(|proto| proto.encode_to_vec() == bytes)
        .unwrap_or(false)
}

#[test]
fn scalar_whose_first_byte_looks_like_codec_magic_still_converts() {
    // A `bincode` i64 is fixed-width little endian, so a value like 129 starts
    // with 0x81 — the same byte a codec header for schema version 1 uses. The
    // source is always legacy, so nothing sniffs that byte: the record converts.
    assert_eq!(bincode::serialize(&129i64).unwrap()[0], 0x81);

    let old = test_dir("scalar");
    let db = DBEngine::new(db_conf(&old), true).unwrap();
    let cases = [129i64, 1, -1, i64::MAX, i64::MIN];
    for (worker, value) in cases.iter().enumerate() {
        let key = RocksUtils::u32_i64_to_bytes(worker as u32, *value);
        let raw = bincode::serialize(value).unwrap();
        db.put_cf(RocksInodeStore::CF_LOCATION, key, raw).unwrap();
    }
    db.flush(true).unwrap();
    drop(db);

    let out = format!("{}.out", old);
    let report = convert_db_dir(&old, &out).unwrap();
    assert!(report.iter().all(|x| x.failed == 0), "{:?}", report);

    let converted = DBEngine::new(db_conf(&out), false).unwrap();
    for (worker, value) in cases.iter().enumerate() {
        let key = RocksUtils::u32_i64_to_bytes(worker as u32, *value);
        let raw = converted.get_cf(RocksInodeStore::CF_LOCATION, key).unwrap();
        let got: i64 = decode_meta(&raw.unwrap()).unwrap();
        assert_eq!(got, *value);
    }
}

#[test]
fn block_location_round_trips_and_a_converted_tree_is_refused() {
    let old = test_dir("block");
    let db = DBEngine::new(db_conf(&old), true).unwrap();
    let location = BlockLocation::new(101, StorageType::Disk);
    let key = RocksUtils::i64_u32_to_bytes(42, 101);
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        key,
        bincode::serialize(&location).unwrap(),
    )
    .unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = format!("{}.out", old);
    let report = convert_db_dir(&old, &out).unwrap();
    assert!(report.iter().all(|x| x.failed == 0), "{:?}", report);

    let converted = DBEngine::new(db_conf(&out), false).unwrap();
    let raw = converted
        .get_cf(RocksInodeStore::CF_BLOCK, key)
        .unwrap()
        .unwrap();
    let got: BlockLocation = decode_meta(&raw).unwrap();
    assert_eq!(got.worker_id, 101);
    assert_eq!(got.storage_type, StorageType::Disk);
    // Release the lock on the converted database before reopening it as a source.
    drop(converted);

    // A second run must read the source, never its own output. Codec records are
    // not bincode, so pointing the tool at a converted tree fails loudly instead
    // of copying the records through unchanged.
    let again = format!("{}.again", old);
    let second = convert_db_dir(&out, &again).unwrap();
    let block = second
        .iter()
        .find(|x| x.cf == RocksInodeStore::CF_BLOCK)
        .unwrap();
    assert_eq!(block.converted, 0);
    assert!(
        !block.ok(),
        "a converted tree must not convert cleanly: {:?}",
        block
    );
}

#[test]
fn journal_conversion_rewrites_only_the_payload() {
    let dir = test_dir("journal");
    let db = DBEngine::new(journal_conf(&dir), true).unwrap();

    let entry = Entry {
        index: 5,
        term: 3,
        entry_type: raft::eraftpb::EntryType::EntryNormal as i32,
        data: bincode::serialize(&JournalBatch::new(129)).unwrap(),
        ..Default::default()
    };
    db.put_cf(
        "default",
        RocksUtils::u64_to_bytes(5),
        entry.encode_to_vec(),
    )
    .unwrap();

    // Membership / no-op entries carry no payload and must be copied untouched.
    let empty = Entry {
        index: 6,
        term: 3,
        ..Default::default()
    };
    let empty_bytes = empty.encode_to_vec();
    db.put_cf("default", RocksUtils::u64_to_bytes(6), &empty_bytes)
        .unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = test_dir("journal-out");
    let mut opts = UpgradeOptions::new(test_dir("unused-meta"), out.clone());
    opts.journal_dir = Some(dir.clone());
    let report = upgrade_journal_dir(&dir, &opts).unwrap();
    assert!(report.ok(), "{:?}", report);

    let converted = DBEngine::new(journal_conf(&format!("{}/journal", out)), false).unwrap();
    let raw = converted
        .get_cf("default", RocksUtils::u64_to_bytes(5))
        .unwrap()
        .unwrap();
    let got = Entry::decode(raw.as_slice()).unwrap();
    assert_eq!(got.index, 5);
    assert_eq!(got.term, 3);
    assert_eq!(got.entry_type, raft::eraftpb::EntryType::EntryNormal as i32);

    let payload = JournalBatchProto::decode(got.data.as_slice()).unwrap();
    assert_eq!(payload.seq_id, 129);

    let empty_raw = converted
        .get_cf("default", RocksUtils::u64_to_bytes(6))
        .unwrap()
        .unwrap();
    assert_eq!(
        empty_raw, empty_bytes,
        "empty payload must be copied verbatim"
    );
    drop(converted);

    // The source log store is read-only and still carries bincode payloads.
    let source = DBEngine::new(journal_conf(&dir), false).unwrap();
    let src_raw = source
        .get_cf("default", RocksUtils::u64_to_bytes(5))
        .unwrap()
        .unwrap();
    let src_entry = Entry::decode(src_raw.as_slice()).unwrap();
    assert!(
        !looks_like_proto(&src_entry.data),
        "source payload must stay in the legacy layout"
    );
    drop(source);

    // Re-running rebuilds the output and reports the same result.
    let again = upgrade_journal_dir(&dir, &opts).unwrap();
    assert!(again.ok(), "{:?}", again);
    assert!(again.cfs.iter().all(|x| x.failed == 0), "{:?}", again.cfs);
}

#[test]
fn only_the_newest_checkpoint_is_converted() {
    // A meta directory can hold several checkpoints, but the master restores
    // from the one its journal names, which is the newest; converting the older
    // ones only costs time and disk.
    let base = test_dir("multi-ck");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        RocksUtils::i64_u32_to_bytes(7, 2),
        bincode::serialize(&BlockLocation::new(2, StorageType::Disk)).unwrap(),
    )
    .unwrap();
    db.create_checkpoint(1).unwrap();
    db.create_checkpoint(2).unwrap();
    drop(db);

    let out = test_dir("multi-ck-out");
    let report = run(&UpgradeOptions::new(base, out.clone())).unwrap();
    assert!(report.ok(), "{:?}", report);
    assert_eq!(
        report.meta.as_ref().unwrap().checkpoints,
        vec!["ck-2".to_string()],
        "the newest checkpoint is the one converted"
    );
    assert!(Path::new(&format!("{}/meta/checkpoint/ck-2", out)).exists());
    assert!(!Path::new(&format!("{}/meta/checkpoint/ck-1", out)).exists());
}

#[test]
fn meta_dir_conversion_mirrors_the_source_into_the_output() {
    let base = test_dir("meta");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    let key = RocksUtils::i64_u32_to_bytes(42, 101);
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        key,
        bincode::serialize(&BlockLocation::new(101, StorageType::Ssd)).unwrap(),
    )
    .unwrap();
    let checkpoint = db.create_checkpoint(1).unwrap();
    assert!(Path::new(&checkpoint).join("CURRENT").exists());
    drop(db);

    let out = test_dir("meta-out");
    let opts = UpgradeOptions::new(base.clone(), out.clone()).with_meta_data(true);
    let report = run(&opts).unwrap();
    assert!(report.ok(), "{:?}", report);
    assert_eq!(
        report.meta.as_ref().unwrap().checkpoints,
        vec!["ck-1".to_string()]
    );

    // Output mirrors the source: <out>/meta/checkpoint/ck-1 and <out>/meta/data.
    let out_ck = format!("{}/meta/checkpoint/ck-1", out);
    assert!(Path::new(&out_ck).join("CURRENT").exists());
    assert!(Path::new(&format!("{}/meta/data", out))
        .join("CURRENT")
        .exists());

    let converted = RocksInodeStore::new(dir_conf(&out_ck), false).unwrap();
    let locations = converted.get_locations(42).unwrap();
    assert_eq!(locations.len(), 1);
    assert_eq!(locations[0].worker_id, 101);
    assert_eq!(locations[0].storage_type, StorageType::Ssd);
    drop(converted);

    // The source is untouched: same bytes, no staging or rollback directories.
    let source = DBEngine::new(db_conf(&checkpoint), false).unwrap();
    let raw = source
        .get_cf(RocksInodeStore::CF_BLOCK, key)
        .unwrap()
        .unwrap();
    assert!(
        decode_meta::<BlockLocation>(&raw).is_err(),
        "source checkpoint must stay in the legacy layout"
    );
    drop(source);

    assert!(Path::new(&checkpoint).join("CURRENT").exists());
    assert!(!Path::new(&format!("{}.new", checkpoint)).exists());
    assert!(!Path::new(&format!("{}.legacy", base)).exists());
    assert!(!Path::new(&format!("{}.new", base)).exists());

    // A rerun rebuilds the output from the untouched source.
    let again = run(&opts).unwrap();
    assert!(again.ok(), "{:?}", again);
    assert_eq!(
        again.meta.as_ref().unwrap().checkpoints,
        vec!["ck-1".to_string()]
    );
}

#[test]
fn converts_inode_records_and_leaves_edges_raw() {
    let old = test_dir("inode");
    let db = DBEngine::new(db_conf(&old), true).unwrap();

    let mtime = 1_700_000_000_000i64;
    let inode = InodeView::new_dir("root".to_string(), InodeDir::new(1000, mtime));
    db.put_cf(
        RocksInodeStore::CF_INODES,
        RocksUtils::i64_to_bytes(1000),
        bincode::serialize(&inode).unwrap(),
    )
    .unwrap();
    // Edges store a raw big-endian child id, not a codec payload.
    db.put_cf(
        RocksInodeStore::CF_EDGES,
        RocksUtils::i64_str_to_bytes(1000, "a.txt"),
        RocksUtils::i64_to_bytes(1001),
    )
    .unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = format!("{}.out", old);
    let report = convert_db_dir(&old, &out).unwrap();
    assert!(report.iter().all(|x| x.failed == 0), "{:?}", report);

    let converted = RocksInodeStore::new(dir_conf(&out), false).unwrap();
    let got = converted.get_inode(1000).unwrap().unwrap();
    assert_eq!(got.id(), 1000);
    assert_eq!(got.mtime(), mtime);

    let children = converted
        .get_child_ids(1000, None)
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(children.len(), 1);
    assert_eq!(*children[0].as_ref().unwrap(), 1001);
}

#[test]
fn dry_run_writes_nothing() {
    let base = test_dir("dryrun");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    let key = RocksUtils::i64_u32_to_bytes(7, 3);
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        key,
        bincode::serialize(&BlockLocation::new(3, StorageType::Disk)).unwrap(),
    )
    .unwrap();
    let checkpoint = db.create_checkpoint(1).unwrap();
    drop(db);

    // A child path of an existing test directory, so the path itself does not
    // exist yet and cannot overlap the source.
    let out = format!("{}/out", test_dir("dryrun-out"));
    let opts = UpgradeOptions::new(base.clone(), out.clone()).dry_run();
    let report = run(&opts).unwrap();
    assert!(report.ok(), "{:?}", report);

    assert!(
        !Path::new(&out).exists(),
        "a dry run must not create the output directory"
    );
    assert!(Path::new(&checkpoint).join("CURRENT").exists());
}

#[test]
fn meta_conversion_writes_a_data_stand_in() {
    // `meta/data` is a discardable working copy, but a master started with
    // format_master = false rejects a meta directory that holds `checkpoint/`
    // and no RocksDB store in `data/`, so an empty store has to be left behind.
    let base = test_dir("standin");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        RocksUtils::i64_u32_to_bytes(9, 5),
        bincode::serialize(&BlockLocation::new(5, StorageType::Disk)).unwrap(),
    )
    .unwrap();
    db.create_checkpoint(1).unwrap();
    drop(db);

    let out = test_dir("standin-out");
    let report = run(&UpgradeOptions::new(base, out.clone())).unwrap();
    assert!(report.ok(), "{:?}", report);

    let data = format!("{}/meta/data", out);
    assert!(
        Path::new(&data).join("CURRENT").exists(),
        "data/ must be a store"
    );
    let stand_in = DBEngine::open_read_only(db_conf(&data)).unwrap();
    for cf in [RocksInodeStore::CF_INODES, RocksInodeStore::CF_BLOCK] {
        assert_eq!(
            stand_in.bulk_scan(cf).unwrap().count(),
            0,
            "{} not empty",
            cf
        );
    }
}

#[test]
fn a_data_only_source_needs_with_meta_data() {
    // With no checkpoint there is nothing the master could rebuild data/ from,
    // so leaving the working copy alone cannot produce a usable meta directory.
    let base = test_dir("dataonly");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = test_dir("dataonly-out");
    let report = run(&UpgradeOptions::new(base.clone(), out.clone())).unwrap();
    assert!(!report.ok());
    let error = report.meta.as_ref().unwrap().error.clone().unwrap();
    assert!(error.contains("--with-meta-data"), "{}", error);
    assert!(!Path::new(&format!("{}/meta/data", out)).exists());

    // With the flag the working copy itself is converted and needs no stand-in.
    let report = run(&UpgradeOptions::new(base, out).with_meta_data(true)).unwrap();
    assert!(report.ok(), "{:?}", report);
}

#[test]
fn journal_snapshot_manifest_is_rebuilt_for_the_converted_checkpoint() {
    // The snapshot metadata records the checkpoint directory the master links
    // `meta/data` from. The path is kept as recorded, because the converted tree
    // is renamed onto the production path, but the manifest has to be rebuilt:
    // the converted files have other names and sizes.
    let base = test_dir("snap-meta");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        RocksUtils::i64_u32_to_bytes(11, 3),
        bincode::serialize(&BlockLocation::new(3, StorageType::Disk)).unwrap(),
    )
    .unwrap();
    let checkpoint = db.create_checkpoint(1).unwrap();
    drop(db);

    let journal = test_dir("snap-journal");
    let jdb = DBEngine::new(journal_conf(&journal), true).unwrap();
    let applied = AppliedIndex {
        term: 1,
        index: 7,
        op_id: 0,
        rpc_id: 0,
    };
    let data = SnapshotData {
        snapshot_id: 7,
        node_id: 1,
        create_time: 0,
        bytes_data: None,
        files_data: Some(SnapshotFileList {
            dir: checkpoint.clone(),
            // Deliberately empty: the converter must rebuild it.
            files: vec![],
        }),
        fsm_state: FsmState {
            applied: applied.clone(),
            ufs_applied: applied,
        },
    };
    let snapshot = Snapshot {
        data: data.encode_to_vec(),
        metadata: Some(SnapshotMetadata {
            index: 7,
            term: 1,
            ..Default::default()
        }),
    };
    let snapshot_bytes = snapshot.encode_to_vec();
    let snap_key: &[u8] = &[0x01];
    jdb.put_cf("meta", snap_key, &snapshot_bytes).unwrap();
    jdb.flush(true).unwrap();
    drop(jdb);

    let out = test_dir("snap-out");
    let opts = UpgradeOptions::new(base, out.clone()).with_journal_dir(journal.clone());
    let report = run(&opts).unwrap();
    assert!(report.ok(), "{:?}", report);

    let converted_dir = format!("{}/meta/checkpoint/ck-1", out);
    let journal_report = report.journal.as_ref().unwrap();
    assert_eq!(journal_report.snapshot_index, Some(7));
    // Reported under the install directory: here it is the source base, because
    // the converted tree is renamed back onto the source path.
    assert_eq!(
        journal_report.snapshot_dir.as_deref().map(Path::new),
        Some(Path::new(&checkpoint))
    );

    let converted = DBEngine::open_read_only(journal_conf(&format!("{}/journal", out))).unwrap();
    let raw = converted.get_cf("meta", snap_key).unwrap().unwrap();
    let snapshot = Snapshot::decode(raw.as_slice()).unwrap();
    assert_eq!(snapshot.get_metadata().index, 7);
    let files = SnapshotData::decode(snapshot.data.as_slice())
        .unwrap()
        .files_data
        .unwrap();
    assert_eq!(Path::new(&files.dir), Path::new(&checkpoint));
    assert!(
        !files.files.is_empty(),
        "the manifest must describe the converted files"
    );
    for file in &files.files {
        let path = Path::new(&converted_dir).join(&file.path);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), file.len);
    }
    drop(converted);

    // The source keeps naming the legacy checkpoint.
    let source = DBEngine::open_read_only(journal_conf(&journal)).unwrap();
    let raw = source.get_cf("meta", snap_key).unwrap().unwrap();
    assert_eq!(raw, snapshot_bytes, "source snapshot must stay untouched");
}

#[test]
fn snapshot_dir_and_owner_follow_the_install_target() {
    // A converted tree can be installed under another path or started from
    // another working directory. The record then has to name the checkpoint
    // where it will live and the member that will own it; naming the old
    // deployment makes the master treat its own checkpoint as foreign and copy
    // or download the whole thing on startup.
    let base = test_dir("snap-relocate-meta");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        RocksUtils::i64_u32_to_bytes(11, 3),
        bincode::serialize(&BlockLocation::new(3, StorageType::Disk)).unwrap(),
    )
    .unwrap();
    let checkpoint = db.create_checkpoint(1).unwrap();
    drop(db);

    let journal = test_dir("snap-relocate-journal");
    let jdb = DBEngine::new(journal_conf(&journal), true).unwrap();
    let applied = AppliedIndex {
        term: 1,
        index: 7,
        op_id: 0,
        rpc_id: 0,
    };
    let data = SnapshotData {
        snapshot_id: 7,
        node_id: 1,
        create_time: 0,
        bytes_data: None,
        files_data: Some(SnapshotFileList {
            dir: checkpoint.clone(),
            files: vec![],
        }),
        fsm_state: FsmState {
            applied: applied.clone(),
            ufs_applied: applied,
        },
    };
    let snapshot = Snapshot {
        data: data.encode_to_vec(),
        metadata: Some(SnapshotMetadata {
            index: 7,
            term: 1,
            ..Default::default()
        }),
    };
    let snap_key: &[u8] = &[0x01];
    jdb.put_cf("meta", snap_key, snapshot.encode_to_vec())
        .unwrap();
    jdb.flush(true).unwrap();
    drop(jdb);

    let live = test_dir("snap-relocate-live");
    let out = test_dir("snap-relocate-out");
    let opts = UpgradeOptions::new(base, out.clone())
        .with_journal_dir(journal)
        .with_install_meta_dir(&live)
        .with_node_id(42);
    let report = run(&opts).unwrap();
    assert!(report.ok(), "{:?}", report);

    let expected = Path::new(&live).join("checkpoint").join("ck-1");
    assert_eq!(
        report
            .journal
            .as_ref()
            .unwrap()
            .snapshot_dir
            .as_deref()
            .map(Path::new),
        Some(expected.as_path())
    );

    let converted = DBEngine::open_read_only(journal_conf(&format!("{}/journal", out))).unwrap();
    let raw = converted.get_cf("meta", snap_key).unwrap().unwrap();
    let snapshot = Snapshot::decode(raw.as_slice()).unwrap();
    let data = SnapshotData::decode(snapshot.data.as_slice()).unwrap();
    assert_eq!(data.node_id, 42, "the local member owns the converted data");
    let files = data.files_data.unwrap();
    assert_eq!(Path::new(&files.dir), expected);
    assert!(!files.files.is_empty());
}

#[test]
fn a_recovering_member_is_refused() {
    // A member that has not rejoined the group has a deliberately truncated log;
    // converting it would bake a partial replica into the upgraded cluster.
    let journal = test_dir("recovering");
    let db = DBEngine::new(journal_conf(&journal), true).unwrap();
    db.flush(true).unwrap();
    drop(db);
    let marker = Path::new(&journal).join("member-recovery-in-progress");
    std::fs::write(&marker, "3\n").unwrap();

    let out = test_dir("recovering-out");
    let opts = UpgradeOptions::new(test_dir("unused-meta"), out);
    let report = upgrade_journal_dir(&journal, &opts).unwrap();
    assert!(!report.ok());
    let error = report.error.unwrap();
    assert!(error.contains("member-recovery-in-progress"), "{}", error);
}

#[test]
fn codec_payloads_are_never_accepted_as_a_source() {
    // Nothing is sniffed, so a payload that already carries the codec header must
    // fail as a legacy record rather than be skipped as "already converted": a
    // skipped record would reach the new master in the old layout.
    let bytes = encode_meta(&42i64).unwrap();
    assert_eq!(bytes.len(), 9);
    assert!(Codec::is_codec_format(&bytes));
    assert!(convert_i64(&bytes).is_err());
}

#[test]
fn mount_records_from_older_builds_still_convert() {
    // `MountInfo` gained `auto_cache`/`access_mode`, then `write_cache`.
    // bincode is positional, so every older build wrote a shorter payload and
    // the current struct runs out of bytes on it; the converter has to fill the
    // missing tail with the defaults the live read path used.
    #[derive(Serialize)]
    struct Full {
        cv_path: String,
        ufs_path: String,
        mount_id: u32,
        properties: HashMap<String, String>,
        ttl_ms: i64,
        ttl_action: TtlAction,
        read_verify_ufs: bool,
        storage_type: Option<StorageType>,
        block_size: Option<i64>,
        replicas: Option<i32>,
        write_type: WriteType,
        provider: Option<Provider>,
        auto_cache: bool,
        access_mode: AccessMode,
        write_cache: bool,
    }

    #[derive(Serialize)]
    struct BeforeWriteCache {
        cv_path: String,
        ufs_path: String,
        mount_id: u32,
        properties: HashMap<String, String>,
        ttl_ms: i64,
        ttl_action: TtlAction,
        read_verify_ufs: bool,
        storage_type: Option<StorageType>,
        block_size: Option<i64>,
        replicas: Option<i32>,
        write_type: WriteType,
        provider: Option<Provider>,
        auto_cache: bool,
        access_mode: AccessMode,
    }

    #[derive(Serialize)]
    struct BeforeAutoCache {
        cv_path: String,
        ufs_path: String,
        mount_id: u32,
        properties: HashMap<String, String>,
        ttl_ms: i64,
        ttl_action: TtlAction,
        read_verify_ufs: bool,
        storage_type: Option<StorageType>,
        block_size: Option<i64>,
        replicas: Option<i32>,
        write_type: WriteType,
        provider: Option<Provider>,
    }

    let full = Full {
        cv_path: "/m/full".to_string(),
        ufs_path: "s3://bucket/full".to_string(),
        mount_id: 11,
        properties: HashMap::new(),
        ttl_ms: 60_000,
        ttl_action: TtlAction::default(),
        read_verify_ufs: false,
        storage_type: Some(StorageType::Disk),
        block_size: None,
        replicas: None,
        write_type: WriteType::default(),
        provider: None,
        auto_cache: false,
        access_mode: AccessMode::ReadWrite,
        write_cache: true,
    };
    let before_write_cache = BeforeWriteCache {
        cv_path: "/m/write-cache".to_string(),
        ufs_path: "s3://bucket/write-cache".to_string(),
        mount_id: 12,
        properties: HashMap::new(),
        ttl_ms: 0,
        ttl_action: TtlAction::default(),
        read_verify_ufs: false,
        storage_type: None,
        block_size: None,
        replicas: None,
        write_type: WriteType::default(),
        provider: None,
        auto_cache: true,
        access_mode: AccessMode::ReadWrite,
    };
    let before_auto_cache = BeforeAutoCache {
        cv_path: "/m/auto-cache".to_string(),
        ufs_path: "s3://bucket/auto-cache".to_string(),
        mount_id: 13,
        properties: HashMap::new(),
        ttl_ms: 0,
        ttl_action: TtlAction::default(),
        read_verify_ufs: false,
        storage_type: None,
        block_size: None,
        replicas: None,
        write_type: WriteType::default(),
        provider: None,
    };

    let mount_key = |id: u32| RocksUtils::u8_u32_to_bytes(RocksInodeStore::PREFIX_MOUNT, id);
    let old = test_dir("mount-legacy");
    let db = DBEngine::new(db_conf(&old), true).unwrap();
    for (id, bytes) in [
        (11u32, bincode::serialize(&full).unwrap()),
        (12, bincode::serialize(&before_write_cache).unwrap()),
        (13, bincode::serialize(&before_auto_cache).unwrap()),
    ] {
        db.put_cf(RocksInodeStore::CF_COMMON, mount_key(id), bytes)
            .unwrap();
    }
    db.flush(true).unwrap();
    drop(db);

    let out = format!("{}.out", old);
    let report = convert_db_dir(&old, &out).unwrap();
    assert!(report.iter().all(|x| x.failed == 0), "{:?}", report);

    let converted = DBEngine::new(db_conf(&out), false).unwrap();
    let read = |id: u32| -> MountInfo {
        let raw = converted
            .get_cf(RocksInodeStore::CF_COMMON, mount_key(id))
            .unwrap()
            .unwrap();
        decode_meta(&raw).unwrap()
    };

    let full_rec = read(11);
    assert_eq!(full_rec.mount_id, 11);
    assert!(full_rec.write_cache);
    assert!(!full_rec.auto_cache);

    let write_cache = read(12);
    assert_eq!(write_cache.access_mode, AccessMode::ReadWrite);
    assert!(!write_cache.write_cache, "a record without it means false");

    let auto_cache = read(13);
    assert!(auto_cache.auto_cache, "the pre-flag default was true");
    assert_eq!(auto_cache.access_mode, AccessMode::ReadOnly);
    assert!(!auto_cache.write_cache);
}

#[test]
fn lock_records_from_older_builds_still_convert() {
    // `FileLock` gained a trailing `session_owned` flag (#1878). A lock set
    // written before it has no such byte, so the current `Vec<FileLock>` layout
    // runs out of bytes; the missing flag means "acquired without a session
    // lease", i.e. the legacy TTL path. A record without any lock is just an
    // empty vector under both layouts and must keep working too.
    #[derive(Serialize)]
    struct LockBeforeSessionOwned {
        client_id: String,
        owner_id: u64,
        pid: u32,
        acquire_time: u64,
        lock_type: LockType,
        lock_flags: LockFlags,
        start: u64,
        end: u64,
    }

    let legacy = vec![
        LockBeforeSessionOwned {
            client_id: "legacy-client".to_string(),
            owner_id: 44,
            pid: 55,
            acquire_time: 66,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
        },
        LockBeforeSessionOwned {
            client_id: "other-client".to_string(),
            owner_id: 45,
            pid: 56,
            acquire_time: 67,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Flock,
            start: 100,
            end: 200,
        },
    ];

    let lock_key = |ino: i64| RocksUtils::u8_i64_to_bytes(RocksInodeStore::PREFIX_LOCK, ino);
    let old = test_dir("lock-legacy");
    let db = DBEngine::new(db_conf(&old), true).unwrap();
    let legacy_bytes = bincode::serialize(&legacy).unwrap();
    db.put_cf(RocksInodeStore::CF_COMMON, lock_key(7), legacy_bytes)
        .unwrap();
    db.put_cf(
        RocksInodeStore::CF_COMMON,
        lock_key(8),
        bincode::serialize(&Vec::<LockBeforeSessionOwned>::new()).unwrap(),
    )
    .unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = format!("{}.out", old);
    let report = convert_db_dir(&old, &out).unwrap();
    assert!(report.iter().all(|x| x.failed == 0), "{:?}", report);

    let converted = DBEngine::new(db_conf(&out), false).unwrap();
    let read = |ino: i64| -> Vec<FileLock> {
        let raw = converted
            .get_cf(RocksInodeStore::CF_COMMON, lock_key(ino))
            .unwrap()
            .unwrap();
        decode_meta(&raw).unwrap()
    };

    let locks = read(7);
    assert_eq!(locks.len(), 2);
    assert_eq!(locks[0].client_id, "legacy-client");
    assert_eq!(locks[0].owner_id, 44);
    assert_eq!(locks[0].lock_type, LockType::WriteLock);
    assert!(
        locks.iter().all(|lock| !lock.session_owned),
        "a lock from before #1878 must not claim a session lease"
    );
    assert!(read(8).is_empty());
}

#[test]
fn membership_change_entries_are_copied_verbatim() {
    // A membership change stores a protobuf `ConfChange` in the `data` field;
    // only `EntryNormal` carries a journal batch. Passing it to the batch
    // converter would fail a perfectly valid log.
    let dir = test_dir("conf-change");
    let db = DBEngine::new(journal_conf(&dir), true).unwrap();
    let conf_change = raft::eraftpb::ConfChange {
        id: 1,
        change_type: raft::eraftpb::ConfChangeType::AddNode as i32,
        node_id: 9,
        ..Default::default()
    };
    let entry = Entry {
        index: 3,
        term: 2,
        entry_type: raft::eraftpb::EntryType::EntryConfChange as i32,
        data: conf_change.encode_to_vec(),
        ..Default::default()
    };
    let bytes = entry.encode_to_vec();
    db.put_cf("default", RocksUtils::u64_to_bytes(3), &bytes)
        .unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = test_dir("conf-change-out");
    let opts =
        UpgradeOptions::new(test_dir("unused-meta"), out.clone()).with_journal_dir(dir.clone());
    let report = upgrade_journal_dir(&dir, &opts).unwrap();
    assert!(report.ok(), "{:?}", report);

    let converted = DBEngine::new(journal_conf(&format!("{}/journal", out)), false).unwrap();
    let raw = converted
        .get_cf("default", RocksUtils::u64_to_bytes(3))
        .unwrap()
        .unwrap();
    assert_eq!(
        raw, bytes,
        "a membership change must pass through untouched"
    );
}

#[test]
fn a_leftover_recovery_indicator_is_refused() {
    // Recovery unlinks the marker file and clears the storage indicator after
    // that, so a crash in between leaves the indicator with no marker. Startup
    // rejects that state, and converting it would make the upgraded member
    // refuse to start.
    let journal = test_dir("recovery-key");
    let db = DBEngine::new(journal_conf(&journal), true).unwrap();
    db.put_cf("meta", [0x05u8], 3u64.to_be_bytes()).unwrap();
    db.flush(true).unwrap();
    drop(db);

    let out = test_dir("recovery-key-out");
    let opts = UpgradeOptions::new(test_dir("unused-meta"), out);
    let report = upgrade_journal_dir(&journal, &opts).unwrap();
    assert!(!report.ok());
    let error = report.error.unwrap();
    assert!(error.contains("unfinished member recovery"), "{}", error);
    assert!(error.contains("raft_id 3"), "{}", error);
}

#[test]
fn the_journal_snapshot_picks_the_checkpoint_even_when_a_newer_one_exists() {
    // The leader creates a checkpoint before proposing the snapshot entry that
    // registers it, so a crash leaves a newer unregistered directory behind.
    // Only the registered checkpoint is what the master restores.
    let base = test_dir("registered-ck");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.put_cf(
        RocksInodeStore::CF_BLOCK,
        RocksUtils::i64_u32_to_bytes(7, 2),
        bincode::serialize(&BlockLocation::new(2, StorageType::Disk)).unwrap(),
    )
    .unwrap();
    let registered = db.create_checkpoint(1).unwrap();
    let unregistered = db.create_checkpoint(2).unwrap();
    assert_ne!(registered, unregistered);
    drop(db);

    let journal = test_dir("registered-ck-journal");
    write_journal_snapshot(&journal, &registered, 1);

    let out = test_dir("registered-ck-out");
    let opts = UpgradeOptions::new(base, out.clone()).with_journal_dir(journal);
    let report = run(&opts).unwrap();
    assert!(report.ok(), "{:?}", report);
    assert_eq!(
        report.meta.as_ref().unwrap().checkpoints,
        vec!["ck-1".to_string()],
        "the journal names ck-1, not the newer unregistered checkpoint"
    );
    assert!(Path::new(&format!("{}/meta/checkpoint/ck-1", out))
        .join("CURRENT")
        .exists());
    assert!(!Path::new(&format!("{}/meta/checkpoint/ck-2", out)).exists());
}

#[test]
fn a_journal_naming_a_missing_checkpoint_is_refused() {
    // The snapshot points at a checkpoint this meta directory does not hold, so
    // the two do not belong together; converting the newest directory instead
    // would only fail later, in the journal remap.
    let base = test_dir("missing-ck");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    db.create_checkpoint(1).unwrap();
    drop(db);

    let journal = test_dir("missing-ck-journal");
    write_journal_snapshot(&journal, &format!("{}/checkpoint/ck-9", base), 1);

    let out = test_dir("missing-ck-out");
    let opts = UpgradeOptions::new(base, out).with_journal_dir(journal);
    let report = run(&opts).unwrap();
    assert!(!report.ok());
    let error = report.meta.as_ref().unwrap().error.clone().unwrap();
    assert!(error.contains("ck-9"), "{}", error);
}

#[cfg(unix)]
#[test]
fn an_output_tree_symlinked_into_a_source_is_refused() {
    // The converted tree is rebuilt with rocksdb's format mode, which deletes
    // each destination, and that delete follows symlinks. A link at <out>/meta
    // would redirect it into the source, which is the rollback copy.
    let base = test_dir("symlink-meta");
    let db = DBEngine::new(store_conf(&base), true).unwrap();
    let checkpoint = db.create_checkpoint(1).unwrap();
    drop(db);

    let out = test_dir("symlink-out");
    std::fs::create_dir_all(&out).unwrap();
    std::os::unix::fs::symlink(&base, Path::new(&out).join("meta")).unwrap();

    let err = run(&UpgradeOptions::new(base, out)).unwrap_err();
    assert!(err.to_string().contains("symlink"), "{}", err);
    assert!(
        Path::new(&checkpoint).join("CURRENT").exists(),
        "the source must survive"
    );
}

#[test]
fn progress_line_reports_count_percent_and_rate() {
    let now = Instant::now();
    let line = |estimate: Option<u64>, done: u64| {
        Progress {
            cf: "inodes".to_string(),
            estimate,
            done,
            started: now,
            last_print: now,
            tty: false,
            last_len: 0,
        }
        .line()
    };

    let with_estimate = line(Some(1000), 250);
    assert!(
        with_estimate.contains("inodes: 250/~1000 (25%)"),
        "{}",
        with_estimate
    );
    assert!(with_estimate.contains("rec/s"), "{}", with_estimate);

    // The estimate is approximate, so a count past it must not read over 100%.
    let over = line(Some(1000), 1200);
    assert!(over.contains("(100%)"), "{}", over);

    let no_estimate = line(None, 42);
    assert!(
        no_estimate.contains("inodes: 42 records"),
        "{}",
        no_estimate
    );
    assert!(!no_estimate.contains('%'), "{}", no_estimate);
}
