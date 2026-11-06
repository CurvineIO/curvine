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

use crate::master::journal::JournalSystem;
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::store::{InodeStore, RocksInodeStore};
use crate::master::meta::FsDir;
use crate::master::metadata_codec::MetadataCodec;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_rocksdb::RocksUtils;
use curvine_runtime::common::Utils;
use std::sync::Arc;

#[test]
fn inode_snapshot_rejects_key_and_payload_id_mismatch() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("inode-snapshot-mismatch-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let first = fs.create("/first", false).unwrap().id;
    let second = fs.create("/second", false).unwrap().id;
    assert_ne!(first, second);
    {
        let dir = fs.fs_dir.read();
        let store = dir.store_ref();
        let inode = store.get_inode(second, None).unwrap().unwrap();
        let mut batch = store.new_batch();
        batch
            .put_cf_raw(
                RocksInodeStore::CF_INODES,
                RocksUtils::i64_to_bytes(first),
                MetadataCodec::encode(&inode).unwrap(),
            )
            .unwrap();
        batch.commit().unwrap();
    }
    {
        let dir = fs.fs_dir.read();
        let error = dir.store_ref().store().get_inode(first).err().unwrap();
        assert!(error.to_string().contains("inode record id mismatch"));
    }
    // The store-snapshot readers own corruption detection: every path that
    // hydrates rows from the store reports the id mismatch. The live-tree
    // reads surface it whenever the touched child hydrates; children stored
    // as full views answer from memory without touching the store.
    {
        let dir = fs.fs_dir.read();
        let lease = dir.begin_snapshot_scan().unwrap();
        let snapshot = lease.snapshot_reader().unwrap();
        let error = match snapshot.resolve_path("/first") {
            Err(error) => error,
            Ok(_) => panic!("snapshot reader must detect the mismatch at /first"),
        };
        assert!(error.to_string().contains("inode record id mismatch"), "{error}");
        assert!(snapshot.resolve_path("/second").unwrap().is_full());
    }
    for error in [
        fs.exists("/first").err(),
        fs.file_status_by_id("/first", Some(first)).err(),
        fs.list_status("/").err(),
    ]
    .into_iter()
    .flatten()
    {
        let message = error.to_string();
        assert!(
            message.contains("inode record id mismatch")
                || message.contains("inode id mismatch"),
            "live read error: {message}"
        );
    }
    let error = fs
        .create_tree()
        .err()
        .expect("corrupt inode must fail restore");
    assert!(error.to_string().contains("inode record id mismatch"));

    // Exercise the parallel deserialization path when more than one CPU is available.
    for i in 0..1000 {
        fs.create(format!("/other-{i}"), false).unwrap();
    }
    let error = fs
        .create_tree()
        .err()
        .expect("corrupt inode must fail restore");
    assert!(error.to_string().contains("inode record id mismatch"));
}

#[test]
fn directory_attribute_migration_rejects_mismatched_inode() {
    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("inode-migration-mismatch-{}", Utils::rand_str(8)));
    let rocks = RocksInodeStore::new(curvine_config::DBConf::new(&path), true).unwrap();
    let mut batch = rocks.new_batch();
    batch
        .put_cf_raw(
            RocksInodeStore::CF_INODES,
            RocksUtils::i64_to_bytes(123),
            MetadataCodec::encode(&FsDir::create_root()).unwrap(),
        )
        .unwrap();
    batch.commit().unwrap();
    let store = InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000).unwrap())).unwrap();
    let error = store.migrate_directory_attributes().unwrap_err();
    assert!(error.to_string().contains("inode record id mismatch"));
    assert!(!store.store().directory_attributes_migrated().unwrap());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
