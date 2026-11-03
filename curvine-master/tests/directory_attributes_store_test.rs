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

use curvine_config::{CfMergeOperator, DBConf};
use curvine_master::master::meta::inode::ttl::TtlBucketList;
use curvine_master::master::meta::inode::{InodeDir, InodeFile, InodeView};
use curvine_master::master::meta::store::{InodeStore, RocksInodeStore};
use curvine_master::master::Master;
use curvine_model::{DirectoryAttributeDelta, DirectoryAttributes};
use curvine_rocksdb::RocksUtils;
use curvine_runtime::common::Utils;

#[test]
fn directory_attributes_and_edges_share_one_batch() {
    let path = Utils::test_sub_dir(format!("dir_attrs_store_{}", Utils::rand_str(12)));
    let conf = DBConf::new(&path)
        .add_cf(RocksInodeStore::CF_DIR_ATTRS)
        .set_cf_merge_operator(
            RocksInodeStore::CF_DIR_ATTRS,
            CfMergeOperator::DirectoryAttributes,
        );
    let store = RocksInodeStore::new(conf, true).unwrap();
    let base = DirectoryAttributes::new(1, 2, 2);
    let mut batch = store.new_batch();
    batch.write_directory_attributes(1, base).unwrap();
    batch.add_child(1, "child", 2).unwrap();
    batch
        .merge_directory_attributes(1, DirectoryAttributeDelta::new(3, 4, 1))
        .unwrap();
    assert_eq!(store.get_directory_attributes(1).unwrap(), None);
    assert_eq!(store.get_child_ids(1, None).unwrap().count(), 0);
    let snapshot = store.read_snapshot();
    batch.commit().unwrap();
    assert_eq!(
        snapshot
            .get_cf(RocksInodeStore::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(1))
            .unwrap(),
        None
    );
    assert_eq!(
        snapshot
            .get_cf(
                RocksInodeStore::CF_EDGES,
                RocksUtils::i64_str_to_bytes(1, "child")
            )
            .unwrap(),
        None
    );
    drop(snapshot);
    let mut expected = base;
    expected
        .apply(DirectoryAttributeDelta::new(3, 4, 1))
        .unwrap();
    assert_eq!(store.get_directory_attributes(1).unwrap(), Some(expected));
    assert_eq!(
        store
            .get_child_ids(1, None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![2]
    );
    let mut batch = store.new_batch();
    batch.delete_directory_attributes(1).unwrap();
    batch.delete_child(1, "child").unwrap();
    batch.commit().unwrap();
    assert_eq!(store.get_directory_attributes(1).unwrap(), None);
    assert_eq!(store.get_child_ids(1, None).unwrap().count(), 0);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn tree_restore_scans_persisted_directory_attributes_without_attr_column_family() {
    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("dir_attrs_restore_{}", Utils::rand_str(12)));
    let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let store = InodeStore::new(
        store,
        std::sync::Arc::new(TtlBucketList::new(60_000).unwrap()),
    )
    .unwrap();
    let (last_inode_id, root) = store.create_tree().unwrap();
    assert_eq!(
        last_inode_id,
        curvine_master::master::meta::inode::ROOT_INODE_ID
    );
    assert!(root.as_dir_ref().is_ok());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn snapshot_directory_child_is_independent_from_in_memory_tree() {
    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("dir_attrs_tree_{}", Utils::rand_str(12)));
    let conf = DBConf::new(&path)
        .add_cf(RocksInodeStore::CF_DIR_ATTRS)
        .set_cf_merge_operator(
            RocksInodeStore::CF_DIR_ATTRS,
            CfMergeOperator::DirectoryAttributes,
        );
    let rocks = RocksInodeStore::new(conf, true).unwrap();
    let store = InodeStore::new(
        rocks,
        std::sync::Arc::new(TtlBucketList::new(60_000).unwrap()),
    )
    .unwrap();
    let mut batch = store.store().new_batch();
    batch
        .write_inode(&InodeView::new_dir(
            "parent".to_owned(),
            InodeDir::new(1, 1),
        ))
        .unwrap();
    batch
        .write_inode(&InodeView::new_file(
            "stale".to_owned(),
            InodeFile::new(42, 1),
        ))
        .unwrap();
    batch.add_child(1, "child", 42).unwrap();
    batch.commit().unwrap();

    let lease = store.begin_snapshot_scan().unwrap();
    let snapshot = lease.snapshot_reader().unwrap();
    let child = snapshot.get_child(1, "child").unwrap().unwrap();
    assert!(child.is_file());
    assert!(snapshot.get_directory_attributes(1).unwrap().is_none());
    drop(snapshot);
    drop(lease);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_snapshot_keeps_directory_attributes_at_its_read_version() {
    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("dir_attrs_snapshot_{}", Utils::rand_str(12)));
    let conf = DBConf::new(&path)
        .add_cf(RocksInodeStore::CF_DIR_ATTRS)
        .set_cf_merge_operator(
            RocksInodeStore::CF_DIR_ATTRS,
            CfMergeOperator::DirectoryAttributes,
        );
    let rocks = RocksInodeStore::new(conf, true).unwrap();
    let store = InodeStore::new(
        rocks,
        std::sync::Arc::new(TtlBucketList::new(60_000).unwrap()),
    )
    .unwrap();
    let base = DirectoryAttributes::new(1, 2, 2);
    let mut batch = store.store().new_batch();
    batch.write_directory_attributes(1, base).unwrap();
    batch
        .write_inode(&InodeView::new_file(
            "stale".to_owned(),
            InodeFile::new(42, 1),
        ))
        .unwrap();
    batch.add_child(1, "old", 42).unwrap();
    batch.commit().unwrap();
    let lease = store.begin_snapshot_scan().unwrap();
    let snapshot = lease.snapshot_reader().unwrap();
    let mut batch = store.store().new_batch();
    batch
        .merge_directory_attributes(1, DirectoryAttributeDelta::new(3, 4, 1))
        .unwrap();
    batch.delete_child(1, "old").unwrap();
    batch.add_child(1, "new", 42).unwrap();
    batch.add_child(1, "dangling", 99).unwrap();
    batch.commit().unwrap();
    assert_eq!(snapshot.get_directory_attributes(1).unwrap(), Some(base));
    assert_eq!(snapshot.get_child_id(1, "old").unwrap(), Some(42));
    let old = snapshot.get_child(1, "old").unwrap().unwrap();
    assert_eq!((old.id(), old.name()), (42, "old"));
    assert!(snapshot.get_child(1, "new").unwrap().is_none());
    let updated_snapshot = lease.snapshot_reader().unwrap();
    assert_eq!(updated_snapshot.get_child_id(1, "old").unwrap(), None);
    assert_eq!(updated_snapshot.get_child_id(1, "new").unwrap(), Some(42));
    let new = updated_snapshot.get_child(1, "new").unwrap().unwrap();
    assert_eq!((new.id(), new.name()), (42, "new"));
    assert!(updated_snapshot.get_child(1, "dangling").is_err());
    drop(updated_snapshot);
    let mut updated = base;
    updated
        .apply(DirectoryAttributeDelta::new(3, 4, 1))
        .unwrap();
    assert_eq!(
        store.store().get_directory_attributes(1).unwrap(),
        Some(updated)
    );
    drop(snapshot);
    drop(lease);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn older_configuration_cannot_open_database_with_attribute_column_family() {
    let path = Utils::test_sub_dir(format!("dir_attrs_rollback_{}", Utils::rand_str(12)));
    let base = DBConf::new(&path);
    let conf = base
        .clone()
        .add_cf(RocksInodeStore::CF_DIR_ATTRS)
        .set_cf_merge_operator(
            RocksInodeStore::CF_DIR_ATTRS,
            CfMergeOperator::DirectoryAttributes,
        );
    let store = RocksInodeStore::new(conf, true).unwrap();
    drop(store);
    assert!(RocksInodeStore::new(base, false).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn missing_attribute_column_family_fails_closed() {
    let path = Utils::test_sub_dir(format!("dir_attrs_missing_{}", Utils::rand_str(12)));
    let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    assert!(store.get_directory_attributes(1).is_err());
    let mut batch = store.new_batch();
    assert!(batch
        .write_directory_attributes(1, DirectoryAttributes::new(1, 2, 2))
        .is_err());
    drop(batch);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
