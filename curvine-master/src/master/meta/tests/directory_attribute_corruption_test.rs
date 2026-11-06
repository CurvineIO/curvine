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

use crate::master::meta::store::RocksInodeStore;
use curvine_config::DBConf;
use curvine_model::DirectoryAttributes;
use curvine_rocksdb::RocksUtils;
use curvine_runtime::common::Utils;

#[test]
fn explicit_write_does_not_overwrite_corrupt_directory_attributes() {
    let path = Utils::test_sub_dir(format!("dir-attributes-corrupt-{}", Utils::rand_str(12)));
    let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let mut batch = store.new_batch();
    let mut unknown_format = [0; 40];
    unknown_format[..4].copy_from_slice(b"CDA5");
    let mut negative_count = DirectoryAttributes::new(0, 0, 2);
    negative_count.children = Some(-1);
    for (id, bytes) in [
        (1, b"corrupt".as_slice()),
        (2, unknown_format.as_slice()),
        (3, negative_count.encode().as_slice()),
    ] {
        batch
            .put_cf_raw(
                RocksInodeStore::CF_DIR_ATTRS,
                RocksUtils::i64_to_bytes(id),
                bytes,
            )
            .unwrap();
    }
    batch.commit().unwrap();

    for id in 1..=3 {
        let mut batch = store.new_batch();
        let error = batch
            .write_directory_attributes(id, DirectoryAttributes::new(0, 0, 2))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid directory attributes record"));
        drop(batch);
        assert!(store.get_directory_attributes(id).is_err());
    }
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn tree_restore_fails_when_directory_attribute_column_is_unavailable() {
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::meta::store::InodeStore;
    use crate::master::Master;
    use curvine_rocksdb::DBEngine;
    use std::sync::Arc;

    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("missing-dir-attrs-{}", Utils::rand_str(12)));
    // Construct an unavailable column family directly; normal stores always
    // register this CF, including when opening a legacy database.
    let conf = DBConf::new(&path)
        .add_cf(RocksInodeStore::CF_INODES)
        .add_cf(RocksInodeStore::CF_EDGES);
    let rocks = RocksInodeStore {
        db: DBEngine::new(conf, true).unwrap(),
    };
    let store = InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000).unwrap())).unwrap();
    let error = store
        .create_tree()
        .err()
        .expect("missing CF must fail restore");
    assert!(error.to_string().contains(RocksInodeStore::CF_DIR_ATTRS));
    drop(store);

    // Normal opening adds the missing CF, preserving legacy-store support.
    let rocks = RocksInodeStore::new(DBConf::new(&path), false).unwrap();
    let store = InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000).unwrap())).unwrap();
    store.migrate_directory_attributes().unwrap();
    store.create_tree().unwrap();
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
