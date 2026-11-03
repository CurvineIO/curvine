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

use curvine_config::CfMergeOperator;
use curvine_model::{DirectoryAttributeDelta, DirectoryAttributes};
use curvine_rocksdb::{configure_directory_attributes_merge, DBConf, DBEngine, WriteBatch};
use curvine_runtime::common::Utils;
use rocksdb::{Options, DB};

#[test]
fn configured_column_family_supports_batch_merge_and_reopen() {
    let path = Utils::test_sub_dir(format!("directory_cf_merge_{}", Utils::rand_str(12)));
    let conf = DBConf::new(&path)
        .add_cf("attributes")
        .set_cf_merge_operator("attributes", CfMergeOperator::DirectoryAttributes);
    let db = DBEngine::new(conf.clone(), false).unwrap();
    let base = DirectoryAttributes::new(1, 2, 3);
    let delta = DirectoryAttributeDelta::new(10, 11, 1).with_base(base);
    let mut batch = WriteBatch::new(&db);
    batch
        .merge_cf("attributes", b"key", delta.encode())
        .unwrap();
    batch.put_cf("default", b"other", b"value").unwrap();
    batch.commit().unwrap();
    let mut expected = base;
    expected.apply(delta).unwrap();
    assert_eq!(
        DirectoryAttributes::decode(&db.get_cf("attributes", b"key").unwrap().unwrap()),
        Some(expected)
    );
    assert_eq!(
        db.get_cf("default", b"other").unwrap(),
        Some(b"value".to_vec())
    );
    drop(db);

    let reopened = DBEngine::new(conf, false).unwrap();
    assert_eq!(
        DirectoryAttributes::decode(&reopened.get_cf("attributes", b"key").unwrap().unwrap()),
        Some(expected)
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn partial_merge_must_not_hide_intermediate_underflow() {
    let path = Utils::test_sub_dir(format!("directory_underflow_{}", Utils::rand_str(12)));
    let mut options = Options::default();
    options.create_if_missing(true);
    configure_directory_attributes_merge(&mut options);
    let db = DB::open(&options, &path).unwrap();
    db.put(b"key", DirectoryAttributes::new(0, 0, 0).encode())
        .unwrap();
    db.put(
        b"overflow",
        DirectoryAttributes::new(0, 0, u32::MAX).encode(),
    )
    .unwrap();
    db.put(b"valid", DirectoryAttributes::new(0, 0, 1).encode())
        .unwrap();
    db.flush().unwrap();
    db.merge(b"key", DirectoryAttributeDelta::new(1, 1, -1).encode())
        .unwrap();
    db.merge(b"key", DirectoryAttributeDelta::new(2, 2, 1).encode())
        .unwrap();
    db.merge(b"overflow", DirectoryAttributeDelta::new(1, 1, 1).encode())
        .unwrap();
    db.merge(b"overflow", DirectoryAttributeDelta::new(2, 2, -1).encode())
        .unwrap();
    db.merge(b"valid", DirectoryAttributeDelta::new(1, 1, -1).encode())
        .unwrap();
    db.merge(b"valid", DirectoryAttributeDelta::new(2, 2, 1).encode())
        .unwrap();
    db.flush().unwrap();
    db.compact_range::<&[u8], &[u8]>(None, None);
    assert!(db.get(b"key").is_err());
    assert!(db.get(b"overflow").is_err());
    let valid = DirectoryAttributes::decode(&db.get(b"valid").unwrap().unwrap()).unwrap();
    assert_eq!((valid.nlink, valid.version), (1, 2));
    drop(db);
    DB::destroy(&options, &path).unwrap();
}

#[test]
fn concurrent_merges_preserve_all_updates() {
    let path = Utils::test_sub_dir(format!("directory_concurrent_{}", Utils::rand_str(12)));
    let mut options = Options::default();
    options.create_if_missing(true);
    configure_directory_attributes_merge(&mut options);
    let db = std::sync::Arc::new(DB::open(&options, &path).unwrap());
    db.put(b"key", DirectoryAttributes::new(0, 0, 0).encode())
        .unwrap();
    let threads: Vec<_> = (0..8)
        .map(|thread| {
            let db = db.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let delta = DirectoryAttributeDelta::new(thread, thread, 1);
                    db.merge(b"key", delta.encode()).unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let result = DirectoryAttributes::decode(&db.get(b"key").unwrap().unwrap()).unwrap();
    assert_eq!((result.nlink, result.version, result.mtime), (800, 800, 7));
    drop(db);
    DB::destroy(&options, &path).unwrap();
}

#[test]
fn merge_directory_attributes_in_rocksdb() {
    let path = Utils::test_sub_dir(format!("directory_merge_{}", Utils::rand_str(12)));
    let mut options = Options::default();
    options.create_if_missing(true);
    configure_directory_attributes_merge(&mut options);
    let db = DB::open(&options, &path).unwrap();

    let base = DirectoryAttributes::new(1, 2, 3);
    let first = DirectoryAttributeDelta::new(10, 11, 1).with_base(base);
    let second = DirectoryAttributeDelta::new(12, 9, -1);
    db.merge(b"new", first.encode()).unwrap();
    db.merge(b"new", second.encode()).unwrap();
    let mut expected = base;
    expected.apply(first).unwrap();
    expected.apply(second).unwrap();
    assert_eq!(
        DirectoryAttributes::decode(&db.get(b"new").unwrap().unwrap()),
        Some(expected)
    );

    db.put(b"existing", base.encode()).unwrap();
    db.merge(b"existing", second.encode()).unwrap();
    let mut expected = base;
    expected.apply(second).unwrap();
    assert_eq!(
        DirectoryAttributes::decode(&db.get(b"existing").unwrap().unwrap()),
        Some(expected)
    );

    db.flush().unwrap();
    drop(db);
    let db = DB::open(&options, &path).unwrap();
    assert_eq!(
        DirectoryAttributes::decode(&db.get(b"existing").unwrap().unwrap()),
        Some(expected)
    );

    db.merge(b"conflicting_base", first.encode()).unwrap();
    let conflicting =
        DirectoryAttributeDelta::new(20, 20, 1).with_base(DirectoryAttributes::new(1, 2, 4));
    db.merge(b"conflicting_base", conflicting.encode()).unwrap();
    assert!(db.get(b"conflicting_base").is_err());

    db.merge(b"missing_base", second.encode()).unwrap();
    assert!(db.get(b"missing_base").is_err());
    db.put(b"invalid", b"invalid").unwrap();
    db.merge(b"invalid", second.encode()).unwrap();
    assert!(db.get(b"invalid").is_err());

    drop(db);
    DB::destroy(&options, &path).unwrap();
}
