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
use curvine_rocksdb::{DBConf, DBEngine, RocksIterator, RocksUtils};
use curvine_runtime::common::{FileUtils, Utils};

// Rocksdb database core function test
#[test]
fn test_rocksdb_scan_and_range_operations() {
    let conf = DBConf::new("../testing/rocks_api_test")
        .add_cf("scan1")
        .add_cf("scan2");

    let db = DBEngine::new(conf, true).unwrap();

    scan_test1(&db, "scan1").unwrap();
    scan_test2(&db, "scan2").unwrap();
}

#[test]
fn test_rocksdb_non_format_creates_missing_base_dir() -> CommonResult<()> {
    let base_dir = Utils::test_sub_dir(format!("rocks_non_format_missing_{}", Utils::rand_str(6)));
    FileUtils::delete_path(&base_dir, true)?;

    let conf = DBConf::new(&base_dir);
    let db = DBEngine::new(conf.clone(), false)?;
    db.put("k".as_bytes(), "v")?;

    assert!(std::path::Path::new(&conf.data_dir)
        .join("CURRENT")
        .is_file());

    Ok(())
}

#[test]
fn test_rocksdb_checkpoint_and_restore_functionality() -> CommonResult<()> {
    FileUtils::delete_path("../testing/test_restore", true)?;

    let conf = DBConf::new("../testing/test_restore/db1");
    let mut db = DBEngine::new(conf, true)?;
    db.put("k1".as_bytes(), "v1")?;

    let ck = db.create_checkpoint(1)?;
    db.put("k2".as_bytes(), "v2")?;

    db.restore(ck)?;

    assert!(db.get("k1".as_bytes())?.is_some());
    assert!(db.get("k2".as_bytes())?.is_none());

    Ok(())
}

#[test]
fn test_rocksdb_checkpoint_flushes_all_column_families() -> CommonResult<()> {
    let base_dir = Utils::test_sub_dir(format!("rocks_multi_cf_checkpoint_{}", Utils::rand_str(6)));
    FileUtils::delete_path(&base_dir, true)?;

    let conf = DBConf::new(&base_dir)
        .set_disable_wal(true)
        .add_cf("cf1")
        .add_cf("cf2");
    let mut db = DBEngine::new(conf, true)?;
    db.put_cf("cf1", b"k1", b"v1")?;
    db.put_cf("cf2", b"k2", b"v2")?;

    let checkpoint = db.create_checkpoint(1)?;
    db.put_cf("cf1", b"late", b"late")?;
    db.restore(checkpoint)?;

    assert_eq!(db.get_cf("cf1", b"k1")?, Some(b"v1".to_vec()));
    assert_eq!(db.get_cf("cf2", b"k2")?, Some(b"v2".to_vec()));
    assert!(db.get_cf("cf1", b"late")?.is_none());

    Ok(())
}

#[test]
fn clear_cf_removes_keys_at_and_above_ff() -> CommonResult<()> {
    let path = Utils::test_sub_dir(format!("rocks_clear_cf_{}", Utils::rand_str(12)));
    let conf = DBConf::new(&path).add_cf("target").add_cf("other");
    let db = DBEngine::new(conf.clone(), true)?;
    let keys: &[&[u8]] = &[b"", b"a", &[0xfe, 0xff], &[0xff], &[0xff, 0], &[0xff, 0xff]];
    for key in keys {
        db.put_cf("target", key, b"value")?;
    }
    db.put_cf("other", [0xff], b"other")?;
    let snapshot = db.read_snapshot();
    db.clear_cf("target")?;
    for key in keys {
        assert!(db.get_cf("target", key)?.is_none(), "key {key:?} survived");
        assert_eq!(
            snapshot.get_pinned_cf("target", key)?.as_deref(),
            Some(&b"value"[..])
        );
    }
    assert_eq!(db.get_cf("other", [0xff])?, Some(b"other".to_vec()));
    db.clear_cf("target")?;
    drop(snapshot);
    drop(db);
    let db = DBEngine::new(conf, false)?;
    for key in keys {
        assert!(
            db.get_cf("target", key)?.is_none(),
            "key {key:?} survived reopen"
        );
    }
    drop(db);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

#[test]
fn pinned_reads_keep_snapshot_and_value_lifetimes() -> CommonResult<()> {
    let path = Utils::test_sub_dir(format!("rocks_pinned_{}", Utils::rand_str(12)));
    let db = DBEngine::new(DBConf::new(&path).add_cf("cf"), true)?;
    db.put_cf("cf", b"key", b"old")?;
    let snapshot = db.read_snapshot();
    let pinned = db.get_pinned_cf("cf", b"key")?.unwrap();
    db.put_cf("cf", b"key", b"new")?;
    assert_eq!(&*pinned, b"old");
    assert_eq!(&*snapshot.get_pinned_cf("cf", b"key")?.unwrap(), b"old");
    assert_eq!(&*db.get_pinned_cf("cf", b"key")?.unwrap(), b"new");
    db.delete_cf("cf", b"key")?;
    assert!(db.get_pinned_cf("cf", b"key")?.is_none());
    assert_eq!(&*snapshot.get_pinned_cf("cf", b"key")?.unwrap(), b"old");
    assert!(db.get_pinned_cf("missing", b"key").is_err());
    drop(pinned);
    drop(snapshot);
    drop(db);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

fn to_vec(iter: RocksIterator) -> Vec<String> {
    let mut vec = vec![];
    for item in iter {
        let kv = item.unwrap();
        vec.push(String::from_utf8_lossy(&kv.1).to_string());
    }

    vec
}

fn scan_test1(db: &DBEngine, cf: &str) -> CommonResult<()> {
    let data: Vec<(u64, &str)> = vec![(0, "c"), (1, "c++"), (2, "java"), (3, "rust")];

    for (k, v) in data {
        db.put_cf(cf, RocksUtils::u64_to_bytes(k), v.as_bytes())?;
    }

    // cf scan
    let iter = db.scan(cf)?;
    let vec = to_vec(iter);
    println!("scan vec {:?}", vec);
    assert_eq!(vec.len(), 4);

    // range scan
    let iter = db.range_scan(cf, RocksUtils::u64_to_bytes(1), RocksUtils::u64_to_bytes(3))?;
    let vec = to_vec(iter);
    println!("scan vec {:?}", vec);
    assert_eq!(vec.len(), 2);

    Ok(())
}

fn scan_test2(db: &DBEngine, cf: &str) -> CommonResult<()> {
    let data: Vec<(i64, &str, &str)> = vec![
        (1001, "1", "c"),
        (1000, "1", "rust1"),
        (1002, "1", "java"),
        (1000, "2", "rust2"),
        (1000, "3", "rust3"),
    ];

    for (id, name, v) in data {
        let key = RocksUtils::i64_str_to_bytes(id, name);
        db.put_cf(cf, key, v.as_bytes())?;
    }

    let start = RocksUtils::i64_to_bytes(1000);
    // range scan
    let iter = db.prefix_scan(cf, start)?;
    let vec = to_vec(iter);
    println!("scan vec {:?}", vec);
    assert_eq!(vec.len(), 3);

    Ok(())
}

#[test]
fn test_rocksdb_prefix_delete_range_operation() -> CommonResult<()> {
    let conf = DBConf::new("../testing/delete_range").add_cf("cf");

    let db = DBEngine::new(conf, true)?;

    let k1 = "a1".as_bytes();
    let k2 = "a2".as_bytes();

    db.put_cf("cf", k1, k1)?;
    db.put_cf("cf", k2, k2)?;

    let upper = RocksUtils::calculate_end_bytes("a".as_bytes());
    db.put_cf("cf", upper, "k3".as_bytes())?;

    db.prefix_delete("cf", "a".as_bytes())?;

    let mut vec = vec![];
    for item in db.scan("cf")? {
        let kv = item?;
        let key = String::from_utf8_lossy(&kv.0).to_string();
        let value = String::from_utf8_lossy(&kv.1).to_string();
        vec.push((key, value));
    }

    println!("vec = {:?}", vec);

    assert_eq!(vec.len(), 1);
    assert_eq!(vec[0].0, "b");
    assert_eq!(vec[0].1, "k3");

    Ok(())
}

/// A single `DbReadSnapshot` must give a consistent multi-CF view: bulk scans
/// on different column families share one snapshot generation, so records
/// written after `read_snapshot()` are invisible to every snapshot iterator,
/// while ordinary iterators see them.
#[test]
fn test_rocksdb_snapshot_bulk_scan_consistent_across_cfs() -> CommonResult<()> {
    let conf = DBConf::new("../testing/snapshot_bulk_scan")
        .add_cf("inodes")
        .add_cf("edges");
    let db = DBEngine::new(conf, true)?;

    db.put_cf("inodes", RocksUtils::i64_to_bytes(1), "root")?;
    db.put_cf(
        "edges",
        RocksUtils::i64_str_to_bytes(1, "child"),
        RocksUtils::i64_to_bytes(2),
    )?;

    db.put_cf(
        "edges",
        RocksUtils::i64_str_to_bytes(2, "other"),
        RocksUtils::i64_to_bytes(4),
    )?;
    let snapshot = db.read_snapshot();

    // Write new records after the snapshot was created.
    db.put_cf("inodes", RocksUtils::i64_to_bytes(2), "child-inode")?;
    db.put_cf(
        "edges",
        RocksUtils::i64_str_to_bytes(1, "child2"),
        RocksUtils::i64_to_bytes(3),
    )?;

    // Snapshot iterators must not see the post-snapshot writes.
    let mut inode_keys = vec![];
    for item in snapshot.bulk_scan("inodes")? {
        let kv = item?;
        let id = RocksUtils::i64_from_bytes(&kv.0)?;
        inode_keys.push(id);
    }
    assert_eq!(inode_keys, vec![1]);

    let mut edge_keys = vec![];
    for item in snapshot.bulk_scan("edges")? {
        let kv = item?;
        let (parent, name) = RocksUtils::i64_str_from_bytes(&kv.0)?;
        edge_keys.push((parent, name));
    }
    assert_eq!(
        edge_keys,
        vec![(1, "child".to_string()), (2, "other".to_string())]
    );

    // Point reads inside the snapshot also see the old generation.
    assert!(snapshot
        .get_cf("inodes", RocksUtils::i64_to_bytes(2))?
        .is_none());
    assert_eq!(
        snapshot.get_cf("inodes", RocksUtils::i64_to_bytes(1))?,
        Some("root".as_bytes().to_vec())
    );

    // Prefix scans inside the snapshot see only the pre-snapshot edges.
    let mut prefix_count = 0;
    for item in snapshot.prefix_scan("edges", RocksUtils::i64_to_bytes(1))? {
        let _ = item?;
        prefix_count += 1;
    }
    assert_eq!(prefix_count, 1);
    assert_eq!(
        snapshot.count_prefix("edges", RocksUtils::i64_to_bytes(1))?,
        1
    );
    assert_eq!(
        snapshot.count_prefix("edges", RocksUtils::i64_to_bytes(2))?,
        1
    );
    assert_eq!(
        snapshot.count_prefix("edges", RocksUtils::i64_to_bytes(3))?,
        0
    );
    assert!(snapshot
        .count_prefix("missing", RocksUtils::i64_to_bytes(1))
        .is_err());
    assert_eq!(
        db.read_snapshot()
            .count_prefix("edges", RocksUtils::i64_to_bytes(1))?,
        2
    );

    // Ordinary (non-snapshot) iterators see the new writes.
    let live_inode_count = db.scan("inodes")?.count();
    assert_eq!(live_inode_count, 2);

    Ok(())
}

#[test]
fn snapshot_count_prefix_handles_unbounded_end() -> CommonResult<()> {
    let path = Utils::test_sub_dir(format!("snapshot-count-{}", Utils::rand_str(6)));
    let db = DBEngine::new(DBConf::new(path).add_cf("keys"), true)?;
    for key in [&b"a"[..], &b"\xff"[..], &b"\xff\x00"[..]] {
        db.put_cf("keys", key, b"value")?;
    }
    let snapshot = db.read_snapshot();
    assert_eq!(snapshot.count_prefix("keys", b"")?, 3);
    assert_eq!(snapshot.count_prefix("keys", b"\xff")?, 2);
    assert_eq!(snapshot.count_prefix("keys", b"\xff\xff")?, 0);
    Ok(())
}
