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

use curvine_common::rocksdb::{DBConf, DBEngine, RocksIterator, RocksUtils};
use orpc::common::FileUtils;
use orpc::CommonResult;

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
