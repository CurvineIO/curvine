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

use curvine_common::rocksdb::DBConf;
use curvine_common::state::BlockLocation;
use curvine_server::master::meta::inode::{InodeDir, InodeFile, InodeView, ROOT_INODE_ID};
use curvine_server::master::meta::store::RocksInodeStore;
use orpc::CommonResult;

#[test]
fn test_inode_dir_add_children_and_sort_alphabetically() {
    let mut root = InodeDir::new(0, 0);
    println!("{:?}", root);
    root.add_file_child("aa1", InodeFile::new(1, 0)).unwrap();
    root.add_file_child("aa3", InodeFile::new(2, 0)).unwrap();
    root.add_file_child("aa2", InodeFile::new(3, 0)).unwrap();
    root.add_file_child("b", InodeFile::new(5, 0)).unwrap();

    root.print_child();

    let children = root.children_vec();

    assert_eq!(children.len(), 4);
    assert_eq!(children[0].name(), "aa1");
    assert_eq!(children[1].name(), "aa2");
    assert_eq!(children[2].name(), "aa3");
    assert_eq!(children[3].name(), "b");
}

#[test]
fn test_inode_path_components_parsing() {
    let p1 = "/a/b/c";
    let vec = InodeView::path_components(p1).unwrap();
    assert_eq!(vec.len(), 4);
    assert_eq!(vec[0], "");
    assert_eq!(vec[1], "a");
}

#[test]
fn test_rocks_inode_store_add_delete_child_and_iterator() -> CommonResult<()> {
    let conf = DBConf::default();
    let db = RocksInodeStore::new(conf, true)?;

    let mut batch = db.new_batch();
    batch.add_child(ROOT_INODE_ID, "c", 3)?;
    batch.add_child(ROOT_INODE_ID, "a1", 1)?;
    batch.add_child(ROOT_INODE_ID, "a2", 2)?;
    batch.commit()?;

    // Test iterator
    let iter = db.get_child_ids(ROOT_INODE_ID, None)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item?)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![1, 2, 3]);

    // Prefix iterator.
    let iter1 = db.get_child_ids(ROOT_INODE_ID, Some("a"))?;
    let mut vec1 = vec![];
    for item in iter1 {
        vec1.push(item.unwrap())
    }
    println!("vec = {:?}", vec1);
    assert_eq!(vec1, vec![1, 2]);

    let mut batch = db.new_batch();
    batch.delete_child(ROOT_INODE_ID, "a1")?;
    batch.commit()?;

    let iter = db.get_child_ids(ROOT_INODE_ID, None)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item?)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![2, 3]);

    Ok(())
}

#[test]
fn test_rocks_block_store_add_delete_location_operations() -> CommonResult<()> {
    let conf = DBConf::default();
    let db = RocksInodeStore::new(conf, true)?;

    let mut batch = db.new_batch();
    batch.add_location(101, &BlockLocation::with_id(1))?;
    batch.add_location(101, &BlockLocation::with_id(2))?;
    batch.add_location(101, &BlockLocation::with_id(3))?;
    batch.add_location(103, &BlockLocation::with_id(1))?;
    batch.commit()?;

    // Test to get all locations of block id
    let iter = db.get_locations(101)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item.worker_id)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![1, 2, 3]);

    let mut batch = db.new_batch();
    batch.delete_location(101, 2)?;
    batch.commit()?;

    let iter = db.get_locations(101)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item.worker_id)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![1, 3]);

    Ok(())
}

#[test]
fn test_delete_all_block_locations_for_specific_worker() -> CommonResult<()> {
    let conf = DBConf::default();
    let db = RocksInodeStore::new(conf, true)?;

    // Setup: Add locations for multiple blocks and workers
    let mut batch = db.new_batch();
    for block_id in 401..405 {
        for worker_id in 1..4 {
            batch.add_location(block_id, &BlockLocation::with_id(worker_id))?;
        }
    }
    batch.commit()?;

    // Test delete_locations for worker 2
    db.delete_locations(2)?;

    // Verify worker 2's locations are removed from CF_LOCATION
    let block_ids_worker2 = db.get_block_ids(2)?;
    assert_eq!(block_ids_worker2.len(), 0);

    // Verify worker 2's locations are removed from CF_BLOCK
    for block_id in 401..405 {
        let locations = db.get_locations(block_id)?;
        assert_eq!(locations.len(), 2); // Only workers 1 and 3 remain
        let worker_ids: Vec<u32> = locations.iter().map(|loc| loc.worker_id).collect();
        assert_eq!(worker_ids, vec![1, 3]);
    }

    // Verify other workers are unaffected
    for worker_id in [1, 3] {
        let block_ids = db.get_block_ids(worker_id)?;
        assert_eq!(block_ids.len(), 4);
    }

    Ok(())
}
