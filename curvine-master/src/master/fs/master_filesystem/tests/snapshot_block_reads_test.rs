// Copyright 2026 OPPO
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

//! Public block-read snapshot fallback regressions. These tests do not
//! reference the shared builder helper, so the suite stays portable to older
//! production inputs for real red-run comparison.

use super::*;
use crate::master::meta::inode::DirNode;
use crate::master::meta::BlockMeta;
use curvine_model::state::FileType;
use curvine_model::BlockLocation;
use std::collections::HashMap;

#[test]
fn build_located_blocks_rejects_short_non_tail_block_like_the_live_path() {
    let fs = test_fs("located-blocks-builder");

    // Two blocks: the non-tail one is shorter than the file's block size —
    // the same corruption the live path rejects.
    let mut file = InodeFile::new(1, 0);
    file.block_size = 1024;
    file.blocks = vec![BlockMeta::new(101, 512), BlockMeta::new(102, 256)];
    let mut locations = HashMap::new();
    locations.insert(101, vec![BlockLocation::with_id(100)]);
    locations.insert(102, vec![BlockLocation::with_id(100)]);

    let err = fs
        .build_located_blocks("/file", &file, locations)
        .unwrap_err();
    assert!(
        err.to_string().contains("block status abnormal"),
        "the non-tail length check must fire, got {err}"
    );
}

#[test]
fn build_located_blocks_orders_blocks_and_reports_status() {
    let fs = test_fs("located-blocks-builder-ok");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);

    let mut file = InodeFile::new(2, 0);
    file.block_size = 1024;
    file.blocks = vec![BlockMeta::new(201, 1024), BlockMeta::new(202, 512)];
    let mut locations = HashMap::new();
    locations.insert(201, vec![BlockLocation::with_id(100)]);
    locations.insert(202, vec![BlockLocation::with_id(100)]);

    let blocks = fs.build_located_blocks("/file", &file, locations).unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].block.id, 201);
    assert_eq!(blocks[0].block.len, 1024);
    assert_eq!(
        blocks[0]
            .locs
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![100]
    );
    assert_eq!(blocks[1].block.id, 202);
    assert_eq!(blocks[1].block.len, 512);
    assert_eq!(
        blocks[1]
            .locs
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![100]
    );
}

#[test]
fn build_located_blocks_reports_lost_blocks_like_the_live_path() {
    let fs = test_fs("located-blocks-builder-lost");

    let mut file = InodeFile::new(3, 0);
    file.block_size = 1024;
    file.blocks = vec![BlockMeta::new(301, 1024)];
    // No location row for the block: the shared builder must report the same
    // lost-block error as the live path.
    let locations = HashMap::new();

    let err = fs
        .build_located_blocks("/file", &file, locations)
        .unwrap_err();
    assert!(
        err.to_string().contains("Lost (no worker can read)"),
        "the lost-block check must fire, got {err}"
    );
}

/// Removes the live child edge for `path` (a file under /data) while keeping
/// the store row, forcing the public block-read to fall back to the store
/// snapshot. Returns the owned inode view for restoration.
fn hide_live_edge_for_snapshot_fallback(
    fs: &MasterFilesystem,
    inode_locks: &InodeLockManager,
    name: &str,
) -> InodeView {
    // Fixture-only live mutation under the parent's inode write lock. Every
    // id is resolved before the lock: file_status inside the locked scope
    // would take a read lock on the same stripe and trip the debug
    // re-entrancy check.
    let parent_id = fs.file_status("/data").unwrap().id;
    let child_id = fs.file_status(&format!("/data/{name}")).unwrap().id;
    let locks = inode_locks.write_inode(parent_id);
    let view = fs.fs_dir.view().unwrap();
    let root = view.root_node();
    let data_view = root
        .children
        .read_child_view("data")
        .0
        .expect("data dir must exist in the live tree");
    let dir_node = match &data_view {
        InodeView::Dir(dir) => dir
            .node
            .clone()
            .unwrap_or_else(|| DirNode::for_edge(&data_view)),
        _ => panic!("data must be a directory"),
    };
    let owned = dir_node
        .delete_child_edge(child_id, name)
        .expect("live edge must be removable");
    // Confirm the split: the live edge is gone, the store row remains.
    assert!(dir_node.children.get_child_edge(name).is_none());
    {
        let snapshot = InodeStoreSnapshot::new(view.store.checked_read_snapshot().unwrap());
        let resolved = snapshot.resolve_path(&format!("/data/{name}")).unwrap();
        let target = resolved.target().expect("the store row must remain");
        assert_eq!(
            target.id(),
            child_id,
            "the store row must be the exact inode"
        );
    }
    drop(view);
    drop(locks);
    owned
}

fn restore_live_edge(fs: &MasterFilesystem, inode_locks: &InodeLockManager, owned: InodeView) {
    // Same pre-resolved id discipline: nothing inside the locked scope takes
    // another inode lock.
    let parent_id = fs.file_status("/data").unwrap().id;
    let locks = inode_locks.write_inode(parent_id);
    let view = fs.fs_dir.view().unwrap();
    let root = view.root_node();
    let data_view = root
        .children
        .read_child_view("data")
        .0
        .expect("data dir must exist in the live tree");
    let dir_node = match &data_view {
        InodeView::Dir(dir) => dir
            .node
            .clone()
            .unwrap_or_else(|| DirNode::for_edge(&data_view)),
        _ => panic!("data must be a directory"),
    };
    dir_node.add_child_edge(owned).unwrap();
    assert!(
        dir_node.children.get_child_edge("a.log").is_some(),
        "the restored edge must be back under /data"
    );
    drop(view);
    drop(locks);
}

#[test]
fn block_locations_snapshot_fallback_rejects_corrupt_non_tail_block() {
    let fs = test_fs("gbl-fallback-corrupt");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.mkdir("/data", true).unwrap();
    fs.create_with_opts(
        "/data/a.log",
        CreateFileOptsBuilder::new().block_size(1024 * 1024).build(),
        OpenFlags::new_create().set_overwrite(true),
    )
    .unwrap();
    let full_len: i64 = 1024 * 1024; // matches the file's block_size option
    let first = fs
        .add_block(
            "/data/a.log",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    fs.complete_file(
        "/data/a.log",
        None,
        full_len,
        vec![CommitBlock {
            block_id: first.block.id,
            block_len: full_len,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        true,
        None,
    )
    .unwrap();
    // Corrupt the stored metadata through the existing store batch API: a
    // short non-tail block is appended after the committed full block.
    let file_id = fs.file_status("/data/a.log").unwrap().id;
    {
        let view = fs.fs_dir.view().unwrap();
        let mut inode = view
            .store
            .get_inode(file_id, None)
            .unwrap()
            .expect("file row exists");
        let second_id = {
            let file = inode.as_file_ref().unwrap();
            crate::master::meta::InodeId::create_block_id(file.id, 1).unwrap()
        };
        if let InodeView::File(f) = &mut inode {
            f.blocks[0].len = 512;
            f.blocks.push(BlockMeta::new(second_id, full_len));
        } else {
            panic!("expected a file inode");
        }
        let mut batch = view.store.store.new_batch();
        batch.write_inode(&inode).unwrap();
        batch
            .add_location(second_id, &BlockLocation::with_id(100))
            .unwrap();
        batch.commit().unwrap();
    }

    let inode_locks = fs.inode_locks_for_test();
    let owned = hide_live_edge_for_snapshot_fallback(&fs, &inode_locks, "a.log");
    // Public entry, held shared view, store-only target: the fallback must
    // run the shared builder's non-tail length check without any cold drain.
    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<FileBlocks>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.get_block_locations("/data/a.log"))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");
    // Receive as a Result first: an old snapshot implementation could succeed
    // here, and the fixture must be torn down before any assertion.
    let received = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .map_err(|_| panic!("the fallback must not wait for a cold window"));
    drop(held);
    worker.join().expect("worker must not panic");
    restore_live_edge(&fs, &inode_locks, owned);
    // Now assert on the preserved outcome.
    let outcome = received.expect("the worker must report a result");
    let err = outcome.unwrap_err();
    assert!(
        err.to_string().contains("block status abnormal"),
        "the snapshot fallback must reject a corrupt non-tail block, got {err}"
    );
    // The hot path over the restored live edge reports the same contract.
    let err = fs.get_block_locations("/data/a.log").unwrap_err();
    assert!(
        err.to_string().contains("block status abnormal"),
        "the live path must agree with the fallback, got {err}"
    );
}

#[test]
fn block_locations_snapshot_fallback_matches_the_live_result() {
    let fs = test_fs("gbl-fallback-match");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.mkdir("/data", true).unwrap();
    fs.create_with_opts(
        "/data/a.log",
        CreateFileOptsBuilder::new().block_size(1024 * 1024).build(),
        OpenFlags::new_create().set_overwrite(true),
    )
    .unwrap();
    let first = fs
        .add_block(
            "/data/a.log",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let full_len: i64 = 1024 * 1024; // matches the file's block_size option
    fs.complete_file(
        "/data/a.log",
        None,
        full_len,
        vec![CommitBlock {
            block_id: first.block.id,
            block_len: full_len,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        true,
        None,
    )
    .unwrap();
    // Append a second full-size block directly in the store (same batch API),
    // so the snapshot fallback has two ordered blocks to report.
    let file_id = fs.file_status("/data/a.log").unwrap().id;
    let second_id = {
        let view = fs.fs_dir.view().unwrap();
        let mut inode = view
            .store
            .get_inode(file_id, None)
            .unwrap()
            .expect("file row exists");
        let second_id = {
            let file = inode.as_file_ref().unwrap();
            crate::master::meta::InodeId::create_block_id(file.id, 1).unwrap()
        };
        if let InodeView::File(f) = &mut inode {
            f.blocks.push(BlockMeta::new(second_id, full_len));
        } else {
            panic!("expected a file inode");
        }
        let mut batch = view.store.store.new_batch();
        batch.write_inode(&inode).unwrap();
        batch
            .add_location(second_id, &BlockLocation::with_id(100))
            .unwrap();
        batch.commit().unwrap();
        second_id
    };
    let live_status = fs.file_status("/data/a.log").unwrap();

    let inode_locks = fs.inode_locks_for_test();
    let owned = hide_live_edge_for_snapshot_fallback(&fs, &inode_locks, "a.log");
    // Held shared view + bounded worker: the snapshot fallback must serve the
    // store-resident file without any cold drain.
    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<FileBlocks>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.get_block_locations("/data/a.log"))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");
    let received = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .map_err(|_| panic!("the fallback must not wait for a cold window"));
    drop(held);
    worker.join().expect("worker must not panic");
    restore_live_edge(&fs, &inode_locks, owned);
    let blocks = received
        .expect("the worker must report a result")
        .expect("the snapshot fallback must serve the store-resident file");

    assert_eq!(blocks.status.id, file_id);
    assert_eq!(blocks.status.len, live_status.len);
    assert_eq!(blocks.block_locs.len(), 2);
    assert_eq!(blocks.block_locs[0].block.id, first.block.id);
    assert_eq!(blocks.block_locs[0].block.len, full_len);
    assert_eq!(blocks.block_locs[1].block.id, second_id);
    assert_eq!(blocks.block_locs[1].block.len, full_len);
    for located in &blocks.block_locs {
        assert_eq!(
            located.locs.iter().map(|l| l.worker_id).collect::<Vec<_>>(),
            vec![100]
        );
    }
    // The hot path over the restored live edge agrees with the fallback.
    let hot = fs.get_block_locations("/data/a.log").unwrap();
    let hot_ids: Vec<_> = hot.block_locs.iter().map(|l| l.block.id).collect();
    let fallback_ids: Vec<_> = blocks.block_locs.iter().map(|l| l.block.id).collect();
    assert_eq!(hot_ids, fallback_ids);
}
