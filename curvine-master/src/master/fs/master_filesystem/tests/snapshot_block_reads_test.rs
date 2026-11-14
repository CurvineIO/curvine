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
use curvine_model::BlockLocation;

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
    let child_id = fs.file_status(format!("/data/{name}")).unwrap().id;
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
    // Save the owned identity before the move, then verify the restored edge
    // carries the exact same inode id and name.
    let owned_name = owned.name().to_string();
    let owned_id = owned.id();
    dir_node.add_child_edge(owned).unwrap();
    let restored = dir_node
        .children
        .get_child_edge(&owned_name)
        .expect("the restored edge must be back under /data");
    assert_eq!(
        restored.id(),
        owned_id,
        "the restored edge must keep the id"
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
    let started = started_rx.recv_timeout(std::time::Duration::from_secs(5));
    // Receive as a Result first: an old snapshot implementation could succeed
    // here, and the fixture must be torn down before any assertion.
    let received_500ms = rx.recv_timeout(std::time::Duration::from_millis(500));
    let timely = received_500ms.is_ok();
    drop(held);
    // One bounded recovery window for the same worker, only when 500ms missed.
    let received = match received_500ms {
        Ok(result) => Ok(result),
        Err(_) => rx.recv_timeout(std::time::Duration::from_secs(5)),
    };
    // Classify the final observation explicitly. A business result (an FsErr
    // included) and a disconnected channel both mean the worker has finished;
    // only a final Timeout means it is still running.
    let delivered = received.is_ok();
    let worker_exited = matches!(
        &received,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    );
    let stuck = matches!(&received, Err(std::sync::mpsc::RecvTimeoutError::Timeout));
    let finished = delivered || worker_exited;
    let join_result = finished.then(|| worker.join());
    if finished {
        restore_live_edge(&fs, &inode_locks, owned);
    }
    // The stuck branch is the only one that skips the join and the restore, so
    // it is reported first: with a final timeout `timely` is false and would
    // mask this diagnosis.
    assert!(
        !stuck,
        "the worker did not report a result within the recovery window; a stuck worker is not joined and the live edge is left as-is"
    );
    // A disconnected channel is its own failure, not a timeliness miss.
    assert!(
        !worker_exited,
        "the worker exited without reporting a result"
    );
    // Completion path: join/restore are already collected above, so the
    // timeliness bar is asserted only now.
    started.expect("worker must start within the bound");
    assert!(timely, "the fallback must not wait for a cold window");
    join_result
        .expect("a finished worker must be joinable")
        .expect("worker must not panic");
    let received = received.expect("the worker must report a result");
    let err = received.unwrap_err();
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
    worker.address.hostname = "fallback-worker-100".to_string();
    let worker_address = worker.address.clone();
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
            // The inode row must stay self-consistent: file len covers both
            // full-size blocks.
            f.len = full_len * 2;
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
    let started = started_rx.recv_timeout(std::time::Duration::from_secs(5));
    // Keep the receive Result: a slow/old implementation must not abort the
    // fixture before held/worker/restore are torn down.
    let received_500ms = rx.recv_timeout(std::time::Duration::from_millis(500));
    let timely = received_500ms.is_ok();
    drop(held);
    // If the 500ms window missed, give the same worker one bounded recovery
    // window after the view is released (no unbounded join in between).
    let received = match received_500ms {
        Ok(result) => Ok(result),
        Err(_) => rx.recv_timeout(std::time::Duration::from_secs(5)),
    };
    // Classify the final observation explicitly. A business result (an FsErr
    // included) and a disconnected channel both mean the worker has finished;
    // only a final Timeout means it is still running.
    let delivered = received.is_ok();
    let worker_exited = matches!(
        &received,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    );
    let stuck = matches!(&received, Err(std::sync::mpsc::RecvTimeoutError::Timeout));
    let finished = delivered || worker_exited;
    let join_result = finished.then(|| worker.join());
    if finished {
        restore_live_edge(&fs, &inode_locks, owned);
    }
    // The stuck branch is the only one that skips the join and the restore, so
    // it is reported first: with a final timeout `timely` is false and would
    // mask this diagnosis.
    assert!(
        !stuck,
        "the worker did not report a result within the recovery window; a stuck worker is not joined and the live edge is left as-is"
    );
    // A disconnected channel is its own failure, not a timeliness miss.
    assert!(
        !worker_exited,
        "the worker exited without reporting a result"
    );
    // Completion path: join/restore are already collected above, so the
    // timeliness bar is asserted only now.
    started.expect("worker must start within the bound");
    assert!(timely, "the fallback must not wait for a cold window");
    let received = received.expect("the worker must report a result");
    let blocks = received.expect("the snapshot fallback must serve the store-resident file");

    assert_eq!(blocks.status.id, file_id);
    assert_eq!(blocks.status.len, live_status.len);
    assert_eq!(live_status.len, full_len * 2);
    assert_eq!(blocks.block_locs.len(), 2);
    assert_eq!(blocks.block_locs[0].block.id, first.block.id);
    assert_eq!(blocks.block_locs[0].block.len, full_len);
    assert_eq!(blocks.block_locs[1].block.id, second_id);
    assert_eq!(blocks.block_locs[1].block.len, full_len);
    for located in &blocks.block_locs {
        assert_eq!(
            located.locs,
            vec![worker_address.clone()],
            "each block must report the worker's full address"
        );
    }
    join_result
        .expect("a finished worker must be joinable")
        .expect("worker must not panic");
    // The hot path over the restored live edge agrees with the fallback:
    // same status id/len and the same block order with full addresses.
    let hot = fs.get_block_locations("/data/a.log").unwrap();
    assert_eq!(hot.status.id, blocks.status.id);
    assert_eq!(hot.status.len, blocks.status.len);
    let hot_blocks: Vec<_> = hot
        .block_locs
        .iter()
        .map(|l| (l.block.id, l.block.len, l.locs.clone()))
        .collect();
    let fallback_blocks: Vec<_> = blocks
        .block_locs
        .iter()
        .map(|l| (l.block.id, l.block.len, l.locs.clone()))
        .collect();
    assert_eq!(hot_blocks, fallback_blocks);
}
