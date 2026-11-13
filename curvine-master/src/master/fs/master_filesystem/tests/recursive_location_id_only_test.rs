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

//! ID-only cache invalidation versus the same file inode, split from
//! `recursive_location_lock_test.rs`; reuse the parent fixture.

use super::*;

/// R7: ID-only cache invalidation vs a recursive delete of the same inode.
/// Design ruling: an ID-level cache rewrite changes no directory edge or token,
/// so when it completes first the plan stays valid and no re-capture is owed.
/// The contract is freshness and no resurrection: the delete's body must read
/// the post-invalidation inode, not replay a stale DTO that still carries the
/// reclaimed block.
///
/// The invalidation is run to completion first (it takes the block location
/// locks itself); driving it synchronously inside the delete's pause window
/// would make the fixture wait on the very locks it is supposed to observe.
#[test]
fn id_only_invalidation_is_not_resurrected_by_a_later_delete() {
    let fs = test_fs_with_two_workers("location-id-only", "session-1", "session-2");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", true).unwrap();
    let block = fs
        .add_block(
            "/a/d0/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    let block_id = block.id;
    {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        for worker in 1..=2 {
            view.store
                .add_block_location(block_id, BlockLocation::with_id(worker))
                .expect("worker location");
        }
    }
    let commit = curvine_model::CommitBlock {
        block_id,
        block_len: 4096,
        locations: vec![BlockLocation::with_id(1), BlockLocation::with_id(2)],
    };
    fs.complete_file(
        "/a/d0/file".to_string(),
        None,
        4096,
        vec![commit],
        "location-fixture".to_string(),
        false,
        None,
    )
    .expect("complete the block");
    // `invalidate_lost_cache_files` only rewrites inodes in the Both storage
    // state with the Delete TTL action; a UFS mtime is what moves the policy
    // into Both.
    fs.set_attr(
        "/a/d0/file",
        curvine_model::SetAttrOptsBuilder::new()
            .ufs_mtime(1_000)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .expect("mark the file as a cache-mode replica");
    assert!(
        fs.file_status("/a/d0/file")
            .unwrap()
            .storage_policy
            .both_exists(),
        "fixture: the file must be in the Both storage state"
    );
    fs.link("/a/d0/file".to_string(), "/outside-keep".to_string())
        .expect("external survivor name");
    let file_id = fs.file_status("/a/d0/file").unwrap().id;

    // Step 1: real ID-level cleanup. Losing worker 1 leaves the block readable
    // by worker 2; losing worker 2 leaves it with no location, which is what
    // makes the by-id rewrite drop the block from the inode.
    fs.delete_locations(1)
        .expect("losing worker 1 must succeed");
    let cleaned = fs
        .delete_locations(2)
        .expect("the by-id cache cleanup must succeed");
    assert!(
        cleaned.removed_block_ids.contains(&block_id),
        "fixture: the cleanup must reclaim the block, got {:?}",
        cleaned.removed_block_ids
    );
    let blocks_after_cleanup = match {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store.get_inode(file_id, None).unwrap()
    } {
        Some(crate::master::meta::inode::InodeView::File(file)) => {
            file.blocks.iter().map(|b| b.id).collect::<Vec<_>>()
        }
        other => panic!("the inode must still be a file, got {other:?}"),
    };
    assert!(
        !blocks_after_cleanup.contains(&block_id),
        "the by-id rewrite must drop the unreadable cache block, got {blocks_after_cleanup:?}"
    );

    // Step 2: recursive delete of the subtree afterwards.
    fs.delete("/a", true)
        .expect("the recursive delete must finish");

    assert!(
        fs.file_status("/a").is_err(),
        "the planned tree must be gone"
    );
    let survivor = fs
        .file_status("/outside-keep")
        .expect("the external name must survive");
    assert_eq!(survivor.id, file_id, "the survivor keeps the same inode");
    let blocks_after_delete = match {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store.get_inode(file_id, None).unwrap()
    } {
        Some(crate::master::meta::inode::InodeView::File(file)) => {
            file.blocks.iter().map(|b| b.id).collect::<Vec<_>>()
        }
        None => Vec::new(),
        other => panic!("the inode must be a file or gone, got {other:?}"),
    };
    assert!(
        !blocks_after_delete.contains(&block_id),
        "the delete must not resurrect the cleaned block from a stale DTO, got \
         {blocks_after_delete:?}"
    );
    assert!(
        stored_locations(&fs, block_id).is_empty(),
        "no location row may remain for the reclaimed block"
    );
}

/// R7: the ID-level cleanup takes the same file inode write lock as the
/// recursive delete, so the two are mutually exclusive rather than racing on
/// the same inode. `delete_lost_worker_locations` acquires
/// `inode_locks.lock_many` (which includes the candidate files' inode write
/// locks) before it calls `invalidate_lost_cache_files`, so an invalidation
/// driven from a second thread has to wait for the delete that holds that lock.
///
/// Both operations are driven on their own threads; the delete's pause window
/// is the barrier, and neither thread waits on a hook the other must release.
#[test]
fn id_only_invalidation_excludes_with_the_delete_on_the_same_file_inode() {
    let fs = test_fs_with_two_workers("location-id-only-exclusive", "session-1", "session-2");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", true).unwrap();
    let block = fs
        .add_block(
            "/a/d0/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    let block_id = block.id;
    {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        for worker in 1..=2 {
            view.store
                .add_block_location(block_id, BlockLocation::with_id(worker))
                .expect("worker location");
        }
    }
    let commit = curvine_model::CommitBlock {
        block_id,
        block_len: 4096,
        locations: vec![BlockLocation::with_id(1), BlockLocation::with_id(2)],
    };
    fs.complete_file(
        "/a/d0/file".to_string(),
        None,
        4096,
        vec![commit],
        "location-fixture".to_string(),
        false,
        None,
    )
    .expect("complete the block");
    fs.set_attr(
        "/a/d0/file",
        curvine_model::SetAttrOptsBuilder::new()
            .ufs_mtime(1_000)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .expect("cache-mode replica");
    fs.link("/a/d0/file".to_string(), "/outside-keep".to_string())
        .expect("external survivor");
    let file_id = fs.file_status("/a/d0/file").unwrap().id;

    let (loc_reached_tx, loc_reached_rx) = mpsc::channel();
    let (loc_release_tx, loc_release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_location_pause_hook(loc_reached_tx, loc_release_rx);

    let delete_fs = fs.clone();
    let delete_handle = std::thread::spawn(move || delete_fs.delete("/a", true));
    loc_reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the delete must reach its location sample");

    // Direct evidence (no scheduler luck): the paused delete already holds this
    // file's inode write lock, so a non-blocking read on the same stripe must
    // fail. This is what makes the exclusion claim about the real holder.
    assert!(
        fs.inode_locks.try_read_inode(file_id).is_none(),
        "the paused delete must hold the file inode write lock"
    );

    // The cleanup runs on its own thread: it needs the file inode write lock,
    // which the paused delete already holds.
    let idl_fs = fs.clone();
    let (idl_done_tx, idl_done_rx) = mpsc::channel();
    let idl_handle = std::thread::spawn(move || {
        let first = idl_fs.delete_locations(1);
        let second = idl_fs.delete_locations(2);
        let _ = idl_done_tx.send((first, second));
    });

    // Mutual exclusion: the cleanup cannot finish while the delete holds the
    // inode write lock.
    let early = idl_done_rx.recv_timeout(Duration::from_millis(500));
    assert!(
        early.is_err(),
        "the ID-level cleanup must block on the delete's file inode lock, but \
         it finished early"
    );

    loc_release_tx.send(()).expect("release the delete");
    delete_handle
        .join()
        .expect("delete thread must not panic")
        .expect("the recursive delete must finish");

    let (first, second) = idl_done_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the cleanup must complete once the delete releases the inode lock");
    let cleaned = second.expect("the by-id cleanup must succeed");
    assert!(
        first.is_ok(),
        "losing worker 1 must also succeed once the lock is free"
    );
    idl_handle.join().expect("cleanup thread must not panic");
    assert!(
        cleaned.removed_block_ids.contains(&block_id),
        "the serialized cleanup must still reclaim the block, got {:?}",
        cleaned.removed_block_ids
    );

    // Legal order (delete first, cleanup second) with no resurrection: the
    // surrogate name keeps the inode, and the reclaimed block is gone from it.
    assert!(fs.file_status("/a").is_err(), "the subtree must be gone");
    let survivor = fs
        .file_status("/outside-keep")
        .expect("the external name must survive");
    assert_eq!(survivor.id, file_id);
    match {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.store.get_inode(file_id, None).unwrap()
    } {
        Some(crate::master::meta::inode::InodeView::File(file)) => assert!(
            file.blocks.iter().all(|b| b.id != block_id),
            "the cleanup must leave the reclaimed block out of the inode"
        ),
        other => panic!("the inode must still be a file, got {other:?}"),
    }
    assert!(
        stored_locations(&fs, block_id).is_empty(),
        "no location row may remain for the reclaimed block"
    );
}
