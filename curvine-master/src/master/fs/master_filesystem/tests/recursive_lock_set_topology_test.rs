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

//! Topology cases split out of `recursive_lock_set_test.rs` by
//! responsibility; reuse the parent fixture through `use super::*`.

use super::*;

#[test]
fn recursive_delete_keeps_a_subtree_that_moved_out_before_the_plan() {
    let fs = test_fs("recursive-moved-out");
    fs.mkdir("/a/d0/d1", true).unwrap();
    fs.create("/a/d0/d1/file", false).unwrap();
    fs.mkdir("/b", false).unwrap();
    let moved_id = fs.file_status("/a/d0/d1").unwrap().id;

    // The move completes before the delete plans: /a no longer owns d1, so d1
    // is out of scope and must survive untouched, keeping its inode id.
    fs.rename("/a/d0/d1", "/b/d1", RenameFlags::empty())
        .unwrap();
    fs.delete("/a", true).unwrap();

    let survivor = fs
        .file_status("/b/d1")
        .expect("the moved subtree must survive the delete");
    assert_eq!(survivor.id, moved_id, "the moved inode must keep its id");
    assert!(
        fs.file_status("/b/d1/file").is_ok(),
        "the moved subtree's child must survive too"
    );
    assert!(
        fs.file_status("/a").is_err(),
        "the source tree must be gone"
    );
}

#[test]
fn recursive_delete_does_not_orphan_a_child_present_before_the_plan() {
    let fs = test_fs("recursive-orphan");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/new", false).unwrap();
    let child_id = fs.file_status("/a/d0/new").unwrap().id;

    // The child is in the subtree when the plan runs: it must be covered, not
    // left behind as an orphan.
    fs.delete("/a", true).unwrap();

    assert!(
        fs.file_status("/a/d0/new").is_err(),
        "the child must be removed with its tree"
    );
    assert!(
        fs.file_status_by_id("/a/d0/new", Some(child_id)).is_err(),
        "no orphan inode may remain reachable by id"
    );
}

#[test]
fn recursive_delete_covers_a_node_moved_into_the_subtree_before_the_plan() {
    let fs = test_fs("recursive-moved-in");
    fs.mkdir("/a/d0", true).unwrap();
    fs.mkdir("/b", true).unwrap();
    fs.create("/b/x", false).unwrap();
    let moved_id = fs.file_status("/b/x").unwrap().id;

    // The node already lives inside the subtree when the plan runs.
    fs.rename("/b/x", "/a/d0/x", RenameFlags::empty()).unwrap();
    fs.delete("/a", true).unwrap();

    assert!(
        fs.file_status("/a/d0/x").is_err(),
        "a node inside the planned subtree must be removed with it"
    );
    assert!(
        fs.file_status_by_id("/a/d0/x", Some(moved_id)).is_err(),
        "the moved-in inode must not survive as an orphan"
    );
    assert!(
        fs.file_status("/b").is_ok(),
        "the empty source dir survives"
    );
}

/// Deleting one name of an externally hard-linked file must keep the inode and
/// its content reachable through the surviving name.
#[test]
fn recursive_delete_keeps_an_external_hardlink_alive() {
    let fs = test_fs("recursive-hardlink-external");
    fs.mkdir("/a", true).unwrap();
    fs.create("/a/file", false).unwrap();
    fs.link("/a/file".to_string(), "/survivor".to_string())
        .unwrap();
    let file_id = fs.file_status("/a/file").unwrap().id;

    fs.delete("/a", true).unwrap();

    let survivor = fs
        .file_status("/survivor")
        .expect("an external hard link must keep the inode alive");
    assert_eq!(
        survivor.id, file_id,
        "the surviving name must still resolve to the same inode"
    );
    assert!(
        fs.file_status("/a").is_err(),
        "the deleted subtree must be gone"
    );
}

/// Removing the last name drops the inode; removing one of two names does not.
#[test]
fn last_link_delete_drops_the_inode_only_at_the_end() {
    let fs = test_fs("recursive-last-link");
    fs.mkdir("/d", true).unwrap();
    fs.create("/d/f", false).unwrap();
    fs.link("/d/f".to_string(), "/d/g".to_string()).unwrap();
    let file_id = fs.file_status("/d/f").unwrap().id;

    fs.delete("/d/f", false).unwrap();
    let remaining = fs
        .file_status("/d/g")
        .expect("the second name must keep the inode");
    assert_eq!(remaining.id, file_id);

    fs.delete("/d/g", false).unwrap();
    assert!(
        fs.file_status_by_id("/d/g", Some(file_id)).is_err(),
        "removing the last name must drop the inode"
    );
    assert!(
        fs.file_status("/d").is_ok(),
        "the parent directory survives"
    );
}

/// R7: the topology changes *after* capture and *before* the lock set is taken.
/// The lock-set path must then refuse to delete a subtree that moved out; the
/// post-lock revalidation either re-plans or falls back, but the moved-out
/// subtree must survive with its inode id.
#[test]
fn subtree_moved_out_between_capture_and_locking_survives() {
    let fs = test_fs("plan-pause-move-out");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", false).unwrap();
    fs.mkdir("/b", false).unwrap();
    let moved_id = fs.file_status("/a/d0").unwrap().id;

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));

    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");
    // The plan is captured; move the subtree out before any lock is taken.
    fs.rename("/a/d0", "/b/d0", RenameFlags::empty())
        .expect("move the subtree out");
    release_tx.send(()).expect("release the planner");

    let result = handle.join().expect("delete thread must not panic");
    result.expect("the delete must finish, by re-plan or fallback");

    let survivor = fs
        .file_status("/b/d0")
        .expect("a subtree moved out between capture and lock must survive");
    assert_eq!(
        survivor.id, moved_id,
        "the moved-out subtree must keep its inode id"
    );
    assert!(
        fs.file_status("/b/d0/file").is_ok(),
        "its child must survive as well"
    );
}

/// R7: a node created inside the subtree in that same window must not become an
/// orphan: it is either removed with the tree or the operation re-plans.
#[test]
fn node_created_between_capture_and_locking_is_not_orphaned() {
    let fs = test_fs("plan-pause-move-in");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/old", false).unwrap();

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));

    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");
    // A new child appears inside the planned subtree after capture.
    fs.create("/a/d0/new", false)
        .expect("create inside the subtree");
    let new_id = fs.file_status("/a/d0/new").unwrap().id;
    release_tx.send(()).expect("release the planner");

    let result = handle.join().expect("delete thread must not panic");
    result.expect("the delete must finish, by re-plan or fallback");

    // Either the tree is gone (the new child went with it) or the operation
    // re-planned; in no case may the child survive as an orphan reachable by id
    // while its parent path is gone.
    let parent_gone = fs.file_status("/a").is_err();
    let child_gone = fs.file_status_by_id("/a/d0/new", Some(new_id)).is_err();
    assert!(parent_gone, "the planned tree must be deleted in the end");
    assert!(
        child_gone,
        "a node created inside the planned subtree must not be orphaned"
    );
}

/// R7: a node is moved *into* the planned subtree after capture. It is either
/// removed with the tree or the operation re-plans; it must never survive as an
/// orphan whose parent path is gone.
#[test]
fn node_moved_in_between_capture_and_locking_is_not_orphaned() {
    let fs = test_fs("plan-pause-move-in-external");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/old", false).unwrap();
    fs.mkdir("/b", true).unwrap();
    fs.create("/b/extra", false).unwrap();

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));

    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");
    // Move an outside node into the planned subtree, after capture.
    fs.rename("/b/extra", "/a/d0/extra", RenameFlags::empty())
        .expect("move a node into the subtree");
    let moved_id = fs.file_status("/a/d0/extra").unwrap().id;
    release_tx.send(()).expect("release the planner");

    let result = handle.join().expect("delete thread must not panic");
    result.expect("the delete must finish, by re-plan or fallback");

    assert!(
        fs.file_status("/a").is_err(),
        "the planned tree must be gone"
    );
    assert!(
        fs.file_status_by_id("/a/d0/extra", Some(moved_id)).is_err(),
        "a node moved into the planned subtree must not be orphaned"
    );
}

/// R7: an internal EXCHANGE in the same window is serialized by the gate, not
/// made to complete concurrently. EXCHANGE always falls back to the exclusive
/// path (`rename` requires `!flags.exchange_mode()` for the sharded path), so it
/// waits for the delete's admission slot; running it synchronously inside the
/// plan pause would make the fixture wait on itself.
///
/// Linearization is therefore allowed in either order: the delete may finish
/// first and the exchange then reports FileNotFound. What must hold is the end
/// state - the tree is gone and neither exchanged inode is left reachable.
#[test]
fn exchange_in_the_same_window_is_serialized_not_concurrent() {
    let fs = test_fs("plan-pause-exchange");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/left", false).unwrap();
    fs.create("/a/d0/right", false).unwrap();
    let left_id = fs.file_status("/a/d0/left").unwrap().id;
    let right_id = fs.file_status("/a/d0/right").unwrap().id;

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    let delete_fs = fs.clone();
    let delete_handle = std::thread::spawn(move || delete_fs.delete("/a", true));
    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");

    // Reaching the pause point already proves the delete passed admission and
    // holds the inode write locks. The exchange requires exclusive mode, so it
    // must wait for that admission slot instead of running concurrently.
    let (exchange_done_tx, exchange_done_rx) = mpsc::channel();
    let exchange_fs = fs.clone();
    let exchange_handle = std::thread::spawn(move || {
        let result = exchange_fs.rename("/a/d0/left", "/a/d0/right", RenameFlags::EXCHANGE);
        let _ = exchange_done_tx.send(());
        result
    });
    // Positive evidence that it is queued: a bounded window in which it must
    // NOT finish. This is an assertion about the other thread's progress, not
    // a sleep hoping the queue order is what we guessed.
    assert!(
        exchange_done_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "the exchange must not complete while the delete owns the admission \
         slot and the inode write locks"
    );
    release_tx.send(()).expect("release the planner");

    let deleted = delete_handle.join().expect("delete thread must not panic");
    let exchanged = exchange_handle
        .join()
        .expect("exchange thread must not panic");

    deleted.expect("the delete must finish");
    // The exchange is allowed to lose the race: the tree may already be gone.
    if let Err(error) = exchanged {
        assert!(
            error.to_string().contains("not found"),
            "an exchange that loses the race must report the missing source, got {error}"
        );
    }

    assert!(fs.file_status("/a").is_err(), "the tree must be gone");
    assert!(
        fs.file_status_by_id("/a/d0/left", Some(left_id)).is_err()
            && fs.file_status_by_id("/a/d0/right", Some(right_id)).is_err(),
        "neither exchanged inode may remain reachable"
    );
}

/// R7: a hard link is added for a file inside the subtree after capture. The
/// nlink accounting must stay consistent: after the tree is deleted no name of
/// that inode may be reachable under the removed tree.
#[test]
fn hardlink_added_between_capture_and_locking_is_accounted() {
    let fs = test_fs("plan-pause-hardlink");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", false).unwrap();
    let file_id = fs.file_status("/a/d0/file").unwrap().id;

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));

    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");
    // A second name for the same inode, still inside the planned subtree.
    fs.link("/a/d0/file".to_string(), "/a/d0/alias".to_string())
        .expect("add a hard link inside the subtree");
    release_tx.send(()).expect("release the planner");

    let result = handle.join().expect("delete thread must not panic");
    result.expect("the delete must finish, by re-plan or fallback");

    assert!(
        fs.file_status("/a").is_err(),
        "the planned tree must be gone"
    );
    assert!(
        fs.file_status_by_id("/a/d0/alias", Some(file_id)).is_err(),
        "the alias added after capture must not survive under the removed tree"
    );
}

/// R7: the inode identity at a planned path changes after capture (same path,
/// different id). The lock-set path must not delete the new identity by id.
#[test]
fn identity_replaced_between_capture_and_locking_is_not_deleted() {
    let fs = test_fs("plan-pause-identity");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/slot", false).unwrap();
    let old_id = fs.file_status("/a/d0/slot").unwrap().id;

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    // Delete the whole tree on a worker thread, paused after capture.
    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));

    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");
    // Replace the identity at the planned path: remove and recreate.
    fs.delete("/a/d0/slot", false)
        .expect("remove the old identity");
    fs.create("/a/d0/slot", false)
        .expect("create a new identity");
    let new_id = fs.file_status("/a/d0/slot").unwrap().id;
    assert_ne!(new_id, old_id, "fixture: the identity must differ");
    release_tx.send(()).expect("release the planner");

    let result = handle.join().expect("delete thread must not panic");
    result.expect("the delete must finish, by re-plan or fallback");

    // The tree is deleted, so the replacement goes with it; what must not happen
    // is the operation deleting only the recorded old id and leaving the new
    // identity behind as a live child of a removed parent.
    assert!(
        fs.file_status("/a").is_err(),
        "the planned tree must be gone"
    );
    assert!(
        fs.file_status_by_id("/a/d0/slot", Some(new_id)).is_err(),
        "the replacement identity must not survive under the removed tree"
    );
}

/// R7: the hard link is a name *outside* the deleted subtree. Adding it in the
/// capture window must not change the outcome: after the subtree is deleted the
/// external name still resolves to the same inode with the same content.
#[test]
fn external_hardlink_created_in_the_window_survives_the_delete() {
    let fs = test_fs("plan-pause-external-hardlink");
    fs.mkdir("/a/d0", true).unwrap();
    fs.create("/a/d0/file", false).unwrap();
    fs.mkdir("/outside", true).unwrap();
    let file_id = fs.file_status("/a/d0/file").unwrap().id;

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.journal_writer
        .arm_plan_pause_hook(reached_tx, release_rx);

    let delete_fs = fs.clone();
    let handle = std::thread::spawn(move || delete_fs.delete("/a", true));
    reached_rx
        .recv_timeout(OBSERVE_WINDOW)
        .expect("the planner must reach the pause point");

    // A new name for the inode, created outside the planned subtree.
    fs.link("/a/d0/file".to_string(), "/outside/alias".to_string())
        .expect("link outside the subtree");
    release_tx.send(()).expect("release the planner");

    handle
        .join()
        .expect("delete thread must not panic")
        .expect("the delete must finish");

    assert!(
        fs.file_status("/a").is_err(),
        "the planned tree must be gone"
    );
    // The external name is outside the plan: the inode must stay alive.
    let survivor = fs
        .file_status("/outside/alias")
        .expect("an external hard link must survive a recursive delete");
    assert_eq!(
        survivor.id, file_id,
        "the external name must still resolve to the same inode"
    );
    assert_eq!(
        survivor.len, 0,
        "the surviving name must keep the inode's content unchanged"
    );
}
