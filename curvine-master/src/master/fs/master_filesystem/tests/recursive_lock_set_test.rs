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

//! c2: recursive mutations must cover their whole subtree through one DFS
//! lock set instead of the exclusive cold window.
//!
//! Which path is taken is judged behaviourally, without any test-only flag or
//! production marker: while this test holds one live view guard, the DFS
//! lock-set path still completes (it only needs the collected lock set),
//! whereas the exclusive cold path has to wait for that guard to be released.
//! A bounded window therefore separates the two implementations.
//!
//! Assertions are linearization-based, not "only one may succeed": a move that
//! happens before the plan simply makes the subtree out of scope, and both the
//! move and the delete succeeding is a legal outcome.

use super::*;
use curvine_model::SetAttrOptsBuilder;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

/// How long the test keeps a live view guard while a mutation runs.
const GUARD_WINDOW: Duration = Duration::from_secs(3);
/// Bound for the mutation observed while the guard is held.
const OBSERVE_WINDOW: Duration = Duration::from_secs(8);

/// Runs `op` while this thread holds a live view guard, and reports whether it
/// finished inside the bound. A timeout is the behavioural signature of the
/// exclusive cold path, which must wait for the guard.
fn run_with_live_view_guard<T: Send + 'static>(
    fs: &MasterFilesystem,
    op: impl FnOnce(MasterFilesystem) -> T + Send + 'static,
) -> Result<T, ()> {
    let handle = fs.fs_dir.clone();
    let held = handle.view().expect("live view guard");
    let (tx, rx) = mpsc::channel();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(op(worker_fs));
    });
    let observed = rx.recv_timeout(OBSERVE_WINDOW);
    // Release the guard, then let the worker finish so nothing leaks.
    drop(held);
    let _ = std::thread::sleep(Duration::from_millis(20));
    worker.join().expect("mutation worker must not panic");
    observed.map_err(|_| ())
}

/// Runs `op` on its own thread and reports whether it finished in the bound,
/// without holding any view guard. Used to judge the normal-admission path: a
/// plan-and-lock implementation completes while unrelated writers keep going,
/// while the exclusive cold path has to drain them first.
fn run_bounded<T: Send + 'static>(
    fs: &MasterFilesystem,
    op: impl FnOnce(MasterFilesystem) -> T + Send + 'static,
) -> Result<T, ()> {
    let (tx, rx) = mpsc::channel();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(op(worker_fs));
    });
    let observed = rx.recv_timeout(OBSERVE_WINDOW);
    worker.join().expect("operation worker must not panic");
    observed.map_err(|_| ())
}

/// Nested chain plus a fan-out at each level, so a DFS plan has real work to
/// do. Returns the deepest directory.
fn build_tree(fs: &MasterFilesystem, root: &str, depth: usize, width: usize) -> String {
    let mut path = root.to_string();
    for level in 0..depth {
        path.push_str(&format!("/d{level}"));
        fs.mkdir(&path, true).unwrap();
        for item in 0..width {
            fs.create(format!("{path}/f{item}"), false).unwrap();
        }
    }
    path
}

#[test]
fn recursive_delete_completes_while_a_live_view_guard_is_held() {
    let fs = test_fs("recursive-lock-set-delete");
    build_tree(&fs, "/deep", 6, 8);
    let root_id = fs.file_status("/deep").unwrap().id;

    let outcome = run_with_live_view_guard(&fs, |fs| fs.delete("/deep", true));

    assert!(
        outcome.is_ok(),
        "a recursive delete must finish through the DFS lock set while a \
         reader holds a live view; a timeout means it still depends on the \
         exclusive cold path"
    );
    outcome
        .expect("delete result")
        .expect("delete must succeed");
    assert!(
        fs.file_status("/deep").is_err(),
        "the whole subtree must be gone"
    );
    assert!(
        fs.file_status_by_id("/deep", Some(root_id)).is_err(),
        "the deleted root inode must not be reachable"
    );
}

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

#[test]
fn recursive_set_attr_covers_children_present_before_the_plan() {
    let fs = test_fs("recursive-set-attr");
    let deepest = build_tree(&fs, "/t", 4, 2);
    let leaf = format!("{deepest}/f1");

    let opts = SetAttrOptsBuilder::new()
        .recursive(true)
        .mode(0o600)
        .build();
    fs.set_attr("/t", opts).unwrap();

    let deep = fs.file_status(&leaf).expect("deep child must exist");
    assert_eq!(
        deep.mode & 0o777,
        0o600,
        "recursive set_attr must reach a child that existed when it planned"
    );
}

#[test]
fn deep_chain_delete_completes_while_a_live_view_guard_is_held() {
    let fs = test_fs("recursive-deep-chain");
    let deepest = build_tree(&fs, "/chain", 64, 1);
    assert!(
        fs.file_status(&deepest).is_ok(),
        "fixture: a 64-level chain must be built"
    );

    let outcome = run_with_live_view_guard(&fs, |fs| fs.delete("/chain", true));

    assert!(
        outcome.is_ok(),
        "a 64-level chain is inside MAX_LOCKS and must complete through the \
         DFS lock set while a reader holds a live view; a timeout means it \
         still takes the exclusive cold path"
    );
    outcome
        .expect("delete result")
        .expect("delete must succeed");
    assert!(fs.file_status("/chain").is_err(), "the chain must be gone");
}

/// Above `SubtreeWritePlan::MAX_LOCKS` (8192) the plan cannot be built, so the
/// operation must fall back to the exclusive cold path and still be correct.
/// No production marker is consulted; the check is the end state.
#[test]
fn oversized_subtree_falls_back_and_still_deletes_correctly() {
    let fs = test_fs("recursive-oversized");
    fs.mkdir("/wide", false).unwrap();
    // 9001 members > 8192: the plan must be rejected and the cold path used.
    for item in 0..9_001 {
        fs.create(format!("/wide/f{item}"), false).unwrap();
    }
    assert_eq!(
        fs.list_status("/wide").unwrap().len(),
        9_001,
        "fixture: the wide directory must hold 9001 children"
    );

    fs.delete("/wide", true)
        .expect("the fallback must still delete");

    assert!(
        fs.file_status("/wide").is_err(),
        "the oversized subtree must be gone after the fallback"
    );
}

/// The DFS plan must run in the normal admission stage: an unrelated writer
/// that keeps touching another subtree must not have to be drained first.
#[test]
fn recursive_delete_completes_alongside_an_unrelated_writer() {
    let fs = test_fs("recursive-concurrent-writer");
    build_tree(&fs, "/target", 5, 4);
    fs.mkdir("/other", false).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let writer_fs = fs.clone();
    let writer = std::thread::spawn(move || {
        let mut rounds = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let path = format!("/other/f{}", rounds % 128);
            let _ = writer_fs.create(&path, false);
            let _ = writer_fs.delete(&path, false);
            rounds += 1;
        }
        rounds
    });

    // Make sure the writer is really running before the delete starts.
    std::thread::sleep(Duration::from_millis(50));
    let outcome = run_bounded(&fs, |fs| fs.delete("/target", true));
    stop.store(true, Ordering::Relaxed);
    let rounds = writer.join().expect("writer thread must stop");

    assert!(
        rounds > 0,
        "fixture: the background writer must have made progress"
    );
    assert!(
        outcome.is_ok(),
        "a recursive delete must complete through the plan while an unrelated \
         writer keeps running; a timeout means it still drains the namespace \
         through the exclusive cold path"
    );
    outcome
        .expect("delete result")
        .expect("the delete must succeed");
    assert!(
        fs.file_status("/target").is_err(),
        "the target subtree must be gone"
    );
}

/// A real body error must be returned verbatim, and the body must not run a
/// second time after it (a retry-on-error fallback could double-write or hide
/// the original failure).
#[cfg(feature = "fault-injection")]
#[test]
fn recursive_delete_body_error_is_returned_verbatim_without_replay() {
    let fs = test_fs("recursive-body-error");
    let deepest = build_tree(&fs, "/err", 3, 2);
    assert!(
        fs.file_status(&deepest).is_ok(),
        "fixture: the tree must be built"
    );

    let faults = curvine_fault::FaultRuntime::process();
    let rule_id = format!(
        "c2-body-error-{}",
        curvine_runtime::common::Utils::rand_str(8)
    );
    // The fault point's context carries the TOP-LEVEL entry name of the
    // apply_delete call (`child_name` = the name passed to
    // unprotected_delete), not each descendant. This case owns the /err
    // subtree exclusively, so matching on "err" scopes the rule to this
    // case's delete while still firing exactly once inside its body.
    faults
        .configure(
            &rule_id,
            curvine_fault::FaultRuleBuilder::named("master.metadata.delete.before_commit")
                .matches("child_name", "err")
                .expect("child_name matcher")
                .times(1)
                .unwrap()
                .return_error("injected c2 body failure")
                .unwrap(),
        )
        .unwrap();

    let result = fs.delete("/err", true);

    let executions = faults
        .rule(&rule_id)
        .expect("fault rule lookup")
        .map(|rule| rule.executions)
        .unwrap_or(0);
    faults.remove(&rule_id).expect("fault rule removal");

    let error = result.expect_err("the injected body error must surface");
    assert!(
        error.to_string().contains("injected c2 body failure"),
        "the body error must be returned verbatim rather than replaced by a \
         fallback, got {error}"
    );
    assert_eq!(
        executions, 1,
        "the body must run once: a second execution would mean an error was \
         answered by re-running the body"
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

/// Recursive free releases block space but must keep the metadata tree, and it
/// must cover children that were present when it planned.
#[test]
fn recursive_free_keeps_metadata_and_covers_children() {
    let fs = test_fs("recursive-free-cover");
    let deepest = build_tree(&fs, "/f", 4, 2);
    let leaf = format!("{deepest}/f1");
    let before = fs.file_status(&leaf).unwrap().id;

    fs.free("/f", true).unwrap();

    let after = fs
        .file_status(&leaf)
        .expect("free must not delete metadata");
    assert_eq!(
        after.id, before,
        "recursive free must cover the same inodes"
    );
}

/// §8.3 depends on the real channel mode, so the writer must report it without
/// any test-only configuration.
#[test]
fn testing_writer_reports_its_real_queue_mode() {
    let fs = test_fs("recursive-queue-mode");
    // The testing fixture builds its writer on the default (unbounded) channel;
    // the query must reflect that real mode rather than a configuration guess.
    assert!(
        fs.journal_writer.queue_never_blocks(),
        "the testing writer's channel is unbounded and must report so"
    );
}

/// §8.3: the optimization only applies while the writer's queue can never
/// block. With a bounded channel (cap > 0) the recursive entry point must
/// return the typed fallback before the body runs, so no side effect can have
/// happened when the caller retries through the exclusive path.
#[test]
fn bounded_journal_queue_falls_back_before_the_body() {
    use curvine_config::ClusterConf;
    use curvine_raft::raft::RoleState;

    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.format_master = true;
    conf.change_test_meta_dir(format!(
        "recursive-bounded-queue-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 8;
    let journal = JournalSystem::from_conf(&conf).expect("journal system");
    let fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    assert!(
        !fs.journal_writer.queue_never_blocks(),
        "a bounded channel must not report an unbounded queue"
    );
    // Keep the fixture under the bounded channel's capacity: this writer has
    // no consumer thread in testing mode, so more than `writer_channel_size`
    // journal writes would block in enqueue before the case under test runs.
    fs.mkdir("/bounded", true).expect("dir");
    fs.create("/bounded/f", false).expect("file");
    assert!(
        fs.journal_writer
            .with_testing_receiver(|receiver| {
                let mut drained = 0usize;
                while receiver.try_recv().is_ok() {
                    drained += 1;
                }
                drained
            })
            .unwrap_or(0)
            >= 2,
        "the fixture writes must be drained so the tested call starts from an empty queue"
    );

    // The body must never run when the queue can block: the entry point has to
    // answer Fallback first. A body that ran here would be a side effect taken
    // before the fallback decision.
    let mut body_ran = false;
    let outcome = fs.run_recursive_metadata_write(
        fs.journal_writer.metadata_epoch().unwrap(),
        "/bounded",
        InodeLockMode::Write,
        true,
        |_| {
            body_ran = true;
            Ok(())
        },
    );

    match outcome {
        Ok(ShardedOutcome::Fallback) => {}
        Ok(ShardedOutcome::Done(_)) => {
            panic!("a bounded journal queue must fall back before the body, not complete")
        }
        Err(error) => panic!("a bounded journal queue must fall back, not fail: {error}"),
    }
    assert!(
        !body_ran,
        "the body must not run when the plan falls back before it"
    );
    // The tree is untouched by the fallback decision.
    assert!(
        fs.file_status("/bounded").is_ok(),
        "the fallback must not have removed anything"
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

    // The exchange runs on its own thread: it must never be required to finish
    // while the planner is paused.
    let exchange_fs = fs.clone();
    let exchange_handle = std::thread::spawn(move || {
        exchange_fs.rename("/a/d0/left", "/a/d0/right", RenameFlags::EXCHANGE)
    });
    // Give the exchange a chance to queue behind the gate before we release.
    std::thread::sleep(Duration::from_millis(100));
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
