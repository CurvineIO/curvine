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
    // Releasing the guard wakes the worker; joining it is what proves the
    // wake-up actually happened, so no grace-period sleep is needed here.
    drop(held);
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

    // The writer must prove real business progress, not just that it looped:
    // a run where every create/delete failed would otherwise still report
    // rounds > 0 and make the test pass without any interleaving at all.
    struct Progress {
        created: u64,
        deleted: u64,
        first_error: Option<String>,
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let writer_fs = fs.clone();
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut announced = false;
        let mut progress = Progress {
            created: 0,
            deleted: 0,
            first_error: None,
        };
        while !writer_stop.load(Ordering::Relaxed) {
            let path = format!("/other/f{}", (progress.created + progress.deleted) % 128);
            match writer_fs.create(&path, false) {
                Ok(_) => {
                    progress.created += 1;
                    if let Err(e) = writer_fs.delete(&path, false) {
                        progress
                            .first_error
                            .get_or_insert_with(|| format!("delete {path}: {e}"));
                        break;
                    }
                    progress.deleted += 1;
                    if !announced {
                        let _ = started_tx.send(());
                        announced = true;
                    }
                }

                Err(e) => {
                    progress
                        .first_error
                        .get_or_insert_with(|| format!("create {path}: {e}"));
                    break;
                }
            }
        }
        let _ = progress_tx.send(progress);
    });

    // Handshake instead of a sleep: the writer announces its first completed
    // create/delete round, so the delete starts against a writer that is
    // demonstrably running.
    started_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the unrelated writer must complete a first round");
    let outcome = run_bounded(&fs, |fs| fs.delete("/target", true));
    stop.store(true, Ordering::Relaxed);

    // Bounded join: a stuck writer must fail the test with a clear reason
    // instead of hanging the whole suite.
    let progress = progress_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the unrelated writer must stop within 30s after the delete returns");
    writer.join().expect("writer thread must stop");

    assert!(
        progress.created > 0 && progress.deleted > 0,
        "fixture: the unrelated writer must have created and deleted real files \
         (created={}, deleted={})",
        progress.created,
        progress.deleted
    );
    assert!(
        progress.first_error.is_none(),
        "the unrelated writer must not be disturbed by the delete: {:?}",
        progress.first_error
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

#[path = "recursive_lock_set_topology_test.rs"]
mod recursive_lock_set_topology_test;
