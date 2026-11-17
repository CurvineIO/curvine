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

//! Deterministic interleavings for the `exists` directory-version fast path.
//!
//! Each case parks a reader at a `cfg(test)` gate, optionally mutates the
//! namespace from the test thread, then releases the reader. Arrival is proved
//! by a channel, the gate is always released, and no sleep is used to claim an
//! interleaving happened.
//!
//! A version change makes the first walk fall back to the full second walk.
//! That walk can then disagree with the first plan, so `exists_memory` retries
//! and the retry legitimately hits a stable plan again. Cases therefore assert
//! the fallback the mutation must force plus the final answer, never that the
//! global hit counter stayed flat: the retry itself, and any nested `exists`
//! inside the mutation, also move those counters.

use super::*;
use crate::master::fs::master_filesystem::memory_read_gates;
use std::sync::Mutex;
use std::time::Duration;

/// Gate reached after the first plan releases its view and before the inode
/// locks are taken. Only tests that mutate the namespace use it.
const GATE_PLAN: &str = "exists-vp-plan-";
/// Gate only the reuse branch reaches.
const GATE_HIT: &str = "exists-vp-hit-";
/// Bounded wait for the gate handshake; a stuck case fails, never hangs.
pub(super) const WAIT: Duration = Duration::from_secs(30);
/// The mutation must finish far inside the hold the gate applies. A reader that
/// reached the gate stays parked until this test releases it unless the hold
/// itself times out, so keeping the mutation under this budget is what proves
/// the window was still open while it ran. If the production hold is ever
/// reduced below this budget, reduce the budget with it.
const MUTATION_BUDGET: Duration = Duration::from_secs(5);

/// The hit/fallback counters are process-wide, so counter-sensitive cases run
/// one at a time. Poison is ignored: each case asserts its own outcome.
static COUNTERS: Mutex<()> = Mutex::new(());

/// Cases here pin the locked fallback's version-reuse branch, which the
/// optimistic walk would otherwise answer before it is reached.
fn counter_guard() -> (
    std::sync::MutexGuard<'static, ()>,
    memory_read_gates::OptimisticMode,
) {
    let counters = COUNTERS.lock().unwrap_or_else(|error| error.into_inner());
    (counters, memory_read_gates::hold_optimistic_mode(true))
}

fn branches(path: &str) -> (u64, u64) {
    (
        memory_read_gates::vp_hits_for(path),
        memory_read_gates::vp_fallbacks_for(path),
    )
}

/// Runs `exists(path)` on a worker, waits until it parks at `gate`, applies
/// `mutate` on the test thread, then releases the reader and returns its result.
///
/// The mutation runs on this thread, so no writer thread exists that a join
/// could block on and no captured object can delay it. The window is proved by
/// two facts: the reader signalled that it reached the gate, and the mutation
/// finished within `MUTATION_BUDGET`, far below the hold the gate applies. An
/// overrun fails the case instead of letting the reader time out unnoticed.
pub(super) fn exists_parked<F>(
    fs: &MasterFilesystem,
    path: &str,
    gate: String,
    mutate: F,
) -> FsResult<bool>
where
    F: FnOnce(&MasterFilesystem),
{
    let (release, entered) = memory_read_gates::install_with_entered(&gate);
    let timeouts_before = memory_read_gates::vp_gate_timeouts();
    let reader_fs = fs.clone();
    let reader_path = path.to_string();
    let reader = std::thread::spawn(move || reader_fs.exists(&reader_path));

    entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the exists version gate");

    let started = std::time::Instant::now();
    mutate(fs);
    let mutated_within_budget = started.elapsed() < MUTATION_BUDGET;

    release.send(()).ok();
    let result = reader.join().expect("the reader panicked");

    // A released gate is the only outcome that keeps the window controlled: a
    // reader that timed out left on its own and the mutation proved nothing.
    assert_eq!(
        memory_read_gates::vp_gate_timeouts(),
        timeouts_before,
        "the gate timed out instead of being released, so the window was not controlled"
    );
    assert!(
        mutated_within_budget,
        "the mutation overran the gate budget, so the parked window cannot be proved"
    );
    result
}

fn no_mutation(_: &MasterFilesystem) {}

#[test]
fn stable_file_and_directory_targets_reuse_the_first_plan() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-stable");
    fs.mkdir("/vp", true).unwrap();
    let file_id = fs.create("/vp/file", true).unwrap().id;
    let dir_id = fs.mkdir("/vp/dir", true).unwrap().id;

    // The reuse gate only fires on the fast path, so reaching it proves the
    // branch instead of assuming it.
    let (hits_file, fallbacks_file) = branches("/vp/file");
    assert!(exists_parked(&fs, "/vp/file", format!("{GATE_HIT}/vp/file"), no_mutation).unwrap());
    let (hits_after_file, fallbacks_after_file) = branches("/vp/file");
    assert_eq!(
        hits_after_file - hits_file,
        1,
        "a stable file read must reuse the plan"
    );
    assert_eq!(
        fallbacks_after_file, fallbacks_file,
        "a stable file read must not fall back"
    );

    let (hits_dir, fallbacks_dir) = branches("/vp/dir");
    assert!(exists_parked(&fs, "/vp/dir", format!("{GATE_HIT}/vp/dir"), no_mutation).unwrap());
    let (hits_after_dir, fallbacks_after_dir) = branches("/vp/dir");
    assert_eq!(
        hits_after_dir - hits_dir,
        1,
        "a stable directory read must reuse the plan"
    );
    assert_eq!(
        fallbacks_after_dir, fallbacks_dir,
        "a stable directory read must not fall back"
    );

    // The reused plan keeps the same target identities as a full walk.
    assert_eq!(fs.file_status("/vp/file").unwrap().id, file_id);
    assert_eq!(fs.file_status("/vp/dir").unwrap().id, dir_id);
}

#[test]
fn missing_last_edge_reuses_the_plan_and_reports_absent() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-missing-edge");
    fs.mkdir("/vp", true).unwrap();
    fs.create("/vp/file", true).unwrap();

    let (hits, fallbacks) = branches("/vp/absent");
    assert!(!exists_parked(
        &fs,
        "/vp/absent",
        format!("{GATE_HIT}/vp/absent"),
        no_mutation
    )
    .unwrap());
    assert_eq!(
        branches("/vp/absent"),
        (hits + 1, fallbacks),
        "an absent last edge is still covered by the captured versions"
    );
}

#[test]
fn cross_parent_rename_between_plan_and_check_falls_back() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-rename");
    fs.mkdir("/vp/a", true).unwrap();
    fs.mkdir("/vp/b", true).unwrap();
    fs.create("/vp/a/file", true).unwrap();

    let (hits, fallbacks) = branches("/vp/a/file");
    let moved = exists_parked(
        &fs,
        "/vp/a/file",
        format!("{GATE_PLAN}/vp/a/file"),
        |writer| {
            writer
                .rename("/vp/a/file", "/vp/b/file", RenameFlags::empty())
                .unwrap();
        },
    )
    .unwrap();

    assert!(!moved, "the source edge is gone, so the name is absent");
    let (hits_after, fallbacks_after) = branches("/vp/a/file");
    assert_eq!(
        fallbacks_after - fallbacks,
        1,
        "a changed parent must fall back to the full second walk"
    );
    assert!(
        hits_after >= hits,
        "the retry may legitimately reuse a stable plan"
    );
    assert!(fs.exists("/vp/b/file").unwrap());
}

#[test]
fn create_between_plan_and_check_falls_back() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-create");
    fs.mkdir("/vp", true).unwrap();

    let (hits, fallbacks) = branches("/vp/new");
    let appeared = exists_parked(&fs, "/vp/new", format!("{GATE_PLAN}/vp/new"), |writer| {
        writer.create("/vp/new", false).unwrap();
    })
    .unwrap();

    assert!(appeared, "the name exists by the time the read completes");
    let (hits_after, fallbacks_after) = branches("/vp/new");
    assert_eq!(
        fallbacks_after - fallbacks,
        1,
        "a new edge must fall back to the full second walk"
    );
    assert!(
        hits_after >= hits,
        "the retry may legitimately reuse a stable plan"
    );
}

#[test]
fn delete_between_plan_and_check_falls_back() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-delete");
    fs.mkdir("/vp", true).unwrap();
    fs.create("/vp/del", true).unwrap();

    let (hits, fallbacks) = branches("/vp/del");
    let present = exists_parked(&fs, "/vp/del", format!("{GATE_PLAN}/vp/del"), |writer| {
        writer.delete("/vp/del", false).unwrap();
    })
    .unwrap();

    assert!(!present, "the edge was deleted before the check");
    let (hits_after, fallbacks_after) = branches("/vp/del");
    assert_eq!(
        fallbacks_after - fallbacks,
        1,
        "a deleted edge must fall back to the full second walk"
    );
    assert!(
        hits_after >= hits,
        "the retry may legitimately reuse a stable plan"
    );
}

#[test]
fn same_name_replacement_after_delete_falls_back() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-swap");
    let original = fs.create("/swap", true).unwrap().id;

    let (hits, fallbacks) = branches("/swap");
    let present = exists_parked(&fs, "/swap", format!("{GATE_PLAN}/swap"), |writer| {
        writer.delete("/swap", false).unwrap();
        writer.create("/swap", false).unwrap();
    })
    .unwrap();

    // The name is present again through a different inode. Reusing the first
    // plan would answer with the deleted identity, so the answer must come
    // from a walk that saw the replacement.
    assert!(present);
    let replaced = fs.file_status("/swap").unwrap().id;
    assert_ne!(
        replaced, original,
        "the fixture must replace the inode, not reopen it"
    );
    let (hits_after, fallbacks_after) = branches("/swap");
    assert_eq!(
        fallbacks_after - fallbacks,
        1,
        "a replaced edge must fall back to the full second walk"
    );
    assert!(
        hits_after >= hits,
        "the retry may legitimately reuse a stable plan"
    );
}

#[test]
fn failed_holder_in_the_window_keeps_the_terminal_contract() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-failed");
    fs.mkdir("/vp", true).unwrap();
    fs.create("/vp/file", true).unwrap();

    let path = "/vp/file";
    let gate = format!("{GATE_PLAN}{path}");
    let (release, entered) = memory_read_gates::install_with_entered(&gate);
    let timeouts_before = memory_read_gates::vp_gate_timeouts();
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || reader_fs.exists(path));
    entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the exists version gate");

    // The store turns terminal inside the window. Reusing the first plan must
    // not answer from it: restore epoch, store availability and the file row
    // check stay in force.
    let cold = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    // `fail_closed` consumes the guard, so it also releases the cold slot.
    cold.fail_closed();
    release.send(()).expect("the parked reader is gone");
    let result = reader.join().unwrap();
    assert_eq!(
        memory_read_gates::vp_gate_timeouts(),
        timeouts_before,
        "the gate timed out instead of being released, so the window was not controlled"
    );

    assert!(
        !matches!(result, Ok(true)),
        "a terminal store must never answer a stable reuse with true: {result:?}"
    );
    match result {
        Ok(true) => panic!("a terminal store must never answer a stable reuse with true"),
        Ok(false) => panic!("a live edge must not turn into absent behind a terminal store"),
        Err(error) => assert!(
            error.to_string().contains("JournalUnavailable"),
            "a terminal store must surface the journal error, got {error}"
        ),
    }
}

#[test]
fn whole_tree_restore_in_the_window_retries_instead_of_reusing() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-restore-tree");
    fs.mkdir("/vp", true).unwrap();
    fs.create("/vp/restored", true).unwrap();

    // The checkpoint keeps the file while the live tree drops it, so a plan
    // captured before the restore must not answer for the restored tree.
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    fs.delete("/vp/restored", false).unwrap();
    assert!(!fs.exists("/vp/restored").unwrap());

    let (hits, fallbacks) = branches("/vp/restored");
    let present = exists_parked(
        &fs,
        "/vp/restored",
        format!("{GATE_PLAN}/vp/restored"),
        |writer| {
            // A whole-tree restore: RocksDB and the in-memory namespace are replaced
            // from the checkpoint. Reusing the pre-restore plan would answer absent
            // for a file the restored tree holds again.
            writer
                .fs_dir
                .begin_cold(DEFAULT_COLD_TIMEOUT)
                .unwrap()
                .restore(&checkpoint, 0)
                .unwrap();
        },
    )
    .unwrap();

    assert!(
        present,
        "the restored tree holds the file, so the retried read must find it"
    );
    let (hits_after, fallbacks_after) = branches("/vp/restored");
    assert_eq!(
        fallbacks_after, fallbacks,
        "a restore moves the epoch, so the retry runs before the version check"
    );
    assert_eq!(
        hits_after - hits,
        1,
        "the retry reads the restored plan and reuses it"
    );
}

#[test]
fn missing_inode_row_behind_a_live_edge_keeps_the_documented_error() {
    let _guard = counter_guard();
    let fs = test_fs("exists-vp-dangling");
    fs.mkdir("/vp", true).unwrap();
    let id = fs.create("/vp/file", true).unwrap().id;

    // Drop only the inode row; the directory edge stays live.
    {
        let view = fs.fs_dir.view().unwrap();
        let mut batch = view.store.new_batch();
        batch.delete_inode(id).unwrap();
        batch.commit().unwrap();
    }

    match fs.exists("/vp/file") {
        Ok(answered) => panic!("a dangling edge must not report {answered}"),
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("directory edge references missing inode"),
                "the documented dangling error must survive the fast path, got {message}"
            );
        }
    }
}
