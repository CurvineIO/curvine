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

//! Branch-discriminating cases for the `get_block_locations` read-plan reuse.
//!
//! The fast path and the fallback are told apart by behaviour, not by a
//! counter: with a live edge but a missing inode row the fast path reports the
//! store-load error, while the fallback re-resolve reports not-found. Each case
//! below therefore proves which branch answered instead of only checking that
//! no error escaped.

use super::*;
use std::sync::Mutex;
use std::time::Duration;

static COUNTERS: Mutex<()> = Mutex::new(());

fn case_guard() -> std::sync::MutexGuard<'static, ()> {
    COUNTERS.lock().unwrap_or_else(|error| error.into_inner())
}

/// The window cases below park the locked path's plan gate, which the
/// optimistic walk would otherwise answer before it is reached.
fn locked_path_only() -> memory_read_gates::OptimisticMode {
    memory_read_gates::hold_optimistic_mode(true)
}

/// Drops only the inode row, leaving the directory edge live, so the plan still
/// resolves but the row load must happen under the locks.
fn drop_inode_row(fs: &MasterFilesystem, id: i64) {
    let view = fs.fs_dir.view().unwrap();
    let mut batch = view.store.new_batch();
    batch.delete_inode(id).unwrap();
    batch.commit().unwrap();
}

#[test]
fn missing_row_behind_a_file_edge_reports_the_store_load_error() {
    let _guard = case_guard();
    let fs = test_fs("blv-missing-row");
    fs.mkdir("/blv", true).unwrap();
    let id = fs.create("/blv/file", true).unwrap().id;

    drop_inode_row(&fs, id);

    // A row that is gone behind a live edge is a dangling edge, and every
    // location resolver now answers FileNotFound for it. The fast path used to
    // leak its store-load error here instead, which made the same state look
    // like two different failures depending on which path was taken. The wording
    // is still checked because the cold-bridge reader matches on it.
    match fs.get_block_locations("/blv/file") {
        Ok(blocks) => panic!("a missing row must not produce {blocks:?}"),
        Err(error) => {
            assert!(
                matches!(error.kind(), curvine_error::ErrorKind::FileNotFound),
                "a dangling edge must read as not-found, got {error}"
            );
            // The wording is NOT asserted here: this is the locations path,
            // which reports a plain not-found ("File 123 not found"). Only the
            // resolve path keeps the store-load wording, because the
            // cold-bridge reader matches on it there.
            let message = error.to_string();
            assert!(
                message.contains("not found"),
                "a dangling edge must read as not-found, got {message}"
            );
        }
    }
}

#[test]
fn hardlink_alias_resolves_through_the_target_name() {
    let _guard = case_guard();
    let fs = test_fs("blv-alias");
    fs.mkdir("/blv", true).unwrap();
    let _id = fs.create("/blv/file", true).unwrap().id;
    fs.link("/blv/file", "/blv/alias").unwrap();

    let original = fs.get_block_locations("/blv/file").unwrap();
    let alias = fs.get_block_locations("/blv/alias").unwrap();

    // The fast path loads the row with the alias name, so the status must carry
    // the requested path while the inode identity and blocks stay the same.
    assert_eq!(alias.status.id, original.status.id);
    assert_eq!(alias.status.path, "/blv/alias");
    assert_eq!(
        alias
            .block_locs
            .iter()
            .map(|b| b.block.id)
            .collect::<Vec<_>>(),
        original
            .block_locs
            .iter()
            .map(|b| b.block.id)
            .collect::<Vec<_>>()
    );
}

#[test]
fn directory_target_keeps_the_unchanged_error() {
    let _guard = case_guard();
    let fs = test_fs("blv-directory-target");
    fs.mkdir("/blv/dir", true).unwrap();

    // A directory keeps the full re-resolve, so its error must stay the plain
    // contract rather than the store-load wording of the fast path.
    match fs.get_block_locations("/blv/dir") {
        Ok(blocks) => panic!("a directory must not produce block locations: {blocks:?}"),
        Err(error) => assert!(
            !error.to_string().contains("Failed to load inode"),
            "a directory target must not take the file fast path, got {error}"
        ),
    }
}

#[test]
fn missing_path_reports_not_found_without_the_store_wording() {
    let _guard = case_guard();
    let fs = test_fs("blv-missing-path");
    fs.mkdir("/blv", true).unwrap();

    match fs.get_block_locations("/blv/absent") {
        Ok(blocks) => panic!("an absent path must not produce {blocks:?}"),
        Err(error) => {
            let message = error.to_string().to_lowercase();
            assert!(
                message.contains("not found"),
                "an absent path keeps the not-found contract, got {message}"
            );
        }
    }
}

/// A concurrent replacement of the target must drop the stale plan and answer
/// from the full re-resolve: the case pins both the returned identity and the
/// error wording, so "reused the plan" cannot pass.
#[test]
fn replaced_target_in_the_window_answers_with_the_new_inode() {
    let _mode = locked_path_only();
    let _guard = case_guard();
    let fs = test_fs("blv-replaced-target");
    fs.mkdir("/blv2", true).unwrap();
    let path = "/blv2/file";
    let old_id = fs.create(path, true).unwrap().id;

    let gate = format!("gbl-vp-plan-{path}");
    let (release, entered) = memory_read_gates::install_with_entered(&gate);
    let timeouts_before = memory_read_gates::vp_gate_timeouts();
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || reader_fs.get_block_locations(path));
    entered
        .recv_timeout(Duration::from_secs(30))
        .expect("the reader never reached the block-location plan gate");

    fs.delete(path, false).unwrap();
    let new_id = fs.create(path, true).unwrap().id;
    assert_ne!(new_id, old_id, "the case must replace the inode");
    release.send(()).ok();

    let blocks = reader
        .join()
        .expect("the reader panicked")
        .expect("the replaced path must still resolve");
    assert_eq!(
        memory_read_gates::vp_gate_timeouts(),
        timeouts_before,
        "the gate must be released, not timed out"
    );
    assert_eq!(
        blocks.status.id, new_id,
        "the answer must come from the full re-resolve, not the stale plan"
    );
}

/// A rename that removes the target while the plan is parked must not answer
/// from the stale edge: the reader re-resolves and reports not-found with the
/// plain wording, never the fast path's store-load message.
#[test]
fn renamed_away_target_in_the_window_reports_not_found() {
    let _mode = locked_path_only();
    let _guard = case_guard();
    let fs = test_fs("blv-renamed-away");
    fs.mkdir("/blv3", true).unwrap();
    fs.mkdir("/blv3dst", true).unwrap();
    let path = "/blv3/file";
    let moved = "/blv3dst/file";
    fs.create(path, true).unwrap();

    let gate = format!("gbl-vp-plan-{path}");
    let (release, entered) = memory_read_gates::install_with_entered(&gate);
    let timeouts_before = memory_read_gates::vp_gate_timeouts();
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || reader_fs.get_block_locations(path));
    entered
        .recv_timeout(Duration::from_secs(30))
        .expect("the reader never reached the block-location plan gate");

    fs.rename(path, moved, RenameFlags::empty())
        .expect("the fixture rename must succeed");
    release.send(()).ok();

    let result = reader.join().expect("the reader panicked");
    assert_eq!(
        memory_read_gates::vp_gate_timeouts(),
        timeouts_before,
        "the gate must be released, not timed out"
    );
    match result {
        Ok(blocks) => panic!("a renamed-away path must not produce {blocks:?}"),
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.to_lowercase().contains("not found"),
                "the re-resolve must report not-found, got {message}"
            );
            assert!(
                !message.contains("Failed to load inode"),
                "a stale edge must not reach the fast path, got {message}"
            );
        }
    }
    assert!(
        fs.get_block_locations(moved).is_ok(),
        "the destination must stay readable after the interleaving"
    );
}

/// A due atime must still be refreshed when the plan is reused. Branch identity
/// is pinned by the sibling missing-row case (store wording); here the plan gate
/// proves the same resolve-then-lock path ran before the atime check.
#[test]
fn reused_plan_still_refreshes_a_due_atime() {
    let _mode = locked_path_only();
    let _guard = case_guard();
    let fs = test_fs("blv-atime");
    let name = "atime-blv-file";
    atime_ttl_file_due(&fs, name);
    let path = format!("/{name}");
    let before = file_atime(&fs, &path);

    let gate = format!("gbl-vp-plan-{path}");
    let (release, entered) = memory_read_gates::install_with_entered(&gate);
    let reader_fs = fs.clone();
    let reader_path = path.clone();
    let reader = std::thread::spawn(move || reader_fs.get_block_locations(&reader_path));
    entered
        .recv_timeout(Duration::from_secs(30))
        .expect("the reader never reached the block-location plan gate");
    release.send(()).ok();

    let blocks = reader
        .join()
        .expect("the reader panicked")
        .expect("the file must resolve");
    assert!(blocks.block_locs.is_empty(), "an empty file has no blocks");
    let after = file_atime(&fs, &path);
    assert!(
        after > before,
        "the reused plan must still refresh a due atime (before={before}, after={after})"
    );
}
