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

//! A dangling edge is an edge that is still in memory while its inode row is
//! gone. The decision under test is that this state is never repaired: it means
//! a delete was observed before its edge was detached, or a repair path was
//! bypassed, so every reader reports one consistent failure and each observation
//! is counted exactly once.

use super::*;
use super::memory_read_gates;
use curvine_error::ErrorKind;
use std::sync::Mutex;

/// The counter is process-wide, so counter-sensitive cases run one at a time.
static COUNTER: Mutex<()> = Mutex::new(());

fn detected() -> f64 {
    crate::master::Master::get_metrics()
        .expect("metrics")
        .namespace_dangling_edge_detected
        .get() as f64
}

/// Leaves a live edge whose inode row is gone.
fn make_dangling(fs: &MasterFilesystem, path: &str) -> i64 {
    let id = fs.create(path, true).expect("create").id;
    let view = fs.fs_dir.view().expect("view");
    let mut batch = view.store.new_batch();
    batch.delete_inode(id).expect("delete row");
    batch.commit().expect("commit");
    id
}

/// VFP branch: optimistic off skips `exists_optimistic`, so the walk reaches the
/// VFP check. A row deleted straight from the store leaves the edge and the
/// children sequence alone, which is exactly the state the counter exists for.
#[test]
fn the_vfp_fast_path_counts_its_dangling_observation_once() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let _mode = memory_read_gates::hold_optimistic_mode(true);
    let fs = test_fs("dangling-vfp");
    fs.mkdir("/dv", true).expect("mkdir");
    make_dangling(&fs, "/dv/file");

    let hits0 = memory_read_gates::vp_hits_for("/dv/file");
    let before = detected();
    let error = fs.exists("/dv/file").expect_err("must not answer");
    let after = detected();
    let hits1 = memory_read_gates::vp_hits_for("/dv/file");

    assert!(
        error.to_string().contains("Failed to load inode"),
        "the VFP branch must keep the store-load wording, got {error}"
    );
    eprintln!("[VFP]  vp_hits {hits0} -> {hits1}, vp_fallbacks {} -> {}, counter {before} -> {after}",
        memory_read_gates::vp_fallbacks_for("/dv/file"), memory_read_gates::vp_fallbacks_for("/dv/file"));
    assert_eq!(hits1 - hits0, 1, "the case must reach the VFP branch, vp_hits {hits0} -> {hits1}");
    assert_eq!(
        after - before,
        1.0,
        "the VFP branch must count once (before {before}, after {after})"
    );
}

/// Verified branch: the reader is parked after its first plan, a real mutation
/// advances the directory sequence inside that window, and the reader therefore
/// cannot reuse the plan and must take the verified path.
#[test]
fn the_verified_path_counts_its_dangling_observation_once() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let _mode = memory_read_gates::hold_optimistic_mode(true);
    let fs = test_fs("dangling-verified");
    fs.mkdir("/dvv", true).expect("mkdir");
    make_dangling(&fs, "/dvv/file");
    fs.create("/dvv/other", false).expect("sibling");

    let before = detected();
    let fallbacks0 = memory_read_gates::vp_fallbacks_for("/dvv/file");
    // The mutation must advance /dvv's sequence, and must not touch the
    // dangling file itself: creating a sibling is enough.
    let result = super::exists_version_fastpath_test::exists_parked(
        &fs,
        "/dvv/file",
        format!("exists-vp-plan-/dvv/file"),
        |fs| {
            fs.create("/dvv/other2", false).expect("sibling");
        },
    );
    let after = detected();
    let fallbacks1 = memory_read_gates::vp_fallbacks_for("/dvv/file");

    eprintln!("[VERIFIED] vp_hits {} -> {}, vp_fallbacks {fallbacks0} -> {fallbacks1}, counter {before} -> {after}",
        memory_read_gates::vp_hits_for("/dvv/file"), memory_read_gates::vp_hits_for("/dvv/file"));
    assert!(result.is_err(), "a dangling edge must not answer, got {result:?}");
    assert_eq!(
        fallbacks1 - fallbacks0,
        1,
        "the case must reach the verified path, vp_fallbacks {fallbacks0} -> {fallbacks1}"
    );
    assert_eq!(
        after - before,
        1.0,
        "the verified branch must count once, not twice (before {before}, after {after})"
    );
}

/// Resolve path via close_file: answers Ok, still counts, and must not go
/// through the exists gates (if resolve were ever routed into the optimistic
/// path, this assertion is what would notice).
#[test]
fn close_of_a_dangling_edge_succeeds_and_still_counts() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let fs = test_fs("dangling-close");
    fs.mkdir("/dc", true).expect("mkdir");
    let id = make_dangling(&fs, "/dc/file");

    let hits0 = memory_read_gates::vp_hits_for("/dc/file");
    let fallbacks0 = memory_read_gates::vp_fallbacks_for("/dc/file");
    let before = detected();
    let result = fs.close_file("/dc/file", "");
    let after = detected();

    eprintln!("[CLOSE] vp_hits {} -> {}, vp_fallbacks {} -> {}, counter {before} -> {after}",
        memory_read_gates::vp_hits_for("/dc/file"), memory_read_gates::vp_hits_for("/dc/file"),
        memory_read_gates::vp_fallbacks_for("/dc/file"), memory_read_gates::vp_fallbacks_for("/dc/file"));
    assert!(result.is_ok(), "closing a file whose row is gone must succeed, got {result:?}");
    assert_eq!(
        memory_read_gates::vp_hits_for("/dc/file") - hits0,
        0,
        "close must not enter the exists optimistic path"
    );
    assert_eq!(
        memory_read_gates::vp_fallbacks_for("/dc/file") - fallbacks0,
        0,
        "close must not enter the exists VFP path"
    );
    assert_eq!(
        after - before,
        1.0,
        "the close path must count its own observation (edge id {id})"
    );
}
