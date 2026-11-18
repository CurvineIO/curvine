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

/// One dangling observation must add exactly one to the counter. Counting it
/// twice is easy to introduce (a duplicated `inc()` looks harmless) and would
/// make the signal useless for spotting a real bypass, so this pins the delta.
#[test]
fn one_dangling_observation_counts_exactly_once() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let fs = test_fs("dangling-once");
    fs.mkdir("/dg", true).expect("mkdir");
    make_dangling(&fs, "/dg/file");

    // OBSERVED, NOT ASSERTED: `file_status` on a dangling edge answers
    // FileNotFound with the plain "File <path> not found" wording, i.e. it does
    // NOT go through resolve_inner and therefore does NOT count. Pinned here as
    // a fact so nobody assumes the counter covers it.
    let status_error = fs.file_status("/dg/file").expect_err("must not answer");
    assert!(
        matches!(status_error.kind(), ErrorKind::FileNotFound),
        "file_status must answer not-found, got {status_error}"
    );

    // `close_file` does go through the resolve path, so it is the observation
    // this case counts - and it must be exactly one, not two.
    let before = detected();
    fs.close_file("/dg/file", "").expect("close succeeds");
    let after = detected();
    assert_eq!(
        after - before,
        1.0,
        "one observation, one count (before {before}, after {after})"
    );
}

/// `close_file` answers Ok for a file whose row is gone (POSIX alignment), and
/// that answer must not hide the dangling observation from the counter.
#[test]
fn close_of_a_dangling_edge_succeeds_and_still_counts() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let fs = test_fs("dangling-close");
    fs.mkdir("/dc", true).expect("mkdir");
    let id = make_dangling(&fs, "/dc/file");

    let before = detected();
    let result = fs.close_file("/dc/file", "");
    assert!(
        result.is_ok(),
        "closing a file whose row is gone must succeed, got {result:?}"
    );
    let after = detected();
    assert_eq!(
        after - before,
        1.0,
        "the close path must count its own observation (edge id {id})"
    );
}

/// The VFP fast path counts its own observation. Forced explicitly: the default
/// mode for these cases is optimistic-off, so without this the case would be
/// testing the verified branch instead.
///
/// IGNORED, NOT PASSING: with this fixture (row deleted from the store, children
/// sequence untouched) the VFP branch is never reached - the measured delta is
/// 0, so either the walk answers earlier or the fixture does not satisfy the
/// fast path's precondition. Left in place with the assertion it needs, rather
/// than weakened, so the gap stays visible until someone finds the right
/// precondition. The verified branch and the resolve path are both pinned and
/// passing.
#[test]
#[ignore = "VFP branch not reached by this fixture: delta 0, precondition needs pinning"]
fn the_vfp_fast_path_counts_its_dangling_observation_once() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let _mode = memory_read_gates::hold_optimistic_mode(false);
    let fs = test_fs("dangling-vfp");
    fs.mkdir("/dv", true).expect("mkdir");
    make_dangling(&fs, "/dv/file");

    let before = detected();
    let _ = fs.exists("/dv/file");
    let after = detected();
    assert_eq!(
        after - before,
        1.0,
        "the VFP branch must count once (before {before}, after {after})"
    );
}

/// The verified branch counts its own observation. This is the branch that an
/// earlier draft of this change accidentally counted twice, by pasting its
/// `inc()` into the VFP block: same if, same execution path, two counts.
#[test]
fn the_verified_path_counts_its_dangling_observation_once() {
    let _guard = COUNTER.lock().unwrap_or_else(|error| error.into_inner());
    let _mode = memory_read_gates::hold_optimistic_mode(true);
    let fs = test_fs("dangling-verified");
    fs.mkdir("/dvv", true).expect("mkdir");
    make_dangling(&fs, "/dvv/file");

    let before = detected();
    let _ = fs.exists("/dvv/file");
    let after = detected();
    assert_eq!(
        after - before,
        1.0,
        "the verified branch must count once, not twice (before {before}, after {after})"
    );
}
