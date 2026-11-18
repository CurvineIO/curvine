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
