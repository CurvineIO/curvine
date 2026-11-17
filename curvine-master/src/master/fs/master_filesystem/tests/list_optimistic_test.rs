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

//! Deterministic interleavings for the listing's page validation.
//!
//! The lister parks after the page is planned and the view is released, before
//! the page locks are taken. A mutation made on the test thread in that window
//! changes the page the plan describes; the version check must notice and the
//! listing must answer for the state after the mutation, never for the stale
//! plan. Arrival is a channel and release is explicit; nothing sleeps.

use super::*;
use crate::master::fs::master_filesystem::memory_read_gates;
use std::time::Duration;

const GATE_PLAN: &str = "list-plan-";
const WAIT: Duration = Duration::from_secs(30);

fn parked_list<F>(fs: &MasterFilesystem, path: &str, mutate: F) -> FsResult<Vec<String>>
where
    F: FnOnce(&MasterFilesystem),
{
    let (release, entered) = memory_read_gates::install_with_entered(&format!("{GATE_PLAN}{path}"));
    let reader_fs = fs.clone();
    let reader_path = path.to_string();
    let reader = std::thread::spawn(move || {
        reader_fs
            .list_status(&reader_path)
            .map(|list| list.into_iter().map(|s| s.name).collect::<Vec<_>>())
    });
    if entered.recv_timeout(WAIT).is_err() {
        let early = reader.join().expect("the lister panicked");
        panic!("the lister never reached the plan gate; it returned early with {early:?}");
    }
    mutate(fs);
    release.send(()).ok();
    reader.join().expect("the lister panicked")
}

/// Every case lists its own directory: the gate is keyed by path, so a shared
/// name would let one case's lister consume another case's gate.
fn seeded(name: &str) -> (MasterFilesystem, String) {
    let fs = test_fs(name);
    let dir = format!("/{name}");
    fs.mkdir(&dir, true).unwrap();
    fs.create(&format!("{dir}/a"), true).unwrap();
    fs.create(&format!("{dir}/b"), true).unwrap();
    (fs, dir)
}

#[test]
fn a_create_after_planning_appears_in_the_page() {
    let (fs, dir) = seeded("list-opt-create");
    let new = format!("{dir}/c");
    let names = parked_list(&fs, &dir, |w| {
        w.create(&new, false).unwrap();
    })
    .unwrap();
    assert_eq!(
        names,
        ["a", "b", "c"],
        "the plan was stale and must be redone"
    );
}

#[test]
fn a_delete_after_planning_disappears_from_the_page() {
    let (fs, dir) = seeded("list-opt-delete");
    let gone = format!("{dir}/a");
    let names = parked_list(&fs, &dir, |w| {
        w.delete(&gone, false).unwrap();
    })
    .unwrap();
    assert_eq!(names, ["b"]);
}

#[test]
fn a_rename_out_after_planning_leaves_the_page() {
    let (fs, dir) = seeded("list-opt-rename");
    let other = format!("{dir}-other");
    fs.mkdir(&other, true).unwrap();
    let (src, dst) = (format!("{dir}/b"), format!("{other}/b"));
    let names = parked_list(&fs, &dir, |w| {
        w.rename(&src, &dst, RenameFlags::empty()).unwrap();
    })
    .unwrap();
    assert_eq!(names, ["a"]);
}

#[test]
fn removing_the_listed_directory_after_planning_is_not_found() {
    let (fs, dir) = seeded("list-opt-remove-dir");
    let target = dir.clone();
    let result = parked_list(&fs, &dir, |w| {
        w.delete(&target, true).unwrap();
    });
    assert!(result.is_err(), "the listed directory is gone: {result:?}");
}

#[test]
fn a_change_in_an_ancestor_after_planning_is_followed() {
    let fs = test_fs("list-opt-ancestor");
    fs.mkdir("/lo-x/y", true).unwrap();
    fs.create("/lo-x/y/f", true).unwrap();
    let result = parked_list(&fs, "/lo-x/y", |w| {
        w.rename("/lo-x", "/lo-z", RenameFlags::empty()).unwrap();
    });
    assert!(result.is_err(), "/lo-x/y no longer exists: {result:?}");
    assert_eq!(fs.list_status("/lo-z/y").unwrap().len(), 1);
}

#[test]
fn an_unchanged_page_is_returned_as_planned() {
    let (fs, dir) = seeded("list-opt-stable");
    let names = parked_list(&fs, &dir, |_| {}).unwrap();
    assert_eq!(names, ["a", "b"]);
}
