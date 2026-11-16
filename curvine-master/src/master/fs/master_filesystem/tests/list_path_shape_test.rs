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

//! Path-shape cases for the paged list read: a child of the root is `/name`,
//! and the root listing feeds `/` (not `//`) to the batched row lookup.

use super::*;

#[test]
fn root_children_are_reported_with_a_single_separator() {
    let fs = test_fs("list-root-shape");
    fs.mkdir("/alpha", true).unwrap();
    fs.create("/beta", true).unwrap();

    let entries = fs.list_status("/").unwrap();
    let paths: Vec<&str> = entries.iter().map(|s| s.path.as_str()).collect();

    assert!(
        paths.contains(&"/alpha") && paths.contains(&"/beta"),
        "root children must be reported as /name, got {paths:?}"
    );
    assert!(
        !paths.iter().any(|p| p.starts_with("//")),
        "no root child may carry a doubled separator, got {paths:?}"
    );
}

#[test]
fn root_listing_resolves_every_child_row() {
    let fs = test_fs("list-root-parent");
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();

    // The root case feeds `/` as the parent path; handing over `//` would miss
    // the rows and return fewer children than the directory holds.
    let entries = fs.list_status("/").unwrap();
    assert_eq!(
        entries.iter().filter(|s| s.path == "/dir").count(),
        1,
        "the root listing must resolve the /dir row exactly once"
    );
    let nested = fs.list_status("/dir").unwrap();
    assert_eq!(
        nested.iter().map(|s| s.path.as_str()).collect::<Vec<_>>(),
        vec!["/dir/file"],
        "a nested listing keeps the parent path as-is"
    );
}

#[test]
fn hardlink_children_are_listed_under_their_own_names() {
    let fs = test_fs("list-hardlink-child");
    fs.mkdir("/hl", true).unwrap();
    fs.create("/hl/file", true).unwrap();
    fs.link("/hl/file", "/hl/alias").unwrap();

    let mut paths: Vec<String> = fs
        .list_status("/hl")
        .unwrap()
        .into_iter()
        .map(|s| s.path)
        .collect();
    paths.sort();

    assert_eq!(
        paths,
        vec!["/hl/alias".to_string(), "/hl/file".to_string()],
        "both the original and the hardlink must be listed under /hl"
    );
}

#[test]
fn a_terminal_store_rejects_the_list_read() {
    let fs = test_fs("list-terminal-store");
    fs.mkdir("/tm", true).unwrap();
    fs.create("/tm/file", true).unwrap();

    let cold = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    cold.fail_closed();

    // The list read keeps its explicit availability check instead of serving a
    // stale in-memory page behind a terminal store.
    let result = fs.list_status("/tm");
    assert!(
        result.is_err(),
        "a terminal store must not answer a list read: {result:?}"
    );
}

#[test]
fn root_page_honours_limit_and_start_after() {
    let fs = test_fs("list-page");
    for name in ["/pa", "/pb", "/pc"] {
        fs.mkdir(name, true).unwrap();
    }

    let first = fs.list_options("/", ListOptions::with_limit(2)).unwrap();
    assert_eq!(first.len(), 2, "the page must honour the limit");

    // `from_status` feeds `status.name`, and both containers resume with an
    // EXCLUDED lower bound (map: Bound::Excluded, packed: skip past the found
    // record), so the next page is strictly after that name.
    let last = first.last().unwrap();
    let cursor_name = last.name.clone();
    let rest = fs
        .list_options("/", ListOptions::from_status(2, last))
        .unwrap();
    assert!(
        rest.iter().all(|s| s.name.as_str() > cursor_name.as_str()),
        "start_after resumes strictly after {cursor_name}, got {:?}",
        rest.iter().map(|s| s.name.as_str()).collect::<Vec<_>>()
    );
    assert!(rest.len() <= 2, "the resumed page keeps the same limit");
    assert!(
        !rest.iter().any(|s| s.name == cursor_name),
        "the cursor entry must not be repeated"
    );
}

#[test]
fn pending_staging_path_is_hidden_from_the_list_read() {
    let fs = test_fs("list-pending");
    fs.mkdir("/pd", true).unwrap();
    let pending = crate::master::meta::trash::pending_entry_path("staged-entry");
    assert!(
        fs.list_status(&pending).is_err(),
        "the pending staging path is invisible to a list read"
    );
    assert!(
        fs.list_options(&pending, ListOptions::default()).is_err(),
        "list_options rejects the pending staging path at the boundary"
    );
}
