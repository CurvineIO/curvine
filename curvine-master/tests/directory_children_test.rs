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

use curvine_master::master::meta::inode::{DirectoryChildren, InodeDir, InodeView};
use std::sync::{mpsc, Arc};

fn file_edge(id: i64) -> InodeView {
    InodeView::new_file(
        format!("f{id}"),
        curvine_master::master::meta::inode::InodeFile::new(id, 1),
    )
}

#[test]
fn concurrent_add_and_read_never_serves_stale_version_as_current() {
    let children = Arc::new(DirectoryChildren::new());
    children.add_child(file_edge(1)).unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    for round in 0..2000 {
        let writer = children.clone();
        let writer_done = done_tx.clone();
        let t = std::thread::spawn(move || {
            for i in 0..8 {
                writer
                    .add_child(InodeView::new_file(
                        format!("w{}-{}", round, i),
                        curvine_master::master::meta::inode::InodeFile::new(1, 1),
                    ))
                    .unwrap();
            }
            writer_done.send(()).unwrap();
        });
        let (view, version) = children.read_child_view("f1");
        assert!(view.is_some(), "f1 must always be present");
        // The read may overlap the writer; the token must not be accepted
        // by version_is when its own validation has already failed.
        assert!(!(!version.is_current() && children.version_is(&version)));
        t.join().unwrap();
    }
    drop(done_tx);
    assert!(done_rx.iter().count() == 2000);
}

#[test]
fn delete_child_returns_owned_inode_and_keeps_handle_consistent() {
    let mut dir = InodeDir::new(1, 1);
    dir.add_child(file_edge(2)).unwrap();
    assert_eq!(dir.children_len(), 1);
    let removed = dir.delete_child(2, "f2").unwrap();
    assert_eq!(removed.id(), 2);
    assert_eq!(dir.children_len(), 0);
}

#[test]
fn listing_is_ordered_and_paginated_before_and_after_sharding() {
    use curvine_config::ChildrenContainer;
    use curvine_master::master::meta::inode::InodeChildren;
    use curvine_model::ListOptions;

    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = DirectoryChildren::with_container(InodeChildren::new(container));
        for id in [9, 1, 12, 2, 10] {
            children.add_child(file_edge(id)).unwrap();
        }
        let check = || {
            let names = |options: &ListOptions| {
                children
                    .list_options(options)
                    .iter()
                    .map(|child| child.name().to_owned())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                names(&ListOptions::default()),
                ["f1", "f10", "f12", "f2", "f9"]
            );
            assert_eq!(
                names(&ListOptions {
                    limit: Some(2),
                    start_after: Some("f10".into()),
                }),
                ["f12", "f2"]
            );
            assert!(names(&ListOptions {
                limit: Some(0),
                start_after: None
            })
            .is_empty());
        };
        check();
        children.seed_shards_for_bulk_load(4096);
        check();
    }
}

#[test]
fn mismatched_child_id_does_not_remove_the_edge() {
    use curvine_config::ChildrenContainer;
    use curvine_master::master::meta::inode::InodeChildren;

    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = DirectoryChildren::with_container(InodeChildren::new(container));
        children.add_child(file_edge(2)).unwrap();
        assert!(children.delete_child(99, "f2").is_err());
        assert_eq!(children.iter_len(), 1, "{container:?}");
        assert_eq!(children.read_child_view("f2").0.unwrap().id(), 2);
    }
}

#[test]
fn sharded_count_waits_for_inflight_mutation() {
    use std::time::Duration;
    let children = Arc::new(DirectoryChildren::new());
    children.seed_shards_for_bulk_load(4096);
    children.add_child(file_edge(1)).unwrap();
    assert_eq!(children.iter_len(), 1); // Populate the count cache.
    let mut mutation = children.begin_mutation("f2");
    let reader = children.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx.send(reader.iter_len()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = result_rx.recv_timeout(Duration::from_millis(100)).ok();
    mutation.add_child(file_edge(2)).unwrap();
    drop(mutation);
    thread.join().unwrap();
    assert_eq!(early, None, "count bypassed an active mutation");
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
}

#[test]
fn sharded_count_cache_invalidates_after_each_mutation() {
    let children = DirectoryChildren::new();
    children.seed_shards_for_bulk_load(4096);
    for id in 0..512 {
        children.add_child(file_edge(id)).unwrap();
    }
    for id in 0..512 {
        assert_eq!(children.iter_len(), 512 - id as usize);
        assert_eq!(children.iter_len(), 512 - id as usize);
        children.delete_child(id, &format!("f{id}")).unwrap();
    }
    assert_eq!(children.iter_len(), 0);
}

#[test]
fn sharded_count_remains_consistent_during_concurrent_renames() {
    let children = Arc::new(DirectoryChildren::new());
    children.seed_shards_for_bulk_load(4096);
    for id in 0..4 {
        children.add_child(file_edge(id)).unwrap();
    }
    std::thread::scope(|scope| {
        for id in 0..4 {
            let children = children.clone();
            scope.spawn(move || {
                let first = format!("f{id}");
                let second = format!("moved-{id}");
                for _ in 0..2000 {
                    children
                        .begin_rename_mutation(&first, &second)
                        .rename_child(id, &first, None, &second)
                        .unwrap();
                    children
                        .begin_rename_mutation(&second, &first)
                        .rename_child(id, &second, None, &first)
                        .unwrap();
                }
            });
        }
        for _ in 0..2000 {
            assert_eq!(children.iter_len(), 4);
        }
    });
    assert_eq!(children.iter_len(), 4);
}

#[test]
fn child_snapshots_survive_concurrent_deletes() {
    let children = Arc::new(DirectoryChildren::new());
    let dir = InodeDir::with_children(1, 1, children.clone());
    let writer_children = children.clone();
    let writer = std::thread::spawn(move || {
        for _ in 0..2000 {
            writer_children.add_child(file_edge(2)).unwrap();
            writer_children.delete_child(2, "f2").unwrap();
        }
    });
    let pattern = glob::Pattern::new("f*").unwrap();
    for _ in 0..2000 {
        if let Some(child) = children.read_child_view("f2").0 {
            assert_eq!((child.id(), child.name()), (2, "f2"));
        }
        for child in dir.children_vec() {
            assert_eq!(child.id(), 2);
            assert_eq!(child.name(), "f2");
        }
        for child in dir.list_options(&Default::default()) {
            assert_eq!(child.id(), 2);
            assert_eq!(child.name(), "f2");
        }
        for child in dir.get_child_by_glob_pattern(&pattern) {
            assert_eq!(child.id(), 2);
            assert_eq!(child.name(), "f2");
        }
    }
    writer.join().unwrap();
}

#[test]
fn child_snapshots_outlive_removed_entries() {
    use curvine_config::ChildrenContainer;
    use curvine_master::master::meta::inode::InodeChildren;

    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = Arc::new(DirectoryChildren::with_container(InodeChildren::new(
            container,
        )));
        let dir = InodeDir::with_children(1, 1, children.clone());
        children.add_child(file_edge(2)).unwrap();
        children
            .add_child(InodeView::new_dir("sub".into(), InodeDir::new(3, 1)))
            .unwrap();

        let (point_snapshot, version) = children.read_child_view("sub");
        let snapshot = dir.children_vec();
        let page = dir.list_options(&Default::default());
        let matches = dir.get_child_by_glob_pattern(&glob::Pattern::new("*").unwrap());
        let copied_page = page.clone();
        drop(page);
        children.delete_child(2, "f2").unwrap();
        children.delete_child(3, "sub").unwrap();
        let point_snapshot = point_snapshot.unwrap();
        assert_eq!((point_snapshot.id(), point_snapshot.name()), (3, "sub"));
        assert!(!version.is_current());
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].name(), "f2");
        assert_eq!(matches[1].name(), "sub");
        assert_eq!(copied_page.len(), 2);
        assert_eq!(copied_page[0].name(), "f2");
        assert_eq!(copied_page[1].name(), "sub");
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].name(), "f2");
        assert_eq!(snapshot[0].id(), 2);
        assert_eq!(snapshot[1].name(), "sub");
        assert_eq!(snapshot[1].id(), 3);
        assert_eq!(dir.children_len(), 0);
    }
}
