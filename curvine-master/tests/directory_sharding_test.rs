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

use curvine_config::ChildrenContainer;
use curvine_master::master::meta::inode::{
    DirectoryChildren, InodeChildren, InodeDir, InodeFile, InodeView,
};
use std::sync::Arc;
use std::time::Duration;

fn file(id: i64) -> InodeView {
    InodeView::new_file(format!("f{id}"), InodeFile::new(id, 1))
}

fn seeded(container: ChildrenContainer, count: i64) -> DirectoryChildren {
    let mut children = InodeChildren::new(container);
    for id in 1..=count {
        children.add_child(file(id)).unwrap();
    }
    DirectoryChildren::with_container(children)
}

fn names(children: &DirectoryChildren) -> Vec<String> {
    children
        .list_options(&Default::default())
        .iter()
        .map(|ptr| ptr.name().to_string())
        .collect()
}

#[test]
fn packed_name_limit_survives_promotion() {
    let children = Arc::new(seeded(ChildrenContainer::Packed, 2048));
    let long_name = "界".repeat(86);
    let add = || InodeView::new_file(long_name.clone(), InodeFile::new(3000, 1));
    assert!(children.add_child(add()).is_err());
    assert!(children
        .begin_rename_mutation("f1", &long_name)
        .rename_child(1, "f1", None, &long_name)
        .is_err());

    let guard = children.begin_mutation("holder");
    let other = children.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        drop(other.begin_mutation("waiter"));
        done_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100));
    drop(guard);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    assert!(matches!(
        blocked,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(format!("{children:?}").contains("sharded: true"));

    assert!(children.add_child(add()).is_err());
    assert!(children
        .begin_mutation(&long_name)
        .add_child(add())
        .is_err());
    assert!(children
        .begin_rename_mutation("f1", &long_name)
        .rename_child(1, "f1", None, &long_name)
        .is_err());
    assert_eq!(children.read_child_view("f1").0.unwrap().id(), 1);
    assert!(children.read_child_view(&long_name).0.is_none());

    let max_name = "x".repeat(255);
    children
        .add_child(InodeView::new_file(
            max_name.clone(),
            InodeFile::new(3001, 1),
        ))
        .unwrap();
    assert!(children.read_child_view(&max_name).0.is_some());

    let map = DirectoryChildren::with_container(InodeChildren::new_map());
    map.add_child(add()).unwrap();
    assert!(map.read_child_view(&long_name).0.is_some());
}

#[test]
fn container_contents_are_preserved_for_every_container() {
    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = seeded(container, 512);
        assert_eq!(children.iter_len(), 512, "{container:?}");
        // This exercises normal mutation for each container; promotion is
        // separately tested with overlapping lock acquisitions.
        {
            let _guard = children.begin_mutation("f1");
        }
        children.add_child(file(900)).unwrap();
        let mut expected_names: Vec<String> = (1..=512).map(|id| format!("f{id}")).collect();
        expected_names.push("f900".to_string());
        expected_names.sort();
        assert_eq!(names(&children), expected_names, "{container:?}");
        assert_eq!(
            children.get_child_edge("f512").unwrap().id(),
            512,
            "{container:?}"
        );
        assert!(
            children.get_child_edge("missing").is_none(),
            "{container:?}"
        );
    }
}

#[test]
fn bulk_restore_seeds_shards_before_children_are_inserted() {
    let children = DirectoryChildren::with_container(InodeChildren::new_map());
    children.seed_shards_for_bulk_load(4096);
    assert!(format!("{children:?}").contains("sharded: true"));

    for id in 1..=128 {
        children.add_child(file(id)).unwrap();
    }

    assert_eq!(children.iter_len(), 128);
    assert_eq!(children.read_child_view("f128").0.unwrap().id(), 128);
}

#[test]
fn bulk_seed_preserves_existing_children() {
    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = seeded(container, 32);
        children.seed_shards_for_bulk_load(4096);
        assert!(format!("{children:?}").contains("sharded: true"));
        assert_eq!(children.iter_len(), 32);
        for id in 1..=32 {
            assert_eq!(
                children.read_child_view(&format!("f{id}")).0.unwrap().id(),
                id
            );
        }
    }
}

#[test]
fn bulk_seed_waits_for_active_mutation() {
    let children = Arc::new(DirectoryChildren::with_container(InodeChildren::new_map()));
    let mut mutation = children.begin_mutation("f1");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let other = children.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        other.seed_shards_for_bulk_load(4096);
        done_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100));
    mutation.add_child(file(1)).unwrap();
    drop(mutation);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    assert!(matches!(
        blocked,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert_eq!(children.read_child_view("f1").0.unwrap().id(), 1);
    assert_eq!(children.iter_len(), 1);
}

#[test]
fn directory_writes_remain_consistent_under_concurrency() {
    let children = Arc::new(seeded(ChildrenContainer::Map, 512));
    let mut handles = Vec::new();
    for thread in 0..4 {
        let children = children.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let id = 1000 + thread * 100 + i;
                children
                    .add_child(InodeView::new_file(
                        format!("t{thread}-{i}"),
                        InodeFile::new(id, 1),
                    ))
                    .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(children.iter_len(), 512 + 400);
    for thread in 0..4 {
        for i in 0..100 {
            assert!(children.get_child_edge(&format!("t{thread}-{i}")).is_some());
        }
    }
    // Delete half of one thread's entries and confirm the count follows.
    for i in 0..50 {
        children.delete_child(1000 + i, &format!("t0-{i}")).unwrap();
    }
    assert_eq!(children.iter_len(), 512 + 350);
}

#[test]
fn paginated_listing_matches_sorted_names_before_and_after_sharding() {
    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = seeded(container, 2048);
        for sharded in [false, true] {
            if sharded {
                children.seed_shards_for_bulk_load(2048);
            }
            let all = names(&children);
            for start in [None, Some("f10"), Some("f100x"), Some("zzzz")] {
                for limit in [0, 1, 2, 17, 100, 1000, 3000] {
                    let options = curvine_model::ListOptions {
                        limit: Some(limit),
                        start_after: start.map(str::to_owned),
                    };
                    let actual: Vec<_> = children
                        .list_options(&options)
                        .iter()
                        .map(|inode| inode.name().to_owned())
                        .collect();
                    let expected: Vec<_> = all
                        .iter()
                        .filter(|name| start.is_none_or(|start| name.as_str() > start))
                        .take(limit)
                        .cloned()
                        .collect();
                    assert_eq!(
                        actual, expected,
                        "{container:?}, sharded={sharded}, start={start:?}, limit={limit}"
                    );
                }
            }
        }
    }
}

#[test]
fn concurrent_shard_writes_never_produce_a_current_version() {
    use fxhash::FxHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::mpsc;

    // The promotion threshold is at most 2048. Force real contention while
    // holding the unsharded write guard, then verify promotion took place.
    let children = Arc::new(seeded(ChildrenContainer::Map, 2048));
    let guard = children.begin_mutation("promotion-holder");
    let other = children.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let promote = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        drop(other.begin_mutation("promotion-waiter"));
        done_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
    drop(guard);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    promote.join().unwrap();
    assert!(format!("{children:?}").contains("sharded: true"));
    let all_names = names(&children);
    for start in [None, Some("f100"), Some("f100x"), Some("zzzz")] {
        for limit in [0, 1, 3, 100] {
            let options = curvine_model::ListOptions {
                limit: Some(limit),
                start_after: start.map(str::to_owned),
            };
            let page: Vec<String> = children
                .list_options(&options)
                .iter()
                .map(|item| item.name().to_owned())
                .collect();
            let expected: Vec<String> = all_names
                .iter()
                .filter(|name| start.map_or(true, |start| name.as_str() > start))
                .take(limit)
                .cloned()
                .collect();
            assert_eq!(page, expected, "start={start:?}, limit={limit}");
        }
    }

    let count = std::thread::available_parallelism()
        .map(|n| n.get().saturating_mul(2))
        .unwrap_or(8)
        .next_power_of_two()
        .clamp(8, 64);
    let shard = |name: &str| {
        let mut hasher = FxHasher::default();
        name.hash(&mut hasher);
        (hasher.finish() as usize) & (count - 1)
    };
    let first = (0..1000)
        .map(|n| format!("first-{n}"))
        .find(|name| shard(name) != shard("f1"))
        .unwrap();
    let second = (0..1000)
        .map(|n| format!("second-{n}"))
        .find(|name| shard(name) != shard(&first))
        .unwrap();
    let first_guard = children.begin_mutation(&first);
    let other = children.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let _guard = other.begin_mutation(&second);
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let version = children.shared_version();
    let incorrectly_current = version.is_current();
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    let one_writer_version = children.shared_version();
    let one_writer_current = one_writer_version.is_current();
    drop(first_guard);
    assert!(
        !one_writer_current,
        "the remaining writer must remain visible"
    );
    assert!(!one_writer_version.is_current());
    assert!(
        !incorrectly_current,
        "two active writers cannot yield a current version"
    );
    assert!(
        !version.is_current(),
        "a version captured during writes stays invalid"
    );
    assert!(children.shared_version().is_current());

    let mut mutation = children.begin_mutation("f1");
    let wrong_child = InodeView::new_file(first.clone(), InodeFile::new(9000, 1));
    assert!(mutation.add_child(wrong_child).is_err());
    drop(mutation);
    assert!(children.read_child_view(&first).0.is_none());
    assert_eq!(children.iter_len(), 2048);

    // The rename guard owns only the source and destination shards; reject
    // a later request that changes the destination to an unlocked shard.
    let wrong_destination = (0..1000)
        .map(|n| format!("unlocked-{n}"))
        .find(|name| shard(name) != shard("f1") && shard(name) != shard(&first))
        .unwrap();
    let mut rename = children.begin_rename_mutation("f1", &first);
    assert!(rename
        .rename_child(1, "f1", None, &wrong_destination)
        .is_err());
    rename.rename_child(1, "f1", None, &first).unwrap();
    drop(rename);
    assert!(children.read_child_view("f1").0.is_none());
    assert_eq!(children.read_child_view(&first).0.unwrap().id(), 1);

    let mut dir = InodeDir::new(9001, 1);
    dir.add_child(file(9002)).unwrap();
    let handle = dir.children_handle();
    children
        .add_child(InodeView::new_dir("sub".into(), dir))
        .unwrap();
    let destination = (0..1000)
        .map(|n| format!("moved-{n}"))
        .find(|name| shard(name) != shard("sub"))
        .unwrap();
    children
        .begin_rename_mutation("sub", &destination)
        .rename_child(9001, "sub", None, &destination)
        .unwrap();
    let moved = children.read_child_view(&destination).0.unwrap();
    let InodeView::Dir(moved) = moved else {
        panic!("directory changed type")
    };
    assert!(Arc::ptr_eq(&handle, &moved.children_handle()));
    assert_eq!(
        moved
            .children_handle()
            .get_child_edge("f9002")
            .unwrap()
            .id(),
        9002
    );
}

#[test]
fn promotion_preserves_directory_node_identity() {
    use std::sync::mpsc;

    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = Arc::new(seeded(container, 2048));
        children
            .add_child(InodeView::new_dir("removed".into(), InodeDir::new(8999, 1)))
            .unwrap();
        children.delete_child(8999, "removed").unwrap();
        children
            .add_child(InodeView::new_dir("sub".into(), InodeDir::new(9000, 1)))
            .unwrap();
        children
            .add_child(InodeView::new_dir("hole".into(), InodeDir::new(9001, 1)))
            .unwrap();
        children.delete_child(9001, "hole").unwrap();
        let original = children.get_child_edge("sub").unwrap();
        let original_id = original.id();
        let original_names = names(&children);
        let guard = children.begin_mutation("holder");
        let other = children.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            drop(other.begin_mutation("waiter"));
            done_tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
        drop(guard);
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.join().unwrap();
        assert!(format!("{children:?}").contains("sharded: true"));
        assert_eq!(names(&children), original_names, "{container:?}");
        let promoted = children.get_child_edge("sub").unwrap();
        assert_eq!(original_id, promoted.id(), "{container:?}");
    }
}

#[test]
fn small_directories_stay_unsharded() {
    let children = seeded(ChildrenContainer::Map, 4);
    {
        let _guard = children.begin_mutation("f1");
    }
    children.add_child(file(9)).unwrap();
    assert_eq!(names(&children), vec!["f1", "f2", "f3", "f4", "f9"]);
    assert_eq!(children.iter_len(), 5);
}

#[test]
fn dir_views_move_between_containers_with_their_own_children() {
    let children = seeded(ChildrenContainer::Packed, 8);
    let dir = InodeView::new_dir("sub".to_string(), InodeDir::new(99, 1));
    children.add_child(dir).unwrap();
    let found = children.read_child_view("sub").0.unwrap();
    assert!(found.is_dir());
    // The projected view is detached; its own children handle stays valid.
    assert!(matches!(found, InodeView::Dir(_)));
}

#[test]
fn concurrent_same_directory_renames_both_complete() {
    use std::sync::Barrier;
    let children = Arc::new(seeded(ChildrenContainer::Map, 512));
    // This test does not force promotion; it checks same-directory behavior.
    {
        let _guard = children.begin_mutation("f1");
    }
    children
        .add_child(InodeView::new_file("x-aaa".into(), InodeFile::new(800, 1)))
        .unwrap();
    children
        .add_child(InodeView::new_file("x-zzz".into(), InodeFile::new(801, 1)))
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    let a_children = children.clone();
    let a_start = start.clone();
    let rename_a = std::thread::spawn(move || {
        a_start.wait();
        let mut mutation = a_children.begin_rename_mutation("x-aaa", "x-renamed-a");
        mutation
            .rename_child(800, "x-aaa", None, "x-renamed-a")
            .unwrap();
    });
    let b_children = children.clone();
    let b_start = start.clone();
    let rename_b = std::thread::spawn(move || {
        b_start.wait();
        let mut mutation = b_children.begin_rename_mutation("x-zzz", "x-renamed-b");
        mutation
            .rename_child(801, "x-zzz", None, "x-renamed-b")
            .unwrap();
    });

    rename_a.join().unwrap();
    rename_b.join().unwrap();
    assert!(children.get_child_edge("x-aaa").is_none());
    assert!(children.get_child_edge("x-zzz").is_none());
    assert_eq!(children.get_child_edge("x-renamed-a").unwrap().id(), 800);
    assert_eq!(children.get_child_edge("x-renamed-b").unwrap().id(), 801);
    assert_eq!(children.iter_len(), 512 + 2);
}

#[test]
fn failed_rename_preserves_source_and_target() {
    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = seeded(container, 4);
        let mut mutation = children.begin_rename_mutation("f1", "f2");
        assert!(mutation
            .rename_child(99, "f1", Some((2, "f2")), "f2")
            .is_err());
        assert!(mutation
            .rename_child(1, "f1", Some((99, "f2")), "f2")
            .is_err());
        assert!(mutation.rename_child(1, "f1", None, "f2").is_err());
        drop(mutation);
        assert_eq!(
            children.read_child_view("f1").0.unwrap().id(),
            1,
            "{container:?}"
        );
        assert_eq!(
            children.read_child_view("f2").0.unwrap().id(),
            2,
            "{container:?}"
        );
    }
}

#[test]
fn rename_rejects_oversized_packed_name_without_removing_source() {
    let children = seeded(ChildrenContainer::Packed, 4);
    let name = "x".repeat(1024);
    let mut mutation = children.begin_rename_mutation("f1", &name);
    assert!(mutation.rename_child(1, "f1", None, &name).is_err());
    drop(mutation);
    assert_eq!(children.read_child_view("f1").0.unwrap().id(), 1);
}

#[test]
fn rename_moves_original_directory_and_children() {
    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = seeded(container, 2);
        let mut dir = InodeDir::new(10, 1);
        dir.add_child(file(11)).unwrap();
        let handle = dir.children_handle();
        children
            .add_child(InodeView::new_dir("sub".into(), dir))
            .unwrap();
        let mut mutation = children.begin_rename_mutation("sub", "moved");
        mutation.rename_child(10, "sub", None, "moved").unwrap();
        drop(mutation);
        assert!(children.read_child_view("sub").0.is_none());
        let moved = children.read_child_view("moved").0.unwrap();
        let InodeView::Dir(moved) = moved else {
            panic!("directory changed type")
        };
        assert!(Arc::ptr_eq(&handle, &moved.children_handle()));
        assert_eq!(
            moved.children_handle().get_child_edge("f11").unwrap().id(),
            11
        );
    }
}

#[test]
fn single_index_rename_replaces_target() {
    let children = Arc::new(seeded(ChildrenContainer::Map, 4));
    {
        let _guard = children.begin_mutation("f1");
    }
    let mut mutation = children.begin_rename_mutation("f1", "f2");
    mutation
        .rename_child(1, "f1", Some((2, "f2")), "f2")
        .unwrap();
    drop(mutation);
    assert!(children.get_child_edge("f1").is_none());
    assert_eq!(children.get_child_edge("f2").unwrap().id(), 1);
    // f1..f4 seeded (4) minus f1, minus replaced f2, plus f2 = 3 entries.
    assert_eq!(children.iter_len(), 3);
}
