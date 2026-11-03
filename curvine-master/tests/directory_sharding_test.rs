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
fn promotion_preserves_container_contents_for_every_container() {
    for container in [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ] {
        let children = seeded(container, 512);
        assert_eq!(children.iter_len(), 512, "{container:?}");
        // A contended mutation at >= threshold promotes to shards: hold the
        // single-index write lock once, then add through a fresh mutation so
        // the promoted shard path is exercised.
        {
            let _guard = children.begin_mutation("f1");
        }
        children.add_child(file(900)).unwrap();
        let mut expected_names: Vec<String> = (1..=512).map(|id| format!("f{id}")).collect();
        expected_names.push("f900".to_string());
        expected_names.sort();
        assert_eq!(names(&children), expected_names, "{container:?}");
        assert_eq!(
            children.get_child_ptr("f512").unwrap().id(),
            512,
            "{container:?}"
        );
        assert!(children.get_child_ptr("missing").is_none(), "{container:?}");
    }
}

#[test]
fn sharded_writes_remain_consistent_under_concurrency() {
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
            assert!(children.get_child_ptr(&format!("t{thread}-{i}")).is_some());
        }
    }
    // Delete half of one thread's entries and confirm the count follows.
    for i in 0..50 {
        children.delete_child(1000 + i, &format!("t0-{i}")).unwrap();
    }
    assert_eq!(children.iter_len(), 512 + 350);
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
    let found = children.get_child_ptr("sub").unwrap();
    assert!(found.is_dir());
    // The dir view is a clone; its own children handle stays valid.
    let view = found.as_ref().clone();
    assert!(matches!(view, InodeView::Dir(_)));
}
