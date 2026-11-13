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

use super::{min_sharded_children, ChildEdge, DirectoryChildren, InodeChildren, ShardedChildren};
use crate::master::meta::inode::{ChildAddResult, InodeDir, InodeView};
use curvine_config::ChildrenContainer;
use curvine_model::ListOptions;
use std::collections::BTreeMap;
use std::sync::Arc;

const CONTAINERS: [ChildrenContainer; 3] = [
    ChildrenContainer::List,
    ChildrenContainer::Map,
    ChildrenContainer::Packed,
];

fn new_children(container: ChildrenContainer) -> Arc<DirectoryChildren> {
    Arc::new(DirectoryChildren::with_container(InodeChildren::new(
        container,
    )))
}

fn add_file(children: &DirectoryChildren, name: &str, id: i64) -> ChildEdge {
    children
        .add_child(InodeView::new_entry(name.to_owned(), id))
        .unwrap();
    ChildEdge::FileId(id)
}

fn add_dir(children: &DirectoryChildren, name: &str, id: i64) -> ChildEdge {
    match children
        .add_child(InodeView::new_dir(name.to_owned(), InodeDir::new(id, 0)))
        .unwrap()
    {
        ChildAddResult::Dir { node, .. } => ChildEdge::Dir(node),
        ChildAddResult::File(_) => panic!("directory insertion returned a file"),
    }
}

fn assert_edges(actual: &[ChildEdge], expected: &[ChildEdge]) {
    let signatures = |edges: &[ChildEdge]| {
        let mut out: Vec<_> = edges
            .iter()
            .map(|edge| (edge.id(), edge.is_dir()))
            .collect();
        out.sort_unstable();
        out
    };
    // Compare a multiset: enumeration has no ordering or deduplication contract.
    assert_eq!(signatures(actual), signatures(expected));
    for edge in expected {
        if let ChildEdge::Dir(expected_node) = edge {
            let Some(ChildEdge::Dir(actual_node)) =
                actual.iter().find(|edge| edge.id() == expected_node.id)
            else {
                panic!("missing live directory handle {}", expected_node.id);
            };
            assert!(Arc::ptr_eq(actual_node, expected_node));
        }
    }
}

fn assert_bounded(
    children: &Arc<DirectoryChildren>,
    max_edges: usize,
    expected: Option<&[ChildEdge]>,
) {
    let before = children.child_edges(&ListOptions::default());
    let version = children.shared_version();
    let sequence = children.sequence.version();
    let was_sharded = children.sharded().is_some();

    let actual = children.child_edges_bounded(max_edges);

    assert_eq!(children.sequence.version(), sequence);
    assert!(
        version.is_current(),
        "enumeration invalidated the read token"
    );
    assert_eq!(children.sharded().is_some(), was_sharded);
    let after = children.child_edges(&ListOptions::default());
    let membership = |edges: &[(String, ChildEdge)]| {
        edges
            .iter()
            .map(|(name, edge)| (name.clone(), edge.id(), edge.is_dir()))
            .collect::<Vec<_>>()
    };
    assert_eq!(membership(&after), membership(&before));
    for ((_, before_edge), (_, after_edge)) in before.iter().zip(&after) {
        assert_edges(
            std::slice::from_ref(after_edge),
            std::slice::from_ref(before_edge),
        );
    }

    match (actual, expected) {
        (Some(actual), Some(expected)) => assert_edges(&actual, expected),
        (None, None) => (),
        (Some(actual), None) => panic!(
            "budget {max_edges} returned truncated success with {} edges",
            actual.len()
        ),
        (None, Some(expected)) => panic!(
            "budget {max_edges} rejected a complete set of {} edges",
            expected.len()
        ),
    }
}

// Use the production router, then verify the migrated container's actual
// occupancy. The bounded search does not assume a hash or a CPU-dependent count.
fn two_shard_pairs() -> [(usize, Vec<String>); 2] {
    let routing = ShardedChildren::new();
    let mut names = BTreeMap::<usize, Vec<String>>::new();
    for serial in 0..4096 {
        let name = format!("edge-{serial}");
        let bucket = names.entry(routing.shard_index(&name)).or_default();
        if bucket.len() < 2 {
            bucket.push(name);
        }
    }
    let mut pairs = names.into_iter().filter(|(_, names)| names.len() == 2);
    let first = pairs.next().expect("no shard has two candidate names");
    let last = pairs
        .next_back()
        .expect("names must cover at least two actual shards");
    assert_ne!(first.0, last.0);
    [first, last]
}

fn assert_shard_counts(children: &DirectoryChildren, expected: &[(usize, usize)]) {
    let shards = children.sharded().expect("directory was not promoted");
    let actual: Vec<_> = shards
        .shards
        .iter()
        .enumerate()
        .filter_map(|(index, shard)| {
            let count = shard.get()?.children.read().len();
            (count > 0).then_some((index, count))
        })
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn bounded_empty_directory_accepts_zero_budget() {
    for container in CONTAINERS {
        let children = new_children(container);
        for promoted in [false, true] {
            if promoted {
                children.seed_shards_for_bulk_load(min_sharded_children());
            }
            assert_eq!(children.sharded().is_some(), promoted);
            for max_edges in [0, 1, usize::MAX] {
                assert_bounded(&children, max_edges, Some(&[]));
            }
        }
    }
}

#[test]
fn bounded_single_file_or_directory_requires_one_edge() {
    for container in CONTAINERS {
        for is_dir in [false, true] {
            let children = new_children(container);
            let expected = [if is_dir {
                add_dir(&children, "only", 10)
            } else {
                add_file(&children, "only", 10)
            }];
            for promoted in [false, true] {
                if promoted {
                    children.seed_shards_for_bulk_load(min_sharded_children());
                }
                assert_eq!(children.sharded().is_some(), promoted);
                assert_bounded(&children, 0, None);
                for max_edges in [1, 2, usize::MAX] {
                    assert_bounded(&children, max_edges, Some(&expected));
                }
            }
        }
    }
}

#[test]
fn bounded_mixed_edges_require_the_complete_budget() {
    for container in CONTAINERS {
        let children = new_children(container);
        let expected = [
            add_file(&children, "z-file", 11),
            add_dir(&children, "m-dir", 22),
            add_file(&children, "a-file", 33),
        ];
        for promoted in [false, true] {
            if promoted {
                children.seed_shards_for_bulk_load(min_sharded_children());
            }
            assert_eq!(children.sharded().is_some(), promoted);
            for max_edges in [0, 1, expected.len() - 1] {
                assert_bounded(&children, max_edges, None);
            }
            for max_edges in [expected.len(), expected.len() + 1, usize::MAX] {
                assert_bounded(&children, max_edges, Some(&expected));
            }
        }
    }
}

#[test]
fn bounded_same_shard_file_aliases_consume_separate_edges() {
    let [(index, names), _] = two_shard_pairs();
    for container in CONTAINERS {
        let children = new_children(container);
        let expected = [
            add_file(&children, &names[0], 77),
            add_file(&children, &names[1], 77),
        ];
        for promoted in [false, true] {
            if promoted {
                children.seed_shards_for_bulk_load(min_sharded_children());
                assert_shard_counts(&children, &[(index, 2)]);
            }
            assert_eq!(children.sharded().is_some(), promoted);
            assert_bounded(&children, 0, None);
            assert_bounded(&children, 1, None);
            for max_edges in [2, 3, usize::MAX] {
                assert_bounded(&children, max_edges, Some(&expected));
            }
        }
    }
}

#[test]
fn bounded_promoted_shards_share_one_budget_and_keep_file_aliases() {
    let [(first_index, first_names), (last_index, last_names)] = two_shard_pairs();
    for container in CONTAINERS {
        let children = new_children(container);
        let expected = [
            add_file(&children, &first_names[0], 77),
            add_dir(&children, &first_names[1], 100),
            add_file(&children, &last_names[0], 77),
            add_dir(&children, &last_names[1], 200),
        ];
        children.seed_shards_for_bulk_load(min_sharded_children());
        assert_shard_counts(&children, &[(first_index, 2), (last_index, 2)]);
        // Each occupied shard fits a budget of two; their combined four edges
        // must still overflow. The two FileId(77) edges must not be deduplicated.
        for max_edges in [0, 1, 2, expected.len() - 1] {
            assert_bounded(&children, max_edges, None);
        }
        for max_edges in [expected.len(), expected.len() + 1, usize::MAX] {
            assert_bounded(&children, max_edges, Some(&expected));
        }
    }
}

#[test]
fn bounded_enumeration_skips_uninitialized_shards_without_creating_them() {
    for container in CONTAINERS {
        let children = new_children(container);
        children.seed_shards_for_bulk_load(min_sharded_children());
        let version = children.shared_version();
        let sequence = children.sequence.version();
        // No listing ran first, so every shard slot is still uninitialized.
        for max_edges in [0, 1, usize::MAX] {
            assert_eq!(
                children
                    .child_edges_bounded(max_edges)
                    .map(|_| Vec::<ChildEdge>::new()),
                Some(Vec::new())
            );
        }
        let shards = children.sharded().expect("directory was not promoted");
        assert!(
            shards.shards.iter().all(|shard| shard.get().is_none()),
            "enumeration initialized shard containers"
        );
        assert!(version.is_current());
        assert_eq!(children.sequence.version(), sequence);
    }
}
