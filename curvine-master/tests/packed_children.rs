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

use curvine_master::master::meta::inode::{ChildItem, InodeDir, InodeView, PackedChildren};
use curvine_model::ListOptions;
use glob::Pattern;
use std::collections::BTreeMap;

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 11) % n as u64) as usize
    }
}

fn file_edge(name: &str, id: i64) -> InodeView {
    InodeView::new_entry(name.to_string(), id)
}

fn dir_edge(name: &str, id: i64) -> InodeView {
    InodeView::new_dir(name.to_string(), InodeDir::new(id, 0))
}

fn make_edge(name: &str, id: i64, is_dir: bool) -> InodeView {
    if is_dir {
        dir_edge(name, id)
    } else {
        file_edge(name, id)
    }
}

type Oracle = BTreeMap<String, (i64, bool)>;

fn names<'a>(items: &'a [ChildItem<'a>]) -> Vec<&'a str> {
    items.iter().map(|item| item.name()).collect()
}

fn assert_matches_oracle(children: &PackedChildren, oracle: &Oracle) {
    assert_eq!(
        children.len(),
        oracle.len(),
        "len disagrees with the oracle"
    );

    let got: Vec<(String, i64)> = children
        .iter()
        .map(|item| (item.name().to_string(), item.id()))
        .collect();
    let want: Vec<(String, i64)> = oracle
        .iter()
        .map(|(name, (id, _))| (name.clone(), *id))
        .collect();
    assert_eq!(got, want, "iter order or ids disagree with the oracle");

    for (name, (id, is_dir)) in oracle {
        let child = children
            .get_child_item(name)
            .unwrap_or_else(|| panic!("child {} is missing", name));
        assert_eq!(child.id(), *id, "wrong id for {}", name);
        assert_eq!(child.is_dir(), *is_dir, "wrong kind for {}", name);
    }
}

#[test]
fn add_get_delete_round_trip_for_both_kinds() {
    let mut children = PackedChildren::new();
    assert!(children.is_empty());
    assert_eq!(children.len(), 0);
    assert!(children.list_options(&ListOptions::default()).is_empty());

    children.add_child(file_edge("a.txt", 11)).unwrap();
    children.add_child(dir_edge("sub", 22)).unwrap();
    assert_eq!(children.len(), 2);
    assert!(!children.is_empty());

    let file = children
        .get_child_item("a.txt")
        .expect("file edge must resolve");
    assert_eq!(file.id(), 11);
    assert_eq!(file.name(), "a.txt");
    assert!(!file.is_dir());
    assert!(file.is_file_entry());

    let dir = children
        .get_child_item("sub")
        .expect("dir edge must resolve");
    assert_eq!(dir.id(), 22);
    assert!(dir.is_dir());

    assert!(children.get_child_item("missing").is_none());

    let removed_file = children.delete_child(11, "a.txt").unwrap();
    assert_eq!(removed_file.id(), 11);
    assert!(removed_file.is_file_entry());
    assert!(children.get_child_item("a.txt").is_none());

    let removed_dir = children.delete_child(22, "sub").unwrap();
    assert_eq!(removed_dir.id(), 22);
    assert!(removed_dir.is_dir());
    assert!(children.is_empty());

    assert!(children.list_options(&ListOptions::default()).is_empty());
    assert_eq!(children.iter().count(), 0);
    assert_eq!(children.iter().size_hint(), (0, Some(0)));
    assert!(children
        .get_child_by_glob_pattern(&Pattern::new("*").unwrap())
        .is_empty());
}

#[test]
fn multibyte_names_round_trip() {
    let mut children = PackedChildren::new();
    let name = "目录-数据.txt";
    children.add_child(file_edge(name, 5)).unwrap();

    assert_eq!(children.get_child_item(name).unwrap().id(), 5);
    let items: Vec<ChildItem<'_>> = children.iter().collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].name(), name);
    assert_eq!(items[0].id(), 5);
}

#[test]
fn delete_rejects_a_mismatched_id_or_a_missing_name() {
    let mut children = PackedChildren::new();
    children.add_child(file_edge("f", 1)).unwrap();
    children.add_child(dir_edge("d", 2)).unwrap();

    assert!(children.delete_child(2, "f").is_err());
    assert!(children.delete_child(1, "d").is_err());
    assert!(children.delete_child(1, "missing").is_err());

    let oracle: Oracle = [("d".to_string(), (2, true)), ("f".to_string(), (1, false))]
        .into_iter()
        .collect();
    assert_matches_oracle(&children, &oracle);
}

#[test]
fn duplicate_add_keeps_the_existing_child_and_its_slot() {
    let mut children = PackedChildren::new();
    children.add_child(file_edge("dup", 1)).unwrap();
    assert!(children.add_child(file_edge("dup", 2)).is_err());
    assert_eq!(children.get_child_item("dup").unwrap().id(), 1);
    assert_eq!(children.len(), 1);

    children.add_child(dir_edge("d", 10)).unwrap();
    assert!(children.add_child(dir_edge("d", 20)).is_err());
    let kept = children.get_child_item("d").unwrap();
    assert_eq!(kept.id(), 10);
    assert!(kept.is_dir());

    children.add_child(dir_edge("d2", 30)).unwrap();
    let d2 = children.get_child_item("d2").unwrap();
    assert_eq!(d2.id(), 30);
    assert!(d2.is_dir());
    assert_eq!(children.len(), 3);
}

#[test]
fn name_length_is_bounded_by_the_record_layout() {
    let mut children = PackedChildren::new();

    let longest = "n".repeat(255);
    children.add_child(file_edge(&longest, 1)).unwrap();
    assert_eq!(children.get_child_item(&longest).unwrap().id(), 1);
    children
        .add_child(dir_edge(&format!("{}d", "n".repeat(254)), 2))
        .unwrap();

    let too_long = "n".repeat(256);
    assert!(children.add_child(file_edge(&too_long, 3)).is_err());
    assert!(children.add_child(dir_edge(&too_long, 4)).is_err());
    assert!(children.get_child_item(&too_long).is_none());
    assert_eq!(children.len(), 2);
}

#[test]
fn child_item_exposes_name_and_id_for_both_kinds() {
    let mut children = PackedChildren::new();
    children.add_child(file_edge("f", 1)).unwrap();
    children.add_child(dir_edge("d", 2)).unwrap();

    let items: Vec<ChildItem<'_>> = children.iter().collect();
    assert_eq!(items.len(), 2);
    assert!(matches!(items[0], ChildItem::DirNode { .. }));
    assert_eq!(items[0].name(), "d");
    assert_eq!(items[0].id(), 2);
    assert!(matches!(items[1], ChildItem::FileEntry { .. }));
    assert_eq!(items[1].name(), "f");
    assert_eq!(items[1].id(), 1);
}

#[test]
fn item_name_matches_the_directory_object() {
    let mut children = PackedChildren::new();
    children.add_child(dir_edge("d", 7)).unwrap();

    // Packed dir edges are immutable name+node records: a rename replaces
    // the edge instead of mutating an in-tree view.
    children.delete_child(7, "d").unwrap();
    let mut renamed = dir_edge("d", 7);
    renamed.change_name("renamed".to_string());
    children.add_child(renamed).unwrap();

    let items: Vec<ChildItem<'_>> = children.iter().collect();
    assert_eq!(items[0].name(), "renamed");
    assert_eq!(items[0].id(), 7);

    let listed = children.list_options(&ListOptions::default());
    assert_eq!(listed[0].name(), "renamed");
    assert_eq!(listed[0].id(), 7);

    assert_eq!(
        children.get_child_item("renamed").unwrap().name(),
        "renamed"
    );
}

#[test]
fn pointer_ownership_matches_the_contract() {
    let mut children = PackedChildren::new();
    children.add_child(file_edge("solo", 42)).unwrap();
    children.add_child(dir_edge("dir", 43)).unwrap();

    let owned = children.get_child_item("solo").unwrap();
    assert_eq!(owned.id(), 42);
    assert!(owned.is_file_entry());

    // Owned projections are safe to clone and keep after the borrow guard.
    let cloned = dir_edge("dir", 43);
    assert_eq!(cloned.id(), 43);
    assert!(cloned.is_dir());

    let moved = children.list_options(&ListOptions::default());
    assert_eq!(names(&moved), vec!["dir", "solo"]);
}

#[test]
fn random_add_and_delete_track_the_oracle_across_chunks() {
    let mut rng = Rng(0x5eed);
    let mut children = PackedChildren::new();
    let mut oracle: Oracle = BTreeMap::new();

    for round in 0..4000usize {
        let name = format!("child-{:05}", rng.below(1200));
        let is_dir = rng.below(4) == 0;
        let id = round as i64 + 1000;

        if let Some((old_id, _)) = oracle.get(&name) {
            let old_id = *old_id;
            assert!(children.add_child(make_edge(&name, id, is_dir)).is_err());
            assert_eq!(children.get_child_item(&name).unwrap().id(), old_id);
        } else if !oracle.is_empty() && rng.below(10) < 3 {
            let victim = oracle.keys().nth(rng.below(oracle.len())).unwrap().clone();
            let victim_id = oracle[&victim].0;
            let removed = children.delete_child(victim_id, &victim).unwrap();
            assert_eq!(removed.id(), victim_id);
            assert_eq!(removed.name(), victim);
            oracle.remove(&victim);
        } else {
            children.add_child(make_edge(&name, id, is_dir)).unwrap();
            oracle.insert(name, (id, is_dir));
        }
    }

    assert!(children.len() > 300, "test must span several chunks");
    assert_matches_oracle(&children, &oracle);
}

#[test]
fn list_options_honours_start_after_and_limit() {
    let mut children = PackedChildren::new();
    for i in 0..600 {
        children
            .add_child(file_edge(&format!("f-{i:04}"), i as i64))
            .unwrap();
    }
    children.add_child(dir_edge("f-0300d", 9999)).unwrap();

    let all = children.list_options(&ListOptions::default());
    assert_eq!(all.len(), 601);
    assert_eq!(names(&all)[0], "f-0000");
    assert_eq!(names(&all)[301], "f-0300d");
    assert!(all[301].is_dir());

    let limited = children.list_options(&ListOptions {
        limit: Some(5),
        start_after: None,
    });
    assert_eq!(
        names(&limited),
        vec!["f-0000", "f-0001", "f-0002", "f-0003", "f-0004"]
    );

    let after = children.list_options(&ListOptions {
        limit: Some(3),
        start_after: Some("f-0010".to_string()),
    });
    assert_eq!(names(&after), vec!["f-0011", "f-0012", "f-0013"]);

    let gap = children.list_options(&ListOptions {
        limit: Some(2),
        start_after: Some("f-00105".to_string()),
    });
    assert_eq!(names(&gap), vec!["f-0011", "f-0012"]);

    assert!(children
        .list_options(&ListOptions {
            limit: None,
            start_after: Some("zzz".to_string())
        })
        .is_empty());
    assert_eq!(
        children
            .list_options(&ListOptions {
                limit: None,
                start_after: Some("aaa".to_string())
            })
            .len(),
        601
    );
    assert!(children
        .list_options(&ListOptions {
            limit: Some(0),
            start_after: None
        })
        .is_empty());

    let mut paged: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = children.list_options(&ListOptions {
            limit: Some(100),
            start_after: cursor.clone(),
        });
        if page.is_empty() {
            break;
        }
        cursor = Some(page.last().unwrap().name().to_string());
        paged.extend(names(&page).into_iter().map(str::to_string));
    }
    let full: Vec<String> = names(&all).into_iter().map(str::to_string).collect();
    assert_eq!(paged, full);
    assert_eq!(paged.len(), 601);
}

#[test]
fn glob_pattern_selects_matching_children() {
    let mut children = PackedChildren::new();
    children.add_child(file_edge("a.txt", 1)).unwrap();
    children.add_child(file_edge("b.txt", 2)).unwrap();
    children.add_child(file_edge("c.log", 3)).unwrap();
    children.add_child(dir_edge("d.txt", 4)).unwrap();

    let txt_hits = children.get_child_by_glob_pattern(&Pattern::new("*.txt").unwrap());
    let mut hits = names(&txt_hits);
    hits.sort();
    assert_eq!(hits, vec!["a.txt", "b.txt", "d.txt"]);

    assert!(children
        .get_child_by_glob_pattern(&Pattern::new("*.zip").unwrap())
        .is_empty());
    assert_eq!(
        children
            .get_child_by_glob_pattern(&Pattern::new("*").unwrap())
            .len(),
        4
    );
}

#[test]
fn iter_size_hint_bounds_the_remaining_records() {
    let mut children = PackedChildren::new();
    for i in 0..10 {
        children
            .add_child(file_edge(&format!("f{i}"), i as i64))
            .unwrap();
    }

    let mut iter = children.iter();
    assert_eq!(iter.size_hint(), (0, Some(10)));
    assert!(iter.next().is_some());
    assert_eq!(iter.size_hint(), (0, Some(9)));
    assert_eq!(iter.count(), 9);
    assert_eq!(children.len(), 10);
    assert_eq!(children.iter().count(), 10);
}

#[test]
fn dir_pointer_survives_growth_and_slot_reuse() {
    let mut children = PackedChildren::new();
    children.add_child(dir_edge("keep", 1)).unwrap();

    // Hold the shared Arc<DirNode> handle: it must survive growth and slot reuse.
    let _kept = children
        .get_child_item("keep")
        .expect("keep must resolve")
        .dir_node()
        .cloned()
        .expect("keep must be a directory edge");

    for i in 0..3000 {
        children
            .add_child(file_edge(&format!("pad-{i:05}"), i as i64 + 100))
            .unwrap();
    }
    for i in 0..300 {
        let name = format!("tmp-{i:03}");
        let id = i as i64 + 5000;
        children.add_child(dir_edge(&name, id)).unwrap();
        children.delete_child(id, &name).unwrap();
    }

    assert_eq!(children.get_child_item("keep").unwrap().id(), 1);
    assert_eq!(children.len(), 3001);
}

#[test]
fn compaction_remaps_directory_slots_without_losing_children() {
    let mut children = PackedChildren::new();
    for i in 0..2000 {
        children
            .add_child(dir_edge(&format!("d-{i:05}"), i as i64 + 1))
            .unwrap();
    }
    for i in 0..500 {
        children
            .add_child(file_edge(&format!("f-{i:05}"), i as i64 + 100000))
            .unwrap();
    }
    assert_eq!(children.len(), 2500);

    for i in 0..1500 {
        let removed = children
            .delete_child(i as i64 + 1, &format!("d-{i:05}"))
            .unwrap();
        assert!(removed.is_dir());
    }
    assert_eq!(children.len(), 1000);

    for i in 1500..2000 {
        let dir = children
            .get_child_item(&format!("d-{i:05}"))
            .unwrap_or_else(|| panic!("d-{i:05} was lost by compaction"));
        assert!(dir.is_dir(), "d-{i:05} came back as a file edge");
        assert_eq!(
            dir.id(),
            i as i64 + 1,
            "d-{i:05} was remapped onto another dir"
        );
    }
    for i in 0..500 {
        let file = children.get_child_item(&format!("f-{i:05}")).unwrap();
        assert!(!file.is_dir());
        assert_eq!(file.id(), i as i64 + 100000);
    }
    for i in 0..1500 {
        assert!(children.get_child_item(&format!("d-{i:05}")).is_none());
    }

    children.add_child(dir_edge("d-new", 777)).unwrap();
    let new_dir = children.get_child_item("d-new").unwrap();
    assert!(new_dir.is_dir());
    assert_eq!(new_dir.id(), 777);
    assert_eq!(children.len(), 1001);

    let listed = children.list_options(&ListOptions::default());
    assert_eq!(listed.len(), 1001);
    assert_eq!(names(&listed)[0], "d-01500");
    assert_eq!(names(&listed)[500], "d-new");
    assert_eq!(names(&listed)[501], "f-00000");
}

#[test]
fn dir_pointer_survives_compaction() {
    let mut children = PackedChildren::new();
    children.add_child(dir_edge("anchor", 1)).unwrap();

    // The Arc<DirNode> handle taken before any compaction runs must keep
    // pointing at the same node object afterwards.
    let anchor = children
        .iter()
        .find(|item| item.name() == "anchor")
        .expect("anchor must resolve");
    assert_eq!(anchor.id(), 1);
    let anchor_node = anchor.dir_node().cloned().expect("dir node handle");

    // Fill and then drain 2000 dir slots so the hole count crosses HOLE_LIMIT
    // and forces compact_dirs to remap every surviving slot index.
    for i in 0..2000 {
        children
            .add_child(dir_edge(&format!("tmp-{i:05}"), i as i64 + 2))
            .unwrap();
    }
    for i in 0..2000 {
        children
            .delete_child(i as i64 + 2, &format!("tmp-{i:05}"))
            .unwrap();
    }
    assert_eq!(children.len(), 1);

    // The Arc handle stays identical: compaction remaps slot indexes, never
    // replaces the shared node.
    let refreshed = children
        .iter()
        .find(|item| item.name() == "anchor")
        .expect("anchor must survive");
    let refreshed_node = refreshed.dir_node().cloned().expect("dir node handle");
    assert!(
        std::sync::Arc::ptr_eq(&anchor_node, &refreshed_node),
        "compaction replaced the shared Arc<DirNode> or remapped its slot wrongly"
    );
}
