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

use curvine_config::{ChildrenContainer, ClusterConf};
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_model::{ListOptions, RenameFlags};
use curvine_runtime::common::Utils;

#[test]
fn removing_original_and_one_alias_keeps_other_alias() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.children_container = ChildrenContainer::Map;
    conf.master.meta_dir = Utils::test_sub_dir(format!("hardlink-life-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("hardlink-life-journal-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();

    let original = fs.create("/original", false).unwrap();
    fs.link("/original", "/alias1").unwrap();
    fs.link("/alias1", "/alias2").unwrap();
    fs.delete("/original", false).unwrap();
    fs.delete("/alias1", false).unwrap();
    assert_eq!(fs.file_status("/alias2").unwrap().id, original.id);
    fs.delete("/alias2", false).unwrap();
    assert!(!fs.exists("/alias2").unwrap());
}

#[test]
fn hardlink_alias_reads_use_the_edge_name() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.children_container = ChildrenContainer::Map;
    conf.master.meta_dir = Utils::test_sub_dir(format!("hardlink-alias-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("hardlink-alias-journal-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();

    fs.mkdir("/dir", true).unwrap();
    fs.mkdir("/other", true).unwrap();
    let file_id = fs.create("/dir/file", false).unwrap().id;
    fs.link("/dir/file", "/dir/alias").unwrap();
    fs.link("/dir/file", "/other/alias2").unwrap();

    // The stat under an alias must carry the requested edge name, not the
    // name stored in the shared inode row.
    let alias = fs.file_status("/dir/alias").unwrap();
    assert_eq!(alias.name, "alias");
    assert_eq!(alias.id, file_id);

    // Directory listings already report edge names for file children.
    let listing = fs.list_status("/dir").unwrap();
    let names: Vec<&str> = listing.iter().map(|entry| entry.name.as_str()).collect();
    assert!(names.contains(&"alias"));
    assert!(names.contains(&"file"));

    // Single-file pagination compares the edge name against the cursor.
    let page = fs
        .list_options(
            "/dir/alias",
            ListOptions {
                start_after: Some("alias".to_string()),
                limit: Some(5),
            },
        )
        .unwrap();
    assert!(page.is_empty(), "cursor at the edge name must end the page");
    let page = fs
        .list_options(
            "/dir/alias",
            ListOptions {
                start_after: Some("aaa".to_string()),
                limit: Some(5),
            },
        )
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].name, "alias");

    // Renaming an alias keeps the requested destination edge name.
    fs.rename("/other/alias2", "/dir/alias3", RenameFlags::empty())
        .unwrap();
    let alias3 = fs.file_status("/dir/alias3").unwrap();
    assert_eq!(alias3.name, "alias3");
    assert_eq!(alias3.id, file_id);
}
