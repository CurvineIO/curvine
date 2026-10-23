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

// The children container is fixed once per process, so the configuration chain
// has to be exercised in its own test binary.

use curvine_config::{ChildrenContainer, ClusterConf};
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::inode::InodeChildren;
use curvine_master::Master;

fn conf(container: ChildrenContainer, tag: &str) -> ClusterConf {
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.children_container = container;
    conf.change_test_meta_dir(tag);
    conf
}

#[test]
fn config_fixes_the_children_container_once_per_process() {
    Master::init_test_metrics();

    let fs =
        JournalSystem::fs_only_for_test(&conf(ChildrenContainer::Map, "children-container-map"))
            .unwrap();
    assert!(
        matches!(InodeChildren::default(), InodeChildren::Map(_)),
        "new directories must use the configured map container"
    );

    fs.mkdir("/tree", true).unwrap();
    fs.mkdir("/tree/sub", true).unwrap();
    let listed = fs.list_status("/tree").unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "sub");

    let err = match JournalSystem::fs_only_for_test(&conf(
        ChildrenContainer::List,
        "children-container-list",
    )) {
        Ok(_) => panic!("a second children container in the same process must be rejected"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("children_container"), "{err}");
}
