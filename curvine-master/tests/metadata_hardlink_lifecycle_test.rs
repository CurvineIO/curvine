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
