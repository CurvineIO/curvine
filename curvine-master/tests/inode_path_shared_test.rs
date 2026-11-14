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

use curvine_master::master::meta::inode::InodeEntry;
use curvine_master::master::meta::inode::{
    DirNode, DirectoryChildren, InodeDir, InodeFile, InodePath, InodeView,
};
use curvine_master::master::meta::store::InodeStore;
use std::sync::Arc;

fn root_with_children() -> (InodeView, Arc<DirectoryChildren>) {
    let root = InodeView::new_dir("/".into(), InodeDir::new(1000, 1));
    let handle = root.children_handle().unwrap();
    handle
        .add_child(InodeView::new_dir("sub".into(), InodeDir::new(1001, 1)))
        .unwrap();
    handle
        .add_child(InodeView::new_file("leaf".into(), InodeFile::new(1002, 1)))
        .unwrap();
    (root, handle)
}

#[test]
fn resolved_path_name_handles_root_and_unicode() {
    let (root, _) = root_with_children();
    let root = DirNode::from_view(&root);
    let store = test_store("name-boundaries");
    for (path, expected) in [("/", "/"), ("/sub", "sub"), ("/sub/目录", "目录")] {
        let resolved = InodePath::resolve(root.clone(), path, &store).unwrap();
        assert_eq!(resolved.path(), path);
        assert_eq!(resolved.name(), expected);
    }
    assert!(InodePath::resolve(root.clone(), "/sub/", &store).is_err());
}

#[test]
fn resolved_root_is_the_live_node() {
    let (root, _handle) = root_with_children();
    let node = DirNode::from_view(&root);
    let store = test_store("live-root");
    let path = InodePath::resolve(node.clone(), "/sub", &store).unwrap();
    let InodeEntry::Dir { node: resolved, .. } = &path.inodes[0] else {
        panic!("root must resolve to a live directory node");
    };
    assert!(Arc::ptr_eq(resolved, &node));
    assert_eq!(path.inodes[1].id(), 1001);
}

use curvine_master::Master;
use curvine_rocksdb::DBConf;
use curvine_runtime::common::Utils;
fn test_store(name: &str) -> InodeStore {
    Master::init_test_metrics();
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-path-shared-{}-{}",
        name,
        Utils::rand_str(6)
    )));
    let rocks = curvine_master::master::meta::store::RocksInodeStore::new(conf, true).unwrap();
    InodeStore::new(
        rocks,
        Arc::new(curvine_master::master::meta::inode::ttl::TtlBucketList::new(60_000).unwrap()),
    )
    .unwrap()
}
