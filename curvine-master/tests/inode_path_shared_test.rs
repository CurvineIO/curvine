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

#[test]
fn versioned_walk_detects_ancestor_and_target_changes() {
    let (root, root_children) = root_with_children();
    let node = DirNode::from_view(&root);
    let store = test_store("versioned-walk");
    let path = InodePath::resolve_versioned(node.clone(), "/sub", &store).unwrap();
    assert!(path.walk_is_current());
    let sub = root_children.read_child_view("sub").0.unwrap();
    sub.children_handle()
        .unwrap()
        .add_child(InodeView::new_file("new".into(), InodeFile::new(1004, 1)))
        .unwrap();
    assert!(!path.walk_is_current());

    let path = InodePath::resolve_versioned(node, "/sub", &store).unwrap();
    assert!(path.walk_is_current());
    root_children
        .add_child(InodeView::new_file("other".into(), InodeFile::new(1005, 1)))
        .unwrap();
    assert!(!path.walk_is_current());
}

#[test]
fn shared_walk_resolves_depth_two_and_reports_version() {
    Master::init_test_metrics();
    let (root, _) = root_with_children();
    let node = DirNode::from_view(&root);
    let store = test_store("shared-walk-ok");
    let inp = InodePath::resolve_versioned(node, "/sub/leaf", &store).unwrap();
    assert_eq!(inp.components.len(), 3);
    assert!(inp.walk_is_current(), "quiescent tree must validate");
    drop(inp);
    drop(root);
    drop(store);
}

#[test]
fn shared_walk_preserves_resolved_directory_views() {
    let (root, root_children) = root_with_children();
    let sub = root_children.read_child_view("sub").0.unwrap();
    let nested = InodeView::new_dir("nested".into(), InodeDir::new(1006, 1));
    let deep = InodeView::new_dir("deep".into(), InodeDir::new(1007, 1));
    deep.children_handle()
        .unwrap()
        .add_child(InodeView::new_dir("last".into(), InodeDir::new(1008, 1)))
        .unwrap();
    nested.children_handle().unwrap().add_child(deep).unwrap();
    sub.children_handle().unwrap().add_child(nested).unwrap();
    let node = DirNode::from_view(&root);
    let store = test_store("shared-directory-views");

    for (path, ids) in [
        ("/", &[1000][..]),
        ("/missing/leaf", &[1000][..]),
        ("/sub/missing", &[1000, 1001][..]),
        ("/sub/nested/missing", &[1000, 1001, 1006][..]),
        ("/sub/nested", &[1000, 1001, 1006][..]),
        ("/sub/nested/deep/last", &[1000, 1001, 1006, 1007, 1008][..]),
    ] {
        let inp = InodePath::resolve_versioned(node.clone(), path, &store).unwrap();
        assert!(inp.walk_is_current(), "{path}");
        assert_eq!(inp.inodes.len(), ids.len(), "{path}");
        for (inode, id) in inp.inodes.iter().zip(ids) {
            assert_eq!(inode.id(), *id, "{path}");
            assert!(
                matches!(inode, InodeEntry::Dir { .. }),
                "{path}: inode {id} is not a directory"
            );
        }
    }

    let deep_path = InodePath::resolve_versioned(node, "/sub/nested/deep/last", &store).unwrap();
    let last = match deep_path.inodes.last() {
        Some(InodeEntry::Dir { node, .. }) => node.clone(),
        _ => panic!("shared walk must end on a live directory node"),
    };
    last.add_child_edge(InodeView::new_dir("new".into(), InodeDir::new(1009, 1)))
        .unwrap();
    assert!(!deep_path.walk_is_current());
}

#[test]
fn shared_walk_detects_concurrent_mutation() {
    Master::init_test_metrics();
    let (root, handle) = root_with_children();
    let node = DirNode::from_view(&root);
    let store = test_store("shared-walk-invalidate");
    let inp = InodePath::resolve_versioned(node, "/sub/leaf", &store).unwrap();
    assert!(inp.walk_is_current());
    // Any dentry mutation flips the sequence to odd and back to even+1, so
    // the captured tokens must stop being current.
    handle
        .add_child(InodeView::new_file("new".into(), InodeFile::new(1003, 1)))
        .unwrap();
    assert!(!inp.walk_is_current(), "mutation must invalidate the walk");
    drop(inp);
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
