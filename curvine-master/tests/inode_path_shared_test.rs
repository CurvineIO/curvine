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

use curvine_master::master::meta::inode::{
    DirectoryChildren, InodeDir, InodeFile, InodePath, InodePtr, InodeView,
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
fn resolve_retains_owned_root_until_path_is_dropped() {
    let (root, handle) = root_with_children();
    let weak = Arc::downgrade(&handle);
    drop(handle);
    let store = test_store("owned-root");
    let path = InodePath::resolve(InodePtr::from_owned(root), "/", &store).unwrap();
    assert!(weak.upgrade().is_some(), "resolved root was dropped early");
    drop(path);
    assert!(weak.upgrade().is_none());

    let (root, handle) = root_with_children();
    let weak = Arc::downgrade(&handle);
    drop(handle);
    let path = InodePath::resolve(InodePtr::from_owned(root), "/sub", &store).unwrap();
    assert!(weak.upgrade().is_some());
    assert_eq!(path.inodes[1].id(), 1001);
    drop(path);
    assert!(weak.upgrade().is_none());
}

#[test]
fn shared_walk_resolves_depth_two_and_reports_version() {
    Master::init_test_metrics();
    let (root, _) = root_with_children();
    let store = test_store("shared-walk-ok");
    let inp = InodePath::resolve_shared(&root, "/sub/leaf", &store).unwrap();
    assert_eq!(inp.components.len(), 3);
    assert!(inp.walk_is_current(), "quiescent tree must validate");
    drop(inp);
    drop(root);
    drop(store);
}

#[test]
fn shared_walk_detects_concurrent_mutation() {
    Master::init_test_metrics();
    let (root, handle) = root_with_children();
    let store = test_store("shared-walk-invalidate");
    let inp = InodePath::resolve_shared(&root, "/sub/leaf", &store).unwrap();
    assert!(inp.walk_is_current());
    // Any dentry mutation flips the sequence to odd and back to even+1, so
    // the captured tokens must stop being current.
    handle
        .add_child(InodeView::new_file("new".into(), InodeFile::new(1003, 1)))
        .unwrap();
    assert!(!inp.walk_is_current(), "mutation must invalidate the walk");
    drop(inp);
    drop(root);
    drop(store);
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
