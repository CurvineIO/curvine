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

use curvine_config::DBConf;
use curvine_master::master::meta::inode::ttl::TtlBucketList;
use curvine_master::master::meta::inode::{InodeDir, InodeFile, InodeView};
use curvine_master::master::meta::store::{InodeStore, RocksInodeStore};
use curvine_master::master::Master;
use curvine_model::{CreateFileOpts, MkdirOpts, StoragePolicy, TtlAction};
use curvine_runtime::common::Utils;
use std::sync::Arc;

#[cfg(feature = "fault-injection")]
#[test]
fn replace_rename_updates_ttl_only_after_commit() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("replace-ttl-{}", Utils::rand_str(8)));
    let rocks = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let buckets = Arc::new(TtlBucketList::new(60_000).unwrap());
    let store = InodeStore::new(rocks, buckets.clone()).unwrap();
    let root = InodeView::new_dir("/".into(), InodeDir::new(1, 0));
    let name = format!("source-{}", Utils::rand_str(8));
    let source = InodeView::new_dir(name.clone(), InodeDir::new(2, 1));
    let mut opts = MkdirOpts::with_create(false);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1_000,
        ttl_action: TtlAction::Delete,
        ..Default::default()
    };
    let target = InodeView::new_dir("target".into(), InodeDir::with_opts(3, 1, opts));
    store.apply_add(&root, &source).unwrap();
    store.apply_add(&root, &target).unwrap();
    let mut renamed = source.clone();
    renamed.change_name("target".into());
    assert_eq!(buckets.total_inodes(), 1);

    let runtime = FaultRuntime::process();
    let rule_id = format!("replace-ttl-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.rename.before_commit")
        .matches("source_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected replace ttl failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = store.apply_replace_rename(&root, &source, &name, &root, &renamed, &target, 10);
    runtime.remove(&rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected replace ttl failure"));
    assert_eq!(buckets.total_inodes(), 1);
    assert!(store.get_inode(2, None).unwrap().is_some());
    assert!(store.get_inode(3, None).unwrap().is_some());

    store
        .apply_replace_rename(&root, &source, &name, &root, &renamed, &target, 10)
        .unwrap();
    assert_eq!(buckets.total_inodes(), 0);
    assert!(store.get_inode(2, None).unwrap().is_some());
    assert!(store.get_inode(3, None).unwrap().is_none());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn replace_hard_link_keeps_surviving_inode_in_ttl_index() {
    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("replace-link-ttl-{}", Utils::rand_str(8)));
    let rocks = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let buckets = Arc::new(TtlBucketList::new(60_000).unwrap());
    let store = InodeStore::new(rocks, buckets.clone()).unwrap();
    let root = InodeView::new_dir("/".into(), InodeDir::new(1, 0));
    let source = InodeView::new_file("source".into(), InodeFile::new(2, 0));
    let mut opts = CreateFileOpts::with_create(false);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1_000,
        ttl_action: TtlAction::Delete,
        ..Default::default()
    };
    let target = InodeView::new_file("target".into(), InodeFile::with_opts(3, 0, opts));
    store.apply_add(&root, &source).unwrap();
    store.apply_add(&root, &target).unwrap();
    store
        .apply_link(&root, &InodeView::new_entry("alias".into(), 3), 3, 1)
        .unwrap();
    let mut renamed = source.clone();
    renamed.change_name("target".into());
    store
        .apply_replace_rename(&root, &source, "source", &root, &renamed, &target, 2)
        .unwrap();
    assert_eq!(buckets.total_inodes(), 1);
    assert_eq!(
        store.get_inode(3, None).unwrap().unwrap().nlink().unwrap(),
        1
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn failed_delete_keeps_ttl_index() {
    Master::init_test_metrics();
    let path = Utils::test_sub_dir(format!("delete-ttl-{}", Utils::rand_str(8)));
    let rocks = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let buckets = Arc::new(TtlBucketList::new(60_000).unwrap());
    let store = InodeStore::new(rocks, buckets.clone()).unwrap();
    let root = InodeView::new_dir("/".into(), InodeDir::new(1, 0));
    let mut opts = MkdirOpts::with_create(false);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1_000,
        ttl_action: TtlAction::Delete,
        ..Default::default()
    };
    let mut dir = InodeView::new_dir("dir".into(), InodeDir::with_opts(2, 1, opts));
    store.apply_add(&root, &dir).unwrap();
    assert_eq!(buckets.total_inodes(), 1);
    // This corrupt edge points to a directory, so staging its file nlink
    // decrement fails after the parent directory has been visited.
    dir.as_dir_mut()
        .unwrap()
        .add_child(InodeView::new_entry("bad".into(), 2))
        .unwrap();
    assert!(store.apply_delete(&root, &dir, "dir", 10).is_err());
    assert_eq!(buckets.total_inodes(), 1);
    assert!(store.get_inode(2, None).unwrap().is_some());
    dir.as_dir_mut().unwrap().delete_child(2, "bad").unwrap();
    store.apply_delete(&root, &dir, "dir", 10).unwrap();
    assert_eq!(buckets.total_inodes(), 0);
    assert!(store.get_inode(2, None).unwrap().is_none());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
