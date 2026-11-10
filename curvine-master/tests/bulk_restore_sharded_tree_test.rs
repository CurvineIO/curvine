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

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_model::SetAttrOpts;
use curvine_runtime::common::Utils;

#[test]
fn restored_large_directory_preserves_nested_tree_and_metadata() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("bulk-restore-sharded-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/large/nested/deep", true).unwrap();
    let leaf = fs.create("/large/nested/deep/leaf", false).unwrap();
    for id in 0..2048 {
        fs.create(format!("/large/file-{id:04}"), false).unwrap();
    }
    fs.set_attr(
        "/large/nested",
        SetAttrOpts {
            mtime: Some(12345),
            ..Default::default()
        },
    )
    .unwrap();
    let nested_before = fs.file_status("/large/nested").unwrap();
    let last_before = fs.file_status("/large/file-2047").unwrap();

    let dir = fs.fs_dir.read();
    let (_, root) = dir.store_ref().create_tree().unwrap();
    let large = root
        .children_handle()
        .unwrap()
        .read_child_view("large")
        .0
        .unwrap();
    let large_children = large.children_handle().unwrap();
    assert!(format!("{large_children:?}").contains("sharded: true"));
    assert_eq!(large_children.iter_len(), 2049);
    let nested = large_children.read_child_view("nested").0.unwrap();
    let nested_status = nested.to_file_status("/large/nested").unwrap();
    assert_eq!(nested_status.id, nested_before.id);
    assert_eq!(nested_status.mtime, nested_before.mtime);
    // A projected view must NOT mutate the live node (S3-B invariant).
    let mut nested_view = large_children.read_child_view("nested").0.unwrap();
    nested_view.update_mtime(23456);
    let updated = large_children.read_child_view("nested").0.unwrap();
    assert_eq!(
        updated.to_file_status("/large/nested").unwrap().mtime,
        nested_before.mtime,
        "projection writes must not reach the live node"
    );
    assert_eq!(updated.children_handle().unwrap().iter_len(), 1);
    let deep = nested
        .children_handle()
        .unwrap()
        .read_child_view("deep")
        .0
        .unwrap();
    let restored_leaf = deep
        .children_handle()
        .unwrap()
        .read_child_view("leaf")
        .0
        .unwrap();
    assert_eq!(restored_leaf.id(), leaf.id);
    assert_eq!(
        large_children.read_child_view("file-2047").0.unwrap().id(),
        last_before.id
    );
    drop(dir);

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(
        fs.file_status("/large/nested/deep/leaf").unwrap().id,
        leaf.id
    );
    assert_eq!(fs.file_status("/large/nested").unwrap().mtime, 12345);
    fs.create("/large/nested/deep/after-restore", false)
        .unwrap();
    fs.set_attr(
        "/large/nested",
        SetAttrOpts {
            mtime: Some(34567),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(fs.file_status("/large/nested").unwrap().mtime, 34567);
    assert!(fs.exists("/large/nested/deep/after-restore").unwrap());
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/large/nested").unwrap().mtime, 34567);
    assert!(fs.exists("/large/nested/deep/after-restore").unwrap());
    journal.shutdown();
}
