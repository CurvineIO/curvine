// Copyright 2026 OPPO.
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

use crate::master::meta::inode::InodeDir;
use curvine_model::{DirectoryAttributeDelta, DirectoryAttributes};

#[test]
fn directory_delta_uses_base_only_until_persisted() {
    let dir = InodeDir::new(1, 42);
    let delta = DirectoryAttributeDelta::for_child(43);
    assert_eq!(
        dir.directory_attribute_delta(delta).base().unwrap().mtime,
        42
    );
    dir.mark_directory_attributes_persisted();
    assert!(dir.directory_attribute_delta(delta).base().is_none());
}

#[test]
fn failed_directory_delta_does_not_publish_partial_state() {
    let dir = InodeDir::new(1, 0);
    let mut base = DirectoryAttributes::new(0, 0, 0);
    base.version = u64::MAX;
    dir.set_directory_attributes(base);
    assert!(dir
        .apply_directory_attribute_delta(DirectoryAttributeDelta::new(1, 1, 1).for_child_entry())
        .is_err());
    let current = dir.directory_attributes();
    assert_eq!((current.children, current.version), (Some(0), u64::MAX));

    base.version = 0;
    dir.set_directory_attributes(base);
    assert!(dir
        .apply_directory_attribute_delta(DirectoryAttributeDelta::new(1, 1, 1).for_child_removal())
        .is_err());
    // The failing update must not have incremented nlink from zero.
    assert!(dir
        .apply_directory_attribute_delta(DirectoryAttributeDelta::new(1, 1, -1))
        .is_err());
    assert_eq!(dir.directory_attributes().version, 0);
}

#[test]
fn concurrent_directory_deltas_keep_all_updates() {
    let dir = std::sync::Arc::new(InodeDir::new(1, 0));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let dir = dir.clone();
            scope.spawn(move || {
                for time in (0..100).rev() {
                    dir.apply_directory_attribute_delta(
                        DirectoryAttributeDelta::new(time, time * 2, 0).for_child_entry(),
                    )
                    .unwrap();
                }
            });
        }
    });
    let current = dir.directory_attributes();
    assert_eq!((current.children, current.version), (Some(800), 800));
    let metadata_copy = dir.clone_metadata();
    let base = metadata_copy
        .directory_attribute_delta(DirectoryAttributeDelta::for_child(0))
        .base()
        .unwrap();
    assert_eq!((base.mtime, base.ctime), (99, 198));
}
