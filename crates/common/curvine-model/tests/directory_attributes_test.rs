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

use curvine_model::{
    children_count_persisted, encode_matching, persist_children_count, DirectoryAttributeDelta,
    DirectoryAttributes, RECORD_FORMAT_READABLE,
};
use std::sync::Mutex;

static CHILDREN_COUNT_PERSISTENCE: Mutex<()> = Mutex::new(());

struct ResetChildrenCountPersistence;

impl Drop for ResetChildrenCountPersistence {
    fn drop(&mut self) {
        persist_children_count(false);
    }
}

#[test]
fn failed_delta_leaves_attributes_unchanged() {
    let mut attributes = DirectoryAttributes::new(1, 2, 0);
    let original = attributes;
    assert_eq!(
        attributes.apply(DirectoryAttributeDelta::new(10, 20, -1)),
        None
    );
    assert_eq!(attributes, original);

    attributes.version = u64::MAX;
    let original = attributes;
    assert_eq!(
        attributes.apply(DirectoryAttributeDelta::new(10, 20, 1)),
        None
    );
    assert_eq!(attributes, original);
}

#[test]
fn negative_persisted_child_counts_are_rejected() {
    let mut invalid = DirectoryAttributes::new(0, 0, 0);
    invalid.children = Some(-1);
    assert_eq!(DirectoryAttributes::decode(&invalid.encode()), None);
    let delta = DirectoryAttributeDelta::for_child(1).with_base(invalid);
    assert_eq!(DirectoryAttributeDelta::decode(&delta.encode()), None);
}

#[test]
fn directory_attribute_delta_combines_only_safe_updates() {
    let first = DirectoryAttributeDelta::new(10, 12, 1);
    let second = DirectoryAttributeDelta::new(15, 14, 2);
    assert_eq!(first.combine(second), second.combine(first));

    let mut attributes = DirectoryAttributes::new(1, 2, 3);
    attributes.apply(first.combine(second).unwrap()).unwrap();
    assert_eq!(attributes.mtime, 15);
    assert_eq!(attributes.ctime, 14);
    assert_eq!(attributes.nlink, 6);
    assert_eq!(attributes.version, 2);

    let negative = DirectoryAttributeDelta::new(15, 14, -1);
    assert_eq!(first.combine(negative), None);
    assert_eq!(negative.combine(first), None);

    let add_child = DirectoryAttributeDelta::for_child(15).for_child_entry();
    let remove_child = DirectoryAttributeDelta::for_child(16).for_child_removal();
    assert_eq!(add_child.combine(remove_child), None);
    assert_eq!(remove_child.combine(add_child), None);
}

#[test]
fn directory_attribute_encoding_rejects_other_record_types() {
    let attributes = DirectoryAttributes::new(11, 12, 13);
    let delta = DirectoryAttributeDelta::new(14, 15, 1);

    assert_eq!(
        DirectoryAttributes::decode(&attributes.encode()),
        Some(attributes)
    );
    assert_eq!(
        DirectoryAttributeDelta::decode(&delta.encode()),
        Some(delta)
    );
    assert_eq!(DirectoryAttributes::decode(&delta.encode()), None);
    assert_eq!(DirectoryAttributeDelta::decode(&attributes.encode()), None);
}

#[test]
fn delta_base_preserves_known_and_unknown_child_counts() {
    for children in [None, Some(0), Some(17)] {
        let mut base = DirectoryAttributes::new(1, 2, 3);
        base.children = children;
        let delta = DirectoryAttributeDelta::for_child(4).with_base(base);
        assert_eq!(
            DirectoryAttributeDelta::decode(&delta.encode())
                .unwrap()
                .base(),
            Some(base)
        );
        assert_eq!(
            DirectoryAttributeDelta::decode(&delta.encode_legacy())
                .unwrap()
                .base()
                .unwrap()
                .children,
            None
        );
    }
}

#[test]
fn unsupported_record_generations_are_rejected() {
    let mut old_base = vec![0; 24];
    old_base[..4].copy_from_slice(b"CDA1");
    assert_eq!(DirectoryAttributes::decode(&old_base), None);

    let mut old_delta = vec![0; 48];
    old_delta[..4].copy_from_slice(b"CDD2");
    assert_eq!(DirectoryAttributeDelta::decode(&old_delta), None);

    let mut newer_base = vec![0; 40];
    newer_base[..4].copy_from_slice(b"CDA5");
    assert_eq!(DirectoryAttributes::decode(&newer_base), None);

    let mut newer_delta = vec![0; 80];
    newer_delta[..4].copy_from_slice(b"CDD5");
    assert_eq!(DirectoryAttributeDelta::decode(&newer_delta), None);
}

#[test]
fn child_count_format_preserves_legacy_records_and_merges_current_records() {
    let _guard = CHILDREN_COUNT_PERSISTENCE.lock().unwrap();
    let _reset = ResetChildrenCountPersistence;
    let base = DirectoryAttributes::new(10, 11, 2);
    let delta = DirectoryAttributeDelta::for_child(12).for_child_entry();

    assert_eq!(RECORD_FORMAT_READABLE, 4);
    persist_children_count(false);
    let legacy = encode_matching(None, &base.encode(), &base.encode_legacy());
    assert_eq!(
        DirectoryAttributes::decode(&legacy).unwrap().children(),
        None
    );

    persist_children_count(true);
    assert!(children_count_persisted());
    let current = encode_matching(None, &base.encode(), &base.encode_legacy());
    let mut attributes = DirectoryAttributes::decode(&current).unwrap();
    attributes.apply(delta).unwrap();
    assert_eq!(attributes.children(), Some(1));
    assert_eq!(
        encode_matching(
            Some(&legacy),
            &attributes.encode(),
            &attributes.encode_legacy()
        ),
        attributes.encode_legacy(),
    );
}

#[test]
fn directory_attribute_delta_preserves_base_order() {
    let base = DirectoryAttributes::new(10, 10, 2);
    let current = DirectoryAttributeDelta::new(20, 20, 1).with_base(base);
    let following = DirectoryAttributeDelta::new(15, 15, 1);

    let combined = current.combine(following).unwrap();
    assert_eq!(combined.mtime, 20);
    assert_eq!(combined.ctime, 20);
    assert_eq!(combined.nlink_delta, 2);
    assert_eq!(combined.version_delta, 2);
    assert_eq!(combined.base(), Some(base));
    assert_eq!(following.combine(current), None);
    assert_eq!(
        current.combine(
            DirectoryAttributeDelta::new(25, 25, 1).with_base(DirectoryAttributes::new(11, 11, 2)),
        ),
        None
    );
}
