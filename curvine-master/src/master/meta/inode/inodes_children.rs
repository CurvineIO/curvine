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

//! Directory children containers.
//!
//! `List` and `Map` keep a full `Box<InodeView>` per child and are retained for
//! benchmarks and rollback. `Packed` delegates to [`PackedChildren`], which
//! inlines file edges as variable-length byte records and keeps directory
//! objects in a side vector.

use crate::master::meta::inode::PackedChildren;
use crate::master::meta::inode::PackedIter;
use crate::master::meta::inode::{ChildItem, InodePtr, InodeView};
use curvine_config::ChildrenContainer;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::ListOptions;
use glob::Pattern;
use std::collections::btree_map::{Entry, Values};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::slice::Iter;
use std::sync::OnceLock;

static CHILDREN_CONTAINER: OnceLock<ChildrenContainer> = OnceLock::new();

#[derive(Debug, Clone)]
pub enum InodeChildren {
    List(Vec<Box<InodeView>>),
    Map(BTreeMap<String, Box<InodeView>>),
    Packed(PackedChildren),
}

impl InodeChildren {
    pub(crate) fn set_container(container: ChildrenContainer) -> CommonResult<()> {
        let current = *CHILDREN_CONTAINER.get_or_init(|| container);
        if current == container {
            return Ok(());
        }
        err_box!(
            "children_container is already fixed to {:?} in this process, cannot switch to {:?}",
            current,
            container
        )
    }

    fn configured_container() -> ChildrenContainer {
        CHILDREN_CONTAINER.get().copied().unwrap_or_default()
    }

    pub fn new(container: ChildrenContainer) -> Self {
        match container {
            ChildrenContainer::List => Self::new_list(),
            ChildrenContainer::Map => Self::new_map(),
            ChildrenContainer::Packed => Self::new_packed(),
        }
    }

    pub fn new_list() -> Self {
        InodeChildren::List(vec![])
    }

    pub fn new_map() -> Self {
        InodeChildren::Map(BTreeMap::new())
    }

    pub fn new_packed() -> Self {
        InodeChildren::Packed(PackedChildren::new())
    }

    // Search for whether the current inode name exists.
    fn search_by_name(list: &[Box<InodeView>], name: &str) -> Result<usize, usize> {
        list.binary_search_by(|f| f.name().cmp(name))
    }

    /// Get children matching glob pattern (e.g., "*.txt", "dir*")
    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodePtr> {
        match self {
            InodeChildren::List(list) => list
                .iter()
                .filter(|child| glob_pattern.matches(child.name()))
                .map(|child| InodePtr::from_ref(child.as_ref()))
                .collect(),
            InodeChildren::Map(map) => map
                .values()
                .filter(|child| glob_pattern.matches(child.name()))
                .map(|child| InodePtr::from_ref(child.as_ref()))
                .collect(),
            InodeChildren::Packed(packed) => packed.get_child_by_glob_pattern(glob_pattern),
        }
    }

    pub(crate) fn matching_views(&self, pattern: &Pattern) -> Vec<InodeView> {
        match self {
            Self::Packed(packed) => packed
                .iter()
                .filter(|item| pattern.matches(item.name()))
                .map(|item| item.into_view())
                .collect(),
            _ => self
                .get_child_by_glob_pattern(pattern)
                .into_iter()
                .map(|child| child.as_ref().clone())
                .collect(),
        }
    }

    pub(crate) fn all_views(&self) -> Vec<InodeView> {
        match self {
            Self::Packed(packed) => packed.iter().map(|item| item.into_view()).collect(),
            _ => self.iter().map(|child| child.as_ref().clone()).collect(),
        }
    }

    pub(crate) fn get_child_item(&self, name: &str) -> Option<ChildItem<'_>> {
        match self {
            Self::List(list) => Self::search_by_name(list, name)
                .ok()
                .map(|index| ChildItem::View(list[index].as_ref())),
            Self::Map(map) => map.get(name).map(|view| ChildItem::View(view.as_ref())),
            Self::Packed(packed) => packed.get_child_item(name),
        }
    }

    pub fn get_child(&self, name: &str) -> Option<InodePtr> {
        self.get_child_item(name).map(|item| item.as_ptr())
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let removed = match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, child_name);
                match index {
                    Ok(v) => {
                        let id = list[v].id();
                        if id != child_id {
                            return err_box!(
                                "Inode status error, expect id {}, actually delete {}",
                                child_id,
                                id
                            );
                        }
                        Some(list.remove(v))
                    }
                    Err(_) => None,
                }
            }

            InodeChildren::Map(map) => {
                if let Some(inode) = map.get(child_name) {
                    let id = inode.id();
                    if id != child_id {
                        return err_box!(
                            "Inode status error, expect id {}, actually delete {}",
                            child_id,
                            id
                        );
                    }
                }
                map.remove(child_name)
            }
            InodeChildren::Packed(packed) => {
                return packed.delete_child(child_id, child_name);
            }
        };

        match removed {
            None => err_box!("Child {} not exists", child_name),
            Some(r) => Ok(*r),
        }
    }

    pub(crate) fn list_views(&self, opts: &ListOptions) -> Vec<InodeView> {
        match self {
            Self::Packed(packed) => packed.list_views(opts),
            _ => self
                .list_options(opts)
                .into_iter()
                .map(|child| child.as_ref().clone())
                .collect(),
        }
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<InodePtr> {
        match self {
            InodeChildren::List(list) => {
                let start = opts
                    .start_after
                    .as_ref()
                    .map(|a| match Self::search_by_name(list, a) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    })
                    .unwrap_or(0);
                let slice = &list[start..];
                let n = opts.limit.unwrap_or(slice.len());
                slice
                    .iter()
                    .take(n)
                    .map(|b| InodePtr::from_ref(b.as_ref()))
                    .collect()
            }

            InodeChildren::Map(map) => {
                let range = opts.start_after.as_ref().map_or(
                    map.range::<str, _>((Bound::Unbounded, Bound::Unbounded)),
                    |a| map.range::<str, _>((Bound::Excluded(a.as_str()), Bound::Unbounded)),
                );
                let n = opts.limit.unwrap_or(usize::MAX);
                range
                    .take(n)
                    .map(|(_, v)| InodePtr::from_ref(v.as_ref()))
                    .collect()
            }

            InodeChildren::Packed(packed) => packed.list_options(opts),
        }
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        // Assert that it should not be FileEntry
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, inode.name());
                match index {
                    Err(v) => {
                        if inode.is_file() {
                            let entry = Box::new(InodeView::new_entry(
                                inode.name().to_string(),
                                inode.id(),
                            ));
                            list.insert(v, entry);
                            Ok(InodePtr::from_owned(inode))
                        } else {
                            let boxed = Box::new(inode);
                            list.insert(v, boxed);
                            Ok(InodePtr::from_ref(list[v].as_ref()))
                        }
                    }

                    Ok(_) => err_box!("Child {} already exists", inode.name()),
                }
            }

            InodeChildren::Map(map) => match map.entry(inode.name().to_owned()) {
                Entry::Vacant(v) => {
                    if inode.is_file() {
                        // Store lightweight FileEntry in map
                        let name = inode.name().to_string();
                        let id = inode.id();
                        v.insert(Box::new(InodeView::new_entry(name, id)));
                        // But return owned pointer to complete object
                        Ok(InodePtr::from_owned(inode))
                    } else {
                        // Directory: move ownership into the map and return a reference.
                        let inserted = v.insert(Box::new(inode));
                        Ok(InodePtr::from_ref(inserted.as_ref()))
                    }
                }

                Entry::Occupied(_) => err_box!("Child {} already exists", inode.name()),
            },

            InodeChildren::Packed(packed) => packed.add_child(inode),
        }
    }

    pub fn iter(&self) -> ChildrenIter<'_> {
        match self {
            InodeChildren::List(list) => ChildrenIter {
                len: list.len(),
                inner: InnerIter::List(list.iter()),
            },

            InodeChildren::Map(map) => ChildrenIter {
                len: map.len(),
                inner: InnerIter::Map(map.values()),
            },

            InodeChildren::Packed(packed) => ChildrenIter {
                len: packed.len(),
                inner: InnerIter::Packed(packed.iter()),
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InodeChildren::List(list) => list.len(),
            InodeChildren::Map(map) => map.len(),
            InodeChildren::Packed(packed) => packed.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for InodeChildren {
    fn default() -> Self {
        Self::new(Self::configured_container())
    }
}

enum InnerIter<'a> {
    List(Iter<'a, Box<InodeView>>),
    Map(Values<'a, String, Box<InodeView>>),
    Packed(PackedIter<'a>),
}

pub struct ChildrenIter<'a> {
    len: usize,
    inner: InnerIter<'a>,
}

impl ChildrenIter<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Iterator for ChildrenIter<'_> {
    type Item = InodePtr;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            InnerIter::List(list) => list.next().map(|child| InodePtr::from_ref(child.as_ref())),
            InnerIter::Map(map) => map.next().map(|child| InodePtr::from_ref(child.as_ref())),
            InnerIter::Packed(iter) => iter.next().map(|item| item.as_ptr()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::InodeDir;

    const CONTAINERS: [ChildrenContainer; 3] = [
        ChildrenContainer::List,
        ChildrenContainer::Map,
        ChildrenContainer::Packed,
    ];

    fn file_edge(name: &str, id: i64) -> InodeView {
        InodeView::new_entry(name.to_string(), id)
    }

    fn dir_edge(name: &str, id: i64) -> InodeView {
        InodeView::new_dir(name.to_string(), InodeDir::new(id, 0))
    }

    fn child_names(ptrs: &[InodePtr]) -> Vec<String> {
        ptrs.iter().map(|ptr| ptr.name().to_string()).collect()
    }

    fn all_names(children: &InodeChildren) -> Vec<String> {
        children.iter().map(|ptr| ptr.name().to_string()).collect()
    }

    #[test]
    fn containers_agree_on_add_get_delete_and_listing() {
        for container in CONTAINERS {
            let mut children = InodeChildren::new(container);

            children.add_child(file_edge("b.txt", 2)).unwrap();
            children.add_child(file_edge("a.txt", 1)).unwrap();
            children.add_child(dir_edge("sub", 3)).unwrap();
            assert_eq!(children.len(), 3, "{container:?}");

            assert_eq!(children.get_child("a.txt").unwrap().id(), 1);
            assert_eq!(children.get_child("sub").unwrap().id(), 3);
            assert!(children.get_child("missing").is_none());

            assert_eq!(
                all_names(&children),
                vec!["a.txt", "b.txt", "sub"],
                "{container:?}"
            );

            let page = children.list_options(&ListOptions {
                limit: Some(1),
                start_after: Some("a.txt".to_string()),
            });
            assert_eq!(child_names(&page), vec!["b.txt"], "{container:?}");

            let matched = children.get_child_by_glob_pattern(&Pattern::new("*.txt").unwrap());
            assert_eq!(
                child_names(&matched),
                vec!["a.txt", "b.txt"],
                "{container:?}"
            );

            assert!(
                children.add_child(file_edge("a.txt", 9)).is_err(),
                "{container:?}"
            );

            assert_eq!(children.delete_child(2, "b.txt").unwrap().id(), 2);
            assert!(children.get_child("b.txt").is_none());
            assert_eq!(all_names(&children), vec!["a.txt", "sub"], "{container:?}");
        }
    }

    #[test]
    fn default_container_follows_the_configured_one() {
        let configured = InodeChildren::configured_container();
        let matched = match InodeChildren::default() {
            InodeChildren::List(_) => configured == ChildrenContainer::List,
            InodeChildren::Map(_) => configured == ChildrenContainer::Map,
            InodeChildren::Packed(_) => configured == ChildrenContainer::Packed,
        };
        assert!(matched, "default container disagrees with {configured:?}");
    }
}
