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
//! All containers store file edges as `FileEntry(name, id)` records and
//! directory edges as `Arc<DirNode>` handles. There is no `Box<InodeView>`
//! directory in the live tree: `InodeView` exists only at the store/journal
//! boundary and inside detached projections. `Packed` inlines file edges as
//! variable-length byte records and keeps directory handles in a side vector.

use crate::master::meta::inode::PackedChildren;
use crate::master::meta::inode::{ChildItem, DirNode, InodeDir, InodeEntry, InodePtr, InodeView};
use curvine_config::ChildrenContainer;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::ListOptions;
use glob::Pattern;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::OnceLock;

static CHILDREN_CONTAINER: OnceLock<ChildrenContainer> = OnceLock::new();

#[derive(Debug, Clone)]
pub enum InodeChildren {
    List(Vec<ChildSlot>),
    Map(BTreeMap<String, ChildSlot>),
    Packed(PackedChildren),
}

/// The live handle returned by `add_child`. Directories carry the slot's
/// own `Arc<DirNode>` — all later writes must go through it; files carry
/// nothing (the caller keeps its owned view for the store write).
pub enum ChildAddResult {
    Dir { name: String, node: Arc<DirNode> },
    File(InodeView),
}

/// One child edge in a `List`/`Map` container. Files carry the inline
/// `FileEntry` record; directories carry the stable `Arc<DirNode>` handle.
#[derive(Debug, Clone)]
pub enum ChildSlot {
    Dir { name: String, node: Arc<DirNode> },
    FileEntry { name: String, id: i64 },
}

impl ChildAddResult {
    /// The entry a caller appends to its resolved path: directories carry
    /// the live node handle, files the hydrated view.
    pub(crate) fn into_entry(self) -> InodeEntry {
        match self {
            Self::Dir { name, node } => InodeEntry::Dir { name, node },
            Self::File(view) => InodeEntry::View(InodePtr::from_owned(view)),
        }
    }
}

impl ChildSlot {
    #[inline]
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Dir { name, .. } => name,
            Self::FileEntry { name, .. } => name,
        }
    }

    #[inline]
    pub(crate) fn id(&self) -> i64 {
        match self {
            Self::Dir { node, .. } => node.id,
            Self::FileEntry { id, .. } => *id,
        }
    }

    /// Classifies an incoming inode into its edge shape. The owned view is
    /// kept by the caller for local use; directories keep only the node
    /// handle, so the projection is detached by construction. A view with a
    /// node backlink (rename move) reuses that node — the live identity must
    /// survive the move.
    pub(crate) fn from_view(inode: &InodeView) -> Self {
        if inode.is_dir() {
            Self::Dir {
                name: inode.name().to_string(),
                node: DirNode::for_edge(inode),
            }
        } else {
            Self::FileEntry {
                name: inode.name().to_string(),
                id: inode.id(),
            }
        }
    }

    #[inline]
    pub(crate) fn as_item(&self) -> ChildItem<'_> {
        match self {
            Self::Dir { name, node } => ChildItem::DirNode { name, node },
            Self::FileEntry { name, id } => ChildItem::FileEntry { name, id: *id },
        }
    }

    /// Detached projection for callers that need the DTO shape; directory
    /// projections never write back to the tree.
    // Borrows by design: every caller iterates shared containers and the
    // slot stays resident; renaming would churn all call sites.
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn into_view(&self) -> InodeView {
        match self {
            Self::Dir { name, node } => InodeDir::from_dir_node_live(name.clone(), node.clone()),
            Self::FileEntry { name, id } => InodeView::new_entry(name.clone(), *id),
        }
    }
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
    fn search_by_name(list: &[ChildSlot], name: &str) -> Result<usize, usize> {
        list.binary_search_by(|f| f.name().cmp(name))
    }

    /// Get children matching glob pattern (e.g., "*.txt", "dir*")
    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodeView> {
        match self {
            InodeChildren::List(list) => list
                .iter()
                .filter(|child| glob_pattern.matches(child.name()))
                .map(ChildSlot::into_view)
                .collect(),
            InodeChildren::Map(map) => map
                .values()
                .filter(|child| glob_pattern.matches(child.name()))
                .map(ChildSlot::into_view)
                .collect(),
            InodeChildren::Packed(packed) => packed
                .get_child_by_glob_pattern(glob_pattern)
                .into_iter()
                .map(|item| item.into_view())
                .collect(),
        }
    }

    pub(crate) fn matching_views(&self, pattern: &Pattern) -> Vec<InodeView> {
        match self {
            Self::Packed(packed) => packed
                .iter()
                .filter(|item| pattern.matches(item.name()))
                .map(|item| item.into_view())
                .collect(),
            _ => self.get_child_by_glob_pattern(pattern),
        }
    }

    pub(crate) fn all_views(&self) -> Vec<InodeView> {
        match self {
            Self::Packed(packed) => packed.iter().map(|item| item.into_view()).collect(),
            _ => match self {
                InodeChildren::List(list) => list.iter().map(ChildSlot::into_view).collect(),
                InodeChildren::Map(map) => map.values().map(ChildSlot::into_view).collect(),
                InodeChildren::Packed(_) => unreachable!("matched above"),
            },
        }
    }

    pub fn get_child_item(&self, name: &str) -> Option<ChildItem<'_>> {
        match self {
            Self::List(list) => Self::search_by_name(list, name)
                .ok()
                .map(|index| list[index].as_item()),
            Self::Map(map) => map.get(name).map(|slot| slot.as_item()),
            Self::Packed(packed) => packed.get_child_item(name),
        }
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
                        Some(list.remove(v).into_view())
                    }
                    Err(_) => None,
                }
            }

            InodeChildren::Map(map) => {
                if let Some(slot) = map.get(child_name) {
                    let id = slot.id();
                    if id != child_id {
                        return err_box!(
                            "Inode status error, expect id {}, actually delete {}",
                            child_id,
                            id
                        );
                    }
                }
                map.remove(child_name).map(|slot| slot.into_view())
            }
            InodeChildren::Packed(packed) => {
                return packed.delete_child(child_id, child_name);
            }
        };

        match removed {
            None => err_box!("Child {} not exists", child_name),
            Some(r) => Ok(r),
        }
    }

    pub(crate) fn list_views(&self, opts: &ListOptions) -> Vec<InodeView> {
        match self {
            Self::Packed(packed) => packed.list_views(opts),
            _ => self
                .list_options(opts)
                .into_iter()
                .map(|item| item.into_view())
                .collect(),
        }
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<ChildItem<'_>> {
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
                slice.iter().take(n).map(ChildSlot::as_item).collect()
            }

            InodeChildren::Map(map) => {
                let range = opts.start_after.as_ref().map_or(
                    map.range::<str, _>((Bound::Unbounded, Bound::Unbounded)),
                    |a| map.range::<str, _>((Bound::Excluded(a.as_str()), Bound::Unbounded)),
                );
                let n = opts.limit.unwrap_or(usize::MAX);
                range.take(n).map(|(_, v)| v.as_item()).collect()
            }

            InodeChildren::Packed(packed) => packed.list_options(opts),
        }
    }

    /// Adds an edge. File inodes are stored as inline `FileEntry` records and
    /// the owned view is returned for local use. Directory edges live only in
    /// the slot's `Arc<DirNode>`; the returned handle is the *same* node the
    /// slot stores, so every later write goes to the live object.
    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<ChildAddResult> {
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, inode.name());
                match index {
                    Err(v) => {
                        let slot = ChildSlot::from_view(&inode);
                        let result = Self::slot_result(&slot, inode);
                        list.insert(v, slot);
                        Ok(result)
                    }

                    Ok(_) => err_box!("Child {} already exists", inode.name()),
                }
            }

            InodeChildren::Map(map) => match map.entry(inode.name().to_owned()) {
                Entry::Vacant(v) => {
                    let slot = ChildSlot::from_view(&inode);
                    let result = Self::slot_result(&slot, inode);
                    v.insert(slot);
                    Ok(result)
                }

                Entry::Occupied(_) => err_box!("Child {} already exists", inode.name()),
            },

            InodeChildren::Packed(packed) => packed.add_child(inode),
        }
    }

    /// The live handle a caller keeps once the edge lands in the slot.
    /// Directories expose the slot's own `Arc<DirNode>` so every later write
    /// reaches the shared node; files expose the caller's owned view because
    /// the slot records only the id and cannot rebuild the file state.
    fn slot_result(slot: &ChildSlot, inode: InodeView) -> ChildAddResult {
        match slot {
            ChildSlot::Dir { name, node } => ChildAddResult::Dir {
                name: name.clone(),
                node: node.clone(),
            },
            ChildSlot::FileEntry { .. } => ChildAddResult::File(inode),
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

    fn all_names(children: &InodeChildren) -> Vec<String> {
        children
            .list_options(&ListOptions::default())
            .iter()
            .map(|item| item.name().to_string())
            .collect()
    }

    #[test]
    fn containers_agree_on_add_get_delete_and_listing() {
        for container in CONTAINERS {
            let mut children = InodeChildren::new(container);

            children.add_child(file_edge("b.txt", 2)).unwrap();
            children.add_child(file_edge("a.txt", 1)).unwrap();
            children.add_child(dir_edge("sub", 3)).unwrap();
            assert_eq!(children.len(), 3, "{container:?}");

            assert_eq!(children.get_child_item("a.txt").unwrap().id(), 1);
            assert_eq!(children.get_child_item("sub").unwrap().id(), 3);
            assert!(children.get_child_item("missing").is_none());

            assert_eq!(
                all_names(&children),
                vec!["a.txt", "b.txt", "sub"],
                "{container:?}"
            );

            let page = children.list_options(&ListOptions {
                limit: Some(1),
                start_after: Some("a.txt".to_string()),
            });
            let page = page
                .iter()
                .map(|item| item.name().to_string())
                .collect::<Vec<_>>();
            assert_eq!(page, vec!["b.txt"], "{container:?}");

            let matched = children.get_child_by_glob_pattern(&Pattern::new("*.txt").unwrap());
            let matched = matched
                .iter()
                .map(|view| view.name().to_string())
                .collect::<Vec<_>>();
            assert_eq!(matched, vec!["a.txt", "b.txt"], "{container:?}");

            assert!(
                children.add_child(file_edge("a.txt", 9)).is_err(),
                "{container:?}"
            );

            assert_eq!(children.delete_child(2, "b.txt").unwrap().id(), 2);
            assert!(children.get_child_item("b.txt").is_none());
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
