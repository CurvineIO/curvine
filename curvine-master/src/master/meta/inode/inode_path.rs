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

use crate::master::meta::glob_utils::parse_glob_pattern;
use crate::master::meta::inode::InodeView::{self, Dir, File, FileEntry};
use crate::master::meta::inode::{
    ChildEdge, DirNode, DirectoryChildren, DirectoryChildrenSharedVersion, InodeDir, InodeFile,
    InodePtr, EMPTY_PARENT_ID, PATH_SEPARATOR,
};
use crate::master::meta::store::InodeStore;
use curvine_core_error::{err_box, try_option, CommonResult};
use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

/// One resolved path element. Live directories carry the stable
/// [`DirNode`] handle; files and persistence DTOs carry the legacy
/// `InodePtr`. Directory live state is only reachable via
/// [`InodeEntry::dir_node`] — there is no `RawPtr`/`&mut` access to a
/// directory's `InodeView`.
pub enum InodeEntry {
    Dir { name: String, node: Arc<DirNode> },
    View(InodePtr),
}

impl Clone for InodeEntry {
    fn clone(&self) -> Self {
        match self {
            Self::Dir { name, node } => Self::Dir {
                name: name.clone(),
                node: node.clone(),
            },
            // RawPtr::clone never takes ownership, so the copy is a borrow.
            Self::View(ptr) => Self::View(ptr.clone()),
        }
    }
}

impl fmt::Debug for InodeEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dir { name, node } => f.debug_tuple("Dir").field(name).field(&node.id).finish(),
            Self::View(ptr) => f.debug_tuple("View").field(&ptr.id()).finish(),
        }
    }
}

impl InodeEntry {
    pub fn id(&self) -> i64 {
        match self {
            // Immutable identity; safe to read without locks.
            Self::Dir { node, .. } => node.id,
            Self::View(ptr) => ptr.id(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Dir { name, .. } => name,
            Self::View(ptr) => ptr.name(),
        }
    }

    /// Live directory handle. File entries return `None`; callers that need
    /// a persistence DTO use [`InodePath::get_inode`].
    pub(crate) fn dir_node(&self) -> Option<&Arc<DirNode>> {
        match self {
            Self::Dir { node, .. } => Some(node),
            Self::View(_) => None,
        }
    }
}

/// One resolved path element. Live directories carry the stable
/// [`DirNode`] handle; files and persistence DTOs carry the legacy
/// `InodePtr`. Directory live state is only reachable via
/// [`InodeEntry::dir_node`] — there is no `RawPtr`/`&mut` access to a
/// directory's `InodeView`.
pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodeEntry>,
    /// Captured (handle, version) per resolved directory level, for callers
    /// that walk shared handles and must validate after reading.
    versions: SmallVec<[DirectoryChildrenSharedVersion; 4]>,
}

impl InodePath {
    /// Versioned walk over shared directory handles. Returns the path plus
    /// the per-level version tokens; the caller re-checks them with
    /// [`Self::walk_is_current`] and retries on invalidation instead of
    /// taking a global lock.
    pub fn resolve_shared<T: AsRef<str>>(
        root_view: &InodeView,
        path: T,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        store.ensure_available()?;
        let components = InodeView::path_components(path.as_ref())?;
        let name = try_option!(
            components.last(),
            "Path {} has no components",
            path.as_ref()
        );
        if name.is_empty() {
            return err_box!("Path {} is invalid", path.as_ref());
        }

        let mut inodes: Vec<InodeEntry> = Vec::with_capacity(components.len());
        let mut versions = SmallVec::new();
        let mut directory = match root_view.children_handle() {
            Some(handle) => handle,
            None => return err_box!("root inode is not a directory"),
        };

        // Preserve the complete directory view at every resolved level.
        versions.push(directory.shared_version());
        let mut root_view = root_view.clone();
        root_view.change_name(components[0].clone());
        inodes.push(InodeEntry::View(InodePtr::from_owned(root_view)));

        for component in components.iter().skip(1) {
            let (child, _child_version) = directory.read_child_view(component);
            let child = match child {
                None => break,
                Some(child) => child,
            };
            let resolved = match child {
                InodeView::FileEntry(entry) => {
                    let full = store.get_inode(entry.id(), Some(entry.name()))?;
                    InodePtr::from_owned(try_option!(
                        full,
                        "Failed to load inode {} from store",
                        entry.id()
                    ))
                }
                other => InodePtr::from_owned(other),
            };
            let next_directory = resolved.as_ref().children_handle();
            inodes.push(InodeEntry::View(resolved));
            match next_directory {
                Some(next) => {
                    versions.push(next.shared_version());
                    directory = next;
                }
                None => break,
            }
        }

        Ok(Self {
            path: path.as_ref().to_string(),
            name: name.to_string(),
            components,
            inodes,
            versions,
        })
    }

    /// Whether every directory level captured by a shared walk is still
    /// quiescent at its recorded version. Cheap atomic re-reads only.
    pub fn walk_is_current(&self) -> bool {
        self.versions.iter().all(|version| version.is_current())
    }

    pub fn resolve<T: AsRef<str>>(
        root: InodePtr,
        path: T,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        Ok(
            Self::resolve_inner::<T, false, false>(root, path, store, &[], 0)?
                .expect("unconditional path resolution always returns a path"),
        )
    }

    /// Resolve while holding locks for every planned inode. Stop before
    /// traversing an inode whose identity differs from the lock plan.
    pub(crate) fn resolve_locked<T: AsRef<str>>(
        root: InodePtr,
        path: T,
        store: &InodeStore,
        ids: &[i64],
        components_len: usize,
    ) -> CommonResult<Option<Self>> {
        Self::resolve_inner::<T, false, true>(root, path, store, ids, components_len)
    }

    /// Capture directory versions before each traversal step and before the
    /// caller reads the final directory's children.
    pub fn resolve_versioned<T: AsRef<str>>(
        root: InodePtr,
        path: T,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        Ok(
            Self::resolve_inner::<T, true, false>(root, path, store, &[], 0)?
                .expect("unconditional path resolution always returns a path"),
        )
    }

    fn resolve_inner<T: AsRef<str>, const TRACK_VERSIONS: bool, const CHECK_IDS: bool>(
        root: InodePtr,
        path: T,
        store: &InodeStore,
        ids: &[i64],
        components_len: usize,
    ) -> CommonResult<Option<Self>> {
        store.ensure_available()?;
        let components = InodeView::path_components(path.as_ref())?;
        let name = try_option!(
            components.last(),
            "Path {} has no components",
            path.as_ref()
        );

        if name.is_empty() {
            return err_box!("Path {} is invalid", path.as_ref());
        }

        if CHECK_IDS && components.len() != components_len {
            return Ok(None);
        }
        let mut inodes: Vec<InodeEntry> = Vec::with_capacity(components.len());
        let mut versions = SmallVec::new();
        // `pending` is the child edge captured at the previous level: `None`
        // means the current element is the root, `Dir` a stable directory
        // handle, `FileId` a file edge to hydrate through the store.
        let mut pending: Option<ChildEdge> = None;
        let mut cur_children: Option<Arc<DirectoryChildren>> = root.as_ref().children_handle();
        // Holds the caller's root pointer until the first push moves it into
        // the entry, so an owned root lives as long as the resolved path.
        let mut owned_root = Some(root);
        let mut index = 0;

        while index < components.len() {
            // Resolve the current element from the pending edge before any
            // child access; the edge id is the plan-identity source.
            let (cur_id, resolved_entry) = match &pending {
                None => {
                    let id = owned_root.as_ref().expect("root kept for first round").id();
                    let entry = InodeEntry::View(owned_root.take().expect("root moved once"));
                    (id, entry)
                }
                Some(ChildEdge::Dir(node)) => {
                    let name = components.get(index).cloned().unwrap_or_default();
                    (
                        node.id,
                        InodeEntry::Dir {
                            name,
                            node: node.clone(),
                        },
                    )
                }
                Some(ChildEdge::FileId(id)) => {
                    let full = store.get_inode(*id, None)?;
                    let Some(full) = full else {
                        return err_box!("Failed to load inode {} from store", id);
                    };
                    let id = full.id();
                    (id, InodeEntry::View(InodePtr::from_owned(full)))
                }
            };
            if CHECK_IDS && ids.get(index).is_none_or(|id| cur_id != *id) {
                return Ok(None);
            }
            if TRACK_VERSIONS {
                if let Some(children) = &cur_children {
                    versions.push(children.shared_version());
                }
            }
            let next = if index + 1 < components.len() {
                let edge = cur_children
                    .as_ref()
                    .and_then(|children| children.get_child_edge(&components[index + 1]));
                edge
            } else {
                None
            };
            inodes.push(resolved_entry);
            // Advance: the captured edge decides the next level's identity.
            // A missing edge stops the walk, exactly like the previous
            // traversal: later components stay unresolved in this path. A
            // file edge continues only so the final component hydrates; it
            // has no children, so deeper components cannot resolve.
            match &next {
                Some(ChildEdge::Dir(node)) => {
                    pending = next.clone();
                    cur_children = Some(node.children.clone());
                }
                Some(ChildEdge::FileId(_)) => {
                    pending = next.clone();
                    cur_children = None;
                }
                None => break,
            }
            index += 1;
        }

        if CHECK_IDS && inodes.len() != ids.len() {
            return Ok(None);
        }
        let inode_path = Self {
            path: path.as_ref().to_string(),
            name: name.to_string(),
            components,
            inodes,
            versions,
        };

        Ok(Some(inode_path))
    }

    fn reconstruct_path_for_match(
        curr_index: i64,
        curr_node: InodePtr,
        mut parent_id: i64,
        components_length: i64,
        parent_map: &HashMap<i64, i64>,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        let mut path_inodes_rebuild = Vec::new();
        let mut idx = curr_index;

        // Leaf
        match curr_node.as_ref() {
            File(..) | Dir(..) => path_inodes_rebuild.push(InodeEntry::View(curr_node)),
            FileEntry(e) => {
                let resolved_leaf_node = match store.get_inode(e.id(), Some(e.name()))? {
                    Some(full_inode) => InodePtr::from_owned(full_inode),
                    None => {
                        return err_box!(
                            "Failed to load parent inode {} from store",
                            curr_node.as_ref().id()
                        )
                    }
                };
                path_inodes_rebuild.push(InodeEntry::View(resolved_leaf_node));
            }
        }

        // Parents
        while idx != 0 {
            let resolved_parent = match store.get_inode(parent_id, None)? {
                Some(full_inode) => InodePtr::from_owned(full_inode),
                None => return err_box!("Failed to load parent inode {} from store", parent_id),
            };
            path_inodes_rebuild.push(InodeEntry::View(resolved_parent));
            idx -= 1;
            if idx != 0 {
                parent_id = match parent_map.get(&parent_id) {
                    Some(parent) => *parent,
                    None => return err_box!("No parent found for inode {}", parent_id),
                };
            }
        }
        // Reverse to get correct order from root to leaf
        path_inodes_rebuild.reverse();

        let components_result: Vec<String> = path_inodes_rebuild
            .iter()
            .map(|node| match node {
                InodeEntry::View(ptr) => ptr.name().to_string(),
                InodeEntry::Dir { .. } => String::new(),
            })
            .collect();

        let path_str = components_result
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("/");

        if path_inodes_rebuild.len() != components_length as usize {
            return err_box!(
                "Path length mismatch during inode rebuild: {} vs {}",
                path_inodes_rebuild.len(),
                components_length
            );
        }

        Ok(Self {
            versions: SmallVec::new(),
            path: path_str,
            name: components_result.last().cloned().unwrap_or_else(|| {
                format!(
                    "Failed to load last name from path {:?}",
                    path_inodes_rebuild
                )
            }),
            components: components_result,
            inodes: path_inodes_rebuild,
        })
    }

    pub fn resolve_for_glob_pattern(
        root: InodePtr,
        pattern: &str,
        store: &InodeStore,
    ) -> CommonResult<Vec<Self>> {
        store.ensure_available()?;
        let components = InodeView::path_components(pattern)?;
        let components_length: i64 = components.len() as i64;
        let mut results = Vec::new();
        let mut queue: VecDeque<(i64, InodePtr, i64)> = VecDeque::new(); // index, node, parent id

        // Directories cannot be hard-linked, so their IDs uniquely identify parents.
        let mut parent_map: HashMap<i64, i64> = HashMap::new();
        queue.push_back((0, root, EMPTY_PARENT_ID)); // Start BFS

        while let Some((curr_index, curr_node, parent_id)) = queue.pop_front() {
            if curr_index == components_length - 1 {
                let inode_path_entry = Self::reconstruct_path_for_match(
                    curr_index,
                    curr_node,
                    parent_id,
                    components_length,
                    &parent_map,
                    store,
                )?;
                results.push(inode_path_entry);
                continue;
            }

            // Expand to next level
            if let Dir(d) = curr_node.as_ref() {
                let next_name = components.get(curr_index as usize + 1).map(|s| s.as_str());
                if let Some(child_name_str) = next_name {
                    // Check the child node name is a glob pattern or not
                    let (is_glob_pattern, glob_pattern) = parse_glob_pattern(child_name_str);
                    if is_glob_pattern {
                        let Some(glob_pattern) = glob_pattern else {
                            return err_box!("invalid glob pattern: {}", child_name_str);
                        };
                        for child in d.get_child_by_glob_pattern(&glob_pattern) {
                            if matches!(child, Dir(_)) {
                                parent_map.insert(child.id(), curr_node.id());
                            }
                            queue.push_back((
                                curr_index + 1,
                                InodePtr::from_owned(child),
                                curr_node.id(),
                            ));
                        }
                    } else if let Some(child) =
                        d.children_handle().read_child_view(child_name_str).0
                    {
                        if matches!(child, Dir(_)) {
                            parent_map.insert(child.id(), curr_node.id());
                        }
                        queue.push_back((
                            curr_index + 1,
                            InodePtr::from_owned(child),
                            curr_node.id(),
                        ));
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn is_root(&self) -> bool {
        self.components.len() <= 1
    }

    // If all inodes on the path already exist, then return true.
    pub fn is_full(&self) -> bool {
        self.components.len() == self.inodes.len()
    }

    // Get the path name.
    pub fn name(&self) -> &str {
        &self.name
    }

    // Get the full full path.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn child_path(&self, child: impl AsRef<str>) -> String {
        if self.is_root() {
            format!("/{}", child.as_ref())
        } else {
            format!("{}{}{}", self.path, PATH_SEPARATOR, child.as_ref())
        }
    }

    pub fn get_components(&self) -> &Vec<String> {
        &self.components
    }

    pub fn get_path(&self, index: usize) -> String {
        if index > self.components.len() {
            return "".to_string();
        }

        self.components[..index].join(PATH_SEPARATOR)
    }

    // Get the previous directory name.
    pub fn get_parent_path(&self) -> String {
        self.get_path(self.components.len() - 1)
    }

    // Get the parent path that already exists on the path, not target path.
    pub fn get_valid_parent_path(&self) -> String {
        self.get_path(self.existing_len())
    }

    pub fn get_component(&self, pos: usize) -> CommonResult<&'_ str> {
        match self.components.get(pos) {
            None => err_box!("Path does not exist"),
            Some(v) => Ok(v),
        }
    }

    /// Component name at `pos` with the same negative-index semantics as
    /// [`Self::get_dir_node`].
    pub(crate) fn get_component_neg(&self, pos: i32) -> CommonResult<&'_ str> {
        let pos = if pos < 0 {
            (self.components.len() as i32 + pos) as usize
        } else {
            pos as usize
        };
        self.get_component(pos)
    }

    pub fn get_inodes(&self) -> &[InodeEntry] {
        &self.inodes
    }

    // Get the last node that already exists on the path.
    // Directory elements project to a detached persistence DTO; live
    // directory access goes through [`Self::get_last_dir_node`].
    pub fn get_last_inode(&self) -> Option<InodePtr> {
        self.get_inode(-1)
    }

    // Convert the last node to InodeDir
    pub fn clone_last_dir(&self) -> CommonResult<InodeDir> {
        if let Some(v) = self.get_inode((self.inodes.len() - 1) as i32) {
            Ok(v.as_dir_ref()?.clone())
        } else {
            err_box!("status error: {}", self.path)
        }
    }

    // Convert the last node to InodeDir
    pub fn clone_last_file(&self) -> CommonResult<InodeFile> {
        if let Some(v) = self.get_last_inode() {
            Ok(v.as_file_ref()?.clone())
        } else {
            err_box!("status error")
        }
    }

    /// Get the inode that already exists in the path
    /// If it is a positive number, it indicates the start position; if it is a negative number, it indicates the start from the end.
    pub fn get_inode(&self, pos: i32) -> Option<InodePtr> {
        let pos = if pos < 0 {
            (self.components.len() as i32 + pos) as usize
        } else {
            pos as usize
        };

        if pos >= self.inodes.len() {
            return None;
        }
        match &self.inodes[pos] {
            InodeEntry::View(ptr) => Some(ptr.clone()),
            // Snapshot DTO: the projection detaches children and attribute
            // state from the live node, so writes on it never reach the tree.
            InodeEntry::Dir { name, node } => Some(InodePtr::from_owned(
                InodeDir::from_dir_node_live(name.clone(), node.clone()),
            )),
        }
    }

    /// Live directory handle at `pos` (negative counts from the end).
    /// Files and DTOs return `None`; all live-directory writes go through
    /// this handle, never through a projected `InodePtr`.
    pub fn get_dir_node(&self, pos: i32) -> Option<Arc<DirNode>> {
        let pos = if pos < 0 {
            (self.components.len() as i32 + pos) as usize
        } else {
            pos as usize
        };
        match self.inodes.get(pos) {
            Some(InodeEntry::Dir { node, .. }) => Some(node.clone()),
            _ => None,
        }
    }

    pub fn get_last_dir_node(&self) -> Option<Arc<DirNode>> {
        self.get_dir_node(-1)
    }

    pub fn len(&self) -> usize {
        self.components.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn existing_len(&self) -> usize {
        self.inodes.len()
    }

    pub fn append(&mut self, inode: InodeEntry) -> CommonResult<()> {
        if self.components.len() == self.inodes.len() {
            return err_box!(
                "Path {} is The path is complete, appending nodes is not allowed",
                self.path
            );
        }

        match self.get_component(self.inodes.len()) {
            Ok(n) if n == inode_name(&inode) => (),
            _ => return err_box!("data status  {:?}", self),
        }

        self.inodes.push(inode);
        Ok(())
    }

    // Determine whether it is an empty directory.
    pub fn is_empty_dir(&self) -> bool {
        match self.get_last_dir_node() {
            Some(node) => node.children.iter_len() == 0,
            None => self.get_last_inode().is_none_or(|v| v.child_len() == 0),
        }
    }

    // Return the last inode only if the path was fully resolved.
    pub fn task_last(mut self) -> Option<InodeEntry> {
        if self.inodes.len() == self.components.len() {
            self.inodes.pop()
        } else {
            None
        }
    }
}

fn inode_name(entry: &InodeEntry) -> &str {
    entry.name()
}

impl fmt::Debug for InodePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InodePath")
            .field("path", &self.path)
            .field("name", &self.name)
            .field("components", &self.components)
            .field("inodes", &self.inodes)
            .field("store", &"<InodeStore>")
            .finish()
    }
}
