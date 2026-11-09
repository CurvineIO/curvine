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
    DirectoryChildrenSharedVersion, InodeDir, InodeFile, InodePtr, EMPTY_PARENT_ID, PATH_SEPARATOR,
};
use crate::master::meta::store::InodeStore;
use curvine_core_error::{err_box, try_option, CommonResult};
use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::fmt;

pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodePtr>,
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

        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut versions = SmallVec::new();
        let mut directory = match root_view.children_handle() {
            Some(handle) => handle,
            None => return err_box!("root inode is not a directory"),
        };

        // Preserve the complete directory view at every resolved level.
        versions.push(directory.shared_version());
        let mut root_view = root_view.clone();
        root_view.change_name(components[0].clone());
        inodes.push(InodePtr::from_owned(root_view));

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
            inodes.push(resolved);
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
        Ok(Self::resolve_inner::<T, false, false>(root, path, store, &[], 0)?
            .expect("unconditional path resolution always returns a path"))
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
        Ok(Self::resolve_inner::<T, true, false>(root, path, store, &[], 0)?
            .expect("unconditional path resolution always returns a path"))
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
        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut versions = SmallVec::new();
        let mut cur_inode = root;
        let mut index = 0;

        while index < components.len() {
            if CHECK_IDS && ids.get(index).is_none_or(|id| cur_inode.id() != *id) {
                return Ok(None);
            }
            if TRACK_VERSIONS {
                if let Some(children) = cur_inode.as_ref().children_handle() {
                    versions.push(children.shared_version());
                }
            }
            let next = if index + 1 < components.len() {
                match cur_inode.as_ref() {
                    Dir(dir) => dir.get_child(&components[index + 1]),
                    _ => None,
                }
            } else {
                None
            };

            let resolved_inode = match cur_inode.as_ref() {
                FileEntry(f) => match store.get_inode(f.id(), Some(f.name()))? {
                    Some(full_inode) => InodePtr::from_owned(full_inode),
                    None => return err_box!("Failed to load inode {} from store", f.id()),
                },
                _ => cur_inode,
            };
            // Preserve ownership when a caller passes an owned root or a
            // child container returns an owned inode.
            inodes.push(resolved_inode);

            match next {
                Some(child) => cur_inode = child,
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
            File(..) | Dir(..) => path_inodes_rebuild.push(curr_node),
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
                path_inodes_rebuild.push(resolved_leaf_node);
            }
        }

        // Parents
        while idx != 0 {
            let resolved_parent = match store.get_inode(parent_id, None)? {
                Some(full_inode) => InodePtr::from_owned(full_inode),
                None => return err_box!("Failed to load parent inode {} from store", parent_id),
            };
            path_inodes_rebuild.push(resolved_parent);
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
            .map(|node| node.as_ref().name().to_string())
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

    /// Resolve all paths matching glob pattern using BFS queue traversal
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

    pub fn get_inodes(&self) -> &Vec<InodePtr> {
        &self.inodes
    }

    // Get the last node that already exists on the path
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

        if pos < self.inodes.len() {
            Some(self.inodes[pos].clone())
        } else {
            None
        }
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

    pub fn append(&mut self, inode: InodePtr) -> CommonResult<()> {
        if self.components.len() == self.inodes.len() {
            return err_box!(
                "Path {} is The path is complete, appending nodes is not allowed",
                self.path
            );
        }

        match self.get_component(self.inodes.len()) {
            Ok(n) if n == inode.name() => (),
            _ => return err_box!("data status  {:?}", self),
        }

        self.inodes.push(inode);
        Ok(())
    }

    // Determine whether it is an empty directory.
    pub fn is_empty_dir(&self) -> bool {
        match self.get_last_inode() {
            Some(v) => v.child_len() == 0,

            _ => true,
        }
    }

    // Return the last inode only if the path was fully resolved.
    pub fn task_last(mut self) -> Option<InodePtr> {
        if self.inodes.len() == self.components.len() {
            self.inodes.pop()
        } else {
            None
        }
    }
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
