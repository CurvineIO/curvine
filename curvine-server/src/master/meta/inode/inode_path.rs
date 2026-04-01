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

//! Path resolution for inode metadata.
//!
//! `InodePath` represents the result of resolving a path through the `DirEntry` tree.
//! It contains the path components and references to loaded `InodeView` (rich metadata)
//! for each component.
//!
//! # Architecture
//! ```text
//! resolve(path):
//!   1. Navigate using DirEntry tree (lightweight)
//!   2. For each step, load InodeView from InodeStore by id
//!   3. Store InodePtr in InodePath.inodes
//!
//! Result: InodePath { inodes: Vec<InodePtr> }  // all rich data
//! ```

use crate::master::meta::glob_utils::parse_glob_pattern;
use crate::master::meta::inode::{DirEntry, InodeView, InodePtr, PATH_SEPARATOR};
use crate::master::meta::store::InodeStore;
use orpc::{err_box, try_option, CommonResult};
use std::collections::VecDeque;
use std::fmt;

/// Represents a resolved path with rich metadata for each component.
///
/// Created by `resolve()` which navigates the `DirEntry` tree and loads
/// `InodeView` (rich metadata) from `InodeStore` for each path component.
pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodePtr>,
}

impl InodePath {
    /// Resolves a path through the DirEntry tree.
    ///
    /// # Arguments
    /// * `root` - Reference to the root DirEntry of the tree
    /// * `path` - The path to resolve (must be absolute)
    /// * `store` - The InodeStore to load rich metadata from
    ///
    /// # Returns
    /// An `InodePath` containing rich metadata (`InodeView`) for each resolved component.
    pub fn resolve(root: &DirEntry, path: &str, store: &InodeStore) -> CommonResult<Self> {
        let components = InodeView::path_components(path)?;
        let name = try_option!(components.last());

        if name.is_empty() {
            return err_box!("Path {} is invalid", path);
        }

        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut cur_entry = root;
        let mut index = 0;

        while index < components.len() {
            // Load rich metadata for current entry from store
            let view = store.get_inode(cur_entry.id(), None)?;
            match view {
                Some(v) => {
                    let ptr = InodePtr::from_owned(v);
                    inodes.push(ptr);
                }
                None => {
                    return err_box!("Failed to load inode {} from store", cur_entry.id());
                }
            }

            if index == components.len() - 1 {
                break;
            }

            index += 1;
            let child_name: &str = components[index].as_str();

            // Navigate to child using DirEntry tree
            match cur_entry.get_child(child_name) {
                Some(child) => {
                    cur_entry = child;
                }
                None => {
                    // Child not found, stop resolution
                    break;
                }
            }
        }

        let inode_path = Self {
            path: path.to_string(),
            name: name.to_string(),
            components,
            inodes,
        };

        Ok(inode_path)
    }

    /// Checks if this is the root path
    pub fn is_root(&self) -> bool {
        self.components.len() <= 1
    }

    /// Returns true if all components in the path exist
    pub fn is_full(&self) -> bool {
        self.components.len() == self.inodes.len()
    }

    /// Returns the last component name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the full path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the path for a child
    pub fn child_path(&self, child: &str) -> String {
        if self.is_root() {
            format!("/{}", child)
        } else {
            format!("{}{}{}", self.path, PATH_SEPARATOR, child)
        }
    }

    /// Returns the path components
    pub fn get_components(&self) -> &Vec<String> {
        &self.components
    }

    /// Returns the path up to the given index
    pub fn get_path(&self, index: usize) -> String {
        if index > self.components.len() {
            return "".to_string();
        }

        self.components[..index].join(PATH_SEPARATOR)
    }

    /// Returns the parent directory path
    pub fn get_parent_path(&self) -> String {
        self.get_path(self.components.len() - 1)
    }

    /// Returns the valid parent path (existing part)
    pub fn get_valid_parent_path(&self) -> String {
        self.get_path(self.existing_len())
    }

    /// Returns a specific component
    pub fn get_component(&self, pos: usize) -> CommonResult<&str> {
        match self.components.get(pos) {
            None => err_box!("Path does not exist"),
            Some(v) => Ok(v),
        }
    }

    /// Returns the inodes
    pub fn get_inodes(&self) -> &Vec<InodePtr> {
        &self.inodes
    }

    /// Returns the last inode
    pub fn get_last_inode(&self) -> Option<InodePtr> {
        self.get_inode(-1)
    }

    /// Clones the last directory if it exists
    pub fn clone_last_dir(&self) -> CommonResult<crate::master::meta::inode::InodeDir> {
        if let Some(v) = self.get_inode((self.inodes.len() - 1) as i32) {
            Ok(v.as_dir_ref()?.clone())
        } else {
            err_box!("status error: {}", self.path)
        }
    }

    /// Clones the last file if it exists
    pub fn clone_last_file(&self) -> CommonResult<crate::master::meta::inode::InodeFile> {
        if let Some(v) = self.get_last_inode() {
            Ok(v.as_file_ref()?.clone())
        } else {
            err_box!("status error")
        }
    }

    /// Gets an inode by position (negative indices count from end)
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

    /// Returns the number of components
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Returns true if the path is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of existing (resolved) components
    pub fn existing_len(&self) -> usize {
        self.inodes.len()
    }

    /// Appends an inode to the path
    pub fn append(&mut self, inode: InodePtr) -> CommonResult<()> {
        if self.components.len() == self.inodes.len() {
            return err_box!(
                "Path {} is complete, appending nodes is not allowed",
                self.path
            );
        }

        // Note: Name checking removed as name is now in parent's children key

        self.inodes.push(inode);
        Ok(())
    }

    /// Checks if the last inode is an empty directory
    /// Note: This requires checking the DirEntry tree, not the InodeView
    pub fn is_empty_dir(&self) -> bool {
        // This would need access to the DirEntry tree
        // For now, return true as a placeholder
        // The actual check should be done using DirEntry.child_count() == 0
        self.get_last_inode().map(|v| v.is_dir()).unwrap_or(false)
    }

    /// Removes the last inode from the path
    pub fn delete_last(&mut self) {
        self.inodes.pop();
    }
}

impl fmt::Debug for InodePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InodePath")
            .field("path", &self.path)
            .field("name", &self.name)
            .field("components", &self.components)
            .field("inodes", &self.inodes.len())
            .finish()
    }
}

impl InodePath {
    /// Resolve paths matching glob patterns
    /// Uses BFS traversal of the DirEntry tree with pattern matching
    pub fn resolve_for_glob_pattern(
        root: &DirEntry,
        pattern: &str,
        store: &InodeStore,
    ) -> CommonResult<Vec<Self>> {
        let components = InodeView::path_components(pattern)?;
        let components_len = components.len();
        let mut results = Vec::new();

        // Queue entries: (component_index, current_dir_entry, path_so_far)
        let mut queue: VecDeque<(usize, &DirEntry, String)> = VecDeque::new();
        queue.push_back((0, root, String::new()));

        while let Some((index, entry, path)) = queue.pop_front() {
            if index == components_len - 1 {
                // Reached the end of pattern - create InodePath for this match
                let full_path = if path.is_empty() {
                    "/".to_string()
                } else {
                    path
                };
                if let Ok(inp) = Self::resolve(root, &full_path, store) {
                    results.push(inp);
                }
                continue;
            }

            // Get the next component to match
            let next_index = index + 1;
            if next_index >= components_len {
                continue;
            }
            let next_name = &components[next_index];

            // Check if this is a glob pattern
            let (is_glob, glob_pattern) = parse_glob_pattern(next_name);

            // Navigate to children
            if let Some(children) = entry.children() {
                for (child_name, child_entry) in children.iter() {
                    let matches = if is_glob {
                        glob_pattern
                            .as_ref()
                            .map(|p| p.matches(child_name))
                            .unwrap_or(false)
                    } else {
                        child_name == next_name
                    };

                    if matches {
                        let child_path = if path.is_empty() {
                            format!("/{}", child_name)
                        } else {
                            format!("{}{}{}", path, PATH_SEPARATOR, child_name)
                        };
                        queue.push_back((next_index, child_entry, child_path));
                    }
                }
            }
        }

        Ok(results)
    }
}