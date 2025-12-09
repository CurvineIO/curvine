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

use crate::master::meta::inode::InodeView::{self, Dir, File, FileEntry};
use crate::master::meta::inode::{InodeDir, InodeFile, InodePtr, PATH_SEPARATOR};
use crate::master::meta::store::InodeStore;
use orpc::{err_box, try_option, CommonResult};
use std::collections::{VecDeque, HashMap};
use std::hash::{Hash, Hasher};
use std::fmt;
use crate::master::meta::glob_utils::parse_glob_pattern;

#[derive(Clone)]
pub struct HashableInodePtr(pub InodePtr);  // Wraps your RawPtr<InodeView>

impl PartialEq for HashableInodePtr {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0.as_ptr(), other.0.as_ptr())
    }
}

impl Eq for HashableInodePtr {}

impl Hash for HashableInodePtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ptr().hash(state);
    }
}

impl std::ops::Deref for HashableInodePtr {
    type Target = InodePtr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<InodePtr> for HashableInodePtr {
    fn from(ptr: InodePtr) -> Self {
        HashableInodePtr(ptr)
    }
}

#[derive(Clone)]
pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodePtr>,
}

impl InodePath {
    pub fn resolve<T: AsRef<str>>(
        root: InodePtr,
        path: T,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        let components = InodeView::path_components(path.as_ref())?;
        let name = try_option!(components.last());

        if name.is_empty() {
            return err_box!("Path {} is invalid", path.as_ref());
        }

        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut cur_inode = root;
        let mut index = 0;

        while index < components.len() {
            //make sure resolved_inode is not a FileEntry
            //if it is a FileEntry, load the complete file data from store
            let resolved_inode = match cur_inode.as_ref() {
                FileEntry(name, id) => {
                    // If it is a FileEntry, load the complete object from store
                    match store.get_inode(*id, Some(name))? {
                        Some(full_inode) => InodePtr::from_owned(full_inode),
                        None => return err_box!("Failed to load inode {} from store", id),
                    }
                }
                _ => cur_inode.clone(),
            };

            inodes.push(resolved_inode);

            if index == components.len() - 1 {
                break;
            }

            index += 1;
            let child_name: &str = components[index].as_str();
            match cur_inode.as_mut() {
                Dir(_, d) => {
                    if let Some(child) = d.get_child_ptr(child_name) {
                        cur_inode = child;
                    } else {
                        // The directory has not been created, so there is no need to search again.
                        break;
                    }
                }

                File(_, _) | FileEntry(_, _) => {
                    // File or FileEntry nodes cannot have children, stop path resolution
                    break;
                }
            }
        }

        let inode_path = Self {
            path: path.as_ref().to_string(),
            name: name.to_string(),
            components,
            inodes,
        };

        Ok(inode_path)
    }

    /// Resolve all paths matching glob pattern using BFS queue traversal
    pub fn resolve_for_glob_pattern(root: InodePtr, pattern: &str, store: &InodeStore) -> CommonResult<Vec<Self>> {
        let components = InodeView::path_components(pattern)?;
        let components_length = components.len();
        let mut results = Vec::new();
        let mut queue: VecDeque<(usize, InodePtr)> = VecDeque::new();  // Just index + node!
        let root_hashable = HashableInodePtr::from(root.clone());
        // Parent map: hash node -> (index, parent) for path reconstruction
        let mut parent_map: HashMap<HashableInodePtr, (usize, Option<InodePtr>)> = HashMap::new();
        parent_map.insert(root_hashable, (0, None)); // Root has no parent
        
        queue.push_back((0, root));  // Start BFS
        
        while let Some((curr_index, curr_node)) = queue.pop_front() {            
            // Resolve current node
            let resolved_node = match &curr_node.as_ref() {
                FileEntry(name, id) => {
                    match store.get_inode(*id, Some(name))? {
                        Some(full_inode) => InodePtr::from_owned(full_inode.clone()),
                        None => return err_box!("Failed to load inode {} from store", id),
                    }
                }
                _ => curr_node,
            };
                        
            if curr_index == components_length - 1 {
                // Reconstruct path
                let mut path_inodes = Vec::new();
                let mut current = resolved_node;
                let mut idx = curr_index;
                
                while idx != 0 {
                    path_inodes.push(current.clone());
                    if let Some((parent_idx, parent)) = parent_map.get(&HashableInodePtr(current.clone())) {
                        current = parent.clone().unwrap();
                        idx = *parent_idx - 1;
                    } else {
                        break;
                    }
                }
                path_inodes.reverse();
                
                let components_result: Vec<String> = path_inodes.iter()
                    .map(|node| node.as_ref().name().to_string()).collect();
                let path_str = components_result.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("/");
                
                results.push(Self {
                    path: path_str,
                    name: components_result.last().cloned().unwrap_or_default(),
                    components: components_result,
                    inodes: path_inodes,
                });
                continue;
            }
            
            // Expand to next level
            if let Dir(_, d) = resolved_node.as_mut() {
                let next_name = components.get(curr_index + 1).map(|s| s.as_str());
                if let Some(child_name_str) = next_name {
                    let (is_glob_pattern, glob_pattern) = parse_glob_pattern(child_name_str);
                    println!("is_glob_pattern: {:?}, glob_pattern: {:?}", is_glob_pattern, glob_pattern);
                    if is_glob_pattern {
                        if let Some(children) = d.get_child_ptr_by_glob_pattern(&glob_pattern.unwrap()) {
                            for child_ptr in children.iter() {
                                let child = child_ptr.clone();
                                let child_hashable = HashableInodePtr(child.clone());
                                parent_map.insert(child_hashable, (curr_index + 1, Some(resolved_node.clone())));
                                queue.push_back((curr_index + 1, child));
                            }
                        }
                    } else if let Some(child) = d.get_child_ptr(child_name_str) {
                        let child_hashable = HashableInodePtr(child.clone());
                        parent_map.insert(child_hashable, (curr_index + 1, Some(resolved_node.clone())));
                        queue.push_back((curr_index + 1, child));
                    }
                }
            }
        }
        println!("results: {:?}", results);
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
            // Assert that lastnode should not be FileEntry
            assert!(!v.is_file_entry());
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

    // Delete the last 1 inodes.
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
            .field("inodes", &self.inodes)
            .field("store", &"<InodeStore>")
            .finish()
    }
}
