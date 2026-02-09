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
use crate::master::meta::inode::InodeView::{self, Container, Dir, File, FileEntry};
use crate::master::meta::inode::{
    InodeContainer, InodeDir, InodeFile, InodePtr, EMPTY_PARENT_ID, PATH_SEPARATOR, ROOT_INODE_ID,
};
use crate::master::meta::store::InodeStore;
use orpc::{err_box, try_option, CommonResult};
use std::collections::{HashMap, VecDeque};
use std::fmt;

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
        println!(
            "DEBUG at InodePath,at resolve, components: {:?}",
            components
        );
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
            println!("DEBUG at InodePath, at resolve, inodes {:?}", inodes);
            let resolved_inode = match cur_inode.as_ref() {
                FileEntry(name, id) => {
                    println!("DEBUG at InodePath, at resolve, resolve inode {:?} with id {:?} is file_entry", name, id);
                    // If it is a FileEntry, load the complete object from store
                    match store.get_inode(*id, Some(name))? {
                        Some(full_inode) => InodePtr::from_owned(full_inode),
                        None => return err_box!("Failed to load inode {} from store", id),
                    }
                }
                Container(name, id) => {
                    println!("DEBUG at InodePath, at resolve, resolve inode {:?} with id {:?} is container", name, id);
                    cur_inode.clone()
                }
                _ => cur_inode.clone(),
            };

            println!(
                "DEBUG at InodePath, at resolve, resolved_inode: {:?}",
                resolved_inode
            );
            inodes.push(resolved_inode);
            println!(
                "DEBUG at InodePath, at resolve ,after append inode, inodes: {:?}",
                inodes
            );

            if index == components.len() - 1 {
                break;
            }

            index += 1;
            let child_name: &str = components[index].as_str();
            println!(
                "DEBUG at InodePath,at resolve, at index: {:?}, child_name: {:?}",
                index, child_name
            );
            match cur_inode.as_mut() {
                Dir(_, d) => {
                    // First attempt: search without container lookup
                    if let Some(child) = d.get_child_ptr(child_name, false) {
                        cur_inode = child;
                        println!("DEBUG at InodePath,at resolve, child of child name, search only file branch, {:?} is directory: {:?}", child_name, cur_inode);
                    } else {
                        // Second attempt: search in containers
                        if let Some(child) = d.get_child_ptr(child_name, true) {
                            cur_inode = child;
                            println!("DEBUG at InodePath,at resolve, child of child name, search file in container branch, {:?} is directory: {:?}", child_name, cur_inode);
                        } else {
                            // The directory has not been created, so there is no need to search again.
                            println!("DEBUG at InodePath,at resolve, The directory has not been created, so there is no need to search again.");
                            break;
                        }
                    }
                }

                File(_, _) | FileEntry(_, _) | Container(_, _) => {
                    println!("DEBUG at InodePath,at resolve, child name is file entry");
                    // File or FileEntry nodes cannot have children, stop path resolution
                    break;
                }
            }
        }

        println!("DEBUG at InodePath, at resolve, name: {:?}", name);
        let inode_path = Self {
            path: path.as_ref().to_string(),
            name: name.to_string(),
            components,
            inodes,
        };
        println!("DEBUG at InodePath, at resolve, done. {:?}", inode_path);
        Ok(inode_path)
    }

    fn reconstruct_path_for_match(
        curr_index: i64,
        curr_node: &InodePtr,
        components_length: i64,
        parent_map: &HashMap<i64, (i64, i64)>,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        let mut path_inodes_rebuild = Vec::new();
        let mut idx = curr_index;
        let mut current_id = curr_node.as_ref().id();

        // Leaf
        match curr_node.as_ref() {
            File(..) | Dir(..) => path_inodes_rebuild.push(curr_node.clone()),
            FileEntry(name, id) => {
                let resolved_leaf_node = match store.get_inode(*id, Some(name))? {
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
            Container(_, _) => {} // will update later
        }

        // Parents
        while idx != 0 {
            if let Some((parent_idx, parent_id)) = parent_map.get(&current_id) {
                if *parent_id == EMPTY_PARENT_ID {
                    // Reached the root
                    break;
                }

                let resolved_parent = match store.get_inode(*parent_id, None)? {
                    Some(full_inode) => InodePtr::from_owned(full_inode),
                    None => {
                        return err_box!("Failed to load parent inode {} from store", parent_id)
                    }
                };

                path_inodes_rebuild.push(resolved_parent);
                idx = *parent_idx;
                current_id = *parent_id;
            } else {
                return err_box!("No parent found for inode {}", current_id);
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
        let components = InodeView::path_components(pattern)?;
        let components_length: i64 = components.len() as i64;
        let mut results = Vec::new();
        let mut queue: VecDeque<(i64, InodePtr)> = VecDeque::new(); // index + node!

        // Parent map: current inode id -> (index, parent inode id) for path reconstruction
        let mut parent_map: HashMap<i64, (i64, i64)> = HashMap::new();
        parent_map.insert(ROOT_INODE_ID, (0, EMPTY_PARENT_ID)); // Root has no parent
        queue.push_back((0, root)); // Start BFS

        while let Some((curr_index, curr_node)) = queue.pop_front() {
            if curr_index == components_length - 1 {
                let inode_path_entry = Self::reconstruct_path_for_match(
                    curr_index,
                    &curr_node,
                    components_length,
                    &parent_map,
                    store,
                )?;
                results.push(inode_path_entry);
                continue;
            }

            // Expand to next level
            if let Dir(_, d) = curr_node.as_mut() {
                let next_name = components.get(curr_index as usize + 1).map(|s| s.as_str());
                if let Some(child_name_str) = next_name {
                    // Check the child node name is a glob pattern or not
                    let (is_glob_pattern, glob_pattern) = parse_glob_pattern(child_name_str);
                    if is_glob_pattern {
                        if let Some(children) =
                            d.get_child_ptr_by_glob_pattern(&glob_pattern.unwrap())
                        {
                            for child_ptr in children.iter() {
                                parent_map.insert(
                                    child_ptr.as_ref().id(),
                                    (curr_index + 1, curr_node.id()),
                                );
                                queue.push_back((curr_index + 1, child_ptr.clone()));
                            }
                        }
                    } else if let Some(child) = d.get_child_ptr(child_name_str, false) {
                        parent_map.insert(child.id(), (curr_index + 1, curr_node.id()));
                        queue.push_back((curr_index + 1, child));
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

    // Convert the last node to InodeFile
    pub fn clone_last_file(&self) -> CommonResult<InodeFile> {
        if let Some(v) = self.get_last_inode() {
            // Assert that lastnode must be File
            assert!(v.is_file());
            Ok(v.as_file_ref()?.clone())
        } else {
            err_box!("status error")
        }
    }

    // Convert the last node to InodeContainer
    pub fn clone_last_container(&self) -> CommonResult<InodeContainer> {
        if let Some(v) = self.get_last_inode() {
            // Assert that lastnode must be Container
            assert!(!v.is_container());
            Ok(v.as_container_ref()?.clone())
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
