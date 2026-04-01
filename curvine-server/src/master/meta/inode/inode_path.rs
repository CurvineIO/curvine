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
use crate::master::meta::inode::{DirEntry, DirEntryRef, InodeView, InodePtr, PATH_SEPARATOR};
use crate::master::meta::store::InodeStore;
use orpc::{err_box, try_option, CommonResult};
use std::collections::VecDeque;
use std::fmt;

pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodePtr>,
    entries: Vec<DirEntryRef>,
}

impl InodePath {
    pub fn resolve(root: &DirEntry, path: &str, store: &InodeStore) -> CommonResult<Self> {
        let components = InodeView::path_components(path)?;
        let name = try_option!(components.last());

        if name.is_empty() {
            return err_box!("Path {} is invalid", path);
        }

        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut entries: Vec<DirEntryRef> = Vec::with_capacity(components.len());
        let mut cur_entry = root;
        let mut index = 0;

        while index < components.len() {
            entries.push(DirEntryRef::from_ref(cur_entry));

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

            match cur_entry.get_child(child_name) {
                Some(child) => {
                    cur_entry = child;
                }
                None => {
                    break;
                }
            }
        }

        let inode_path = Self {
            path: path.to_string(),
            name: name.to_string(),
            components,
            inodes,
            entries,
        };

        Ok(inode_path)
    }

    pub fn is_root(&self) -> bool {
        self.components.len() <= 1
    }

    pub fn is_full(&self) -> bool {
        self.components.len() == self.inodes.len()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn child_path(&self, child: &str) -> String {
        if self.is_root() {
            format!("/{}", child)
        } else {
            format!("{}{}{}", self.path, PATH_SEPARATOR, child)
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

    pub fn get_parent_path(&self) -> String {
        self.get_path(self.components.len() - 1)
    }

    pub fn get_valid_parent_path(&self) -> String {
        self.get_path(self.existing_len())
    }

    pub fn get_component(&self, pos: usize) -> CommonResult<&str> {
        match self.components.get(pos) {
            None => err_box!("Path does not exist"),
            Some(v) => Ok(v),
        }
    }

    pub fn get_inodes(&self) -> &Vec<InodePtr> {
        &self.inodes
    }

    pub fn get_last_inode(&self) -> Option<InodePtr> {
        self.get_inode(-1)
    }

    pub fn clone_last_dir(&self) -> CommonResult<crate::master::meta::inode::InodeDir> {
        if let Some(v) = self.get_inode((self.inodes.len() - 1) as i32) {
            Ok(v.as_dir_ref()?.clone())
        } else {
            err_box!("status error: {}", self.path)
        }
    }

    pub fn clone_last_file(&self) -> CommonResult<crate::master::meta::inode::InodeFile> {
        if let Some(v) = self.get_last_inode() {
            Ok(v.as_file_ref()?.clone())
        } else {
            err_box!("status error")
        }
    }

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
                "Path {} is complete, appending nodes is not allowed",
                self.path
            );
        }

        self.inodes.push(inode);
        Ok(())
    }

    pub fn append_with_entry(&mut self, inode: InodePtr, entry: DirEntryRef) -> CommonResult<()> {
        if self.components.len() == self.inodes.len() {
            return err_box!(
                "Path {} is complete, appending nodes is not allowed",
                self.path
            );
        }

        self.inodes.push(inode);
        self.entries.push(entry);
        Ok(())
    }

    pub fn is_empty_dir(&self) -> bool {
        self.get_last_inode().map(|v| v.is_dir()).unwrap_or(false)
    }

    pub fn delete_last(&mut self) {
        self.inodes.pop();
        self.entries.pop();
    }

    pub fn get_last_entry(&self) -> Option<&DirEntryRef> {
        self.entries.last()
    }

    pub fn get_parent_entry(&self) -> Option<&DirEntryRef> {
        let len = self.entries.len();
        if len >= 1 && self.existing_len() < self.len() {
            Some(&self.entries[len - 1])
        } else if len >= 2 {
            Some(&self.entries[len - 2])
        } else {
            None
        }
    }

    pub fn get_last_entry_mut(&mut self) -> Option<&mut DirEntryRef> {
        self.entries.last_mut()
    }

    pub fn get_parent_entry_mut(&mut self) -> Option<&mut DirEntryRef> {
        let len = self.entries.len();
        if len >= 2 {
            Some(&mut self.entries[len - 2])
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
            .field("inodes", &self.inodes.len())
            .finish()
    }
}

impl InodePath {
    pub fn resolve_for_glob_pattern(
        root: &DirEntry,
        pattern: &str,
        store: &InodeStore,
    ) -> CommonResult<Vec<Self>> {
        let components = InodeView::path_components(pattern)?;
        let components_len = components.len();
        let mut results = Vec::new();

        let mut queue: VecDeque<(usize, &DirEntry, String)> = VecDeque::new();
        queue.push_back((0, root, String::new()));

        while let Some((index, entry, path)) = queue.pop_front() {
            if index == components_len - 1 {
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

            let next_index = index + 1;
            if next_index >= components_len {
                continue;
            }
            let next_name = &components[next_index];

            let (is_glob, glob_pattern) = parse_glob_pattern(next_name);

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