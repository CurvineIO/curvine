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
use crate::master::meta::inode::{DirTree, EntryKey, InodePtr, InodeView, PATH_SEPARATOR};
use crate::master::meta::store::InodeStore;
use orpc::{err_box, try_option, CommonResult};
use std::fmt;

/// Holds the rich inode materialized for the current request.
///
/// Directories come from the in-memory dir tree, while the terminal file inode
/// may be loaded from the store. Callers should keep using this object for the
/// rest of the request instead of resolving the same entity again by path.
pub struct ResolvedInode {
    pub inode: InodePtr,
    pub entry_key: Option<EntryKey>,
}

impl ResolvedInode {
    pub fn new(inode: InodePtr, entry_key: Option<EntryKey>) -> Self {
        Self { inode, entry_key }
    }
}

/// Request-scoped rich view for a resolved path.
///
/// `InodePath` is not only a path shell. Once `resolve()` succeeds, every
/// resolved component in `resolved` becomes the authoritative rich entity view
/// for this request. Helpers that already hold `&InodePath` should derive file
/// status and block metadata from it instead of reloading the same inode.
pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    resolved: Vec<ResolvedInode>,
}

impl InodePath {
    pub fn resolve(tree: &DirTree, path: &str, store: &InodeStore) -> CommonResult<Self> {
        let components = InodeView::path_components(path)?;
        let name = try_option!(components.last());

        if name.is_empty() {
            return err_box!("Path {} is invalid", path);
        }

        let mut resolved: Vec<ResolvedInode> = Vec::with_capacity(components.len());
        let mut cur_key = tree.root_key();
        let mut index = 0;

        while index < components.len() {
            let cur_entry = tree.entry(cur_key)?;
            let view = InodeView::new_dir(cur_entry.as_dir().clone());
            let inode_ptr = InodePtr::from_owned(view);
            resolved.push(ResolvedInode::new(inode_ptr, Some(cur_key)));

            if index == components.len() - 1 {
                break;
            }

            index += 1;
            let child_name: &str = components[index].as_str();

            if let Some(child_key) = cur_entry.get_child(child_name) {
                cur_key = child_key;
                continue;
            }

            // The directory tree stays id-only. When the leaf is a file, we
            // materialize a rich inode once and keep using it within this
            // request.
            if let Some(child_inode) = store.lookup_child(cur_entry.id(), child_name)? {
                let child_ptr = InodePtr::from_owned(child_inode);
                resolved.push(ResolvedInode::new(child_ptr, None));
            }
            break;
        }

        let inode_path = Self {
            path: path.to_string(),
            name: name.to_string(),
            components,
            resolved,
        };

        Ok(inode_path)
    }

    pub fn is_root(&self) -> bool {
        self.components.len() <= 1
    }

    pub fn is_full(&self) -> bool {
        self.components.len() == self.resolved.len()
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

    pub fn get_inodes(&self) -> Vec<InodePtr> {
        self.resolved.iter().map(|r| r.inode.clone()).collect()
    }

    pub fn get_last_inode(&self) -> Option<InodePtr> {
        self.get_inode(-1)
    }

    pub fn clone_last_dir(&self) -> CommonResult<crate::master::meta::inode::InodeDir> {
        if let Some(v) = self.get_inode((self.resolved.len() - 1) as i32) {
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

        self.resolved.get(pos).map(|r| r.inode.clone())
    }

    pub fn len(&self) -> usize {
        self.components.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn existing_len(&self) -> usize {
        self.resolved.len()
    }

    pub fn append(&mut self, inode: InodePtr, entry_key: Option<EntryKey>) -> CommonResult<()> {
        if self.components.len() == self.resolved.len() {
            return err_box!(
                "Path {} is complete, appending nodes is not allowed",
                self.path
            );
        }

        self.resolved.push(ResolvedInode::new(inode, entry_key));
        Ok(())
    }

    pub fn is_empty_dir(&self) -> bool {
        self.get_last_inode().map(|v| v.is_dir()).unwrap_or(false)
    }

    pub fn delete_last(&mut self) {
        self.resolved.pop();
    }

    pub fn get_last_entry_key(&self) -> Option<EntryKey> {
        self.resolved.last().and_then(|r| r.entry_key)
    }

    pub fn get_parent_entry_key(&self) -> Option<EntryKey> {
        let len = self.resolved.len();
        if len >= 1 && self.existing_len() < self.len() {
            self.resolved[len - 1].entry_key
        } else if len >= 2 {
            self.resolved[len - 2].entry_key
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
            .field("resolved", &self.resolved.len())
            .finish()
    }
}

impl InodePath {
    pub fn resolve_for_glob_pattern(
        tree: &DirTree,
        pattern: &str,
        store: &InodeStore,
    ) -> CommonResult<Vec<Self>> {
        let components = InodeView::path_components(pattern)?;
        let components_len = components.len();
        let mut results = Vec::new();

        let mut stack: Vec<(String, usize)> = vec![(String::new(), 0)];
        while let Some((path, index)) = stack.pop() {
            if index == components_len - 1 {
                let full_path = if path.is_empty() {
                    "/".to_string()
                } else {
                    path
                };
                if let Ok(inp) = Self::resolve(tree, &full_path, store) {
                    results.push(inp);
                }
                continue;
            }

            let full_path = if path.is_empty() {
                "/".to_string()
            } else {
                path.clone()
            };
            let inp = match Self::resolve(tree, &full_path, store) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let parent = match inp.get_last_inode() {
                Some(v) if v.is_dir() => v,
                _ => continue,
            };
            let next_name = &components[index + 1];
            let (is_glob, glob_pattern) = parse_glob_pattern(next_name);

            for (child_name, _child_id) in store.list_children(parent.id())? {
                let matches = if is_glob {
                    glob_pattern
                        .as_ref()
                        .map(|p| p.matches(&child_name))
                        .unwrap_or(false)
                } else {
                    child_name == *next_name
                };
                if matches {
                    let child_path = if path.is_empty() {
                        format!("/{}", child_name)
                    } else {
                        format!("{}{}{}", path, PATH_SEPARATOR, child_name)
                    };
                    stack.push((child_path, index + 1));
                }
            }
        }

        Ok(results)
    }
}
