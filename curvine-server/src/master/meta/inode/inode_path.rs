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

use crate::master::meta::inode::InodeView::{self, Dir, File};
use crate::master::meta::inode::{InodeDir, InodeFile, InodePtr, PATH_SEPARATOR};
use orpc::{err_box, try_option, CommonResult};

#[derive(Debug, Clone)]
pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodePtr>,
}

impl InodePath {
    pub fn resolve<T: AsRef<str>>(root: InodePtr, path: T) -> CommonResult<Self> {
        let components = InodeView::path_components(path.as_ref())?;

        let name = try_option!(components.last());
        if name.is_empty() {
            return err_box!("Path {} is invalid", path.as_ref());
        }

        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut cur_inode = root;
        let mut index = 0;

        while index < components.len() {
            inodes.push(cur_inode.clone());

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

                File(_, _) => {
                    // The current path is a file, there is no need to search again.
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

    // Delete the last 1 inodes.
    pub fn delete_last(&mut self) {
        self.inodes.pop();
    }
}
