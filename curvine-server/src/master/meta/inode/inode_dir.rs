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

use crate::master::meta::feature::{AclFeature, DirFeature};
use crate::master::meta::inode::inodes_children::InodeChildren;
use crate::master::meta::inode::InodeView::{Dir, File};
use crate::master::meta::inode::{
    ChildrenIter, Inode, InodeFile, InodePtr, InodeView, EMPTY_PARENT_ID,
};
use curvine_common::state::{MkdirOpts, StoragePolicy};
use glob::Pattern;
use orpc::CommonResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeDir {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,

    pub(crate) features: DirFeature,
    
    pub(crate) container_index: HashMap::<String, String>,
    #[serde(skip)]
    children: InodeChildren,
}

impl InodeDir {
    pub fn new(id: i64, time: i64) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: Default::default(),
            features: Default::default(),
            container_index: HashMap::new(),
            children: InodeChildren::new_map(),
        }
    }
    
    // pub fn remove_from_container_index(&mut self, file_name: &str) {  
    //     self.children.remove_from_container_index(file_name);  
    // }  
      
    // pub fn add_to_container_index(&mut self, file_name: String, container_name: String) {  
    //     self.children.add_to_container_index(file_name, container_name);  
    // } 
    

   pub fn remove_from_container_index(&mut self, file_name: &str) -> Option<String> {  
        self.container_index.remove(file_name)  
    }

    pub fn add_to_container_index(&mut self, file_name: String, container_name: String) {  
        self.container_index.insert(file_name, container_name);  
    } 
    pub fn get_container_for_file(&self, file_name: &str) -> Option<&String> {  
        self.container_index.get(file_name)  
    } 
    pub fn with_opts(id: i64, time: i64, opts: MkdirOpts) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: opts.storage_policy,
            features: DirFeature {
                acl: AclFeature {
                    mode: opts.mode,
                    owner: opts.owner,
                    group: opts.group,
                },
                x_attr: opts.x_attr,
            },
            container_index: HashMap::new(),
            children: InodeChildren::new_map(),
        }
    }

    /// Add a word node and return a reference to that word node.
    pub fn add_child(&mut self, mut inode: InodeView) -> CommonResult<InodePtr> {
        inode.set_parent_id(self.id);
        self.children.add_child(inode)
    }

    /// Get the child node of the specified inode name.
    pub fn get_child(&self, name: &str) -> Option<&InodeView> {
        self.children.get_child(name)
    }

    pub fn get_child_ptr(
        &mut self,
        name: &str,
        is_enable_container_search: bool,
    ) -> Option<InodePtr> {
        self.children
            .get_child_ptr(name, is_enable_container_search)
    }

    pub fn get_child_ptr_by_glob_pattern(
        &mut self,
        glob_pattern: &Pattern,
    ) -> Option<Vec<InodePtr>> {
        self.children.get_child_ptr_by_glob_pattern(glob_pattern)
    }

    pub fn update_mtime(&mut self, time: i64) {
        if time > self.mtime {
            self.mtime = time
        }
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        self.children.delete_child(child_id, child_name)
    }

    pub fn print_child(&self) {
        for child in self.children.iter() {
            let t = if child.is_dir() { "dir" } else { "file" };

            println!("{} {} {}", t, child.id(), child.name());
        }
    }

    pub fn children_iter(&self) -> ChildrenIter<'_> {
        self.children.iter()
    }

    pub fn children_vec(&self) -> Vec<InodeView> {
        self.children.iter().cloned().collect()
    }

    pub fn children_len(&self) -> usize {
        self.children.len()
    }

    pub fn add_file_child(&mut self, name: &str, file: InodeFile) -> CommonResult<InodePtr> {
        self.add_child(File(name.to_string(), file))
    }

    pub fn add_dir_child(&mut self, name: &str, dir: InodeDir) -> CommonResult<InodePtr> {
        self.add_child(Dir(name.to_string(), dir))
    }
}

impl Inode for InodeDir {
    fn id(&self) -> i64 {
        self.id
    }

    fn parent_id(&self) -> i64 {
        self.parent_id
    }

    fn is_dir(&self) -> bool {
        true
    }

    fn nlink(&self) -> u32 {
        self.nlink
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }
}

impl PartialEq for InodeDir {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
