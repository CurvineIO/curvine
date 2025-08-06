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

#![allow(clippy::large_enum_variant)]

use crate::master::meta::feature::AclFeature;
use crate::master::meta::inode::ttl_types::TtlConfig;
use crate::master::meta::inode::InodeView::{Dir, File};
use crate::master::meta::inode::{
    Inode, InodeDir, InodeFile, InodePtr, PATH_SEPARATOR, ROOT_INODE_ID,
};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts, StoragePolicy};
use curvine_common::utils::SerdeUtils;
use orpc::common::Utils;
use orpc::{err_box, CommonResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, LinkedList};
use std::fmt::{Debug, Formatter};

#[derive(Serialize, Deserialize)]
#[repr(i8)]
pub enum InodeView {
    File(InodeFile) = 1,
    Dir(InodeDir) = 2,
}

impl InodeView {
    pub fn is_dir(&self) -> bool {
        match self {
            File(_) => false,
            Dir(_) => true,
        }
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    pub fn is_link(&self) -> bool {
        matches!(self, File(v) if v.file_type == FileType::Link)
    }

    pub fn id(&self) -> i64 {
        match self {
            File(f) => f.id(),
            Dir(d) => d.id(),
        }
    }
    pub fn ttl_config(&self) -> Option<TtlConfig> {
        match self {
            File(f) => TtlConfig::from_storage_policy(&f.storage_policy),
            Dir(d) => TtlConfig::from_storage_policy(&d.storage_policy),
        }
    }
    pub fn name(&self) -> &str {
        match self {
            File(f) => f.name(),
            Dir(d) => d.name(),
        }
    }

    pub fn change_name(&mut self, name: &str) {
        match self {
            File(f) => f.name = name.to_string(),
            Dir(d) => d.name = name.to_string(),
        }
    }

    pub fn path_components(path: &str) -> CommonResult<Vec<String>> {
        if !Self::is_full_path(path) {
            return err_box!("Absolute path required, but got {}", path);
        }

        if path == PATH_SEPARATOR {
            Ok(vec![PATH_SEPARATOR.to_string()])
        } else {
            let components: Vec<String> = path.split(PATH_SEPARATOR).map(String::from).collect();

            if components.is_empty() {
                return err_box!("Path parsing failed: {}", path);
            }

            Ok(components)
        }
    }

    pub fn is_full_path(path: &str) -> bool {
        path.starts_with(PATH_SEPARATOR)
    }

    pub fn get_child(&self, name: &str) -> Option<&InodeView> {
        match self {
            File(_) => None,
            Dir(d) => d.get_child(name),
        }
    }

    pub fn get_child_ptr(&mut self, name: &str) -> Option<InodePtr> {
        match self {
            File(_) => None,
            Dir(d) => d.get_child_ptr(name),
        }
    }

    // Test and print for use. Memory copying occurs.
    pub fn children(&self) -> Vec<&InodeView> {
        let mut vec = Vec::with_capacity(8);
        if let Dir(d) = self {
            for item in d.children_iter() {
                vec.push(item)
            }
        }

        vec
    }

    pub fn child_len(&self) -> usize {
        match self {
            File(_) => 0,
            Dir(d) => d.children_iter().len(),
        }
    }

    // Add child nodes.
    pub fn add_child(&mut self, child: InodeView) -> CommonResult<InodePtr> {
        match self {
            File(f) => err_box!("Path not a dir: {}", f.name()),
            Dir(d) => d.add_child(child),
        }
    }

    pub fn delete_child(&mut self, id: i64, name: &str) -> CommonResult<InodeView> {
        match self {
            File(f) => err_box!("Path not a dir: {}", f.name()),
            Dir(ref mut d) => d.delete_child(id, name),
        }
    }

    pub fn mtime(&self) -> i64 {
        match self {
            File(f) => f.mtime(),
            Dir(d) => d.mtime(),
        }
    }

    pub fn update_mtime(&mut self, time: i64) {
        match self {
            File(ref mut f) => {
                if time > f.mtime {
                    f.mtime = time
                }
            }
            Dir(ref mut d) => {
                if time > d.mtime {
                    d.mtime = time
                }
            }
        }
    }

    pub fn set_parent_id(&mut self, parent_id: i64) {
        match self {
            File(f) => f.parent_id = parent_id,
            Dir(d) => d.parent_id = parent_id,
        }
    }

    pub fn atime(&self) -> i64 {
        match self {
            File(f) => f.atime(),
            Dir(d) => d.atime(),
        }
    }

    pub fn as_dir_mut(&mut self) -> CommonResult<&mut InodeDir> {
        match self {
            File(_) => err_box!("Not a dir"),
            Dir(ref mut d) => Ok(d),
        }
    }

    pub fn as_dir_ref(&self) -> CommonResult<&InodeDir> {
        match self {
            File(_) => err_box!("Not a dir"),
            Dir(ref d) => Ok(d),
        }
    }

    pub fn as_file_ref(&self) -> CommonResult<&InodeFile> {
        match self {
            File(f) => Ok(f),
            Dir(_) => err_box!("Not a file"),
        }
    }

    pub fn as_file_mut(&mut self) -> CommonResult<&mut InodeFile> {
        match self {
            File(ref mut f) => Ok(f),
            Dir(_) => err_box!("Not a file"),
        }
    }

    pub fn acl(&self) -> &AclFeature {
        match self {
            File(f) => &f.features.acl,
            Dir(d) => &d.features.acl,
        }
    }

    pub fn acl_mut(&mut self) -> &mut AclFeature {
        match self {
            File(f) => &mut f.features.acl,
            Dir(d) => &mut d.features.acl,
        }
    }

    pub fn storage_policy(&self) -> &StoragePolicy {
        match self {
            File(f) => &f.storage_policy,
            Dir(d) => &d.storage_policy,
        }
    }

    pub fn storage_policy_mut(&mut self) -> &mut StoragePolicy {
        match self {
            File(f) => &mut f.storage_policy,
            Dir(d) => &mut d.storage_policy,
        }
    }

    pub fn x_attr(&self) -> &HashMap<String, Vec<u8>> {
        match self {
            File(f) => &f.features.x_attr,
            Dir(d) => &d.features.x_attr,
        }
    }

    pub fn x_attr_mut(&mut self) -> &mut HashMap<String, Vec<u8>> {
        match self {
            File(f) => &mut f.features.x_attr,
            Dir(d) => &mut d.features.x_attr,
        }
    }

    pub fn as_ptr(&mut self) -> InodePtr {
        InodePtr::from_ref(self)
    }

    pub fn set_attr(&mut self, opts: SetAttrOpts) {
        if let Some(owner) = opts.owner {
            self.acl_mut().owner = owner;
        }

        if let Some(group) = opts.group {
            self.acl_mut().group = group;
        }

        if let Some(mode) = opts.mode {
            self.acl_mut().mode = mode;
        }

        if let Some(ttl_ms) = opts.ttl_ms {
            self.storage_policy_mut().ttl_ms = ttl_ms;
        }

        if let Some(ttl_action) = opts.ttl_action {
            self.storage_policy_mut().ttl_action = ttl_action;
        }

        for attr in opts.add_x_attr {
            self.x_attr_mut().insert(attr.0, attr.1);
        }

        for key in opts.remove_x_attr {
            let _ = self.x_attr_mut().remove(&key);
        }
    }

    pub fn to_file_status(&self, path: &str) -> FileStatus {
        let acl = self.acl();
        let mut status = FileStatus {
            id: self.id(),
            path: path.to_owned(),
            name: self.name().to_string(),
            is_dir: self.is_dir(),
            mtime: self.mtime(),
            atime: self.atime(),
            children_num: self.child_len() as i32,
            is_complete: false,
            len: 0,
            replicas: 0,
            block_size: 0,
            file_type: FileType::File,
            x_attr: Default::default(),
            storage_policy: Default::default(),
            owner: acl.owner.to_owned(),
            group: acl.group.to_owned(),
            mode: acl.mode,
            target: None,
        };

        match self {
            File(f) => {
                status.is_complete = f.is_complete();
                status.len = f.len;
                status.replicas = f.replicas as i32;
                status.block_size = f.block_size;
                status.file_type = f.file_type;
                status.x_attr = f.features.x_attr.clone();
                status.storage_policy = f.storage_policy.clone();
                status.target = f.target.clone();
            }

            Dir(d) => {
                status.file_type = FileType::Dir;
                status.len = d.children_len() as i64;
                status.x_attr = d.features.x_attr.clone();
                status.storage_policy = d.storage_policy.clone();
            }
        }

        status
    }

    /// Print directory structure, output is the same as the linux tree command
    /// example:
    /// .
    // ├── a1
    // ├── a2
    // └── a3
    //     └── b
    //         └── c
    pub fn print_tree(&self) {
        Self::print_tree0(self, "".to_string(), 0)
    }

    pub fn is_root(&self) -> bool {
        self.id() == ROOT_INODE_ID
    }

    fn print_tree0(inode: &InodeView, prefix: String, level: usize) {
        if level == 0 {
            println!(".")
        }

        let children = inode.children();
        for (index, item) in children.iter().enumerate() {
            if index == children.len() - 1 {
                println!("{}└── {}", prefix, item.name());
                if item.is_dir() {
                    let prefix_new = format!("{}    ", prefix);
                    Self::print_tree0(item, prefix_new, level + 1);
                }
            } else {
                println!("{}├── {}", prefix, item.name());
                if item.is_dir() {
                    let prefix_new = format!("{}│   ", prefix);
                    Self::print_tree0(item, prefix_new, level + 1);
                }
            }
        }
    }

    pub fn sum_hash(&self) -> u128 {
        let mut res: u128 = 0;
        let mut stack = LinkedList::new();
        stack.push_back(self);

        while let Some(v) = stack.pop_front() {
            let bytes = SerdeUtils::serialize(&v).unwrap();
            if !v.is_root() {
                let hash = Utils::crc32(&bytes) as u128;
                // let inode: InodeView = SerdeUtils::deserialize(&bytes).unwrap();
                // info!("id = {}[{}], detail = {:?}", inode.id(), hash, inode);
                res += hash
            }

            for child in v.children() {
                stack.push_front(child)
            }
        }

        res
    }
}

impl Clone for InodeView {
    fn clone(&self) -> Self {
        match self {
            File(f) => File(f.clone()),
            Dir(d) => Dir(d.clone()),
        }
    }
}

impl Debug for InodeView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            File(x) => x.fmt(f),
            Dir(x) => x.fmt(f),
        }
    }
}

impl PartialEq for InodeView {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
