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
use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::{
    Inode, InodeDir, InodeFile, InodePtr, PATH_SEPARATOR, ROOT_INODE_ID,
};
use core::panic;
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
    File(String, InodeFile) = 1,
    Dir(String, InodeDir) = 2,
    FileEntry(String, i64) = 3,
}

impl InodeView {
    pub fn is_dir(&self) -> bool {
        match self {
            File(_, _) => false,
            Dir(_, _) => true,
            _ => false,
        }
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    pub fn is_file_entry(&self) -> bool {
        matches!(self, FileEntry(_, _))
    }

    pub fn is_link(&self) -> bool {
        matches!(self, File(_, v) if v.file_type == FileType::Link)
    }

    pub fn id(&self) -> i64 {
        match self {
            File(_, f) => f.id(),
            Dir(_, d) => d.id(),
            FileEntry(_, id) => *id,
        }
    }
    pub fn ttl_config(&self) -> Option<TtlConfig> {
        match self {
            File(_, f) => TtlConfig::from_storage_policy(&f.storage_policy),
            Dir(_, d) => TtlConfig::from_storage_policy(&d.storage_policy),
            FileEntry(_, _) => None,
        }
    }
    pub fn name(&self) -> &str {
        match self {
            File(name, _) => name,
            Dir(name, _) => name,
            FileEntry(name, _) => name,
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
            File(_, _) => None,
            Dir(_, d) => d.get_child(name),
            FileEntry(..) => None,
        }
    }

    pub fn get_child_ptr(&mut self, name: &str) -> Option<InodePtr> {
        match self {
            File(_, _) => None,
            Dir(_, d) => d.get_child_ptr(name),
            FileEntry(..) => None,
        }
    }

    // Test and print for use. Memory copying occurs.
    pub fn children(&self) -> Vec<&InodeView> {
        let mut vec = Vec::with_capacity(8);
        if let Dir(_, d) = self {
            for item in d.children_iter() {
                vec.push(item)
            }
        }

        vec
    }

    pub fn child_len(&self) -> usize {
        match self {
            File(_, _) => 0,
            Dir(_, d) => d.children_iter().len(),
            FileEntry(..) => 0,
        }
    }

    // Add child nodes.
    pub fn add_child(&mut self, child: InodeView) -> CommonResult<InodePtr> {
        match self {
            File(name, _) => err_box!("Path not a dir: {}", name),
            Dir(_, d) => d.add_child(child),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn delete_child(&mut self, id: i64, name: &str) -> CommonResult<InodeView> {
        match self {
            File(name, _) => err_box!("Path not a dir: {}", name),
            Dir(_, d) => d.delete_child(id, name),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn mtime(&self) -> i64 {
        match self {
            File(_, f) => f.mtime(),
            Dir(_, d) => d.mtime(),
            FileEntry(..) => 0,
        }
    }

    pub fn update_mtime(&mut self, time: i64) {
        match self {
            File(_, ref mut f) => {
                if time > f.mtime {
                    f.mtime = time
                }
            }
            Dir(_, ref mut d) => {
                if time > d.mtime {
                    d.mtime = time
                }
            }
            FileEntry(..) => (),
        }
    }

    pub fn set_parent_id(&mut self, parent_id: i64) {
        match self {
            File(_, f) => f.parent_id = parent_id,
            Dir(_, d) => d.parent_id = parent_id,
            FileEntry(..) => (),
        }
    }

    pub fn change_name(&mut self, new_name: String) {
        match self {
            File(name, _) => *name = new_name,
            Dir(name, _) => *name = new_name,
            FileEntry(name, _) => *name = new_name,
        }
    }

    pub fn atime(&self) -> i64 {
        match self {
            File(_, f) => f.atime(),
            Dir(_, d) => d.atime(),
            FileEntry(..) => 0,
        }
    }

    pub fn as_dir_mut(&mut self) -> CommonResult<&mut InodeDir> {
        match self {
            File(_, _) => err_box!("Not a dir"),
            Dir(_, ref mut d) => Ok(d),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn as_dir_ref(&self) -> CommonResult<&InodeDir> {
        match self {
            File(_, _) => err_box!("Not a dir"),
            Dir(_, ref d) => Ok(d),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn as_file_ref(&self) -> CommonResult<&InodeFile> {
        match self {
            File(_, f) => Ok(f),
            Dir(_, _) => err_box!("Not a file"),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn as_file_mut(&mut self) -> CommonResult<&mut InodeFile> {
        match self {
            File(_, ref mut f) => Ok(f),
            Dir(_, _) => err_box!("Not a file"),
            _ => err_box!("FileEntry is cannot be mutated: {}", self.name()),
        }
    }

    pub fn acl(&self) -> &AclFeature {
        match self {
            File(_, f) => &f.features.acl,
            Dir(_, d) => &d.features.acl,
            FileEntry(..) => {
                panic!("FileEntry does not support ACL access")
            }
        }
    }

    pub fn acl_mut(&mut self) -> &mut AclFeature {
        match self {
            File(_, f) => &mut f.features.acl,
            Dir(_, d) => &mut d.features.acl,
            FileEntry(..) => {
                panic!("FileEntry does not support mutable ACL access")
            }
        }
    }

    pub fn storage_policy(&self) -> &StoragePolicy {
        match self {
            File(_, f) => &f.storage_policy,
            Dir(_, d) => &d.storage_policy,
            FileEntry(..) => {
                panic!("FileEntry does not support storage policy access")
            }
        }
    }

    pub fn storage_policy_mut(&mut self) -> &mut StoragePolicy {
        match self {
            File(_, f) => &mut f.storage_policy,
            Dir(_, d) => &mut d.storage_policy,
            FileEntry(..) => {
                panic!("FileEntry does not support mutable storage policy access")
            }
        }
    }

    pub fn x_attr(&self) -> &HashMap<String, Vec<u8>> {
        match self {
            File(_, f) => &f.features.x_attr,
            Dir(_, d) => &d.features.x_attr,
            FileEntry(..) => {
                panic!("FileEntry does not support x_attr access")
            }
        }
    }

    pub fn x_attr_mut(&mut self) -> &mut HashMap<String, Vec<u8>> {
        match self {
            File(_, f) => &mut f.features.x_attr,
            Dir(_, d) => &mut d.features.x_attr,
            FileEntry(..) => {
                panic!("FileEntry does not support mutable x_attr access")
            }
        }
    }

    pub fn nlink(&self) -> u32 {
        match self {
            File(_, f) => f.nlink(),
            Dir(_, _) => 1,
            FileEntry(_, _) => {
                panic!("FileEntry does not support nlink")
            }
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

        // Handle time modifications
        if let Some(atime) = opts.atime {
            match self {
                File(_, f) => f.atime = atime,
                Dir(_, d) => d.atime = atime,
                _ => (),
            }
        }

        if let Some(mtime) = opts.mtime {
            match self {
                File(_, f) => f.mtime = mtime,
                Dir(_, d) => d.mtime = mtime,
                _ => (),
            }
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
            name: self.name().to_owned(),
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
            nlink: self.nlink(),
            target: None,
        };

        match self {
            File(_, f) => {
                status.is_complete = f.is_complete();
                status.len = f.len;
                status.replicas = f.replicas as i32;
                status.block_size = f.block_size;
                status.file_type = f.file_type;
                status.x_attr = f.features.x_attr.clone();
                status.storage_policy = f.storage_policy.clone();
                status.target = f.target.clone();
            }

            Dir(_, d) => {
                status.file_type = FileType::Dir;
                status.len = d.children_len() as i64;
                status.x_attr = d.features.x_attr.clone();
                status.storage_policy = d.storage_policy.clone();
            }

            FileEntry(_, _inode_id) => {
                panic!("FileEntry does not support to_file_status");
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
            File(name, f) => File(name.clone(), f.clone()),
            Dir(name, d) => Dir(name.clone(), d.clone()),
            FileEntry(name, id) => FileEntry(name.clone(), *id),
        }
    }
}

impl Debug for InodeView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            File(name, x) => write!(f, "File(name={}, x={:?})", name, x),
            Dir(name, x) => write!(f, "Dir(name={}, x={:?})", name, x),
            FileEntry(name, id) => write!(f, "FileEntry(name={}, id={})", name, id),
        }
    }
}

impl PartialEq for InodeView {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
