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

//! Rich metadata enum for inode entries.
//!
//! `InodeView` holds the complete metadata for an inode, loaded from InodeStore.
//! For tree navigation, use `DirEntry` which is lightweight and only contains id + kind.
//!
//! # Architecture
//! ```text
//! InodeView (rich metadata enum)
//!   ├── Dir(Box<InodeDir>)  - directory metadata
//!   └── File(Box<InodeFile>) - file metadata
//! ```
//!
//! Note: FileEntry variant has been removed. Lightweight file entries in the tree
//! are now represented as `DirEntry { entry: InodeEntry::File(id), children: None }`.

use crate::master::meta::feature::AclFeature;
use crate::master::meta::inode::{Inode, InodeDir, InodeFile, PATH_SEPARATOR, ROOT_INODE_ID};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts, StoragePolicy, TtlAction};
use curvine_common::utils::SerdeUtils;
use orpc::common::{LocalTime, Utils};
use orpc::{err_box, CommonResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

/// Rich metadata enum for inode entries.
/// Contains complete metadata for files and directories.
///
/// For tree navigation, use `DirEntry` instead.
#[derive(Serialize, Deserialize)]
#[repr(i8)]
pub enum InodeView {
    /// File with complete metadata
    File(Box<InodeFile>) = 1,
    /// Directory with complete metadata
    Dir(Box<InodeDir>) = 2,
    // FileEntry variant removed - use DirEntry { entry: InodeEntry::File(id), ... } instead
}

impl InodeView {
    /// Creates a new file view
    pub fn new_file(file: InodeFile) -> Self {
        InodeView::File(Box::new(file))
    }

    /// Creates a new directory view
    pub fn new_dir(dir: InodeDir) -> Self {
        InodeView::Dir(Box::new(dir))
    }

    /// Returns true if this is a directory
    pub fn is_dir(&self) -> bool {
        matches!(self, InodeView::Dir(_))
    }

    /// Returns true if this is a file
    pub fn is_file(&self) -> bool {
        matches!(self, InodeView::File(_))
    }

    /// Returns true if this is a symlink
    pub fn is_link(&self) -> bool {
        matches!(self, InodeView::File(f) if f.file_type == FileType::Link)
    }

    /// Returns the inode id
    pub fn id(&self) -> i64 {
        match self {
            InodeView::File(f) => f.id(),
            InodeView::Dir(d) => d.id(),
        }
    }

    /// Returns the name from the directory if available
    /// Note: Name is primarily stored in the parent's children map key.
    /// This method is for compatibility during transition.
    pub fn name(&self) -> &str {
        match self {
            InodeView::File(_f) => "",
            InodeView::Dir(_d) => "",
        }
    }

    /// Changes the name (for compatibility during transition)
    /// Note: This is a no-op in the new design as name is stored in parent's children key.
    pub fn change_name(&mut self, _new_name: String) {}

    /// Parses a path into components
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

    /// Checks if a path is a full (absolute) path
    pub fn is_full_path(path: &str) -> bool {
        path.starts_with(PATH_SEPARATOR)
    }

    /// Returns the modification time
    pub fn mtime(&self) -> i64 {
        match self {
            InodeView::File(f) => f.mtime(),
            InodeView::Dir(d) => d.mtime(),
        }
    }

    /// Updates the modification time if the new time is greater
    pub fn update_mtime(&mut self, time: i64) {
        match self {
            InodeView::File(f) => {
                if time > f.mtime {
                    f.mtime = time
                }
            }
            InodeView::Dir(d) => {
                if time > d.mtime {
                    d.mtime = time
                }
            }
        }
    }

    /// Increments the link count
    pub fn incr_nlink(&mut self) {
        match self {
            InodeView::File(f) => f.nlink += 1,
            InodeView::Dir(d) => d.nlink += 1,
        }
    }

    /// Decrements the link count
    pub fn decr_nlink(&mut self) {
        match self {
            InodeView::File(f) => {
                if f.nlink > 0 {
                    f.nlink -= 1
                }
            }
            InodeView::Dir(d) => {
                if d.nlink > 0 {
                    d.nlink -= 1
                }
            }
        }
    }

    /// Sets the parent id
    pub fn set_parent_id(&mut self, parent_id: i64) {
        match self {
            InodeView::File(f) => f.parent_id = parent_id,
            InodeView::Dir(d) => d.parent_id = parent_id,
        }
    }

    /// Returns the access time
    pub fn atime(&self) -> i64 {
        match self {
            InodeView::File(f) => f.atime(),
            InodeView::Dir(d) => d.atime(),
        }
    }

    /// Returns a mutable reference to the directory data
    pub fn as_dir_mut(&mut self) -> CommonResult<&mut InodeDir> {
        match self {
            InodeView::Dir(d) => Ok(d),
            _ => err_box!("Not a directory"),
        }
    }

    /// Returns a reference to the directory data
    pub fn as_dir_ref(&self) -> CommonResult<&InodeDir> {
        match self {
            InodeView::Dir(d) => Ok(d),
            _ => err_box!("Not a directory"),
        }
    }

    /// Returns a reference to the file data
    pub fn as_file_ref(&self) -> CommonResult<&InodeFile> {
        match self {
            InodeView::File(f) => Ok(f),
            _ => err_box!("Not a file"),
        }
    }

    /// Returns a mutable reference to the file data
    pub fn as_file_mut(&mut self) -> CommonResult<&mut InodeFile> {
        match self {
            InodeView::File(f) => Ok(f),
            _ => err_box!("Not a file"),
        }
    }

    /// Returns the ACL feature
    pub fn acl(&self) -> &AclFeature {
        match self {
            InodeView::File(f) => &f.features.acl,
            InodeView::Dir(d) => &d.features.acl,
        }
    }

    /// Returns a mutable reference to the ACL feature
    pub fn acl_mut(&mut self) -> &mut AclFeature {
        match self {
            InodeView::File(f) => &mut f.features.acl,
            InodeView::Dir(d) => &mut d.features.acl,
        }
    }

    /// Returns the storage policy
    pub fn storage_policy(&self) -> &StoragePolicy {
        match self {
            InodeView::File(f) => &f.storage_policy,
            InodeView::Dir(d) => &d.storage_policy,
        }
    }

    /// Returns a mutable reference to the storage policy
    pub fn storage_policy_mut(&mut self) -> &mut StoragePolicy {
        match self {
            InodeView::File(f) => &mut f.storage_policy,
            InodeView::Dir(d) => &mut d.storage_policy,
        }
    }

    /// Returns the expiration time in milliseconds if TTL is set
    pub fn expiration_ms(&self) -> Option<i64> {
        let sp = self.storage_policy();
        if sp.ttl_ms > 0 && sp.ttl_action != TtlAction::None {
            Some(self.mtime().saturating_add(sp.ttl_ms))
        } else {
            None
        }
    }

    /// Checks if this inode has expired based on TTL
    pub fn is_expired(&self) -> bool {
        let sp = self.storage_policy();
        if sp.ttl_action != TtlAction::None && sp.ttl_ms > 0 {
            LocalTime::mills() as i64 > self.mtime().saturating_add(sp.ttl_ms)
        } else {
            false
        }
    }

    /// Returns the extended attributes
    pub fn x_attr(&self) -> &HashMap<String, Vec<u8>> {
        match self {
            InodeView::File(f) => &f.features.x_attr,
            InodeView::Dir(d) => &d.features.x_attr,
        }
    }

    /// Returns a mutable reference to the extended attributes
    pub fn x_attr_mut(&mut self) -> &mut HashMap<String, Vec<u8>> {
        match self {
            InodeView::File(f) => &mut f.features.x_attr,
            InodeView::Dir(d) => &mut d.features.x_attr,
        }
    }

    /// Returns the link count
    pub fn nlink(&self) -> u32 {
        match self {
            InodeView::File(f) => f.nlink(),
            InodeView::Dir(d) => d.nlink(),
        }
    }

    /// Applies attribute options
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
                InodeView::File(f) => f.atime = atime,
                InodeView::Dir(d) => d.atime = atime,
            }
        }

        if let Some(mtime) = opts.mtime {
            match self {
                InodeView::File(f) => f.mtime = mtime,
                InodeView::Dir(d) => d.mtime = mtime,
            }
        }

        if let Some(ttl_ms) = opts.ttl_ms {
            self.storage_policy_mut().ttl_ms = ttl_ms;
        }

        if let Some(ttl_action) = opts.ttl_action {
            if self.storage_policy_mut().ttl_action != TtlAction::Free {
                self.storage_policy_mut().ttl_action = ttl_action;
            }
        }

        for attr in opts.add_x_attr {
            self.x_attr_mut().insert(attr.0, attr.1);
        }

        for key in opts.remove_x_attr {
            let _ = self.x_attr_mut().remove(&key);
        }

        if let Some(ufs_mtime) = opts.ufs_mtime {
            if self.is_file() {
                self.storage_policy_mut().save_ufs(ufs_mtime);
            }
        }
    }

    /// Converts to FileStatus
    /// Note: `path` and `name` should be provided by the caller as they are stored
    /// in the parent's children map key, not in InodeView.
    pub fn to_file_status(&self, path: &str, name: &str) -> FileStatus {
        let acl = self.acl();
        let mut status = FileStatus {
            id: self.id(),
            path: path.to_owned(),
            name: name.to_owned(),
            is_dir: self.is_dir(),
            mtime: self.mtime(),
            atime: self.atime(),
            children_num: 0,
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
            InodeView::File(f) => {
                status.is_complete = f.is_complete();
                status.len = f.len;
                status.replicas = f.replicas as i32;
                status.block_size = f.block_size as i64;
                status.file_type = f.file_type;
                status.x_attr = f.features.x_attr.clone();
                status.storage_policy = f.storage_policy.clone();
                status.target = f.target.clone();
            }

            InodeView::Dir(d) => {
                status.file_type = FileType::Dir;
                status.len = 0;
                status.x_attr = d.features.x_attr.clone();
                status.storage_policy = d.storage_policy.clone();
            }
        }

        status
    }

    /// Checks if this is the root inode
    pub fn is_root(&self) -> bool {
        self.id() == ROOT_INODE_ID
    }

    /// Calculates a hash sum for verification (used in testing)
    pub fn sum_hash(&self) -> u128 {
        let bytes = SerdeUtils::serialize(&self).unwrap();
        Utils::crc32(&bytes) as u128
    }
}

impl Clone for InodeView {
    fn clone(&self) -> Self {
        match self {
            InodeView::File(f) => InodeView::File(f.clone()),
            InodeView::Dir(d) => InodeView::Dir(d.clone()),
        }
    }
}

impl Debug for InodeView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InodeView::File(v) => write!(f, "File(id={}, len={})", v.id, v.len),
            InodeView::Dir(v) => write!(f, "Dir(id={})", v.id),
        }
    }
}

impl PartialEq for InodeView {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Inode for InodeView {
    fn id(&self) -> i64 {
        self.id()
    }

    fn parent_id(&self) -> i64 {
        match self {
            InodeView::File(f) => f.parent_id(),
            InodeView::Dir(d) => d.parent_id(),
        }
    }

    fn is_dir(&self) -> bool {
        self.is_dir()
    }

    fn nlink(&self) -> u32 {
        self.nlink()
    }

    fn mtime(&self) -> i64 {
        self.mtime()
    }

    fn atime(&self) -> i64 {
        self.atime()
    }
}