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

//! Directory metadata structure.
//!
//! `InodeDir` holds the pure metadata for a directory inode.
//! Children are managed separately in `DirEntry` (tree layer).
//!
//! # Architecture
//! ```text
//! InodeDir (pure metadata)
//!   - id, parent_id
//!   - mtime, atime, nlink
//!   - storage_policy
//!   - features (ACL, xattr)
//!
//! DirEntry (tree layer)
//!   - entry: InodeEntry::Dir(id)
//!   - children: InodeChildren
//! ```

use crate::master::meta::feature::{AclFeature, DirFeature};
use crate::master::meta::inode::{Inode, EMPTY_PARENT_ID};
use curvine_common::state::{MkdirOpts, StoragePolicy};
use serde::{Deserialize, Serialize};

/// Directory metadata - pure metadata without children.
///
/// Children are managed in `DirEntry` (tree layer) which is separate from this struct.
/// This separation allows the tree to be lightweight (id-only) while rich metadata
/// is loaded on demand from `InodeStore`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeDir {
    /// Inode id
    pub(crate) id: i64,
    /// Parent inode id
    pub(crate) parent_id: i64,
    /// Modification time
    pub(crate) mtime: i64,
    /// Access time
    pub(crate) atime: i64,
    /// Link count (number of entries pointing to this directory)
    pub(crate) nlink: u32,
    /// Storage policy for files created in this directory
    pub(crate) storage_policy: StoragePolicy,
    /// Directory features (ACL, extended attributes)
    pub(crate) features: DirFeature,
    // Note: children field removed - children are now managed in DirEntry (tree layer)
}

impl InodeDir {
    /// Creates a new directory with the given id and time
    pub fn new(id: i64, time: i64) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: Default::default(),
            features: Default::default(),
        }
    }

    /// Creates a new directory with the given options
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
        }
    }

    /// Updates the modification time if the new time is greater
    pub fn update_mtime(&mut self, time: i64) {
        if time > self.mtime {
            self.mtime = time
        }
    }

    /// Returns the parent inode id
    pub fn parent_id(&self) -> i64 {
        self.parent_id
    }

    /// Sets the parent inode id
    pub fn set_parent_id(&mut self, parent_id: i64) {
        self.parent_id = parent_id;
    }

    /// Returns the modification time
    pub fn mtime(&self) -> i64 {
        self.mtime
    }

    /// Returns the access time
    pub fn atime(&self) -> i64 {
        self.atime
    }

    /// Returns the link count
    pub fn nlink(&self) -> u32 {
        self.nlink
    }

    /// Returns the storage policy
    pub fn storage_policy(&self) -> &StoragePolicy {
        &self.storage_policy
    }

    /// Returns a mutable reference to the storage policy
    pub fn storage_policy_mut(&mut self) -> &mut StoragePolicy {
        &mut self.storage_policy
    }

    /// Returns the features
    pub fn features(&self) -> &DirFeature {
        &self.features
    }

    /// Returns a mutable reference to the features
    pub fn features_mut(&mut self) -> &mut DirFeature {
        &mut self.features
    }

    /// Returns the ACL feature
    pub fn acl(&self) -> &AclFeature {
        &self.features.acl
    }

    /// Returns a mutable reference to the ACL feature
    pub fn acl_mut(&mut self) -> &mut AclFeature {
        &mut self.features.acl
    }

    /// Returns the extended attributes
    pub fn x_attr(&self) -> &std::collections::HashMap<String, Vec<u8>> {
        &self.features.x_attr
    }

    /// Returns a mutable reference to the extended attributes
    pub fn x_attr_mut(&mut self) -> &mut std::collections::HashMap<String, Vec<u8>> {
        &mut self.features.x_attr
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