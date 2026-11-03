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
use crate::master::meta::inode::DirectoryChildren;
use crate::master::meta::inode::{Inode, InodeFile, InodePtr, InodeView, EMPTY_PARENT_ID};
use curvine_codec::{CodecResult, Decode, Encode, Reader, Writer};
use curvine_core_error::CommonResult;
use curvine_model::{ListOptions, MkdirOpts, StoragePolicy, INTERNAL_CTIME_XATTR};
use glob::Pattern;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeDir {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,

    pub(crate) features: DirFeature,

    #[serde(skip)]
    children: Arc<DirectoryChildren>,
}

impl InodeDir {
    /// Copy persisted directory fields. The copy gets a fresh, empty child
    /// index: children live in the edges, not the inode record, and the
    /// caller must adopt the live handle before publishing the copy.
    pub(crate) fn clone_metadata(&self) -> Self {
        let Self {
            id,
            parent_id,
            mtime,
            atime,
            nlink,
            storage_policy,
            features,
            children: _,
        } = self;
        Self {
            id: *id,
            parent_id: *parent_id,
            mtime: *mtime,
            atime: *atime,
            nlink: *nlink,
            storage_policy: storage_policy.clone(),
            features: features.clone(),
            children: Arc::new(DirectoryChildren::new()),
        }
    }

    /// Attach the original subtree after its updated metadata has been persisted.
    /// The caller owns the detached directory and still holds the namespace lock.
    pub(crate) fn adopt_children(&mut self, detached: Self) {
        debug_assert_eq!(self.id, detached.id);
        self.children = detached.children;
    }

    pub fn new(id: i64, time: i64) -> Self {
        Self::with_children(id, time, Arc::new(DirectoryChildren::new()))
    }

    pub fn with_children(id: i64, time: i64, children: Arc<DirectoryChildren>) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: Default::default(),
            features: Default::default(),
            children,
        }
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
            children: Arc::new(DirectoryChildren::new()),
        }
    }

    /// Add a word node and return a reference to that word node.
    pub fn add_child(&mut self, mut inode: InodeView) -> CommonResult<InodePtr> {
        inode.set_parent_id(self.id);
        self.children.add_child(inode)
    }

    pub fn get_child(&self, name: &str) -> Option<InodePtr> {
        self.children.get_child_ptr(name)
    }

    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodePtr> {
        self.children.get_child_by_glob_pattern(glob_pattern)
    }

    pub fn update_mtime(&mut self, time: i64) {
        if time > self.mtime {
            self.mtime = time;
            self.update_ctime(time);
        }
    }

    pub fn update_ctime(&mut self, time: i64) {
        if time > self.ctime() {
            self.features.x_attr.insert(
                INTERNAL_CTIME_XATTR.to_string(),
                time.to_le_bytes().to_vec(),
            );
        }
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        self.children.delete_child(child_id, child_name)
    }

    /// Shared child index handle for versioned readers.
    pub fn children_handle(&self) -> Arc<DirectoryChildren> {
        self.children.clone()
    }

    pub fn print_child(&self) {
        for child in self.children_vec() {
            let t = if child.is_dir() { "dir" } else { "file" };

            println!("{} {} {}", t, child.id(), child.name());
        }
    }

    pub fn children_iter(&self) -> impl Iterator<Item = InodeView> {
        self.children_vec().into_iter()
    }

    pub fn list_options(&self, options: &ListOptions) -> Vec<InodePtr> {
        self.children.list_options(options)
    }

    pub fn children_vec(&self) -> Vec<InodeView> {
        let children = self.children.read_all();
        children.iter().map(|item| item.as_ref().clone()).collect()
    }

    pub fn children_len(&self) -> usize {
        self.children.iter_len()
    }

    pub fn add_file_child(&mut self, name: &str, file: InodeFile) -> CommonResult<InodePtr> {
        self.add_child(InodeView::new_file(name.to_string(), file))
    }

    pub fn add_dir_child(&mut self, name: &str, dir: InodeDir) -> CommonResult<InodePtr> {
        self.add_child(InodeView::new_dir(name.to_string(), dir))
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

    fn ctime(&self) -> i64 {
        self.features
            .x_attr
            .get(INTERNAL_CTIME_XATTR)
            .and_then(|bytes| bytes.as_slice().try_into().ok())
            .map(i64::from_le_bytes)
            .unwrap_or(self.mtime)
    }
}

impl PartialEq for InodeDir {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Encode for InodeDir {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i64(self.id)?;
        writer.put_i64(self.parent_id)?;
        writer.put_i64(self.mtime)?;
        writer.put_i64(self.atime)?;
        writer.put_u32(self.nlink)?;
        self.storage_policy.encode(writer)?;
        self.features.encode(writer)
    }
}

impl Decode for InodeDir {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            id: reader.get_i64()?,
            parent_id: reader.get_i64()?,
            mtime: reader.get_i64()?,
            atime: reader.get_i64()?,
            nlink: reader.get_u32()?,
            storage_policy: StoragePolicy::decode(reader)?,
            features: DirFeature::decode(reader)?,
            // Children are persisted as edges, not inside the inode record.
            children: Arc::new(DirectoryChildren::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{StorageState, StorageType, TtlAction};
    use std::collections::HashMap;

    #[test]
    fn inode_dir_round_trips_all_fields() {
        let dir = InodeDir {
            id: 101,
            parent_id: 7,
            mtime: 1_000,
            atime: 2_000,
            nlink: 3,
            storage_policy: StoragePolicy {
                storage_type: StorageType::Disk,
                ttl_ms: 60_000,
                ttl_action: TtlAction::Delete,
                ufs_mtime: 1_234,
                state: StorageState::Cv,
            },
            features: DirFeature {
                x_attr: HashMap::from([("user.k".to_string(), vec![1u8, 2, 3])]),
                acl: AclFeature {
                    owner: "owner".to_string(),
                    group: "group".to_string(),
                    mode: 0o755,
                },
            },
            children: Arc::new(DirectoryChildren::new()),
        };

        let mut writer = Writer::new(1).unwrap();
        dir.encode(&mut writer).unwrap();
        let bytes = writer.into_inner();
        let mut reader = Reader::new(&bytes, 1).unwrap();
        let decoded = InodeDir::decode(&mut reader).unwrap();
        assert!(reader.is_empty());

        assert_eq!(decoded.id, dir.id);
        assert_eq!(decoded.parent_id, dir.parent_id);
        assert_eq!(decoded.mtime, dir.mtime);
        assert_eq!(decoded.atime, dir.atime);
        assert_eq!(decoded.nlink, dir.nlink);
        assert_eq!(decoded.storage_policy, dir.storage_policy);
        assert_eq!(decoded.features.x_attr, dir.features.x_attr);
        assert_eq!(decoded.features.acl.owner, dir.features.acl.owner);
        assert_eq!(decoded.features.acl.group, dir.features.acl.group);
        assert_eq!(decoded.features.acl.mode, dir.features.acl.mode);
        // Children are stored as edges outside the inode record, never encoded.
        assert_eq!(decoded.children_len(), 0);
    }
}
