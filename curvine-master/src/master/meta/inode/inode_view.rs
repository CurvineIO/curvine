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
use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::{
    Inode, InodeDir, InodeFile, InodePtr, PATH_SEPARATOR, ROOT_INODE_ID,
};
use crate::master::MetadataCodec;
use curvine_codec::{CodecError, CodecResult, Decode, Encode, Reader, Writer};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{
    FileGen, FileStatus, FileType, SetAttrOpts, StoragePolicy, TtlAction, TtlType,
    INTERNAL_CTIME_XATTR, INTERNAL_TTL_TYPE_XATTR,
};
use curvine_runtime::common::{LocalTime, Utils};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, LinkedList};
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedFile {
    pub name: String,
    pub file: InodeFile,
}

impl NamedFile {
    pub fn new(name: String, file: InodeFile) -> Self {
        NamedFile { name, file }
    }
}

impl Deref for NamedFile {
    type Target = InodeFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for NamedFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Encode for NamedFile {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(&self.name)?;
        self.file.encode(writer)
    }
}

impl Decode for NamedFile {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            name: reader.get_str()?,
            file: InodeFile::decode(reader)?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedDir {
    pub name: String,
    pub dir: InodeDir,
}

impl NamedDir {
    pub fn new(name: String, dir: InodeDir) -> Self {
        NamedDir { name, dir }
    }
}

impl Deref for NamedDir {
    type Target = InodeDir;

    fn deref(&self) -> &Self::Target {
        &self.dir
    }
}

impl DerefMut for NamedDir {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dir
    }
}

impl Encode for NamedDir {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(&self.name)?;
        self.dir.encode(writer)
    }
}

impl Decode for NamedDir {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            name: reader.get_str()?,
            dir: InodeDir::decode(reader)?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedEntry {
    pub name: String,
    pub id: i64,
}

impl NamedEntry {
    pub fn new(name: String, id: i64) -> Self {
        NamedEntry { name, id }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Encode for NamedEntry {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(&self.name)?;
        writer.put_i64(self.id)
    }
}

impl Decode for NamedEntry {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            name: reader.get_str()?,
            id: reader.get_i64()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[repr(i8)]
pub enum InodeView {
    File(Box<NamedFile>) = 1,
    Dir(Box<NamedDir>) = 2,
    FileEntry(Box<NamedEntry>) = 3,
}

impl InodeView {
    /// Copy an inode for persistence; directory edges are stored separately.
    /// Its children are empty: adopt the live children before it replaces the live directory.
    pub(crate) fn clone_metadata(&self) -> Self {
        match self {
            Dir(dir) => Self::new_dir(dir.name.clone(), dir.dir.clone_metadata()),
            _ => self.clone(),
        }
    }

    pub(crate) fn adopt_directory_children(&mut self, detached: Self) {
        debug_assert_eq!(self.is_dir(), detached.is_dir());
        if let (Dir(updated), Dir(original)) = (self, detached) {
            updated.dir.adopt_children(original.dir);
        }
    }

    pub fn new_dir(name: String, dir: InodeDir) -> Self {
        Dir(Box::new(NamedDir::new(name, dir)))
    }

    pub fn new_file(name: String, file: InodeFile) -> Self {
        File(Box::new(NamedFile::new(name, file)))
    }

    pub fn new_entry(name: String, id: i64) -> Self {
        FileEntry(Box::new(NamedEntry::new(name, id)))
    }

    pub fn is_dir(&self) -> bool {
        matches!(self, Dir(_))
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    /// Version identity for files; `None` for directories and unresolved
    /// entries. See [`FileGen`].
    pub fn gen(&self) -> Option<FileGen> {
        match self {
            File(f) => Some(f.gen()),
            _ => None,
        }
    }

    pub fn is_file_entry(&self) -> bool {
        matches!(self, FileEntry(_))
    }

    pub fn is_link(&self) -> bool {
        matches!(self, File(v) if v.file_type == FileType::Link)
    }

    pub fn id(&self) -> i64 {
        match self {
            File(f) => f.id(),
            Dir(d) => d.id(),
            FileEntry(e) => e.id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            File(f) => &f.name,
            Dir(d) => &d.name,
            FileEntry(e) => &e.name,
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

    pub fn get_child(&self, name: &str) -> Option<InodePtr> {
        match self {
            File(_) => None,
            Dir(d) => d.get_child(name),
            FileEntry(_) => None,
        }
    }

    // Test and print for use.
    pub fn children(&self) -> Vec<InodePtr> {
        let mut vec = Vec::with_capacity(8);
        if let Dir(d) = self {
            vec.extend(d.children_iter());
        }

        vec
    }

    pub fn child_len(&self) -> usize {
        match self {
            File(_) => 0,
            Dir(d) => d.children_iter().len(),
            FileEntry(..) => 0,
        }
    }

    // Add child nodes.
    pub fn add_child(&mut self, child: InodeView) -> CommonResult<InodePtr> {
        match self {
            File(f) => err_box!("Path not a dir: {}", f.name),
            Dir(d) => d.add_child(child),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn delete_child(&mut self, id: i64, name: &str) -> CommonResult<InodeView> {
        match self {
            File(f) => err_box!("Path not a dir: {}", f.name),
            Dir(d) => d.delete_child(id, name),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn mtime(&self) -> i64 {
        match self {
            File(f) => f.mtime(),
            Dir(d) => d.mtime(),
            FileEntry(..) => 0,
        }
    }

    pub fn ctime(&self) -> i64 {
        match self {
            File(f) => f.ctime(),
            Dir(d) => d.ctime(),
            FileEntry(..) => 0,
        }
    }

    pub fn update_mtime(&mut self, time: i64) {
        match self {
            File(f) => f.update_mtime(time),
            Dir(d) => d.update_mtime(time),
            FileEntry(..) => (),
        }
    }

    pub fn update_ctime(&mut self, time: i64) {
        match self {
            File(f) => f.update_ctime(time),
            Dir(d) => d.update_ctime(time),
            FileEntry(..) => (),
        }
    }

    pub fn incr_nlink(&mut self, ctime: i64) {
        match self {
            File(f) => {
                f.nlink += 1;
                f.update_ctime(ctime);
            }
            Dir(d) => {
                d.nlink += 1;
                d.update_ctime(ctime);
            }
            FileEntry(..) => (),
        }
    }

    pub fn dec_nlink(&mut self, ctime: i64) {
        match self {
            File(f) => {
                if f.nlink > 0 {
                    f.nlink -= 1;
                    f.update_ctime(ctime);
                }
            }
            Dir(d) => {
                if d.nlink > 0 {
                    d.nlink -= 1;
                    d.update_ctime(ctime);
                }
            }
            FileEntry(..) => (),
        }
    }

    pub fn set_parent_id(&mut self, parent_id: i64) {
        match self {
            File(f) => f.parent_id = parent_id,
            Dir(d) => d.parent_id = parent_id,
            FileEntry(..) => (),
        }
    }

    pub fn change_name(&mut self, new_name: String) {
        match self {
            File(f) => f.name = new_name,
            Dir(d) => d.name = new_name,
            FileEntry(e) => e.name = new_name,
        }
    }

    pub fn atime(&self) -> i64 {
        match self {
            File(f) => f.atime(),
            Dir(d) => d.atime(),
            FileEntry(..) => 0,
        }
    }

    pub fn as_dir_mut(&mut self) -> CommonResult<&mut InodeDir> {
        match self {
            File(_) => err_box!("Not a dir"),
            Dir(d) => Ok(d),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn as_dir_ref(&self) -> CommonResult<&InodeDir> {
        match self {
            File(_) => err_box!("Not a dir"),
            Dir(d) => Ok(d),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn as_file_ref(&self) -> CommonResult<&InodeFile> {
        match self {
            File(f) => Ok(f),
            Dir(_) => err_box!("Not a file"),
            _ => err_box!("Inode type error: {}", self.name()),
        }
    }

    pub fn as_file_mut(&mut self) -> CommonResult<&mut InodeFile> {
        match self {
            File(f) => Ok(f),
            Dir(_) => err_box!("Not a file"),
            _ => err_box!("FileEntry is cannot be mutated: {}", self.name()),
        }
    }

    pub fn acl(&self) -> CommonResult<&AclFeature> {
        match self {
            File(f) => Ok(&f.features.acl),
            Dir(d) => Ok(&d.features.acl),
            FileEntry(..) => err_box!("FileEntry does not support ACL access: {}", self.name()),
        }
    }

    pub fn acl_mut(&mut self) -> CommonResult<&mut AclFeature> {
        match self {
            File(f) => Ok(&mut f.features.acl),
            Dir(d) => Ok(&mut d.features.acl),
            FileEntry(..) => err_box!("FileEntry does not support mutable ACL access"),
        }
    }

    pub fn storage_policy(&self) -> CommonResult<&StoragePolicy> {
        match self {
            File(f) => Ok(&f.storage_policy),
            Dir(d) => Ok(&d.storage_policy),
            FileEntry(..) => {
                err_box!(
                    "FileEntry does not support storage policy access: {}",
                    self.name()
                )
            }
        }
    }

    pub fn storage_policy_mut(&mut self) -> CommonResult<&mut StoragePolicy> {
        match self {
            File(f) => Ok(&mut f.storage_policy),
            Dir(d) => Ok(&mut d.storage_policy),
            FileEntry(..) => err_box!("FileEntry does not support mutable storage policy access"),
        }
    }

    /// Base time used for TTL expiration: atime when the internal atime marker
    /// is set (atime-based mount policy), otherwise mtime (legacy behavior).
    pub fn ttl_base_ms(&self) -> CommonResult<i64> {
        let typ = TtlType::from_xattr_value(
            self.x_attr()?
                .get(INTERNAL_TTL_TYPE_XATTR)
                .map(|v| v.as_slice()),
        );
        Ok(match typ {
            TtlType::Atime => self.atime(),
            TtlType::Mtime => self.mtime(),
        })
    }

    pub fn expiration_ms(&self) -> CommonResult<Option<i64>> {
        let sp = self.storage_policy()?;
        if sp.ttl_ms > 0 && sp.ttl_action != TtlAction::None {
            Ok(Some(self.ttl_base_ms()?.saturating_add(sp.ttl_ms)))
        } else {
            Ok(None)
        }
    }

    pub fn is_expired(&self) -> CommonResult<bool> {
        let sp = self.storage_policy()?;
        if sp.ttl_action != TtlAction::None && sp.ttl_ms > 0 {
            Ok(LocalTime::mills() as i64 > self.ttl_base_ms()?.saturating_add(sp.ttl_ms))
        } else {
            Ok(false)
        }
    }

    pub fn x_attr(&self) -> CommonResult<&HashMap<String, Vec<u8>>> {
        match self {
            File(f) => Ok(&f.features.x_attr),
            Dir(d) => Ok(&d.features.x_attr),
            FileEntry(..) => err_box!("FileEntry does not support x_attr access: {}", self.name()),
        }
    }

    pub fn x_attr_mut(&mut self) -> CommonResult<&mut HashMap<String, Vec<u8>>> {
        match self {
            File(f) => Ok(&mut f.features.x_attr),
            Dir(d) => Ok(&mut d.features.x_attr),
            FileEntry(..) => err_box!("FileEntry does not support mutable x_attr access"),
        }
    }

    pub fn nlink(&self) -> CommonResult<u32> {
        match self {
            File(f) => Ok(f.nlink()),
            Dir(d) => Ok(d.nlink()),
            FileEntry(_) => err_box!("FileEntry does not support nlink: {}", self.name()),
        }
    }

    pub fn as_ptr(&mut self) -> InodePtr {
        InodePtr::from_ref(self)
    }

    pub fn set_attr(&mut self, opts: SetAttrOpts) -> CommonResult<()> {
        if let Some(owner) = opts.owner {
            self.acl_mut()?.owner = owner;
        }

        if let Some(group) = opts.group {
            self.acl_mut()?.group = group;
        }

        if let Some(mode) = opts.mode {
            self.acl_mut()?.mode = mode;
        }

        // Handle time modifications
        if let Some(atime) = opts.atime {
            match self {
                File(f) => f.atime = atime,
                Dir(d) => d.atime = atime,
                _ => (),
            }
        }

        if let Some(mtime) = opts.mtime {
            match self {
                File(f) => f.mtime = mtime,
                Dir(d) => d.mtime = mtime,
                _ => (),
            }
        }

        if let Some(ttl_ms) = opts.ttl_ms {
            self.storage_policy_mut()?.ttl_ms = ttl_ms;
        }

        if let Some(ttl_action) = opts.ttl_action {
            let storage_policy = self.storage_policy_mut()?;
            if storage_policy.ttl_action != TtlAction::Free {
                storage_policy.ttl_action = ttl_action;
            }
        }

        for attr in opts.add_x_attr {
            // The TTL-type marker is master-owned (set by the create/mount
            // merge path). Unlike ctime it is never re-injected here, so
            // filtering it cannot break journal replay, while it still blocks
            // client-side flips of the TTL base via complete/close opts.
            if attr.0 != INTERNAL_TTL_TYPE_XATTR {
                self.x_attr_mut()?.insert(attr.0, attr.1);
            }
        }

        for key in opts.remove_x_attr {
            if key != INTERNAL_CTIME_XATTR && key != INTERNAL_TTL_TYPE_XATTR {
                let _ = self.x_attr_mut()?.remove(&key);
            }
        }

        if let Some(ufs_mtime) = opts.ufs_mtime {
            if self.is_file() {
                self.storage_policy_mut()?.save_ufs(ufs_mtime);
            }
        }

        Ok(())
    }

    pub fn to_file_status(&self, path: &str) -> CommonResult<FileStatus> {
        match self {
            File(f) => {
                let acl = &f.features.acl;
                Ok(FileStatus {
                    id: self.id(),
                    path: path.to_owned(),
                    name: self.name().to_owned(),
                    is_dir: false,
                    mtime: self.mtime(),
                    atime: self.atime(),
                    children_num: 0,
                    is_complete: f.is_complete(),
                    len: f.len,
                    replicas: f.replicas as i32,
                    block_size: f.block_size as i64,
                    file_type: f.file_type,
                    x_attr: f.features.x_attr.clone(),
                    storage_policy: f.storage_policy.clone(),
                    owner: acl.owner.to_owned(),
                    group: acl.group.to_owned(),
                    mode: acl.mode,
                    nlink: f.nlink(),
                    target: f.target.clone(),
                })
            }

            Dir(d) => {
                let acl = &d.features.acl;
                Ok(FileStatus {
                    id: self.id(),
                    path: path.to_owned(),
                    name: self.name().to_owned(),
                    is_dir: true,
                    mtime: self.mtime(),
                    atime: self.atime(),
                    children_num: self.child_len() as i32,
                    is_complete: false,
                    len: d.children_len() as i64,
                    replicas: 0,
                    block_size: 0,
                    file_type: FileType::Dir,
                    x_attr: d.features.x_attr.clone(),
                    storage_policy: d.storage_policy.clone(),
                    owner: acl.owner.to_owned(),
                    group: acl.group.to_owned(),
                    mode: acl.mode,
                    nlink: d.nlink(),
                    target: None,
                })
            }

            FileEntry(_) => err_box!("FileEntry does not support to_file_status: {}", self.name()),
        }
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

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let mut res: u128 = 0;
        let mut stack = LinkedList::new();
        stack.push_back(InodePtr::from_ref(self));

        while let Some(v) = stack.pop_front() {
            let bytes = MetadataCodec::encode(v.as_ref())?;
            if !v.is_root() {
                let hash = Utils::crc32(&bytes) as u128;
                res += hash
            }

            for child in v.children() {
                stack.push_front(child)
            }
        }

        Ok(res)
    }
}

impl Clone for InodeView {
    fn clone(&self) -> Self {
        match self {
            File(f) => File(f.clone()),
            Dir(d) => Dir(d.clone()),
            FileEntry(e) => FileEntry(e.clone()),
        }
    }
}

impl Debug for InodeView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            File(v) => write!(f, "File(name={}, x={:?})", v.name, v.file),
            Dir(v) => write!(f, "Dir(name={}, x={:?})", v.name, v.dir),
            FileEntry(v) => write!(f, "FileEntry(name={}, id={})", v.name, v.id),
        }
    }
}

impl PartialEq for InodeView {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

const TAG_FILE: u8 = 1;
const TAG_DIR: u8 = 2;
const TAG_ENTRY: u8 = 3;

impl Encode for InodeView {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        match self {
            File(file) => {
                writer.put_u8(TAG_FILE)?;
                file.encode(writer)
            }
            Dir(dir) => {
                writer.put_u8(TAG_DIR)?;
                dir.encode(writer)
            }
            FileEntry(entry) => {
                writer.put_u8(TAG_ENTRY)?;
                entry.encode(writer)
            }
        }
    }
}

impl Decode for InodeView {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        match reader.get_u8()? {
            TAG_FILE => Ok(File(Box::new(NamedFile::decode(reader)?))),
            TAG_DIR => Ok(Dir(Box::new(NamedDir::decode(reader)?))),
            TAG_ENTRY => Ok(FileEntry(Box::new(NamedEntry::decode(reader)?))),
            other => Err(CodecError::invalid_tag("InodeView", other as i64)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_expires_on_mtime_by_default() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode
            .set_attr(SetAttrOpts {
                atime: Some(100),
                mtime: Some(1_000),
                ..Default::default()
            })
            .unwrap();
        inode
            .set_attr(SetAttrOpts {
                ttl_ms: Some(500),
                ttl_action: Some(TtlAction::Delete),
                ..Default::default()
            })
            .unwrap();

        // Base is mtime: 1_000 + 500.
        assert_eq!(inode.expiration_ms().unwrap(), Some(1_500));
    }

    #[test]
    fn ttl_expires_on_atime_when_marked() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        // The marker is master-owned and set by the create/mount merge path;
        // stamp it directly the same way (SetAttr must not be able to add it).
        inode.x_attr_mut().unwrap().insert(
            INTERNAL_TTL_TYPE_XATTR.to_string(),
            TtlType::Atime.to_xattr_value(),
        );
        inode
            .set_attr(SetAttrOpts {
                atime: Some(100),
                mtime: Some(1_000),
                ..Default::default()
            })
            .unwrap();
        inode
            .set_attr(SetAttrOpts {
                ttl_ms: Some(500),
                ttl_action: Some(TtlAction::Delete),
                ..Default::default()
            })
            .unwrap();

        // Base switches to atime once the internal marker is present:
        // 100 + 500. A read that refreshes atime pushes expiration out.
        assert_eq!(inode.expiration_ms().unwrap(), Some(600));

        inode
            .set_attr(SetAttrOpts {
                atime: Some(10_000),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(inode.expiration_ms().unwrap(), Some(10_500));
    }

    #[test]
    fn ttl_without_policy_has_no_expiration() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode.x_attr_mut().unwrap().insert(
            INTERNAL_TTL_TYPE_XATTR.to_string(),
            TtlType::Atime.to_xattr_value(),
        );
        inode
            .set_attr(SetAttrOpts {
                atime: Some(100),
                ..Default::default()
            })
            .unwrap();

        // Marker without an active TTL policy must not expire anything.
        assert_eq!(inode.expiration_ms().unwrap(), None);
        assert!(!inode.is_expired().unwrap());
    }

    #[test]
    fn metadata_clone_preserves_persisted_fields_without_copying_children() {
        let mut inode = InodeView::new_dir("parent".into(), InodeDir::new(7001, 42));
        inode.set_parent_id(7000);
        inode.incr_nlink(43);
        inode
            .set_attr(SetAttrOpts {
                owner: Some("owner".into()),
                group: Some("group".into()),
                mode: Some(0o2750),
                atime: Some(45),
                mtime: Some(46),
                ttl_ms: Some(10_000),
                add_x_attr: HashMap::from([("user.research".into(), vec![1, 2, 3])]),
                ..Default::default()
            })
            .unwrap();
        inode
            .add_child(InodeView::new_dir("child".into(), InodeDir::new(7002, 44)))
            .unwrap();
        let copy = inode.clone_metadata();
        assert_eq!(inode.child_len(), 1);
        assert_eq!(copy.child_len(), 0);
        assert_eq!(
            MetadataCodec::encode(&inode).unwrap(),
            MetadataCodec::encode(&copy).unwrap()
        );
    }

    #[test]
    fn setattr_updates_ctime_when_mtime_is_omitted() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode
            .set_attr(SetAttrOpts {
                atime: Some(2),
                mtime: None,
                add_x_attr: HashMap::from([(
                    INTERNAL_CTIME_XATTR.to_string(),
                    3_i64.to_le_bytes().to_vec(),
                )]),
                ..Default::default()
            })
            .unwrap();

        let status = inode.to_file_status("/file").unwrap();
        assert_eq!(status.atime, 2);
        assert_eq!(status.mtime, 1);
        assert_eq!(status.ctime(), 3);
    }

    #[test]
    fn legacy_setattr_replay_keeps_ctime_fallback_deterministic() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode
            .set_attr(SetAttrOpts {
                atime: Some(2),
                ..Default::default()
            })
            .unwrap();

        let status = inode.to_file_status("/file").unwrap();
        assert_eq!(status.ctime(), status.mtime);
        assert!(!status.x_attr.contains_key(INTERNAL_CTIME_XATTR));
    }

    #[test]
    fn setattr_cannot_remove_internal_ctime() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode
            .set_attr(SetAttrOpts {
                add_x_attr: HashMap::from([(
                    INTERNAL_CTIME_XATTR.to_string(),
                    3_i64.to_le_bytes().to_vec(),
                )]),
                ..Default::default()
            })
            .unwrap();
        inode
            .set_attr(SetAttrOpts {
                remove_x_attr: vec![INTERNAL_CTIME_XATTR.to_string()],
                ..Default::default()
            })
            .unwrap();

        assert_eq!(inode.ctime(), 3);
    }

    #[test]
    fn mtime_and_nlink_updates_advance_persisted_ctime() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode
            .set_attr(SetAttrOpts {
                add_x_attr: HashMap::from([(
                    INTERNAL_CTIME_XATTR.to_string(),
                    2_i64.to_le_bytes().to_vec(),
                )]),
                ..Default::default()
            })
            .unwrap();

        inode.update_mtime(3);
        assert_eq!(inode.ctime(), 3);

        inode.incr_nlink(4);
        assert_eq!(inode.ctime(), 4);

        inode.dec_nlink(5);
        assert_eq!(inode.ctime(), 5);
    }

    #[test]
    fn setattr_reuses_persisted_ctime_during_replay() {
        let mut inode = InodeView::new_file("file".to_string(), InodeFile::new(1, 1));
        inode
            .set_attr(SetAttrOpts {
                atime: Some(2),
                add_x_attr: HashMap::from([(
                    INTERNAL_CTIME_XATTR.to_string(),
                    3_i64.to_le_bytes().to_vec(),
                )]),
                ..Default::default()
            })
            .unwrap();

        assert_eq!(inode.ctime(), 3);
    }
}
