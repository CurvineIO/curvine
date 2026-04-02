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

//! Tree layer types for inode metadata.
//!
//! These types form the lightweight in-memory tree structure used for navigation.
//! Rich metadata is stored separately in InodeView and loaded from InodeStore on demand.
//!
//! # Architecture
//! ```text
//! DirEntry (tree node, lightweight)
//!   ├── entry: InodeEntry (File(id) | Dir(id))
//!   └── children: Option<Box<InodeChildren>>
//!
//! InodeChildren = BTreeMap<String, Box<DirEntry>>
//! ```
//!
//! # Design Principles
//! - Name belongs to edge: stored only in children map key, not duplicated in DirEntry
//! - Tree handles navigation: resolve/DFS/BFS use DirEntry tree
//! - Store handles metadata: rich data (InodeView) loaded from InodeStore by id

use orpc::common::Utils;
use orpc::sys::RawPtr;
use std::collections::BTreeMap;
use std::fmt;

/// Lightweight entry enum - only contains id and kind.
/// Used inside DirEntry to identify the inode without carrying rich metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeEntry {
    /// File inode with its id
    File(i64),
    /// Directory inode with its id
    Dir(i64),
}

impl InodeEntry {
    /// Returns the inode id
    #[inline]
    pub fn id(&self) -> i64 {
        match self {
            InodeEntry::File(id) | InodeEntry::Dir(id) => *id,
        }
    }

    /// Returns true if this is a directory entry
    #[inline]
    pub fn is_dir(&self) -> bool {
        matches!(self, InodeEntry::Dir(_))
    }

    /// Returns true if this is a file entry
    #[inline]
    pub fn is_file(&self) -> bool {
        matches!(self, InodeEntry::File(_))
    }
}

/// Children container for directory entries.
/// Key is the child name (edge), value is the child tree node.
#[derive(Debug, Clone, Default)]
pub struct InodeChildren {
    inner: BTreeMap<String, Box<DirEntry>>,
}

impl InodeChildren {
    /// Creates an empty children container
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of children
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no children
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Gets a child by name
    pub fn get(&self, name: &str) -> Option<&DirEntry> {
        self.inner.get(name).map(|b| b.as_ref())
    }

    /// Gets a mutable child by name
    pub fn get_mut(&mut self, name: &str) -> Option<&mut DirEntry> {
        self.inner.get_mut(name).map(|b| b.as_mut())
    }

    /// Adds a child entry. Returns true if the child was newly inserted.
    pub fn insert(&mut self, name: String, entry: DirEntry) -> bool {
        use std::collections::btree_map::Entry;
        match self.inner.entry(name) {
            Entry::Vacant(v) => {
                v.insert(Box::new(entry));
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Removes a child by name. Returns the removed entry if it existed.
    pub fn remove(&mut self, name: &str) -> Option<DirEntry> {
        self.inner.remove(name).map(|b| *b)
    }

    /// Returns an iterator over children
    pub fn iter(&self) -> impl Iterator<Item = (&str, &DirEntry)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), v.as_ref()))
    }

    /// Returns a mutable iterator over children
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut DirEntry)> {
        self.inner.iter_mut().map(|(k, v)| (k.as_str(), v.as_mut()))
    }
}

/// Tree node - lightweight structure for navigation.
/// Similar to the dentry (directory entry) concept in file systems.
///
/// Each node contains:
/// - `entry`: identifies the inode (id + kind) without rich metadata
/// - `children`: only present for directories, contains child nodes
///
/// Names are stored in the parent's children map key, not duplicated here.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// The inode entry (id + kind)
    pub entry: InodeEntry,
    /// Children for directories, None for files
    pub children: Option<Box<InodeChildren>>,
}

impl DirEntry {
    /// Creates a new file entry
    pub fn new_file(inode_id: i64) -> Self {
        DirEntry {
            entry: InodeEntry::File(inode_id),
            children: None,
        }
    }

    /// Creates a new directory entry with empty children
    pub fn new_dir(inode_id: i64) -> Self {
        DirEntry {
            entry: InodeEntry::Dir(inode_id),
            children: Some(Box::new(InodeChildren::new())),
        }
    }

    /// Creates a directory entry without children (for lazy loading scenarios)
    pub fn new_dir_empty(inode_id: i64) -> Self {
        DirEntry {
            entry: InodeEntry::Dir(inode_id),
            children: None,
        }
    }

    /// Returns the inode id
    #[inline]
    pub fn id(&self) -> i64 {
        self.entry.id()
    }

    /// Returns true if this is a directory
    #[inline]
    pub fn is_dir(&self) -> bool {
        self.entry.is_dir()
    }

    /// Returns true if this is a file
    #[inline]
    pub fn is_file(&self) -> bool {
        self.entry.is_file()
    }

    /// Gets the children (only for directories)
    pub fn children(&self) -> Option<&InodeChildren> {
        self.children.as_deref()
    }

    /// Gets mutable children (only for directories)
    pub fn children_mut(&mut self) -> Option<&mut InodeChildren> {
        self.children.as_deref_mut()
    }

    /// Gets a child by name (only for directories)
    pub fn get_child(&self, name: &str) -> Option<&DirEntry> {
        self.children.as_ref()?.get(name)
    }

    /// Gets a mutable child by name (only for directories)
    pub fn get_child_mut(&mut self, name: &str) -> Option<&mut DirEntry> {
        self.children.as_deref_mut()?.get_mut(name)
    }

    /// Adds a child (only for directories)
    pub fn add_child(&mut self, name: String, child: DirEntry) -> bool {
        if let Some(ref mut children) = self.children {
            children.insert(name, child)
        } else {
            false
        }
    }

    /// Removes a child (only for directories)
    pub fn remove_child(&mut self, name: &str) -> Option<DirEntry> {
        self.children.as_mut()?.remove(name)
    }

    /// Returns the number of children (0 for files)
    pub fn child_count(&self) -> usize {
        self.children.as_ref().map(|c| c.len()).unwrap_or(0)
    }

    /// Returns a raw pointer to this entry (for tree traversal)
    pub fn as_ptr(&mut self) -> DirEntryRef {
        DirEntryRef::from_ref(self)
    }

    /// Prints the tree structure for debugging
    pub fn print_tree(&self) {
        self.print_tree_recursive("", 0);
    }

    fn print_tree_recursive(&self, name: &str, depth: usize) {
        let indent = "  ".repeat(depth);
        let kind = if self.is_dir() { "D" } else { "F" };
        println!("{}{}: {} (id={})", indent, name, kind, self.id());

        if let Some(children) = self.children() {
            for (child_name, child) in children.iter() {
                child.print_tree_recursive(child_name, depth + 1);
            }
        }
    }

    /// Calculates a hash sum for verification (used in testing)
    /// Hashes the tree structure: ids + kinds + edge names
    pub fn sum_hash(&self) -> u128 {
        self.sum_hash_recursive("")
    }

    fn sum_hash_recursive(&self, name: &str) -> u128 {
        let mut hash: u128 = 0;

        let id_bytes = self.id().to_be_bytes();
        hash += Utils::crc32(&id_bytes) as u128;

        let kind_byte = if self.is_dir() { 1u8 } else { 0u8 };
        hash += Utils::crc32(&[kind_byte]) as u128;

        hash += Utils::crc32(name.as_bytes()) as u128;

        if let Some(children) = self.children() {
            for (child_name, child) in children.iter() {
                hash += child.sum_hash_recursive(child_name);
            }
        }

        hash
    }
}

/// Reference type for DirEntry (used in tree traversal)
pub type DirEntryRef = RawPtr<DirEntry>;

impl fmt::Display for InodeEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InodeEntry::File(id) => write!(f, "File({})", id),
            InodeEntry::Dir(id) => write!(f, "Dir({})", id),
        }
    }
}

impl fmt::Display for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DirEntry {{ entry: {}, children: {} }}",
            self.entry,
            self.children.as_ref().map(|c| c.len()).unwrap_or(0)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_entry() {
        let file = InodeEntry::File(123);
        assert_eq!(file.id(), 123);
        assert!(file.is_file());
        assert!(!file.is_dir());

        let dir = InodeEntry::Dir(456);
        assert_eq!(dir.id(), 456);
        assert!(dir.is_dir());
        assert!(!dir.is_file());
    }

    #[test]
    fn test_dir_entry_file() {
        let file = DirEntry::new_file(123);
        assert_eq!(file.id(), 123);
        assert!(file.is_file());
        assert!(file.children.is_none());
        assert_eq!(file.child_count(), 0);
    }

    #[test]
    fn test_dir_entry_dir() {
        let mut dir = DirEntry::new_dir(456);
        assert_eq!(dir.id(), 456);
        assert!(dir.is_dir());
        assert!(dir.children.is_some());
        assert_eq!(dir.child_count(), 0);

        // Add a child
        let child = DirEntry::new_file(789);
        assert!(dir.add_child("child.txt".to_string(), child));
        assert_eq!(dir.child_count(), 1);

        // Get child
        let found = dir.get_child("child.txt");
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), 789);

        // Remove child
        let removed = dir.remove_child("child.txt");
        assert!(removed.is_some());
        assert_eq!(dir.child_count(), 0);
    }

    #[test]
    fn test_inode_children() {
        let mut children = InodeChildren::new();
        assert!(children.is_empty());

        children.insert("a".to_string(), DirEntry::new_file(1));
        children.insert("b".to_string(), DirEntry::new_dir(2));

        assert_eq!(children.len(), 2);
        assert!(children.get("a").unwrap().is_file());
        assert!(children.get("b").unwrap().is_dir());

        // Test iteration
        let names: Vec<&str> = children.iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["a", "b"]); // BTreeMap is sorted
    }
}