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

//! Directory-only tree types for master metadata.
//!
//! Files are resolved from `InodeStore` on demand. Only directories are stored
//! in the in-memory tree and addressed by stable `EntryKey`s.

use crate::master::meta::inode::{Inode, InodeDir, ROOT_INODE_ID};
use orpc::common::Utils;
use orpc::{err_box, CommonResult};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryKey {
    slot: u32,
    generation: u32,
}

impl EntryKey {
    pub const INVALID: Self = Self {
        slot: u32::MAX,
        generation: 0,
    };

    pub fn new(slot: u32, generation: u32) -> Self {
        Self { slot, generation }
    }

    pub fn slot(self) -> usize {
        self.slot as usize
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirChildren {
    inner: BTreeMap<String, EntryKey>,
}

impl DirChildren {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn get(&self, name: &str) -> Option<EntryKey> {
        self.inner.get(name).copied()
    }

    pub fn insert(&mut self, name: String, key: EntryKey) -> bool {
        use std::collections::btree_map::Entry;
        match self.inner.entry(name) {
            Entry::Vacant(v) => {
                v.insert(key);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn remove(&mut self, name: &str) -> Option<EntryKey> {
        self.inner.remove(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, EntryKey)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), *v))
    }
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    dir: Box<InodeDir>,
    children: Box<DirChildren>,
}

impl DirEntry {
    pub fn new_dir(dir: InodeDir) -> Self {
        Self {
            dir: Box::new(dir),
            children: Box::new(DirChildren::new()),
        }
    }

    pub fn new_dir_with_id(inode_id: i64, time: i64) -> Self {
        Self::new_dir(InodeDir::new(inode_id, time))
    }

    #[inline]
    pub fn id(&self) -> i64 {
        self.dir.id()
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        self.id() == ROOT_INODE_ID
    }

    pub fn as_dir(&self) -> &InodeDir {
        &self.dir
    }

    pub fn as_dir_mut(&mut self) -> &mut InodeDir {
        &mut self.dir
    }

    pub fn children(&self) -> &DirChildren {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut DirChildren {
        &mut self.children
    }

    pub fn get_child(&self, name: &str) -> Option<EntryKey> {
        self.children.get(name)
    }

    pub fn add_child(&mut self, name: String, child: EntryKey) -> bool {
        self.children.insert(name, child)
    }

    pub fn remove_child(&mut self, name: &str) -> Option<EntryKey> {
        self.children.remove(name)
    }

    pub fn child_count(&self) -> usize {
        self.children.len()
    }
}

#[derive(Debug, Clone)]
struct EntrySlot {
    generation: u32,
    value: Option<DirEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct EntryArena {
    slots: Vec<EntrySlot>,
    free_list: Vec<u32>,
}

impl EntryArena {
    pub fn insert(&mut self, entry: DirEntry) -> EntryKey {
        if let Some(slot) = self.free_list.pop() {
            let idx = slot as usize;
            let generation = self.slots[idx].generation.saturating_add(1).max(1);
            self.slots[idx].generation = generation;
            self.slots[idx].value = Some(entry);
            EntryKey::new(slot, generation)
        } else {
            let slot = self.slots.len() as u32;
            self.slots.push(EntrySlot {
                generation: 1,
                value: Some(entry),
            });
            EntryKey::new(slot, 1)
        }
    }

    pub fn get(&self, key: EntryKey) -> Option<&DirEntry> {
        let slot = self.slots.get(key.slot())?;
        if slot.generation != key.generation() {
            return None;
        }
        slot.value.as_ref()
    }

    pub fn get_mut(&mut self, key: EntryKey) -> Option<&mut DirEntry> {
        let slot = self.slots.get_mut(key.slot())?;
        if slot.generation != key.generation() {
            return None;
        }
        slot.value.as_mut()
    }

    pub fn remove(&mut self, key: EntryKey) -> Option<DirEntry> {
        let slot = self.slots.get_mut(key.slot())?;
        if slot.generation != key.generation() {
            return None;
        }
        let entry = slot.value.take();
        if entry.is_some() {
            self.free_list.push(key.slot);
        }
        entry
    }
}

#[derive(Debug, Clone)]
pub struct DirTree {
    root_key: EntryKey,
    arena: EntryArena,
}

impl DirTree {
    pub fn new(root: DirEntry) -> Self {
        let mut arena = EntryArena::default();
        let root_key = arena.insert(root);
        Self { root_key, arena }
    }

    pub fn root_key(&self) -> EntryKey {
        self.root_key
    }

    pub fn arena(&self) -> &EntryArena {
        &self.arena
    }

    pub fn arena_mut(&mut self) -> &mut EntryArena {
        &mut self.arena
    }

    pub fn root_entry(&self) -> CommonResult<&DirEntry> {
        self.entry(self.root_key)
    }

    pub fn entry(&self, key: EntryKey) -> CommonResult<&DirEntry> {
        match self.arena.get(key) {
            Some(entry) => Ok(entry),
            None => err_box!("DirEntry not found for key {:?}", key),
        }
    }

    pub fn entry_mut(&mut self, key: EntryKey) -> CommonResult<&mut DirEntry> {
        match self.arena.get_mut(key) {
            Some(entry) => Ok(entry),
            None => err_box!("DirEntry not found for key {:?}", key),
        }
    }

    pub fn insert_dir(&mut self, dir: InodeDir) -> EntryKey {
        self.arena.insert(DirEntry::new_dir(dir))
    }

    pub fn remove(&mut self, key: EntryKey) -> Option<DirEntry> {
        if key == self.root_key {
            return None;
        }
        self.arena.remove(key)
    }

    pub fn print_tree(&self) {
        if let Ok(root) = self.root_entry() {
            self.print_tree_recursive("", root, 0);
        }
    }

    fn print_tree_recursive(&self, name: &str, entry: &DirEntry, depth: usize) {
        let indent = "  ".repeat(depth);
        println!("{}{}: D (id={})", indent, name, entry.id());
        for (child_name, child_key) in entry.children().iter() {
            if let Some(child) = self.arena.get(child_key) {
                self.print_tree_recursive(child_name, child, depth + 1);
            }
        }
    }

    pub fn sum_hash(&self) -> u128 {
        self.root_entry()
            .map(|root| self.sum_hash_recursive("", root))
            .unwrap_or_default()
    }

    fn sum_hash_recursive(&self, name: &str, entry: &DirEntry) -> u128 {
        let mut hash: u128 = 0;
        hash += Utils::crc32(&entry.id().to_be_bytes()) as u128;
        hash += Utils::crc32(&[1u8]) as u128;
        hash += Utils::crc32(name.as_bytes()) as u128;
        for (child_name, child_key) in entry.children().iter() {
            if let Some(child) = self.arena.get(child_key) {
                hash += self.sum_hash_recursive(child_name, child);
            }
        }
        hash
    }
}

impl fmt::Display for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DirEntry {{ id: {}, children: {} }}",
            self.id(),
            self.child_count()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_children_insert_and_iter() {
        let mut children = DirChildren::new();
        let a = EntryKey::new(1, 1);
        let b = EntryKey::new(2, 1);
        assert!(children.insert("a".to_string(), a));
        assert!(children.insert("b".to_string(), b));
        assert_eq!(children.get("a"), Some(a));
        let names: Vec<&str> = children.iter().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_entry_arena_generation() {
        let mut arena = EntryArena::default();
        let key1 = arena.insert(DirEntry::new_dir(InodeDir::new(1, 0)));
        assert!(arena.get(key1).is_some());
        let _ = arena.remove(key1);
        assert!(arena.get(key1).is_none());
        let key2 = arena.insert(DirEntry::new_dir(InodeDir::new(2, 0)));
        assert_eq!(key1.slot(), key2.slot());
        assert_ne!(key1.generation(), key2.generation());
    }
}
