//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::fs::dcache::SharedStr;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize, Serialize, Default, Clone, Debug)]
pub struct DirEntry {
    pub scan_complete: bool,
    /// Keys are shared with the child inode's `name`, so each file name is
    /// stored once instead of once per dentry and once per inode.
    pub children: BTreeMap<SharedStr, u64>,
    pub deleted_children: BTreeSet<SharedStr>,
}

impl DirEntry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn remove_child(&mut self, name: &str) {
        self.children.remove(name);
    }

    /// Remove a child and hand back its shared name key.
    ///
    /// Callers that move a dentry (exchange/unlink rollback) reuse the key
    /// instead of allocating a second copy of the same name.
    pub fn remove_child_entry(&mut self, name: &str) -> Option<SharedStr> {
        self.children.remove_entry(name).map(|(name, _)| name)
    }

    /// Link `name` to `ino`, taking ownership of the shared key.
    pub fn add_child(&mut self, name: SharedStr, ino: u64) {
        self.deleted_children.remove(&name);
        self.children.insert(name, ino);
    }

    /// Link `name` to `ino`, reusing the existing key allocation when the
    /// dentry already exists (avoids an allocation on the LOOKUP hit path).
    pub fn link_child(&mut self, name: &str, ino: u64) {
        self.deleted_children.remove(name);
        match self.children.get_mut(name) {
            Some(slot) => *slot = ino,
            None => {
                self.children.insert(SharedStr::owned(name.to_owned()), ino);
            }
        }
    }

    /// Mark a removed dentry name as pending a deferred delete.
    ///
    /// Takes the shared key from `remove_child_entry` so the deleted-name set
    /// does not allocate a second copy.
    pub fn mark_deleted_child(&mut self, name: SharedStr) {
        self.deleted_children.insert(name);
    }

    pub fn clear_deleted_child(&mut self, name: &str) {
        self.deleted_children.remove(name);
    }

    pub fn is_deleted_child(&self, name: &str) -> bool {
        self.deleted_children.contains(name)
    }
}
