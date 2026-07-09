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

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize, Serialize, Default, Clone, Debug)]
pub struct DirEntry {
    pub scan_complete: bool,
    pub children: BTreeMap<String, u64>,
    pub deleted_children: BTreeSet<String>,
}

impl DirEntry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn remove_child(&mut self, name: &str) {
        self.children.remove(name);
    }

    pub fn add_child(&mut self, name: String, ino: u64) {
        self.deleted_children.remove(&name);
        self.children.insert(name, ino);
    }

    pub fn mark_deleted_child(&mut self, name: &str) {
        self.deleted_children.insert(name.to_owned());
    }

    pub fn clear_deleted_child(&mut self, name: &str) {
        self.deleted_children.remove(name);
    }

    pub fn is_deleted_child(&self, name: &str) -> bool {
        self.deleted_children.contains(name)
    }
}
