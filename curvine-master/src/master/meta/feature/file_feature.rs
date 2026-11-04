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

use crate::master::meta::feature::{AclFeature, WriteFeature};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// File extension function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileFeature {
    pub(crate) x_attr: HashMap<String, Vec<u8>>,
    pub(crate) file_write: Option<WriteFeature>,
    pub(crate) acl: AclFeature,
}

impl FileFeature {
    pub fn new() -> Self {
        Self {
            x_attr: HashMap::default(),
            file_write: None,
            acl: AclFeature::default(),
        }
    }

    pub fn add_attr<K: AsRef<str>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.x_attr
            .insert(key.as_ref().to_string(), Vec::from(value.as_ref()));
    }

    pub fn remove_attr<K: AsRef<str>>(&mut self, key: K) {
        self.x_attr.remove(key.as_ref());
    }

    pub fn set_attrs(&mut self, map: HashMap<String, Vec<u8>>) {
        self.x_attr = map
    }

    /// Drop one client's writer marker. Returns whether the marker existed.
    ///
    /// Only the named client is removed: another client may be writing the same
    /// file, and a blanket clear would make the file look complete while it is
    /// still being written (#1880).
    pub fn complete_write(&mut self, client_name: impl AsRef<str>) -> bool {
        match self.file_write {
            None => false,
            Some(ref mut f) => {
                let removed = f.clients.remove(client_name.as_ref());
                if f.clients.is_empty() {
                    let _ = self.file_write.take();
                }
                removed
            }
        }
    }

    pub fn set_writing(&mut self, client_name: String) {
        match &mut self.file_write {
            Some(f) => {
                f.clients.insert(client_name);
            }

            None => {
                self.file_write = Some(WriteFeature::new(client_name));
            }
        }
    }

    pub fn all_attr(&self) -> &HashMap<String, Vec<u8>> {
        &self.x_attr
    }

    pub fn set_mode(&mut self, mode: u32) {
        self.acl.mode = mode;
    }

    pub fn set_user(&mut self, user: impl Into<String>, group: impl Into<String>) {
        self.acl.owner = user.into();
        self.acl.group = group.into();
    }
}

impl Default for FileFeature {
    fn default() -> Self {
        Self::new()
    }
}
