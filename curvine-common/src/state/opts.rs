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

use crate::state::TtlAction;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetAttrOpts {
    pub recursive: bool,
    pub replicas: Option<i32>,
    pub owner: Option<String>,
    pub group: Option<String>,
    pub mode: Option<u32>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub add_x_attr: HashMap<String, Vec<u8>>,
    pub remove_x_attr: Vec<String>,
}

impl SetAttrOpts {
    // Recursive setting, only allows setting acl related attributes
    pub fn child_opts(&self) -> Self {
        Self {
            recursive: false,
            replicas: self.replicas,
            owner: self.owner.clone(),
            group: self.group.clone(),
            mode: self.mode,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: HashMap::default(),
            remove_x_attr: vec![],
        }
    }
}

pub struct SetAttrOptsBuilder {
    recursive: bool,
    replicas: Option<i32>,
    owner: Option<String>,
    group: Option<String>,
    mode: Option<u32>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    add_x_attr: HashMap<String, Vec<u8>>,
    remove_x_attr: Vec<String>,
}

impl Default for SetAttrOptsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SetAttrOptsBuilder {
    pub fn new() -> Self {
        Self {
            recursive: false,
            replicas: None,
            owner: None,
            group: None,
            mode: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: HashMap::new(),
            remove_x_attr: vec![],
        }
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        let _ = self.replicas.insert(replicas);
        self
    }

    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        let _ = self.owner.insert(owner.into());
        self
    }

    pub fn group(mut self, group: impl Into<String>) -> Self {
        let _ = self.group.insert(group.into());
        self
    }

    pub fn mode(mut self, mode: u32) -> Self {
        let _ = self.mode.insert(mode);
        self
    }

    pub fn ttl_ms(mut self, ms: i64) -> Self {
        let _ = self.ttl_ms.insert(ms);
        self
    }

    pub fn ttl_action(mut self, action: TtlAction) -> Self {
        let _ = self.ttl_action.insert(action);
        self
    }

    pub fn add_x_attr(mut self, key: impl Into<String>, value: Vec<u8>) -> Self {
        self.add_x_attr.insert(key.into(), value);
        self
    }

    pub fn remove_x_attr(mut self, key: impl Into<String>) -> Self {
        self.remove_x_attr.push(key.into());
        self
    }

    pub fn build(self) -> SetAttrOpts {
        SetAttrOpts {
            recursive: self.recursive,
            replicas: self.replicas,
            owner: self.owner,
            group: self.group,
            mode: self.mode,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            add_x_attr: self.add_x_attr,
            remove_x_attr: self.remove_x_attr,
        }
    }
}
