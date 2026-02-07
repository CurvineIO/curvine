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

use crate::conf::ClientConf;
use crate::state::*;
use orpc::common::ByteUnit;
use orpc::sys;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CreateFileOpts {
    pub create_parent: bool,
    pub replicas: u16,
    pub block_size: i64,
    pub file_type: FileType,
    pub x_attr: HashMap<String, Vec<u8>>,
    pub storage_policy: StoragePolicy,
    pub mode: u32,
    pub client_name: String,
    pub owner: String,
    pub group: String,
}

impl CreateFileOpts {
    pub fn with_create(create_parent: bool) -> Self {
        Self {
            file_type: FileType::File,
            replicas: 1,
            block_size: ByteUnit::MB as i64 * 64,
            x_attr: Default::default(),
            storage_policy: Default::default(),
            create_parent,
            client_name: "".to_string(),
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
            owner: "".to_string(),
            group: "".to_string(),
        }
    }

    pub fn dir_opts(&self) -> MkdirOpts {
        MkdirOpts {
            create_parent: self.create_parent,
            x_attr: HashMap::default(),
            storage_policy: StoragePolicy::default(),
            mode: self.mode,
            owner: self.owner.clone(),
            group: self.group.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateFileOptsBuilder {
    create_parent: bool,
    replicas: i32,
    block_size: i64,
    file_type: FileType,
    x_attr: HashMap<String, Vec<u8>>,
    storage_policy: StoragePolicy,
    mode: u32,
    client_name: Option<String>,
    pub owner: String,
    pub group: String,
}

impl Default for CreateFileOptsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateFileOptsBuilder {
    pub fn new() -> Self {
        Self {
            create_parent: false,
            replicas: 1,
            block_size: (64 * ByteUnit::MB) as i64,
            file_type: FileType::File,
            x_attr: HashMap::new(),
            storage_policy: StoragePolicy::default(),
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
            client_name: None,
            owner: "".to_string(),
            group: "".to_string(),
        }
    }

    pub fn with_conf(conf: &ClientConf) -> Self {
        Self {
            create_parent: false,
            replicas: conf.replicas,
            block_size: conf.block_size,
            file_type: FileType::File,
            x_attr: Default::default(),
            storage_policy: StoragePolicy {
                storage_type: conf.storage_type,
                ttl_ms: conf.ttl_ms,
                ttl_action: conf.ttl_action,
                ..Default::default()
            },
            mode: conf.get_mode(),
            client_name: None,
            owner: "".to_string(),
            group: "".to_string(),
        }
    }

    pub fn create_parent(mut self, flag: bool) -> Self {
        self.create_parent = flag;
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        self.replicas = replicas;
        self
    }

    pub fn block_size(mut self, block_size: i64) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn file_type(mut self, file_type: FileType) -> Self {
        self.file_type = file_type;
        self
    }

    pub fn x_attr(mut self, key: String, value: Vec<u8>) -> Self {
        self.x_attr.insert(key, value);
        self
    }

    pub fn storage_type(mut self, t: StorageType) -> Self {
        self.storage_policy.storage_type = t;
        self
    }

    pub fn ttl_ms(mut self, ms: i64) -> Self {
        self.storage_policy.ttl_ms = ms;
        self
    }

    pub fn ttl_action(mut self, action: TtlAction) -> Self {
        self.storage_policy.ttl_action = action;
        self
    }

    pub fn ufs_mtime(mut self, mtime: i64) -> Self {
        self.storage_policy.ufs_mtime = mtime;
        self
    }

    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    pub fn client_name(mut self, name: String) -> Self {
        let _ = self.client_name.insert(name);
        self
    }

    pub fn owner(mut self, owner: String) -> Self {
        self.owner = owner;
        self
    }

    pub fn group(mut self, group: String) -> Self {
        self.group = group;
        self
    }

    pub fn acl(mut self, uid: u32, gid: u32, mode: u32) -> Self {
        self.owner = sys::get_username_by_uid(uid).unwrap_or("".to_string());
        self.group = sys::get_groupname_by_gid(gid).unwrap_or("".to_string());
        self.mode = mode;
        self
    }

    pub fn build(self) -> CreateFileOpts {
        CreateFileOpts {
            create_parent: self.create_parent,
            replicas: self.replicas as u16,
            block_size: self.block_size,
            file_type: self.file_type,
            x_attr: self.x_attr,
            storage_policy: self.storage_policy,
            mode: self.mode,
            client_name: self.client_name.unwrap_or_default(),
            owner: self.owner,
            group: self.group,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MkdirOpts {
    pub create_parent: bool,
    pub x_attr: HashMap<String, Vec<u8>>,
    pub storage_policy: StoragePolicy,
    pub mode: u32,
    pub owner: String,
    pub group: String,
}

impl MkdirOpts {
    pub fn with_create(create_parent: bool) -> Self {
        Self {
            create_parent,
            x_attr: HashMap::default(),
            storage_policy: StoragePolicy::default(),
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
            owner: "".to_string(),
            group: "".to_string(),
        }
    }

    pub fn parent_opts(&self) -> Self {
        Self {
            create_parent: self.create_parent,
            x_attr: HashMap::default(),
            storage_policy: StoragePolicy::default(),
            mode: self.mode,
            owner: self.owner.clone(),
            group: self.group.clone(),
        }
    }
}

pub struct MkdirOptsBuilder {
    create_parent: bool,
    x_attr: HashMap<String, Vec<u8>>,
    storage_policy: StoragePolicy,
    mode: u32,
    pub owner: String,
    pub group: String,
}

impl Default for MkdirOptsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MkdirOptsBuilder {
    pub fn new() -> Self {
        Self {
            create_parent: false,
            x_attr: HashMap::new(),
            storage_policy: StoragePolicy::default(),
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
            owner: "".to_string(),
            group: "".to_string(),
        }
    }

    pub fn with_conf(conf: &ClientConf) -> Self {
        Self {
            create_parent: false,
            x_attr: HashMap::new(),
            storage_policy: StoragePolicy {
                storage_type: conf.storage_type,
                ttl_ms: conf.ttl_ms,
                ttl_action: conf.ttl_action,
                ..Default::default()
            },
            mode: conf.get_mode(),
            owner: "".to_string(),
            group: "".to_string(),
        }
    }

    pub fn create_parent(mut self, flag: bool) -> Self {
        self.create_parent = flag;
        self
    }

    pub fn x_attr(mut self, key: String, value: Vec<u8>) -> Self {
        self.x_attr.insert(key, value);
        self
    }

    pub fn storage_type(mut self, t: StorageType) -> Self {
        self.storage_policy.storage_type = t;
        self
    }

    pub fn ttl_ms(mut self, ms: i64) -> Self {
        self.storage_policy.ttl_ms = ms;
        self
    }

    pub fn ttl_action(mut self, action: TtlAction) -> Self {
        self.storage_policy.ttl_action = action;
        self
    }

    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    pub fn owner(mut self, owner: String) -> Self {
        self.owner = owner;
        self
    }

    pub fn group(mut self, group: String) -> Self {
        self.group = group;
        self
    }

    pub fn acl(mut self, uid: u32, gid: u32, mode: u32) -> Self {
        self.owner = sys::get_username_by_uid(uid).unwrap_or(uid.to_string());
        self.group = sys::get_groupname_by_gid(gid).unwrap_or(gid.to_string());
        self.mode = mode;
        self
    }

    pub fn build(self) -> MkdirOpts {
        MkdirOpts {
            create_parent: self.create_parent,
            x_attr: self.x_attr,
            storage_policy: self.storage_policy,
            mode: self.mode,
            owner: self.owner,
            group: self.group,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SetAttrOpts {
    pub recursive: bool,
    pub replicas: Option<i32>,
    pub owner: Option<String>,
    pub group: Option<String>,
    pub mode: Option<u32>,
    #[serde(default)]
    pub atime: Option<i64>, // Access time in milliseconds
    #[serde(default)]
    pub mtime: Option<i64>, // Modification time in milliseconds
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub add_x_attr: HashMap<String, Vec<u8>>,
    pub remove_x_attr: Vec<String>,
    pub ufs_mtime: Option<i64>,
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
            atime: None,
            mtime: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: HashMap::default(),
            remove_x_attr: vec![],
            ufs_mtime: None,
        }
    }
}

pub struct SetAttrOptsBuilder {
    recursive: bool,
    replicas: Option<i32>,
    owner: Option<String>,
    group: Option<String>,
    mode: Option<u32>,
    atime: Option<i64>,
    mtime: Option<i64>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    add_x_attr: HashMap<String, Vec<u8>>,
    remove_x_attr: Vec<String>,
    ufs_mtime: Option<i64>,
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
            atime: None,
            mtime: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: HashMap::new(),
            remove_x_attr: vec![],
            ufs_mtime: None,
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

    pub fn atime(mut self, atime: i64) -> Self {
        let _ = self.atime.insert(atime);
        self
    }

    pub fn mtime(mut self, mtime: i64) -> Self {
        let _ = self.mtime.insert(mtime);
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

    pub fn ufs_mtime(mut self, mtime: i64) -> Self {
        let _ = self.ufs_mtime.insert(mtime);
        self
    }

    pub fn build(self) -> SetAttrOpts {
        SetAttrOpts {
            recursive: self.recursive,
            replicas: self.replicas,
            owner: self.owner,
            group: self.group,
            mode: self.mode,
            atime: self.atime,
            mtime: self.mtime,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            add_x_attr: self.add_x_attr,
            remove_x_attr: self.remove_x_attr,
            ufs_mtime: self.ufs_mtime,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub limit: Option<usize>,
    pub start_after: Option<String>,
}

impl ListOptions {
    pub fn from_status(limit: usize, status: &FileStatus) -> Self {
        Self {
            limit: Some(limit),
            start_after: Some(status.name.to_owned()),
        }
    }
}
