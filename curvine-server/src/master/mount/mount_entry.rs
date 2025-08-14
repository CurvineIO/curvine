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

use crate::master::mount::ConsistencyStrategy;
use curvine_common::proto::MountOptions;
use curvine_common::proto::MountPointInfo;
use curvine_common::state::StorageType;
use std::collections::HashMap;
use std::fmt;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct MountPointEntry {
    pub(crate) id: u32,
    pub(crate) curvine_uri: String,
    pub(crate) ufs_uri: String,
    pub(crate) properties: HashMap<String, String>,
    //cache related
    pub(crate) auto_cache: bool,
    pub(crate) cache_ttl_secs: Option<u64>,
    pub(crate) consistency_strategy: ConsistencyStrategy,
    pub(crate) block_size: Option<u64>,
    pub(crate) replicas: Option<u32>,
    pub(crate) storage_type: Option<StorageType>,
}

impl MountPointEntry {
    pub fn new(id: u32, curvine_uri: String, ufs_uri: String, mnt_opt: MountOptions) -> Self {
        let consistency_conf = mnt_opt.consistency_config.unwrap_or_default();
        let strategy = ConsistencyStrategy::from(consistency_conf);
        let block_size = mnt_opt.block_size;
        let replicas = mnt_opt.replicas;
        let storage_type = mnt_opt.storage_type.map(|x| StorageType::from(x));

        MountPointEntry {
            id,
            curvine_uri,
            ufs_uri,
            properties: mnt_opt.properties,
            auto_cache: mnt_opt.auto_cache,
            cache_ttl_secs: mnt_opt.cache_ttl_secs,
            consistency_strategy: strategy,
            block_size,
            replicas,
            storage_type,
        }
    }

    pub fn to_mount_options(&self) -> MountOptions {
        MountOptions {
            update: false,
            properties: self.properties.clone(),
            auto_cache: self.auto_cache,
            cache_ttl_secs: self.cache_ttl_secs,
            consistency_config: Some(self.consistency_strategy.into()),
            storage_type: self.storage_type.map(|x| x.into()),
            block_size: self.block_size,
            replicas: self.replicas,
        }
    }
}

impl fmt::Display for MountPointEntry {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "id: {}, curvine_uri: {}, ufs_uri: {}, properties: {:?}, auto_cache: {}, cache_ttl_secs: {:?}, consistency_strategy: {:?}, block_size: {:?}, replicas: {:?}, storage_type: {:?}",
            self.id, self.curvine_uri, self.ufs_uri, self.properties, self.auto_cache, self.cache_ttl_secs, self.consistency_strategy, self.block_size, self.replicas, self.storage_type,
        )
    }
}

impl From<MountPointEntry> for MountPointInfo {
    fn from(val: MountPointEntry) -> Self {
        MountPointInfo {
            curvine_path: val.curvine_uri,
            ufs_path: val.ufs_uri,
            mount_id: val.id,
            properties: val.properties,
            auto_cache: val.auto_cache,
            cache_ttl_secs: val.cache_ttl_secs,
            consistency_config: ConsistencyStrategy::into(val.consistency_strategy),
            storage_type: val.storage_type.map(|x| x.into()),
            block_size: val.block_size,
            replicas: val.replicas,
        }
    }
}
