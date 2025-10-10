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
use crate::fs::Path;
use crate::state::{StorageType, TtlAction};
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::{err_box, CommonError, CommonResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[repr(i32)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    FromPrimitive,
    IntoPrimitive,
    Default,
    Deserialize,
    Serialize,
)]
pub enum MountType {
    #[default]
    Cst = 0,
    Orch = 1,
}

impl TryFrom<&str> for MountType {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let typ = match value.to_uppercase().as_str() {
            "CST" => MountType::Cst,
            "ORCH" => MountType::Orch,
            _ => return err_box!("invalid mount type: {}", value),
        };

        Ok(typ)
    }
}

#[repr(i32)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    FromPrimitive,
    IntoPrimitive,
    Default,
    Deserialize,
    Serialize,
)]
pub enum ConsistencyStrategy {
    #[default]
    None = 0,
    Always = 1,
}

impl TryFrom<&str> for ConsistencyStrategy {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let typ = match value.to_uppercase().as_str() {
            "NONE" => ConsistencyStrategy::None,
            "ALWAYS" => ConsistencyStrategy::Always,
            _ => return err_box!("invalid strategy type: {}", value),
        };

        Ok(typ)
    }
}

/// Mount information structure
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub struct MountInfo {
    pub cv_path: String,
    pub ufs_path: String,
    pub mount_id: u32,
    pub properties: HashMap<String, String>,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,
    pub consistency_strategy: ConsistencyStrategy,
    pub storage_type: Option<StorageType>,
    pub block_size: Option<i64>,
    pub replicas: Option<i32>,
    pub mount_type: MountType,
    pub quota_rate: Option<f64>,
}

impl MountInfo {
    pub fn auto_cache(&self) -> bool {
        self.ttl_ms > 0
    }

    pub fn get_ttl(&self) -> Option<String> {
        if self.auto_cache() {
            None
        } else {
            Some(format!("{}s", self.ttl_ms / 1000))
        }
    }

    pub fn get_ufs_path(&self, path: &Path) -> CommonResult<Path> {
        if !path.is_cv() {
            return err_box!("path {} is not cv path", path);
        }

        let sub_path = path.path().replacen(&self.cv_path, "", 1);
        Path::from_str(format!("{}/{}", self.ufs_path, sub_path))
    }

    pub fn get_cv_path(&self, path: &Path) -> CommonResult<Path> {
        if path.is_cv() {
            return err_box!("path {} is not ufs path", path);
        }

        let sub_path = path.full_path().replacen(&self.ufs_path, "", 1);
        Path::from_str(format!("{}/{}", self.cv_path, sub_path))
    }

    pub fn toggle_path(&self, path: &Path) -> CommonResult<Path> {
        if path.is_cv() {
            self.get_ufs_path(path)
        } else {
            self.get_cv_path(path)
        }
    }
}

#[derive(Debug, Clone)]
pub struct MountOptions {
    pub update: bool,
    pub add_properties: HashMap<String, String>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub consistency_strategy: Option<ConsistencyStrategy>,
    pub storage_type: Option<StorageType>,
    pub block_size: Option<i64>,
    pub replicas: Option<i32>,
    pub mount_type: MountType,
    pub remove_properties: Vec<String>,
    pub quota_rate: Option<f64>,
}

impl MountOptions {
    /// Create a new MountOptionsBuilder
    pub fn builder() -> MountOptionsBuilder {
        MountOptionsBuilder::new()
    }

    pub fn to_info(self, mount_id: u32, cv_path: &str, ufs_path: &str) -> MountInfo {
        MountInfo {
            cv_path: cv_path.to_string(),
            ufs_path: ufs_path.to_string(),
            mount_id,
            properties: self.add_properties,
            ttl_ms: self.ttl_ms.unwrap_or(0),
            ttl_action: self.ttl_action.unwrap_or(TtlAction::None),
            consistency_strategy: self
                .consistency_strategy
                .unwrap_or(ConsistencyStrategy::None),
            storage_type: self.storage_type,
            block_size: self.block_size,
            replicas: self.replicas,
            mount_type: self.mount_type,
            quota_rate: self.quota_rate,
        }
    }
}

#[derive(Default)]
pub struct MountOptionsBuilder {
    update: bool,
    add_properties: HashMap<String, String>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    consistency_strategy: Option<ConsistencyStrategy>,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    mount_type: MountType,
    remove_properties: Vec<String>,
    quota_rate: Option<f64>,
}

impl MountOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_conf(conf: &ClientConf, update: bool) -> Self {
        let builder = Self::new();
        if update {
            return builder;
        }

        builder.ttl_ms(conf.ttl_ms).ttl_action(conf.ttl_action)
    }

    pub fn update(mut self, update: bool) -> Self {
        self.update = update;
        self
    }

    pub fn add_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.add_properties.insert(key.into(), value.into());
        self
    }

    pub fn set_properties(mut self, props: HashMap<String, String>) -> Self {
        self.add_properties = props;
        self
    }

    pub fn ttl_ms(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = Some(ttl_ms);
        self
    }

    pub fn ttl_action(mut self, ttl_action: TtlAction) -> Self {
        self.ttl_action = Some(ttl_action);
        self
    }

    pub fn consistency_strategy(mut self, strategy: ConsistencyStrategy) -> Self {
        self.consistency_strategy = Some(strategy);
        self
    }

    pub fn storage_type(mut self, storage_type: StorageType) -> Self {
        self.storage_type = Some(storage_type);
        self
    }

    pub fn block_size(mut self, block_size: i64) -> Self {
        self.block_size = Some(block_size);
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        self.replicas = Some(replicas);
        self
    }

    pub fn mount_type(mut self, mount_type: MountType) -> Self {
        self.mount_type = mount_type;
        self
    }

    pub fn remove_property(mut self, property: impl Into<String>) -> Self {
        self.remove_properties.push(property.into());
        self
    }

    pub fn quota_rate(mut self, quota_rate: f64) -> Self {
        self.quota_rate = Some(quota_rate);
        self
    }

    pub fn build(self) -> MountOptions {
        MountOptions {
            update: self.update,
            add_properties: self.add_properties,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            consistency_strategy: self.consistency_strategy,
            storage_type: self.storage_type,
            block_size: self.block_size,
            replicas: self.replicas,
            mount_type: self.mount_type,
            remove_properties: self.remove_properties,
            quota_rate: self.quota_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::Path;
    use crate::state::MountInfo;

    #[test]
    fn test_path_cst() {
        let info = MountInfo {
            ufs_path: "s3://spark/test1".to_string(),
            cv_path: "/spark/test1".to_string(),
            ..Default::default()
        };
        let path = Path::from_str("s3://spark/test1/1.csv").unwrap();
        assert_eq!(
            info.get_cv_path(&path).unwrap().full_path(),
            "/spark/test1/1.csv"
        );

        let path = Path::from_str("s3://spark/test1/test/dt=2025/1.csv").unwrap();
        assert_eq!(
            info.get_cv_path(&path).unwrap().full_path(),
            "/spark/test1/test/dt=2025/1.csv"
        );

        let path = Path::from_str("/spark/test1/1.csv").unwrap();
        assert_eq!(
            info.get_ufs_path(&path).unwrap().full_path(),
            "s3://spark/test1/1.csv"
        );

        let path = Path::from_str("/spark/test1/test/dt=2025/1.csv").unwrap();
        assert_eq!(
            info.get_ufs_path(&path).unwrap().full_path(),
            "s3://spark/test1/test/dt=2025/1.csv"
        );
    }

    #[test]
    fn test_path_arch() {
        let info = MountInfo {
            ufs_path: "s3://spark/a/b".to_string(),
            cv_path: "/my".to_string(),
            ..Default::default()
        };
        let path = Path::from_str("s3://spark/a/b/1.csv").unwrap();
        assert_eq!(info.get_cv_path(&path).unwrap().full_path(), "/my/1.csv");

        let path = Path::from_str("s3://spark/a/b/c/dt=2025/1.csv").unwrap();
        assert_eq!(
            info.get_cv_path(&path).unwrap().full_path(),
            "/my/c/dt=2025/1.csv"
        );

        let path = Path::from_str("/my/1.csv").unwrap();
        assert_eq!(
            info.get_ufs_path(&path).unwrap().full_path(),
            "s3://spark/a/b/1.csv"
        );

        let path = Path::from_str("/my/c/dt=2025/1.csv").unwrap();
        assert_eq!(
            info.get_ufs_path(&path).unwrap().full_path(),
            "s3://spark/a/b/c/dt=2025/1.csv"
        );
    }
}
