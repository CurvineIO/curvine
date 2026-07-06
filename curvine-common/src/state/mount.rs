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
use crate::state::{CreateFileOpts, CreateFileOptsBuilder, StoragePolicy, StorageType, TtlAction};
use bincode::Options;
use log::warn;
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::common::DurationUnit;
use orpc::{err_box, CommonError, CommonResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::ErrorKind;

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
pub enum Provider {
    #[default]
    Auto,
    OssHdfs,
    Opendal,
}

impl TryFrom<&str> for Provider {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let typ = match value.to_lowercase().as_str() {
            "auto" => Provider::Auto,
            "oss-hdfs" => Provider::OssHdfs,
            "opendal" => Provider::Opendal,
            _ => return err_box!("invalid provider: {}", value),
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
pub enum AccessMode {
    #[default]
    ReadOnly = 0,
    ReadWrite = 1,
}

impl AccessMode {
    pub fn is_read_only(&self) -> bool {
        matches!(self, AccessMode::ReadOnly)
    }
}

impl TryFrom<&str> for AccessMode {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mode = match value.to_lowercase().as_str() {
            "read_only" => AccessMode::ReadOnly,
            "read_write" => AccessMode::ReadWrite,
            _ => return err_box!("invalid access mode: {}", value),
        };

        Ok(mode)
    }
}

/// Mount information structure
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq, Default)]
pub struct MountInfo {
    pub cv_path: String,
    pub ufs_path: String,
    pub mount_id: u32,
    pub properties: HashMap<String, String>,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,
    pub read_verify_ufs: bool,
    pub storage_type: Option<StorageType>,
    pub block_size: Option<i64>,
    pub replicas: Option<i32>,
    pub write_type: WriteType,
    pub provider: Option<Provider>,
    pub auto_cache: bool,
    pub access_mode: AccessMode,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyMountInfo {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
}

impl From<LegacyMountInfo> for MountInfo {
    fn from(info: LegacyMountInfo) -> Self {
        Self {
            cv_path: info.cv_path,
            ufs_path: info.ufs_path,
            mount_id: info.mount_id,
            properties: info.properties,
            ttl_ms: info.ttl_ms,
            ttl_action: info.ttl_action,
            read_verify_ufs: info.read_verify_ufs,
            storage_type: info.storage_type,
            block_size: info.block_size,
            replicas: info.replicas,
            write_type: info.write_type,
            provider: info.provider,
            auto_cache: true,
            access_mode: AccessMode::ReadOnly,
        }
    }
}

impl MountInfo {
    pub fn decode_persisted(bytes: &[u8]) -> CommonResult<Self> {
        match bincode::deserialize::<Self>(bytes) {
            Ok(info) => Ok(info),
            Err(e) if Self::is_unexpected_eof(&e) => {
                let legacy: LegacyMountInfo = bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .reject_trailing_bytes()
                    .deserialize(bytes)
                    .map_err(|legacy_e| {
                        warn!("legacy MountInfo decode also failed: {}", legacy_e);
                        e
                    })?;
                Ok(legacy.into())
            }
            Err(e) => err_box!(e),
        }
    }

    fn is_unexpected_eof(err: &bincode::Error) -> bool {
        matches!(
            err.as_ref(),
            bincode::ErrorKind::Io(e) if e.kind() == ErrorKind::UnexpectedEof
        )
    }

    pub fn auto_cache(&self) -> bool {
        self.auto_cache && self.ttl_ms > 0
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

    pub fn get_create_opts(&self, conf: &ClientConf) -> CreateFileOpts {
        let opts = CreateFileOptsBuilder::with_conf(conf).build();
        self.merge_create_opts(opts)
    }

    pub fn get_sync_opts(&self, conf: &ClientConf, ufs_mtime: i64, ufs_len: i64) -> CreateFileOpts {
        let opts = CreateFileOptsBuilder::with_conf(conf)
            .ufs_mtime_len(ufs_mtime, ufs_len)
            .build();
        self.merge_create_opts(opts)
    }

    pub fn merge_create_opts(&self, opts: CreateFileOpts) -> CreateFileOpts {
        CreateFileOpts {
            create_parent: true,
            replicas: self.replicas.unwrap_or(opts.replicas as i32) as u16,
            block_size: self.block_size.unwrap_or(opts.block_size),
            file_type: opts.file_type,
            x_attr: opts.x_attr,
            storage_policy: StoragePolicy {
                ttl_ms: self.ttl_ms,
                ttl_action: self.ttl_action,
                storage_type: self
                    .storage_type
                    .unwrap_or(opts.storage_policy.storage_type),
                ufs_mtime: opts.storage_policy.ufs_mtime,
                ..Default::default()
            },
            mode: opts.mode,
            client_name: opts.client_name,
            owner: opts.owner,
            group: opts.group,
            sync_ufs_meta: opts.sync_ufs_meta,
            ufs_len: opts.ufs_len,
        }
    }

    pub fn is_cache_mode(&self) -> bool {
        self.write_type == WriteType::CacheMode
    }

    pub fn is_fs_mode(&self) -> bool {
        self.write_type == WriteType::FsMode
    }

    pub fn is_read_only_cache_mode(&self) -> bool {
        self.is_cache_mode() && self.access_mode.is_read_only()
    }

    pub fn merge_with(self, mnt_opt: MountOptions) -> MountInfo {
        let mut properties = self.properties;

        for (k, v) in mnt_opt.add_properties {
            properties.insert(k, v);
        }

        for k in &mnt_opt.remove_properties {
            properties.remove(k);
        }

        MountInfo {
            cv_path: self.cv_path,
            ufs_path: self.ufs_path,
            mount_id: self.mount_id,
            properties,
            ttl_ms: mnt_opt.ttl_ms.unwrap_or(self.ttl_ms),
            ttl_action: mnt_opt.ttl_action.unwrap_or(self.ttl_action),
            read_verify_ufs: mnt_opt.read_verify_ufs,
            storage_type: mnt_opt.storage_type.or(self.storage_type),
            block_size: mnt_opt.block_size.or(self.block_size),
            replicas: mnt_opt.replicas.or(self.replicas),
            write_type: self.write_type,
            provider: mnt_opt.provider.or(self.provider),
            auto_cache: mnt_opt.auto_cache.unwrap_or(self.auto_cache),
            access_mode: mnt_opt.access_mode.unwrap_or(self.access_mode),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MountOptions {
    pub update: bool,
    pub add_properties: HashMap<String, String>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub read_verify_ufs: bool,
    pub storage_type: Option<StorageType>,
    pub block_size: Option<i64>,
    pub replicas: Option<i32>,
    pub remove_properties: Vec<String>,
    pub write_type: WriteType,
    pub provider: Option<Provider>,
    pub auto_cache: Option<bool>,
    pub access_mode: Option<AccessMode>,
}

impl MountOptions {
    /// Create a new MountOptionsBuilder
    pub fn builder() -> MountOptionsBuilder {
        MountOptionsBuilder::new()
    }

    pub fn to_info(self, mount_id: u32, cv_path: &str, ufs_path: &str) -> MountInfo {
        let ttl_action = match self.write_type {
            WriteType::CacheMode => TtlAction::Delete,
            WriteType::FsMode => TtlAction::Free,
        };

        MountInfo {
            cv_path: cv_path.to_string(),
            ufs_path: ufs_path.to_string(),
            mount_id,
            properties: self.add_properties,
            ttl_ms: self.ttl_ms.unwrap_or(0),
            ttl_action,
            read_verify_ufs: self.read_verify_ufs,
            storage_type: self.storage_type,
            block_size: self.block_size,
            replicas: self.replicas,
            write_type: self.write_type,
            provider: self.provider,
            auto_cache: self.auto_cache.unwrap_or(true),
            access_mode: self.access_mode.unwrap_or_default(),
        }
    }
}

#[derive(Default)]
pub struct MountOptionsBuilder {
    update: bool,
    add_properties: HashMap<String, String>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    remove_properties: Vec<String>,
    write_type: WriteType,
    provider: Option<Provider>,
    auto_cache: Option<bool>,
    access_mode: Option<AccessMode>,
}

impl MountOptionsBuilder {
    pub fn new() -> Self {
        Self {
            write_type: WriteType::CacheMode,
            ttl_ms: Some(7 * DurationUnit::DAY as i64),
            ttl_action: Some(TtlAction::Delete),
            auto_cache: Some(true),
            access_mode: Some(AccessMode::ReadOnly),
            ..Default::default()
        }
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

    pub fn read_verify_ufs(mut self, read_verify_ufs: bool) -> Self {
        self.read_verify_ufs = read_verify_ufs;
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

    pub fn remove_property(mut self, property: impl Into<String>) -> Self {
        self.remove_properties.push(property.into());
        self
    }

    pub fn write_type(mut self, write_type: WriteType) -> Self {
        self.write_type = write_type;
        self
    }

    pub fn provider(mut self, provider: Provider) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn auto_cache(mut self, auto_cache: bool) -> Self {
        self.auto_cache = Some(auto_cache);
        self
    }

    pub fn access_mode(mut self, access_mode: AccessMode) -> Self {
        self.access_mode = Some(access_mode);
        self
    }

    pub fn build(self) -> MountOptions {
        MountOptions {
            update: self.update,
            add_properties: self.add_properties,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            read_verify_ufs: self.read_verify_ufs,
            storage_type: self.storage_type,
            block_size: self.block_size,
            replicas: self.replicas,
            remove_properties: self.remove_properties,
            write_type: self.write_type,
            provider: self.provider,
            auto_cache: self.auto_cache,
            access_mode: self.access_mode,
        }
    }
}

/// Write type for mount operations:
/// - CacheMode: Write data directly to the underlying storage (UFS), bypassing cache.
/// - FsMode: Write data to Curvine filesystem (cache) only.
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
pub enum WriteType {
    #[default]
    CacheMode = 0,
    FsMode = 1,
}

impl TryFrom<&str> for WriteType {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let typ = match value {
            "cache_mode" => WriteType::CacheMode,
            "fs_mode" => WriteType::FsMode,
            _ => return err_box!("invalid write type: {}", value),
        };

        Ok(typ)
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::Path;
    use crate::state::{AccessMode, MountInfo, MountOptions, WriteType};

    #[test]
    fn test_access_mode_parse() {
        assert_eq!(
            AccessMode::try_from("read_only").unwrap(),
            AccessMode::ReadOnly
        );
        assert_eq!(
            AccessMode::try_from("read_write").unwrap(),
            AccessMode::ReadWrite
        );
        assert!(AccessMode::try_from("invalid").is_err());
    }

    #[test]
    fn test_mount_options_default_auto_cache_and_access_mode() {
        let info = MountOptions::builder().build().to_info(1, "/mnt", "s3://b");
        assert!(info.auto_cache);
        assert_eq!(info.access_mode, AccessMode::ReadOnly);
    }

    #[test]
    fn test_mount_info_merge_auto_cache_and_access_mode() {
        let info = MountInfo {
            cv_path: "/mnt".to_string(),
            ufs_path: "s3://b".to_string(),
            auto_cache: true,
            access_mode: AccessMode::ReadOnly,
            ..Default::default()
        };

        let unchanged = info.clone().merge_with(MountOptions::builder().build());
        assert!(unchanged.auto_cache);
        assert_eq!(unchanged.access_mode, AccessMode::ReadOnly);

        let updated = info.merge_with(
            MountOptions::builder()
                .auto_cache(false)
                .access_mode(AccessMode::ReadWrite)
                .build(),
        );
        assert!(!updated.auto_cache);
        assert_eq!(updated.access_mode, AccessMode::ReadWrite);
    }

    #[test]
    fn test_is_fs_mode_resync_guard() {
        // resync is only allowed when mount is fs_mode
        let fs_mode_mount = MountInfo {
            cv_path: "/mnt/fs".to_string(),
            ufs_path: "s3://b/k".to_string(),
            write_type: WriteType::FsMode,
            auto_cache: true,
            ..Default::default()
        };
        assert!(fs_mode_mount.is_fs_mode());
        assert!(!fs_mode_mount.is_cache_mode());

        let cache_mode_mount = MountInfo {
            cv_path: "/mnt/cache".to_string(),
            ufs_path: "s3://b/k".to_string(),
            write_type: WriteType::CacheMode,
            auto_cache: true,
            ..Default::default()
        };
        assert!(!cache_mode_mount.is_fs_mode());
        assert!(cache_mode_mount.is_cache_mode());
    }

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

        let path = Path::from_str("cv://curvine-pro/spark/test1/test/dt=2025/1.csv").unwrap();
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

    #[test]
    fn test_bidirectional_path_conversion() {
        // Mount config: s3://flink/user → /mnt/s3
        let info = MountInfo {
            ufs_path: "s3://flink/user".to_string(),
            cv_path: "/mnt/s3".to_string(),
            ..Default::default()
        };

        // Test 1: UFS → CV (Import) - root level file
        let ufs_path = Path::from_str("s3://flink/user/batch_add_path_migrate_task.py").unwrap();
        let cv_result = info.get_cv_path(&ufs_path).unwrap();
        assert_eq!(
            cv_result.full_path(),
            "/mnt/s3/batch_add_path_migrate_task.py"
        );

        // Test 2: CV → UFS (Export) - root level file
        let cv_path = Path::from_str("/mnt/s3/batch_add_path_migrate_task.py").unwrap();
        let ufs_result = info.get_ufs_path(&cv_path).unwrap();
        assert_eq!(
            ufs_result.full_path(),
            "s3://flink/user/batch_add_path_migrate_task.py"
        );

        // Test 3: UFS → CV (Import) - nested directory
        let ufs_nested = Path::from_str("s3://flink/user/dir1/dir2/file.txt").unwrap();
        let cv_nested = info.get_cv_path(&ufs_nested).unwrap();
        assert_eq!(cv_nested.full_path(), "/mnt/s3/dir1/dir2/file.txt");

        // Test 4: CV → UFS (Export) - nested directory
        let cv_nested = Path::from_str("/mnt/s3/dir1/dir2/file.txt").unwrap();
        let ufs_nested = info.get_ufs_path(&cv_nested).unwrap();
        assert_eq!(ufs_nested.full_path(), "s3://flink/user/dir1/dir2/file.txt");

        // Test 5: UFS → CV (Import) - special characters in path
        let ufs_special =
            Path::from_str("s3://flink/user/test_data/dt=2025-01-30/part-00000.parquet").unwrap();
        let cv_special = info.get_cv_path(&ufs_special).unwrap();
        assert_eq!(
            cv_special.full_path(),
            "/mnt/s3/test_data/dt=2025-01-30/part-00000.parquet"
        );

        // Test 6: CV → UFS (Export) - special characters in path
        let cv_special =
            Path::from_str("/mnt/s3/test_data/dt=2025-01-30/part-00000.parquet").unwrap();
        let ufs_special = info.get_ufs_path(&cv_special).unwrap();
        assert_eq!(
            ufs_special.full_path(),
            "s3://flink/user/test_data/dt=2025-01-30/part-00000.parquet"
        );

        // Test 7: Verify is_cv() detection
        assert!(cv_path.is_cv());
        assert!(!ufs_path.is_cv());

        // Test 8: Round-trip conversion (UFS → CV → UFS)
        let original_ufs = Path::from_str("s3://flink/user/data/test.csv").unwrap();
        let to_cv = info.get_cv_path(&original_ufs).unwrap();
        let back_to_ufs = info.get_ufs_path(&to_cv).unwrap();
        assert_eq!(original_ufs.full_path(), back_to_ufs.full_path());

        // Test 9: Round-trip conversion (CV → UFS → CV)
        let original_cv = Path::from_str("/mnt/s3/data/test.csv").unwrap();
        let to_ufs = info.get_ufs_path(&original_cv).unwrap();
        let back_to_cv = info.get_cv_path(&to_ufs).unwrap();
        assert_eq!(original_cv.full_path(), back_to_cv.full_path());
    }
}
