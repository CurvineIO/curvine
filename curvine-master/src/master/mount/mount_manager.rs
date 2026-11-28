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

#![allow(unused)]
use crate::master::fs::MasterFilesystem;
use crate::master::mount::MountTable;
use crate::master::{self, SyncFsDir};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{self, CurvineURI, Path};
use curvine_model::{MkdirOpts, MountInfo, MountOptions};
use curvine_ufs_api::S3Conf;
use log::info;

pub struct MountManager {
    master_fs: MasterFilesystem,
    mount_table: MountTable,
}

impl MountManager {
    pub fn new(master_fs: MasterFilesystem) -> Self {
        let fs_dir = master_fs.fs_dir.clone();
        MountManager {
            master_fs,
            mount_table: MountTable::new(fs_dir),
        }
    }

    /// recovery mount points from store
    pub fn restore(&self) -> FsResult<()> {
        self.mount_table.restore()
    }

    pub fn restore_best_effort(&self) {
        self.mount_table.restore_best_effort()
    }

    fn create_mount_point(&self, mount_path: &str) -> FsResult<bool> {
        let exist = self.master_fs.exists(mount_path)?;
        if exist {
            return Ok(true);
        }

        let opts = MkdirOpts::with_create(true);
        self.master_fs.mkdir_with_opts(mount_path, opts)?;
        Ok(true)
    }

    fn normalize_mount_config(mount: &mut MountInfo, allow_env_credentials: bool) -> FsResult<()> {
        if mount.write_cache && !mount.write_cache_enabled() {
            return err_box!(
                "write_cache requires cache_mode with read_write access_mode for mount {}",
                mount.cv_path
            );
        }

        let path = Path::from_str(&mount.ufs_path)?;
        if !matches!(path.scheme(), Some("s3" | "s3a")) {
            return Ok(());
        }

        let properties = std::mem::take(&mut mount.properties);
        mount.properties = S3Conf::canonicalize_properties(properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })?;
        S3Conf::validate(&mount.properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })?;
        let credential = S3Conf::credential_plan(&mount.properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })?;

        // `role` and `auto` resolve the identity from the node environment, so the mount would use
        // the cluster's own cloud identity. The mount RPC has no authorization today (#1972), so
        // that capability is opt-in per master.
        if !allow_env_credentials && credential.is_environment_resolved() {
            return err_box!(
                "mount {} resolves its S3 credential from the node environment through {} = {}; \
                 set master.s3_allow_env_credentials = true to allow it",
                mount.ufs_path,
                S3Conf::CRED_TYPE,
                mount
                    .properties
                    .get(S3Conf::CRED_TYPE)
                    .map(String::as_str)
                    .unwrap_or("auto")
            );
        }

        Ok(())
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    fn add_mount(
        &self,
        mnt_id: Option<u32>,
        mount_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.mount_table.assign_mount_id()?,
        };
        let mut mount = mnt_opt.clone().to_info(assign_id, mount_path, ufs_path);
        Self::normalize_mount_config(&mut mount, self.master_fs.conf.s3_allow_env_credentials)?;
        let _ = self.create_mount_point(mount_path)?;

        let mut normalized_options = mnt_opt.clone();
        normalized_options.add_properties = mount.properties;
        self.mount_table
            .add_mount(assign_id, mount_path, ufs_path, &normalized_options)
    }

    fn update_mount(&self, cv_path: &str, mnt_opt: &MountOptions) -> FsResult<()> {
        let path = Path::from_str(cv_path)?;
        let Some(existing) = self.get_mount_info(&path)? else {
            return err_box!("mount point {} not found for update", cv_path);
        };
        let update = Self::canonicalize_update_options(&existing.ufs_path, mnt_opt)?;
        let mut merged = existing.merge_with(update);
        Self::normalize_mount_config(&mut merged, self.master_fs.conf.s3_allow_env_credentials)?;

        self.mount_table.update_mount(merged)
    }

    /// Canonicalize the incoming property spellings before they are merged into the stored mount.
    ///
    /// A stored `MountInfo` already holds canonical keys, so an update that names a key by a legacy
    /// alias — for example rotating a session token through `s3.session_token`, or a role ARN
    /// through `s3.role_arn` — would otherwise leave the old canonical key and the new alias side by
    /// side, and normalization would reject the pair as a conflict although the caller supplied
    /// only one spelling. Conflicts *within* the update are still detected by
    /// `canonicalize_properties`.
    fn canonicalize_update_options(
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<MountOptions> {
        let path = Path::from_str(ufs_path)?;
        if !matches!(path.scheme(), Some("s3" | "s3a")) {
            return Ok(mnt_opt.clone());
        }

        let mut update = mnt_opt.clone();
        let properties = std::mem::take(&mut update.add_properties);
        update.add_properties = S3Conf::canonicalize_properties(properties).map_err(|err| {
            FsError::common(format!("Invalid mount update for {}: {}", ufs_path, err))
        })?;
        update.remove_properties = update
            .remove_properties
            .iter()
            .map(|key| S3Conf::canonical_key(key).to_string())
            .collect();

        Ok(update)
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    pub fn mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if mnt_opt.update {
            return self.update_mount(cv_path, mnt_opt);
        }

        self.add_mount(mnt_id, cv_path, ufs_path, mnt_opt)
    }

    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        self.mount_table.unprotected_add_mount(info)
    }

    pub fn umount(&self, cv_path: &str) -> FsResult<()> {
        self.mount_table.umount(cv_path)
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        let info = self.mount_table.get_mount_info_by_id(id)?;
        self.umount(&info.cv_path)
    }

    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        self.mount_table.unprotected_umount_by_id(id)
    }

    pub fn has_mounted(&self, id: u32) -> FsResult<bool> {
        self.mount_table.has_mounted(id)
    }

    /**
     * use ufs_uri to find mount entry
     */
    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.mount_table.get_mount_info(path)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let table = self.mount_table.get_mount_table()?;

        let mut entries = Vec::new();
        table.iter().for_each(|entry| {
            entries.push(entry.clone());
        });
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::MountManager;
    use curvine_model::{AccessMode, MountInfo, MountOptions, WriteType};

    fn mount_info(write_type: WriteType, access_mode: AccessMode, write_cache: bool) -> MountInfo {
        MountOptions::builder()
            .write_type(write_type)
            .access_mode(access_mode)
            .write_cache(write_cache)
            .build()
            .to_info(1, "/mnt", "file:///tmp/curvine-mount")
    }

    #[test]
    fn normalize_mount_config_accepts_write_cache_for_cache_mode_read_write() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadWrite, true);

        MountManager::normalize_mount_config(&mut info, false).unwrap();
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_read_only_cache_mode() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadOnly, true);

        let err = MountManager::normalize_mount_config(&mut info, false).unwrap_err();
        assert!(err
            .to_string()
            .contains("write_cache requires cache_mode with read_write"));
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_fs_mode() {
        let mut info = mount_info(WriteType::FsMode, AccessMode::ReadWrite, true);

        let err = MountManager::normalize_mount_config(&mut info, false).unwrap_err();
        assert!(err
            .to_string()
            .contains("write_cache requires cache_mode with read_write"));
    }

    fn s3_mount_info(properties: &[(&str, &str)]) -> MountInfo {
        let mut builder = MountOptions::builder();
        for (key, value) in properties {
            builder = builder.add_property(*key, *value);
        }
        builder.build().to_info(1, "/mnt", "s3://bucket/path")
    }

    // `role` and `auto` use the node's own cloud identity, and the mount RPC has no authorization,
    // so the master must refuse them unless the deployment opted in.
    #[test]
    fn normalize_mount_config_gates_environment_credentials() {
        let mut mount = s3_mount_info(&[
            ("s3.endpoint_url", "http://s3.example.com"),
            ("s3.region_name", "cn-test-1"),
            ("s3.credentials.type", "auto"),
        ]);

        let err = MountManager::normalize_mount_config(&mut mount, false).unwrap_err();
        assert!(
            err.to_string().contains("master.s3_allow_env_credentials"),
            "unexpected error: {}",
            err
        );
        MountManager::normalize_mount_config(&mut mount, true)
            .expect("an opted-in master accepts an environment credential");
    }

    #[test]
    fn normalize_mount_config_keeps_static_credentials_ungated() {
        let mut mount = s3_mount_info(&[
            ("s3.endpoint_url", "http://s3.example.com"),
            ("s3.region_name", "cn-test-1"),
            ("s3.credentials.access", "access-key"),
            ("s3.credentials.secret", "secret-key"),
        ]);

        MountManager::normalize_mount_config(&mut mount, false)
            .expect("a static key pair needs no opt-in");
    }

    // A `role` mount carrying its own source key pair uses the caller's credential, not the node
    // identity, so it passes the gate like static/sts; a bare `role` stays gated.
    #[test]
    fn normalize_mount_config_allows_role_with_a_source_key_without_opt_in() {
        let props = [
            ("s3.endpoint_url", "http://s3.example.com"),
            ("s3.region_name", "cn-test-1"),
            ("s3.credentials.type", "role"),
            ("s3.credentials.role_arn", "arn:aws:iam::1:role/ufs"),
        ];

        let mut keyed = s3_mount_info(&props);
        keyed.properties.insert(
            "s3.credentials.access".to_string(),
            "access-key".to_string(),
        );
        keyed.properties.insert(
            "s3.credentials.secret".to_string(),
            "secret-key".to_string(),
        );
        MountManager::normalize_mount_config(&mut keyed, false)
            .expect("a role mount with a source key pair needs no opt-in");

        let mut bare = s3_mount_info(&props);
        let err = MountManager::normalize_mount_config(&mut bare, false).unwrap_err();
        assert!(err.to_string().contains("master.s3_allow_env_credentials"));
    }

    #[test]
    fn normalize_mount_config_accepts_disabled_write_cache() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadOnly, false);

        MountManager::normalize_mount_config(&mut info, false).unwrap();
    }
}
