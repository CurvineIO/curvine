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
use curvine_common::error::FsError;
use curvine_common::fs::{self, CurvineURI, Path};
use curvine_common::state::{MkdirOpts, MountInfo, MountOptions};
use curvine_common::FsResult;
use log::info;
use orpc::{err_box, try_option};
use std::collections::HashMap;

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
    pub fn restore(&self) {
        self.mount_table.restore();
    }

    fn validate(&self, mount_path: &str, ufs_path: &str) -> FsResult<()> {
        if mount_path == "/" {
            return err_box!("mount point can not be root");
        }

        //mount table check
        if self.mount_table.exists(ufs_path) {
            return err_box!("mount point {} already exists in mount table", ufs_path);
        }

        let exists_path = self.mount_table.ufs_path_conflict(ufs_path);
        if let Some(exists_path) = exists_path {
            return err_box!(
                "ufs path conflict, {} already exists in mount table",
                exists_path
            );
        }

        let exists_path = self.mount_table.mnt_path_conflict(ufs_path);
        if let Some(exists_path) = exists_path {
            return err_box!(
                "mount path conflict, {} already exists in mount table",
                exists_path
            );
        }
        Ok(())
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
        //step1
        self.validate(mount_path, ufs_path)?;

        //step2
        let _ = self.create_mount_point(mount_path)?;

        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.mount_table.assign_mount_id()?,
        };

        //step3
        self.mount_table
            .add_mount(assign_id, mount_path, ufs_path, mnt_opt)
    }

    fn update_mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if !self.mount_table.exists(cv_path) {
            return err_box!("update mode: mount point {} does not exist", ufs_path);
        }

        self.mount_table.umount(cv_path)?;

        self.create_mount_point(cv_path)?;

        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.mount_table.assign_mount_id()?,
        };

        self.mount_table
            .add_mount(assign_id, cv_path, ufs_path, mnt_opt)
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
            return self.update_mount(mnt_id, cv_path, ufs_path, mnt_opt);
        }

        self.add_mount(mnt_id, cv_path, cv_path, mnt_opt)
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

    /**
     * use ufs_uri to find mount entry
     */
    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.mount_table.get_mount_entry(path)
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
