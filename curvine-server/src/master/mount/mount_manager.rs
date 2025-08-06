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
use curvine_common::proto::{MountOptions, MountPointInfo};
use curvine_common::state::MkdirOpts;
use curvine_common::FsResult;
use log::info;
use orpc::{err_box, try_option};
use std::collections::HashMap;

pub struct MountManager {
    master_fs: Option<MasterFilesystem>,
    mount_table: MountTable,
}

impl MountManager {
    pub fn new(master_fs: MasterFilesystem) -> Self {
        let fs_dir = master_fs.fs_dir();
        MountManager {
            master_fs: Some(master_fs),
            mount_table: MountTable::new(fs_dir),
        }
    }

    pub fn new_partial(fs_dir: SyncFsDir) -> Self {
        MountManager {
            master_fs: None,
            mount_table: MountTable::new(fs_dir),
        }
    }

    pub fn set_master_fs(&mut self, master_fs: MasterFilesystem) {
        self.master_fs = Some(master_fs);
    }

    /// recovery mount points from store
    pub fn restore(&self) {
        self.mount_table.restore();
    }

    fn validate(&self, mount_uri: &CurvineURI, ufs_uri: &CurvineURI) -> FsResult<()> {
        let ufs_path = match ufs_uri.normalize_uri() {
            Some(ufs_path) => ufs_path,
            None => return err_box!("bad ufs path"),
        };

        if mount_uri.is_root() {
            return err_box!("mount point can not be root");
        }

        //mount table check
        if self.mount_table.exists(&ufs_path) {
            return err_box!("mount point alread exists in mount table");
        }

        let exists_path = self.mount_table.ufs_path_conflict(&ufs_path);
        if let Some(exists_path) = exists_path {
            return err_box!(
                "ufs path conclict, {} already exists in mount table",
                exists_path
            );
        }

        let exists_path = self.mount_table.mnt_path_conflict(&ufs_path);
        if let Some(exists_path) = exists_path {
            return err_box!(
                "mount path conclict, {} already exists in mount table",
                exists_path
            );
        }
        Ok(())
    }

    fn create_mount_point(&self, mount_path: &String) -> FsResult<bool> {
        let exist = self.master_fs.as_ref().unwrap().exists(mount_path)?;
        if exist {
            info!("mount point {} already exists", mount_path);
            return Ok(true);
        }

        info!("try create mount point {}", mount_path);
        let opts = MkdirOpts::with_create(true);
        self.master_fs
            .as_ref()
            .unwrap()
            .mkdir_with_opts(mount_path, opts)?;
        Ok(true)
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    fn add_mount(
        &self,
        mnt_id: Option<u32>,
        mount_uri: &CurvineURI,
        ufs_uri: &CurvineURI,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        //step1
        self.validate(mount_uri, ufs_uri)?;
        info!(
            "mount {} to {} validate ok",
            ufs_uri.full_path(),
            mount_uri.full_path()
        );

        let mount_path = mount_uri.path().to_string();
        let ufs_path = try_option!(ufs_uri.normalize_uri()); //already validate
        info!("normalize ufs uri : {}", ufs_path);

        //step2
        let ret = self.create_mount_point(&mount_path);
        match ret {
            Ok(_) => info!("mount point path {} created", mount_path),
            Err(e) => return Err(e),
        }

        let assign_id = match mnt_id {
            Some(id) => Ok(id),
            None => self.mount_table.assign_mount_id(),
        };

        //step3
        match assign_id {
            Ok(mount_id) => {
                info!("assign mount id {}", mount_id);
                self.mount_table
                    .add_mount(mount_id, &mount_path, &ufs_path, mnt_opt)
            }
            Err(e) => Err(e),
        }
    }

    fn update_mount(
        &self,
        mnt_id: Option<u32>,
        mount_uri: &CurvineURI,
        ufs_uri: &CurvineURI,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        let ufs_path = match ufs_uri.normalize_uri() {
            Some(path) => path,
            None => return err_box!("bad ufs path"),
        };

        if !self.mount_table.exists(&ufs_path) {
            return err_box!("update mode: mount point {} does not exist", ufs_path);
        }

        info!(
            "update mode: updating mount {} to {}, new mnt opt {:?}",
            ufs_uri.full_path(),
            mount_uri.full_path(),
            mnt_opt
        );

        let mount_path = mount_uri.path().to_string();

        self.mount_table.umount(&mount_path)?;
        info!("existing mount point {} unmounted for update", mount_path);

        let ret = self.create_mount_point(&mount_path);
        match ret {
            Ok(_) => info!("mount point path {} recreated", mount_path),
            Err(e) => return Err(e),
        }

        // 使用相同的ID重新挂载
        let assign_id = match mnt_id {
            Some(id) => Ok(id),
            None => self.mount_table.assign_mount_id(),
        };

        match assign_id {
            Ok(mount_id) => {
                info!("reassign mount id {} for update", mount_id);
                self.mount_table
                    .add_mount(mount_id, &mount_path, &ufs_path, mnt_opt)
            }
            Err(e) => Err(e),
        }
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    pub fn mount(
        &self,
        mnt_id: Option<u32>,
        mount_uri: &CurvineURI,
        ufs_uri: &CurvineURI,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if mnt_opt.update {
            return self.update_mount(mnt_id, mount_uri, ufs_uri, mnt_opt);
        }

        self.add_mount(mnt_id, mount_uri, ufs_uri, mnt_opt)
    }

    pub fn umount(&self, mount_uri: &CurvineURI) -> FsResult<()> {
        let mount_path = mount_uri.path().to_string();
        info!("try unmount point path {}", mount_path);
        self.mount_table.umount(&mount_path)
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        let mnt = self.mount_table.get_mount_entry_by_id(id);
        match mnt {
            Ok(mnt_entry) => {
                let mnt_uri = CurvineURI::new(mnt_entry.curvine_uri)?;
                self.umount(&mnt_uri)
            }
            Err(e) => err_box!("failed umount by id {}, {}", id, e.to_string()),
        }
    }

    /**
     * use ufs_uri to find mount entry
     */
    pub fn get_mount_point(&self, path: &Path) -> FsResult<Option<MountPointInfo>> {
        let mount_entry = self.mount_table.get_mount_entry(path)?;

        //trans to mountPointInfo
        Ok(mount_entry.map(|entry| entry.into()))
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountPointInfo>> {
        let table = self.mount_table.get_mount_table()?;

        let mut entries = Vec::new();
        table.iter().for_each(|entry| {
            entries.push(entry.clone().into());
        });
        Ok(entries)
    }
}
