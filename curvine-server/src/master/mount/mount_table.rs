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

use crate::master::SyncFsDir;
use curvine_common::conf::{UfsConf, UfsConfBuilder};
use curvine_common::fs::Path;
use curvine_common::state::{MountInfo, MountOptions};
use curvine_common::FsResult;
use log::info;
use orpc::{err_box, try_option};
use rand::Rng;
use std::collections::HashMap;
use std::convert::Into;
use std::sync::RwLock;

pub struct MountTableInner {
    ufs2mountid: HashMap<String, u32>,
    mountid2entry: HashMap<u32, MountInfo>,
    mountpath2id: HashMap<String, u32>,
}

pub struct MountTable {
    inner: RwLock<MountTableInner>,
    fs_dir: SyncFsDir,
}

impl MountTable {
    pub fn new(fs_dir: SyncFsDir) -> Self {
        MountTable {
            inner: RwLock::new(MountTableInner {
                ufs2mountid: HashMap::new(),
                mountid2entry: HashMap::new(),
                mountpath2id: HashMap::new(),
            }),
            fs_dir,
        }
    }

    //for new master node
    pub fn restore(&self) {
        if let Ok(mounts) = self.fs_dir.read().get_mount_table() {
            for mnt in mounts {
                self.unprotected_add_mount(mnt).unwrap();
            }
        }
    }

    // ufs maybe mounted already or has prefix overlap with existing mounts
    pub fn exists(&self, ufs_path: &str) -> bool {
        let inner = self.inner.read().unwrap();

        // full match check
        if inner.ufs2mountid.contains_key(ufs_path) {
            return true;
        }

        false
    }

    pub fn check_conflict(&self, cv_path: &str, ufs_path: &str) -> FsResult<()> {
        let inner = self.inner.read().unwrap();
        for info in inner.mountid2entry.values() {
            if Path::has_prefix(cv_path, &info.cv_path) {
                return err_box!("mount point {} is a prefix of {}", info.cv_path, cv_path);
            }

            if Path::has_prefix(&info.cv_path, cv_path) {
                return err_box!("mount point {} is a prefix of {}", cv_path, info.cv_path);
            }

            if Path::has_prefix(ufs_path, &info.ufs_path) {
                return err_box!("mount point {} is a prefix of {}", info.ufs_path, ufs_path);
            }

            if Path::has_prefix(&info.ufs_path, ufs_path) {
                return err_box!("mount point {} is a prefix of {}", ufs_path, info.ufs_path);
            }
        }

        Ok(())
    }

    // mountid maybe occupied
    fn has_mounted(&self, mount_id: u32) -> bool {
        let inner = self.inner.read().unwrap();
        inner.mountid2entry.contains_key(&mount_id)
    }

    // mount_path maybe mounted by other ufs
    fn mount_point_inuse(&self, cv_path: &str) -> bool {
        let inner = self.inner.read().unwrap();
        inner.mountpath2id.contains_key(cv_path)
    }

    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        info!("add mount: {:?}", info);

        let mut inner = self.inner.write().unwrap();
        inner
            .ufs2mountid
            .insert(info.ufs_path.to_string(), info.mount_id);
        inner
            .mountpath2id
            .insert(info.cv_path.to_string(), info.mount_id);
        inner.mountid2entry.insert(info.mount_id, info);

        Ok(())
    }

    pub fn add_mount(
        &self,
        mount_id: u32,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if self.exists(ufs_path) {
            return err_box!("{} already exists in mount table", ufs_path);
        }

        if self.mount_point_inuse(cv_path) {
            return err_box!("{} already exists in mount table", ufs_path);
        }

        self.check_conflict(cv_path, ufs_path)?;

        let info = mnt_opt.clone().to_info(mount_id, cv_path, ufs_path);
        self.unprotected_add_mount(info.clone())?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.store_mount(info, true)?;
        Ok(())
    }

    pub fn assign_mount_id(&self) -> FsResult<u32> {
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let new_id = rng.gen::<u32>();
            if !self.has_mounted(new_id) {
                return Ok(new_id);
            }
        }

        err_box!("failed assign mount id")
    }

    pub fn umount(&self, mount_path: &str) -> FsResult<()> {
        let mut inner = self.inner.write().unwrap();

        let mount_id = match inner.mountpath2id.get(mount_path) {
            Some(&id) => id,
            None => return err_box!("failed found {} to umount", mount_path),
        };

        let ufs_path = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => entry.ufs_path.clone(),
            None => return err_box!("failed found {} matched mountentry to umount", mount_id),
        };

        inner.ufs2mountid.remove(&ufs_path);
        inner.mountpath2id.remove(mount_path);
        inner.mountid2entry.remove(&mount_id);

        let mut fs_dir = self.fs_dir.write();
        fs_dir.unmount(mount_id)?;

        Ok(())
    }

    /// use ufs_uri to find ufs config
    pub fn get_ufs_conf(&self, ufs_path: &String) -> FsResult<UfsConf> {
        let inner = self.inner.read().unwrap();

        let mount_id = match inner.ufs2mountid.get(ufs_path) {
            Some(&id) => id,
            None => return err_box!("failed found {}", ufs_path),
        };

        let properties = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => &entry.properties,
            None => return err_box!("failed found {} properties", mount_id),
        };

        let mut ufs_conf_builder = UfsConfBuilder::default();
        for (k, v) in properties {
            ufs_conf_builder.add_config(k, v);
        }
        let ufs_conf = ufs_conf_builder.build();
        Ok(ufs_conf)
    }

    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        let list = path.get_possible_mounts();
        let is_cv = path.is_cv();
        let inner = self.inner.read().unwrap();

        for mnt in list {
            let option_id = if is_cv {
                inner.mountpath2id.get(&mnt)
            } else {
                inner.ufs2mountid.get(&mnt)
            };
            if let Some(id) = option_id {
                let entry = try_option!(inner.mountid2entry.get(id));
                return Ok(Some(entry.clone()));
            }
        }

        Ok(None)
    }

    pub fn get_mount_info_by_id(&self, mount_id: u32) -> FsResult<MountInfo> {
        let inner = self.inner.read().unwrap();
        let entry = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => entry.clone(),
            None => return err_box!("failed found {} entry", mount_id),
        };
        Ok(entry)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let inner = self.inner.read().unwrap();
        let table = inner.mountid2entry.values().cloned().collect();
        Ok(table)
    }
}
