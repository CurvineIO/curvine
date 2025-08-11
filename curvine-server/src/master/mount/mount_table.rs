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

use crate::master::mount::MountPointEntry;
use crate::master::SyncFsDir;
use curvine_common::conf::{UfsConf, UfsConfBuilder};
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::proto::MountOptions;
use curvine_common::FsResult;
use log::info;
use orpc::{err_box, try_option};
use rand::Rng;
use std::collections::HashMap;
use std::convert::Into;
use std::sync::RwLock;

pub struct MountTableInner {
    ufs2mountid: HashMap<String, u32>,
    mountid2entry: HashMap<u32, MountPointEntry>,
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
                // Creating a MountOptions Object
                let mount_options = mnt.to_mount_options();
                let _ =
                    self.insert_mount_table(mnt.id, &mnt.curvine_uri, &mnt.ufs_uri, &mount_options);
                info!("recovering {} mount to {}", mnt.ufs_uri, mnt.curvine_uri);
            }
        } else {
            info!("mount table restore nothing.")
        }
    }

    // ufs maybe mounted already or has prefix overlap with existing mounts
    pub fn exists(&self, ufs_uri: &String) -> bool {
        let inner = self.inner.read().unwrap();

        // full match check
        if inner.ufs2mountid.contains_key(ufs_uri) {
            return true;
        }

        false
    }

    pub fn ufs_path_conflict(&self, ufs_uri: &String) -> Option<String> {
        // prefix check
        let inner = self.inner.read().unwrap();
        for existing_uri in inner.ufs2mountid.keys() {
            //add '/' suffix for mnt_uri and existing_uri
            //avoid 's3://abcde' s3://abc conflict, this is two different path
            //after add '/', 's3://abcde/' s3://abc/ won't conflict
            let ufs_uri = if ufs_uri.ends_with("/") {
                ufs_uri.clone()
            } else {
                format!("{}/", ufs_uri)
            };

            let existing_uri = if existing_uri.ends_with("/") {
                existing_uri.clone()
            } else {
                format!("{}/", existing_uri)
            };

            if ufs_uri.starts_with(&existing_uri) {
                return Some(existing_uri.clone());
            }

            if existing_uri.starts_with(&ufs_uri) {
                return Some(existing_uri.clone());
            }
        }

        None
    }

    pub fn mnt_path_conflict(&self, mnt_uri: &String) -> Option<String> {
        // prefix check
        let inner = self.inner.read().unwrap();
        for existing_uri in inner.mountpath2id.keys() {
            //mnt path is root, skip this prefix
            if existing_uri.eq("/") {
                continue;
            }

            //add '/' suffix for mnt_uri and existing_uri
            //avoid '/abcde' and '/abc' conflict, this is two different path
            //after add '/', '/abcde/' and '/abc/' won't conflict
            let mnt_uri = if mnt_uri.ends_with("/") {
                mnt_uri.clone()
            } else {
                format!("{}/", mnt_uri)
            };

            let existing_uri = if existing_uri.ends_with("/") {
                existing_uri.clone()
            } else {
                format!("{}/", existing_uri)
            };

            if mnt_uri.starts_with(&existing_uri) {
                return Some(existing_uri.clone());
            }

            if existing_uri.starts_with(&mnt_uri) {
                return Some(existing_uri.clone());
            }
        }

        None
    }

    // mountid maybe occupied
    fn has_mounted(&self, mount_id: u32) -> bool {
        let inner = self.inner.read().unwrap();
        inner.mountid2entry.contains_key(&mount_id)
    }

    // mount_path maybe mounted by other ufs
    fn mount_point_inuse(&self, mount_path: &String) -> bool {
        let inner = self.inner.read().unwrap();
        inner.mountpath2id.contains_key(mount_path)
    }

    pub fn insert_mount_table(
        &self,
        mount_id: u32,
        mnt_path: &String,
        ufs_path: &String,
        mnt_opt: &MountOptions,
    ) -> FsResult<MountPointEntry> {
        if self.exists(ufs_path) {
            return err_box!("{} already exists in mount table", ufs_path);
        }

        if self.mount_point_inuse(mnt_path) {
            return err_box!("{} already exists in mount table", ufs_path);
        }

        let mut inner = self.inner.write().unwrap();
        inner.ufs2mountid.insert(ufs_path.clone(), mount_id);
        inner.mountpath2id.insert(mnt_path.clone(), mount_id);

        let mount_entry = MountPointEntry::new(
            mount_id,
            mnt_path.clone(),
            ufs_path.clone(),
            mnt_opt.clone(),
        );
        info!("mount entry is {:?}", mount_entry);
        inner.mountid2entry.insert(mount_id, mount_entry.clone());

        Ok(mount_entry)
    }

    pub fn add_mount(
        &self,
        mount_id: u32,
        mnt_path: &String,
        ufs_path: &String,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        let mount_entry = self.insert_mount_table(mount_id, mnt_path, ufs_path, mnt_opt)?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.mount(mount_entry)?;
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
        Err(FsError::common("failed assign mount id"))
    }

    pub fn umount(&self, mount_path: &String) -> FsResult<()> {
        let mut inner = self.inner.write().unwrap();

        let mount_id = match inner.mountpath2id.get(mount_path) {
            Some(&id) => id,
            None => return err_box!("failed found {} to umount", mount_path),
        };

        let ufs_path = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => entry.ufs_uri.clone(),
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

    pub fn get_mount_entry(&self, path: &Path) -> FsResult<Option<MountPointEntry>> {
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

    pub fn get_mount_entry_by_id(&self, mount_id: u32) -> FsResult<MountPointEntry> {
        let inner = self.inner.read().unwrap();
        let entry = match inner.mountid2entry.get(&mount_id) {
            Some(entry) => entry.clone(),
            None => return err_box!("failed found {} entry", mount_id),
        };
        Ok(entry)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountPointEntry>> {
        let inner = self.inner.read().unwrap();
        let table = inner.mountid2entry.values().cloned().collect();
        Ok(table)
    }
}
