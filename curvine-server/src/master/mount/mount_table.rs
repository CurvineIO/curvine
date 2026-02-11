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
use log::{error, info, warn};
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
        let mounts = match self.fs_dir.read().get_mount_table() {
            Ok(mounts) => mounts,
            Err(e) => {
                error!(
                    "mount restore failed: unable to load mount table from metadata store, err={}",
                    e
                );
                return;
            }
        };

        let total = mounts.len();
        if total == 0 {
            info!("mount restore completed: no entries found");
            return;
        }
        info!(
            "mount restore started: {} entries loaded from metadata store",
            total
        );

        let mut failed = 0usize;

        for mnt in mounts {
            if let Err(e) = self.unprotected_add_mount(mnt) {
                failed += 1;
                error!("mount restore entry failed: err={}", e);
            }
        }
        let restored = total - failed;

        if failed == 0 {
            info!(
                "mount restore completed successfully: restored={}, failed={}",
                restored, failed
            );
        } else {
            warn!(
                "mount restore completed with errors: restored={}, failed={}",
                restored, failed
            );
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

    pub fn mount_path_exists(&self, cv_path: &str) -> bool {
        self.mount_point_inuse(cv_path)
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
            return err_box!("{} already exists in mount table", cv_path);
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

    pub fn unprotected_umount_by_id(&self, mount_id: u32) -> FsResult<()> {
        let mut inner = self.inner.write().unwrap();

        let info = match inner.mountid2entry.remove(&mount_id) {
            Some(entry) => entry,
            None => return err_box!("failed found {} entry", mount_id),
        };

        inner.ufs2mountid.remove(&info.ufs_path);
        inner.mountpath2id.remove(&info.cv_path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::MountTable;
    use crate::master::fs::MasterFilesystem;
    use crate::master::journal::JournalSystem;
    use crate::master::Master;
    use curvine_common::conf::ClusterConf;
    use curvine_common::fs::Path;
    use curvine_common::state::MountOptions;
    use orpc::common::Utils;
    use orpc::CommonResult;
    use std::collections::HashMap;

    struct TableEnv {
        _conf: ClusterConf,
        _js: JournalSystem,
        table: MountTable,
    }

    fn new_table_env(name: &str, format_master: bool) -> TableEnv {
        Master::init_test_metrics();

        let mut conf = ClusterConf {
            testing: true,
            ..Default::default()
        };
        conf.change_test_meta_dir(format!("mount-table-unit-test/{}", name));
        conf.format_master = format_master;
        conf.journal.enable = false;

        let js = JournalSystem::from_conf(&conf).unwrap();
        let fs = MasterFilesystem::with_js(&conf, &js);
        let table = MountTable::new(fs.fs_dir.clone());

        TableEnv {
            _conf: conf,
            _js: js,
            table,
        }
    }

    #[test]
    fn test_get_ufs_conf_and_lookup_errors() -> CommonResult<()> {
        let env = new_table_env(&format!("ufs-conf-{}", Utils::rand_str(6)), true);

        let mut props = HashMap::new();
        props.insert("ak".to_string(), "sk".to_string());
        let opts = MountOptions::builder().set_properties(props).build();
        env.table.add_mount(100, "/m", "oss://b/m", &opts)?;

        let conf = env.table.get_ufs_conf(&"oss://b/m".to_string())?;
        assert_eq!(conf.get("ak"), Some("sk"));

        let err = env
            .table
            .get_ufs_conf(&"oss://b/not-exists".to_string())
            .unwrap_err();
        assert!(err.to_string().contains("failed found"));

        Ok(())
    }

    #[test]
    fn test_get_mount_info_none_and_umount_missing() -> CommonResult<()> {
        let env = new_table_env(&format!("none-{}", Utils::rand_str(6)), true);
        let opts = MountOptions::builder().build();
        env.table.add_mount(200, "/cv/x", "oss://bucket/x", &opts)?;

        let not_found = env.table.get_mount_info(&Path::from_str("/other/path")?)?;
        assert!(not_found.is_none());

        let err = env.table.umount("/cv/not-found").unwrap_err();
        assert!(err.to_string().contains("failed found"));

        Ok(())
    }

    #[test]
    fn test_get_mount_info_by_id_error_and_restore() -> CommonResult<()> {
        let name = format!("restore-{}", Utils::rand_str(6));
        {
            let env = new_table_env(&name, true);
            let opts = MountOptions::builder().build();
            env.table
                .add_mount(300, "/restore/a", "oss://restore/a", &opts)?;
        }

        let env2 = new_table_env(&name, false);
        assert!(env2.table.get_mount_table()?.is_empty());
        env2.table.restore();
        assert_eq!(env2.table.get_mount_table()?.len(), 1);

        let err = env2.table.get_mount_info_by_id(999999).unwrap_err();
        assert!(err.to_string().contains("failed found"));

        Ok(())
    }
}
