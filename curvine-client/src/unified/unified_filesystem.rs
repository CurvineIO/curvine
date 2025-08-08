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

use crate::file::CurvineFileSystem;
use crate::unified::{UfsFileSystem, UnifiedReader, UnifiedWriter};
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::proto::MountOptions;
use curvine_common::state::{FileStatus, MasterInfo, MountInfo, SetAttrOpts};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::{FastHashMap, LocalTime};
use orpc::runtime::Runtime;
use orpc::sync::AtomicCounter;
use orpc::{err_ext, CommonResult};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct MountItem {
    info: Arc<MountInfo>,
    ufs: UfsFileSystem,
}

#[derive(Clone)]
struct MountState {
    mounts: Arc<Mutex<FastHashMap<String, MountItem>>>,
    last_update: Arc<AtomicCounter>,
    update_interval: u64,
}

impl MountItem {
    // Get the ufs path of the curvine path
    fn ufs_path(&self, path: &Path) -> CommonResult<Path> {
        // parse real path
        // mnt: /xuen-test s3://flink/xuen-test
        // Path /xuen-test/a -> s3://flink/xuen-test/a
        let sub_path = path.path().replacen(&self.info.curvine_path, "", 1);
        Path::from_str(format!("{}/{}", self.info.ufs_path, sub_path))
    }
}

impl MountState {
    fn new(update_interval: u64) -> Self {
        Self {
            mounts: Arc::new(Mutex::new(FastHashMap::default())),
            last_update: Arc::new(AtomicCounter::new(0)),
            update_interval,
        }
    }

    fn need_update(&self) -> bool {
        LocalTime::mills() > self.update_interval + self.last_update.get()
    }

    async fn check_update(&self, fs: &CurvineFileSystem, force: bool) -> FsResult<()> {
        if self.need_update() || force {
            let mounts = fs.get_all_mounts().await?;
            let mut state = self.mounts.lock().unwrap();
            state.clear();

            for item in mounts {
                let path = Path::from_str(&item.ufs_path)?;
                let ufs = UfsFileSystem::new(&path, item.properties.clone())?;

                state.insert(
                    item.curvine_path.to_owned(),
                    MountItem {
                        info: Arc::new(item),
                        ufs,
                    },
                );
            }

            debug!("update mounts, size {}", state.len());
            self.last_update.set(LocalTime::mills());
            Ok(())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, PartialOrd, PartialEq, Debug)]
enum CacheValidity {
    Valid,
    Invalid,
    NotExists,
}

impl CacheValidity {
    fn is_valid(&self) -> bool {
        self == &CacheValidity::Valid
    }
}

#[derive(Clone)]
pub struct UnifiedFileSystem {
    cv: CurvineFileSystem,
    mount_state: MountState,
    enable_unified: bool,
    enable_read_ufs: bool,
}

impl UnifiedFileSystem {
    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let update_interval = conf.client.mount_update_ttl.as_millis() as u64;
        let enable_unified = conf.client.enable_unified_fs;

        let cv = CurvineFileSystem::with_rt(conf, rt.clone())?;
        let fs = UnifiedFileSystem {
            cv,
            mount_state: MountState::new(update_interval),
            enable_unified,
            enable_read_ufs: true,
        };

        Ok(fs)
    }

    pub fn cv(&self) -> &CurvineFileSystem {
        &self.cv
    }

    // Check if the path is a mount point, if so, return the mount point information
    async fn get_mount(&self, path: &Path) -> FsResult<Option<(Path, MountItem)>> {
        // If unified file system is not enabled, return None
        if !self.enable_unified {
            return Ok(None);
        }

        // Check if the mount point information needs to be updated
        self.mount_state.check_update(&self.cv, false).await?;

        let state = self.mount_state.mounts.lock().unwrap();
        if state.is_empty() {
            return Ok(None);
        }

        // Find the mount point information for the path
        for mount_path in path.get_possible_mounts() {
            if let Some(item) = state.get(&mount_path).cloned() {
                let ufs_path = item.ufs_path(path)?;
                return Ok(Some((ufs_path, item)));
            }
        }

        Ok(None)
    }

    pub async fn get_master_info(&self) -> FsResult<MasterInfo> {
        self.cv.get_master_info().await
    }

    pub async fn get_master_info_bytes(&self) -> FsResult<BytesMut> {
        self.cv.get_master_info_bytes().await
    }

    // Get the file status in curvine. If it does not exist or expired, return None
    pub async fn get_cache_status(&self, path: &Path) -> FsResult<Option<FileStatus>> {
        match self.cv.get_status(path).await {
            Ok(v) => {
                if v.is_complete && !v.is_expired() {
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }

            Err(e) => match e {
                FsError::FileNotFound(_) => Ok(None),
                FsError::Expired(_) => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub async fn mount(&self, ufs_path: &Path, cv_path: &Path, opts: MountOptions) -> FsResult<()> {
        self.cv
            .fs_client
            .mount(ufs_path.full_path(), cv_path.full_path(), opts)
            .await?;
        self.mount_state.check_update(&self.cv, true).await?;
        Ok(())
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<()> {
        self.cv.fs_client.umount(cv_path.path()).await?;
        self.mount_state.check_update(&self.cv, true).await?;
        Ok(())
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.cv.clone_runtime()
    }

    // If the path lies outside the mount point, the operation behaves as a full delete.
    // If it’s within the mount point, only the associated cache files will be removed. (ufs will be ignored)
    pub async fn free(&self, path: &Path, recursive: bool) -> FsResult<()> {
        self.cv.delete(path, recursive).await
    }

    async fn get_cache_validity(
        &self,
        cv_path: &Path,
        ufs_path: &Path,
        mount: &MountItem,
    ) -> FsResult<CacheValidity> {
        let validity = match self.get_cache_status(cv_path).await? {
            Some(cv_status) => {
                let ufs_status = mount.ufs.get_status(&ufs_path).await?;
                if cv_status.len == ufs_status.len
                    && cv_status.storage_policy.ufs_mtime != 0
                    && cv_status.storage_policy.ufs_mtime == ufs_status.mtime
                {
                    CacheValidity::Valid
                } else {
                    CacheValidity::Invalid
                }
            }
            None => CacheValidity::NotExists,
        };
        Ok(validity)
    }
}

impl FileSystem<UnifiedWriter, UnifiedReader, ClusterConf> for UnifiedFileSystem {
    fn conf(&self) -> &ClusterConf {
        self.cv.conf()
    }

    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        match self.get_mount(path).await? {
            None => self.cv.mkdir(path, create_parent).await,
            Some((path, mount)) => mount.ufs.mkdir(&path, create_parent).await,
        }
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<UnifiedWriter> {
        match self.get_mount(path).await? {
            None => Ok(UnifiedWriter::Cv(self.cv.create(path, overwrite).await?)),
            Some((path, mount)) => mount.ufs.create(&path, overwrite).await,
        }
    }

    async fn append(&self, path: &Path) -> FsResult<UnifiedWriter> {
        match self.get_mount(path).await? {
            None => Ok(UnifiedWriter::Cv(self.cv.append(path).await?)),
            Some((path, mount)) => mount.ufs.append(&path).await,
        }
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        match self.get_mount(path).await? {
            None => self.cv.exists(path).await,
            Some((path, mount)) => mount.ufs.exists(&path).await,
        }
    }

    async fn open(&self, path: &Path) -> FsResult<UnifiedReader> {
        let (ufs_path, mount) = match self.get_mount(path).await? {
            None => return Ok(UnifiedReader::Cv(self.cv.open(path).await?)),
            Some(v) => v,
        };

        let read_cache = self.get_cache_validity(path, &ufs_path, &mount).await?;

        // Read data from the curvine cache
        if read_cache.is_valid() {
            info!(
                "Read from Curvine(cache), ufs path {}, cv path: {}",
                ufs_path, path
            );
            return Ok(UnifiedReader::Cv(self.cv.open(path).await?));
        }

        if mount.info.auto_cache {
            match self
                .cv
                .async_cache(&ufs_path, mount.info.get_ttl(), false)
                .await
            {
                Err(e) => warn!("Submit async cache error for {}: {}", ufs_path, e),
                Ok(res) => info!(
                    "Submit async cache successfully for {}, job res {}",
                    ufs_path, res
                ),
            }
        }

        // Reading from ufs
        if self.enable_read_ufs {
            info!("Read from ufs, ufs path {}, cv path: {}", ufs_path, path);
            mount.ufs.open(&ufs_path).await
        } else {
            err_ext!(FsError::file_expired(path.path()))
        }
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        match self.get_mount(src).await? {
            None => self.cv.rename(src, dst).await,
            Some((src_path, mount)) => {
                let dst_path = mount.ufs_path(dst)?;
                let rename_result = mount.ufs.rename(&src_path, &dst_path).await?;
                if rename_result {
                    // to find out the cache status and then rename or delete it
                    let valid = self.get_cache_validity(src, &dst_path, &mount).await?;
                    match valid {
                        CacheValidity::Valid => {
                            self.cv.rename(src, dst).await?;
                        }
                        CacheValidity::Invalid => {
                            self.cv.delete(src, true).await?;
                        }
                        _ => {
                            // ignore
                        }
                    };
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        match self.get_mount(path).await? {
            None => self.cv.delete(path, recursive).await,
            Some((path, mount)) => {
                if (self.get_cache_status(&path).await?).is_some() {
                    self.cv.delete(&path, recursive).await?;
                }
                mount.ufs.delete(&path, recursive).await
            }
        }
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        match self.get_mount(path).await? {
            None => self.cv.get_status(path).await,
            Some((path, mount)) => mount.ufs.get_status(&path).await,
        }
    }

    async fn get_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        match self.get_mount(path).await? {
            None => self.cv.get_status_bytes(path).await,
            Some((path, mount)) => mount.ufs.get_status_bytes(&path).await,
        }
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        match self.get_mount(path).await? {
            None => self.cv.list_status(path).await,
            Some((path, mount)) => mount.ufs.list_status(&path).await,
        }
    }

    async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        match self.get_mount(path).await? {
            None => self.cv.list_status_bytes(path).await,
            Some((path, mount)) => mount.ufs.list_status_bytes(&path).await,
        }
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        self.cv.set_attr(path, opts).await
    }
}
