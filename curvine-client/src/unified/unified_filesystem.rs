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

use crate::file::{CurvineFileSystem, FsClient, FsContext};
use crate::rpc::JobMasterClient;
use crate::unified::{MountCache, MountValue, UnifiedReader, UnifiedWriter};
use crate::ClientMetrics;
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    ConsistencyStrategy, FileStatus, LoadJobCommand, LoadJobResult, MasterInfo, MountInfo,
    MountOptions, SetAttrOpts,
};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::runtime::Runtime;
use orpc::{err_box, err_ext};
use std::sync::Arc;

#[derive(Clone, Copy, PartialOrd, PartialEq, Debug)]
enum CacheValidity {
    Valid,
    Invalid,
}

impl CacheValidity {
    fn is_valid(&self) -> bool {
        self == &CacheValidity::Valid
    }
}

#[derive(Clone)]
pub struct UnifiedFileSystem {
    cv: CurvineFileSystem,
    mount_cache: Arc<MountCache>,
    enable_unified: bool,
    enable_read_ufs: bool,
    metrics: &'static ClientMetrics,
}

impl UnifiedFileSystem {
    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let update_interval = conf.client.mount_update_ttl;
        let enable_unified = conf.client.enable_unified_fs;
        let enable_read_ufs = conf.client.enable_read_ufs;

        let cv = CurvineFileSystem::with_rt(conf, rt.clone())?;
        let fs = UnifiedFileSystem {
            cv,
            mount_cache: Arc::new(MountCache::new(update_interval.as_millis() as u64)),
            enable_unified,
            enable_read_ufs,
            metrics: FsContext::get_metrics(),
        };

        Ok(fs)
    }

    pub fn cv(&self) -> CurvineFileSystem {
        self.cv.clone()
    }

    pub fn fs_client(&self) -> Arc<FsClient> {
        self.cv.fs_client()
    }

    // Check if the path is a mount point, if so, return the mount point information
    async fn get_mount(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        if !path.is_cv() {
            return err_box!("path is not curvine path");
        }

        if !self.enable_unified {
            return Ok(None);
        }

        let state = self.mount_cache.get_mount(self, path).await?;
        if let Some(mnt) = state {
            let ufs_path = mnt.get_ufs_path(path)?;
            Ok(Some((ufs_path, mnt)))
        } else {
            Ok(None)
        }
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
        self.cv.mount(ufs_path, cv_path, opts).await?;
        self.mount_cache.check_update(self, true).await?;
        Ok(())
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<()> {
        self.cv.umount(cv_path).await?;
        self.mount_cache.remove(cv_path);
        Ok(())
    }

    // According to cv_path, find the mount point and get the corresponding ufs_path path.
    pub async fn get_ufs_path(&self, path: &Path) -> FsResult<Option<String>> {
        match self.get_mount(path).await? {
            Some((ufs_path, _)) => Ok(Some(ufs_path.to_string())),
            None => Ok(None),
        }
    }

    pub async fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.cv.get_mount_info(path).await
    }

    pub async fn get_mount_info_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.cv.get_mount_info_bytes(path).await
    }

    pub async fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        self.cv.get_mount_table().await
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.cv.clone_runtime()
    }

    // If the path lies outside the mount point, the operation behaves as a full delete.
    // If it's within the mount point, only the associated cache files will be removed. (ufs will be ignored)
    pub async fn free(&self, path: &Path, recursive: bool) -> FsResult<()> {
        self.cv.delete(path, recursive).await
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        self.cv.symlink(target, link, force).await
    }

    async fn get_cache_validity(
        &self,
        cv_path: &Path,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<CacheValidity> {
        let cv_status = match self.get_cache_status(cv_path).await? {
            Some(v) => v,
            None => return Ok(CacheValidity::Invalid),
        };

        if mount.info.consistency_strategy == ConsistencyStrategy::None {
            Ok(CacheValidity::Valid)
        } else {
            let ufs_status = mount.ufs.get_status(ufs_path).await?;
            if cv_status.len == ufs_status.len
                && cv_status.storage_policy.ufs_mtime != 0
                && cv_status.storage_policy.ufs_mtime == ufs_status.mtime
            {
                Ok(CacheValidity::Valid)
            } else {
                Ok(CacheValidity::Invalid)
            }
        }
    }

    pub async fn async_cache(&self, source_path: &Path) -> FsResult<LoadJobResult> {
        let client = JobMasterClient::new(self.fs_client());
        let command = LoadJobCommand::builder(source_path.clone_uri()).build();
        client.submit_load_job(command).await
    }

    pub async fn cleanup(&self) {
        self.cv.cleanup().await
    }
}

impl FileSystem<UnifiedWriter, UnifiedReader, ClusterConf> for UnifiedFileSystem {
    fn conf(&self) -> &ClusterConf {
        self.cv.conf()
    }

    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        match self.get_mount(path).await? {
            None => self.cv.mkdir(path, create_parent).await,
            Some((ufs_path, mount)) => mount.ufs.mkdir(&ufs_path, create_parent).await,
        }
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<UnifiedWriter> {
        match self.get_mount(path).await? {
            None => Ok(UnifiedWriter::Cv(self.cv.create(path, overwrite).await?)),
            Some((ufs_path, mount)) => mount.ufs.create(&ufs_path, overwrite).await,
        }
    }

    async fn append(&self, path: &Path) -> FsResult<UnifiedWriter> {
        match self.get_mount(path).await? {
            None => Ok(UnifiedWriter::Cv(self.cv.append(path).await?)),
            Some((ufs_path, mount)) => mount.ufs.append(&ufs_path).await,
        }
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        match self.get_mount(path).await? {
            None => self.cv.exists(path).await,
            Some((ufs_path, mount)) => mount.ufs.exists(&ufs_path).await,
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
            self.metrics
                .mount_cache_hits
                .with_label_values(&[mount.mount_id()])
                .inc();

            return Ok(UnifiedReader::Cv(self.cv.open(path).await?));
        } else {
            self.metrics
                .mount_cache_misses
                .with_label_values(&[mount.mount_id()])
                .inc();
        }

        if mount.info.auto_cache() {
            match self.async_cache(&ufs_path).await {
                Err(e) => warn!("Submit async cache error for {}: {}", ufs_path, e),
                Ok(res) => info!(
                    "Submit async cache successfully for {}, job id {}, target_path {}",
                    ufs_path, res.job_id, res.target_path
                ),
            }
        }

        // Reading from ufs
        if self.enable_read_ufs {
            info!("Read from ufs, ufs path {}, cv path: {}", ufs_path, path);
            mount.ufs.open(&ufs_path).await
        } else {
            err_ext!(FsError::unsupported_ufs_read(path.path()))
        }
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        match self.get_mount(src).await? {
            None => self.cv.rename(src, dst).await,
            Some((src_ufs, mount)) => {
                let dst_ufs = mount.get_ufs_path(dst)?;
                let _ = mount.ufs.rename(&src_ufs, &dst_ufs).await?;

                if self.cv.exists(src).await? {
                    self.cv.delete(src, true).await?;
                }

                Ok(true)
            }
        }
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        match self.get_mount(path).await? {
            None => self.cv.delete(path, recursive).await,
            Some((ufs_path, mount)) => {
                // delete cache
                if self.cv.exists(path).await? {
                    self.cv.delete(path, recursive).await?;
                }

                // delete ufs
                mount.ufs.delete(&ufs_path, recursive).await
            }
        }
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        match self.get_mount(path).await? {
            None => self.cv.get_status(path).await,
            Some((ufs_path, mount)) => {
                if mount.info.cv_path == path.path() {
                    return self.cv.get_status(path).await;
                }
                mount.ufs.get_status(&ufs_path).await
            }
        }
    }

    async fn get_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        match self.get_mount(path).await? {
            None => self.cv.get_status_bytes(path).await,
            Some((ufs_path, mount)) => {
                if mount.info.cv_path == path.path() {
                    return self.cv.get_status_bytes(path).await;
                }
                mount.ufs.get_status_bytes(&ufs_path).await
            }
        }
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        match self.get_mount(path).await? {
            None => self.cv.list_status(path).await,
            Some((ufs_path, mount)) => mount.ufs.list_status(&ufs_path).await,
        }
    }

    async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        match self.get_mount(path).await? {
            None => self.cv.list_status_bytes(path).await,
            Some((ufs_path, mount)) => mount.ufs.list_status_bytes(&ufs_path).await,
        }
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        self.cv.set_attr(path, opts).await
    }
}
