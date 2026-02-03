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

use crate::file::{CurvineFileSystem, FsClient, FsContext, FsReader};
use crate::rpc::JobMasterClient;
use crate::unified::{CacheSyncWriter, MountCache, MountValue, UnifiedReader, UnifiedWriter};
use crate::ClientMetrics;
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader};
use curvine_common::state::{
    ConsistencyStrategy, CreateFileOpts, FileAllocOpts, FileLock, FileStatus, LoadJobCommand,
    MasterInfo, MkdirOpts, MkdirOptsBuilder, MountInfo, MountOptions, OpenFlags, SetAttrOpts,
    WriteType,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::TimeSpent;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, err_ext};
use std::sync::Arc;

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
        let enable_read_ufs = conf.client.enable_rust_read_ufs;

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

    pub fn conf(&self) -> &ClusterConf {
        self.cv.conf()
    }

    pub fn cv(&self) -> &CurvineFileSystem {
        &self.cv
    }

    pub fn fs_context(&self) -> &Arc<FsContext> {
        &self.cv.fs_context
    }

    pub fn fs_client(&self) -> Arc<FsClient> {
        self.cv.fs_client()
    }

    // Check if the path is a mount point, if so, return the mount point information
    pub async fn get_mount(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
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
    pub async fn get_cv_status(&self, path: &Path) -> FsResult<Option<FileStatus>> {
        let status = match self.cv.get_status(path).await {
            Ok(v) => v,

            Err(e) => {
                return if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                    err_box!("failed to get status file {}: {}", path, e)
                } else {
                    Ok(None)
                }
            }
        };

        // Return None if expired
        if status.is_expired() {
            return Ok(None);
        }

        // Return Some if:
        // 1. File only exists in Curvine (cv_only), or
        // 2. File is complete (cached from UFS and ready to use)
        if status.is_cv_only() || status.is_complete {
            Ok(Some(status))
        } else {
            // File is being cached but not complete yet
            Ok(None)
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

    pub async fn toggle_path(&self, path: &Path, check_cache: bool) -> FsResult<Option<Path>> {
        if check_cache {
            let state = self.mount_cache.get_mount(self, path).await?;
            if let Some(mnt) = state {
                let toggle_path = mnt.toggle_path(path)?;
                Ok(Some(toggle_path))
            } else {
                Ok(None)
            }
        } else {
            match self.get_mount_info(path).await? {
                Some(mnt) => {
                    let toggle_path = mnt.toggle_path(path)?;
                    Ok(Some(toggle_path))
                }
                None => Ok(None),
            }
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
        match self.get_mount(link).await? {
            None => self.cv.symlink(target, link, force).await,
            Some(_) => err_ext!(FsError::unsupported("symlink")),
        }
    }

    pub async fn link(&self, src_path: &Path, dst_path: &Path) -> FsResult<()> {
        match self.get_mount(src_path).await? {
            None => self.cv.link(src_path, dst_path).await,
            Some(_) => err_ext!(FsError::unsupported("link")),
        }
    }

    pub async fn resize(&self, path: &Path, opts: FileAllocOpts) -> FsResult<()> {
        match self.get_mount(path).await? {
            None => self.cv.resize(path, opts).await,
            Some(_) => err_ext!(FsError::unsupported("resize")),
        }
    }

    async fn get_cv_reader(
        &self,
        cv_path: &Path,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<Option<FsReader>> {
        let reader = match self.cv().open(cv_path).await {
            Ok(reader) => reader,
            Err(e) => {
                return if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                    err_box!("failed to open curvine file {}: {}", cv_path, e)
                } else {
                    Ok(None)
                }
            }
        };

        let cv_status = reader.status();
        if cv_status.is_cv_only() || mount.info.consistency_strategy == ConsistencyStrategy::None {
            Ok(Some(reader))
        } else {
            let ufs_status = mount.ufs.get_status(ufs_path).await?;
            if cv_status.len == ufs_status.len
                && cv_status.storage_policy.ufs_mtime != 0
                && cv_status.storage_policy.ufs_mtime == ufs_status.mtime
            {
                Ok(Some(reader))
            } else {
                Ok(None)
            }
        }
    }

    pub fn async_cache(&self, source_path: &Path) -> FsResult<()> {
        let client = JobMasterClient::new(self.fs_client());
        let source_path = source_path.clone_uri();

        self.fs_context().rt().spawn(async move {
            let command = LoadJobCommand::builder(source_path.clone()).build();
            let res = client.submit_load_job(command).await;
            match res {
                Err(e) => warn!("submit async cache error for {}: {}", source_path, e),
                Ok(res) => info!(
                    "submit async cache successfully for {}, job id {}, target_path {}",
                    source_path, res.job_id, res.target_path
                ),
            }
        });

        Ok(())
    }

    pub async fn wait_job_complete(&self, path: &Path, mark: &str) -> FsResult<()> {
        let client = JobMasterClient::new(self.fs_client());
        let job_id = CommonUtils::create_job_id(path.full_path());

        let res = client.wait_job_complete(job_id, mark).await;
        match res {
            Ok(_) | Err(FsError::JobNotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn cleanup(&self) {
        self.cv.cleanup().await
    }

    pub fn disable_unified(&mut self) {
        self.enable_unified = false
    }

    pub async fn open_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<UnifiedWriter> {
        match self.get_mount(path).await? {
            None => {
                let writer = self.cv.open_with_opts(path, opts, flags).await?;
                Ok(UnifiedWriter::Cv(writer))
            }

            Some((ufs_path, mount)) => match mount.info.write_type {
                WriteType::Cache => {
                    let opts = mount.info.get_create_opts(&self.conf().client);
                    let writer = self.cv.open_with_opts(path, opts, flags).await?;
                    Ok(UnifiedWriter::Cv(writer))
                }

                WriteType::Through => {
                    let writer = if flags.append() {
                        mount.ufs.append(&ufs_path).await?
                    } else {
                        mount.ufs.create(&ufs_path, flags.overwrite()).await?
                    };
                    Ok(writer)
                }

                _ => {
                    // ufs creates an empty file to prevent errors when accessing metadata.
                    mount.ufs.create(&ufs_path, flags.overwrite()).await?;

                    let writer = CacheSyncWriter::new(self, path, &mount, flags).await?;
                    Ok(UnifiedWriter::CacheSync(writer))
                }
            },
        }
    }

    pub async fn mkdir_with_opts(
        &self,
        path: &Path,
        opts: MkdirOpts,
    ) -> FsResult<Option<FileStatus>> {
        match self.get_mount(path).await? {
            None => {
                let status = self.cv.mkdir_with_opts(path, opts).await?;
                Ok(Some(status))
            }

            Some((ufs_path, mount)) => {
                let flag = mount.ufs.mkdir(&ufs_path, opts.create_parent).await?;
                if !flag {
                    err_ext!(FsError::file_exists(ufs_path.path()))
                } else {
                    match self.cv.mkdir_with_opts(path, opts).await {
                        Ok(status) => Ok(Some(status)),
                        Err(e) => {
                            warn!(
                                "failed to create directory in cache for {}, ignoring: {}",
                                path, e
                            );
                            Ok(None)
                        }
                    }
                }
            }
        }
    }

    pub async fn fuse_set_attr(
        &self,
        path: &Path,
        opts: SetAttrOpts,
    ) -> FsResult<Option<FileStatus>> {
        match self.get_mount(path).await? {
            None => {
                let status = self.cv.set_attr(path, opts).await?;
                Ok(Some(status))
            }

            Some(_) => {
                // ufs currently does not support set attr, so it returns None.
                // mount.ufs.set_attr(&ufs_path, opts).await?;
                Ok(None)
            }
        }
    }

    pub async fn get_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        match self.get_mount(path).await? {
            None => self.cv.get_lock(path, lock).await,
            Some(_) => err_ext!(FsError::unsupported("get_lock")),
        }
    }

    pub async fn set_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        match self.get_mount(path).await? {
            None => self.cv.set_lock(path, lock).await,
            Some(_) => err_ext!(FsError::unsupported("set_lock")),
        }
    }
}

impl FileSystem<UnifiedWriter, UnifiedReader> for UnifiedFileSystem {
    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["mkdir".to_string()],
        );

        let opts = MkdirOptsBuilder::with_conf(&self.cv.conf().client)
            .create_parent(create_parent)
            .build();
        match self.mkdir_with_opts(path, opts).await {
            Ok(_) => Ok(true),
            Err(FsError::FileAlreadyExists(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<UnifiedWriter> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["create".to_string()],
        );

        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(overwrite);
        let opts = self.cv.create_opts_builder().build();
        self.open_with_opts(path, opts, flags).await
    }

    async fn append(&self, path: &Path) -> FsResult<UnifiedWriter> {
        match self.get_mount(path).await? {
            None => Ok(UnifiedWriter::Cv(self.cv.append(path).await?)),
            Some((ufs_path, mount)) => mount.ufs.append(&ufs_path).await,
        }
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["exists".to_string()],
        );
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

        if let Some(reader) = self.get_cv_reader(path, &ufs_path, &mount).await? {
            info!(
                "read from Curvine(cache), ufs path {}, cv path: {}",
                ufs_path, path
            );

            self.metrics
                .mount_cache_hits
                .with_label_values(&[mount.mount_id()])
                .inc();

            Ok(UnifiedReader::Cv(reader))
        } else {
            self.metrics
                .mount_cache_misses
                .with_label_values(&[mount.mount_id()])
                .inc();

            if mount.info.auto_cache() {
                self.async_cache(&ufs_path)?;
            }

            // Reading from ufs
            if self.enable_read_ufs {
                info!("read from ufs, ufs path {}, cv path: {}", ufs_path, path);
                mount.ufs.open(&ufs_path).await
            } else {
                err_ext!(FsError::unsupported_ufs_read(path.path()))
            }
        }
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["rename".to_string()],
        );
        match self.get_mount(src).await? {
            None => self.cv.rename(src, dst).await,
            Some((src_ufs, mount)) => {
                // For write-through modes, we must wait for the sync job to complete before renaming in UFS.
                // This ensures all data has been fully written to UFS, preventing data loss or inconsistency
                // that could occur if we rename a file while its data is still being synced.
                if mount.info.is_write_through() {
                    self.wait_job_complete(src, "rename").await?;
                }

                let dst_ufs = mount.get_ufs_path(dst)?;
                let _ = mount.ufs.rename(&src_ufs, &dst_ufs).await?;

                // After rename, the file's mtime changes, making the cached data invalid
                if let Err(e) = self.cv.delete(src, true).await {
                    if !matches!(e, FsError::FileNotFound(_)) {
                        warn!("failed to delete cache for {}: {}", src, e);
                    }
                }

                Ok(true)
            }
        }
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["delete".to_string()],
        );
        match self.get_mount(path).await? {
            None => self.cv.delete(path, recursive).await,
            Some((ufs_path, mount)) => {
                // Delete from UFS
                mount.ufs.delete(&ufs_path, recursive).await?;

                // delete cache
                if let Err(e) = self.cv.delete(path, recursive).await {
                    if !matches!(e, FsError::FileNotFound(_)) {
                        warn!("failed to delete cache for {}: {}", path, e);
                    }
                };

                Ok(())
            }
        }
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["get_status".to_string()],
        );
        match self.get_mount(path).await? {
            None => self.cv.get_status(path).await,
            Some((ufs_path, mount)) => {
                if let Some(v) = self.get_cv_status(path).await? {
                    Ok(v)
                } else {
                    mount.ufs.get_status(&ufs_path).await
                }
            }
        }
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["list_status".to_string()],
        );
        match self.get_mount(path).await? {
            None => self.cv.list_status(path).await,
            Some((ufs_path, mount)) => mount.ufs.list_status(&ufs_path).await,
        }
    }

    async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["list_status".to_string()],
        );
        match self.get_mount(path).await? {
            None => self.cv.list_status_bytes(path).await,
            Some((ufs_path, mount)) => mount.ufs.list_status_bytes(&ufs_path).await,
        }
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["set_attr".to_string()],
        );
        match self.get_mount(path).await? {
            None => {
                self.cv.set_attr(path, opts).await?;
                Ok(())
            }
            Some((_, _)) => Ok(()), // ignore setting attr on ufs mount paths
        }
    }
}
