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
use crate::unified::{FallbackFsReader, MountCache, MountValue, UnifiedReader, UnifiedWriter};
use crate::ClientMetrics;
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, FsKind, Path, Reader, Writer};
use curvine_common::state::{
    CreateFileOpts, FileAllocOpts, FileLock, FileStatus, JobStatus, LoadJobCommand, MasterInfo,
    MkdirOpts, MkdirOptsBuilder, MountInfo, MountOptions, OpenFlags, SetAttrOpts,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::common::TimeSpent;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, err_ext};
use std::sync::Arc;

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum CacheValidity {
    Valid,
    Invalid(Option<FileStatus>),
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

    pub async fn get_mount_checked(
        &self,
        path: &Path,
    ) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        match self.get_mount(path).await? {
            Some(v) if v.1.info.is_cache_mode() => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    pub async fn get_master_info(&self) -> FsResult<MasterInfo> {
        self.cv.get_master_info().await
    }

    pub async fn get_master_info_bytes(&self) -> FsResult<BytesMut> {
        self.cv.get_master_info_bytes().await
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

    pub async fn free(&self, path: &Path) -> FsResult<()> {
        match self.get_mount(path).await? {
            None => err_box!(
                "the current file is not mounted to ufs, so the `free` command cannot be executed."
            ),
            Some((_, mnt)) => {
                if mnt.info.is_fs_mode() {
                    self.cv.free(path).await
                } else {
                    self.cv.delete(path, false).await
                }
            }
        }
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        match self.get_mount_checked(link).await? {
            None => self.cv.symlink(target, link, force).await,
            Some(_) => err_ext!(FsError::unsupported("symlink")),
        }
    }

    pub async fn link(&self, src_path: &Path, dst_path: &Path) -> FsResult<()> {
        match self.get_mount_checked(src_path).await? {
            None => self.cv.link(src_path, dst_path).await,
            Some(_) => err_ext!(FsError::unsupported("link")),
        }
    }

    pub async fn resize(&self, path: &Path, opts: FileAllocOpts) -> FsResult<()> {
        match self.get_mount_checked(path).await? {
            None => self.cv.resize(path, opts).await,
            Some(_) => err_ext!(FsError::unsupported("resize")),
        }
    }

    async fn check_cache_validity(
        &self,
        cv_status: &FileStatus,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<CacheValidity> {
        if cv_status.is_expired() {
            return Ok(CacheValidity::Invalid(None));
        }

        if !cv_status.is_complete() || !cv_status.ufs_exists() {
            return Ok(CacheValidity::Invalid(None));
        }

        if !mount.info.read_verify_ufs {
            return Ok(CacheValidity::Valid);
        }

        let ufs_status = mount.ufs.get_status(ufs_path).await?;
        if cv_status.len == ufs_status.len
            && cv_status.storage_policy.ufs_mtime != 0
            && cv_status.storage_policy.ufs_mtime == ufs_status.mtime
        {
            Ok(CacheValidity::Valid)
        } else {
            Ok(CacheValidity::Invalid(Some(ufs_status)))
        }
    }

    async fn get_cv_reader(
        &self,
        cv_path: &Path,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<Option<FallbackFsReader>> {
        let mut blocks = match self.cv.get_block_locations(cv_path).await {
            Ok(blocks) => blocks,
            Err(e) => {
                if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                    error!("failed to get block locations for {}: {}", cv_path, e)
                }
                return Ok(None);
            }
        };

        if mount.info.is_fs_mode() {
            if blocks.cv_exists() {
                let cv_reader = FsReader::new(cv_path.clone(), self.cv.fs_context(), blocks)?;
                Ok(Some(FallbackFsReader::new(
                    cv_reader,
                    ufs_path.clone(),
                    mount.ufs.clone(),
                )))
            } else if blocks.ufs_exists() {
                Ok(None)
            } else {
                err_box!("path {} data lost", cv_path)
            }
        } else {
            match self
                .check_cache_validity(&blocks.status, ufs_path, mount)
                .await?
            {
                CacheValidity::Valid => {
                    if blocks.status.ufs_exists() {
                        blocks.status.mtime = blocks.status.storage_policy.ufs_mtime;
                    }
                    let cv_reader = FsReader::new(cv_path.clone(), self.cv.fs_context(), blocks)?;
                    Ok(Some(FallbackFsReader::new(
                        cv_reader,
                        ufs_path.clone(),
                        mount.ufs.clone(),
                    )))
                }
                CacheValidity::Invalid(_) => Ok(None),
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

    pub async fn wait_job_complete(&self, path: &Path, fail_if_not_found: bool) -> FsResult<()> {
        if !path.is_cv() {
            return err_box!("the current file {} is not a cache file", path);
        }
        let (ufs_path, mnt) = match self.get_mount(path).await? {
            Some((ufs_path, mnt)) => (ufs_path, mnt),
            None => return err_box!("the current file {} is not mounted to ufs", path),
        };

        let job_id = if mnt.info.is_fs_mode() {
            CommonUtils::create_job_id(path.full_path())
        } else {
            CommonUtils::create_job_id(ufs_path.full_path())
        };
        let client = JobMasterClient::new(self.fs_client());
        client.wait_job_complete(job_id, fail_if_not_found).await
    }

    pub async fn get_job_status(&self, path: &Path) -> FsResult<JobStatus> {
        let client = JobMasterClient::new(self.fs_client());
        let job_id = CommonUtils::create_job_id(path.full_path());
        client.get_job_status(job_id).await
    }

    pub async fn cleanup(&self) {
        self.cv.cleanup().await
    }

    pub fn disable_unified(&mut self) {
        self.enable_unified = false
    }

    pub async fn copy_ufs_file(
        &self,
        path: &Path,
        mnt: &MountValue,
        opts: CreateFileOpts,
        cv_len: i64,
    ) -> FsResult<()> {
        let ufs_path = mnt.get_ufs_path(path)?;
        let mut reader = mnt.ufs.open(&ufs_path).await?;
        if reader.len() != cv_len {
            return err_box!(
                "file length mismatch: cv_path={:?}, ufs_path={:?}, ufs_len={}, cv_len={}",
                path,
                ufs_path,
                reader.len(),
                cv_len
            );
        }

        let flags = OpenFlags::new_create().set_overwrite(true);
        let mut writer = self.cv.open_with_opts(path, opts, flags).await?;

        loop {
            let data = reader.async_read(None).await?;
            if data.is_empty() {
                break;
            }
            writer.async_write(data).await?;
        }
        reader.complete().await?;
        writer.complete().await?;

        Ok(())
    }

    pub async fn open_for_write(&self, path: &Path) -> FsResult<UnifiedWriter> {
        let opts = self.cv().create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_write_only().set_create(true);
        self.open_with_opts(path, opts, flags).await
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

            Some((_, mount)) if mount.info.is_fs_mode() => {
                let mut writer = self.cv.open_with_opts(path, opts.clone(), flags).await?;
                if writer.file_blocks().cv_exists() || flags.overwrite() {
                    Ok(UnifiedWriter::Cv(writer))
                } else {
                    writer.complete().await?;

                    info!(
                        "copying data from UFS to CV, path={}, len={}",
                        path,
                        writer.status().len
                    );
                    self.copy_ufs_file(path, &mount, opts.clone(), writer.status().len)
                        .await?;

                    let writer = self.cv.open_with_opts(path, opts, flags).await?;
                    Ok(UnifiedWriter::Cv(writer))
                }
            }

            Some((ufs_path, mount)) => {
                if let Err(e) = self.cv.delete(path, false).await {
                    if !matches!(e, FsError::FileNotFound(_)) {
                        warn!("failed to delete cache for {}: {}", path, e);
                    }
                }

                let writer = if flags.append() {
                    mount.ufs.append(&ufs_path).await?
                } else {
                    mount.ufs.create(&ufs_path, flags.overwrite()).await?
                };
                Ok(writer)
            }
        }
    }

    pub async fn mkdir_with_opts(
        &self,
        path: &Path,
        opts: MkdirOpts,
    ) -> FsResult<Option<FileStatus>> {
        match self.get_mount_checked(path).await? {
            None => {
                let status = self.cv.mkdir_with_opts(path, opts).await?;
                Ok(Some(status))
            }

            Some((ufs_path, mount)) => {
                let flag = mount.ufs.mkdir(&ufs_path, opts.create_parent).await?;
                if !flag {
                    err_ext!(FsError::file_exists(ufs_path.path()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn fuse_set_attr(
        &self,
        path: &Path,
        opts: SetAttrOpts,
    ) -> FsResult<Option<FileStatus>> {
        match self.get_mount_checked(path).await? {
            None => {
                let status = self.cv.set_attr(path, opts).await?;
                Ok(Some(status))
            }

            Some(_) => Ok(None),
        }
    }

    pub async fn get_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        match self.get_mount_checked(path).await? {
            None => self.cv.get_lock(path, lock).await,
            Some(_) => err_ext!(FsError::unsupported("get_lock")),
        }
    }

    pub async fn set_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        match self.get_mount_checked(path).await? {
            None => self.cv.set_lock(path, lock).await,
            Some(_) => err_ext!(FsError::unsupported("set_lock")),
        }
    }
}

impl FileSystem<UnifiedWriter, UnifiedReader> for UnifiedFileSystem {
    fn fs_kind(&self) -> FsKind {
        FsKind::Cv
    }

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
        let flags = OpenFlags::new_append().set_create(true);
        let opts = self.cv.create_opts_builder().build();
        self.open_with_opts(path, opts, flags).await
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["exists".to_string()],
        );
        match self.get_mount_checked(path).await? {
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

            Ok(UnifiedReader::Fallback(reader))
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

        match self.get_mount_checked(src).await? {
            None => self.cv.rename(src, dst).await,
            Some((src_ufs, mount)) => {
                let dst_ufs = mount.get_ufs_path(dst)?;
                let res = mount.ufs.rename(&src_ufs, &dst_ufs).await?;

                // After rename, the file's mtime changes, making the cached data invalid
                if let Err(e) = self.cv.delete(src, true).await {
                    if !matches!(e, FsError::FileNotFound(_)) {
                        warn!("failed to delete cache for {}: {}", src, e);
                    }
                }

                Ok(res)
            }
        }
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["delete".to_string()],
        );

        match self.get_mount_checked(path).await? {
            None => self.cv.delete(path, recursive).await,
            Some((ufs_path, mount)) => {
                if path.path() == mount.info.cv_path {
                    return err_box!(
                        "cannot delete mount point root: cv_path={}, ufs_path={}",
                        mount.info.cv_path,
                        mount.info.ufs_path
                    );
                }

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

            Some((_, mnt)) if mnt.info.is_fs_mode() => self.cv.get_status(path).await,

            Some((ufs_path, mnt)) => match self.cv.get_status(path).await {
                Ok(mut v) => match self.check_cache_validity(&v, &ufs_path, &mnt).await? {
                    CacheValidity::Valid => {
                        if v.ufs_exists() {
                            v.mtime = v.storage_policy.ufs_mtime;
                        }
                        Ok(v)
                    }
                    CacheValidity::Invalid(Some(ufs_status)) => Ok(ufs_status),
                    CacheValidity::Invalid(None) => mnt.ufs.get_status(&ufs_path).await,
                },

                Err(e) => {
                    if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                        warn!("failed to get status file {}: {}", path, e);
                    };
                    mnt.ufs.get_status(&ufs_path).await
                }
            },
        }
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["list_status".to_string()],
        );

        match self.get_mount_checked(path).await? {
            None => self.cv.list_status(path).await,
            Some((ufs_path, mount)) => mount.ufs.list_status(&ufs_path).await,
        }
    }

    async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["list_status".to_string()],
        );

        match self.get_mount_checked(path).await? {
            None => self.cv.list_status_bytes(path).await,
            Some((ufs_path, mount)) => mount.ufs.list_status_bytes(&ufs_path).await,
        }
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["set_attr".to_string()],
        );

        match self.get_mount_checked(path).await? {
            None => {
                self.cv.set_attr(path, opts).await?;
                Ok(())
            }
            Some((_, _)) => Ok(()), // ignore setting attr on ufs mount paths
        }
    }
}
