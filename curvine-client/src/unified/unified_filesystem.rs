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
use crate::unified::{
    CacheSyncReader, CacheSyncWriter, MountCache, MountValue, PdpcController, UnifiedReader,
    UnifiedWriter,
};
use crate::ClientMetrics;
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader};
use curvine_common::state::{
    ConsistencyStrategy, CreateFileOpts, FileAllocOpts, FileLock, FileStatus, JobStatus,
    JobTaskState, LoadJobCommand, MasterInfo, MkdirOpts, MkdirOptsBuilder, MountInfo, MountOptions,
    OpenFlags, SetAttrOpts, SyncLifecycleOwner, SyncLifecycleState, WriteType,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::common::TimeSpent;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, err_ext};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum CacheValidity {
    Valid,
    Invalid(Option<FileStatus>),
}

#[allow(clippy::large_enum_variant)]
enum CvReadResult {
    Hit(UnifiedReader),
    Miss {
        allow_async_cache: bool,
        source_snapshot: Option<SourceSnapshot>,
        expected_target_mtime: Option<i64>,
        expected_target_missing: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SourceSnapshot {
    id: i64,
    len: i64,
    mtime: i64,
}

impl SourceSnapshot {
    fn from_file_status(status: &FileStatus) -> Self {
        Self {
            id: status.id,
            len: status.len,
            mtime: status.mtime,
        }
    }

    fn from_job_status(status: &JobStatus) -> Option<Self> {
        match (
            status.expected_source_id,
            status.expected_source_len,
            status.expected_source_mtime,
        ) {
            (Some(id), Some(len), Some(mtime)) => Some(Self { id, len, mtime }),
            _ => None,
        }
    }

    fn source_generation(&self) -> String {
        CommonUtils::source_generation(self.id, self.len, self.mtime)
    }
}

#[derive(Clone, Copy)]
enum JobWaitPolicy {
    Strict,
    IgnoreStaleFailure,
}

struct PathJobContext {
    expected_snapshot: Option<SourceSnapshot>,
    wait_job_id: Option<String>,
    should_cancel_job: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingSyncOwner {
    Publish,
    Hydrate,
    Unknown,
}

#[derive(Clone)]
pub struct UnifiedFileSystem {
    cv: CurvineFileSystem,
    mount_cache: Arc<MountCache>,
    pdpc: Arc<PdpcController>,
    enable_unified: bool,
    enable_read_ufs: bool,
    metrics: &'static ClientMetrics,
}

impl UnifiedFileSystem {
    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let update_interval = conf.client.mount_update_ttl;
        let enable_unified = conf.client.enable_unified_fs;
        let enable_read_ufs = conf.client.enable_rust_read_ufs;
        let pdpc = Arc::new(PdpcController::new(&conf.client));

        let cv = CurvineFileSystem::with_rt(conf, rt.clone())?;
        let fs = UnifiedFileSystem {
            cv,
            mount_cache: Arc::new(MountCache::new(update_interval.as_millis() as u64)),
            pdpc,
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

    pub fn pdpc(&self) -> &Arc<PdpcController> {
        &self.pdpc
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

    fn source_generation(status: &FileStatus) -> String {
        CommonUtils::source_generation(status.id, status.len, status.mtime)
    }

    fn marker_generation_matches_status(
        status: &FileStatus,
        marker: &curvine_common::state::SyncLifecycleMarker,
    ) -> bool {
        Self::source_generation(status) == marker.generation
    }

    fn keep_incomplete_cv_only_visible(status: &FileStatus) -> bool {
        !status.is_complete() && status.is_cv_only()
    }

    fn classify_pending_sync_owner(
        marker: &curvine_common::state::SyncLifecycleMarker,
    ) -> PendingSyncOwner {
        match marker.owner {
            SyncLifecycleOwner::Publish => PendingSyncOwner::Publish,
            SyncLifecycleOwner::Hydrate => PendingSyncOwner::Hydrate,
            SyncLifecycleOwner::Unknown => PendingSyncOwner::Unknown,
        }
    }

    async fn check_pending_sync_validity(
        &self,
        cv_path: &Path,
        cv_status: &FileStatus,
        ufs_path: &Path,
        mount: &MountValue,
        marker: &curvine_common::state::SyncLifecycleMarker,
    ) -> FsResult<CacheValidity> {
        let cv_generation = Self::source_generation(cv_status);
        if !Self::marker_generation_matches_status(cv_status, marker) {
            if matches!(marker.owner, SyncLifecycleOwner::Publish) {
                if !cv_status.is_complete() && cv_status.is_cv_only() {
                    log::debug!(
                        "check_cache_validity: VALID - pending publish marker mismatch with newer incomplete cv-only generation, cv_path={}, ufs_path={}, marker_generation={}, current_generation={}, cv_len={}",
                        cv_path,
                        ufs_path,
                        marker.generation,
                        cv_generation,
                        cv_status.len
                    );
                    return Ok(CacheValidity::Valid);
                }
                return match self.get_ufs_status_for_validity(mount, ufs_path).await {
                    Ok(ufs_status) => {
                        log::debug!(
                            "check_cache_validity: INVALID - pending publish marker mismatch with existing ufs target, cv_path={}, ufs_path={}, marker_generation={}, current_generation={}, cv_len={}, ufs_len={}",
                            cv_path,
                            ufs_path,
                            marker.generation,
                            cv_generation,
                            cv_status.len,
                            ufs_status.len
                        );
                        Ok(CacheValidity::Invalid(Some(ufs_status)))
                    }
                    Err(FsError::FileNotFound(_) | FsError::Expired(_)) => {
                        log::debug!(
                            "check_cache_validity: VALID - pending publish marker mismatch with missing ufs target keeps CV visible, cv_path={}, ufs_path={}, marker_generation={}, current_generation={}, cv_len={}",
                            cv_path,
                            ufs_path,
                            marker.generation,
                            cv_generation,
                            cv_status.len
                        );
                        Ok(CacheValidity::Valid)
                    }
                    Err(e) => Err(e),
                };
            }

            log::debug!(
                "check_cache_validity: INVALID - pending marker generation mismatch, cv_path={}, ufs_path={}, marker_generation={}, current_generation={}, cv_len={}",
                cv_path,
                ufs_path,
                marker.generation,
                cv_generation,
                cv_status.len
            );

            return match self.get_ufs_status_for_validity(mount, ufs_path).await {
                Ok(ufs_status) => Ok(CacheValidity::Invalid(Some(ufs_status))),
                Err(FsError::FileNotFound(_) | FsError::Expired(_)) => {
                    Ok(CacheValidity::Invalid(None))
                }
                Err(e) => Err(e),
            };
        }

        let pending_unknown_visibility = async || -> FsResult<CacheValidity> {
            if cv_status.is_cv_only() {
                return Ok(CacheValidity::Valid);
            }

            match self.get_ufs_status_for_validity(mount, ufs_path).await {
                Ok(ufs_status) => {
                    let ufs_generation = Self::source_generation(&ufs_status);
                    if ufs_generation == marker.generation {
                        Ok(CacheValidity::Valid)
                    } else {
                        log::debug!(
                            "check_cache_validity: INVALID - pending unknown owner with generation mismatch, cv_path={}, ufs_path={}, marker_generation={}, ufs_generation={}, cv_len={}, ufs_len={}",
                            cv_path,
                            ufs_path,
                            marker.generation,
                            ufs_generation,
                            cv_status.len,
                            ufs_status.len
                        );
                        Ok(CacheValidity::Invalid(Some(ufs_status)))
                    }
                }
                Err(FsError::FileNotFound(_) | FsError::Expired(_)) => Ok(CacheValidity::Valid),
                Err(e) => Err(e),
            }
        };

        match Self::classify_pending_sync_owner(marker) {
            PendingSyncOwner::Publish => {
                log::debug!(
                    "check_cache_validity: VALID - pending publish owner keeps CV visible (close-to-open), cv_path={}, ufs_path={}, marker_generation={}, cv_len={}",
                    cv_path,
                    ufs_path,
                    marker.generation,
                    cv_status.len
                );
                Ok(CacheValidity::Valid)
            }
            PendingSyncOwner::Hydrate => {
                match self.get_ufs_status_for_validity(mount, ufs_path).await {
                    Ok(ufs_status) => {
                        let source_generation = Self::source_generation(&ufs_status);
                        if source_generation == marker.generation {
                            Ok(CacheValidity::Valid)
                        } else {
                            log::debug!(
                            "check_cache_validity: INVALID - pending hydrate generation mismatch, cv_path={}, ufs_path={}, marker_generation={}, current_generation={}, cv_len={}, ufs_len={}",
                            cv_path,
                            ufs_path,
                            marker.generation,
                            source_generation,
                            cv_status.len,
                            ufs_status.len
                        );
                            Ok(CacheValidity::Invalid(Some(ufs_status)))
                        }
                    }
                    Err(FsError::FileNotFound(_) | FsError::Expired(_)) => {
                        Ok(CacheValidity::Invalid(None))
                    }
                    Err(e) => Err(e),
                }
            }
            PendingSyncOwner::Unknown => pending_unknown_visibility().await,
        }
    }

    async fn check_cache_validity(
        &self,
        cv_path: &Path,
        cv_status: &FileStatus,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<CacheValidity> {
        if cv_status.is_expired() {
            return Ok(CacheValidity::Invalid(None));
        }

        if let Some(sync_state) = cv_status.sync_state() {
            match sync_state {
                SyncLifecycleState::Committed => {}
                SyncLifecycleState::Pending | SyncLifecycleState::Committing => {
                    if let Some(marker) = cv_status.sync_marker() {
                        return self
                            .check_pending_sync_validity(
                                cv_path, cv_status, ufs_path, mount, &marker,
                            )
                            .await;
                    }
                    if cv_status.is_cv_only() {
                        log::debug!(
                            "check_cache_validity: VALID - pending sync without marker keeps cv-only visible, ufs_path={}, cv_len={}",
                            ufs_path,
                            cv_status.len
                        );
                        return Ok(CacheValidity::Valid);
                    }
                    return Ok(CacheValidity::Invalid(None));
                }
                SyncLifecycleState::Aborted => {
                    let owner = cv_status
                        .sync_marker()
                        .map(|marker| marker.owner)
                        .unwrap_or(SyncLifecycleOwner::Unknown);
                    if matches!(owner, SyncLifecycleOwner::Hydrate) {
                        log::debug!(
                            "check_cache_validity: INVALID - aborted hydrate marker treats CV as stale, ufs_path={}, cv_len={}, cv_only={}",
                            ufs_path,
                            cv_status.len,
                            cv_status.is_cv_only()
                        );
                        return Ok(CacheValidity::Invalid(None));
                    }
                    if cv_status.is_complete() {
                        log::debug!(
                            "check_cache_validity: VALID - aborted publish keeps CV visible, ufs_path={}, cv_len={}, cv_only={}",
                            ufs_path,
                            cv_status.len,
                            cv_status.is_cv_only()
                        );
                        return Ok(CacheValidity::Valid);
                    }
                    return Ok(CacheValidity::Invalid(None));
                }
                _ => {
                    log::debug!(
                        "check_cache_validity: INVALID - sync state {:?}, ufs_path={}, cv_len={}, cv_only={}",
                        sync_state,
                        ufs_path,
                        cv_status.len,
                        cv_status.is_cv_only()
                    );
                    return Ok(CacheValidity::Invalid(None));
                }
            }
        }

        if Self::keep_incomplete_cv_only_visible(cv_status) {
            log::debug!(
                "check_cache_validity: VALID - incomplete cv-only entry keeps CV visible, ufs_path={}, cv_len={}",
                ufs_path,
                cv_status.len
            );
            return Ok(CacheValidity::Valid);
        }

        if !cv_status.is_complete() {
            log::debug!(
                "check_cache_validity: INVALID - cache not complete, ufs_path={}, cv_len={}",
                ufs_path,
                cv_status.len
            );
            return Ok(CacheValidity::Invalid(None));
        }

        if mount.info.consistency_strategy == ConsistencyStrategy::None {
            return Ok(CacheValidity::Valid);
        }

        if cv_status.is_cv_only() {
            return Ok(CacheValidity::Valid);
        }

        if cv_status.storage_policy.ufs_mtime == 0 {
            return Ok(CacheValidity::Valid);
        }

        let ufs_status = match self.get_ufs_status_for_validity(mount, ufs_path).await {
            Ok(status) => status,
            Err(FsError::FileNotFound(_) | FsError::Expired(_))
                if matches!(mount.info.write_type, WriteType::AsyncThrough) =>
            {
                log::debug!(
                    "check_cache_validity: VALID - async-through transient missing ufs target keeps CV visible, ufs_path={}, cv_len={}, cv_ufs_mtime={}",
                    ufs_path,
                    cv_status.len,
                    cv_status.storage_policy.ufs_mtime
                );
                return Ok(CacheValidity::Valid);
            }
            Err(e) => return Err(e),
        };

        if cv_status.len == ufs_status.len && cv_status.storage_policy.ufs_mtime == ufs_status.mtime
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
    ) -> FsResult<CvReadResult> {
        let reader = match self.cv().open(cv_path).await {
            Ok(reader) => reader,
            Err(e) => {
                let cv_status = if matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                    None
                } else {
                    error!("failed to open curvine file {}: {}", cv_path, e);
                    match self.cv().get_status(cv_path).await {
                        Ok(status) => Some(status),
                        Err(FsError::FileNotFound(_) | FsError::Expired(_)) => None,
                        Err(status_err) => {
                            warn!(
                                "failed to get curvine status after open failure for {}: {}",
                                cv_path, status_err
                            );
                            None
                        }
                    }
                };
                return Ok(Self::miss_result_for_cv_open_error(cv_status.as_ref()));
            }
        };

        match self
            .check_cache_validity(cv_path, reader.status(), ufs_path, mount)
            .await?
        {
            CacheValidity::Valid => {
                if Self::keep_incomplete_cv_only_visible(reader.status()) {
                    let sync_reader = CacheSyncReader::new(self.cv(), cv_path).await?;
                    Ok(CvReadResult::Hit(UnifiedReader::CacheSync(sync_reader)))
                } else {
                    Ok(CvReadResult::Hit(UnifiedReader::Cv(reader)))
                }
            }
            CacheValidity::Invalid(ufs_status) => {
                let allow_async_cache = Self::allow_async_cache_on_miss(Some(reader.status()));
                Ok(CvReadResult::Miss {
                    allow_async_cache,
                    source_snapshot: ufs_status.as_ref().map(SourceSnapshot::from_file_status),
                    expected_target_mtime: Some(reader.status().mtime),
                    expected_target_missing: false,
                })
            }
        }
    }

    fn allow_async_cache_on_miss(cv_status: Option<&FileStatus>) -> bool {
        match cv_status {
            Some(status) => {
                if matches!(
                    status.sync_state(),
                    Some(SyncLifecycleState::Pending | SyncLifecycleState::Committing)
                ) {
                    return false;
                }
                status.is_complete() || !status.is_cv_only()
            }
            None => true,
        }
    }

    fn miss_result_for_cv_open_error(cv_status: Option<&FileStatus>) -> CvReadResult {
        CvReadResult::Miss {
            allow_async_cache: Self::allow_async_cache_on_miss(cv_status),
            source_snapshot: None,
            expected_target_mtime: cv_status.map(|status| status.mtime),
            expected_target_missing: cv_status.is_none(),
        }
    }

    fn async_cache(
        &self,
        source_path: &Path,
        source_snapshot: Option<SourceSnapshot>,
        expected_target_mtime: Option<i64>,
        expected_target_missing: bool,
    ) -> FsResult<()> {
        let client = JobMasterClient::new(self.fs_client());
        let source_path_obj = source_path.clone();
        let source_path = source_path.clone_uri();
        let source_generation = source_snapshot.map(|snapshot| snapshot.source_generation());
        let admission_key = self
            .pdpc()
            .hydrate_key(&source_path_obj, source_generation.as_deref());
        let admission_guard = self
            .pdpc()
            .try_acquire_hydrate(&source_path_obj, source_generation.as_deref());

        if admission_guard.is_none() {
            log::debug!(
                "skip async cache submit for {} because path admission key {} is already in-flight, source_generation={:?}",
                source_path,
                admission_key,
                source_generation
            );
            return Ok(());
        }

        self.fs_context().rt().spawn(async move {
            let _admission_guard = admission_guard;
            let res = if let Some(source) = source_snapshot {
                client
                    .submit_hydrate_with_source(
                        source_path.clone(),
                        source.id,
                        source.len,
                        source.mtime,
                        expected_target_mtime,
                        expected_target_missing,
                    )
                    .await
            } else {
                client
                    .submit_hydrate(
                        source_path.clone(),
                        expected_target_mtime,
                        expected_target_missing,
                    )
                    .await
            };
            match res {
                Err(e) => warn!("submit async cache error for {}: {}", source_path, e),
                Ok(res) => info!(
                    "submit async cache successfully for {}, job id {}, target_path {}, source_generation={:?}, admission_key={}",
                    source_path,
                    res.job_id,
                    res.target_path,
                    source_generation,
                    admission_key
                ),
            }
        });

        Ok(())
    }

    fn canonical_job_id(path: &Path, source_snapshot: Option<SourceSnapshot>) -> Option<String> {
        source_snapshot.map(|source| {
            CommonUtils::create_job_id_with_generation(path.full_path(), source.source_generation())
        })
    }

    fn canonical_job_id_with_generation(path: &Path, generation: Option<String>) -> Option<String> {
        generation.and_then(|generation| {
            if generation.is_empty() {
                None
            } else {
                Some(CommonUtils::create_job_id_with_generation(
                    path.full_path(),
                    generation,
                ))
            }
        })
    }

    fn wait_job_id(
        path: &Path,
        source_snapshot: Option<SourceSnapshot>,
        source_generation: Option<String>,
        owner_lease_job_id: Option<String>,
    ) -> Option<String> {
        owner_lease_job_id
            .or_else(|| Self::canonical_job_id(path, source_snapshot))
            .or_else(|| Self::canonical_job_id_with_generation(path, source_generation))
    }

    fn path_job_context(
        path: &Path,
        source_snapshot: Option<SourceSnapshot>,
        source_generation: Option<String>,
        owner_lease_job_id: Option<String>,
        should_cancel_job: bool,
    ) -> PathJobContext {
        PathJobContext {
            expected_snapshot: source_snapshot,
            wait_job_id: Self::wait_job_id(
                path,
                source_snapshot,
                source_generation,
                owner_lease_job_id,
            ),
            should_cancel_job,
        }
    }

    fn should_cancel_mount_meta_job(status: &FileStatus) -> bool {
        status.sync_state().map_or(!status.is_complete, |state| {
            matches!(
                state,
                SyncLifecycleState::Pending | SyncLifecycleState::Committing
            )
        })
    }

    fn should_use_async_rename_fast_path(status: &FileStatus) -> bool {
        if status.is_dir {
            return false;
        }

        status.is_complete() && status.is_cv_only()
    }

    async fn submit_async_publish_after_rename(&self, path: &Path, mark: &str) -> FsResult<()> {
        let status = match self.cv.get_status(path).await {
            Ok(status) => status,
            Err(FsError::FileNotFound(_) | FsError::Expired(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        if status.is_dir {
            return Ok(());
        }

        let snapshot = SourceSnapshot::from_file_status(&status);
        let generation = snapshot.source_generation();
        let command = LoadJobCommand::publish_builder(path.clone_uri())
            .source_generation(generation.clone())
            .expected_source_id(snapshot.id)
            .expected_source_len(snapshot.len)
            .expected_source_mtime(snapshot.mtime)
            .build();
        match JobMasterClient::new(self.fs_client())
            .submit_load_job(command.clone())
            .await
        {
            Ok(res) => {
                info!(
                    "[{}] submit async-through rename publish for {}, job id {}, generation={}",
                    mark, path, res.job_id, generation
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn should_ignore_stale_failure(
        expected_snapshot: Option<SourceSnapshot>,
        status: &JobStatus,
    ) -> bool {
        matches!(
            (
                expected_snapshot,
                SourceSnapshot::from_job_status(status),
            ),
            (Some(expected), Some(job)) if expected != job
        )
    }

    fn inspect_failed_status(
        mark: &str,
        path: &Path,
        expected_snapshot: Option<SourceSnapshot>,
        status: &JobStatus,
    ) -> Option<bool> {
        if !matches!(status.state, JobTaskState::Failed | JobTaskState::Canceled) {
            return None;
        }

        let job_snapshot = SourceSnapshot::from_job_status(status);
        if Self::should_ignore_stale_failure(expected_snapshot, status) {
            warn!(
                "[{}] ignore stale async-through job {} for {}: {:?}, progress: {}, expected_source={:?}, job_source={:?}",
                mark,
                status.job_id,
                path,
                status.state,
                status.progress.message,
                expected_snapshot,
                job_snapshot
            );
            return Some(true);
        }

        warn!(
            "[{}] keep async-through job failure for {} (job {}): {:?}, progress: {}, expected_source={:?}, job_source={:?}",
            mark,
            path,
            status.job_id,
            status.state,
            status.progress.message,
            expected_snapshot,
            job_snapshot
        );
        Some(false)
    }

    async fn resolve_path_job_context(
        &self,
        path: &Path,
        mark: &str,
        wait_for_complete_source: bool,
    ) -> PathJobContext {
        let client_conf = &self.conf().client;
        let deadline = Instant::now() + Duration::from_secs(client_conf.close_timeout_secs.max(1));
        let poll_interval = client_conf.sync_check_interval_min;
        let mut last_lease = None;
        let mut last_generation = None;

        loop {
            match self.cv.get_status(path).await {
                Ok(status) if status.is_complete => {
                    let (lease, generation) = match status.sync_marker() {
                        Some(marker) => (Some(marker.lease), Some(marker.generation)),
                        None => (None, None),
                    };
                    let should_cancel = Self::should_cancel_mount_meta_job(&status);
                    return Self::path_job_context(
                        path,
                        Some(SourceSnapshot::from_file_status(&status)),
                        generation,
                        lease,
                        should_cancel,
                    );
                }
                Ok(status) => {
                    let (lease, generation) = match status.sync_marker() {
                        Some(marker) => (Some(marker.lease), Some(marker.generation)),
                        None => (None, None),
                    };
                    if lease.is_some() {
                        last_lease = lease.clone();
                    }
                    if generation.is_some() {
                        last_generation = generation.clone();
                    }
                    let should_cancel = Self::should_cancel_mount_meta_job(&status);
                    if !wait_for_complete_source {
                        return Self::path_job_context(
                            path,
                            None,
                            generation,
                            lease,
                            should_cancel,
                        );
                    }
                }
                Err(FsError::FileNotFound(_) | FsError::Expired(_)) => {
                    return Self::path_job_context(path, None, last_generation, last_lease, false);
                }
                Err(e) => {
                    warn!(
                        "[{}] failed to get source snapshot for {} before waiting job: {}",
                        mark, path, e
                    );
                    return Self::path_job_context(path, None, last_generation, last_lease, false);
                }
            }

            if !wait_for_complete_source || Instant::now() >= deadline {
                if wait_for_complete_source {
                    warn!(
                        "[{}] source {} remained incomplete while resolving job context",
                        mark, path
                    );
                }
                return Self::path_job_context(path, None, last_generation, last_lease, false);
            }

            sleep(poll_interval).await;
        }
    }

    async fn wait_path_job(
        &self,
        client: &JobMasterClient,
        ctx: &PathJobContext,
        mark: &str,
    ) -> FsResult<()> {
        match ctx.wait_job_id.as_deref() {
            None => Ok(()),
            Some(job_id) => match client.wait_job_complete(job_id, mark).await {
                Ok(()) | Err(FsError::JobNotFound(_)) => Ok(()),
                Err(e) => Err(e),
            },
        }
    }

    async fn inspect_wait_error(
        client: &JobMasterClient,
        path: &Path,
        mark: &str,
        wait_err: &FsError,
        ctx: &PathJobContext,
    ) -> bool {
        let Some(job_id) = ctx.wait_job_id.as_deref() else {
            return false;
        };
        match client.get_job_status(job_id).await {
            Ok(status) => {
                if let Some(ignore) =
                    Self::inspect_failed_status(mark, path, ctx.expected_snapshot, &status)
                {
                    return ignore;
                }
            }
            Err(FsError::JobNotFound(_)) => {}
            Err(status_err) => {
                warn!(
                    "[{}] failed to inspect async-through job {} for {}: {}, original wait error: {}",
                    mark, job_id, path, status_err, wait_err
                );
            }
        }
        false
    }

    async fn wait_path_job_with_policy(
        &self,
        path: &Path,
        mark: &str,
        policy: JobWaitPolicy,
        wait_for_complete_source: bool,
    ) -> FsResult<()> {
        let client = JobMasterClient::new(self.fs_client());
        let ctx = self
            .resolve_path_job_context(path, mark, wait_for_complete_source)
            .await;
        match self.wait_path_job(&client, &ctx, mark).await {
            Ok(()) => Ok(()),
            Err(wait_err) => match policy {
                JobWaitPolicy::Strict => Err(wait_err),
                JobWaitPolicy::IgnoreStaleFailure => {
                    if Self::inspect_wait_error(&client, path, mark, &wait_err, &ctx).await {
                        Ok(())
                    } else {
                        Err(wait_err)
                    }
                }
            },
        }
    }

    async fn cancel_job_if_exists(client: &JobMasterClient, job_id: &str, path: &Path, mark: &str) {
        match client.cancel_job(job_id).await {
            Ok(()) => info!(
                "[{}] canceled async-through job {} for {}",
                mark, job_id, path
            ),
            Err(FsError::JobNotFound(_)) => {}
            Err(e) => warn!(
                "[{}] failed to cancel async-through job {} for {}: {}",
                mark, job_id, path, e
            ),
        }
    }

    pub async fn wait_job_complete(&self, path: &Path, mark: &str) -> FsResult<()> {
        self.wait_path_job_with_policy(path, mark, JobWaitPolicy::Strict, true)
            .await
    }

    async fn wait_job_complete_for_mount_meta_op(&self, path: &Path, mark: &str) -> FsResult<()> {
        self.wait_path_job_with_policy(path, mark, JobWaitPolicy::IgnoreStaleFailure, true)
            .await
    }

    async fn cancel_job_for_mount_meta_op(&self, path: &Path, mark: &str) {
        let client = JobMasterClient::new(self.fs_client());
        let ctx = self.resolve_path_job_context(path, mark, false).await;
        if !ctx.should_cancel_job {
            return;
        }
        if let Some(job_id) = ctx.wait_job_id.as_deref() {
            Self::cancel_job_if_exists(&client, job_id, path, mark).await;
        }
        let legacy_job_id = CommonUtils::create_job_id(path.full_path());
        if ctx.wait_job_id.as_deref() != Some(legacy_job_id.as_str()) {
            Self::cancel_job_if_exists(&client, &legacy_job_id, path, mark).await;
        }
    }

    pub async fn cleanup(&self) {
        self.cv.cleanup().await
    }

    async fn invalidate_cv_cache_for_mount_write(&self, path: &Path) {
        if let Err(e) = self.cv.delete(path, false).await {
            if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                warn!("failed to invalidate mount cache file {}: {}", path, e);
            }
        }
    }

    async fn invalidate_cv_cache_for_mount_meta_path(&self, path: &Path, mark: &str) {
        let recursive = match self.cv.get_status(path).await {
            Ok(status) => status.is_dir,
            Err(FsError::FileNotFound(_) | FsError::Expired(_)) => false,
            Err(e) => {
                warn!(
                    "[{}] failed to get cache status for {} before invalidation: {}",
                    mark, path, e
                );
                false
            }
        };

        if let Err(e) = self.cv.delete(path, recursive).await {
            if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                warn!("[{}] failed to invalidate cache for {}: {}", mark, path, e);
            }
        }
    }

    fn should_preserve_cv_cache_on_mount_rename(write_type: WriteType) -> bool {
        matches!(
            write_type,
            WriteType::AsyncThrough | WriteType::CacheThrough
        )
    }

    async fn try_rename_cv_cache_for_mount_path(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let mut attempts = 0usize;
        loop {
            match self.cv.rename(src, dst).await {
                Ok(_) => return Ok(true),
                Err(FsError::FileNotFound(_) | FsError::Expired(_)) => {
                    if attempts >= 5 {
                        return Ok(false);
                    }
                    attempts += 1;
                    sleep(Duration::from_millis(2)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn get_ufs_status_for_validity(
        &self,
        mount: &MountValue,
        ufs_path: &Path,
    ) -> FsResult<FileStatus> {
        if let Some(status) = self.pdpc().get_ufs_status_hint(ufs_path) {
            return Ok(status);
        }

        match mount.ufs.get_status(ufs_path).await {
            Ok(status) => {
                self.pdpc().put_ufs_status_hint(ufs_path, &status);
                Ok(status)
            }
            Err(e @ FsError::FileNotFound(_)) | Err(e @ FsError::Expired(_)) => {
                self.pdpc().clear_ufs_status_hint(ufs_path);
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn invalidate_open_notfound_for_ufs_path(&self, ufs_path: &Path) {
        self.pdpc().clear_ufs_open_notfound(ufs_path);
        self.pdpc().clear_ufs_status_hint(ufs_path);
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
                    self.invalidate_open_notfound_for_ufs_path(&ufs_path);
                    let opts = mount.info.get_create_opts(&self.conf().client);
                    let writer = self.cv.open_with_opts(path, opts, flags).await?;
                    Ok(UnifiedWriter::Cv(writer))
                }

                WriteType::Through => {
                    self.invalidate_open_notfound_for_ufs_path(&ufs_path);
                    // Through-mode writes bypass Curvine data path, so stale cache must be removed
                    // before writing to avoid serving outdated cache on subsequent reads.
                    self.invalidate_cv_cache_for_mount_write(path).await;

                    let writer = if flags.append() {
                        mount.ufs.append(&ufs_path).await?
                    } else {
                        mount.ufs.create(&ufs_path, flags.overwrite()).await?
                    };
                    Ok(writer)
                }

                WriteType::CacheThrough => {
                    self.invalidate_open_notfound_for_ufs_path(&ufs_path);
                    let mut cv_flags = flags;
                    if flags.overwrite() {
                        mount.ufs.create(&ufs_path, true).await?;
                        if !cv_flags.create() {
                            cv_flags = cv_flags.set_create(true);
                        }
                    }
                    let writer = CacheSyncWriter::new(self, path, &mount, cv_flags).await?;
                    Ok(UnifiedWriter::CacheSync(writer))
                }

                WriteType::AsyncThrough => {
                    self.invalidate_open_notfound_for_ufs_path(&ufs_path);
                    // Async-through should not synchronously depend on UFS create:
                    // transient UFS network errors must not surface as write-path EIO.
                    let mut cv_flags = flags;
                    if flags.overwrite() && !cv_flags.create() {
                        cv_flags = cv_flags.set_create(true);
                    }
                    let writer = CacheSyncWriter::new(self, path, &mount, cv_flags).await?;
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
                self.invalidate_open_notfound_for_ufs_path(&ufs_path);
                let cv_status = match self.cv.mkdir_with_opts(path, opts).await {
                    Ok(status) => Some(status),
                    Err(FsError::FileAlreadyExists(_)) => self.cv.get_status(path).await.ok(),
                    Err(e) => {
                        warn!(
                            "failed to create directory in cache for {}, ignoring: {}",
                            path, e
                        );
                        None
                    }
                };

                if !flag {
                    err_ext!(FsError::file_exists(ufs_path.path()))
                } else {
                    Ok(cv_status)
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
            Some((ufs_path, mount)) => {
                self.invalidate_open_notfound_for_ufs_path(&ufs_path);
                if matches!(mount.info.write_type, WriteType::Through) {
                    self.invalidate_cv_cache_for_mount_write(path).await;
                }
                mount.ufs.append(&ufs_path).await
            }
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

        if matches!(mount.info.write_type, WriteType::Through) {
            // Through mode treats UFS as the single source of truth for reads.
            if self.enable_read_ufs {
                info!(
                    "read from ufs(through), ufs path {}, cv path: {}",
                    ufs_path, path
                );
                let reader = mount.ufs.open(&ufs_path).await?;
                self.pdpc().put_ufs_status_hint(&ufs_path, reader.status());
                return Ok(reader);
            } else {
                return err_ext!(FsError::unsupported_ufs_read(path.path()));
            }
        }
        match self.get_cv_reader(path, &ufs_path, &mount).await? {
            CvReadResult::Hit(reader) => {
                info!(
                    "read from Curvine(cache), ufs path {}, cv path: {}",
                    ufs_path, path
                );

                self.metrics
                    .mount_cache_hits
                    .with_label_values(&[mount.mount_id()])
                    .inc();

                Ok(reader)
            }
            CvReadResult::Miss {
                allow_async_cache,
                source_snapshot,
                expected_target_mtime,
                expected_target_missing,
            } => {
                self.metrics
                    .mount_cache_misses
                    .with_label_values(&[mount.mount_id()])
                    .inc();

                // Reading from ufs
                if self.enable_read_ufs {
                    info!("read from ufs, ufs path {}, cv path: {}", ufs_path, path);
                    if self.pdpc().should_skip_ufs_open_probe(&ufs_path) {
                        return err_ext!(FsError::file_not_found(ufs_path.full_path()));
                    }
                    let reader = match mount.ufs.open(&ufs_path).await {
                        Ok(reader) => {
                            self.pdpc().clear_ufs_open_notfound(&ufs_path);
                            self.pdpc().put_ufs_status_hint(&ufs_path, reader.status());
                            reader
                        }
                        Err(e @ FsError::FileNotFound(_)) | Err(e @ FsError::Expired(_)) => {
                            self.pdpc().mark_ufs_open_notfound(&ufs_path);
                            self.pdpc().clear_ufs_status_hint(&ufs_path);
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    };

                    if mount.info.auto_cache() && allow_async_cache {
                        let source_snapshot = source_snapshot
                            .or_else(|| Some(SourceSnapshot::from_file_status(reader.status())));
                        self.async_cache(
                            &ufs_path,
                            source_snapshot,
                            expected_target_mtime,
                            expected_target_missing,
                        )?;
                    } else if mount.info.auto_cache() {
                        info!(
                            "skip async cache for {}, cv path {} because cache entry is incomplete and cv-only",
                            ufs_path, path
                        );
                    }

                    Ok(reader)
                } else {
                    if mount.info.auto_cache() && allow_async_cache {
                        self.async_cache(
                            &ufs_path,
                            source_snapshot,
                            expected_target_mtime,
                            expected_target_missing,
                        )?;
                    } else if mount.info.auto_cache() {
                        info!(
                            "skip async cache for {}, cv path {} because cache entry is incomplete and cv-only",
                            ufs_path, path
                        );
                    }
                    err_ext!(FsError::unsupported_ufs_read(path.path()))
                }
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
                let is_async_through = matches!(mount.info.write_type, WriteType::AsyncThrough);
                let mut skip_ufs_rename = false;
                if is_async_through {
                    let skip_wait = self
                        .cv
                        .get_status(src)
                        .await
                        .ok()
                        .map(|status| Self::should_use_async_rename_fast_path(&status))
                        .unwrap_or(false);
                    if skip_wait {
                        self.cancel_job_for_mount_meta_op(src, "rename-src").await;
                        skip_ufs_rename = true;
                    } else {
                        self.wait_job_complete_for_mount_meta_op(src, "rename")
                            .await?;
                    }
                } else if mount.info.is_write_through() {
                    self.wait_job_complete_for_mount_meta_op(src, "rename")
                        .await?;
                }

                let preserve_cv_visibility =
                    Self::should_preserve_cv_cache_on_mount_rename(mount.info.write_type);
                let cv_rename_applied = if preserve_cv_visibility {
                    self.try_rename_cv_cache_for_mount_path(src, dst).await?
                } else {
                    false
                };

                let dst_ufs = mount.get_ufs_path(dst)?;
                let mut publish_required_for_ufs_materialization = skip_ufs_rename;
                if !skip_ufs_rename {
                    if let Err(e) = mount.ufs.rename(&src_ufs, &dst_ufs).await {
                        let is_missing =
                            matches!(e, FsError::FileNotFound(_) | FsError::Expired(_));
                        if !(is_async_through && is_missing) {
                            if cv_rename_applied {
                                if let Err(rollback_err) = self.cv.rename(dst, src).await {
                                    warn!(
                                        "failed to rollback CV rename {} -> {} after UFS rename failure: {}",
                                        dst, src, rollback_err
                                    );
                                }
                            }
                            return Err(e);
                        }
                        if is_async_through && is_missing {
                            publish_required_for_ufs_materialization = true;
                        }
                    }
                }
                if is_async_through {
                    if let Err(e) = self.submit_async_publish_after_rename(dst, "rename").await {
                        if publish_required_for_ufs_materialization {
                            if cv_rename_applied {
                                if let Err(rollback_err) = self.cv.rename(dst, src).await {
                                    warn!(
                                        "[rename] failed to rollback CV rename {} -> {} after async publish enqueue failure: {}",
                                        dst, src, rollback_err
                                    );
                                }
                            }
                            self.invalidate_cv_cache_for_mount_meta_path(
                                src,
                                "rename-submit-failed-src",
                            )
                            .await;
                            self.invalidate_cv_cache_for_mount_meta_path(
                                dst,
                                "rename-submit-failed-dst",
                            )
                            .await;
                            return Err(e);
                        }
                        warn!(
                            "[rename] failed to submit async-through publish for {} after rename: {}",
                            dst, e
                        );
                    }
                }

                self.invalidate_open_notfound_for_ufs_path(&src_ufs);
                self.invalidate_open_notfound_for_ufs_path(&dst_ufs);

                if cv_rename_applied {
                    self.invalidate_cv_cache_for_mount_meta_path(src, "rename-src-stale")
                        .await;
                } else {
                    self.invalidate_cv_cache_for_mount_meta_path(src, "rename-src")
                        .await;
                    self.invalidate_cv_cache_for_mount_meta_path(dst, "rename-dst")
                        .await;
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
                let is_async_through = matches!(mount.info.write_type, WriteType::AsyncThrough);

                if is_async_through {
                    // Invalidate async-through source first so in-flight jobs cannot commit stale data.
                    self.cancel_job_for_mount_meta_op(path, "delete").await;
                    if let Err(e) = self.cv.delete(path, recursive).await {
                        if !matches!(e, FsError::FileNotFound(_)) {
                            warn!("failed to delete cache for {}: {}", path, e);
                        }
                    }
                }

                // Delete from UFS
                mount.ufs.delete(&ufs_path, recursive).await?;
                self.invalidate_open_notfound_for_ufs_path(&ufs_path);

                if !is_async_through {
                    // delete cache
                    if let Err(e) = self.cv.delete(path, recursive).await {
                        if !matches!(e, FsError::FileNotFound(_)) {
                            warn!("failed to delete cache for {}: {}", path, e);
                        }
                    };
                }

                Ok(())
            }
        }
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let _timer = TimeSpent::timer_counter_vec(
            Arc::new(FsContext::get_metrics().metadata_operation_duration.clone()),
            vec!["get_status".to_string()],
        );

        let (ufs_path, mount) = match self.get_mount(path).await? {
            None => return self.cv.get_status(path).await,
            Some(v) => v,
        };

        if matches!(mount.info.write_type, WriteType::Through) {
            return mount.ufs.get_status(&ufs_path).await;
        }

        match self.cv.get_status(path).await {
            Ok(v) => match self
                .check_cache_validity(path, &v, &ufs_path, &mount)
                .await?
            {
                CacheValidity::Valid => Ok(v),
                CacheValidity::Invalid(Some(ufs_status)) => Ok(ufs_status),
                CacheValidity::Invalid(None) => mount.ufs.get_status(&ufs_path).await,
            },

            Err(e) => {
                if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                    warn!("failed to get status file {}: {}", path, e);
                };
                mount.ufs.get_status(&ufs_path).await
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
