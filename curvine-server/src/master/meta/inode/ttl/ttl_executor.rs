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

use crate::common::UfsFactory;
use crate::master::fs::MasterFilesystem;
use crate::master::job::JobManager;
use crate::master::meta::inode::ttl_types::{TtlError, TtlResult};
use crate::master::meta::inode::{Inode, InodeView, ROOT_INODE_ID};
use crate::master::mount::MountManager;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{LoadJobCommand, TtlAction};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// TTL Executor Module
//
// This module provides the execution layer for TTL operations on inodes.
// It integrates with the filesystem to perform actual delete and free operations.
//
// Key Features:
// - Filesystem-integrated TTL execution
// - Intelligent path resolution and caching
// - Real filesystem operations (delete/free)
// - High-performance batch processing
// - Complete error handling and monitoring
// - Support for different storage policies
// - UFS (Unified File System) integration for data migration

#[derive(Clone)]
pub struct InodeTtlExecutor {
    filesystem: MasterFilesystem,
    path_cache: Arc<RwLock<HashMap<u64, String>>>,
    mount_manager: Arc<MountManager>,
    factory: Arc<UfsFactory>,
    job_manager: Arc<JobManager>,
}

impl InodeTtlExecutor {
    pub fn with_managers(
        filesystem: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        factory: Arc<UfsFactory>,
        job_manager: Arc<JobManager>,
    ) -> Self {
        Self {
            filesystem,
            path_cache: Arc::new(RwLock::new(HashMap::new())),
            mount_manager,
            factory,
            job_manager,
        }
    }

    fn get_inode_path(&self, inode_id: u64) -> TtlResult<String> {
        if let Ok(cache) = self.path_cache.read() {
            if let Some(path) = cache.get(&inode_id) {
                debug!("Cache hit for inode {}: {}", inode_id, path);
                return Ok(path.clone());
            }
        }

        let path = self.resolve_inode_path(inode_id as i64)?;

        if let Ok(mut cache) = self.path_cache.write() {
            cache.insert(inode_id, path.clone());
            debug!("Cached path for inode {}: {}", inode_id, path);
        }

        Ok(path)
    }

    fn resolve_inode_path(&self, inode_id: i64) -> TtlResult<String> {
        self.build_path_recursive(inode_id)
    }

    fn build_path_recursive(&self, inode_id: i64) -> TtlResult<String> {
        if inode_id == ROOT_INODE_ID {
            return Ok("/".to_string());
        }

        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        if let Ok(Some(inode_view)) = fs_dir_guard.store.get_inode(inode_id, None) {
            match &inode_view {
                InodeView::File(name, file) => {
                    let parent_path = self.build_path_recursive(file.parent_id())?;
                    let file_path = if parent_path == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", parent_path, name)
                    };
                    return Ok(file_path);
                }
                InodeView::Dir(name, dir) => {
                    let parent_path = self.build_path_recursive(dir.parent_id())?;
                    let dir_path = if parent_path == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", parent_path, name)
                    };
                    return Ok(dir_path);
                }
                InodeView::FileEntry(name, _) => {
                    // For empty files, we can't determine parent_id, so return a basic path
                    return Ok(format!("/{}", name));
                }
                InodeView::Container(name, _) => {
                    // For containers, we can't determine parent_id, so return a basic path
                    return Ok(format!("/{}", name));
                } // will update
            }
        }

        Err(TtlError::ServiceError(format!(
            "Cannot resolve path for inode {}",
            inode_id
        )))
    }

    pub fn get_inode_from_store(&self, inode_id: u64) -> TtlResult<Option<InodeView>> {
        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        fs_dir_guard
            .store
            .get_inode(inode_id as i64, None)
            .map_err(|e| TtlError::ServiceError(format!("Failed to get inode from store: {}", e)))
    }

    pub fn delete_inode(&self, inode_id: u64) -> TtlResult<()> {
        debug!("Deleting inode: {}", inode_id);
        let path = self.get_inode_path(inode_id)?;
        match self.filesystem.delete(&path, true) {
            Ok(_) => {
                info!("Successfully deleted file(dir): {}", path);

                if let Ok(mut cache) = self.path_cache.write() {
                    cache.remove(&inode_id);
                }

                Ok(())
            }
            Err(e) => {
                error!("Failed to delete file {}: {}", path, e);
                Err(TtlError::ActionExecutionError(format!(
                    "Delete failed: {}",
                    e
                )))
            }
        }
    }

    pub fn free_inode(&self, inode_id: u64) -> TtlResult<()> {
        info!("Freeing inode: {}", inode_id);

        let path = self.get_inode_path(inode_id)?;
        let inode = self.get_inode_from_store(inode_id)?.ok_or_else(|| {
            TtlError::ActionExecutionError(format!("Inode {} not found", inode_id))
        })?;

        match inode {
            InodeView::File(_, file) => {
                let action = file.storage_policy.ttl_action;
                self.free(inode_id, &path, action, false)?;
            }
            InodeView::Dir(_, dir) => {
                let action = dir.storage_policy.ttl_action;
                self.free(inode_id, &path, action, true)?;
            }
            InodeView::FileEntry(..) => {
                return Err(TtlError::ActionExecutionError(format!(
                    "Cannot free empty file: {}",
                    path
                )));
            }
            InodeView::Container(_, _) => {} // will update
        }

        Ok(())
    }

    fn free(
        &self,
        inode_id: u64,
        path: &str,
        action: TtlAction,
        is_directory: bool,
    ) -> TtlResult<()> {
        info!(
            "Freeing {}: {}, action={:?}",
            self.resource_type(is_directory),
            path,
            action
        );

        let cv_path = Path::from_str(path)
            .map_err(|e| TtlError::ActionExecutionError(format!("Invalid path {}: {}", path, e)))?;

        let mount_info = self
            .mount_manager
            .get_mount_info(&cv_path)
            .map_err(|e| {
                TtlError::ActionExecutionError(format!("Failed to get mount info: {}", e))
            })?
            .ok_or_else(|| {
                TtlError::ActionExecutionError(format!("No mount point for path: {}", path))
            })?;

        let ufs_path = mount_info.get_ufs_path(&cv_path).map_err(|e| {
            TtlError::ActionExecutionError(format!("Failed to get UFS path: {}", e))
        })?;

        let ufs_exists = self.check_ufs_exists(&mount_info, &ufs_path)?;

        match (ufs_exists, action) {
            (true, TtlAction::Persist | TtlAction::Evict) => {
                info!(
                    "UFS {} already exists, skipping job: {}",
                    self.resource_type(is_directory),
                    ufs_path.full_path()
                );
                self.handle_skip_job(inode_id, action, path, is_directory)?;
            }
            _ => {
                info!(
                    "Submitting export job for {}: {}",
                    self.resource_type(is_directory),
                    path
                );
                self.submit_export_job(
                    inode_id,
                    path,
                    &ufs_path,
                    action,
                    is_directory,
                    mount_info,
                )?;
            }
        }

        Ok(())
    }

    // Helper: Get resource type string (eliminates repetition)
    #[inline]
    fn resource_type(&self, is_directory: bool) -> &'static str {
        if is_directory {
            "directory"
        } else {
            "file"
        }
    }

    fn check_ufs_exists(
        &self,
        mount_info: &curvine_common::state::MountInfo,
        ufs_path: &Path,
    ) -> TtlResult<bool> {
        let handle = tokio::runtime::Handle::current();

        let ufs_path_clone = ufs_path.clone();
        let mount_info_clone = mount_info.clone();
        let factory = self.factory.clone();

        let exists = handle.block_on(async move {
            let ufs = factory
                .get_ufs(&mount_info_clone)
                .map_err(|e| TtlError::ServiceError(format!("Failed to get UFS: {}", e)))?;

            let result: Result<bool, TtlError> = match ufs.get_status(&ufs_path_clone).await {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            };
            result
        })?;

        Ok(exists)
    }

    fn handle_skip_job(
        &self,
        _inode_id: u64,
        action: TtlAction,
        cv_path: &str,
        is_directory: bool,
    ) -> TtlResult<()> {
        let resource_type = self.resource_type(is_directory);

        match action {
            TtlAction::Persist => {
                info!(
                    "Persist: UFS {} exists, keeping Curvine {}: {}",
                    resource_type, resource_type, cv_path
                );
            }
            TtlAction::Evict => {
                info!(
                    "Evict: UFS {} exists, deleting Curvine {}: {}",
                    resource_type, resource_type, cv_path
                );
                self.filesystem.delete(cv_path, is_directory).map_err(|e| {
                    TtlError::ActionExecutionError(format!("Failed to delete {}: {}", cv_path, e))
                })?;
            }
            _ => {}
        }
        Ok(())
    }

    fn submit_export_job(
        &self,
        inode_id: u64,
        cv_path: &str,
        ufs_path: &Path,
        action: TtlAction,
        is_directory: bool,
        _mount_info: curvine_common::state::MountInfo,
    ) -> TtlResult<()> {
        let command = LoadJobCommand {
            source_path: cv_path.to_string(),
            target_path: Some(ufs_path.full_path().to_string()),
            overwrite: Some(matches!(action, TtlAction::Flush)),
            replicas: None,
            block_size: None,
            storage_type: None,
            ttl_ms: None,
            ttl_action: None,
        };

        let job_result = self.job_manager.submit_load_job(command).map_err(|e| {
            TtlError::ActionExecutionError(format!("Failed to submit export job: {}", e))
        })?;

        info!(
            "Export job submitted for {}: job_id={}, inode={}, action={:?}",
            self.resource_type(is_directory),
            job_result.job_id,
            inode_id,
            action
        );

        self.register_export_callback(
            job_result.job_id,
            inode_id,
            action,
            cv_path.to_string(),
            is_directory,
        )?;

        Ok(())
    }

    fn register_export_callback(
        &self,
        job_id: String,
        inode_id: u64,
        action: TtlAction,
        cv_path: String,
        is_directory: bool,
    ) -> TtlResult<()> {
        let filesystem = self.filesystem.clone();

        self.job_manager.jobs().register_completion_callback(
            job_id.clone(),
            move |job_id_str, _old_state, new_state, job_ctx| {
                if new_state == curvine_common::state::JobTaskState::Completed {
                    info!(
                        "Export job {} completed successfully for inode {}",
                        job_id_str, inode_id
                    );

                    // Note: resource_type is captured from closure, but we can't call self here
                    // So we compute it inline (consistent with other methods now using helper)
                    let resource_type = if is_directory { "directory" } else { "file" };

                    match action {
                        TtlAction::Persist => {
                            info!(
                                "Persist completed, keeping Curvine {}: {}",
                                resource_type, cv_path
                            );
                        }
                        TtlAction::Evict | TtlAction::Flush => {
                            info!(
                                "Evict/Flush completed, deleting Curvine {}: {}",
                                resource_type, cv_path
                            );
                            // Use recursive=true for directories, false for files
                            if let Err(e) = filesystem.delete(&cv_path, is_directory) {
                                error!(
                                    "Failed to delete Curvine {} after export: {}",
                                    resource_type, e
                                );
                            }
                        }
                        _ => {}
                    }
                } else {
                    warn!(
                        "Export job {} failed: {}",
                        job_id_str, job_ctx.progress.message
                    );
                }
            },
        );

        Ok(())
    }
}
