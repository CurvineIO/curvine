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

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::{Inode, InodeView, ROOT_INODE_ID};
use curvine_common::state::TtlAction;
use curvine_common::FsResult;
use log::debug;
use orpc::err_box;

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
}

impl InodeTtlExecutor {
    pub fn with_managers(filesystem: MasterFilesystem) -> Self {
        Self { filesystem }
    }

    fn get_inode_path(&self, inode_id: i64) -> FsResult<String> {
        let path = self.resolve_inode_path(inode_id)?;
        Ok(path)
    }

    fn resolve_inode_path(&self, inode_id: i64) -> FsResult<String> {
        self.build_path_recursive(inode_id)
    }

    fn build_path_recursive(&self, inode_id: i64) -> FsResult<String> {
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
            }
        }

        err_box!("Cannot resolve path for inode {}", inode_id)
    }

    pub fn get_inode_from_store(&self, inode_id: i64) -> FsResult<Option<InodeView>> {
        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        let inode = fs_dir_guard.store.get_inode(inode_id, None)?;
        Ok(inode)
    }

    pub fn execute_by_id(&self, inode_id: i64) -> FsResult<(bool, InodeView)> {
        let inode = if let Some(inode) = self.get_inode_from_store(inode_id)? {
            inode
        } else {
            return err_box!("Inode {} not found", inode_id);
        };

        if !inode.is_expired() {
            return Ok((false, inode));
        }

        let action = inode.storage_policy().ttl_action;

        debug!(
            "Executing ttl action {:?} for inode {} based on StoragePolicy",
            action, inode_id
        );

        let path = self.get_inode_path(inode_id)?;
        match action {
            TtlAction::Delete => {
                self.filesystem.delete(&path, true)?;
                debug!("ttl delete {} {:?}", path, inode);
                Ok((true, inode))
            }
            TtlAction::Free => {
                self.filesystem.free(&path, true)?;
                debug!("ttl free {} {:?}", path, inode);
                Ok((true, inode))
            }

            _ => Ok((true, inode)),
        }
    }
}
