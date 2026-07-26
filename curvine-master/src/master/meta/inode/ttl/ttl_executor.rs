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
use crate::master::meta::FsDir;
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
        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        Self::build_path_from_store(&fs_dir_guard, inode_id)
    }

    fn build_path_from_store(fs_dir: &FsDir, inode_id: i64) -> FsResult<String> {
        if inode_id == ROOT_INODE_ID {
            return Ok("/".to_string());
        }

        let mut current_id = inode_id;
        let mut components = Vec::new();
        let mut visited = Vec::new();

        while current_id != ROOT_INODE_ID {
            if visited.contains(&current_id) {
                return err_box!("Cycle detected while resolving inode path {}", inode_id);
            }
            visited.push(current_id);

            let inode_view = match fs_dir.store.get_inode(current_id, None)? {
                Some(inode_view) => inode_view,
                None => {
                    return err_box!(
                        "Cannot resolve path for inode {} (missing ancestor {})",
                        inode_id,
                        current_id
                    );
                }
            };

            match &inode_view {
                InodeView::File(f) => {
                    components.push(f.name.clone());
                    current_id = f.parent_id();
                }
                InodeView::Dir(d) => {
                    components.push(d.name.clone());
                    current_id = d.parent_id();
                }
                InodeView::FileEntry(e) => {
                    // FileEntry does not carry parent_id, so preserve the previous fallback.
                    components.push(e.name.clone());
                    break;
                }
            }
        }

        components.reverse();
        Ok(format!("/{}", components.join("/")))
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

        if !inode.is_expired()? {
            return Ok((false, inode));
        }

        let action = inode.storage_policy()?.ttl_action;

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
