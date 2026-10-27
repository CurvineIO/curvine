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

use crate::master::fs::{DeleteMode, MasterFilesystem};
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::meta::trash;
use curvine_core_error::err_ext;
use curvine_error::{FsError, FsResult};
use curvine_model::TtlAction;
use log::debug;

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
        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        fs_dir_guard.get_inode_path(inode_id)
    }

    pub fn get_inode_from_store(&self, inode_id: i64) -> FsResult<Option<InodeView>> {
        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        let inode = fs_dir_guard.store.get_inode(inode_id, None)?;
        Ok(inode)
    }

    fn is_trash_entry(inode: &InodeView) -> bool {
        if inode.is_file_entry() {
            return false;
        }
        inode
            .x_attr()
            .map(|x_attr| x_attr.contains_key(trash::TRASH_XATTR_ORIGIN))
            .unwrap_or(false)
    }

    pub fn is_active(&self) -> bool {
        self.filesystem.master_monitor.is_active()
    }

    fn is_empty_dir(&self, path: &str) -> FsResult<bool> {
        let fs_dir = self.filesystem.fs_dir();
        let fs_dir_guard = fs_dir.read();
        let inp = InodePath::resolve(fs_dir_guard.root_ptr(), path, &fs_dir_guard.store)?;
        Ok(inp.is_empty_dir())
    }

    // Returns (handled, inode, reclaimed_bytes). `handled` drives TTL bucket
    // bookkeeping; `reclaimed_bytes` lets quota eviction count only space that
    // was actually released.
    pub fn execute_by_id(&self, inode_id: i64) -> FsResult<(bool, InodeView, i64)> {
        // Expiry removes the entry permanently. Routing it through the recycle
        // bin would keep the blocks for another retention window while this
        // executor reports the length as reclaimed, so the quota never sees the
        // space come back. Quota eviction keeps its own explicit `AsyncPurge`.
        self.execute_by_id_with_mode(inode_id, DeleteMode::SkipTrash)
    }

    /// Executes the TTL action for `inode_id` with an explicit delete mode.
    ///
    /// Quota eviction passes [`DeleteMode::AsyncPurge`] so a victim is reclaimed
    /// by the trash purger instead of occupying the recycle bin for the whole
    /// retention window, which would make eviction unable to relieve pressure.
    pub fn execute_by_id_with_mode(
        &self,
        inode_id: i64,
        delete_mode: DeleteMode,
    ) -> FsResult<(bool, InodeView, i64)> {
        let inode = if let Some(inode) = self.get_inode_from_store(inode_id)? {
            inode
        } else {
            return err_ext!(FsError::file_not_found(format!("inode_id={}", inode_id)));
        };

        // Trash entries are governed by trash retention, not TTL or quota
        // eviction. Detect them from the internal marker before the expiry
        // check: the marker is cheap (no path walk) and makes a trashed entry
        // report `handled` with zero reclaimed bytes even when it has no TTL,
        // so eviction drops it from the candidate set without counting it as
        // progress. Blocks stay allocated until a purge reclaims them.
        if Self::is_trash_entry(&inode) {
            debug!("skip ttl action for trash entry inode_id={}", inode_id);
            return Ok((true, inode, 0));
        }

        if !inode.is_expired()? {
            return Ok((false, inode, 0));
        }

        let action = inode.storage_policy()?.ttl_action;

        debug!(
            "Executing ttl action {:?} for inode {} based on StoragePolicy",
            action, inode_id
        );

        let path = self.get_inode_path(inode_id)?;
        if trash::is_trash_path(&path) {
            // Entries placed under /.Trash without the internal marker (for
            // example restored/foreign entries) are still not TTL candidates.
            debug!("skip ttl action for trash path {} ({})", path, inode_id);
            return Ok((true, inode, 0));
        }

        match action {
            TtlAction::Delete => {
                // A non-empty directory stays until its children are gone.
                // Returning (false, inode, ..) puts it back in its TTL bucket.
                // Free is not gated here: it drops cached blocks and leaves children.
                if inode.is_dir() && !self.is_empty_dir(&path)? {
                    debug!("skip ttl delete for non-empty directory {}", inode_id);
                    return Ok((false, inode, 0));
                }
                // Unlinking one name of a hard-linked file keeps the shared
                // blocks, so only the last link reclaims the file size. A staged
                // async purge has not reclaimed anything yet either.
                let reclaimed = match &inode {
                    InodeView::File(f)
                        if f.nlink() <= 1 && !matches!(delete_mode, DeleteMode::AsyncPurge) =>
                    {
                        f.len.max(0)
                    }
                    _ => 0,
                };
                self.filesystem.delete_with_mode(&path, true, delete_mode)?;
                debug!("ttl delete {} {:?}", path, inode);
                Ok((true, inode, reclaimed))
            }
            TtlAction::Free => {
                let reclaimed = match &inode {
                    InodeView::File(f) => f.len.max(0),
                    _ => 0,
                };
                self.filesystem.free(&path, true)?;
                debug!("ttl free {} {:?}", path, inode);
                Ok((true, inode, reclaimed))
            }

            _ => Ok((true, inode, 0)),
        }
    }
}
