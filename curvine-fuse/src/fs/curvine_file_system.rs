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

use crate::fs::operator::*;
use crate::fs::state::{FileHandle, NodeState, OpenSnapshot};
use crate::raw::fuse_abi::*;
use crate::raw::FuseDirentList;
use crate::session::{FuseBuf, FuseResponse};
use crate::*;
use crate::{err_fuse, FuseError, FuseResult, FuseUtils};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::{ClusterConf, FuseConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, StateReader, StateWriter};
use curvine_common::state::{
    CreateFileOptsBuilder, FileAllocMode, FileAllocOpts, FileLock, FileStatus, LockFlags, LockType,
    MkdirOptsBuilder, OpenFlags, SetAttrOpts,
};
use log::{debug, error, info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::runtime::Runtime;
use orpc::sys::FFIUtils;
use orpc::{sys, ternary, try_option};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::bytes::BytesMut;

pub struct CurvineFileSystem {
    fs: UnifiedFileSystem,
    state: NodeState,
    conf: FuseConf,
}

impl CurvineFileSystem {
    pub fn new(conf: ClusterConf, rt: Arc<Runtime>) -> FuseResult<Self> {
        let fuse_conf = conf.fuse.clone();
        let fs = UnifiedFileSystem::with_rt(conf, rt)?;
        let state = NodeState::new(fs.clone());

        let fuse_fs = Self {
            fs,
            state,
            conf: fuse_conf,
        };

        Ok(fuse_fs)
    }

    fn fill_open_flags(conf: &FuseConf, v: u32) -> u32 {
        let mut flags = v;
        if conf.direct_io {
            flags |= FUSE_FOPEN_DIRECT_IO;
        } else {
            flags |= FUSE_FOPEN_KEEP_CACHE;
        }
        if conf.cache_readdir {
            flags |= FUSE_FOPEN_CACHE_DIR
        }
        if conf.non_seekable {
            flags |= FUSE_FOPEN_NONSEEKABLE
        }

        flags
    }

    pub fn conf(&self) -> &FuseConf {
        &self.conf
    }

    pub fn status_to_attr(conf: &FuseConf, status: &FileStatus) -> FuseResult<fuse_attr> {
        let blocks = ((status.len + 511) / 512) as u64;

        let mtime_sec = (status.mtime.max(0) / 1000) as u64;
        let mtime_nsec = ((status.mtime.max(0) % 1000) * 1_000_000) as u32;

        let atime_sec = (status.atime.max(0) / 1000) as u64;
        let atime_nsec = ((status.atime.max(0) % 1000) * 1_000_000) as u32;

        let ctime_sec = mtime_sec;
        let ctime_nsec = mtime_nsec;

        let uid = if status.owner.is_empty() {
            conf.uid
        } else if let Ok(numeric_uid) = status.owner.parse::<u32>() {
            numeric_uid
        } else {
            match sys::get_uid_by_name(&status.owner) {
                Some(uid) => uid,
                None => conf.uid,
            }
        };

        let gid = if status.group.is_empty() {
            conf.gid
        } else if let Ok(numeric_gid) = status.group.parse::<u32>() {
            numeric_gid
        } else {
            match sys::get_gid_by_name(&status.group) {
                Some(gid) => gid,
                None => conf.gid,
            }
        };

        let mode = if status.mode != 0 {
            FuseUtils::get_mode(status.mode, status.file_type)
        } else {
            FuseUtils::get_mode(FUSE_DEFAULT_MODE & !conf.umask, status.file_type)
        };
        let size = FuseUtils::fuse_st_size(status);

        // For links, nlink should be greater than 1
        // Now we use the actual nlink from FileStatus
        let nlink = status.nlink;

        Ok(fuse_attr {
            ino: status.id as u64,
            size,
            blocks,
            atime: atime_sec,
            mtime: mtime_sec,
            ctime: ctime_sec,
            atimensec: atime_nsec,
            mtimensec: mtime_nsec,
            ctimensec: ctime_nsec,
            mode,
            nlink,
            uid,
            gid,
            rdev: 0,
            blksize: FUSE_BLOCK_SIZE as u32,
            padding: 0,
        })
    }

    pub fn create_entry_out(conf: &FuseConf, attr: fuse_attr) -> fuse_entry_out {
        fuse_entry_out {
            nodeid: attr.ino,
            generation: 0,
            entry_valid: conf.entry_ttl.as_secs(),
            attr_valid: conf.attr_ttl.as_secs(),
            entry_valid_nsec: conf.entry_ttl.subsec_nanos(),
            attr_valid_nsec: conf.attr_ttl.subsec_nanos(),
            attr,
        }
    }

    pub fn new_dot_status(name: &str) -> FileStatus {
        FileStatus::with_name(FUSE_UNKNOWN_INO as i64, name.to_string(), true)
    }

    fn to_file_lock(&self, arg: &fuse_lk_in) -> FileLock {
        let client_id = self.fs.cv().fs_context().clone_client_name();
        FileLock {
            client_id,
            owner_id: arg.owner,
            pid: arg.lk.pid,
            lock_type: LockType::from(arg.lk.typ as u8),
            lock_flags: LockFlags::from(arg.lk_flags as u8),
            start: arg.lk.start,
            end: arg.lk.end,
            ..Default::default()
        }
    }

    async fn fs_unlock(&self, handler: &FileHandle, flags: LockFlags) -> FuseResult<()> {
        if let Some(owner_id) = handler.remove_lock(flags) {
            let client_id = self.fs.cv().fs_context().clone_client_name();
            let path = Path::from_str(&handler.status.path)?;

            let mut lock = FileLock {
                client_id,
                owner_id,
                lock_type: LockType::UnLock,
                lock_flags: flags,
                ..Default::default()
            };
            if flags == LockFlags::Plock {
                lock.start = 0;
                lock.end = u64::MAX;
            }

            self.fs.set_lock(&path, lock).await?;
        }

        Ok(())
    }

    async fn fs_get_status(&self, path: &Path) -> FuseResult<FileStatus> {
        let status = match self.fs.get_status(path).await {
            Ok(v) => v,
            Err(e) => {
                return match e {
                    FsError::FileNotFound(_) => err_fuse!(libc::ENOENT, "{}", e),
                    _ => Err(FuseError::from(e)),
                }
            }
        };
        Ok(status)
    }

    fn can_retry_stale_write_open(flags: OpenFlags, err: &FuseError) -> bool {
        err.errno == libc::ENOENT
            && flags.write_only()
            && (flags.truncate() || flags.append())
            && !flags.create()
    }

    fn can_retry_stale_create_open(flags: OpenFlags, err: &FuseError) -> bool {
        err.errno == libc::ENOENT && flags.write() && flags.create()
    }

    fn can_retry_stale_open(flags: OpenFlags, err: &FuseError) -> bool {
        Self::can_retry_stale_write_open(flags, err)
            || Self::can_retry_stale_create_open(flags, err)
    }

    fn should_force_create_for_namespace_stale_open(
        flags: OpenFlags,
        err: &FuseError,
        namespace_entry_matches_ino: bool,
    ) -> bool {
        err.errno == libc::ENOENT
            && namespace_entry_matches_ino
            && flags.write_only()
            && (flags.truncate() || flags.append())
            && !flags.create()
    }

    fn namespace_entry_matches_ino(&self, ino: u64, path: &Path) -> bool {
        let parent_ino = match self.state.get_parent_id(ino) {
            Ok(v) if v != 0 => v,
            _ => return false,
        };

        self.state.lookup_node_id(parent_ino, Some(path.name())) == Some(ino)
    }

    async fn wait_read_visibility_barrier(&self, ino: u64, path: &Path) {
        if !self.state.has_open_writer_handles(ino) && !self.state.is_writer_closing(ino) {
            return;
        }

        self.state.wait_no_open_writer_handles(ino).await;
        self.state.wait_writer_release_barrier(ino).await;
        if let Err(e) = self.invalidate_cache(path) {
            warn!(
                "failed to invalidate cache for {} after read barrier: {}",
                path, e
            );
        }
    }

    async fn wait_lookup_visibility_barrier<T: AsRef<str>>(&self, parent: u64, name: Option<T>) {
        if let Some(ino) = self.state.lookup_node_id(parent, name) {
            let path = match self.state.get_path(ino) {
                Ok(path) => path,
                Err(_) => return,
            };
            self.wait_read_visibility_barrier(ino, &path).await;
        }
    }

    async fn lookup_status_hint(
        &self,
        parent: u64,
        name: Option<&str>,
    ) -> Option<(u64, FileStatus)> {
        let ino = self.state.lookup_node_id(parent, name)?;
        if let Some(status) = self.state.writer_status_snapshot(ino).await {
            return Some((ino, status));
        }
        self.state
            .handle_status_snapshot(ino)
            .map(|status| (ino, status))
    }

    fn lookup_retry_window(&self) -> Duration {
        let mut retry_window = self.conf.negative_ttl;
        let min_window = Duration::from_millis(30);
        let max_window = Duration::from_millis(200);
        if retry_window < min_window {
            retry_window = min_window;
        }
        if retry_window > max_window {
            retry_window = max_window;
        }
        retry_window
    }

    async fn retry_lookup_status_after_enoent(
        &self,
        ino: u64,
        path: &Path,
    ) -> FuseResult<FileStatus> {
        let retry_window = self.lookup_retry_window();
        let start = Instant::now();
        let mut retries = 0u32;

        loop {
            if !self.namespace_entry_matches_ino(ino, path) {
                return err_fuse!(
                    libc::ENOENT,
                    "lookup namespace entry disappeared for {}",
                    path
                );
            }

            if let Some(status) = self.state.writer_status_snapshot(ino).await {
                return Ok(status);
            }
            if let Some(status) = self.state.handle_status_snapshot(ino) {
                return Ok(status);
            }

            self.wait_read_visibility_barrier(ino, path).await;
            self.invalidate_cache(path)?;

            match self.get_cached_status(path).await {
                Ok(status) => return Ok(status),
                Err(e) if e.errno == libc::ENOENT && start.elapsed() < retry_window => {}
                Err(e) => return Err(e),
            }

            retries += 1;
            if start.elapsed() >= retry_window {
                return err_fuse!(
                    libc::ENOENT,
                    "lookup visibility not ready for {} after {} retries",
                    path,
                    retries
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn load_lookup_status(
        &self,
        parent: u64,
        name: Option<&str>,
        path: &Path,
    ) -> FuseResult<FileStatus> {
        if let Some((_, status)) = self.lookup_status_hint(parent, name).await {
            return Ok(status);
        }

        match self.get_cached_status(path).await {
            Ok(status) => Ok(status),
            Err(e) if e.errno == libc::ENOENT => {
                if let Some((ino, status)) = self.lookup_status_hint(parent, name).await {
                    return if self.namespace_entry_matches_ino(ino, path) {
                        Ok(status)
                    } else {
                        Err(e)
                    };
                }

                if let Some(ino) = self.state.lookup_node_id(parent, name) {
                    return self.retry_lookup_status_after_enoent(ino, path).await;
                }

                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn fs_set_attr(
        &self,
        path: &Path,
        opts: SetAttrOpts,
    ) -> FuseResult<Option<FileStatus>> {
        match self.fs.fuse_set_attr(path, opts).await {
            Ok(v) => Ok(v),
            Err(e) => {
                let e: FuseError = e.into();
                err_fuse!(e.errno, "Failed to set attr {}: {}", path, e)
            }
        }
    }

    async fn lookup_path<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
        path: &Path,
    ) -> FuseResult<fuse_attr> {
        let name = name.as_ref();
        let status = self.get_cached_status(path).await?;
        let attr = self.state.do_lookup(parent, name, &status)?;
        Ok(attr)
    }

    fn lookup_status<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
        status: &FileStatus,
    ) -> FuseResult<fuse_attr> {
        let name = name.as_ref();
        let attr = self.state.do_lookup(parent, name, status)?;
        Ok(attr)
    }

    async fn read_dir_common(
        &self,
        header: &fuse_in_header,
        arg: &fuse_read_in,
        plus: bool,
    ) -> FuseResult<FuseDirentList> {
        let handle = self.state.find_dir_handle(header.nodeid, arg.fh)?;

        let mut map = self.state.node_write();
        let mut res = FuseDirentList::new(arg);
        for (index, status) in handle.get_list(arg.offset as usize) {
            let attr = if status.name != FUSE_CURRENT_DIR && status.name != FUSE_PARENT_DIR {
                if self.conf.enable_meta_cache {
                    let path = Path::from_str(&status.path)?;
                    self.state.meta_cache().put_status(&path, status.clone());
                }
                map.do_lookup(header.nodeid, Some(&status.name), status)?
            } else {
                Self::status_to_attr(&self.conf, status)?
            };
            let entry = Self::create_entry_out(&self.conf, attr);

            if plus {
                if !res.add_plus((index + 1) as u64, status, entry) {
                    break;
                }
            } else if !res.add((index + 1) as u64, status, entry) {
                break;
            }
        }
        Ok(res)
    }

    async fn check_permissions(
        &self,
        path: &Path,
        header: &fuse_in_header,
        mask: u32,
    ) -> FuseResult<()> {
        if header.uid == 0 || !self.conf.check_permission {
            return Ok(());
        }
        if self.open_snapshot_contract_enabled() {
            let snapshot = self.load_open_snapshot(path).await?;
            self.check_permissions_with_snapshot(&snapshot, header, mask)
        } else {
            let status = self.get_cached_status(path).await?;
            self.check_access_permissions(&status, header, mask)
        }
    }

    fn open_snapshot_contract_enabled(&self) -> bool {
        true
    }

    async fn load_open_snapshot(&self, path: &Path) -> FuseResult<OpenSnapshot> {
        let status = self.get_cached_status(path).await?;
        Ok(OpenSnapshot::new(status))
    }

    fn check_permissions_with_snapshot(
        &self,
        snapshot: &OpenSnapshot,
        header: &fuse_in_header,
        mask: u32,
    ) -> FuseResult<()> {
        if header.uid == 0 || !self.conf.check_permission {
            return Ok(());
        }
        self.check_access_permissions(snapshot.status(), header, mask)
    }

    async fn resolve_open_snapshot(
        &self,
        ino: u64,
        path: &Path,
        header: &fuse_in_header,
        action: OpenAction,
        flags: OpenFlags,
    ) -> FuseResult<(Option<OpenSnapshot>, bool)> {
        if !self.open_snapshot_contract_enabled() {
            let fallback_create_open = self
                .resolve_open_legacy(ino, path, header, action, flags)
                .await?;
            return Ok((None, fallback_create_open));
        }

        if action.read() {
            self.wait_read_visibility_barrier(ino, path).await;
        }

        if header.uid == 0 || !self.conf.check_permission {
            return Ok((None, false));
        }

        let attempt = self.load_open_snapshot(path).await;
        match attempt {
            Ok(snapshot) => {
                if let Err(e) =
                    self.check_permissions_with_snapshot(&snapshot, header, action.acl_mask())
                {
                    if action.read() && e.errno == libc::ENOENT {
                        let snapshot = self
                            .retry_read_open_snapshot_after_enoent(ino, path, header, action)
                            .await?;
                        return Ok((Some(snapshot), false));
                    } else if !Self::can_retry_stale_open(flags, &e) {
                        return Err(e);
                    }

                    let fallback_create_open = self
                        .allow_stale_write_open_fallback(ino, path, header, action, flags)
                        .await?;
                    Ok((None, fallback_create_open))
                } else {
                    Ok((Some(snapshot), false))
                }
            }
            Err(e) => {
                if action.read() && e.errno == libc::ENOENT {
                    let snapshot = self
                        .retry_read_open_snapshot_after_enoent(ino, path, header, action)
                        .await?;
                    Ok((Some(snapshot), false))
                } else if !Self::can_retry_stale_open(flags, &e) {
                    Err(e)
                } else {
                    let fallback_create_open = self
                        .allow_stale_write_open_fallback(ino, path, header, action, flags)
                        .await?;
                    Ok((None, fallback_create_open))
                }
            }
        }
    }

    async fn retry_read_open_snapshot_after_enoent(
        &self,
        ino: u64,
        path: &Path,
        header: &fuse_in_header,
        action: OpenAction,
    ) -> FuseResult<OpenSnapshot> {
        let retry_window = self.conf.negative_ttl.max(Duration::from_millis(20));
        let start = Instant::now();
        let mut retries = 0u32;

        loop {
            self.wait_read_visibility_barrier(ino, path).await;
            self.invalidate_cache(path)?;

            match self.load_open_snapshot(path).await {
                Ok(snapshot) => {
                    match self.check_permissions_with_snapshot(&snapshot, header, action.acl_mask())
                    {
                        Ok(()) => return Ok(snapshot),
                        Err(e) if e.errno == libc::ENOENT && start.elapsed() < retry_window => {}
                        Err(e) => return Err(e),
                    }
                }
                Err(e) if e.errno == libc::ENOENT && start.elapsed() < retry_window => {}
                Err(e) => return Err(e),
            }

            retries += 1;
            if start.elapsed() >= retry_window {
                return err_fuse!(
                    libc::ENOENT,
                    "stale read-open visibility not ready for {} after {} retries",
                    path,
                    retries
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn resolve_open_legacy(
        &self,
        ino: u64,
        path: &Path,
        header: &fuse_in_header,
        action: OpenAction,
        flags: OpenFlags,
    ) -> FuseResult<bool> {
        let mut fallback_create_open = false;
        if action.read() {
            self.wait_read_visibility_barrier(ino, path).await;
        }
        if let Err(e) = self
            .check_permissions(path, header, action.acl_mask())
            .await
        {
            if action.read() && e.errno == libc::ENOENT {
                self.wait_read_visibility_barrier(ino, path).await;
                self.invalidate_cache(path)?;
                self.check_permissions(path, header, action.acl_mask())
                    .await?;
            } else if !Self::can_retry_stale_open(flags, &e) {
                return Err(e);
            }

            if action.read() {
                fallback_create_open = false;
            } else {
                fallback_create_open = self
                    .allow_stale_write_open_fallback(ino, path, header, action, flags)
                    .await?;
            }
        }

        Ok(fallback_create_open)
    }

    async fn allow_stale_write_open_fallback(
        &self,
        ino: u64,
        path: &Path,
        header: &fuse_in_header,
        action: OpenAction,
        flags: OpenFlags,
    ) -> FuseResult<bool> {
        if action.read() {
            return Ok(false);
        }

        if let Ok(parent_id) = self.state.get_parent_id(ino) {
            if parent_id != 0 {
                let parent_path = self.state.get_path(parent_id)?;
                if let Err(parent_err) = self
                    .check_permissions(&parent_path, header, (libc::W_OK | libc::X_OK) as u32)
                    .await
                {
                    if parent_err.errno != libc::ENOENT {
                        return Err(parent_err);
                    }
                }
            }
        }

        self.wait_read_visibility_barrier(ino, path).await;
        self.invalidate_cache(path)?;
        if flags.create() {
            debug!(
                "allow stale create-open fallback for {} with flags={} and O_CREAT",
                path,
                flags.access_mark()
            );
            return Ok(true);
        }
        match self
            .check_permissions(path, header, action.acl_mask())
            .await
        {
            Ok(()) => {
                debug!(
                    "allow stale write-open fallback for {} with flags={} without O_CREAT",
                    path,
                    flags.access_mark()
                );
                Ok(false)
            }

            Err(e)
                if Self::should_force_create_for_namespace_stale_open(
                    flags,
                    &e,
                    self.namespace_entry_matches_ino(ino, path),
                ) =>
            {
                warn!(
                    "force O_CREAT for stale namespace entry ino={} path={} flags={} after {}",
                    ino,
                    path,
                    flags.access_mark(),
                    e
                );
                Ok(true)
            }

            Err(e) => Err(e),
        }
    }

    /// Check if the current user has the requested access permissions
    fn check_access_permissions(
        &self,
        status: &FileStatus,
        header: &fuse_in_header,
        mask: u32,
    ) -> FuseResult<()> {
        let file_uid = self.resolve_file_uid(&status.owner);
        let file_gid = self.resolve_file_gid(&status.group);
        let permission_bits = self.get_effective_permission_bits(
            status.mode,
            header.uid,
            header.gid,
            file_uid,
            file_gid,
        );

        debug!(
            "Access check: file_uid={}, file_gid={}, current_uid={}, current_gid={}, mode={:o}, permission_bits={:o}, mask={:o}",
            file_uid, file_gid, header.uid, header.gid, status.mode, permission_bits, mask
        );

        let has_permission = self.check_permission_mask(permission_bits, mask);
        debug!("Final access result: {}", has_permission);
        if has_permission {
            Ok(())
        } else {
            err_fuse!(
                libc::EACCES,
                "Permission denied to search ino: {}, op: {}",
                header.nodeid,
                header.opcode
            )
        }
    }

    /// Resolve file owner UID from string (supports both numeric and username)
    pub fn resolve_file_uid(&self, owner: &str) -> u32 {
        if owner.is_empty() {
            return self.conf.uid;
        }

        // Try to parse as numeric uid first
        if let Ok(numeric_uid) = owner.parse::<u32>() {
            return numeric_uid;
        }

        // If not numeric, try to lookup by username
        match sys::get_uid_by_name(owner) {
            Some(uid) => uid,
            None => {
                debug!(
                    "Failed to resolve username '{}', using fallback UID {}",
                    owner, self.conf.uid
                );
                self.conf.uid // Fallback to config uid
            }
        }
    }

    /// Resolve file group GID from string (supports both numeric and group name)
    pub fn resolve_file_gid(&self, group: &str) -> u32 {
        if group.is_empty() {
            return self.conf.gid;
        }

        // Try to parse as numeric gid first
        if let Ok(numeric_gid) = group.parse::<u32>() {
            return numeric_gid;
        }

        // If not numeric, try to lookup by group name
        match sys::get_gid_by_name(group) {
            Some(gid) => gid,
            None => {
                debug!(
                    "Failed to resolve group '{}', using fallback GID {}",
                    group, self.conf.gid
                );
                self.conf.gid // Fallback to config gid
            }
        }
    }

    /// Determine which permission bits to check based on user relationship to file
    fn get_effective_permission_bits(
        &self,
        mode: u32,
        current_uid: u32,
        current_gid: u32,
        file_uid: u32,
        file_gid: u32,
    ) -> u32 {
        if current_uid == file_uid {
            // Owner permissions (bits 8-10)
            (mode >> 6) & 0o7
        } else if current_gid == file_gid {
            // Group permissions (bits 5-7)
            (mode >> 3) & 0o7
        } else {
            // Other permissions (bits 2-4)
            mode & 0o7
        }
    }

    /// Check if the permission bits satisfy the requested access mask
    #[allow(unused)]
    fn check_permission_mask(&self, permission_bits: u32, mask: u32) -> bool {
        #[cfg(not(target_os = "linux"))]
        {
            true
        }

        #[cfg(target_os = "linux")]
        {
            // F_OK (0) - only check if file exists, no permission check needed
            if mask == 0 {
                debug!("F_OK only check - always allowed");
                return true;
            }

            let mut has_permission = true;

            // Check read permission (R_OK = 4)
            if (mask & libc::R_OK as u32) != 0 {
                let has_read = (permission_bits & 0o4) != 0;
                has_permission = has_permission && has_read;
                debug!(
                    "Read permission check: requested=true, granted={}",
                    has_read
                );
            }

            // Check write permission (W_OK = 2)
            if (mask & libc::W_OK as u32) != 0 {
                let has_write = (permission_bits & 0o2) != 0;
                has_permission = has_permission && has_write;
                debug!(
                    "Write permission check: requested=true, granted={}",
                    has_write
                );
            }

            // Check execute permission (X_OK = 1)
            if (mask & libc::X_OK as u32) != 0 {
                let has_execute = (permission_bits & 0o1) != 0;
                has_permission = has_permission && has_execute;
                debug!(
                    "Execute permission check: requested=true, granted={}",
                    has_execute
                );
            }

            debug!(
                "Permission mask check: mask={:o}, permission_bits={:o}, result={}",
                mask, permission_bits, has_permission
            );

            has_permission
        }
    }

    fn fuse_setattr_to_opts(setattr: &fuse_setattr_in) -> FuseResult<SetAttrOpts> {
        // Only set fields when the corresponding valid flag is present
        let owner = if (setattr.valid & FATTR_UID) != 0 {
            match orpc::sys::get_username_by_uid(setattr.uid) {
                Some(username) => Some(username),
                None => Some(setattr.uid.to_string()),
            }
        } else {
            None
        };

        let group = if (setattr.valid & FATTR_GID) != 0 {
            match orpc::sys::get_groupname_by_gid(setattr.gid) {
                Some(groupname) => Some(groupname),
                None => Some(setattr.gid.to_string()),
            }
        } else {
            None
        };

        // Strip file type bits; keep only permission and special bits
        let mode = if (setattr.valid & FATTR_MODE) != 0 {
            Some(setattr.mode & 0o7777)
        } else {
            None
        };

        // Handle time modifications
        let mut atime = None;
        let mut mtime = None;

        if (setattr.valid & FATTR_ATIME) != 0 {
            atime = Some((setattr.atime * 1000) as i64);
        } else if (setattr.valid & FATTR_ATIME_NOW) != 0 {
            atime = Some(orpc::common::LocalTime::mills() as i64);
        }

        if (setattr.valid & FATTR_MTIME) != 0 {
            mtime = Some((setattr.mtime * 1000) as i64);
        } else if (setattr.valid & FATTR_MTIME_NOW) != 0 {
            mtime = Some(orpc::common::LocalTime::mills() as i64);
        }

        Ok(SetAttrOpts {
            owner,
            group,
            mode,
            atime,
            mtime,
            ..Default::default()
        })
    }

    async fn fs_resize(
        &self,
        path: &Path,
        ino: u64,
        fh: u64,
        opts: FileAllocOpts,
    ) -> FuseResult<()> {
        opts.validate()?;
        if fh != 0 {
            let handle = self.state.find_handle(ino, fh)?;
            if let Some(writer) = &handle.writer {
                writer.lock().await.resize(opts).await?;
            } else {
                return err_fuse!(libc::EACCES);
            }
        } else {
            self.fs.resize(path, opts).await?;
        };

        self.invalidate_cache(path)?;
        Ok(())
    }

    async fn get_cached_status(&self, path: &Path) -> FuseResult<FileStatus> {
        if self.conf.enable_meta_cache {
            if let Some(status) = self.state.meta_cache().get_status(path) {
                return Ok(status);
            }
        }

        let status = self.fs_get_status(path).await?;
        if self.conf.enable_meta_cache {
            self.state.meta_cache().put_status(path, status.clone());
        }

        Ok(status)
    }

    async fn get_cached_list(&self, path: &Path) -> FuseResult<Vec<FileStatus>> {
        if self.conf.enable_meta_cache {
            if let Some(list) = self.state.meta_cache().get_list(path) {
                return Ok(list);
            }
        }

        let list = self.fs.list_status(path).await?;
        let mut res = Vec::with_capacity(list.len() + 2);
        res.push(CurvineFileSystem::new_dot_status(FUSE_CURRENT_DIR));
        res.push(CurvineFileSystem::new_dot_status(FUSE_PARENT_DIR));
        for status in list {
            res.push(status);
        }

        if self.conf.enable_meta_cache {
            self.state.meta_cache().put_list(path, res.clone());
        }

        Ok(res)
    }
    fn invalidate_cache(&self, path: &Path) -> FuseResult<()> {
        if !self.conf.enable_meta_cache {
            return Ok(());
        }

        self.state.meta_cache().invalidate(path);

        if let Ok(Some(parent)) = path.parent() {
            self.state.meta_cache().invalidate_list(&parent);
        }

        Ok(())
    }
}

impl fs::FileSystem for CurvineFileSystem {
    async fn init(&self, op: Init<'_>) -> FuseResult<fuse_init_out> {
        if op.arg.major < FUSE_KERNEL_VERSION && op.arg.minor < FUSE_KERNEL_MINOR_VERSION {
            return err_fuse!(
                libc::EPROTO,
                "Unsupported FUSE ABI version {}.{}",
                op.arg.major,
                op.arg.minor
            );
        }

        let mut out_flags = FUSE_BIG_WRITES
            | FUSE_ASYNC_READ
            | FUSE_ASYNC_DIO
            | FUSE_SPLICE_MOVE
            | FUSE_SPLICE_WRITE
            | FUSE_SPLICE_READ
            | FUSE_READDIRPLUS_AUTO;

        let max_write = FuseUtils::get_fuse_buf_size() - FUSE_BUFFER_HEADER_SIZE;
        let page_size = sys::get_pagesize()?;
        let max_pages = if op.arg.flags & FUSE_MAX_PAGES != 0 {
            out_flags |= FUSE_MAX_PAGES;
            (max_write - 1) / page_size + 1
        } else {
            0
        };

        out_flags |= op.arg.flags;
        if self.conf.write_back_cache {
            out_flags |= FUSE_WRITEBACK_CACHE;
        } else {
            out_flags &= !FUSE_WRITEBACK_CACHE;
        }

        let out = fuse_init_out {
            major: op.arg.major,
            minor: op.arg.minor,
            max_readahead: op.arg.max_readahead,
            flags: out_flags,
            max_background: self.conf.max_background,
            congestion_threshold: self.conf.congestion_threshold,
            max_write: max_write as u32,
            #[cfg(feature = "fuse3")]
            time_gran: 1,
            #[cfg(feature = "fuse3")]
            max_pages: max_pages as u16,
            #[cfg(feature = "fuse3")]
            padding: 0,
            #[cfg(feature = "fuse3")]
            unused: 0,
        };

        Ok(out)
    }

    // Query inode.
    async fn lookup(&self, op: Lookup<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        let id = op.header.nodeid;

        let (parent, name) = if name == FUSE_CURRENT_DIR {
            (id, None)
        } else if name == FUSE_PARENT_DIR {
            let parent = self.state.get_parent_id(id)?;
            (parent, None)
        } else {
            (id, Some(name.to_string()))
        };

        let parent_path = self.state.get_path(parent)?;
        self.check_permissions(&parent_path, op.header, libc::X_OK as u32)
            .await?;

        // Get the path.
        let path = self.state.get_path_common(parent, name.as_deref())?;
        self.wait_lookup_visibility_barrier(parent, name.as_deref())
            .await;
        let status = self
            .load_lookup_status(parent, name.as_deref(), &path)
            .await?;
        let res = self.lookup_status(parent, name.as_deref(), &status);

        let entry = match res {
            Ok(attr) => {
                let mut entry = Self::create_entry_out(&self.conf, attr);
                let keep_attr = self.state.should_keep_attr(entry.nodeid, &status)?;
                if !keep_attr {
                    entry.entry_valid = 0;
                    entry.attr_valid = 0;
                    entry.entry_valid_nsec = 0;
                    entry.attr_valid_nsec = 0;
                }
                entry
            }

            Err(e) if e.errno == libc::ENOENT && !self.conf.negative_ttl.is_zero() => {
                fuse_entry_out {
                    entry_valid: self.conf.negative_ttl.as_secs(),
                    entry_valid_nsec: self.conf.negative_ttl.subsec_nanos(),
                    ..Default::default()
                }
            }

            Err(e) => return Err(e),
        };

        Ok(entry)
    }

    async fn get_xattr(&self, op: GetXAttr<'_>) -> FuseResult<BytesMut> {
        let name = try_option!(op.name.to_str());

        // Handle system extended attributes FIRST, before any path resolution
        // This avoids unnecessary operations and provides fastest response
        // Kernel may still query these even if FUSE_POSIX_ACL is disabled in init response
        // Kernel requested POSIX_ACL support (kernel_requested_POSIX_ACL: 1048576)
        // but we disabled it in our response, yet kernel still queries ACL attributes
        match name {
            "security.capability"
            | "security.selinux"
            | "system.posix_acl_access"
            | "system.posix_acl_default" => {
                return err_fuse!(libc::ENODATA, "get_xattr {}", name);
            }
            _ => {
                // Continue with normal processing for other attributes
            }
        }

        let path = self.state.get_path(op.header.nodeid)?;
        debug!("Getting xattr: path='{}' name='{}'", path, name);

        let status = self.get_cached_status(&path).await?;

        let mut buf = FuseBuf::default();
        match name {
            "id" => {
                let value = status.id.to_string();
                if op.arg.size == 0 {
                    buf.add_xattr_out(value.len())
                } else {
                    buf.add_slice(value.as_bytes());
                }
            }
            _ => {
                // For other xattr names, try to get from file's xattr
                if let Some(value) = status.x_attr.get(name) {
                    if op.arg.size == 0 {
                        buf.add_xattr_out(value.len())
                    } else if op.arg.size < value.len() as u32 {
                        return err_fuse!(
                            libc::ERANGE,
                            "Buffer too small for xattr value: {} < {}",
                            op.arg.size,
                            value.len()
                        );
                    } else {
                        buf.add_slice(value);
                    }
                } else {
                    return err_fuse!(libc::ENODATA, "No such attribute: {}", name);
                }
            }
        }

        Ok(buf.take())
    }

    // setfattr -n system.posix_acl_access -v "user::rw-,group::r--,other::r--" /curvine-fuse/file
    // Set POSIX ACL attributes for files and directories
    async fn set_xattr(&self, op: SetXAttr<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path(op.header.nodeid)?;

        // Get the xattr value from the request
        let value_slice: &[u8] = op.value;

        debug!(
            "Setting xattr: path='{}' name='{}' value='{}'",
            path,
            name,
            String::from_utf8_lossy(value_slice)
        );

        // Handle system extended attributes - return EOPNOTSUPP for unsupported attributes
        match name {
            "security.capability"
            | "security.selinux"
            | "system.posix_acl_access"
            | "system.posix_acl_default" => {
                return err_fuse!(libc::EOPNOTSUPP, "not support set_xattr {}", name);
            }
            _ => {
                // Continue with normal processing for other attributes
            }
        }

        // Create SetAttrOpts with the xattr to add
        let mut add_x_attr = HashMap::new();
        add_x_attr.insert(name.to_string(), value_slice.to_vec());

        let opts = SetAttrOpts {
            add_x_attr,
            ..Default::default()
        };

        let _ = self.fs_set_attr(&path, opts).await?;
        self.invalidate_cache(&path)?;
        Ok(())
    }

    // setfattr -x system.posix_acl_access /curvine-fuse/file
    // Remove POSIX ACL attributes from files and directories
    async fn remove_xattr(&self, op: RemoveXAttr<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path(op.header.nodeid)?;

        debug!("Removing xattr: path='{}' name='{}'", path, name);

        // Handle system extended attributes silently to avoid ERROR logs
        // Return success for system attributes without forwarding to backend
        match name {
            "security.capability"
            | "security.selinux"
            | "system.posix_acl_access"
            | "system.posix_acl_default" => {
                // Silently ignore system extended attributes removal
                // Return success to avoid ERROR logs
                return Ok(());
            }

            _ => (),
        }

        // Create SetAttrOpts with the xattr to remove
        let opts = SetAttrOpts {
            remove_x_attr: vec![name.to_string()],
            ..Default::default()
        };

        let _ = self.fs_set_attr(&path, opts).await?;
        self.invalidate_cache(&path)?;
        Ok(())
    }

    // listxattr /curvine-fuse/file
    // List all extended attributes for a file or directory
    async fn list_xattr(&self, op: ListXAttr<'_>) -> FuseResult<BytesMut> {
        let path = self.state.get_path(op.header.nodeid)?;
        debug!("Listing xattrs: path='{}' size={}", path, op.arg.size);

        let status = self.get_cached_status(&path).await?;

        // Build the list of xattr names
        let mut xattr_names = Vec::new();

        // Add custom xattr names from the file
        for name in status.x_attr.keys() {
            xattr_names.extend_from_slice(name.as_bytes());
            xattr_names.push(0); // null terminator
        }

        // Add the special "id" attribute
        xattr_names.extend_from_slice(b"id\0");

        let mut buf = FuseBuf::default();

        // If size is 0, just return the total size needed
        if op.arg.size == 0 {
            buf.add_xattr_out(xattr_names.len());
        } else {
            // Check if the provided buffer is large enough
            if op.arg.size < xattr_names.len() as u32 {
                return err_fuse!(
                    libc::ERANGE,
                    "Buffer too small: {} < {}",
                    op.arg.size,
                    xattr_names.len()
                );
            }
            // Return the actual xattr names data
            buf.add_slice(&xattr_names);
        }

        Ok(buf.take())
    }

    async fn get_attr(&self, op: GetAttr<'_>) -> FuseResult<fuse_attr_out> {
        let path = self.state.get_path(op.header.nodeid)?;
        let status = match self.get_cached_status(&path).await {
            Ok(status) => status,
            Err(e) if e.errno == libc::ENOENT => {
                if let Some(status) = self.state.writer_status_snapshot(op.header.nodeid).await {
                    status
                } else if self.namespace_entry_matches_ino(op.header.nodeid, &path) {
                    if let Some(status) = self.state.handle_status_snapshot(op.header.nodeid) {
                        status
                    } else if self.state.is_writer_closing(op.header.nodeid) {
                        self.wait_read_visibility_barrier(op.header.nodeid, &path)
                            .await;
                        self.get_cached_status(&path).await?
                    } else {
                        return Err(e);
                    }
                } else if self.state.is_writer_closing(op.header.nodeid) {
                    self.wait_read_visibility_barrier(op.header.nodeid, &path)
                        .await;
                    self.get_cached_status(&path).await?
                } else {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        };

        let mut fuse_attr = Self::status_to_attr(&self.conf, &status)?;
        fuse_attr.ino = op.header.nodeid;

        let keep_cache = self.state.should_keep_attr(op.header.nodeid, &status)?;
        let (attr_valid, attr_valid_nsec) = if keep_cache {
            (
                self.conf.attr_ttl.as_secs(),
                self.conf.attr_ttl.subsec_nanos(),
            )
        } else {
            (0, 0)
        };

        let attr = fuse_attr_out {
            attr_valid,
            attr_valid_nsec,
            dummy: 0,
            attr: fuse_attr,
        };
        Ok(attr)
    }

    // Modify properties
    //The chown, chmod, and truncate commands will access the interface.
    // @todo is not implemented at this time, and this interface will not cause inode to be familiar with.
    async fn set_attr(&self, op: SetAttr<'_>) -> FuseResult<fuse_attr_out> {
        debug!(
            "Setting attr: path='{}', opts={:?}",
            op.header.nodeid, op.arg
        );
        let path = self.state.get_path(op.header.nodeid)?;

        // Convert setattr to opts with UID/GID numeric fallback
        let mut opts = match Self::fuse_setattr_to_opts(op.arg) {
            Ok(opts) => {
                debug!("Converted setattr opts: {:?}", opts);
                opts
            }
            Err(e) => {
                error!("Failed to convert setattr opts: {}", e);
                return Err(e);
            }
        };

        // Apply chown suid/sgid rules when owner or group changes on regular files.
        // If kernel didn't provide FATTR_MODE, we still need to clear bits accordingly.
        if (op.arg.valid & (FATTR_UID | FATTR_GID)) != 0 {
            // Fetch current status to determine file type and mode
            let cur_status = self.get_cached_status(&path).await?;
            if cur_status.file_type == curvine_common::state::FileType::File {
                let mut new_mode = if let Some(mode) = opts.mode {
                    mode
                } else {
                    cur_status.mode
                };
                // Always clear S_ISUID on chown
                new_mode &= !libc::S_ISUID as u32;
                // Clear S_ISGID when file is group-executable; keep it when not group-executable
                let group_exec = (new_mode & 0o010) != 0;
                if group_exec {
                    new_mode &= !libc::S_ISGID as u32;
                }
                opts.mode = Some(new_mode & 0o7777);
            }
        }

        let mut status = match self.fs_set_attr(&path, opts).await? {
            Some(v) => v,
            None => self.fs_get_status(&path).await?,
        };

        if (op.arg.valid & FATTR_SIZE) != 0 {
            let expect_len = op.arg.size as i64;
            if expect_len != status.len {
                let resize_opts = FileAllocOpts::with_truncate(expect_len);
                self.fs_resize(&path, op.header.nodeid, op.arg.fh, resize_opts)
                    .await?;
                status.len = expect_len;
            }
        }

        self.invalidate_cache(&path)?;
        let mut attr = Self::status_to_attr(&self.conf, &status)?;
        attr.ino = op.header.nodeid;

        let attr = fuse_attr_out {
            attr_valid: self.conf.attr_ttl.as_secs(),
            attr_valid_nsec: self.conf.attr_ttl.subsec_nanos(),
            dummy: 0,
            attr,
        };
        Ok(attr)
    }

    // This interface is not supported at present
    async fn access(&self, op: Access<'_>) -> FuseResult<()> {
        let path = self.state.get_path(op.header.nodeid)?;

        // Check parent directory execute permission for path traversal
        if let Ok(parent_id) = self.state.get_parent_id(op.header.nodeid) {
            // Skip when parent_id is invalid (e.g., root has no parent). Inode 0 is invalid.
            if parent_id != 0 {
                let parent_path = self.state.get_path(parent_id)?;
                self.check_permissions(&parent_path, op.header, libc::X_OK as u32)
                    .await?;
            }
        }

        // Get file status to check permissions
        self.check_permissions(&path, op.header, op.arg.mask)
            .await?;

        Ok(())
    }

    // Open the directory.
    async fn open_dir(&self, op: OpenDir<'_>) -> FuseResult<fuse_open_out> {
        let action = OpenAction::try_from(op.arg.flags)?;

        // Check directory permissions based on open action
        let dir_path = self.state.get_path(op.header.nodeid)?;
        self.check_permissions(&dir_path, op.header, action.acl_mask())
            .await?;

        let list = self.get_cached_list(&dir_path).await?;
        let handle = self.state.new_dir_handle(op.header.nodeid, list).await?;
        let open_flags = Self::fill_open_flags(&self.conf, op.arg.flags);
        let attr = fuse_open_out {
            fh: handle.fh,
            open_flags,
            padding: 0,
        };

        Ok(attr)
    }

    // Get file system profile information.
    async fn stat_fs(&self, _: StatFs<'_>) -> FuseResult<fuse_kstatfs> {
        let info = self.fs.get_master_info().await?;

        let block_size = 4 * ByteUnit::KB as u32;
        let total_blocks = (info.capacity / block_size as i64) as u64;
        let free_blocks = (info.available / block_size as i64) as u64;

        let res = fuse_kstatfs {
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: FUSE_UNKNOWN_INODES,
            ffree: FUSE_UNKNOWN_INODES,
            bsize: block_size,
            namelen: FUSE_MAX_NAME_LENGTH as u32,
            frsize: block_size,
            padding: 0,
            spare: [0; 6],
        };

        Ok(res)
    }

    // Create a directory.
    async fn mkdir(&self, op: MkDir<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let path = self.state.get_path_name(op.header.nodeid, name)?;

        let mut opts = MkdirOptsBuilder::with_conf(&self.fs.conf().client);
        // Apply requested mode and ownership to directory if provided
        if op.arg.mode != 0 {
            opts = opts.acl(
                op.header.uid,
                op.header.gid,
                op.arg.mode & 0o7777 & !op.arg.umask,
            )
        }

        let status = match self.fs.mkdir_with_opts(&path, opts.build()).await {
            Ok(status) => match status {
                Some(v) => v,
                None => self.fs.get_status(&path).await?,
            },

            Err(e) => {
                let e: FuseError = e.into();
                return err_fuse!(e.errno, "mkdir {}: {}", path, e);
            }
        };

        self.invalidate_cache(&path)?;
        let entry = self.lookup_status(op.header.nodeid, Some(name), &status)?;
        Ok(Self::create_entry_out(&self.conf, entry))
    }

    async fn allocate(&self, op: FAllocate<'_>) -> FuseResult<()> {
        let path = self.state.get_path(op.header.nodeid)?;

        let opts = FileAllocOpts {
            truncate: false,
            off: op.arg.offset as i64,
            len: op.arg.length as i64,
            mode: FileAllocMode::from_bits_truncate(op.arg.mode as i32),
        };

        opts.validate()?;
        self.fs_resize(&path, op.header.nodeid, op.arg.fh, opts)
            .await
    }

    // Release the directory, curvine does not need to implement this interface
    async fn release_dir(&self, op: ReleaseDir<'_>) -> FuseResult<()> {
        match self.state.remove_dir_handle(op.header.nodeid, op.arg.fh) {
            Some(_) => (),
            None => return err_fuse!(libc::EBADF),
        };
        Ok(())
    }

    async fn read_dir(&self, op: ReadDir<'_>) -> FuseResult<FuseDirentList> {
        self.read_dir_common(op.header, op.arg, false).await
    }

    async fn read_dir_plus(&self, op: ReadDirPlus<'_>) -> FuseResult<FuseDirentList> {
        self.read_dir_common(op.header, op.arg, true).await
    }

    async fn read(&self, op: Read<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        handle.read(&self.state, op, reply).await
    }

    async fn open(&self, op: Open<'_>) -> FuseResult<fuse_open_out> {
        let path = self.state.get_path(op.header.nodeid)?;
        let action = OpenAction::try_from(op.arg.flags)?;
        let flags = OpenFlags::new(op.arg.flags);
        let (open_snapshot, fallback_create_open) = self
            .resolve_open_snapshot(op.header.nodeid, &path, op.header, action, flags)
            .await?;

        let opts = CreateFileOptsBuilder::with_conf(&self.fs.conf().client);
        let mut open_flags = op.arg.flags;
        if fallback_create_open {
            open_flags |= libc::O_CREAT as u32;
        }
        let handle = match self
            .state
            .new_handle(op.header.nodeid, &path, open_flags, opts.build())
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                warn!(
                    "open failed ino={} path={} req_flags={} effective_flags={} err={}",
                    op.header.nodeid,
                    path,
                    OpenFlags::new(op.arg.flags).access_mark(),
                    OpenFlags::new(open_flags).access_mark(),
                    e
                );
                return Err(e);
            }
        };

        if self.conf.direct_io {
            open_flags |= FUSE_FOPEN_DIRECT_IO;
        } else {
            let keep_status = open_snapshot
                .as_ref()
                .map(OpenSnapshot::status)
                .unwrap_or_else(|| handle.status());
            let keep_cache = self
                .state
                .should_keep_cache(op.header.nodeid, keep_status)?;
            if keep_cache {
                open_flags |= FUSE_FOPEN_KEEP_CACHE;
            } else {
                open_flags |= FUSE_FOPEN_DIRECT_IO;
            }
        }

        let entry = fuse_open_out {
            fh: handle.fh,
            open_flags,
            padding: 0,
        };

        // Invalidate cache for write operations because:
        // 1. O_TRUNC flag may truncate the file immediately, changing file size
        // 2. Overwrite operations may change file metadata (size, mtime) immediately
        // 3. Ensures subsequent read operations get fresh metadata
        if action.write() {
            self.invalidate_cache(&path)?;
        }

        Ok(entry)
    }

    async fn create(&self, op: Create<'_>) -> FuseResult<fuse_create_out> {
        if !FuseUtils::s_isreg(op.arg.mode) {
            return err_fuse!(libc::EINVAL);
        }

        let id = op.header.nodeid;
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        if self.state.is_pending_delete(id) {
            return err_fuse!(libc::ETXTBSY, "file has been deleted or unlinked");
        }

        let path = self.state.get_path_common(id, Some(name))?;
        let node = self.state.find_node(id, Some(name))?;
        let flags = op.arg.flags;

        // create opts
        let mut opts = CreateFileOptsBuilder::with_conf(&self.fs.conf().client);
        // Apply requested mode and ownership to the new file if provided
        if op.arg.mode != 0 {
            opts = opts.acl(
                op.header.uid,
                op.header.gid,
                op.arg.mode & 0o7777 & !op.arg.umask,
            )
        }
        let handle = match self
            .state
            .new_handle(node.id, &path, flags, opts.build())
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                warn!(
                    "create failed parent_ino={} node_ino={} path={} flags={} err={}",
                    id,
                    node.id,
                    path,
                    OpenFlags::new(flags).access_mark(),
                    e
                );
                return Err(e);
            }
        };

        let attr = self.lookup_status(id, Some(name), handle.status())?;
        self.invalidate_cache(&path)?;
        let r = fuse_create_out(
            fuse_entry_out {
                nodeid: handle.ino,
                generation: 0,
                entry_valid: self.conf.entry_ttl.as_secs(),
                attr_valid: self.conf.attr_ttl.as_secs(),
                entry_valid_nsec: self.conf.entry_ttl.subsec_nanos(),
                attr_valid_nsec: self.conf.attr_ttl.subsec_nanos(),
                attr,
            },
            fuse_open_out {
                fh: handle.fh,
                open_flags: op.arg.flags,
                padding: 0,
            },
        );

        Ok(r)
    }

    async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        handle.write(op, reply).await
    }

    async fn flush(&self, op: Flush<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        self.fs_unlock(&handle, LockFlags::Plock).await?;
        handle.flush_for_close(Some(reply)).await?;

        let path = Path::from_str(&handle.status.path)?;
        self.invalidate_cache(&path)?;
        Ok(())
    }

    async fn release(&self, op: Release<'_>, reply: FuseResponse) -> FuseResult<()> {
        let ino = op.header.nodeid;
        let handle = self.state.find_handle(ino, op.arg.fh)?;
        let path = Path::from_str(&handle.status.path)?;

        if let Err(e) = self.fs_unlock(&handle, LockFlags::Flock).await {
            warn!(
                "release ino={} fh={} flock unlock failed: {}",
                ino, op.arg.fh, e
            );
        }
        if let Err(e) = self.fs_unlock(&handle, LockFlags::Plock).await {
            warn!(
                "release ino={} fh={} plock unlock failed: {}",
                ino, op.arg.fh, e
            );
        }

        // Serialize with write-open on the same logical path to avoid
        // close/open races that can terminate writer channels unexpectedly.
        let path_lock = self.state.writer_path_lock(&path);
        let _guard = path_lock.lock().await;

        let mark_writer_closing = handle.has_writer();
        if mark_writer_closing {
            self.state.mark_writer_closing(ino);
        }

        let complete_writer =
            handle.has_writer() && !self.state.has_other_writer_handles(ino, op.arg.fh);
        let complete_result = handle
            .finalize_for_release(complete_writer, Some(reply))
            .await;
        if self.state.remove_handle(ino, op.arg.fh).is_none() {
            if mark_writer_closing {
                self.state.clear_writer_closing(ino);
            }
            return err_fuse!(libc::EBADF);
        }
        if mark_writer_closing {
            self.state.clear_writer_closing(ino);
        }

        let has_open_handles = self.state.has_open_handles(ino);

        if !has_open_handles && self.state.remove_pending_delete(ino) {
            info!(
                "release ino={}: no more open handles, executing delayed deletion of {}",
                ino, path
            );
            if let Err(e) = self.fs.delete(&path, false).await {
                warn!("failed to delete {} after last handle closed: {}", path, e);
            }
        }

        self.invalidate_cache(&path)?;

        complete_result
    }

    async fn forget(&self, op: Forget<'_>) -> FuseResult<()> {
        self.state.forget_node(op.header.nodeid, op.arg.nlookup)
    }

    async fn unlink(&self, op: Unlink<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let parent_ino = op.header.nodeid;

        let path = self.state.get_path_common(parent_ino, Some(name))?;
        if self.state.should_delete_now(parent_ino, Some(name))? {
            self.fs.delete(&path, false).await?;
        }
        self.state.unlink_node(parent_ino, Some(name))?;
        self.invalidate_cache(&path)?;

        Ok(())
    }

    async fn link(&self, op: Link<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        let oldnodeid = op.arg.oldnodeid;

        let des_path = self.state.get_path_common(op.header.nodeid, Some(name))?;
        let src_path = self.state.get_path(oldnodeid)?;

        debug!(
            "link: src_path={}, des_path={}, oldnodeid={}, parent={}",
            src_path, des_path, oldnodeid, op.header.nodeid
        );

        self.fs.link(&src_path, &des_path).await?;
        let src_status = self.get_cached_status(&src_path).await?;
        self.state.find_link_inode(src_status.id, oldnodeid);

        self.invalidate_cache(&des_path)?;
        self.invalidate_cache(&src_path)?;

        let attr = self
            .lookup_path(op.header.nodeid, Some(name), &des_path)
            .await?;

        let result = Self::create_entry_out(&self.conf, attr);
        Ok(result)
    }

    async fn rm_dir(&self, op: RmDir<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path_common(op.header.nodeid, Some(name))?;

        self.fs.delete(&path, false).await?;
        self.state.unlink_node(op.header.nodeid, Some(name))?;

        self.invalidate_cache(&path)?;
        Ok(())
    }

    async fn rename(&self, op: Rename<'_>) -> FuseResult<()> {
        let old_name = try_option!(op.old_name.to_str());
        let new_name = try_option!(op.new_name.to_str());
        if new_name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        if let Some(old_ino) = self.state.lookup_node_id(op.header.nodeid, Some(old_name)) {
            self.state.wait_no_open_writer_handles(old_ino).await;
            self.state.wait_writer_release_barrier(old_ino).await;
        }

        if let Some(dst_ino) = self.state.lookup_node_id(op.arg.newdir, Some(new_name)) {
            self.state.wait_no_open_writer_handles(dst_ino).await;
            self.state.wait_writer_release_barrier(dst_ino).await;
        }

        let (old_path, new_path) =
            self.state
                .get_path2(op.header.nodeid, old_name, op.arg.newdir, new_name)?;

        self.fs.rename(&old_path, &new_path).await?;

        self.state
            .rename_node(op.header.nodeid, old_name, op.arg.newdir, new_name)?;

        self.invalidate_cache(&old_path)?;
        self.invalidate_cache(&new_path)?;

        Ok(())
    }

    async fn batch_forget(&self, op: BatchForget<'_>) -> FuseResult<()> {
        self.state.batch_forget_node(&op.nodes)
    }

    // Create a symbolic link
    async fn symlink(&self, op: Symlink<'_>) -> FuseResult<fuse_entry_out> {
        let linkname = try_option!(op.linkname.to_str());
        let target = try_option!(op.target.to_str());
        let id = op.header.nodeid;
        debug!("symlink: linkname={:?}, target={:?}", linkname, target);

        if linkname.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let (parent, linkname) = if linkname == FUSE_CURRENT_DIR {
            (id, None)
        } else if linkname == FUSE_PARENT_DIR {
            let parent = self.state.get_parent_id(id)?;
            (parent, None)
        } else {
            (id, Some(linkname))
        };

        let link_path = self.state.get_path_common(parent, linkname)?;
        self.fs.symlink(target, &link_path, false).await?;

        self.invalidate_cache(&link_path)?;

        let entry = self.lookup_path(parent, linkname, &link_path).await?;
        Ok(Self::create_entry_out(&self.conf, entry))
    }

    // Read the target of a symbolic link
    async fn readlink(&self, op: Readlink<'_>) -> FuseResult<BytesMut> {
        let path = self.state.get_path(op.header.nodeid)?;

        // Get file status to read the symlink target
        let status = self.get_cached_status(&path).await?;

        // Check if it's actually a symlink
        if status.file_type != curvine_common::state::FileType::Link {
            return err_fuse!(libc::EINVAL, "Not a symbolic link: {}", path);
        }

        // Get the target from the file status
        let curvine_target = match status.target {
            Some(target) => target,
            None => {
                return err_fuse!(libc::ENODATA, "Symbolic link has no target: {}", path);
            }
        };

        // Return the original target path as stored (POSIX standard behavior)
        let os_bytes = FFIUtils::get_os_bytes(&curvine_target);
        let mut result = BytesMut::with_capacity(os_bytes.len() + 1);
        result.extend_from_slice(os_bytes);
        result.extend_from_slice(&[0]);

        Ok(result.split_to(result.len() - 1))
    }

    async fn fsync(&self, op: FSync<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        handle.flush(Some(reply)).await?;

        let path = Path::from_str(&handle.status.path)?;
        self.invalidate_cache(&path)?;
        Ok(())
    }

    /// Create a file system node (mknod system call)
    ///
    /// This function handles the creation of file system nodes:
    /// - For regular files: delegates to `create()` and immediately closes the handle
    /// - For directories: delegates to `mkdir()`
    /// - For other types (devices, fifos, etc.): returns EPERM error
    ///
    /// # Arguments
    /// * `op` - MkNod operation containing:
    ///   - `mode`: file type and permissions
    ///   - `umask`: file creation mask
    ///   - `name`: name of the node to create
    ///
    /// # Returns
    /// * `Ok(fuse_entry_out)` - Entry information for the created node
    /// * `Err(FuseError)` - Error if creation fails or unsupported type
    async fn mk_nod(&self, op: MkNod<'_>) -> FuseResult<fuse_entry_out> {
        if FuseUtils::s_isreg(op.arg.mode) {
            let create_in = fuse_create_in {
                flags: OpenFlags::new_create().value(),
                mode: op.arg.mode,
                umask: op.arg.umask,
                padding: op.arg.padding,
            };
            let op = Create {
                header: op.header,
                arg: &create_in,
                name: op.name,
            };
            let res = self.create(op).await?;
            let handle = self.state.remove_handle(res.0.nodeid, res.1.fh);
            if handle.is_none() {
                return err_fuse!(libc::EBADF);
            }
            let out = fuse_entry_out {
                nodeid: res.0.nodeid,
                generation: res.0.generation,
                entry_valid: res.0.entry_valid,
                attr_valid: res.0.attr_valid,
                entry_valid_nsec: res.0.entry_valid_nsec,
                attr_valid_nsec: res.0.attr_valid_nsec,
                attr: res.0.attr,
            };
            Ok(out)
        } else if FuseUtils::is_dir(op.arg.mode) {
            let mkdir_in = fuse_mkdir_in {
                mode: op.arg.mode,
                umask: op.arg.umask,
            };
            let op = MkDir {
                header: op.header,
                arg: &mkdir_in,
                name: op.name,
            };
            self.mkdir(op).await
        } else {
            err_fuse!(libc::EPERM)
        }
    }

    async fn get_lk(&self, op: GetLk<'_>) -> FuseResult<fuse_lk_out> {
        let path = self.state.get_path(op.header.nodeid)?;
        let lock = self.to_file_lock(op.arg);
        let client_id = lock.client_id.clone();

        let conflict = self.fs.get_lock(&path, lock).await?;
        let lk = match conflict {
            Some(lk) => fuse_file_lock {
                start: lk.start,
                end: lk.end,
                typ: lk.lock_type as u32,
                pid: ternary!(client_id == lk.client_id, lk.pid, 0),
            },

            None => fuse_file_lock {
                typ: LockType::UnLock as u32,
                ..Default::default()
            },
        };

        Ok(fuse_lk_out { lk })
    }

    async fn set_lk(&self, op: SetLk<'_>) -> FuseResult<()> {
        let path = self.state.get_path(op.header.nodeid)?;
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;

        let lock = self.to_file_lock(op.arg);
        let (flag, owner_id) = (lock.lock_flags, lock.owner_id);

        let conflict = self.fs.set_lock(&path, lock).await?;
        if conflict.is_none() {
            handle.add_lock(flag, owner_id);
            Ok(())
        } else {
            err_fuse!(libc::EAGAIN)
        }
    }

    async fn set_lkw(&self, op: SetLkW<'_>) -> FuseResult<()> {
        let path = self.state.get_path(op.header.nodeid)?;
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;

        let conf = &self.fs.conf().client;
        let check_interval_min = conf.sync_check_interval_min;
        let check_interval_max = conf.sync_check_interval_max;
        let log_ticks = conf.sync_check_log_tick;

        let mut ticks = 0;
        let time = TimeSpent::new();

        let lock = self.to_file_lock(op.arg);
        loop {
            let conflict = self.fs.set_lock(&path, lock.clone()).await?;
            if conflict.is_none() {
                handle.add_lock(lock.lock_flags, lock.owner_id);
                return Ok(());
            }

            ticks += 1;
            let sleep_time = check_interval_max.min(check_interval_min * ticks);
            tokio::time::sleep(sleep_time).await;

            if ticks % log_ticks == 0 {
                info!("waiting lock for {}, elapsed: {} ms", path, time.used_ms());
            }
        }
    }

    async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        self.state.persist(writer).await
    }

    async fn restore(&self, reader: &mut StateReader) -> FuseResult<()> {
        self.state.restore(reader).await
    }
}
