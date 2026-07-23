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

use crate::fs::dcache::CleanerTask;
use crate::fs::operator::*;
use crate::fs::plock_wait_registry::{LockOwner, PlockWaitGuard, PlockWaitRegistry};
use crate::fs::state::{FileHandle, NodeState};
use crate::fuse_metrics::{
    ReaddirTimer, INVAL_REASON_FLUSH, INVAL_REASON_FSYNC, INVAL_REASON_RELEASE, INVAL_REASON_RESIZE,
};
use crate::raw::fuse_abi::*;
use crate::raw::FuseDirentList;
use crate::session::{FuseBuf, FuseResponse};
use crate::*;
use crate::{err_fuse, FuseResult, FuseUtils};
use bytes::BytesMut;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::{ClusterConf, FuseConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, RpcCode, StateReader, StateWriter};
use curvine_common::state::{
    is_special_file_type, FileAllocMode, FileAllocOpts, FileLock, FileStatus, FileType, LockFlags,
    LockType, OpenFlags, SetAttrOpts,
};
use curvine_common::MAX_FILE_SIZE;
use log::{debug, info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::runtime::Runtime;
use orpc::sys::FFIUtils;
use orpc::{sys, try_option};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::Arc;

pub struct CurvineFileSystem {
    fs: UnifiedFileSystem,
    state: Arc<NodeState>,
    conf: FuseConf,
    plock_waits: Arc<PlockWaitRegistry>,
}

impl CurvineFileSystem {
    pub fn new(conf: ClusterConf, rt: Arc<Runtime>) -> FuseResult<Self> {
        FuseMetrics::ensure_init()?;

        let fuse_conf = conf.fuse.clone();
        let fs = UnifiedFileSystem::with_rt(conf, rt)?;
        let state = Arc::new(NodeState::new(fs.clone())?);

        CleanerTask::start(fuse_conf.node_cache_ttl.as_millis() as u64, state.clone())?;

        let fuse_fs = Self {
            fs,
            state,
            conf: fuse_conf,
            plock_waits: PlockWaitRegistry::new(),
        };

        Ok(fuse_fs)
    }

    pub fn conf(&self) -> &FuseConf {
        &self.conf
    }

    fn normalize_fallocate(
        current_len: i64,
        offset: u64,
        length: u64,
        raw_mode: u32,
    ) -> FuseResult<Option<FileAllocOpts>> {
        if length == 0 {
            return err_fuse!(libc::EINVAL, "fallocate length must be greater than zero");
        }

        let mode = match FileAllocMode::from_bits(raw_mode as i32) {
            Some(mode) => mode,
            None => {
                return err_fuse!(
                    libc::EOPNOTSUPP,
                    "unsupported fallocate mode: {:#x}",
                    raw_mode
                )
            }
        };
        // The backend resize API only accepts a target file length and cannot
        // preserve the byte range required by ZERO_RANGE.
        let allowed = FileAllocMode::DEFAULT | FileAllocMode::KEEP_SIZE;
        if !(mode & !allowed).is_empty() {
            return err_fuse!(
                libc::EOPNOTSUPP,
                "unsupported fallocate mode: {:#x}",
                raw_mode
            );
        }

        let offset = match i64::try_from(offset) {
            Ok(offset) => offset,
            Err(_) => return err_fuse!(libc::EFBIG, "fallocate offset exceeds supported size"),
        };
        let length = match i64::try_from(length) {
            Ok(length) => length,
            Err(_) => return err_fuse!(libc::EFBIG, "fallocate length exceeds supported size"),
        };
        let end = match offset.checked_add(length) {
            Some(end) => end,
            None => return err_fuse!(libc::EFBIG, "fallocate range overflows"),
        };
        if end > MAX_FILE_SIZE {
            return err_fuse!(
                libc::EFBIG,
                "fallocate end {} exceeds maximum file size {}",
                end,
                MAX_FILE_SIZE
            );
        }

        // Curvine allocates storage lazily across the cluster, so KEEP_SIZE has
        // no logical metadata change to apply. Writes into the range still
        // allocate blocks normally without exposing a larger st_size early.
        if mode.contains(FileAllocMode::KEEP_SIZE) {
            return Ok(None);
        }

        let target_len = current_len.max(end);
        if target_len == current_len {
            return Ok(None);
        }

        Ok(Some(FileAllocOpts {
            truncate: false,
            off: 0,
            len: target_len,
            mode,
        }))
    }

    pub(crate) fn fs(&self) -> &UnifiedFileSystem {
        &self.fs
    }

    fn validate_set_xattr_flags(flags: u32, exists: bool) -> FuseResult<()> {
        let create = libc::XATTR_CREATE as u32;
        let replace = libc::XATTR_REPLACE as u32;
        let known = create | replace;

        if flags & !known != 0 || flags & known == known {
            return err_fuse!(libc::EINVAL, "Invalid setxattr flags: {:#x}", flags);
        }
        if flags & create != 0 && exists {
            return err_fuse!(libc::EEXIST, "Extended attribute already exists");
        }
        if flags & replace != 0 && !exists {
            return err_fuse!(libc::ENODATA, "Extended attribute does not exist");
        }

        Ok(())
    }

    async fn ensure_writable_path(&self, path: &Path, rpc_code: RpcCode) -> FuseResult<()> {
        if self.conf.readonly {
            return Err(FsError::read_only(path.full_path()).into());
        }

        if let Some((_, mnt)) = self.fs.get_mount(path, RpcCode::FileStatus).await? {
            if mnt.info.is_read_only_cache_mode() {
                return Err(FsError::read_only(format!(
                    "{} on read_only cache_mode mount {}",
                    rpc_code, path
                ))
                .into());
            }
        }
        Ok(())
    }

    pub fn state(&self) -> &Arc<NodeState> {
        &self.state
    }

    fn retain_first_error(first: &mut FuseResult<()>, next: FuseResult<()>) {
        if first.is_ok() {
            *first = next;
        }
    }

    /// Shared rename path resolution used by both `rename` and `rename2`.
    async fn rename_paths(
        &self,
        old_id: u64,
        old_name: &OsStr,
        new_dir: u64,
        new_name: &OsStr,
    ) -> FuseResult<()> {
        let old_name = try_option!(old_name.to_str());
        let new_name = try_option!(new_name.to_str());
        if new_name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let (old_path, new_path) = self.state.get_path2(old_id, old_name, new_dir, new_name)?;
        self.ensure_writable_path(&old_path, RpcCode::Rename)
            .await?;
        self.ensure_writable_path(&new_path, RpcCode::Rename)
            .await?;

        self.state
            .fs_rename(old_id, old_name, new_dir, new_name)
            .await
    }

    /// Whether a RENAME2 `flags` value is supported. Checked against the RAW
    /// value (not `RenameFlags::from_bits_truncate`, which would silently drop
    /// unknown high bits and let a flag the kernel required pass as a plain
    /// rename). The FUSE-facing client only issues flag-less renames, so any
    /// non-zero flag (NO_REPLACE/EXCHANGE/WHITEOUT/…) is rejected with ENOSYS.
    fn rename2_flags_supported(flags: u32) -> bool {
        flags == 0
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
            if let Err(e) = self.fs_unlock_owner(handler, flags, owner_id).await {
                // Preserve the owner locally when the backend unlock fails so a
                // retained handle can retry the cleanup.
                handler.add_lock(flags, owner_id);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn fs_unlock_owner(
        &self,
        handler: &FileHandle,
        flags: LockFlags,
        owner_id: u64,
    ) -> FuseResult<()> {
        let client_id = self.fs.cv().fs_context().clone_client_name();
        let path = Path::from_str(&handler.status().path)?;

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

        if let Err(e) = self.fs.set_lock(&path, lock).await {
            return Err(e.into());
        }

        Ok(())
    }

    /// Emit `negative_entry_returned_total`, gated on the `metrics_enabled`
    /// observation kill-switch. No-op when metrics are disabled.
    fn record_negative_entry(&self) {
        if self.conf.metrics_enabled {
            FuseMetrics::with(|m| m.record_negative_entry());
        }
    }

    async fn read_dir_common(
        &self,
        header: &fuse_in_header,
        arg: &fuse_read_in,
        plus: bool,
    ) -> FuseResult<FuseDirentList> {
        // Time the whole batch. A `ReaddirTimer` guard records
        // `readdir_duration_us{status=error}` on drop unless `success(n)` is called
        // on the Ok path (which records duration + `readdir_entries`). An early `?`
        // return or async cancellation between awaits is therefore counted as an
        // error with no `entries` observation. Gated on the `metrics_enabled`
        // kill-switch: disabled => `None`, no timing at all.
        let timer = ReaddirTimer::start(self.conf.metrics_enabled);
        let (res, entries) = self.read_dir_common_inner(header, arg, plus).await?;
        if let Some(timer) = timer {
            timer.success(entries);
        }
        Ok(res)
    }

    /// The readdir resume cookie for the entry at zero-based directory position
    /// `index`: the offset the kernel must pass on the next readdir to continue
    /// AFTER this entry, i.e. the position of the FOLLOWING entry (`index + 1`).
    ///
    /// This is deliberately a tiny named function so the +1 is covered by a
    /// regression test: encoding `index` instead makes the last entry of a batch
    /// carry a cookie equal to the batch's start offset, so the kernel re-requests
    /// the same offset forever and readdir never terminates (issue #1116).
    fn readdir_next_cookie(index: u64) -> u64 {
        index + 1
    }

    /// Returns the encoded dirent list plus the number of entries successfully
    /// added this batch (`index - arg.offset`), which is what `readdir_entries`
    /// observes — NOT `FuseDirentList`'s byte length and NOT the directory total.
    async fn read_dir_common_inner(
        &self,
        header: &fuse_in_header,
        arg: &fuse_read_in,
        plus: bool,
    ) -> FuseResult<(FuseDirentList, u64)> {
        let handle = self.state.find_dir_handle(header.nodeid, arg.fh)?;

        let mut res = FuseDirentList::new(arg);
        let mut index = arg.offset;
        let mut batch = handle.get_batch(arg.offset as usize).await?;
        {
            let mut dir = self.state.dir_write();
            while let Some(status) = batch.pop_front() {
                let attr = if status.name != FUSE_CURRENT_DIR && status.name != FUSE_PARENT_DIR {
                    let inode = dir.lookup(header.nodeid, &status.name, status.clone())?;
                    let attr = FuseUtils::status_to_attr(&self.conf, &inode.status)?;
                    // readdir materializes the child into the dcache; count it as a
                    // status-cache put (mirrors the pre-refactor read_dir_common).
                    self.state.record_status_put();
                    attr
                } else {
                    FuseUtils::status_to_attr(&self.conf, &status)?
                };

                let entry = FuseUtils::create_entry_out(&self.conf, attr);
                // dirent `off` is the resume cookie = position of the NEXT entry.
                // See `readdir_next_cookie` (issue #1116 infinite-loop guard).
                let next_off = Self::readdir_next_cookie(index);
                if !res.add_dirent(plus, next_off, &status, entry) {
                    batch.push_front(status);
                    break;
                }
                index += 1;
            }
        }
        handle.set_buf(batch).await?;

        let entries = index.saturating_sub(arg.offset);
        Ok((res, entries))
    }

    /// Whether access(2) must enforce mode bits for the caller.
    ///
    /// Linux lets root bypass R_OK/W_OK checks, but still validates X_OK against
    /// the file mode (see access(2)). Other FUSE ops keep the broader root bypass
    /// in `check_permissions`.
    fn posix_access_requires_mode_check(uid: u32, mask: u32) -> bool {
        uid != 0 || (mask & libc::X_OK as u32) != 0
    }

    async fn check_permissions(&self, header: &fuse_in_header, mask: u32) -> FuseResult<()> {
        if header.uid == 0 || !self.conf.check_permission {
            return Ok(());
        }
        let status = self.state.fs_stat(header.nodeid, None).await?;
        self.check_access_permissions(&status, header, mask)
    }

    /// Linux root access(2) bypasses R_OK/W_OK but still validates X_OK against any
    /// execute bit in the file mode, not the caller's owner/group/other class.
    fn check_root_access_permissions(status: &FileStatus) -> FuseResult<()> {
        if (status.mode & 0o111) != 0 {
            Ok(())
        } else {
            err_fuse!(
                libc::EACCES,
                "Permission denied: root X_OK requires any execute bit in mode {:o}",
                status.mode
            )
        }
    }

    /// POSIX `stat(2)` requires execute (search) permission on every directory in
    /// the path prefix. FUSE getattr is inode-based, so enforce that by walking the
    /// dcache parent chain (nftw FTW_NS when a parent directory is not searchable).
    async fn check_traverse_permissions(
        &self,
        ino: u64,
        header: &fuse_in_header,
    ) -> FuseResult<()> {
        if header.uid == 0 || !self.conf.check_permission {
            return Ok(());
        }

        let mut dir_ino = {
            let dir = self.state.dir_read();
            match dir.get_inode(ino, None) {
                None => {
                    return err_fuse!(
                        libc::EACCES,
                        "Permission denied: cannot verify traverse permission for uncached ino {}",
                        ino
                    );
                }
                Some(inode) if inode.is_root() => return Ok(()),
                Some(inode) => inode.parent,
            }
        };

        while dir_ino != 0 {
            let check_header = fuse_in_header {
                uid: header.uid,
                gid: header.gid,
                nodeid: dir_ino,
                ..Default::default()
            };
            let cached_status = {
                let dir = self.state.dir_read();
                dir.get_inode(dir_ino, None)
                    .map(|inode| inode.clone_status())
            };
            if let Some(status) = cached_status {
                self.check_access_permissions(&status, &check_header, libc::X_OK as u32)?;
            } else {
                self.check_permissions(&check_header, libc::X_OK as u32)
                    .await?;
            }

            let is_root = {
                let dir = self.state.dir_read();
                dir.get_inode_check(dir_ino, None)?.is_root()
            };
            if is_root {
                break;
            }
            dir_ino = self.state.get_parent_ino(dir_ino)?;
        }

        Ok(())
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

        let has_permission = Self::permission_mask_allows(permission_bits, mask);
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
    fn permission_mask_allows(permission_bits: u32, mask: u32) -> bool {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (permission_bits, mask);
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

    #[allow(unused)]
    fn check_permission_mask(&self, permission_bits: u32, mask: u32) -> bool {
        Self::permission_mask_allows(permission_bits, mask)
    }

    fn check_setattr_permission(
        check_permission: bool,
        caller_uid: u32,
        caller_gid: u32,
        file_uid: u32,
        valid: u32,
        target_gid: Option<u32>,
    ) -> FuseResult<()> {
        if !check_permission || caller_uid == 0 {
            return Ok(());
        }

        if (valid & FATTR_UID) != 0 {
            return err_fuse!(
                libc::EPERM,
                "setattr uid change requires privilege for uid {}",
                caller_uid
            );
        }

        if (valid & FATTR_GID) != 0 {
            if caller_uid != file_uid {
                return err_fuse!(
                    libc::EPERM,
                    "setattr gid change denied for non-owner uid {}",
                    caller_uid
                );
            }
            if let Some(gid) = target_gid {
                if !FuseUtils::caller_in_file_group(caller_gid, gid) {
                    return err_fuse!(
                        libc::EPERM,
                        "setattr gid change denied for gid {} not in caller groups",
                        gid
                    );
                }
            }
        }

        if (valid & FATTR_MODE) != 0 && caller_uid != file_uid {
            return err_fuse!(
                libc::EPERM,
                "setattr mode change denied for non-owner uid {}",
                caller_uid
            );
        }

        Ok(())
    }

    /// Negotiate init reply flags as an explicit allowlist, not a blind echo:
    /// 1. Kernel-negotiated: `SUPPORTED_INIT_FLAGS & kernel_flags` (only caps the
    ///    daemon implements AND the kernel offered — see `SUPPORTED_INIT_FLAGS` for
    ///    what that deliberately excludes).
    /// 2. Config-gated, forced on regardless of the kernel offer:
    ///    `FUSE_WRITEBACK_CACHE` (when `write_back_cache`) and `FUSE_SPLICE_*` (when
    ///    `enable_splice` — splice drives the fuse fd directly, so it is advertised
    ///    on config alone).
    fn negotiate_out_flags(kernel_flags: u32, write_back_cache: bool, enable_splice: bool) -> u32 {
        let mut out = SUPPORTED_INIT_FLAGS & kernel_flags;
        if write_back_cache {
            out |= FUSE_WRITEBACK_CACHE;
        }
        if enable_splice {
            out |= FUSE_SPLICE_MOVE | FUSE_SPLICE_WRITE | FUSE_SPLICE_READ;
        }
        out
    }

    /// Whether the kernel's advertised FUSE ABI version is at least the
    /// daemon's minimum.
    ///
    /// Compared as a `(major, minor)` tuple against `FUSE_MIN_ABI`: reject any
    /// version below 7.31 (this correctly rejects a low-major/high-minor combo
    /// such as 6.40, which the old `major < 7 && minor < 31` check let through).
    /// A higher major also compares as supported here; `init` handles that case
    /// separately with a version-only reply (see `version_only_init_out`) before
    /// this predicate gates the too-old case.
    fn abi_supported(major: u32, minor: u32) -> bool {
        (major, minor) >= FUSE_MIN_ABI
    }

    /// The INIT reply for a kernel whose major ABI is newer than the daemon's:
    /// advertise only the daemon's own `(major, minor)` with no negotiated
    /// flags, and leave every other field zeroed.
    ///
    /// Per the fuse.h version-negotiation convention, when the kernel major is
    /// larger than the daemon's, userspace replies with its own major and skips
    /// negotiation, and the kernel is expected to re-issue INIT at a matching
    /// major. This mirrors libfuse `_do_init`'s `arg->major > 7` early return:
    /// a `fuse_init_in` at an unknown higher major may not share our field
    /// layout, so we must not interpret its flags. The kernel does not read the
    /// other reply fields on a major mismatch, so they stay 0.
    fn version_only_init_out() -> fuse_init_out {
        fuse_init_out {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            ..Default::default()
        }
    }
}

impl fs::FileSystem for CurvineFileSystem {
    async fn init(&self, op: Init<'_>) -> FuseResult<fuse_init_out> {
        if !Self::abi_supported(op.arg.major, op.arg.minor) {
            return err_fuse!(
                libc::EPROTO,
                "unsupported FUSE ABI {}.{}: curvine requires >= {}.{}",
                op.arg.major,
                op.arg.minor,
                FUSE_KERNEL_VERSION,
                FUSE_KERNEL_MINOR_VERSION
            );
        }

        // Kernel major newer than ours: reply with our own version only and do
        // NOT negotiate flags — the `fuse_init_in` layout at an unknown higher
        // major cannot be trusted (see `version_only_init_out`). Only when the
        // majors already match do we run the allowlist negotiation below.
        if op.arg.major > FUSE_KERNEL_VERSION {
            info!(
                "FUSE init: kernel offered abi={}.{} (major > {}); replying version-only \
                 {}.{} and skipping capability negotiation",
                op.arg.major,
                op.arg.minor,
                FUSE_KERNEL_VERSION,
                FUSE_KERNEL_VERSION,
                FUSE_KERNEL_MINOR_VERSION,
            );
            return Ok(Self::version_only_init_out());
        }

        // Negotiate an explicit allowlist (see `negotiate_out_flags`): only caps
        // the daemon implements AND the kernel offered, plus config-gated
        // writeback/splice. `max_pages` then keys off the negotiated result.
        let out_flags = Self::negotiate_out_flags(
            op.arg.flags,
            self.conf.write_back_cache,
            self.conf.enable_splice,
        );

        let max_write = FuseUtils::get_fuse_buf_size() - FUSE_BUFFER_HEADER_SIZE;
        let page_size = sys::get_pagesize()?;
        let max_pages = if out_flags & FUSE_MAX_PAGES != 0 {
            (max_write - 1) / page_size + 1
        } else {
            0
        };

        // If `fuse.max_readahead_kb` is configured, raise the negotiated
        // `max_readahead` so the kernel cap (min(bdi.max_readahead_kb,
        // fuse_conn.max_readahead)) does not silently shrink reads.
        let max_readahead = match self.conf.max_readahead_kb {
            Some(kb) => op.arg.max_readahead.max(kb.saturating_mul(1024)),
            None => op.arg.max_readahead,
        };

        let out = fuse_init_out {
            // Advertise the daemon's own ABI, not the kernel's: curvine only
            // implements the 7.31 struct/semantics, so it must not claim a higher
            // version. The higher-major case is already handled above; here the
            // majors match, and we reply with our own minor so we never claim a
            // minor the kernel does not have.
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            max_readahead,
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

        // Log the negotiated capability set: what we advertise vs. what the
        // kernel offered but we did not enable (so an operator can see why a
        // capability is inactive). Report BOTH the version we advertise on the
        // wire (`negotiated_abi`, always our own) and what the kernel offered
        // (`kernel_offered_abi`) — the two differ in minor when the kernel is
        // newer (e.g. offered 7.40, connection runs at 7.31), so a single "abi="
        // field would misreport the live version. Note SPLICE/WRITEBACK may
        // appear in "enabled" without being kernel-offered — they are
        // config-gated daemon requests.
        let dropped = op.arg.flags & !out_flags;
        info!(
            "FUSE init negotiated: negotiated_abi={}.{} kernel_offered_abi={}.{} \
             enabled=[{}] kernel_offered_not_enabled=[{}]",
            FUSE_KERNEL_VERSION,
            FUSE_KERNEL_MINOR_VERSION,
            op.arg.major,
            op.arg.minor,
            fuse_init_flag_names(out_flags).join(", "),
            fuse_init_flag_names(dropped).join(", "),
        );

        Ok(out)
    }

    // Query inode.
    async fn lookup(&self, op: Lookup<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        self.check_permissions(op.header, libc::X_OK as u32).await?;

        let negative_entry = || fuse_entry_out {
            entry_valid: self.conf.negative_ttl.as_secs(),
            entry_valid_nsec: self.conf.negative_ttl.subsec_nanos(),
            ..Default::default()
        };

        let res = self.state.fs_lookup(op.header.nodeid, name).await;
        let entry = match res {
            Ok(mut attr) => {
                self.state.update_writer_len(&mut attr).await;
                FuseUtils::create_entry_out(&self.conf, attr)
            }

            Err(e) if e.errno == libc::ENOENT && !self.conf.negative_ttl.is_zero() => {
                self.record_negative_entry();
                negative_entry()
            }

            Err(e) => return Err(e),
        };

        Ok(entry)
    }

    async fn get_xattr(&self, op: GetXAttr<'_>) -> FuseResult<BytesMut> {
        let name = try_option!(op.name.to_str());
        FuseUtils::check_xattr(name, XattrOp::Get)?;

        let status = self.state.fs_stat(op.header.nodeid, None).await?;
        FuseUtils::check_user_xattr_namespace(status.file_type, name)?;
        let mut buf = FuseBuf::default();
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

        Ok(buf.take())
    }

    async fn ioctl(&self, op: Ioctl<'_>) -> FuseResult<BytesMut> {
        let path = self.state.get_path(op.header.nodeid)?;
        let mut status = self.state.fs_stat(op.header.nodeid, None).await?;
        let flag_bytes = FuseUtils::ioctl_flag_bytes() as u32;
        let (result, out_flags) = match op.arg.cmd {
            FuseUtils::FS_IOC_GETFLAGS => {
                if op.arg.out_size < flag_bytes {
                    return err_fuse!(libc::EINVAL, "ioctl out buffer too small");
                }
                (0, FuseUtils::file_flags_from_status(&status))
            }
            FuseUtils::FS_IOC_SETFLAGS => {
                self.ensure_writable_path(&path, RpcCode::SetAttr).await?;
                if op.arg.in_size < flag_bytes {
                    return err_fuse!(libc::EINVAL, "ioctl in buffer too small");
                }
                let requested = FuseUtils::decode_ioctl_file_flags(op.in_data)?;
                let new_flags = FuseUtils::normalize_ioctl_file_flags(requested);
                let opts = FuseUtils::set_attr_for_file_flags(&status, new_flags);
                status = self.state.fs_set_attr(op.header.nodeid, opts).await?;
                (0, FuseUtils::file_flags_from_status(&status))
            }
            _ => return err_fuse!(libc::ENOTTY, "unsupported ioctl cmd {:#x}", op.arg.cmd),
        };

        let out = fuse_ioctl_out {
            result,
            flags: 0,
            in_iovs: 0,
            out_iovs: 0,
        };
        let mut buf = BytesMut::from(FuseUtils::struct_as_bytes(&out));
        FuseUtils::append_ioctl_file_flags(&mut buf, out_flags, op.arg.out_size);
        Ok(buf)
    }

    async fn set_xattr(&self, op: SetXAttr<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        FuseUtils::check_xattr(name, XattrOp::Set)?;
        let path = self.state.get_path(op.header.nodeid)?;
        self.ensure_writable_path(&path, RpcCode::SetAttr).await?;

        // Serialize the existence check and update within this FUSE process so
        // concurrent CREATE/REPLACE requests cannot both validate stale state.
        let _guard = self.state.lock_path(&path).await;
        let status = self.state.fs_stat(op.header.nodeid, None).await?;
        FuseUtils::check_user_xattr_namespace(status.file_type, name)?;
        if FuseUtils::file_has_immutable_or_append(&status) {
            return err_fuse!(
                libc::EPERM,
                "xattr modification not permitted on immutable/append-only file"
            );
        }
        Self::validate_set_xattr_flags(op.arg.flags, status.x_attr.contains_key(name))?;

        // Get the xattr value from the request
        let value_slice: &[u8] = op.value;

        // Create SetAttrOpts with the xattr to add
        let mut add_x_attr = HashMap::new();
        add_x_attr.insert(name.to_string(), value_slice.to_vec());

        let opts = SetAttrOpts {
            add_x_attr,
            ..Default::default()
        };

        let _ = self.state.fs_set_attr(op.header.nodeid, opts).await?;
        Ok(())
    }

    async fn remove_xattr(&self, op: RemoveXAttr<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        FuseUtils::check_xattr(name, XattrOp::Remove)?;

        let path = self.state.get_path(op.header.nodeid)?;
        self.ensure_writable_path(&path, RpcCode::SetAttr).await?;

        // Share the same path lock as set_xattr so a conditional REPLACE does
        // not validate existence immediately before a concurrent removal.
        let _guard = self.state.lock_path(&path).await;
        let status = self.state.fs_stat(op.header.nodeid, None).await?;
        FuseUtils::check_user_xattr_namespace(status.file_type, name)?;
        if FuseUtils::file_has_immutable_or_append(&status) {
            return err_fuse!(
                libc::EPERM,
                "xattr modification not permitted on immutable/append-only file"
            );
        }
        if !status.x_attr.contains_key(name) {
            return err_fuse!(libc::ENODATA, "No such attribute: {}", name);
        }

        debug!("Removing xattr: path='{}' name='{}'", path, name);

        let opts = SetAttrOpts {
            remove_x_attr: vec![name.to_string()],
            ..Default::default()
        };
        let _ = self.state.fs_set_attr(op.header.nodeid, opts).await?;

        Ok(())
    }

    async fn list_xattr(&self, op: ListXAttr<'_>) -> FuseResult<BytesMut> {
        let status = self.state.fs_stat(op.header.nodeid, None).await?;

        // Build the list of xattr names
        let mut xattr_names = Vec::new();

        // Add custom xattr names from the file
        for name in status.x_attr.keys() {
            // Hidden/protected attributes must not be advertised when getxattr
            // deliberately makes them unreadable.
            if FuseUtils::check_xattr(name, XattrOp::Get).is_err() {
                continue;
            }
            xattr_names.extend_from_slice(name.as_bytes());
            xattr_names.push(0); // null terminator
        }

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
        self.check_traverse_permissions(op.header.nodeid, op.header)
            .await?;

        let status = self.state.fs_stat(op.header.nodeid, None).await?;

        let mut fuse_attr = FuseUtils::status_to_attr(&self.conf, &status)?;
        fuse_attr.ino = op.header.nodeid;
        self.state.update_writer_len(&mut fuse_attr).await;
        let attr = fuse_attr_out {
            attr_valid: self.conf.attr_ttl.as_secs(),
            attr_valid_nsec: self.conf.attr_ttl.subsec_nanos(),
            dummy: 0,
            attr: fuse_attr,
        };

        Ok(attr)
    }

    // Modify properties
    //The chown, chmod, and truncate commands will access the interface.
    // @todo is not implemented at this time, and this interface will not cause inode to be familiar with.
    async fn set_attr(&self, op: SetAttr<'_>) -> FuseResult<fuse_attr_out> {
        let path = self.state.get_path(op.header.nodeid)?;
        self.ensure_writable_path(&path, RpcCode::SetAttr).await?;

        self.check_traverse_permissions(op.header.nodeid, op.header)
            .await?;

        let cur_status = self.state.fs_stat(op.header.nodeid, None).await?;
        let file_uid = self.resolve_file_uid(&cur_status.owner);
        let file_gid = self.resolve_file_gid(&cur_status.group);
        let target_gid = if (op.arg.valid & FATTR_GID) != 0 {
            Some(op.arg.gid)
        } else {
            None
        };
        Self::check_setattr_permission(
            self.conf.check_permission,
            op.header.uid,
            op.header.gid,
            file_uid,
            op.arg.valid,
            target_gid,
        )?;

        // Convert setattr to opts with UID/GID numeric fallback
        let mut opts = FuseUtils::fuse_setattr_to_opts(op.arg)?;

        if (op.arg.valid & FATTR_MODE) != 0 {
            if let Some(mode) = opts.mode {
                let in_file_group = FuseUtils::caller_in_file_group(op.header.gid, file_gid);
                opts.mode = Some(FuseUtils::normalize_chmod_mode(
                    mode,
                    op.header.uid,
                    in_file_group,
                ));
            }
        }

        // Apply chown suid/sgid rules when owner or group changes on regular files.
        // If kernel didn't provide FATTR_MODE, we still need to clear bits accordingly.
        if (op.arg.valid & (FATTR_UID | FATTR_GID)) != 0 && cur_status.file_type == FileType::File {
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

        let mut status = self.state.fs_set_attr(op.header.nodeid, opts).await?;
        if (op.arg.valid & FATTR_SIZE) != 0 {
            let expect_len = op.arg.size as i64;
            if expect_len != status.len {
                let resize_opts = FileAllocOpts::with_truncate(expect_len);
                self.state
                    .fs_resize(op.header.nodeid, op.arg.fh, resize_opts)
                    .await?;
                status.len = expect_len;
                self.state
                    .invalid_cache(op.header.nodeid, None, INVAL_REASON_RESIZE);
            }
        }

        let mut attr = FuseUtils::status_to_attr(&self.conf, &status)?;
        attr.ino = op.header.nodeid;
        // Metadata-only setattr (for example fchmod after write) may race ahead of
        // the writer's final metadata commit. Never let its stale size shrink the
        // kernel inode below the bytes already accepted by the active writer.
        self.state.update_writer_len(&mut attr).await;
        let attr = fuse_attr_out {
            attr_valid: self.conf.attr_ttl.as_secs(),
            attr_valid_nsec: self.conf.attr_ttl.subsec_nanos(),
            dummy: 0,
            attr,
        };
        Ok(attr)
    }

    async fn access(&self, op: Access<'_>) -> FuseResult<()> {
        if !self.conf.check_permission {
            return Ok(());
        }
        if !Self::posix_access_requires_mode_check(op.header.uid, op.arg.mask) {
            return Ok(());
        }
        let status = self.state.fs_stat(op.header.nodeid, None).await?;
        if op.header.uid == 0 {
            Self::check_root_access_permissions(&status)
        } else {
            self.check_access_permissions(&status, op.header, op.arg.mask)
        }
    }

    // Open the directory.
    async fn open_dir(&self, op: OpenDir<'_>) -> FuseResult<fuse_open_out> {
        let action = OpenAction::try_from(op.arg.flags)?;

        // Check directory permissions based on open action
        let dir_path = self.state.get_path(op.header.nodeid)?;
        self.check_permissions(op.header, action.acl_mask()).await?;

        let handle = self
            .state
            .new_dir_handle(op.header.nodeid, &dir_path)
            .await?;
        let open_flags = FuseUtils::dir_open_flags(&self.conf);
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
        let ino = op.header.nodeid;
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let path = self.state.get_path_name(op.header.nodeid, name)?;
        self.ensure_writable_path(&path, RpcCode::Mkdir).await?;

        let opts = FuseUtils::mkdir_opts(&op, &self.fs);
        let attr = self.state.fs_mkdir(ino, name, opts).await?;
        Ok(FuseUtils::create_entry_out(&self.conf, attr))
    }

    async fn allocate(&self, op: FAllocate<'_>) -> FuseResult<()> {
        let path = self.state.get_path(op.header.nodeid)?;
        self.ensure_writable_path(&path, RpcCode::ResizeFile)
            .await?;

        let status = self.state.fs_stat(op.header.nodeid, None).await?;
        let writer_len = self
            .state
            .get_writer_len(op.header.nodeid)
            .await
            .map(|len| len as i64)
            .unwrap_or(status.len);
        let current_len = status.len.max(writer_len);
        let Some(opts) =
            Self::normalize_fallocate(current_len, op.arg.offset, op.arg.length, op.arg.mode)?
        else {
            return Ok(());
        };

        self.state
            .fs_resize(op.header.nodeid, op.arg.fh, opts)
            .await?;
        self.state
            .invalid_cache(op.header.nodeid, None, INVAL_REASON_RESIZE);
        Ok(())
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
        if is_special_file_type(handle.status().file_type) {
            return err_fuse!(
                libc::EOPNOTSUPP,
                "read not supported for special file nodes"
            );
        }
        handle.read(&self.state, op, reply).await
    }

    async fn open(&self, op: Open<'_>) -> FuseResult<fuse_open_out> {
        let action = OpenAction::try_from(op.arg.flags)?;
        let path = self.state.get_path(op.header.nodeid)?;
        let status = self.state.fs_stat(op.header.nodeid, None).await?;
        if is_special_file_type(status.file_type) {
            let truncate = (op.arg.flags & libc::O_TRUNC as u32) != 0;
            if action.write() || truncate {
                return err_fuse!(libc::EACCES, "special file nodes are read-only metadata");
            }
            self.check_permissions(op.header, action.acl_mask()).await?;
            let ino = op.header.nodeid;
            let handle = self.state.new_meta_handle(ino, status).await?;
            let open_flags = FuseUtils::file_open_flags(&self.conf, false);
            return Ok(fuse_open_out {
                fh: handle.fh(),
                open_flags,
                padding: 0,
            });
        }
        let truncate = (op.arg.flags & libc::O_TRUNC as u32) != 0;
        if action.write() || truncate {
            self.ensure_writable_path(&path, RpcCode::CreateFile)
                .await?;
        }
        self.check_permissions(op.header, action.acl_mask()).await?;

        let ino = op.header.nodeid;
        let opts = FuseUtils::open_opts(&self.fs);

        let handle = self.state.fs_open(ino, op.arg.flags, opts).await?;

        let keep_cache = if self.conf.direct_io {
            false
        } else {
            // Page cache consistency is handled here, not via explicit inode
            // invalidation, because: (1) FUSE_NOTIFY_INVAL_INODE can deadlock inside
            // send_inode_out on some older kernels; (2) on open the kernel issues a
            // fresh getattr and auto-invalidates the page cache if mtime/size changed
            // (CAP_AUTO_INVAL_DATA, on by default since Linux 2.6.35), so no notify is
            // needed. Note: with enable_meta_cache, keep_cache may return true after a
            // remote modification (stale reads) — an intentional consistency/perf trade.
            self.state.keep_cache(ino, &handle.status())
        };
        let open_flags = FuseUtils::file_open_flags(&self.conf, keep_cache);

        let entry = fuse_open_out {
            fh: handle.fh(),
            open_flags,
            padding: 0,
        };

        Ok(entry)
    }

    async fn create(&self, op: Create<'_>) -> FuseResult<fuse_create_out> {
        if !FuseUtils::s_isreg(op.arg.mode) {
            return err_fuse!(libc::EIO);
        }

        let ino = op.header.nodeid;
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let path = self.state.get_path_common(ino, Some(name))?;
        self.ensure_writable_path(&path, RpcCode::CreateFile)
            .await?;

        let mut opts = FuseUtils::create_opts(&op, &self.fs);
        let parent_status = self.state.fs_stat(ino, None).await?;
        FuseUtils::apply_setgid_parent_group(&mut opts, &parent_status);

        let handle = self.state.fs_create(ino, name, op.arg.flags, opts).await?;
        let attr = FuseUtils::status_to_attr(&self.conf, &handle.status())?;

        if attr.ino != handle.ino() {
            return err_fuse!(
                libc::EIO,
                "ino mismatch after create: dcache returned ino={} but handle has ino={}",
                attr.ino,
                handle.ino()
            );
        }

        let r = fuse_create_out(
            fuse_entry_out {
                nodeid: handle.ino(),
                generation: 0,
                entry_valid: self.conf.entry_ttl.as_secs(),
                attr_valid: self.conf.attr_ttl.as_secs(),
                entry_valid_nsec: self.conf.entry_ttl.subsec_nanos(),
                attr_valid_nsec: self.conf.attr_ttl.subsec_nanos(),
                attr,
            },
            fuse_open_out {
                fh: handle.fh(),
                open_flags: FuseUtils::file_open_flags(&self.conf, true),
                padding: 0,
            },
        );

        Ok(r)
    }

    async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        if is_special_file_type(handle.status().file_type) {
            return err_fuse!(
                libc::EOPNOTSUPP,
                "write not supported for special file nodes"
            );
        }
        handle.write(op, reply).await
    }

    async fn flush(&self, op: Flush<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        if handle.has_writer() {
            self.state
                .invalid_cache(op.header.nodeid, None, INVAL_REASON_FLUSH);
        }

        if op.arg.lock_owner != 0 {
            self.fs_unlock_owner(&handle, LockFlags::Plock, op.arg.lock_owner)
                .await?;
            handle.take_plock_if_owner(op.arg.lock_owner);
        }
        handle.flush(Some(reply)).await
    }

    async fn release(&self, op: Release<'_>, reply: FuseResponse) -> FuseResult<()> {
        let ino = op.header.nodeid;
        let path = self.state.get_path(ino)?;
        let _guard = self.state.lock_path(&path).await;

        let (handle, mut release_result) = self.state.release_handle(ino, op.arg.fh).await?;

        if handle.has_writer() {
            self.state
                .invalid_cache(op.header.nodeid, None, INVAL_REASON_RELEASE);
        }

        let flock_result = self.fs_unlock(&handle, LockFlags::Flock).await;
        if let Err(e) = &flock_result {
            warn!(
                "failed to release flock for ino={}, path={}: {}",
                ino, path, e
            );
        }
        Self::retain_first_error(&mut release_result, flock_result);

        let plock_result = self.fs_unlock(&handle, LockFlags::Plock).await;
        if let Err(e) = &plock_result {
            warn!(
                "failed to release plock for ino={}, path={}: {}",
                ino, path, e
            );
        }
        Self::retain_first_error(&mut release_result, plock_result);

        match self.state.deferred_delete_ready(ino).await {
            Ok(true) => {
                debug!(
                    "release ino={}: no more open handles, executing delayed deletion of {}",
                    ino, path
                );
                let delete_result = self
                    .state
                    .complete_deferred_delete(ino, self.fs.delete(&path, false).await);
                if let Err(e) = &delete_result {
                    warn!(
                        "failed to delete {} after last handle closed; retaining pending delete: {}",
                        path, e
                    );
                }
                Self::retain_first_error(&mut release_result, delete_result);
            }
            Ok(false) => {}
            Err(e) => {
                warn!(
                    "failed to evaluate deferred delete for ino={}, path={}: {}",
                    ino, path, e
                );
                Self::retain_first_error(&mut release_result, Err(e));
            }
        }

        reply.send_rep(release_result).await?;
        Ok(())
    }

    async fn forget(&self, op: Forget<'_>) -> FuseResult<()> {
        self.state.forget(op.header.nodeid, op.arg.nlookup)
    }

    async fn unlink(&self, op: Unlink<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path_common(op.header.nodeid, Some(name))?;
        self.ensure_writable_path(&path, RpcCode::Delete).await?;
        self.state.fs_unlink(op.header.nodeid, name).await?;
        Ok(())
    }

    async fn link(&self, op: Link<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        let oldnodeid = op.arg.oldnodeid;

        self.state.fs_fsync(oldnodeid, None).await?;

        let des_path = self.state.get_path_common(op.header.nodeid, Some(name))?;
        let src_path = self.state.get_path(oldnodeid)?;
        self.ensure_writable_path(&src_path, RpcCode::Link).await?;
        self.ensure_writable_path(&des_path, RpcCode::Link).await?;

        debug!(
            "link: src_path={}, des_path={}, oldnodeid={}, parent={}",
            src_path, des_path, oldnodeid, op.header.nodeid
        );

        self.fs.link(&src_path, &des_path).await?;
        let attr = self
            .state
            .lookup_link(op.header.nodeid, name, oldnodeid)
            .await?;

        let result = FuseUtils::create_entry_out(&self.conf, attr);
        Ok(result)
    }

    async fn rm_dir(&self, op: RmDir<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path_common(op.header.nodeid, Some(name))?;
        self.ensure_writable_path(&path, RpcCode::Delete).await?;

        self.fs.delete(&path, false).await?;
        self.state.unlink(op.header.nodeid, name, false)?;

        Ok(())
    }

    async fn rename(&self, op: Rename<'_>) -> FuseResult<()> {
        self.rename_paths(op.header.nodeid, op.old_name, op.arg.newdir, op.new_name)
            .await
    }

    async fn rename2(&self, op: Rename2<'_>) -> FuseResult<()> {
        // The FUSE-facing client rename path only issues flag-less renames, so
        // NO_REPLACE/EXCHANGE/WHITEOUT are not plumbed through to the master RPC.
        // Reject any flag with ENOSYS (POSIX-correct, and strictly better than
        // the previous "all RENAME2 -> ENOSYS via the dispatch wildcard").
        if !Self::rename2_flags_supported(op.arg.flags) {
            return err_fuse!(
                libc::ENOSYS,
                "RENAME2 flags 0x{:x} not supported (flag-less rename only)",
                op.arg.flags
            );
        }
        self.rename_paths(op.header.nodeid, op.old_name, op.arg.newdir, op.new_name)
            .await
    }

    async fn batch_forget(&self, op: BatchForget<'_>) -> FuseResult<()> {
        self.state.batch_forget(&op.nodes)
    }

    // Create a symbolic link
    async fn symlink(&self, op: Symlink<'_>) -> FuseResult<fuse_entry_out> {
        let linkname = try_option!(op.linkname.to_str());
        let target = try_option!(op.target.to_str());
        let id = op.header.nodeid;

        if linkname.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        if FuseUtils.is_dot(linkname) {
            return err_fuse!(libc::EIO, "not support name {}", linkname);
        }

        let link_path = self.state.get_path_common(id, Some(linkname))?;
        self.ensure_writable_path(&link_path, RpcCode::Symlink)
            .await?;
        let owner = sys::get_username_by_uid(op.header.uid).unwrap_or(op.header.uid.to_string());
        let group = sys::get_groupname_by_gid(op.header.gid).unwrap_or(op.header.gid.to_string());
        self.fs
            .symlink_with_owner_group(target, &link_path, false, Some(owner), Some(group))
            .await?;

        let attr = self.state.lookup_common(id, linkname).await?;
        Ok(FuseUtils::create_entry_out(&self.conf, attr))
    }

    // Read the target of a symbolic link
    async fn readlink(&self, op: Readlink<'_>) -> FuseResult<BytesMut> {
        // Get file status to read the symlink target
        let status = self.state.fs_stat(op.header.nodeid, None).await?;

        // Check if it's actually a symlink
        if status.file_type != FileType::Link {
            return err_fuse!(libc::EINVAL, "Not a symbolic link: {}", status.path);
        }

        // Get the target from the file status
        let curvine_target = match status.target {
            Some(target) => target,
            None => {
                return err_fuse!(
                    libc::ENODATA,
                    "Symbolic link has no target: {}",
                    status.path
                );
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
        self.state.fs_fsync(op.header.nodeid, None).await?;

        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;
        if handle.has_writer() {
            let path = self.state.get_path(op.header.nodeid)?;
            self.ensure_writable_path(&path, RpcCode::CreateFile)
                .await?;
        }
        handle.flush(Some(reply)).await?;

        if handle.has_writer() {
            self.state
                .invalid_cache(op.header.nodeid, None, INVAL_REASON_FSYNC);
        }

        Ok(())
    }

    /// Create a filesystem node (`mknod`):
    /// - regular file: delegates to `create()` then closes the handle;
    /// - directory: delegates to `mkdir()`;
    /// - char/block/fifo/socket: creates a metadata-only special node;
    /// - other types: returns EPERM.
    async fn mk_nod(&self, op: MkNod<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

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
            let (_, close_result) = self.state.release_handle(res.0.nodeid, res.1.fh).await?;
            close_result?;
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
        } else if let Some(file_type) = FuseUtils::special_file_type_from_mode(op.arg.mode) {
            let path = self.state.get_path_name(op.header.nodeid, name)?;
            self.ensure_writable_path(&path, RpcCode::CreateFile)
                .await?;
            let mut opts = FuseUtils::mknod_opts(&op, &self.fs, file_type);
            let parent_status = self.state.fs_stat(op.header.nodeid, None).await?;
            FuseUtils::apply_setgid_parent_group(&mut opts, &parent_status);
            self.fs.create_special_node(&path, opts).await?;
            let attr = self.state.lookup_common(op.header.nodeid, name).await?;
            Ok(FuseUtils::create_entry_out(&self.conf, attr))
        } else {
            err_fuse!(libc::EPERM)
        }
    }

    async fn get_lk(&self, op: GetLk<'_>) -> FuseResult<fuse_lk_out> {
        let path = self.state.get_path(op.header.nodeid)?;
        let lock = self.to_file_lock(op.arg);

        self.state.fs_fsync(op.header.nodeid, None).await?;

        let conflict = self.fs.get_lock(&path, lock).await?;
        let lk = match conflict {
            Some(lk) => fuse_file_lock {
                start: lk.start,
                end: lk.end,
                typ: lk.lock_type as u32,
                pid: lk.pid,
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
        self.ensure_writable_path(&path, RpcCode::SetLock).await?;
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;

        self.state.fs_fsync(op.header.nodeid, None).await?;

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
        self.ensure_writable_path(&path, RpcCode::SetLock).await?;
        let handle = self.state.find_handle(op.header.nodeid, op.arg.fh)?;

        self.state.fs_fsync(op.header.nodeid, None).await?;

        let conf = &self.fs.conf().client;
        let check_interval_min_ms = conf.sync_check_interval_min_ms;
        let check_interval_max_ms = conf.sync_check_interval_max_ms;
        let log_ticks = conf.sync_check_log_tick;

        let mut ticks: u64 = 0;
        let time = TimeSpent::new();

        // NOTE: `setlkw_wait_duration_us` is NOT observed here. Its RAII timer is
        // created one layer up, in `FuseReceiver::dispatch_meta_interrupt` before
        // the `select!`, so that an interrupt arriving before this poll loop ever
        // runs still records a wait sample. Observing it here would miss exactly
        // that immediate-cancellation case. See the comment there.

        let lock = self.to_file_lock(op.arg);
        let wait_guard = PlockWaitGuard::new(
            self.plock_waits.clone(),
            LockOwner::new(lock.client_id.clone(), lock.owner_id),
        );
        loop {
            wait_guard.clear_blocked_by();

            let conflict = self.fs.set_lock(&path, lock.clone()).await?;
            if conflict.is_none() {
                handle.add_lock(lock.lock_flags, lock.owner_id);
                return Ok(());
            }

            let blocker = conflict.as_ref().expect("conflict lock");
            if wait_guard
                .register_blocked_by(LockOwner::new(blocker.client_id.clone(), blocker.owner_id))
            {
                return err_fuse!(libc::EDEADLK);
            }

            ticks += 1;
            let sleep_ms = check_interval_max_ms.min(check_interval_min_ms.saturating_mul(ticks));
            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;

            if ticks.is_multiple_of(log_ticks as u64) {
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

#[cfg(test)]
mod tests {
    use crate::{FATTR_GID, FATTR_MODE, FATTR_MTIME, FATTR_UID};
    use curvine_common::state::FileAllocMode;

    #[test]
    fn posix_access_requires_mode_check_for_root() {
        use super::CurvineFileSystem;

        assert!(!CurvineFileSystem::posix_access_requires_mode_check(
            0,
            libc::R_OK as u32
        ));
        assert!(!CurvineFileSystem::posix_access_requires_mode_check(
            0,
            libc::W_OK as u32
        ));
        assert!(!CurvineFileSystem::posix_access_requires_mode_check(0, 0));
        assert!(CurvineFileSystem::posix_access_requires_mode_check(
            0,
            libc::X_OK as u32
        ));
        assert!(CurvineFileSystem::posix_access_requires_mode_check(
            0,
            (libc::W_OK | libc::X_OK) as u32
        ));
        assert!(CurvineFileSystem::posix_access_requires_mode_check(
            1000,
            libc::R_OK as u32
        ));
    }

    #[test]
    fn root_access_checks_any_execute_bit_not_owner_class() {
        use super::CurvineFileSystem;
        use curvine_common::state::{FileStatus, FileType};

        let mut readonly = FileStatus::with_name(1, "readonly".to_string(), false);
        readonly.file_type = FileType::File;
        readonly.mode = 0o100400;
        assert!(CurvineFileSystem::check_root_access_permissions(&readonly).is_err());

        let mut other_execute = FileStatus::with_name(2, "other-x".to_string(), false);
        other_execute.file_type = FileType::File;
        other_execute.mode = 0o100001;
        assert!(CurvineFileSystem::check_root_access_permissions(&other_execute).is_ok());

        let mut group_execute = FileStatus::with_name(3, "group-x".to_string(), false);
        group_execute.file_type = FileType::File;
        group_execute.mode = 0o100010;
        assert!(CurvineFileSystem::check_root_access_permissions(&group_execute).is_ok());
    }

    #[test]
    fn access01_mode_masks_deny_root_x_ok_on_non_executable_files() {
        use super::CurvineFileSystem;

        let readonly_owner = (0o100400u32 >> 6) & 0o7;
        let writeonly_owner = (0o100200u32 >> 6) & 0o7;

        assert!(!CurvineFileSystem::permission_mask_allows(
            readonly_owner,
            libc::X_OK as u32
        ));
        assert!(!CurvineFileSystem::permission_mask_allows(
            writeonly_owner,
            libc::X_OK as u32
        ));
        assert!(!CurvineFileSystem::permission_mask_allows(
            readonly_owner,
            (libc::W_OK | libc::X_OK) as u32
        ));
        assert!(CurvineFileSystem::permission_mask_allows(
            readonly_owner,
            libc::R_OK as u32
        ));
        assert!(CurvineFileSystem::permission_mask_allows(
            writeonly_owner,
            libc::W_OK as u32
        ));
    }

    #[test]
    fn fallocate_default_converts_range_to_target_length() {
        let opts = super::CurvineFileSystem::normalize_fallocate(4096, 4096, 4096, 0)
            .unwrap()
            .unwrap();
        assert_eq!(opts.off, 0);
        assert_eq!(opts.len, 8192);
        assert_eq!(opts.mode, FileAllocMode::DEFAULT);
    }

    #[test]
    fn fallocate_inside_file_does_not_shrink_it() {
        let opts = super::CurvineFileSystem::normalize_fallocate(8192, 1024, 4096, 0).unwrap();
        assert!(opts.is_none());
    }

    #[test]
    fn fallocate_keep_size_preserves_logical_length() {
        let opts = super::CurvineFileSystem::normalize_fallocate(
            4096,
            4096,
            4096,
            FileAllocMode::KEEP_SIZE.bits() as u32,
        )
        .unwrap();
        assert!(opts.is_none());
    }

    #[test]
    fn fallocate_rejects_invalid_ranges_and_modes() {
        let zero_len = super::CurvineFileSystem::normalize_fallocate(0, 0, 0, 0).unwrap_err();
        assert_eq!(zero_len.errno, libc::EINVAL);

        let too_large =
            super::CurvineFileSystem::normalize_fallocate(0, u64::MAX, 1, 0).unwrap_err();
        assert_eq!(too_large.errno, libc::EFBIG);

        let unsupported =
            super::CurvineFileSystem::normalize_fallocate(0, 0, 4096, 0x02).unwrap_err();
        assert_eq!(unsupported.errno, libc::EOPNOTSUPP);

        let zero_range = super::CurvineFileSystem::normalize_fallocate(
            8192,
            4096,
            4096,
            FileAllocMode::ZERO_RANGE.bits() as u32,
        )
        .unwrap_err();
        assert_eq!(zero_range.errno, libc::EOPNOTSUPP);
    }

    #[test]
    fn setattr_permission_denies_non_owner_chown() {
        let err = super::CurvineFileSystem::check_setattr_permission(
            true, 1000, 1000, 0, FATTR_UID, None,
        )
        .unwrap_err();
        assert_eq!(err.errno(), libc::EPERM);
    }

    #[test]
    fn setattr_permission_denies_owner_uid_change() {
        let err = super::CurvineFileSystem::check_setattr_permission(
            true, 1000, 1000, 1000, FATTR_UID, None,
        )
        .unwrap_err();
        assert_eq!(err.errno(), libc::EPERM);
    }

    #[test]
    fn setattr_permission_allows_root_chown() {
        super::CurvineFileSystem::check_setattr_permission(true, 0, 0, 1000, FATTR_UID, None)
            .unwrap();
    }

    #[test]
    fn setattr_permission_allows_owner_mode_change() {
        super::CurvineFileSystem::check_setattr_permission(
            true, 1000, 1000, 1000, FATTR_MODE, None,
        )
        .unwrap();
    }

    #[test]
    fn setattr_permission_denies_owner_gid_to_foreign_group() {
        let err = super::CurvineFileSystem::check_setattr_permission(
            true,
            1000,
            100,
            1000,
            FATTR_GID,
            Some(200),
        )
        .unwrap_err();
        assert_eq!(err.errno(), libc::EPERM);
    }

    #[test]
    fn setattr_permission_allows_owner_gid_to_own_group() {
        super::CurvineFileSystem::check_setattr_permission(
            true,
            1000,
            100,
            1000,
            FATTR_GID,
            Some(100),
        )
        .unwrap();
    }

    #[test]
    fn setattr_permission_ignores_mtime_only_changes() {
        super::CurvineFileSystem::check_setattr_permission(true, 1000, 1000, 0, FATTR_MTIME, None)
            .unwrap();
    }

    /// Pin that `FuseMetrics::ensure_init()` runs before `NodeState::new()` in
    /// `CurvineFileSystem::new`: the event-driven gauges are silently no-op'd by
    /// `with()` (drifting permanently low, no panic) if a mutation fires before
    /// init. A behavioral check is useless (the process-global `OnceCell` is
    /// already init by other tests in this binary), so we assert on the source
    /// text — scoped to the body of `fn new`, so this test's own doc/assert
    /// literals don't satisfy `find()` and mask a real removal.
    #[test]
    fn ensure_init_precedes_node_state() {
        let src = include_str!("curvine_file_system.rs");
        let body_start = src
            .find("pub fn new(conf: ClusterConf")
            .expect("CurvineFileSystem::new signature not found");
        // First method after `new`; bounds the body so later occurrences (incl.
        // this test's own text) are excluded.
        let body_end = body_start
            + src[body_start..]
                .find("pub fn state(")
                .expect("CurvineFileSystem::state not found after new()");
        let body = &src[body_start..body_end];

        let init_at = body
            .find("FuseMetrics::ensure_init()")
            .expect("CurvineFileSystem::new must call FuseMetrics::ensure_init()");
        let node_state_at = body
            .find("NodeState::new(")
            .expect("CurvineFileSystem::new must construct NodeState::new(..)");
        assert!(
            init_at < node_state_at,
            "FuseMetrics::ensure_init() must precede NodeState::new() in \
             CurvineFileSystem::new so the legacy gauges' event-driven updates \
             (FuseMetrics::with) land on an initialized singleton"
        );
    }

    use super::CurvineFileSystem;
    use crate::{
        fuse_init_flag_names, FUSE_ATOMIC_O_TRUNC, FUSE_BIG_WRITES, FUSE_DO_READDIRPLUS,
        FUSE_EXPORT_SUPPORT, FUSE_FLOCK_LOCKS, FUSE_HAS_IOCTL_DIR, FUSE_KERNEL_MINOR_VERSION,
        FUSE_KERNEL_VERSION, FUSE_MAX_PAGES, FUSE_POSIX_ACL, FUSE_POSIX_LOCKS, FUSE_SPLICE_MOVE,
        FUSE_SPLICE_READ, FUSE_SPLICE_WRITE, FUSE_WRITEBACK_CACHE, SUPPORTED_INIT_FLAGS,
    };

    #[test]
    fn set_xattr_flags_enforce_create_and_replace_semantics() {
        let create = libc::XATTR_CREATE as u32;
        let replace = libc::XATTR_REPLACE as u32;

        CurvineFileSystem::validate_set_xattr_flags(0, false).unwrap();
        CurvineFileSystem::validate_set_xattr_flags(0, true).unwrap();
        CurvineFileSystem::validate_set_xattr_flags(create, false).unwrap();
        CurvineFileSystem::validate_set_xattr_flags(replace, true).unwrap();

        assert_eq!(
            CurvineFileSystem::validate_set_xattr_flags(create, true)
                .unwrap_err()
                .errno(),
            libc::EEXIST
        );
        assert_eq!(
            CurvineFileSystem::validate_set_xattr_flags(replace, false)
                .unwrap_err()
                .errno(),
            libc::ENODATA
        );
        assert_eq!(
            CurvineFileSystem::validate_set_xattr_flags(create | replace, false)
                .unwrap_err()
                .errno(),
            libc::EINVAL
        );
        assert_eq!(
            CurvineFileSystem::validate_set_xattr_flags(1 << 31, false)
                .unwrap_err()
                .errno(),
            libc::EINVAL
        );
    }

    // #1122 + allowlist: the daemon must never advertise FUSE_ATOMIC_O_TRUNC
    // (open does not truncate) or any other unsupported capability, even when
    // the kernel offers it. The allowlist mask drops them.
    //
    // FUSE_EXPORT_SUPPORT is included here (dropped even when offered): its `.`/`..`
    // reconstruction relies on root `.`/`..` lookups that currently return ENOENT,
    // so the daemon must not advertise it. Not advertising it leaves the kernel's
    // `fc->export_support` unset, so the kernel never issues the root `.`/`..` LOOKUP.
    #[test]
    fn negotiate_out_flags_drops_unsupported_kernel_caps() {
        let unsupported = FUSE_ATOMIC_O_TRUNC
            | FUSE_POSIX_ACL
            | FUSE_HAS_IOCTL_DIR
            | FUSE_EXPORT_SUPPORT
            | (1u32 << 30);
        let out = CurvineFileSystem::negotiate_out_flags(unsupported, false, false);
        assert_eq!(
            out, 0,
            "no unsupported kernel-offered bit may be advertised"
        );
    }

    // EXPORT_SUPPORT must NOT be in the allowlist: advertising it turns on the
    // kernel's `.`/`..` handle-reconstruction path, which the daemon cannot serve
    // (root `.`/`..` lookups return ENOENT). This pins that a future edit cannot
    // silently re-add it. Paired with `negotiate_out_flags_drops_unsupported_kernel_caps`
    // (proves it is dropped even when the kernel offers it).
    #[test]
    fn export_support_not_in_allowlist() {
        assert_eq!(
            SUPPORTED_INIT_FLAGS & FUSE_EXPORT_SUPPORT,
            0,
            "FUSE_EXPORT_SUPPORT must not be advertised until root `.`/`..` lookup works"
        );
    }

    // Supported caps pass through when the kernel offers them. Guards against
    // regressing distributed locking / readdirplus, which were only enabled by
    // the old blind OR.
    #[test]
    fn negotiate_out_flags_passes_through_supported_caps() {
        let out = CurvineFileSystem::negotiate_out_flags(SUPPORTED_INIT_FLAGS, false, false);
        assert_eq!(
            out, SUPPORTED_INIT_FLAGS,
            "all supported+offered caps survive"
        );
        // Explicit regression guards for the previously-implicit caps.
        assert_eq!(out & FUSE_POSIX_LOCKS, FUSE_POSIX_LOCKS);
        assert_eq!(out & FUSE_FLOCK_LOCKS, FUSE_FLOCK_LOCKS);
        assert_eq!(out & FUSE_DO_READDIRPLUS, FUSE_DO_READDIRPLUS);
    }

    // A supported cap the kernel did NOT offer must not be advertised (no
    // phantom capabilities).
    #[test]
    fn negotiate_out_flags_no_phantom_when_kernel_offers_nothing() {
        let out = CurvineFileSystem::negotiate_out_flags(0, false, false);
        assert_eq!(out, 0);
        assert_eq!(out & FUSE_MAX_PAGES, 0);
        assert_eq!(out & FUSE_POSIX_LOCKS, 0);
    }

    // WRITEBACK is a config-gated daemon-requested cap: present iff write_back,
    // absent otherwise even when the kernel offers it.
    #[test]
    fn negotiate_out_flags_writeback_is_config_gated() {
        let on = CurvineFileSystem::negotiate_out_flags(0, true, false);
        assert_eq!(on & FUSE_WRITEBACK_CACHE, FUSE_WRITEBACK_CACHE);
        let off = CurvineFileSystem::negotiate_out_flags(FUSE_WRITEBACK_CACHE, false, false);
        assert_eq!(off & FUSE_WRITEBACK_CACHE, 0);
    }

    // SPLICE is config-gated and forced (not masked by the kernel offer, since
    // the channel drives splice(2) directly).
    #[test]
    fn negotiate_out_flags_splice_is_config_gated() {
        let splice = FUSE_SPLICE_MOVE | FUSE_SPLICE_WRITE | FUSE_SPLICE_READ;
        let on = CurvineFileSystem::negotiate_out_flags(0, false, true);
        assert_eq!(
            on & splice,
            splice,
            "splice advertised on config even if kernel omits it"
        );
        let off = CurvineFileSystem::negotiate_out_flags(splice, false, false);
        assert_eq!(off & splice, 0, "splice not advertised when disabled");
    }

    // Containment: output never contains a bit outside the allowed universe,
    // regardless of what the kernel offers.
    #[test]
    fn negotiate_out_flags_containment() {
        let splice = FUSE_SPLICE_MOVE | FUSE_SPLICE_WRITE | FUSE_SPLICE_READ;
        let allowed = SUPPORTED_INIT_FLAGS | splice | FUSE_WRITEBACK_CACHE;
        let out = CurvineFileSystem::negotiate_out_flags(0xFFFF_FFFF, true, true);
        assert_eq!(out & !allowed, 0, "no bit outside the allowed universe");
    }

    #[test]
    fn abi_supported_tuple_comparison() {
        // Accept >= 7.31, including a higher major.
        assert!(CurvineFileSystem::abi_supported(7, 31));
        assert!(CurvineFileSystem::abi_supported(7, 32));
        assert!(CurvineFileSystem::abi_supported(8, 0));
        // Reject anything below 7.31 — including the low-major/high-minor combo
        // the old `major < 7 && minor < 31` check let through.
        assert!(!CurvineFileSystem::abi_supported(7, 30));
        assert!(!CurvineFileSystem::abi_supported(6, 40));
        assert!(!CurvineFileSystem::abi_supported(6, 0));
        assert!(!CurvineFileSystem::abi_supported(0, 0));
    }

    // Higher-major short reply (mirrors libfuse `_do_init`'s `arg->major > 7`
    // path): advertise only our own version, negotiate no flags, zero the rest.
    #[test]
    fn version_only_init_out_advertises_own_version_no_flags() {
        let out = CurvineFileSystem::version_only_init_out();
        assert_eq!(out.major, FUSE_KERNEL_VERSION);
        assert_eq!(out.minor, FUSE_KERNEL_MINOR_VERSION);
        assert_eq!(
            out.flags, 0,
            "no capability negotiation on a major mismatch"
        );
        // Fields the kernel does not read on a major mismatch stay zeroed.
        assert_eq!(out.max_readahead, 0);
        assert_eq!(out.max_write, 0);
        assert_eq!(out.max_background, 0);
        assert_eq!(out.congestion_threshold, 0);
    }

    #[test]
    fn atomic_o_trunc_bit_value_matches_uapi() {
        // uapi fuse.h: FUSE_ATOMIC_O_TRUNC = (1 << 3)
        assert_eq!(FUSE_ATOMIC_O_TRUNC, 1 << 3);
    }

    #[test]
    fn fuse_init_flag_names_maps_known_and_unknown() {
        // Known bits render as names.
        let names = fuse_init_flag_names(FUSE_POSIX_LOCKS | FUSE_DO_READDIRPLUS);
        assert!(names.contains(&"POSIX_LOCKS".to_string()));
        assert!(names.contains(&"DO_READDIRPLUS".to_string()));
        // Empty flags => empty list.
        assert!(fuse_init_flag_names(0).is_empty());
        // An unknown bit is surfaced as a hex token, not silently dropped.
        let unknown = 1u32 << 30;
        let names = fuse_init_flag_names(FUSE_BIG_WRITES | unknown);
        assert!(names.contains(&"BIG_WRITES".to_string()));
        assert!(names.iter().any(|n| n == "0x40000000"));
        // EXPORT_SUPPORT stays wired into logging even though it left the allowlist
        // (the documented reason the constant is retained), so an offered-but-dropped
        // bit is still rendered by name rather than as an opaque hex token.
        assert!(fuse_init_flag_names(FUSE_EXPORT_SUPPORT).contains(&"EXPORT_SUPPORT".to_string()));
    }

    #[test]
    fn rename2_flags_supported_only_accepts_zero() {
        // Flag-less rename is the only supported form.
        assert!(CurvineFileSystem::rename2_flags_supported(0));
        // Known rename flags are rejected (client does not plumb them through).
        assert!(!CurvineFileSystem::rename2_flags_supported(1)); // RENAME_NOREPLACE
        assert!(!CurvineFileSystem::rename2_flags_supported(2)); // RENAME_EXCHANGE
        assert!(!CurvineFileSystem::rename2_flags_supported(4)); // RENAME_WHITEOUT
                                                                 // An unknown high bit must also be rejected — checked against the raw
                                                                 // value, so it is not silently truncated away (the RenameFlags footgun).
        assert!(!CurvineFileSystem::rename2_flags_supported(1 << 6));
    }

    // Issue #1116: readdir must terminate. The dirent `off` field is the kernel's
    // resume cookie; encoding the current entry's position instead of the next
    // one makes the last entry of a batch carry a cookie equal to the offset the
    // kernel resumes at, so the kernel re-requests the same offset forever.
    //
    // This harness faithfully replays the kernel readdir protocol against the
    // PRODUCTION pieces: each round calls the real `DirHandle::get_batch` (which
    // owns the forward-only skip/positioning semantics) to fetch entries starting
    // at `offset`, then derives the next `offset` from the last entry's cookie via
    // the production `CurvineFileSystem::readdir_next_cookie`. Termination, no
    // duplicates, and no omissions are all asserted. With the pre-fix cookie
    // formula (`index`) the loop never advances past the tail entry and the round
    // guard trips — which is exactly the regression this pins.
    mod readdir_termination {
        use crate::fs::state::DirHandle;
        use curvine_common::fs::{ListStream, Path};
        use curvine_common::state::FileStatus;
        use orpc::runtime::{AsyncRuntime, RpcRuntime};

        fn entries(names: &[&str]) -> Vec<FileStatus> {
            names
                .iter()
                .enumerate()
                .map(|(i, n)| FileStatus::with_name(i as i64, n.to_string(), false))
                .collect()
        }

        // Replay the kernel readdir loop with a caller-supplied cookie function so
        // the test can exercise both the production formula and (as a discriminator)
        // the buggy pre-fix one. Returns the sequence of names handed to the kernel,
        // or Err if the loop fails to terminate within `max_rounds`.
        fn replay_readdir<F>(
            names: &[&str],
            cookie: F,
            max_rounds: usize,
        ) -> Result<Vec<String>, String>
        where
            F: Fn(u64) -> u64,
        {
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let path = Path::from_str("/d").unwrap();
                let mut seen = Vec::new();
                let mut offset: u64 = 0;

                for _ in 0..max_rounds {
                    // A fresh handle each round models the worst case where the
                    // stream is (re)positioned purely from `offset` — the same
                    // path the real daemon takes on a resumed/rebuilt readdir.
                    let handle = DirHandle::new(
                        1,
                        1,
                        &path,
                        1000,
                        ListStream::from_vec(entries(names)),
                    );
                    let batch = handle.get_batch(offset as usize).await.unwrap();
                    if batch.is_empty() {
                        return Ok(seen); // kernel sees 0 entries => readdir done
                    }

                    // The kernel consumes the batch and remembers the LAST entry's
                    // cookie as the offset for its next request.
                    let mut index = offset;
                    let mut last_cookie = offset;
                    for st in batch {
                        seen.push(st.name.clone());
                        last_cookie = cookie(index);
                        index += 1;
                    }
                    offset = last_cookie;
                }
                Err(format!(
                    "readdir did not terminate within {} rounds (offset stuck at {}, {} entries emitted)",
                    max_rounds,
                    offset,
                    seen.len()
                ))
            })
        }

        #[test]
        fn production_cookie_terminates_and_enumerates_each_entry_once() {
            let names = ["a", "b", "c", "d", "e"];
            let seen = replay_readdir(&names, super::CurvineFileSystem::readdir_next_cookie, 100)
                .expect("production readdir_next_cookie must let readdir terminate");
            assert_eq!(
                seen,
                names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "every entry enumerated exactly once, in order, with no repeats"
            );
        }

        // Discriminator: the pre-fix formula (cookie = current index) makes the
        // loop stick on the tail entry and never terminate. This proves the
        // harness above actually catches an off-by-one regression rather than
        // passing vacuously.
        #[test]
        fn prefix_cookie_would_loop_forever() {
            let names = ["a", "b", "c", "d", "e"];
            let result = replay_readdir(&names, |index| index, 100);
            assert!(
                result.is_err(),
                "cookie=index (pre-#1116 bug) must fail to terminate, got {:?}",
                result
            );
        }

        // Multi-batch variant that exercises the `FuseDirentList` response-size
        // cutoff path (reviewer request on PR #1236). The real
        // `read_dir_common_inner` stops filling a response when
        // `res.add_dirent(...)` returns false (kernel buffer full), pushes the
        // rejected entry back, and only advances `index` for entries actually
        // emitted — so the kernel resumes from the LAST EMITTED entry's cookie,
        // not from the whole `get_batch` result. `max_emit_per_round` models that
        // buffer limit: at most that many entries are handed to the kernel each
        // round, the rest are dropped (as if pushed back), and the next request
        // resumes from the last emitted cookie. This pins that `index + 1` still
        // advances correctly when a listing is split across several responses.
        fn replay_readdir_batched<F>(
            names: &[&str],
            cookie: F,
            max_emit_per_round: usize,
            max_rounds: usize,
        ) -> Result<Vec<String>, String>
        where
            F: Fn(u64) -> u64,
        {
            assert!(max_emit_per_round >= 1, "must emit at least one per round");
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let path = Path::from_str("/d").unwrap();
                let mut seen = Vec::new();
                let mut offset: u64 = 0;

                for _ in 0..max_rounds {
                    let handle =
                        DirHandle::new(1, 1, &path, 1000, ListStream::from_vec(entries(names)));
                    let batch = handle.get_batch(offset as usize).await.unwrap();
                    if batch.is_empty() {
                        return Ok(seen); // kernel sees 0 entries => readdir done
                    }

                    // Emit at most `max_emit_per_round` entries this round, mirroring
                    // the response-buffer cutoff. `index` only advances for emitted
                    // entries; the next offset is the last EMITTED entry's cookie.
                    let mut index = offset;
                    let mut last_cookie = offset;
                    for st in batch.into_iter().take(max_emit_per_round) {
                        seen.push(st.name.clone());
                        last_cookie = cookie(index);
                        index += 1;
                    }
                    offset = last_cookie;
                }
                Err(format!(
                    "readdir did not terminate within {} rounds (offset stuck at {}, {} entries emitted)",
                    max_rounds,
                    offset,
                    seen.len()
                ))
            })
        }

        // With the production cookie, a listing split across many small responses
        // still enumerates every entry exactly once, in order, and terminates.
        #[test]
        fn production_cookie_terminates_across_multiple_response_buffers() {
            let names = ["a", "b", "c", "d", "e", "f", "g"];
            // Emit 2 per round => 4 rounds of data + 1 terminating empty round.
            let seen = replay_readdir_batched(
                &names,
                super::CurvineFileSystem::readdir_next_cookie,
                2,
                100,
            )
            .expect("production cookie must terminate under a split response buffer");
            assert_eq!(
                seen,
                names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "split-response readdir enumerates each entry once, in order, no repeats/gaps"
            );
        }

        // Even down to a single entry per response (the harshest split), the
        // production cookie advances correctly and terminates.
        #[test]
        fn production_cookie_terminates_with_single_entry_responses() {
            let names = ["a", "b", "c"];
            let seen = replay_readdir_batched(
                &names,
                super::CurvineFileSystem::readdir_next_cookie,
                1,
                100,
            )
            .expect("production cookie must terminate at one entry per response");
            assert_eq!(seen, vec!["a", "b", "c"]);
        }

        // Discriminator for the split path: cookie=index loops forever here too,
        // proving the multi-batch harness genuinely exercises the regression.
        #[test]
        fn prefix_cookie_loops_forever_under_split_response() {
            let names = ["a", "b", "c", "d", "e", "f", "g"];
            let result = replay_readdir_batched(&names, |index| index, 2, 100);
            assert!(
                result.is_err(),
                "cookie=index must fail to terminate under split responses, got {:?}",
                result
            );
        }

        // Reviewer request (PR #1236): the two harnesses above rebuild a fresh
        // DirHandle with limit=1000 every round, so `get_batch` returns the whole
        // remainder in one shot and never drives DirHandle's OWN batching — the
        // `buf.len() < limit` fill loop and the `set_buf` push-back of leftovers.
        // That is the reused-stream production path (a directory is opened once
        // and the same stream is reused across every readdir in the session).
        //
        // This harness keeps a SINGLE handle across rounds with a SMALL limit, and
        // models the FUSE response cutoff by emitting at most `max_emit_per_round`
        // of what `get_batch` returned and pushing the rest back with the real
        // `set_buf` — exactly what `read_dir_common_inner` does when
        // `add_dirent` reports the kernel buffer is full. So cookie advancement is
        // validated together with DirHandle's real limit batching + set_buf
        // leftovers, not just a harness-side `take()`.
        //
        // Note: with a reused stream the resume `off` never re-enters get_batch's
        // skip branch (that fires only on a fresh `index == 0`); the stream just
        // advances forward and leftovers ride in `buf`. Termination here comes
        // from the stream draining, and the cookie must still march 1:1 with the
        // entries so nothing is dropped or repeated across the buf boundary.
        fn replay_readdir_reused_handle<F>(
            names: &[&str],
            cookie: F,
            limit: usize,
            max_emit_per_round: usize,
            max_rounds: usize,
        ) -> Result<Vec<String>, String>
        where
            F: Fn(u64) -> u64,
        {
            assert!(limit >= 1, "limit must be >= 1");
            assert!(max_emit_per_round >= 1, "must emit at least one per round");
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let path = Path::from_str("/d").unwrap();
                // One handle, reused across all rounds — the production session path.
                let handle =
                    DirHandle::new(1, 1, &path, limit, ListStream::from_vec(entries(names)));
                let mut seen = Vec::new();
                let mut offset: u64 = 0;

                for _ in 0..max_rounds {
                    let mut batch = handle.get_batch(offset as usize).await.unwrap();
                    if batch.is_empty() {
                        return Ok(seen); // kernel sees 0 entries => readdir done
                    }

                    // Emit at most `max_emit_per_round`; `index` (and the cookie)
                    // advance only for emitted entries.
                    let mut index = offset;
                    let mut last_cookie = offset;
                    let mut emitted = 0;
                    while emitted < max_emit_per_round {
                        match batch.pop_front() {
                            Some(st) => {
                                seen.push(st.name.clone());
                                last_cookie = cookie(index);
                                index += 1;
                                emitted += 1;
                            }
                            None => break,
                        }
                    }
                    // Push the unemitted remainder back, exactly as the daemon does
                    // when the kernel response buffer fills mid-batch.
                    handle.set_buf(batch).await.unwrap();
                    offset = last_cookie;
                }
                Err(format!(
                    "readdir did not terminate within {} rounds (offset stuck at {}, {} entries emitted)",
                    max_rounds,
                    offset,
                    seen.len()
                ))
            })
        }

        // Reused handle + small DirHandle limit (2) + response cutoff (1 per round):
        // DirHandle's own fill/limit batching and set_buf leftovers are both on the
        // path, and the production cookie still enumerates every entry once, in
        // order, and terminates.
        //
        // This test is NOT vacuous: it calls the real `get_batch`/`set_buf`, so a
        // regression in the limit fill loop or the leftover push-back would drop,
        // duplicate, or reorder entries and the exact-sequence assertion below
        // would fail. A cookie=index discriminator is deliberately NOT added on
        // this path: with a reused stream the resume `off` is never used to
        // position the stream (the stream only moves forward and leftovers ride in
        // `buf`), so even the buggy cookie terminates here — which is exactly why
        // the bug stayed latent in production (see PR summary). The infinite-loop
        // regression is pinned by the fresh-handle discriminators above
        // (`prefix_cookie_would_loop_forever` / `_under_split_response`).
        #[test]
        fn production_cookie_terminates_with_reused_handle_small_limit() {
            let names = ["a", "b", "c", "d", "e", "f", "g"];
            let seen = replay_readdir_reused_handle(
                &names,
                super::CurvineFileSystem::readdir_next_cookie,
                2, // DirHandle limit: real batching, not one-shot
                1, // emit 1 per round: forces set_buf push-back of the leftover
                100,
            )
            .expect("production cookie must terminate on a reused handle with a small limit");
            assert_eq!(
                seen,
                names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "reused-handle batching enumerates each entry once, in order, no repeats/gaps"
            );
        }
    }
}
