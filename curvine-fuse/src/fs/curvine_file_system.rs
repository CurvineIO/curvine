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
use crate::fs::state::NodeState;
use crate::raw::fuse_abi::*;
use crate::raw::FuseDirentList;
use crate::session::{FuseBuf, FuseResponse};
use crate::*;
use crate::{err_fuse, FuseError, FuseResult, FuseUtils};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::{ClusterConf, FuseConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    CreateFileOptsBuilder, FileAllocMode, FileAllocOpts, FileStatus, MkdirOptsBuilder, OpenFlags,
    SetAttrOpts,
};
use log::{debug, error, info, warn};
use orpc::common::ByteUnit;
use orpc::runtime::Runtime;
use orpc::sys::FFIUtils;
use orpc::{sys, try_option};
use std::collections::HashMap;
use std::sync::Arc;
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
        }
        if conf.kernel_cache {
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
        let blocks = (status.len as f64 / FUSE_BLOCK_SIZE as f64).ceil() as u64;

        let ctime_sec = if status.atime > 0 {
            (status.atime / 1000) as u64
        } else {
            (status.mtime / 1000) as u64
        };
        let ctime_nsec = if status.atime > 0 {
            ((status.atime % 1000) * 1000) as u32
        } else {
            ((status.mtime % 1000) * 1000) as u32
        };

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
                None => conf.uid,
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
            atime: ctime_sec,
            mtime: ctime_sec,
            ctime: ctime_sec,
            atimensec: ctime_nsec,
            mtimensec: ctime_nsec,
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

    fn new_dot_status(name: &str) -> FileStatus {
        FileStatus::with_name(FUSE_UNKNOWN_INO as i64, name.to_string(), true)
    }

    async fn fs_list_status(&self, parent: u64, path: &Path) -> FuseResult<Vec<FileStatus>> {
        let mut res = vec![];
        res.push(Self::new_dot_status(FUSE_CURRENT_DIR));
        res.push(Self::new_dot_status(FUSE_PARENT_DIR));

        let list = self.fs.list_status(path).await?;
        let list = if self.conf.read_dir_fill_ino {
            self.state.fill_ino(parent, list)?
        } else {
            list
        };

        for status in list {
            res.push(status);
        }

        Ok(res)
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

    // fuse.c peer implementation of lookup_path function.
    async fn lookup_path<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
        path: &Path,
    ) -> FuseResult<fuse_attr> {
        let status = self.fs_get_status(path).await?;
        let attr = self.state.do_lookup(parent, name, &status)?;
        // Apply time overrides if present
        Ok(attr)
    }

    fn lookup_status<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
        status: &FileStatus,
    ) -> FuseResult<fuse_attr> {
        let attr = self.state.do_lookup(parent, name, status)?;
        Ok(attr)
    }

    async fn read_dir_common(
        &self,
        header: &fuse_in_header,
        arg: &fuse_read_in,
        plus: bool,
    ) -> FuseResult<FuseDirentList> {
        let path = self.state.get_path(header.nodeid)?;
        let dir_status = self.fs_get_status(&path).await?;

        // Check directory read permission for reading directory contents
        self.check_access_permissions(&dir_status, header, libc::R_OK as u32)?;

        let list = self.fs_list_status(header.nodeid, &path).await?;

        let start_index = arg.offset as usize;
        let mut res = FuseDirentList::new(arg);
        for (index, status) in list.iter().enumerate().skip(start_index) {
            let attr = Self::status_to_attr(&self.conf, status)?;
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

    /// Check if the current user has the requested access permissions
    fn check_access_permissions(
        &self,
        status: &FileStatus,
        header: &fuse_in_header,
        mask: u32,
    ) -> FuseResult<()> {
        // Root (uid=0) bypasses permission checks
        if header.uid == 0 {
            return Ok(());
        }

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
        if let Some((ufs_path, _)) = self.fs.get_mount(path).await? {
            warn!(
                "ufs {} -> {} does not support resize, will ignore",
                path, ufs_path
            );
            return Ok(());
        }

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
            | FUSE_SPLICE_WRITE;
        let max_write = FuseUtils::get_fuse_buf_size() - FUSE_BUFFER_HEADER_SIZE;
        let page_size = sys::get_pagesize()?;
        let max_pages = if op.arg.flags & FUSE_MAX_PAGES != 0 {
            out_flags |= FUSE_MAX_PAGES;
            (max_write - 1) / page_size + 1
        } else {
            0
        };

        let out = fuse_init_out {
            major: op.arg.major,
            minor: op.arg.minor,
            max_readahead: op.arg.max_readahead,
            flags: op.arg.flags | out_flags,
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
        let parent_status = self.fs_get_status(&parent_path).await?;
        self.check_access_permissions(&parent_status, op.header, libc::X_OK as u32)?;

        // Get the path.
        let path = self.state.get_path_common(parent, name.as_deref())?;
        let res = self.lookup_path(parent, name.as_deref(), &path).await;

        let entry = match res {
            Ok(attr) => Self::create_entry_out(&self.conf, attr),

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

    // getfattr /curvine-fuse/x.log -n id
    // Query the inode id of node in curvine system, and it is useful to troubleshoot problems.
    // Output:
    // # file: curvine-fuse/x.log
    // id="1057"
    async fn get_xattr(&self, op: GetXAttr<'_>) -> FuseResult<BytesMut> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path(op.header.nodeid)?;

        debug!("Getting xattr: path='{}' name='{}'", path, name);

        let status = match self.fs.get_status(&path).await {
            Ok(status) => status,
            Err(e) => {
                error!("Failed to get status for {}: {}", path, e);
                return err_fuse!(libc::ENOENT, "File not found: {}", path);
            }
        };

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
            "security.capability" => {
                debug!("Querying file capabilities for: {}", path);
                // Return ENODATA silently to avoid ERROR logs
                let err = FuseError::new(libc::ENODATA, "No capabilities set".into());
                return Err(err);
            }
            "security.selinux" => {
                debug!("Querying SELinux context for: {}", path);
                // Return ENODATA silently to avoid ERROR logs
                let err = FuseError::new(libc::ENODATA, "No SELinux context set".into());
                return Err(err);
            }
            "system.posix_acl_access" | "system.posix_acl_default" => {
                debug!("Querying POSIX ACL for: {}", path);
                // Return ENODATA silently to avoid ERROR logs
                let err = FuseError::new(libc::ENODATA, "POSIX ACLs not supported".into());
                return Err(err);
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

        info!(
            "Setting xattr: path='{}' name='{}' value='{}'",
            path,
            name,
            String::from_utf8_lossy(value_slice)
        );

        // Accept the SELinux labels in the FUSE layer. Add a guard in set_xattr so the driver simply ACKs the write instead of forwarding it.
        if name == "security.selinux" {
            debug!("Ignoring SELinux label on {}", path);
            return Ok(());
        }

        // Create SetAttrOpts with the xattr to add
        let mut add_x_attr = HashMap::new();
        add_x_attr.insert(name.to_string(), value_slice.to_vec());

        let opts = SetAttrOpts {
            add_x_attr,
            ..Default::default()
        };

        let _ = self.fs_set_attr(&path, opts).await?;
        Ok(())
    }

    // setfattr -x system.posix_acl_access /curvine-fuse/file
    // Remove POSIX ACL attributes from files and directories
    async fn remove_xattr(&self, op: RemoveXAttr<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path(op.header.nodeid)?;

        info!("Removing xattr: path='{}' name='{}'", path, name);

        // Handle system extended attributes silently to avoid ERROR logs
        match name {
            "security.capability" => {
                debug!("Removing file capabilities for: {}", path);
                // Return success silently to avoid ERROR logs
                return Ok(());
            }
            "security.selinux" => {
                debug!("Removing SELinux context for: {}", path);
                // Return success silently to avoid ERROR logs
                return Ok(());
            }
            "system.posix_acl_access" | "system.posix_acl_default" => {
                debug!("Removing POSIX ACL for: {}", path);
                // Return success silently to avoid ERROR logs
                return Ok(());
            }
            _ => {
                // Handle user-defined attributes and other attributes
            }
        }

        // Create SetAttrOpts with the xattr to remove
        let opts = SetAttrOpts {
            remove_x_attr: vec![name.to_string()],
            ..Default::default()
        };

        let _ = self.fs_set_attr(&path, opts).await?;
        Ok(())
    }

    // listxattr /curvine-fuse/file
    // List all extended attributes for a file or directory
    async fn list_xattr(&self, op: ListXAttr<'_>) -> FuseResult<BytesMut> {
        let path = self.state.get_path(op.header.nodeid)?;
        debug!("Listing xattrs: path='{}' size={}", path, op.arg.size);

        let status = match self.fs.get_status(&path).await {
            Ok(status) => status,
            Err(e) => {
                error!("Failed to get status for {}: {}", path, e);
                return err_fuse!(libc::ENOENT, "File not found: {}", path);
            }
        };

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

    // Get the attribute of the specified inode.
    async fn get_attr(&self, op: GetAttr<'_>) -> FuseResult<fuse_attr_out> {
        let path = self.state.get_path(op.header.nodeid)?;

        let attr = self
            .lookup_path::<String>(op.header.nodeid, None, &path)
            .await?;

        let attr = fuse_attr_out {
            attr_valid: self.conf.attr_ttl.as_secs(),
            attr_valid_nsec: self.conf.attr_ttl.subsec_nanos(),
            dummy: 0,
            attr,
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
            let cur_status = self.fs_get_status(&path).await?;
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

        // Handle file size change (truncate/resize)
        if (op.arg.valid & FATTR_SIZE) != 0 {
            let expect_len = op.arg.size as i64;
            if expect_len != status.len {
                let resize_opts = FileAllocOpts::with_truncate(expect_len);
                self.fs_resize(&path, op.header.nodeid, op.arg.fh, resize_opts)
                    .await?;
                status.len = expect_len;
            }
        }

        let attr = Self::status_to_attr(&self.conf, &status)?;
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
                let parent_status = self.fs_get_status(&parent_path).await?;
                self.check_access_permissions(&parent_status, op.header, libc::X_OK as u32)?;
            }
        }

        // Get file status to check permissions
        let status = self.fs.get_status(&path).await?;
        self.check_access_permissions(&status, op.header, op.arg.mask)?;

        Ok(())
    }

    // Open the directory.
    async fn open_dir(&self, op: OpenDir<'_>) -> FuseResult<fuse_open_out> {
        let action = OpenAction::try_from(op.arg.flags)?;

        // Check directory permissions based on open action
        let dir_path = self.state.get_path(op.header.nodeid)?;
        let dir_status = self.fs_get_status(&dir_path).await?;

        // Use existing permission check function to verify access
        self.check_access_permissions(&dir_status, op.header, action.acl_mask())?;

        let fh = self.state.next_fh();
        let open_flags = Self::fill_open_flags(&self.conf, op.arg.flags);
        let attr = fuse_open_out {
            fh,
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
            opts = opts.acl(op.header.uid, op.header.gid, op.arg.mode & 0o7777)
        }

        let status = match self.fs.fuse_mkdir(&path, opts.build()).await {
            Ok(status) => match status {
                Some(v) => v,
                None => self.fs.get_status(&path).await?,
            },

            Err(e) => {
                let e: FuseError = e.into();
                return err_fuse!(e.errno, "mkdir {}: {}", path, e);
            }
        };

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
        let _ = self.state.get_path(op.header.nodeid)?;
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
        let opts = CreateFileOptsBuilder::with_conf(&self.fs.conf().client);
        let handle = self
            .state
            .new_handle(op.header.nodeid, &path, op.arg.flags, opts.build())
            .await?;

        // Check file access permissions before opening
        let action = OpenAction::try_from(op.arg.flags)?;
        self.check_access_permissions(handle.status(), op.header, action.acl_mask())?;

        let open_flags = Self::fill_open_flags(&self.conf, op.arg.flags);
        let entry = fuse_open_out {
            fh: handle.fh,
            open_flags,
            padding: 0,
        };

        Ok(entry)
    }

    async fn create(&self, op: Create<'_>) -> FuseResult<fuse_create_out> {
        if !FuseUtils::s_isreg(op.arg.mode) {
            return err_fuse!(libc::EIO);
        }

        let id = op.header.nodeid;
        let name = try_option!(op.name.to_str());
        if name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let path = self.state.get_path_common(id, Some(name))?;
        let node = self.state.find_node(id, Some(name))?;
        let flags = op.arg.flags;

        // create opts
        let mut opts = CreateFileOptsBuilder::with_conf(&self.fs.conf().client);
        // Apply requested mode and ownership to the new file if provided
        if op.arg.mode != 0 {
            opts = opts.acl(op.header.uid, op.header.gid, op.arg.mode & 0o7777)
        }
        let handle = self
            .state
            .new_handle(node.id, &path, flags, opts.build())
            .await?;

        let attr = self.lookup_status(id, Some(name), handle.status())?;
        let open_flags = Self::fill_open_flags(&self.conf, flags);
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
                open_flags,
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
        handle.flush(Some(reply)).await
    }

    async fn release(&self, op: Release<'_>, reply: FuseResponse) -> FuseResult<()> {
        let handle = self.state.remove_handle(op.header.nodeid, op.arg.fh);
        if let Some(handle) = handle {
            handle.complete(Some(reply)).await
        } else {
            err_fuse!(libc::EIO)
        }
    }

    async fn forget(&self, op: Forget<'_>) -> FuseResult<()> {
        self.state.forget_node(op.header.nodeid, op.arg.nlookup)
    }

    async fn unlink(&self, op: Unlink<'_>) -> FuseResult<()> {
        let name = try_option!(op.name.to_str());
        let path = self.state.get_path_common(op.header.nodeid, Some(name))?;

        self.fs.delete(&path, false).await?;
        // self.state.unlink_name(op.header.nodeid, name);

        debug!(
            "unlink: removed name mapping for parent={}, name={}",
            op.header.nodeid, name
        );

        Ok(())
    }

    async fn link(&self, op: Link<'_>) -> FuseResult<fuse_entry_out> {
        let name = try_option!(op.name.to_str());
        let oldnodeid = op.arg.oldnodeid;

        let des_path = self.state.get_path_common(op.header.nodeid, Some(name))?;
        let src_path = self.state.get_path(oldnodeid)?;
        let src_status = self.fs_get_status(&src_path).await?;

        debug!(
            "link: src_path={}, des_path={}, oldnodeid={}, parent={}",
            src_path, des_path, oldnodeid, op.header.nodeid
        );

        if self.fs.exists(&des_path).await? {
            return err_fuse!(libc::EEXIST, "File already exists: {}", des_path);
        }

        if src_status.is_dir {
            return err_fuse!(libc::EPERM, "Cannot create link to directory: {}", src_path);
        }

        self.fs.link(&src_path, &des_path).await?;
        // self.state.link_node(op.header.nodeid, name, oldnodeid)?;
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
        Ok(())
    }

    async fn rename(&self, op: Rename<'_>) -> FuseResult<()> {
        let old_name = try_option!(op.old_name.to_str());
        let new_name = try_option!(op.new_name.to_str());
        if new_name.len() > FUSE_MAX_NAME_LENGTH {
            return err_fuse!(libc::ENAMETOOLONG);
        }

        let (old_path, new_path) =
            self.state
                .get_path2(op.header.nodeid, old_name, op.arg.newdir, new_name)?;

        // Perform the rename operation
        self.fs.rename(&old_path, &new_path).await?;

        // Update FUSE state after successful backend rename
        self.state
            .rename_node(op.header.nodeid, old_name, op.arg.newdir, new_name)?;

        info!("Successfully renamed {} to {}", old_path, new_path);
        Ok(())
    }

    // interrupt request, curvine each future has a timeout time and will not block for a long time, so this interface is not required.
    async fn interrupt(&self, _: Interrupt<'_>) -> FuseResult<()> {
        Ok(())
    }

    async fn batch_forget(&self, op: BatchForget<'_>) -> FuseResult<()> {
        self.state.batch_forget_node(op.nodes)
    }

    // Create a symbolic link
    async fn symlink(&self, op: Symlink<'_>) -> FuseResult<fuse_entry_out> {
        let linkname = try_option!(op.linkname.to_str());
        let target = try_option!(op.target.to_str());
        let id = op.header.nodeid;
        info!("symlink: linkname={:?}, target={:?}", linkname, target);

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

        // Call backend filesystem to create the symbolic link
        // Use force=false to prevent overwriting existing files (standard ln -s behavior)
        self.fs.symlink(target, &link_path, false).await?;

        // Get the created symlink's attributes
        let entry = self.lookup_path(parent, linkname, &link_path).await?;

        Ok(Self::create_entry_out(&self.conf, entry))
    }

    // Read the target of a symbolic link
    async fn readlink(&self, op: Readlink<'_>) -> FuseResult<BytesMut> {
        let path = self.state.get_path(op.header.nodeid)?;

        // Get file status to read the symlink target
        let status = self.fs_get_status(&path).await?;

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
        handle.flush(Some(reply)).await
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
            if let Some(handle) = handle {
                handle.complete(None).await?;
            } else {
                return err_fuse!(libc::EIO);
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
}
