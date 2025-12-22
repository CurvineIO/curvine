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

use crate::raw::fuse_abi::*;
use crate::{err_fuse, FuseResult};
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use tokio_util::bytes::Bytes;

#[derive(Debug)]
pub enum FuseOperator<'a> {
    Notimplemented,
    Empty,
    Init(Init<'a>),
    StatFs(StatFs<'a>),
    ReadDir(ReadDir<'a>),
    Lookup(Lookup<'a>),
    GetAttr(GetAttr<'a>),
    SetAttr(SetAttr<'a>),
    GetXAttr(GetXAttr<'a>),
    SetXAttr(SetXAttr<'a>),
    RemoveXAttr(RemoveXAttr<'a>),
    OpenDir(OpenDir<'a>),
    Mkdir(MkDir<'a>),
    FAllocate(FAllocate<'a>),
    ReleaseDir(ReleaseDir<'a>),
    Access(Access<'a>),
    ReadDirPlus(ReadDirPlus<'a>),
    Forget(Forget<'a>),
    Read(Read<'a>),
    Flush(Flush<'a>),
    Open(Open<'a>),
    Write(Write<'a>),
    MkNod(MkNod<'a>),
    Create(Create<'a>),
    Release(Release<'a>),
    Unlink(Unlink<'a>),
    RmDir(RmDir<'a>),
    Link(Link<'a>),
    BatchForget(BatchForget<'a>),
    Rename(Rename<'a>),
    Rename2(Rename2<'a>),
    Interrupt(Interrupt<'a>),
    ListXAttr(ListXAttr<'a>),
    FSync(FSync<'a>),
    Symlink(Symlink<'a>),
    Readlink(Readlink<'a>),
    GetLk(GetLk<'a>),
    SetLk(SetLk<'a>),
    SetLkW(SetLkW<'a>),
}

#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenAction {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

impl OpenAction {
    pub fn try_from(flags: u32) -> FuseResult<Self> {
        let mode = {
            #[cfg(target_os = "linux")]
            {
                (flags as i32) & libc::O_ACCMODE
            }
            #[cfg(not(target_os = "linux"))]
            {
                flags as i32
            }
        };

        match mode {
            libc::O_RDONLY => Ok(Self::ReadOnly),
            libc::O_WRONLY => Ok(Self::WriteOnly),
            libc::O_RDWR => Ok(Self::ReadWrite),
            _ => err_fuse!(
                libc::EOPNOTSUPP,
                "Failed to open: Not supported open flag {}",
                flags
            ),
        }
    }

    pub fn read(&self) -> bool {
        *self == Self::ReadWrite || *self == Self::ReadOnly
    }

    pub fn write(&self) -> bool {
        *self == Self::ReadWrite || *self == Self::WriteOnly
    }

    pub fn acl_mask(&self) -> u32 {
        match self {
            OpenAction::ReadOnly => libc::R_OK as u32,
            OpenAction::WriteOnly => libc::W_OK as u32,
            OpenAction::ReadWrite => (libc::R_OK | libc::W_OK) as u32,
        }
    }
}

// View file and directory information. ls, ll command
#[derive(Debug)]
pub struct StatFs<'a> {
    pub header: &'a fuse_in_header,
}

#[derive(Debug)]
pub struct ReadDir<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_read_in,
}

#[derive(Debug)]
pub struct Init<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_init_in,
}

#[derive(Debug)]
pub struct Lookup<'a> {
    pub header: &'a fuse_in_header,
    pub name: &'a OsStr,
}

// Get file attributes.
#[derive(Debug)]
pub struct GetAttr<'a> {
    pub header: &'a fuse_in_header,
}

#[derive(Debug)]
pub struct SetAttr<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_setattr_in,
}

#[derive(Debug)]
pub struct OpenDir<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_open_in,
}

/// Create a directory.
#[derive(Debug)]
pub struct MkDir<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_mkdir_in,
    pub name: &'a OsStr,
}

// Preallocate disk space.
#[derive(Debug)]
pub struct FAllocate<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_fallocate_in,
}

// Request to destroy the file system.
#[derive(Debug)]
pub struct Destroy<'a> {
    pub header: &'a fuse_in_header,
}

/// When the kernel no longer needs information about an inode, it will send a Forget request to the user state
/// The user-state file system should clean up the resources of the corresponding node when receiving the Forget request to free up memory or other resources.
/// The main purpose of this interface is to maintain the reference count of file system nodes and ensure that no longer needed nodes are properly released.
///
///Each node in the file system has a reference count, recording how many paths are pointing to the node.
// When the reference count of a node decreases to zero, it means that there is no path pointing to the node and its resources can be safely released.
// Forget requests to notify the user-state file system that the reference count of a node needs to be reduced.
#[derive(Debug)]
pub struct Forget<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_forget_in,
}

/// Create and open a file.
#[derive(Debug)]
pub struct Create<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_create_in,
    pub name: &'a OsStr,
}

/// Open a file and perform read or write operations.
#[derive(Debug)]
pub struct Open<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_open_in,
}

// Read data.
#[derive(Debug)]
pub struct Read<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_read_in,
}

// Write data.
pub struct Write<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_write_in,
    pub data: Bytes,
}

impl Debug for Write<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("header", self.header)
            .field("arg", &self.arg)
            .field("data_len", &self.data.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct ReleaseDir<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_release_in,
}

#[derive(Debug)]
pub struct Access<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_access_in,
}

#[derive(Debug)]
pub struct GetXAttr<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_getxattr_in,
    pub name: &'a OsStr,
}

/// SetXAttr request structure:
/// ```text
/// +0                         +40                    +56           +56+len(name)+1
/// |--------------------------|---------------------|-------------|--------------------------|
/// |    fuse_in_header        | fuse_setxattr_in    |    name     |    value                 |
/// |      (40 bytes)          |     (16 bytes)      | (variable)  | (size bytes)             |
/// |--------------------------|---------------------|-------------|--------------------------|
/// ```

#[derive(Debug)]
pub struct SetXAttr<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_setxattr_in,
    pub name: &'a OsStr,
    pub value: &'a [u8],
}

#[derive(Debug)]
pub struct RemoveXAttr<'a> {
    pub header: &'a fuse_in_header,
    pub name: &'a OsStr,
}

#[derive(Debug)]
pub struct ReadDirPlus<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_read_in,
}

#[derive(Debug)]
pub struct Flush<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_flush_in,
}

#[derive(Debug)]
pub struct MkNod<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_mknod_in,
    pub name: &'a OsStr,
}

#[derive(Debug)]
pub struct Release<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_release_in,
}

#[derive(Debug)]
pub struct Unlink<'a> {
    pub header: &'a fuse_in_header,
    pub name: &'a OsStr,
}

#[derive(Debug)]
pub struct Link<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_link_in,
    pub name: &'a OsStr,
}

#[derive(Debug)]
pub struct RmDir<'a> {
    pub header: &'a fuse_in_header,
    pub name: &'a OsStr,
}

#[derive(Debug)]
pub struct BatchForget<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_batch_forget_in,
    pub nodes: &'a [fuse_forget_one],
}

// Rename a file.
#[derive(Debug)]
pub struct Rename<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_rename_in,
    pub old_name: &'a OsStr,
    pub new_name: &'a OsStr,
}

// Rename a file with flags (RENAME2).
#[derive(Debug)]
pub struct Rename2<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_rename2_in,
    pub old_name: &'a OsStr,
    pub new_name: &'a OsStr,
}

#[derive(Debug)]
pub struct Interrupt<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_interrupt_in,
}

#[derive(Debug)]
pub struct ListXAttr<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_getxattr_in,
}

#[derive(Debug)]
pub struct FSync<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_fsync_in,
}

/// ```txt
/// /linkname -> /target_path
/// linkname: /linkname
/// target: /target_path
/// ```
#[derive(Debug)]
pub struct Symlink<'a> {
    pub header: &'a fuse_in_header,
    // symlink name
    pub linkname: &'a OsStr,
    // target name that the symlink point to
    pub target: &'a OsStr,
}

// Read the target of a symbolic link.
#[derive(Debug)]
pub struct Readlink<'a> {
    pub header: &'a fuse_in_header,
}

#[derive(Debug)]
pub struct GetLk<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_lk_in,
}

#[derive(Debug)]
pub struct SetLk<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_lk_in,
}

#[derive(Debug)]
pub struct SetLkW<'a> {
    pub header: &'a fuse_in_header,
    pub arg: &'a fuse_lk_in,
}
