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

use num_enum::{FromPrimitive, IntoPrimitive};

#[repr(u32)]
#[derive(Debug, FromPrimitive, IntoPrimitive, Copy, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum FuseOpCode {
    #[num_enum(default)]
    NOT_SUPPORTED = 0,

    FUSE_LOOKUP = 1,
    FUSE_FORGET = 2, // no reply
    FUSE_GETATTR = 3,
    FUSE_SETATTR = 4,
    FUSE_READLINK = 5,
    FUSE_SYMLINK = 6,
    FUSE_MKNOD = 8,
    FUSE_MKDIR = 9,
    FUSE_UNLINK = 10,
    FUSE_RMDIR = 11,
    FUSE_RENAME = 12,
    FUSE_LINK = 13,
    FUSE_OPEN = 14,
    FUSE_READ = 15,
    FUSE_WRITE = 16,
    FUSE_STATFS = 17,
    FUSE_RELEASE = 18,
    FUSE_FSYNC = 20,
    FUSE_SETXATTR = 21,
    FUSE_GETXATTR = 22,
    FUSE_LISTXATTR = 23,
    FUSE_REMOVEXATTR = 24,
    FUSE_FLUSH = 25,
    FUSE_INIT = 26,
    FUSE_OPENDIR = 27,
    FUSE_READDIR = 28,
    FUSE_RELEASEDIR = 29,
    FUSE_FSYNCDIR = 30,
    FUSE_GETLK = 31,
    FUSE_SETLK = 32,
    FUSE_SETLKW = 33,
    FUSE_ACCESS = 34,
    FUSE_CREATE = 35,
    FUSE_INTERRUPT = 36,
    FUSE_BMAP = 37,
    FUSE_DESTROY = 38,
    FUSE_IOCTL = 39,
    FUSE_POLL = 40,
    FUSE_NOTIFY_REPLY = 41,
    FUSE_BATCH_FORGET = 42,
    FUSE_FALLOCATE = 43,
    FUSE_READDIRPLUS = 44,
    FUSE_RENAME2 = 45,
    FUSE_LSEEK = 46,

    CUSE_INIT = 4096,
}
