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

use crate::fs::operator::{GetAttr, Init, Lookup, OpenDir, StatFs};
use crate::fs::FileSystem;
use crate::raw::fuse_abi::*;
use crate::FuseError;
use crate::FuseResult;
use crate::*;
use curvine_config::FuseConf;
use log::info;

pub const TEST_DIR_ATTR: fuse_attr = fuse_attr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: 1702345521,
    mtime: 1702345521,
    ctime: 1702345521,
    atimensec: 0,
    mtimensec: 0,
    ctimensec: 0,
    mode: libc::S_IFDIR as u32 | 0o755u32,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,
    blksize: 512,
    padding: 0,
};

pub struct TestFileSystem {
    pub conf: FuseConf,
}

impl TestFileSystem {
    pub fn new(conf: FuseConf) -> Self {
        Self { conf }
    }
}

impl FileSystem for TestFileSystem {
    async fn init(&self, op: Init<'_>) -> FuseResult<fuse_init_out> {
        let out = fuse_init_out {
            major: 7,
            minor: 31,
            max_readahead: op.arg.max_readahead,
            flags: op.arg.flags,
            max_background: 16,
            congestion_threshold: 0,
            max_write: 16 * 1024 * 1024,
            #[cfg(feature = "fuse3")]
            time_gran: 0,
            #[cfg(feature = "fuse3")]
            max_pages: 0,
            #[cfg(feature = "fuse3")]
            padding: 0,
            #[cfg(feature = "fuse3")]
            unused: 0,
        };

        Ok(out)
    }

    async fn lookup(&self, op: Lookup<'_>) -> FuseResult<fuse_entry_out> {
        info!(
            "lookup parent id: {}, {}",
            op.header.nodeid,
            op.name.to_string_lossy()
        );
        Err(FuseError::new(libc::ENOENT, "".into()))
    }

    async fn get_attr(&self, op: GetAttr<'_>) -> FuseResult<fuse_attr_out> {
        info!("getattr parent id: {}", op.header.nodeid);

        let attr = fuse_attr_out {
            attr_valid: 1,
            attr_valid_nsec: 1,
            dummy: 0,
            attr: TEST_DIR_ATTR,
        };

        Ok(attr)
    }

    async fn open_dir(&self, _: OpenDir<'_>) -> FuseResult<fuse_open_out> {
        let fh = TEST_DIR_ATTR.ino | FILE_HANDLE_READ_BIT | FILE_HANDLE_WRITE_BIT;
        let attr = fuse_open_out {
            fh,
            open_flags: 0,
            padding: 0,
        };

        Ok(attr)
    }

    async fn stat_fs(&self, _: StatFs<'_>) -> FuseResult<fuse_kstatfs> {
        let attr = fuse_kstatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 10,
            ffree: 0,
            bsize: 512,
            namelen: 255,
            frsize: 0,
            padding: 0,
            spare: [0; 6],
        };

        Ok(attr)
    }
}
