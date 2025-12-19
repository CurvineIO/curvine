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

use crate::raw::fuse_abi::{fuse_dirent, fuse_direntplus, fuse_entry_out, fuse_read_in};
use crate::FuseUtils;
use curvine_common::state::FileStatus;
use orpc::sys::FFIUtils;
use std::mem::size_of;
use tokio_util::bytes::BytesMut;

#[derive(Debug)]
pub struct FuseDirentList {
    max_size: usize,
    buf: BytesMut,
}

impl FuseDirentList {
    pub fn new(arg: &fuse_read_in) -> Self {
        let max_size = arg.size as usize;
        Self {
            max_size,
            buf: BytesMut::with_capacity(max_size),
        }
    }

    pub fn take(self) -> BytesMut {
        self.buf
    }

    pub fn add(&mut self, off: u64, status: &FileStatus, entry: fuse_entry_out) -> bool {
        let typ = entry.attr.mode >> 12;
        let name = FFIUtils::get_os_bytes(&status.name);

        let header = fuse_dirent {
            ino: entry.attr.ino,
            off,
            namelen: name.len() as u32,
            typ,
        };
        self.add_buf(&header, name)
    }

    pub fn add_plus(&mut self, off: u64, status: &FileStatus, entry: fuse_entry_out) -> bool {
        let typ = entry.attr.mode >> 12;
        let name = FFIUtils::get_os_bytes(&status.name);
        let ino = entry.attr.ino;

        let header = fuse_direntplus {
            entry_out: entry,
            dirent: fuse_dirent {
                ino,
                off,
                namelen: name.len() as u32,
                typ,
            },
        };
        self.add_buf(&header, name)
    }

    /// Add an entry to the buffer and return false if the buff is already full.
    fn add_buf<T>(&mut self, data: &T, name: &[u8]) -> bool {
        let bytes = FuseUtils::struct_as_bytes(data);

        let data_len = bytes.len() + name.len();
        // 64 bytes aligned
        let align_len = (data_len + size_of::<u64>() - 1) & !(size_of::<u64>() - 1);

        if self.buf.len() + align_len > self.max_size {
            return false;
        }

        self.buf.extend_from_slice(bytes);
        self.buf.extend_from_slice(name);

        let pad_len = align_len - data_len;
        self.buf.extend_from_slice(&[0u8; 8][..pad_len]);

        true
    }
}
