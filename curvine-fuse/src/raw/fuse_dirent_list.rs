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
use bytes::BytesMut;
use curvine_model::FileStatus;
use curvine_sys::FFIUtils;
use std::mem::size_of;

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

    pub fn remaining(&self) -> usize {
        self.max_size.saturating_sub(self.buf.len())
    }

    /// Bytes `add_dirent` will consume for `name`, including 8-byte alignment padding.
    pub fn dirent_len(plus: bool, name: &str) -> usize {
        let header = if plus {
            size_of::<fuse_direntplus>()
        } else {
            size_of::<fuse_dirent>()
        };
        Self::aligned(header + FFIUtils::get_os_bytes(name).len())
    }

    fn aligned(len: usize) -> usize {
        (len + size_of::<u64>() - 1) & !(size_of::<u64>() - 1)
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

    pub fn add_dirent(
        &mut self,
        plus: bool,
        off: u64,
        status: &FileStatus,
        entry: fuse_entry_out,
    ) -> bool {
        if plus {
            self.add_plus(off, status, entry)
        } else {
            self.add(off, status, entry)
        }
    }

    /// Add an entry to the buffer and return false if the buffer is already full.
    fn add_buf<T>(&mut self, data: &T, name: &[u8]) -> bool {
        let bytes = FuseUtils::struct_as_bytes(data);

        let data_len = bytes.len() + name.len();
        let align_len = Self::aligned(data_len);

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

#[cfg(test)]
mod tests {
    use super::*;

    /// `dirent_len` is the budget READDIR uses to decide which entries fit, so it must agree with
    /// what `add_dirent` actually writes, including the 8-byte alignment padding.
    #[test]
    fn dirent_len_matches_bytes_add_dirent_writes() {
        let arg = fuse_read_in {
            size: 1 << 16,
            ..Default::default()
        };
        // 1, 7, 8 and 9 bytes cover the padding boundaries of both dirent headers.
        for plus in [false, true] {
            for name in ["a", "abcdefg", "abcdefgh", "abcdefghi"] {
                let mut list = FuseDirentList::new(&arg);
                let status = FileStatus::with_name(2, name.to_string(), false);
                let before = list.remaining();
                assert!(list.add_dirent(plus, 1, &status, fuse_entry_out::default()));
                assert_eq!(
                    FuseDirentList::dirent_len(plus, name),
                    before - list.remaining(),
                    "plus={plus} name={name}"
                );
            }
        }
    }
}
