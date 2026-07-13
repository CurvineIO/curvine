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

use crate::raw::fuse_abi::fuse_getxattr_out;
use crate::FuseUtils;
use bytes::BytesMut;

pub struct FuseBuf {
    buf: BytesMut,
}

impl FuseBuf {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
        }
    }

    pub fn add_xattr_out(&mut self, len: usize) {
        let attr = fuse_getxattr_out {
            size: len as u32,
            padding: 0,
        };
        self.add_slice(FuseUtils::struct_as_bytes(&attr));
    }

    pub fn add_slice(&mut self, dst: &[u8]) {
        self.buf.extend_from_slice(dst);
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn take(self) -> BytesMut {
        self.buf
    }
}

impl Default for FuseBuf {
    fn default() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }
}
