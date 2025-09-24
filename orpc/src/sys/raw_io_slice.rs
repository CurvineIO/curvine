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

use crate::sys::RawIO;

// The description of the underlying io object points to a certain fragment of the file or network.
#[derive(Debug, Clone)]
pub struct RawIOSlice {
    raw: RawIO,
    off: Option<i64>,
    len: usize,
}

impl RawIOSlice {
    pub fn new(raw: RawIO, off: Option<i64>, len: usize) -> Self {
        Self { raw, off, len }
    }

    pub fn raw_io(&self) -> RawIO {
        self.raw
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn off(&self) -> Option<i64> {
        self.off
    }

    pub fn as_slice(&self) -> &[u8] {
        panic!("IoSlice does not support as_bytes")
    }

    pub fn split_to(&mut self, at: usize) -> Self {
        if at >= self.len {
            let result = self.clone();
            self.len = 0;
            result
        } else {
            let result = Self {
                raw: self.raw,
                off: self.off,
                len: at,
            };

            // Update current slice
            if let Some(ref mut offset) = self.off {
                *offset += at as i64;
            }
            self.len -= at;

            result
        }
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        if at >= self.len {
            Self {
                raw: self.raw,
                off: self.off.map(|o| o + self.len as i64),
                len: 0,
            }
        } else {
            let result = Self {
                raw: self.raw,
                off: self.off.map(|o| o + at as i64),
                len: self.len - at,
            };

            self.len = at;

            result
        }
    }

    pub fn copy_to_slice(&mut self, _dst: &mut [u8]) {
        panic!("IOSlice does not support copy_to_slice: use read system call instead")
    }

    pub fn advance(&mut self, cnt: usize) {
        let advance_len = cnt.min(self.len);

        if let Some(ref mut offset) = self.off {
            *offset += advance_len as i64;
        }

        self.len -= advance_len;
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }
}
