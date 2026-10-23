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

//! Record framing shared by every [`Record`](crate::Record).
//!
//! One record is `[key_len][value_len][key][value]`. The layout does not carry
//! the lengths of a particular record; it says how many bytes each length
//! field occupies, so a single [`Layout`] describes the whole area while keys
//! and values stay variable length. Business data (kinds, flags, ids) lives
//! inside the value bytes, which keeps the container format-agnostic.

use std::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    key_len_width: u8,
    value_len_width: u8,
}

impl Default for Layout {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

impl Layout {
    pub const fn new(key_len_width: u8, value_len_width: u8) -> Self {
        assert!(
            key_len_width == 1 || key_len_width == 2 || key_len_width == 4 || key_len_width == 8,
            "key length width must be 1, 2, 4 or 8"
        );
        assert!(
            value_len_width == 1
                || value_len_width == 2
                || value_len_width == 4
                || value_len_width == 8,
            "value length width must be 1, 2, 4 or 8"
        );
        Self {
            key_len_width,
            value_len_width,
        }
    }

    #[inline]
    pub const fn key_len_width(&self) -> u8 {
        self.key_len_width
    }

    #[inline]
    pub const fn value_len_width(&self) -> u8 {
        self.value_len_width
    }

    #[inline]
    pub const fn header_len(&self) -> usize {
        self.key_len_width as usize + self.value_len_width as usize
    }

    #[inline]
    pub const fn max_key_len(&self) -> usize {
        Self::max_len(self.key_len_width)
    }

    #[inline]
    pub const fn max_value_len(&self) -> usize {
        Self::max_len(self.value_len_width)
    }

    #[inline]
    pub fn key_len(&self, buf: &[u8], off: usize) -> usize {
        let width = self.key_len_width as usize;
        Self::read_len(&buf[off..off + width], self.key_len_width)
    }

    #[inline]
    pub fn value_len(&self, buf: &[u8], off: usize) -> usize {
        let start = off + self.key_len_width as usize;
        let width = self.value_len_width as usize;
        Self::read_len(&buf[start..start + width], self.value_len_width)
    }

    #[inline]
    pub fn record_len(&self, buf: &[u8], off: usize) -> usize {
        self.header_len() + self.key_len(buf, off) + self.value_len(buf, off)
    }

    #[inline]
    pub const fn record_len_of(&self, key_len: usize, value_len: usize) -> usize {
        self.header_len() + key_len + value_len
    }

    #[inline]
    pub fn key_range(&self, buf: &[u8], off: usize) -> Range<usize> {
        let start = off + self.header_len();
        start..start + self.key_len(buf, off)
    }

    #[inline]
    pub fn value_range(&self, buf: &[u8], off: usize) -> Range<usize> {
        let start = self.key_range(buf, off).end;
        start..start + self.value_len(buf, off)
    }

    #[inline]
    pub(crate) fn write_header_and_key(
        &self,
        buf: &mut [u8],
        at: usize,
        key: &[u8],
        value_len: usize,
    ) -> usize {
        let header_len = self.header_len();
        self.write_header(&mut buf[at..at + header_len], key.len(), value_len);

        let key_start = at + header_len;
        let value_start = key_start + key.len();
        buf[key_start..value_start].copy_from_slice(key);
        value_start
    }

    #[inline]
    fn write_header(&self, header: &mut [u8], key_len: usize, value_len: usize) {
        let key_width = self.key_len_width as usize;
        Self::write_len(&mut header[..key_width], self.key_len_width, key_len);
        Self::write_len(
            &mut header[key_width..self.header_len()],
            self.value_len_width,
            value_len,
        );
    }

    #[inline]
    fn read_len(buf: &[u8], width: u8) -> usize {
        match width {
            1 => buf[0] as usize,
            2 => u16::from_le_bytes(buf[..2].try_into().unwrap()) as usize,
            4 => u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize,
            8 => u64::from_le_bytes(buf[..8].try_into().unwrap()) as usize,
            _ => unreachable!("record length width must be 1, 2, 4 or 8"),
        }
    }

    #[inline]
    fn write_len(buf: &mut [u8], width: u8, len: usize) {
        match width {
            1 => buf[0] = len as u8,
            2 => buf[..2].copy_from_slice(&(len as u16).to_le_bytes()),
            4 => buf[..4].copy_from_slice(&(len as u32).to_le_bytes()),
            8 => buf[..8].copy_from_slice(&(len as u64).to_le_bytes()),
            _ => unreachable!("record length width must be 1, 2, 4 or 8"),
        }
    }

    #[inline]
    const fn max_len(width: u8) -> usize {
        match width {
            1 => u8::MAX as usize,
            2 => u16::MAX as usize,
            4 => u32::MAX as usize,
            _ => usize::MAX,
        }
    }
}
