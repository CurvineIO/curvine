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

use crate::Layout;
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct Chunk {
    buf: Box<[u8]>,
    used: u16,
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunk")
            .field("used", &self.used)
            .field("cap", &self.buf.len())
            .finish()
    }
}

impl Chunk {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            buf: vec![0u8; cap].into_boxed_slice(),
            used: 0,
        }
    }

    #[inline]
    pub fn buf(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub(crate) fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    #[inline]
    pub fn used(&self) -> usize {
        self.used as usize
    }

    #[inline]
    pub(crate) fn cap(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub(crate) fn set_used(&mut self, used: u16) {
        debug_assert!(used as usize <= self.cap());
        self.used = used;
    }

    pub(crate) fn grow(&mut self, new_cap: usize) {
        debug_assert!(new_cap >= self.used());
        let mut buf = vec![0u8; new_cap].into_boxed_slice();
        buf[..self.used()].copy_from_slice(&self.buf[..self.used()]);
        self.buf = buf;
    }

    pub(crate) fn insert_at_with(
        &mut self,
        at: usize,
        layout: Layout,
        key: &[u8],
        value_len: usize,
        write_value: impl FnOnce(&mut [u8]),
    ) {
        let record_len = layout.record_len_of(key.len(), value_len);
        let used = self.used();
        debug_assert!(used + record_len <= self.cap());
        self.buf.copy_within(at..used, at + record_len);
        let value_start = layout.write_header_and_key(&mut self.buf, at, key, value_len);
        write_value(&mut self.buf[value_start..value_start + value_len]);
        self.used = (used + record_len) as u16;
    }

    pub(crate) fn remove_at(&mut self, at: usize, record_len: usize) {
        let used = self.used();
        self.buf.copy_within(at + record_len..used, at);
        self.used = (used - record_len) as u16;
    }
}
