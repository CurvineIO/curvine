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
    used: u32,
    last_off: u32,
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
            last_off: 0,
        }
    }

    #[inline]
    pub fn buf(&self) -> &[u8] {
        &self.buf[..self.used()]
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
    pub(crate) fn last_off(&self) -> usize {
        self.last_off as usize
    }

    pub(crate) fn set_used(&mut self, used: usize, layout: Layout) {
        debug_assert!(used <= self.cap());
        self.used = used as u32;
        self.last_off = if used == 0 {
            0
        } else {
            self.last_record_off(layout) as u32
        };
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
        assert!(
            used + record_len <= self.cap(),
            "record of {} bytes does not fit the {} free bytes of a {} byte chunk",
            record_len,
            self.cap() - used,
            self.cap()
        );
        self.buf.copy_within(at..used, at + record_len);
        let value_start = layout.write_header_and_key(&mut self.buf, at, key, value_len);
        let value_end = value_start + value_len;
        self.buf[value_start..value_end].fill(0);
        write_value(&mut self.buf[value_start..value_end]);
        self.last_off = if at >= used {
            at
        } else {
            self.last_off() + record_len
        } as u32;
        self.used = (used + record_len) as u32;
    }

    pub(crate) fn remove_at(&mut self, at: usize, record_len: usize, layout: Layout) {
        let used = self.used();
        self.buf.copy_within(at + record_len..used, at);
        self.used = (used - record_len) as u32;
        let last = self.last_off();
        self.last_off = if at == last {
            self.last_record_off(layout)
        } else {
            last - record_len
        } as u32;
    }

    fn last_record_off(&self, layout: Layout) -> usize {
        let used = self.used();
        let mut off = 0usize;
        loop {
            let next = off + layout.record_len(&self.buf, off);
            if next >= used {
                return off;
            }
            off = next;
        }
    }
}
