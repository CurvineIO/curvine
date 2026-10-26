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
use crate::{Chunk, InsertOutcome};
use crate::{CHUNK_MIN, DEFAULT_CHUNK_SIZE};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};

pub struct Record {
    layout: Layout,
    chunk_size: usize,
    chunks: Vec<Chunk>,
    len: u32,
}

impl Clone for Record {
    fn clone(&self) -> Self {
        Self {
            layout: self.layout,
            chunk_size: self.chunk_size,
            chunks: self.chunks.clone(),
            len: self.len,
        }
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new(Layout::default())
    }
}

impl Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("layout", &self.layout)
            .field("chunk_size", &self.chunk_size)
            .field("len", &self.len)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

impl Record {
    pub fn new(layout: Layout) -> Self {
        Self::with_chunk_size(layout, DEFAULT_CHUNK_SIZE)
    }

    pub(crate) fn next_capacity(min: usize) -> usize {
        let mut cap = CHUNK_MIN;
        while cap < min {
            cap *= 2;
        }
        cap
    }

    pub fn with_chunk_size(layout: Layout, chunk_size: usize) -> Self {
        assert!(
            chunk_size >= CHUNK_MIN,
            "chunk size {} is below the {} byte minimum",
            chunk_size,
            CHUNK_MIN
        );

        assert!(
            chunk_size <= u32::MAX as usize / 8,
            "chunk size {} leaves no headroom for a u32 used counter",
            chunk_size
        );

        Self {
            layout,
            chunk_size,
            chunks: Vec::new(),
            len: 0,
        }
    }

    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    #[inline]
    pub(crate) fn record_len(&self, buf: &[u8], off: usize) -> usize {
        self.layout.record_len(buf, off)
    }

    #[inline]
    pub(crate) fn key_at<'a>(&self, buf: &'a [u8], off: usize) -> &'a [u8] {
        &buf[self.layout.key_range(buf, off)]
    }

    #[inline]
    pub(crate) fn value_at<'a>(&self, buf: &'a [u8], off: usize) -> &'a [u8] {
        &buf[self.layout.value_range(buf, off)]
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let (ci, off) = self.find(key).ok()?;
        Some(self.value_at(self.chunks[ci].buf(), off))
    }

    pub fn find(&self, key: &[u8]) -> Result<(usize, usize), (usize, usize)> {
        if self.chunks.is_empty() {
            return Err((0, 0));
        }

        let lo = self
            .chunks
            .partition_point(|c| self.key_at(c.buf(), 0) <= key);
        if lo == 0 {
            return Err((0, 0));
        }

        let ci = lo - 1;
        let chunk = &self.chunks[ci];
        let buf = chunk.buf();
        let used = buf.len();

        let mut off = 0usize;
        while off < used {
            match self.key_at(buf, off).cmp(key) {
                Ordering::Equal => return Ok((ci, off)),
                Ordering::Greater => return Err((ci, off)),
                Ordering::Less => off += self.record_len(buf, off),
            }
        }
        Err((ci, used))
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> InsertOutcome {
        self.insert_with(key, value.len(), |dst| dst.copy_from_slice(value))
    }

    pub fn insert_with(
        &mut self,
        key: &[u8],
        value_len: usize,
        write_value: impl FnOnce(&mut [u8]),
    ) -> InsertOutcome {
        assert!(
            key.len() <= self.layout.max_key_len(),
            "key of {} bytes exceeds a {}-byte key length field",
            key.len(),
            self.layout.key_len_width()
        );

        assert!(
            value_len <= self.layout.max_value_len(),
            "value of {} bytes exceeds a {}-byte value length field",
            value_len,
            self.layout.value_len_width()
        );

        let layout = self.layout;
        let record_len = layout.record_len_of(key.len(), value_len);
        assert!(
            record_len <= self.chunk_size,
            "record of {} bytes exceeds chunk size {}",
            record_len,
            self.chunk_size
        );

        if self.chunks.is_empty() {
            let mut chunk = Chunk::new(Self::next_capacity(record_len));
            chunk.insert_at_with(0, layout, key, value_len, write_value);
            self.chunks.push(chunk);
            self.len += 1;
            #[cfg(debug_assertions)]
            self.debug_assert_record(0, 0, key, value_len);
            return InsertOutcome::Inserted;
        }

        let (mut ci, mut po) = match self.find(key) {
            Ok(_) => return InsertOutcome::Duplicate,
            Err(pos) => pos,
        };

        loop {
            let chunk = &self.chunks[ci];
            if chunk.used() + record_len <= chunk.cap() {
                break;
            }
            if po == chunk.used() && ci + 1 == self.chunks.len() && chunk.cap() >= self.chunk_size {
                let mut tail = Chunk::new(Self::next_capacity(record_len));
                tail.insert_at_with(0, layout, key, value_len, write_value);
                self.chunks.push(tail);
                self.len += 1;
                #[cfg(debug_assertions)]
                self.debug_assert_record(self.chunks.len() - 1, 0, key, value_len);
                return InsertOutcome::Inserted;
            }
            let single_record = chunk.used() == layout.record_len(chunk.buf(), 0);
            if chunk.cap() < self.chunk_size || single_record {
                let needed = Self::next_capacity(chunk.used() + record_len);
                self.chunks[ci].grow(needed);
                break;
            }
            self.split(ci);
            (ci, po) = match self.find(key) {
                Ok(_) => return InsertOutcome::Duplicate,
                Err(pos) => pos,
            };
        }

        self.chunks[ci].insert_at_with(po, layout, key, value_len, write_value);
        self.len += 1;
        #[cfg(debug_assertions)]
        self.debug_assert_record(ci, po, key, value_len);
        InsertOutcome::Inserted
    }

    fn split(&mut self, ci: usize) {
        let layout = self.layout;
        let used = self.chunks[ci].used();
        let mut split = 0usize;
        let mut off = 0usize;
        while off < used {
            let next = off + layout.record_len(self.chunks[ci].buf(), off);
            if next > used / 2 {
                break;
            }
            split = next;
            off = next;
        }
        if split == 0 {
            split = layout.record_len(self.chunks[ci].buf(), 0);
        }
        let tail = used - split;
        assert!(
            split > 0 && tail > 0,
            "split must not create an empty chunk"
        );

        let mut new_chunk = Chunk::new(Self::next_capacity(tail));
        new_chunk.buf_mut()[..tail].copy_from_slice(&self.chunks[ci].buf()[split..used]);
        new_chunk.set_used(tail);

        let old_cap = self.chunks[ci].cap();
        let prefix_cap = Self::next_capacity(split);
        if prefix_cap < old_cap {
            let mut prefix = Chunk::new(prefix_cap);
            prefix.buf_mut()[..split].copy_from_slice(&self.chunks[ci].buf()[..split]);
            prefix.set_used(split);
            self.chunks[ci] = prefix;
        } else {
            self.chunks[ci].set_used(split);
        }

        self.chunks.insert(ci + 1, new_chunk);
    }

    pub fn remove(&mut self, ci: usize, off: usize) {
        assert!(self.len > 0, "remove on an empty record area");
        assert!(
            ci < self.chunks.len(),
            "chunk index {} is out of range for {} chunks",
            ci,
            self.chunks.len()
        );

        let layout = self.layout;
        let used = self.chunks[ci].used();
        assert!(
            off < used,
            "offset {} is past the {} used bytes of chunk {}",
            off,
            used,
            ci
        );

        let mut boundary = 0usize;
        while boundary < off {
            boundary += layout.record_len(self.chunks[ci].buf(), boundary);
        }
        assert!(
            boundary == off,
            "offset {} is not a record boundary in chunk {}",
            off,
            ci
        );

        let record_len = layout.record_len(self.chunks[ci].buf(), off);
        assert!(
            off + record_len <= used,
            "record at offset {} of chunk {} runs past the {} used bytes",
            off,
            ci,
            used
        );

        self.chunks[ci].remove_at(off, record_len);
        if self.chunks[ci].used() == 0 {
            self.chunks.remove(ci);
        }
        self.len -= 1;
    }

    pub fn rewrite(&mut self, mut f: impl FnMut(&mut [u8])) {
        let layout = self.layout;
        for chunk in self.chunks.iter_mut() {
            let used = chunk.used();
            let mut off = 0usize;
            while off < used {
                let range = layout.value_range(chunk.buf(), off);
                off = range.end;
                f(&mut chunk.buf_mut()[range]);
            }
        }
    }

    #[inline]
    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    #[inline]
    pub fn allocated_bytes(&self) -> usize {
        self.chunks.iter().map(Chunk::cap).sum()
    }

    #[cfg(debug_assertions)]
    fn debug_assert_record(&self, ci: usize, off: usize, key: &[u8], value_len: usize) {
        let buf = self.chunks[ci].buf();
        debug_assert_eq!(
            self.layout.key_len(buf, off),
            key.len(),
            "key length prefix disagrees with the key written"
        );
        debug_assert_eq!(
            self.layout.value_len(buf, off),
            value_len,
            "value length prefix disagrees with the value written"
        );
    }
}
