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

//! Chunked variable-length record area.
//!
//! An ordered sequence of byte records kept sorted by a caller-supplied key and
//! spread over capacity-graded chunks. Every record is `[key_len][value_len]
//! [key][value]`, with the length prefixes sized by the area's [`Layout`].
//! The layout describes field widths, not the lengths of any particular record,
//! so one layout covers a whole area of variable-length records; business data
//! lives in the value bytes and the container owns framing, placement,
//! ordering and chunk management.
//!
//! This crate is the storage primitive behind `PackedChildren`; other
//! variable-length metadata (e.g. block lists or extended attributes on
//! `InodeFile`/`InodeDir`) can reuse the same chunking without copying the
//! split/compaction logic.
//!
//! # Key ordering
//!
//! Keys are compared as unsigned bytes (`slice::cmp`), so the area order is the
//! byte order of the encoded key. Integer keys must be encoded big-endian to
//! sort numerically. Byte order agrees with Unicode code point order for UTF-8
//! names, but not with the UTF-16 code unit order of Java `String.compareTo`: a
//! name in a supplementary plane (emoji, CJK extension B) sorts after
//! `U+E000..U+FFFF` here and before it there, so a readdir cursor produced by a
//! Java client cannot be compared against byte order for such names.
//!
//! # Contract for callers
//!
//! Record bytes are read through [`Record::chunks`] and edited through
//! [`Record::rewrite`] or [`Record::insert_with`], which hand out one value
//! slice per record so framing and ordering stay container-owned. Nothing
//! inside the area has a stable address: insert, remove and split all move
//! records within and between chunks, so a caller must not keep a pointer into
//! a chunk across a mutating call, nor erase the lifetime of such a slice by
//! casting it to a raw pointer.
//!
//! [`Record::insert_with`] zeroes the value slot before running the closure, so
//! a closure that writes only part of the slice leaves zero bytes rather than
//! the previous occupant of that memory. A key that is already present returns
//! [`InsertOutcome::Duplicate`] and the closure is not called at all. The
//! closure must not panic: the header is written and the following records are
//! already shifted when it runs, so a panic leaves the chunk framing
//! inconsistent.
//!
//! # Capacity ladder
//!
//! An empty area allocates nothing. Chunk capacities come from a [`CHUNK_MIN`]
//! byte ladder that doubles independently of the area's chunk size
//! ([`DEFAULT_CHUNK_SIZE`] unless [`Record::with_chunk_size`] picks another), so
//! `chunk_size` does not bound a chunk: growing stops only once a chunk reaches
//! `next_capacity(2 * chunk_size)`, which is `2 * chunk_size` when the chunk
//! size is itself a ladder step and below `4 * chunk_size` otherwise. A full
//! chunk splits at a record boundary near the middle, which bounds a single
//! in-chunk memmove by the chunk size instead of the area size; appending past
//! the end of the last chunk starts a new chunk instead of splitting.

mod chunk;
pub use self::chunk::Chunk;

mod layout;
pub use self::layout::Layout;

mod record;
pub use self::record::Record;

pub const CHUNK_MIN: usize = 64;

pub const DEFAULT_CHUNK_SIZE: usize = 4096;

#[must_use = "an insert that reports Duplicate stored nothing"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Duplicate,
}
