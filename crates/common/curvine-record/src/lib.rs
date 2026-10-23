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
//! Contract for callers: record bytes are read through [`Record::chunks`] and
//! edited through [`Record::rewrite`], which hands out one value slice per
//! record so framing and ordering stay container-owned. Nothing inside the area
//! has a stable address, so callers must not hand out pointers into a chunk.
//!
//! Capacity ladder: an empty area allocates nothing, the first chunk is
//! [`CHUNK_MIN`] bytes and doubles up to the area's chunk size
//! ([`DEFAULT_CHUNK_SIZE`] unless [`Record::with_chunk_size`] picks another).
//! A full chunk splits at a record boundary near the middle, which bounds a
//! single in-chunk memmove by the chunk size instead of the area size. A chunk
//! holding a single record grows up to `2 * chunk_size` before it can split.

mod chunk;
pub use self::chunk::Chunk;

mod layout;
pub use self::layout::Layout;

mod record;
pub use self::record::Record;

pub const CHUNK_MIN: usize = 64;

pub const DEFAULT_CHUNK_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Duplicate,
}
