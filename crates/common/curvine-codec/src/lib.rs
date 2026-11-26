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

//! Curvine's compact binary codec for persisted metadata records.
//!
//! This crate replaces bincode for metadata (meta DB values) and journal
//! payloads, pairing a frozen field encoding with a per-record schema version
//! that governs future evolution between codec-format binaries.
//!
//! The bincode-to-codec switch is a one-time hard cutover: journal entries in
//! the raft log and meta DB values written by the old bincode paths are not
//! read back, and the legacy decode fallbacks (`MountInfo::decode_persisted`,
//! the pre-protobuf journal replay schema) were removed together with their
//! compat tests. Existing bincode data has to be converted offline before the
//! switch (or the metadata rebuilt); the schema version in the record header
//! does not bridge the bincode-to-codec transition.
//!
//! # Field encoding (frozen once released)
//!
//! - Integers: fixed-width little-endian (`u8..u64`, `i8..i64`).
//! - `bool`: 1 byte, only `0`/`1` accepted on read.
//! - `f32`/`f64`: IEEE 754 bit pattern, little-endian.
//! - `String`/`&str`/`[u8]`/`Vec<T>`: `u32` LE length prefix followed by the
//!   content (`String` bytes must be valid UTF-8; the reader validates).
//! - `Option<T>`: 1 byte tag (`0` = `None`, `1` = `Some`) followed by `T`.
//! - `[u8]` slices use the shared `u32` prefix; `[u8; N]`/`[T; N]` write `N`
//!   elements with no prefix (the length is pinned by the type).
//! - `HashMap`/`BTreeMap`: `u32` LE pair count, then key/value pairs.
//!   `HashMap` iteration order is unspecified, so its bytes are not
//!   reproducible; persisted records use `BTreeMap` (sorted key order) when
//!   byte stability matters (golden tests, checksums, journal diffs).
//! - Enums: the discriminant is pinned by each `Encode`/`Decode` impl and
//!   validated on read.
//! - `usize`/`isize` are intentionally not implemented (platform-dependent
//!   width); `char` is not needed so far.
//!
//! # Persisted record header
//!
//! Records written through [`Codec::encode`] are laid out as
//! `[header 1B][payload]` where the single header byte packs the magic and the
//! schema version:
//!
//! ```text
//! bit:  7  6 | 5  4  3  2  1  0
//!       MAGIC | SCHEMA_VERSION
//! ```
//!
//! - `MAGIC` (2 bits) is `0b10`. Most legacy bincode payloads start outside
//!   that range (`InodeView` starts with a low enum tag, and short string
//!   length prefixes stay small), but journal batches are the exception: see
//!   the sniffing note below.
//! - `SCHEMA_VERSION` (6 bits) is `1..=63`; `0` is invalid.
//!
//! [`Codec::is_codec_format`] is a payload-detection helper for tests and
//! tooling only; no production read path dispatches on it. That is
//! deliberate: the check is a heuristic — a legacy `JournalBatch` whose
//! `seq_id` low byte lands in `0x81..=0xBF` (63 of 256 candidates, ~1 in
//! 4 batches) passes it — so sniffing could not have supported a reliable
//! bincode-to-codec rolling upgrade even before the fallbacks were removed
//! (see the hard-cutover note above). A false positive is caught only when
//! the remaining bytes disagree with the target layout; a payload that
//! happens to fit decodes without an error. Callers must decide the format
//! from external context (journal switch point, column-family marker) before
//! decoding.
//!
//! [`Codec::decode`] always parses that header first (see
//! [`Codec::parse_version`]) and hands the recovered version to the payload
//! decoder through [`Reader::version`]. [`Codec::encode_unversioned`] writes a
//! headerless payload for layout tests only; its output is not a record.
//!
//! `Vec<u8>` is copied in bulk and shares the layout of `Writer::put_bytes`, so
//! byte-valued fields need no hand-written impls; other bulk byte fields can call
//! `put_bytes` / `get_bytes` directly for the same single-copy layout.
//!
//! # Compatibility rules
//!
//! - Field encodings are frozen once released; evolution only appends fields.
//! - Writers stamp records with the schema version they encode; decoders gate
//!   appended fields with `reader.version() >= N` (or [`Reader::has_version`])
//!   and fill in defaults for older records. Both `Writer` and `Reader` always
//!   carry a version.
//! - Persisted records avoid `HashMap` fields: iteration order is unspecified,
//!   so bytes are not reproducible. Use `BTreeMap` when order matters.
//! - Reads strictly reject trailing bytes.
//!
//! # Writing impls for persisted types
//!
//! Enums pin their discriminant by hand and freeze it under a golden-bytes
//! test; appended fields gate on the record's schema version.

#![forbid(unsafe_code)]

mod error;
pub use error::*;

mod impls;

mod reader;
pub use self::reader::Reader;

mod writer;
pub use self::writer::Writer;

mod codec;
pub use codec::*;

pub const MAGIC: u8 = 0b10;

pub const MAX_VERSION: u8 = 0b11_1111;

pub const CURRENT_VERSION: u8 = 1;

pub const INIT_CAPACITY: usize = 512;

/// Upper bound on decoder pre-allocation. Length prefixes are capped against
/// the remaining input and reservations are scaled down by the element size;
/// this absolute cap backs both up for hostile payloads.
pub const MAX_ALLOC: usize = 4096;

pub type CodecResult<T> = Result<T, CodecError>;
