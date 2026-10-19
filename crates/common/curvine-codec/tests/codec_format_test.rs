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

//! Format-discrimination tests against real bincode payloads.
//!
//! These tests pin down how the codec header magic separates new records from
//! the bincode-era payloads still living in meta DB and journal logs, and how
//! sniffing false positives fail loudly instead of decoding silently.

use curvine_codec::{
    Codec, CodecError, CodecResult, Decode, Encode, Reader, Writer, CURRENT_VERSION, MAGIC,
};
use serde::Serialize;

/// Stand-in for the bincode-encoded `InodeView` stored as meta DB values: an
/// enum whose variant tag bincode writes as a `u32` LE discriminant, so the
/// first byte is always small (0x00..=0x02 for File/Dir/FileEntry).
#[derive(Serialize)]
enum LegacyInode {
    File,
    Dir,
    FileEntry,
}

/// Stand-in for the bincode-encoded `JournalBatch` journal payload: the first
/// field is the monotonically increasing `seq_id: u64`, whose low byte sweeps
/// the whole byte range.
#[derive(Serialize)]
struct LegacyBatch {
    seq_id: u64,
    count: u32,
}

/// Stand-in for the bincode-encoded `MountInfo` in CF_COMMON: the first field
/// is a string, so bincode writes its `u64` LE length prefix first.
#[derive(Serialize)]
struct LegacyMount {
    cv_path: String,
}

#[test]
fn legacy_inode_payloads_are_not_codec_format() {
    for variant in [LegacyInode::File, LegacyInode::Dir, LegacyInode::FileEntry] {
        let bytes = bincode::serialize(&variant).unwrap();
        assert!(!Codec::is_codec_format(&bytes), "variant bytes {:?}", bytes);
    }
}

#[test]
fn legacy_mount_payloads_are_not_codec_format() {
    // Typical mount paths stay far below 128 bytes, so the u64 length prefix
    // has small leading bytes.
    for path in ["cv", "/data", "/mnt/curvine/longer/path/with/nested/dirs"] {
        let bytes = bincode::serialize(&LegacyMount {
            cv_path: path.to_string(),
        })
        .unwrap();
        assert!(!Codec::is_codec_format(&bytes), "path {}", path);
    }
}

#[test]
fn legacy_journal_seq_id_false_positive_fails_loudly() {
    // seq_id 129 has low byte 0x81: exactly magic 0b10 + version 1. This is
    // the documented sniffing overlap with legacy journal batches (bytes
    // 0x81..=0xBF, 63 in 256 candidates, ~1 in 4); deterministic dispatch
    // will use journal context instead. What the codec guarantees is that
    // such a false positive never decodes silently.
    let bytes = bincode::serialize(&LegacyBatch {
        seq_id: 129,
        count: 0,
    })
    .unwrap();
    assert_eq!(bytes[0], 0x81);
    assert!(Codec::is_codec_format(&bytes));

    // Payload has 11 bytes after the header: a u64 reads 8 and leaves 3
    // trailing bytes, so the decode must fail instead of returning garbage.
    let result: CodecResult<u64> = Codec::decode(&bytes);
    assert!(result.is_err());

    // The 12-byte legacy batch (u64 + u32) can never be reinterpreted as a
    // complete codec value: a String payload would read a u32 length prefix
    // of 0 and leave 7 trailing bytes behind.
    let result: CodecResult<String> = Codec::decode(&bytes);
    assert!(result.is_err());
}

#[test]
fn legacy_journal_normal_seq_ids_are_not_codec_format() {
    for seq_id in [0u64, 1, 2, 63, 64, 127, 192, 255, 256, 1000] {
        let bytes = bincode::serialize(&LegacyBatch { seq_id, count: 0 }).unwrap();
        // seq_id low byte 0x80..=0xBF (128..=191) is the only overlap window.
        let in_overlap = (128..=191).contains(&(seq_id & 0xFF));
        assert_eq!(
            Codec::is_codec_format(&bytes),
            in_overlap,
            "seq_id {}",
            seq_id
        );
    }
}

/// Hand-written impl showcase: this is the pattern real types (InodeView,
/// JournalBatch, MountInfo) will follow when migrating off bincode.
struct GoldenRecord {
    id: u64,
    name: String,
    tags: Vec<String>,
    flag: Option<u32>,
}

impl Encode for GoldenRecord {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_u64(self.id)?;
        writer.put_str(&self.name)?;
        writer.put_len(self.tags.len())?;
        for tag in &self.tags {
            writer.put_str(tag)?;
        }
        match self.flag {
            Some(value) => {
                writer.put_u8(1)?;
                writer.put_u32(value)?;
            }
            None => writer.put_u8(0)?,
        }
        Ok(())
    }
}

impl Decode for GoldenRecord {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let id = reader.get_u64()?;
        let name = reader.get_str()?;
        let len = reader.take_len()?;
        let mut tags = Vec::with_capacity(len);
        for _ in 0..len {
            tags.push(reader.get_str()?);
        }
        let flag = match reader.get_bool()? {
            true => Some(reader.get_u32()?),
            false => None,
        };
        Ok(Self {
            id,
            name,
            tags,
            flag,
        })
    }
}

fn golden_record() -> GoldenRecord {
    GoldenRecord {
        id: 1,
        name: String::from("cv"),
        tags: vec![String::from("a")],
        flag: Some(7),
    }
}

/// The exact bytes of a bare payload. This assertion is a tripwire: the
/// encoding is frozen, so any diff here is a compatibility break.
#[test]
fn golden_record_bare_bytes_are_frozen() {
    let bytes = Codec::encode_unversioned(&golden_record()).unwrap();
    assert_eq!(
        bytes.as_slice(),
        &[
            // id: u64 LE
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
            // name: u32 len + "cv"
            0x02, 0x00, 0x00, 0x00, b'c', b'v', //
            // tags: u32 len + one string "a"
            0x01, 0x00, 0x00, 0x00, //
            0x01, 0x00, 0x00, 0x00, b'a', //
            // flag: Some tag + u32 LE
            0x01, //
            0x07, 0x00, 0x00, 0x00,
        ]
    );

    let mut reader = Reader::new(&bytes, CURRENT_VERSION).unwrap();
    let decoded = GoldenRecord::decode(&mut reader).unwrap();
    assert_eq!(decoded.id, 1);
    assert_eq!(decoded.name, "cv");
    assert_eq!(decoded.tags, vec![String::from("a")]);
    assert_eq!(decoded.flag, Some(7));
}

#[test]
fn golden_record_persisted_bytes_are_frozen() {
    let bytes = Codec::encode(CURRENT_VERSION, &golden_record()).unwrap();
    assert_eq!(bytes[0], (MAGIC << 6) | CURRENT_VERSION);
    assert_eq!(bytes[0], 0x81);
    assert_eq!(bytes.len(), 1 + 28);

    let decoded: GoldenRecord = Codec::decode(&bytes).unwrap();
    assert_eq!(decoded.id, 1);
    assert_eq!(decoded.name, "cv");
    assert_eq!(decoded.tags, vec![String::from("a")]);
    assert_eq!(decoded.flag, Some(7));
}

/// Captures what `Reader::version()` hands to a `Decode` impl, proving the
/// persisted header's version reaches field-level gating logic.
struct CapturedVersion(u8);

impl Decode for CapturedVersion {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(CapturedVersion(reader.version()))
    }
}

#[test]
fn decode_exposes_schema_version() {
    // Header-only record: version 1, empty payload.
    let bytes = [(MAGIC << 6) | 1];
    assert_eq!(Codec::parse_version(&bytes).unwrap(), 1);
    let decoded: CapturedVersion = Codec::decode(&bytes).unwrap();
    assert_eq!(decoded.0, 1);
}

/// Persisted metadata types are typically enums: the discriminant is pinned
/// by hand and frozen under a golden-bytes assertion.
enum EntryKind {
    File,
    Dir,
}

impl Encode for EntryKind {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_u8(match self {
            EntryKind::File => 0,
            EntryKind::Dir => 1,
        })
    }
}

impl Decode for EntryKind {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        match reader.get_u8()? {
            0 => Ok(EntryKind::File),
            1 => Ok(EntryKind::Dir),
            other => Err(CodecError::decode(format!("invalid EntryKind tag {other}"))),
        }
    }
}

#[test]
fn enum_discriminant_bytes_are_frozen() {
    let file = Codec::encode(CURRENT_VERSION, &EntryKind::File).unwrap();
    assert_eq!(&file[1..], &[0x00]);
    let dir = Codec::encode(CURRENT_VERSION, &EntryKind::Dir).unwrap();
    assert_eq!(&dir[1..], &[0x01]);
    // Unknown discriminants are rejected on read.
    let mut bad = Codec::encode(CURRENT_VERSION, &EntryKind::File).unwrap();
    bad[1] = 0x02;
    let result: CodecResult<EntryKind> = Codec::decode(&bad);
    assert!(result.is_err());
}

/// Version-gating sample: `ttl_ms` is appended in schema version 2; v1
/// records omit it and decode with the default. Real types copy this pattern
/// for their first schema evolution.
struct GatedRecord {
    id: u64,
    ttl_ms: Option<u64>,
}

impl Encode for GatedRecord {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_u64(self.id)?;
        if writer.version() >= 2 {
            self.ttl_ms.encode(writer)?;
        }
        Ok(())
    }
}

impl Decode for GatedRecord {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let id = reader.get_u64()?;
        let ttl_ms = if reader.version() >= 2 {
            Option::<u64>::decode(reader)?
        } else {
            None
        };
        Ok(Self { id, ttl_ms })
    }
}

#[test]
fn version_gated_field_round_trip() {
    let record = GatedRecord {
        id: 7,
        ttl_ms: Some(70),
    };

    // A schema-v2 payload records the appended field; the v2 reader sees it.
    let mut writer = Writer::new(2).unwrap();
    record.encode(&mut writer).unwrap();
    let bytes = writer.into_inner();
    assert_eq!(bytes.len(), 8 + 1 + 8);
    let mut reader = Reader::new(&bytes, 2).unwrap();
    let decoded = GatedRecord::decode(&mut reader).unwrap();
    assert_eq!(decoded.id, 7);
    assert_eq!(decoded.ttl_ms, Some(70));

    // A v1 payload has no appended field: the default (None) is filled in.
    let mut writer = Writer::new(1).unwrap();
    record.encode(&mut writer).unwrap();
    let bytes = writer.into_inner();
    assert_eq!(bytes.len(), 8);
    let mut reader = Reader::new(&bytes, 1).unwrap();
    let decoded = GatedRecord::decode(&mut reader).unwrap();
    assert_eq!(decoded.id, 7);
    assert_eq!(decoded.ttl_ms, None);
}
