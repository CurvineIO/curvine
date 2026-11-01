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

//! Per-chunk CRC32C checksums for worker block data.
//!
//! A block is split into fixed-size chunks; the last chunk may be shorter.
//! Each chunk has one CRC32C. The encoded form is stored in a sidecar file
//! next to the block file (see `CHECKSUM_FILE_SUFFIX`).
//!
//! Encoded layout (little endian):
//!
//! | Field      | Size          | Notes                               |
//! | ---------- | ------------- | ----------------------------------- |
//! | magic      | 4             | `CHECKSUM_MAGIC`                    |
//! | version    | 2             | `CHECKSUM_VERSION`                  |
//! | reserved   | 2             | must be 0                           |
//! | chunk_size | 4             | bytes per chunk, > 0                |
//! | data_len   | 8             | covered block bytes                 |
//! | crcs       | 4 * chunks    | `chunks = ceil(data_len/chunk_size)` |
//! | trailer    | 4             | CRC32C of all preceding bytes       |
//!
//! The trailer makes a torn or zeroed sidecar fail to decode instead of
//! producing false mismatches against valid block data.

use curvine_core_error::err_box;
use curvine_io::IOResult;
use std::ops::Range;

/// Default chunk size (64 KiB). Matches common read sizes so a typical read
/// verifies a whole number of chunks.
pub const DEFAULT_CHECKSUM_CHUNK_SIZE: u32 = 64 * 1024;

/// Suffix appended to the block file path for its checksum sidecar.
pub const CHECKSUM_FILE_SUFFIX: &str = ".crc";

const CHECKSUM_MAGIC: [u8; 4] = *b"CVCK";
const CHECKSUM_VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 2 + 4 + 8;
const CRC_LEN: usize = 4;

/// CRC32C of `data`.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Per-chunk CRC32C checksums of one block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockChecksum {
    chunk_size: u32,
    data_len: u64,
    crcs: Vec<u32>,
}

impl BlockChecksum {
    /// Compute checksums for `data` split into `chunk_size` chunks.
    ///
    /// This walks the whole buffer in a separate pass. Use it to re-derive a
    /// checksum from bytes already on disk or in memory (sidecar rebuild,
    /// tests). On the write path, feed data through [`ChecksumBuilder`] as it
    /// is written instead.
    pub fn compute(chunk_size: u32, data: &[u8]) -> IOResult<Self> {
        let mut builder = ChecksumBuilder::new(chunk_size)?;
        builder.update(data);
        Ok(builder.finish())
    }

    /// Build from already computed chunk CRCs. `crcs.len()` must equal
    /// `chunk_count(data_len, chunk_size)`.
    pub fn from_parts(chunk_size: u32, data_len: u64, crcs: Vec<u32>) -> IOResult<Self> {
        check_chunk_size(chunk_size)?;
        let expected = chunk_count(data_len, chunk_size);
        if crcs.len() as u64 != expected {
            return err_box!(
                "checksum count mismatch: data_len={}, chunk_size={}, expected={}, got={}",
                data_len,
                chunk_size,
                expected,
                crcs.len()
            );
        }
        Ok(Self {
            chunk_size,
            data_len,
            crcs,
        })
    }

    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    pub fn crcs(&self) -> &[u32] {
        &self.crcs
    }

    pub fn chunk_count(&self) -> u64 {
        self.crcs.len() as u64
    }

    /// Byte range `[start, end)` covered by chunk `index`, or `None` when out
    /// of range. The last chunk may be shorter than `chunk_size`.
    pub fn chunk_range(&self, index: u64) -> Option<Range<u64>> {
        if index >= self.chunk_count() {
            return None;
        }
        let start = index * self.chunk_size as u64;
        let end = (start + self.chunk_size as u64).min(self.data_len);
        Some(start..end)
    }

    /// Indexes of chunks overlapping the byte range `[offset, offset + len)`.
    /// The range is clamped to `data_len`; an empty or out-of-range request
    /// yields an empty range.
    ///
    /// The result includes the first and last chunks even when the request
    /// covers them only partly. A CRC covers a whole chunk, so verifying a
    /// read that starts or ends inside a chunk requires reading that full
    /// chunk (see [`Self::chunk_range`]) before calling [`Self::verify_chunk`].
    pub fn chunks_for_range(&self, offset: u64, len: u64) -> Range<u64> {
        let end = offset.saturating_add(len).min(self.data_len);
        if offset >= end {
            return 0..0;
        }
        let chunk = self.chunk_size as u64;
        (offset / chunk)..end.div_ceil(chunk)
    }

    /// Check `data` against chunk `index`. `data` must be the full chunk.
    pub fn verify_chunk(&self, index: u64, data: &[u8]) -> IOResult<()> {
        let Some(range) = self.chunk_range(index) else {
            return err_box!(
                "checksum chunk {} out of range, chunk_count={}",
                index,
                self.chunk_count()
            );
        };
        let expected_len = range.end - range.start;
        if data.len() as u64 != expected_len {
            return err_box!(
                "checksum chunk {} length mismatch: expected={}, got={}",
                index,
                expected_len,
                data.len()
            );
        }
        let expected = self.crcs[index as usize];
        let actual = crc32c(data);
        if expected != actual {
            return err_box!(
                "checksum mismatch at chunk {} [{}, {}): expected={:#010x}, actual={:#010x}",
                index,
                range.start,
                range.end,
                expected,
                actual
            );
        }
        Ok(())
    }

    /// Size in bytes of the encoded form.
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.crcs.len() * CRC_LEN + CRC_LEN
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        buf.extend_from_slice(&CHECKSUM_MAGIC);
        buf.extend_from_slice(&CHECKSUM_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&self.chunk_size.to_le_bytes());
        buf.extend_from_slice(&self.data_len.to_le_bytes());
        for crc in &self.crcs {
            buf.extend_from_slice(&crc.to_le_bytes());
        }
        let trailer = crc32c(&buf);
        buf.extend_from_slice(&trailer.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> IOResult<Self> {
        if buf.len() < HEADER_LEN + CRC_LEN {
            return err_box!("checksum data too short: {} bytes", buf.len());
        }
        let (body, trailer) = buf.split_at(buf.len() - CRC_LEN);
        let expected_trailer = u32::from_le_bytes(trailer.try_into().unwrap());
        let actual_trailer = crc32c(body);
        if expected_trailer != actual_trailer {
            return err_box!(
                "checksum data corrupted: trailer expected={:#010x}, actual={:#010x}",
                expected_trailer,
                actual_trailer
            );
        }

        if body[0..4] != CHECKSUM_MAGIC {
            return err_box!("invalid checksum magic: {:?}", &body[0..4]);
        }
        let version = u16::from_le_bytes(body[4..6].try_into().unwrap());
        if version != CHECKSUM_VERSION {
            return err_box!("unsupported checksum version: {}", version);
        }
        let reserved = u16::from_le_bytes(body[6..8].try_into().unwrap());
        if reserved != 0 {
            return err_box!("invalid checksum reserved field: {}", reserved);
        }
        let chunk_size = u32::from_le_bytes(body[8..12].try_into().unwrap());
        let data_len = u64::from_le_bytes(body[12..20].try_into().unwrap());
        check_chunk_size(chunk_size)?;

        let (crc_chunks, rest) = body[HEADER_LEN..].as_chunks::<CRC_LEN>();
        if !rest.is_empty() {
            return err_box!("invalid checksum body length: {}", body.len() - HEADER_LEN);
        }
        let crcs = crc_chunks.iter().map(|c| u32::from_le_bytes(*c)).collect();
        Self::from_parts(chunk_size, data_len, crcs)
    }
}

/// Incremental per-chunk CRC32C computation for data written sequentially
/// from block offset 0. Input slices may cross chunk boundaries arbitrarily.
///
/// This is the intended write-path API: calling [`Self::update`] from the
/// write loop computes CRCs while the bytes are already being touched, with no
/// second pass over the data as [`BlockChecksum::compute`] would need.
#[derive(Debug, Clone)]
pub struct ChecksumBuilder {
    chunk_size: u32,
    data_len: u64,
    crcs: Vec<u32>,
    /// CRC of the current partial chunk.
    partial_crc: u32,
    /// Bytes in the current partial chunk.
    partial_len: u32,
}

impl ChecksumBuilder {
    pub fn new(chunk_size: u32) -> IOResult<Self> {
        check_chunk_size(chunk_size)?;
        Ok(Self {
            chunk_size,
            data_len: 0,
            crcs: Vec::new(),
            partial_crc: 0,
            partial_len: 0,
        })
    }

    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Bytes consumed so far.
    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    pub fn update(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let room = (self.chunk_size - self.partial_len) as usize;
            let take = room.min(data.len());
            let (head, rest) = data.split_at(take);
            self.partial_crc = crc32c::crc32c_append(self.partial_crc, head);
            self.partial_len += take as u32;
            self.data_len += take as u64;
            if self.partial_len == self.chunk_size {
                self.crcs.push(self.partial_crc);
                self.partial_crc = 0;
                self.partial_len = 0;
            }
            data = rest;
        }
    }

    pub fn finish(mut self) -> BlockChecksum {
        if self.partial_len > 0 {
            self.crcs.push(self.partial_crc);
        }
        BlockChecksum {
            chunk_size: self.chunk_size,
            data_len: self.data_len,
            crcs: self.crcs,
        }
    }
}

/// Number of chunks needed to cover `data_len` bytes.
pub fn chunk_count(data_len: u64, chunk_size: u32) -> u64 {
    if chunk_size == 0 {
        return 0;
    }
    data_len.div_ceil(chunk_size as u64)
}

/// Sidecar checksum path for a block file path.
pub fn checksum_path(block_path: &str) -> String {
    format!("{}{}", block_path, CHECKSUM_FILE_SUFFIX)
}

fn check_chunk_size(chunk_size: u32) -> IOResult<()> {
    if chunk_size == 0 {
        return err_box!("checksum chunk size must be > 0");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 + 7) as u8).collect()
    }

    #[test]
    fn crc32c_matches_known_vector() {
        // RFC 3720 B.4: 32 bytes of zeros.
        assert_eq!(crc32c(&[0u8; 32]), 0x8a91_36aa);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn compute_splits_on_chunk_boundaries() {
        let data = sample(10);
        let sum = BlockChecksum::compute(4, &data).unwrap();
        assert_eq!(sum.data_len(), 10);
        assert_eq!(sum.chunk_count(), 3);
        assert_eq!(
            sum.crcs(),
            &[
                crc32c(&data[0..4]),
                crc32c(&data[4..8]),
                crc32c(&data[8..10])
            ]
        );
        assert_eq!(sum.chunk_range(2), Some(8..10));
        assert_eq!(sum.chunk_range(3), None);
    }

    #[test]
    fn compute_exact_multiple_has_no_partial_chunk() {
        let data = sample(8);
        let sum = BlockChecksum::compute(4, &data).unwrap();
        assert_eq!(sum.chunk_count(), 2);
        assert_eq!(sum.chunk_range(1), Some(4..8));
    }

    #[test]
    fn compute_empty_data_has_no_chunks() {
        let sum = BlockChecksum::compute(4, &[]).unwrap();
        assert_eq!(sum.data_len(), 0);
        assert_eq!(sum.chunk_count(), 0);
        assert_eq!(sum.chunks_for_range(0, 10), 0..0);
    }

    #[test]
    fn builder_matches_compute_for_any_split() {
        let data = sample(1000);
        let expected = BlockChecksum::compute(64, &data).unwrap();
        for step in [1usize, 3, 63, 64, 65, 128, 999, 1000] {
            let mut builder = ChecksumBuilder::new(64).unwrap();
            for part in data.chunks(step) {
                builder.update(part);
            }
            assert_eq!(builder.data_len(), 1000);
            assert_eq!(builder.finish(), expected, "step={}", step);
        }
    }

    #[test]
    fn zero_chunk_size_is_rejected() {
        assert!(BlockChecksum::compute(0, b"abc").is_err());
        assert!(ChecksumBuilder::new(0).is_err());
        assert!(BlockChecksum::from_parts(0, 0, vec![]).is_err());
        assert_eq!(chunk_count(10, 0), 0);
    }

    #[test]
    fn from_parts_rejects_wrong_crc_count() {
        assert!(BlockChecksum::from_parts(4, 10, vec![1, 2]).is_err());
        assert!(BlockChecksum::from_parts(4, 10, vec![1, 2, 3, 4]).is_err());
        assert!(BlockChecksum::from_parts(4, 10, vec![1, 2, 3]).is_ok());
    }

    #[test]
    fn chunks_for_range_covers_partial_overlaps() {
        let sum = BlockChecksum::compute(4, &sample(10)).unwrap();
        assert_eq!(sum.chunks_for_range(0, 4), 0..1);
        assert_eq!(sum.chunks_for_range(3, 2), 0..2);
        assert_eq!(sum.chunks_for_range(4, 4), 1..2);
        assert_eq!(sum.chunks_for_range(5, 100), 1..3);
        assert_eq!(sum.chunks_for_range(9, 1), 2..3);
        assert_eq!(sum.chunks_for_range(10, 1), 0..0);
        assert_eq!(sum.chunks_for_range(2, 0), 0..0);
        assert_eq!(sum.chunks_for_range(u64::MAX, u64::MAX), 0..0);
    }

    #[test]
    fn verify_chunk_detects_single_byte_flip() {
        let mut data = sample(10);
        let sum = BlockChecksum::compute(4, &data).unwrap();
        for i in 0..sum.chunk_count() {
            let r = sum.chunk_range(i).unwrap();
            sum.verify_chunk(i, &data[r.start as usize..r.end as usize])
                .unwrap();
        }

        data[5] ^= 0x01;
        assert!(sum.verify_chunk(1, &data[4..8]).is_err());
        assert!(sum.verify_chunk(0, &data[0..4]).is_ok());
    }

    #[test]
    fn verify_chunk_detects_zeroed_data() {
        let data = sample(8);
        let sum = BlockChecksum::compute(4, &data).unwrap();
        assert!(sum.verify_chunk(0, &[0u8; 4]).is_err());
    }

    #[test]
    fn verify_chunk_rejects_bad_index_or_length() {
        let data = sample(10);
        let sum = BlockChecksum::compute(4, &data).unwrap();
        assert!(sum.verify_chunk(3, &data[8..10]).is_err());
        assert!(sum.verify_chunk(2, &data[8..9]).is_err());
        assert!(sum.verify_chunk(0, &data[0..3]).is_err());
    }

    #[test]
    fn encode_decode_round_trip() {
        for len in [0usize, 1, 4, 10, 4096] {
            let sum = BlockChecksum::compute(4, &sample(len)).unwrap();
            let buf = sum.encode();
            assert_eq!(buf.len(), sum.encoded_len());
            assert_eq!(BlockChecksum::decode(&buf).unwrap(), sum, "len={}", len);
        }
    }

    #[test]
    fn decode_rejects_corrupted_input() {
        let sum = BlockChecksum::compute(4, &sample(10)).unwrap();
        let buf = sum.encode();

        // Any single-byte flip fails the trailer check.
        for i in 0..buf.len() {
            let mut bad = buf.clone();
            bad[i] ^= 0x80;
            assert!(BlockChecksum::decode(&bad).is_err(), "flip at {}", i);
        }

        // Truncated, empty, and all-zero sidecars.
        assert!(BlockChecksum::decode(&buf[..buf.len() - 1]).is_err());
        assert!(BlockChecksum::decode(&[]).is_err());
        assert!(BlockChecksum::decode(&vec![0u8; buf.len()]).is_err());
    }

    #[test]
    fn decode_rejects_valid_trailer_with_bad_header() {
        fn reseal(mut body: Vec<u8>) -> Vec<u8> {
            let trailer = crc32c(&body);
            body.extend_from_slice(&trailer.to_le_bytes());
            body
        }
        let sum = BlockChecksum::compute(4, &sample(10)).unwrap();
        let encoded = sum.encode();
        let body = encoded[..encoded.len() - CRC_LEN].to_vec();

        let mut bad_magic = body.clone();
        bad_magic[0] = b'X';
        assert!(BlockChecksum::decode(&reseal(bad_magic)).is_err());

        let mut bad_version = body.clone();
        bad_version[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert!(BlockChecksum::decode(&reseal(bad_version)).is_err());

        let mut bad_reserved = body.clone();
        bad_reserved[6] = 1;
        assert!(BlockChecksum::decode(&reseal(bad_reserved)).is_err());

        let mut zero_chunk = body.clone();
        zero_chunk[8..12].copy_from_slice(&0u32.to_le_bytes());
        assert!(BlockChecksum::decode(&reseal(zero_chunk)).is_err());

        let mut wrong_len = body.clone();
        wrong_len[12..20].copy_from_slice(&100u64.to_le_bytes());
        assert!(BlockChecksum::decode(&reseal(wrong_len)).is_err());

        let mut ragged = body;
        ragged.push(0);
        assert!(BlockChecksum::decode(&reseal(ragged)).is_err());
    }

    #[test]
    fn checksum_path_appends_suffix() {
        assert_eq!(checksum_path("/data/blk_1"), "/data/blk_1.crc");
    }
}
