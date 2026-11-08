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

//! Durable write watermark of a staging block.
//!
//! A worker flushes (fsyncs) a staging block on `write_flush`, but the flushed
//! length was not recorded anywhere. After an unclean shutdown the file size can
//! therefore include bytes that never reached the disk, so the recovered block
//! advertised a length whose tail was undefined.
//!
//! The watermark sidecar next to the staging file records the last length that
//! was successfully flushed. Recovery truncates the staging file back to it, so
//! a restarted block never advertises unsynced bytes. A missing or unreadable
//! sidecar is ignored: that keeps blocks written by older workers (and the SPDK
//! layout, which has no staging file) on the previous behavior.
//!
//! Encoded layout (little endian):
//!
//! | Field    | Size | Notes                        |
//! | -------- | ---- | ---------------------------- |
//! | magic    | 4    | `WATERMARK_MAGIC`            |
//! | version  | 2    | `WATERMARK_VERSION`          |
//! | reserved | 2    | must be 0                    |
//! | len      | 8    | durable bytes of the block   |
//! | crc      | 4    | CRC32C of the 16 bytes above |
//!
//! The trailing CRC makes a torn or partial write fail to decode instead of
//! truncating a healthy block to a garbage length.

use curvine_io::IOResult;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Suffix appended to the staging block path for its watermark sidecar.
pub const WATERMARK_FILE_SUFFIX: &str = ".wmk";

const WATERMARK_MAGIC: [u8; 4] = *b"CVWM";
const WATERMARK_VERSION: u16 = 1;
const WATERMARK_LEN: usize = 4 + 2 + 2 + 8 + 4;

/// Whether `path` is a watermark sidecar rather than a block file.
pub fn is_sidecar(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(WATERMARK_FILE_SUFFIX))
}

/// Sidecar path of `block_path`.
pub fn path_of(block_path: &Path) -> PathBuf {
    let mut name = block_path.as_os_str().to_os_string();
    name.push(WATERMARK_FILE_SUFFIX);
    PathBuf::from(name)
}

/// Path of the block a sidecar belongs to, `None` when `path` is not one.
pub fn block_path_of(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(WATERMARK_FILE_SUFFIX)?;
    if stem.is_empty() {
        return None;
    }
    Some(path.with_file_name(stem))
}

pub fn encode(len: u64) -> [u8; WATERMARK_LEN] {
    let mut buf = [0u8; WATERMARK_LEN];
    buf[0..4].copy_from_slice(&WATERMARK_MAGIC);
    buf[4..6].copy_from_slice(&WATERMARK_VERSION.to_le_bytes());
    buf[8..16].copy_from_slice(&len.to_le_bytes());
    let crc = crc32c::crc32c(&buf[..16]);
    buf[16..20].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Durable length stored in `buf`, `None` when the sidecar is truncated,
/// corrupted or from another format version.
pub fn decode(buf: &[u8]) -> Option<u64> {
    if buf.len() != WATERMARK_LEN || buf[0..4] != WATERMARK_MAGIC {
        return None;
    }
    if u16::from_le_bytes(buf[4..6].try_into().ok()?) != WATERMARK_VERSION {
        return None;
    }
    let crc = u32::from_le_bytes(buf[16..20].try_into().ok()?);
    if crc32c::crc32c(&buf[..16]) != crc {
        return None;
    }
    Some(u64::from_le_bytes(buf[8..16].try_into().ok()?))
}

/// Record `len` as the durable length of `block_path`.
///
/// The sidecar is synced before returning, so a crash after this call cannot
/// resurrect a shorter watermark.
pub fn write(block_path: &Path, len: u64) -> IOResult<()> {
    let path = path_of(block_path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    file.write_all(&encode(len))?;
    file.sync_all()?;
    Ok(())
}

/// Durable length recorded for `block_path`, `None` when there is no usable
/// sidecar.
pub fn read(block_path: &Path) -> Option<u64> {
    let buf = fs::read(path_of(block_path)).ok()?;
    decode(&buf)
}

/// Remove the sidecar of `block_path`, ignoring a missing file.
pub fn remove(block_path: &Path) -> IOResult<()> {
    match fs::remove_file(path_of(block_path)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Truncate `block_path` to its recorded watermark when the file is longer.
///
/// Returns the length the file was truncated to, `None` when nothing was done
/// (no sidecar, no watermark yet, or the file is not longer than the watermark).
pub fn apply(block_path: &Path) -> IOResult<Option<u64>> {
    let Some(watermark) = read(block_path) else {
        return Ok(None);
    };
    let file_len = fs::metadata(block_path)?.len();
    if file_len <= watermark {
        return Ok(None);
    }
    let file = OpenOptions::new().write(true).open(block_path)?;
    // Dropping the unsynced tail must itself be durable before the block is
    // advertised with the shorter length.
    file.set_len(watermark)?;
    file.sync_all()?;
    Ok(Some(watermark))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_block(name: &str) -> PathBuf {
        let dir = env::temp_dir().join("curvine-write-watermark-tests");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path_of(&path));
        path
    }

    #[test]
    fn encode_decode_round_trip() {
        for len in [0u64, 1, 4096, u64::MAX] {
            assert_eq!(decode(&encode(len)), Some(len));
        }
    }

    #[test]
    fn decode_rejects_torn_and_foreign_data() {
        let good = encode(1024);
        assert!(decode(&good).is_some());
        for i in 0..good.len() {
            let mut bad = good;
            bad[i] ^= 0x01;
            assert_eq!(decode(&bad), None, "flip at {}", i);
        }
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&good[..good.len() - 1]), None);
        assert_eq!(decode(&[0u8; WATERMARK_LEN]), None);
    }

    #[test]
    fn sidecar_naming_round_trips() {
        let block = Path::new("/data/staging/blk_42");
        let sidecar = path_of(block);
        assert_eq!(sidecar, PathBuf::from("/data/staging/blk_42.wmk"));
        assert!(is_sidecar(&sidecar));
        assert!(!is_sidecar(block));
        assert_eq!(block_path_of(&sidecar), Some(block.to_path_buf()));
        assert_eq!(block_path_of(block), None);
        assert_eq!(block_path_of(Path::new("/data/staging/.wmk")), None);
    }

    #[test]
    fn write_read_and_remove() -> IOResult<()> {
        let block = temp_block("blk_1");
        fs::write(&block, [7u8; 8])?;
        assert_eq!(read(&block), None);

        write(&block, 4)?;
        assert_eq!(read(&block), Some(4));

        remove(&block)?;
        assert_eq!(read(&block), None);
        // Removing twice is not an error.
        remove(&block)?;

        let _ = fs::remove_file(&block);
        Ok(())
    }

    #[test]
    fn apply_truncates_only_unsynced_tails() -> IOResult<()> {
        let block = temp_block("blk_2");
        fs::write(&block, [1u8; 10])?;

        // No watermark: leave the file alone.
        assert_eq!(apply(&block)?, None);
        assert_eq!(fs::metadata(&block)?.len(), 10);

        // Watermark at the file length: nothing to do.
        write(&block, 10)?;
        assert_eq!(apply(&block)?, None);

        // A longer file is truncated to the durable prefix.
        fs::write(&block, [2u8; 16])?;
        assert_eq!(apply(&block)?, Some(10));
        assert_eq!(fs::metadata(&block)?.len(), 10);

        // A watermark beyond the file is ignored.
        write(&block, 32)?;
        assert_eq!(apply(&block)?, None);
        assert_eq!(fs::metadata(&block)?.len(), 10);

        let _ = fs::remove_file(&block);
        remove(&block)?;
        Ok(())
    }
}
