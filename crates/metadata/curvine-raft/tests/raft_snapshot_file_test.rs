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

use curvine_core_error::CommonResult;
use curvine_io::LocalFile;
use curvine_raft::raft::snapshot::{FileReader, FileWriter};
use curvine_runtime::common::{FileUtils, Utils};

#[test]
fn test_raft_snapshot_file_read_write_with_checksum_validation() -> CommonResult<()> {
    let (file, checksum) = create_file()?;

    let mut reader = FileReader::from_file(&file, 0, 1024)?;
    let write_file = Utils::test_file();
    let mut writer = FileWriter::from_file(&write_file, 1024)?;

    loop {
        let chunk = reader.read_chunk()?;
        if chunk.is_empty() {
            break;
        } else {
            writer.write_chunk(&chunk[..])?;
        }
    }

    println!(
        "file checksum {}, read checksum {}, write checksum {}",
        checksum,
        reader.checksum(),
        writer.checksum()
    );

    assert_eq!(checksum, reader.checksum());
    assert_eq!(checksum, writer.checksum());
    assert_eq!(writer.write_len(), 100 * 1024);

    drop(reader);
    drop(writer);

    FileUtils::delete_path(file, false)?;
    FileUtils::delete_path(write_file, false)?;

    Ok(())
}

/// Incompressible bytes expand under zlib. The old `len + 256` output buffer
/// is smaller than that expansion for a 1 MiB chunk, which used to truncate
/// the stream and fail the receiver checksum.
#[test]
fn test_raft_snapshot_incompressible_chunk_roundtrip() -> CommonResult<()> {
    let file = Utils::test_file();
    let chunk_size = 1024 * 1024;
    // Two full chunks plus a short tail, similar to the 66-byte shortfall.
    let total = chunk_size * 2 + 66;
    let mut raw = vec![0u8; total];
    let mut state = 0x1234_5678_9abc_def0u64;
    for byte in &mut raw {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (state >> 33) as u8;
    }

    let mut checksum = 0u64;
    {
        let mut file_writer = LocalFile::with_write(&file, true)?;
        file_writer.write_all(&raw)?;
        checksum += Utils::crc32(&raw[..chunk_size]) as u64;
        checksum += Utils::crc32(&raw[chunk_size..chunk_size * 2]) as u64;
        checksum += Utils::crc32(&raw[chunk_size * 2..]) as u64;
    }

    let mut reader = FileReader::from_file(&file, 0, chunk_size)?;
    let write_file = Utils::test_file();
    let mut writer = FileWriter::from_file(&write_file, chunk_size)?;
    let mut chunks = 0;
    loop {
        let chunk = reader.read_chunk()?;
        if chunk.is_empty() {
            break;
        }
        chunks += 1;
        if chunks <= 2 {
            assert!(
                chunk.len() > chunk_size + 256,
                "compressed chunk {} is not larger than the old buffer",
                chunk.len()
            );
        }
        writer.write_chunk(&chunk[..])?;
    }

    assert_eq!(chunks, 3);
    assert_eq!(checksum, reader.checksum());
    assert_eq!(checksum, writer.checksum());
    assert_eq!(writer.write_len(), total as u64);

    drop(reader);
    drop(writer);
    FileUtils::delete_path(file, false)?;
    FileUtils::delete_path(write_file, false)?;
    Ok(())
}

fn create_file() -> CommonResult<(String, u64)> {
    let file = Utils::test_file();

    let mut checksum = 0;

    let mut writer = LocalFile::with_write(&file, true)?;
    for _ in 0..100 {
        let str = Utils::rand_str(1024);
        writer.write_all(str.as_bytes())?;
        checksum += Utils::crc32(str.as_bytes()) as u64;
    }

    Ok((file, checksum))
}
