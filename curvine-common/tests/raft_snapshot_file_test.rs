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

use curvine_common::raft::snapshot::{FileReader, FileWriter};
use orpc::common::{FileUtils, Utils};
use orpc::io::LocalFile;
use orpc::CommonResult;

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
