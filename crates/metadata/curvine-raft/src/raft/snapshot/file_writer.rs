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

use crate::raft::{RaftResult, RaftUtils};
use curvine_core_error::err_box;
use curvine_io::LocalFile;
use curvine_runtime::common::Utils;
use flate2::read::ZlibDecoder;

// Decompress the compressed data block and write it to the file.
pub struct FileWriter {
    inner: LocalFile,
    chunk_size: usize,
    checksum: u64,
}

impl FileWriter {
    pub fn from_file<T: AsRef<str>>(file: T, chunk_size: usize) -> RaftResult<Self> {
        let inner = LocalFile::with_write(file, false)?;

        let writer = Self {
            inner,
            chunk_size,
            checksum: 0,
        };
        Ok(writer)
    }

    pub fn write_chunk(&mut self, chunk: &[u8]) -> RaftResult<()> {
        if chunk.is_empty() {
            return err_box!("Snapshot chunk is empty");
        }

        // The sender reads at most one configured chunk of raw bytes. Stop at
        // that size so a corrupt frame cannot expand without bound.
        let mut decoder = ZlibDecoder::new(chunk);
        let decompress_data = RaftUtils::zlib_read_limited(&mut decoder, self.chunk_size)?;
        if decompress_data.is_empty() {
            return err_box!(
                "Snapshot chunk decompressed to empty data, compressed_len {}",
                chunk.len()
            );
        }

        self.inner.write_all(&decompress_data)?;
        self.checksum += Utils::crc32(&decompress_data) as u64;

        Ok(())
    }

    pub fn checksum(&self) -> u64 {
        self.checksum
    }

    pub fn write_len(&self) -> u64 {
        self.inner.pos() as u64
    }
}
