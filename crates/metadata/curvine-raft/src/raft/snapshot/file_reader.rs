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
use bytes::BytesMut;
use curvine_core_error::CommonResult;
use curvine_io::LocalFile;
use curvine_runtime::common::Utils;
use flate2::read::ZlibEncoder;
use flate2::Compression;

// Read a file and read it after compression.
pub struct FileReader {
    inner: LocalFile,
    read_buf: BytesMut,
    chunk_size: usize,
    checksum: u64,
}

impl FileReader {
    pub fn from_file<T: AsRef<str>>(file: T, offset: u64, chunk_size: usize) -> RaftResult<Self> {
        let inner = LocalFile::with_read(file, offset)?;

        let reader = Self {
            inner,
            read_buf: BytesMut::with_capacity(chunk_size),
            checksum: 0,
            chunk_size,
        };
        Ok(reader)
    }

    pub fn read_chunk(&mut self) -> CommonResult<BytesMut> {
        // Read file data.
        let remaining = self.inner.len() - self.inner.pos();
        if remaining <= 0 {
            return Ok(BytesMut::new());
        }

        let len = self.chunk_size.min(remaining as usize);
        self.read_buf.reserve(len);
        unsafe {
            self.read_buf.set_len(len);
        }

        // Read data of the specified size of the file.
        let mut data = self.read_buf.split_to(len);
        self.inner.read_all(&mut data)?;
        self.checksum += Utils::crc32(&data) as u64;

        // Incompressible bytes can grow. compressBound is large enough for any
        // zlib stream of this input, so the read cannot truncate a valid chunk.
        let mut encoder = ZlibEncoder::new(&data[..], Compression::default());
        let max_len = RaftUtils::zlib_compress_bound(len);
        Ok(RaftUtils::zlib_read_limited(&mut encoder, max_len)?)
    }

    pub fn checksum(&self) -> u64 {
        self.checksum
    }

    pub fn has_remaining(&self) -> bool {
        self.inner.len() - self.inner.pos() > 0
    }

    pub fn len(&self) -> i64 {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
