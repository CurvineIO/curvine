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

use bytes::{Bytes, BytesMut};
use orpc::sys::DataSlice;

pub mod block;
pub mod file;
pub mod p2p;
pub mod rpc;
pub mod unified;

mod client_metrics;
pub use self::client_metrics::ClientMetrics;

pub const FILE_MIN_ALIGN_SIZE: usize = 4 * 1024;

#[derive(Debug)]
pub struct FileChunk {
    pub off: i64,
    pub data: DataSlice,
}

impl FileChunk {
    pub fn new(off: i64, data: DataSlice) -> Self {
        Self { off, data }
    }

    pub fn with_buf(off: i64, data: BytesMut) -> Self {
        Self {
            off,
            data: DataSlice::Buffer(data),
        }
    }

    pub fn with_bytes(off: i64, data: Bytes) -> Self {
        Self {
            off,
            data: DataSlice::Bytes(data),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            off: -1,
            data: DataSlice::Empty,
        }
    }
}

// [start, end)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileSlice {
    pub start: i64,
    pub end: i64,
}

impl FileSlice {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    pub fn len(&self) -> i64 {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
