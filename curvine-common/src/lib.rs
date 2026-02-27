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

use crate::error::FsError;

pub mod conf;
pub mod error;
pub mod executor;
pub mod fs;
#[cfg(feature = "raft")]
pub mod raft;
#[cfg(feature = "raft")]
pub mod rocksdb;
pub mod state;
pub mod utils;
pub mod version;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/protos/proto.rs"));

    #[cfg(feature = "raft")]
    pub mod raft {
        include!(concat!(env!("OUT_DIR"), "/protos/raft.rs"));
    }
}

pub type FsResult<T> = Result<T, FsError>;

pub const FILE_BUFFER_SIZE: usize = 128 * 1024;
