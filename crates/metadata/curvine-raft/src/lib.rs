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

pub mod conf;
pub mod raft;
pub mod utils;

pub mod rocksdb {
    pub use curvine_rocksdb::*;
}

pub mod proto {
    pub mod raft {
        include!(concat!(env!("OUT_DIR"), "/protos/raft.rs"));
    }
}

pub use curvine_error::{FsError, FsResult, MAX_FILE_SIZE};

impl From<crate::raft::RaftError> for FsError {
    fn from(value: crate::raft::RaftError) -> Self {
        Self::Raft(curvine_core_error::ErrorImpl::with_source(
            value.to_string().into(),
        ))
    }
}
