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

pub use curvine_io::{
    BlockDevice, BlockIO, CacheManager, DataSlice, IOError, IOResult, LocalFile, ReadAheadTask,
};

// `io_error` and `block_io` were public module paths before the `curvine-io`
// extraction, so they stay reachable here even though `curvine-io` only
// re-exports the items.
pub mod io_error {
    pub use curvine_io::IOError;
}

pub mod block_io {
    pub use curvine_io::{BlockDevice, BlockIO};
}

pub mod net;
pub mod retry;

pub mod spdk_conf;
pub use self::spdk_conf::{BdevInfo, NvmeTarget, SpdkConf};
