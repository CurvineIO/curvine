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

use bytes::Bytes;
use sha2::{Digest, Sha256};

mod cache_manager;
pub use self::cache_manager::{
    CacheGetResult, CacheGetResultTag, CacheManager, CacheSnapshot, CachedChunk,
};

mod discovery;
pub use self::discovery::{DiscoveryService, DiscoverySnapshot};

mod transfer;
pub use self::transfer::{ChunkId, TransferRequest, TransferResponse};

mod service;
pub use self::service::{P2pReadTraceContext, P2pService, P2pState, P2pStatsSnapshot};

fn sha256_bytes(data: &[u8]) -> Bytes {
    let mut hasher = Sha256::new();
    hasher.update(data);
    Bytes::from(hasher.finalize().to_vec())
}
