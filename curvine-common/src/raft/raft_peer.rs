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

// RaftPeer is now defined in conf/peer.rs to avoid circular dependencies.
// This module is kept for backward compatibility but the types are re-exported
// from conf module in raft/mod.rs.
// See crate::conf::RaftPeer for the actual implementation.

use crate::conf::{JournalConf, RaftPeer};

/// Extension trait for RaftPeer to add server-specific functionality
#[allow(dead_code)]
pub trait RaftPeerExt {
    fn from_conf(conf: &JournalConf) -> RaftPeer;
}

impl RaftPeerExt for RaftPeer {
    fn from_conf(conf: &JournalConf) -> RaftPeer {
        RaftPeer::from_addr(conf.hostname.clone(), conf.rpc_port)
    }
}
