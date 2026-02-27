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

use orpc::common::Utils;
use orpc::io::net::InetAddr;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Node ID type for raft peers
pub type NodeId = u64;

/// Represents a raft peer address
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct RaftPeer {
    pub id: NodeId,
    pub hostname: String,
    pub port: u16,
}

impl RaftPeer {
    pub fn new<T: AsRef<str>>(id: NodeId, hostname: T, port: u16) -> Self {
        Self {
            id,
            hostname: hostname.as_ref().to_string(),
            port,
        }
    }

    pub fn from_addr<T: AsRef<str>>(hostname: T, port: u16) -> Self {
        let id = Self::create_id(format!("{}{}", hostname.as_ref(), port));
        Self::new(id, hostname, port)
    }

    fn create_id<T: AsRef<str>>(address: T) -> NodeId {
        (Utils::murmur3(address.as_ref().as_bytes())) as NodeId
    }

    pub fn to_addr(&self) -> InetAddr {
        InetAddr::new(self.hostname.clone(), self.port)
    }
}

impl Display for RaftPeer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}:{}", self.id, self.hostname, self.port)
    }
}

impl Default for RaftPeer {
    fn default() -> Self {
        Self {
            id: 0,
            hostname: "".to_string(),
            port: 0,
        }
    }
}
