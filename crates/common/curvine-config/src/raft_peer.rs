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

use curvine_net::net::InetAddr;
use curvine_runtime::common::Utils;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Display, Formatter};

pub type NodeId = u64;

fn deserialize_trimmed_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Ok(value.trim().to_string())
}

// Represents a raft address
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct RaftPeer {
    pub id: NodeId,
    #[serde(default, deserialize_with = "deserialize_trimmed_string")]
    pub hostname: String,
    pub port: u16,
}

impl RaftPeer {
    pub fn new<T: AsRef<str>>(id: NodeId, hostname: T, port: u16) -> Self {
        Self {
            id,
            hostname: hostname.as_ref().trim().to_string(),
            port,
        }
    }

    pub fn from_addr<T: AsRef<str>>(hostname: T, port: u16) -> Self {
        let hostname = hostname.as_ref().trim();
        let id = Self::create_id(format!("{}{}", hostname, port));
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

#[cfg(test)]
mod tests {
    use super::RaftPeer;
    use curvine_net::net::InetAddr;

    #[test]
    fn serde_trims_hostname() {
        let peer: RaftPeer = toml::from_str(
            r#"
                id = 1
                hostname = " master1 "
                port = 8996
            "#,
        )
        .unwrap();

        assert_eq!(peer.hostname, "master1");
        assert_eq!(peer.to_addr().hostname, "master1");
        assert_eq!(peer.to_addr(), InetAddr::new(" master1 ", 8996));
    }
}
