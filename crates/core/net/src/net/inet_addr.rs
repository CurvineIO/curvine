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

#![allow(clippy::should_implement_trait)]

use crate::net::NetUtils;
use curvine_io::IOResult;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Display, Formatter};
use std::net::{SocketAddr, ToSocketAddrs};
use std::vec::IntoIter;

fn deserialize_trimmed_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Ok(value.trim().to_string())
}

// Create a socket address based on the hostname and port number.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct InetAddr {
    #[serde(default, deserialize_with = "deserialize_trimmed_string")]
    pub hostname: String,
    pub port: u16,
}

impl InetAddr {
    pub fn new<T: Into<String>>(hostname: T, port: u16) -> Self {
        Self {
            hostname: hostname.into().trim().to_string(),
            port,
        }
    }

    // Get a local address
    pub fn local(port: u16) -> Self {
        let host_name = NetUtils::local_hostname();
        Self::new(host_name, port)
    }

    // Resolve the address.
    pub fn resolved(&self) -> IOResult<IntoIter<SocketAddr>> {
        let iter = self.to_socket_addrs()?;
        Ok(iter)
    }

    pub fn as_pair(&self) -> (&str, u16) {
        (self.hostname.as_str(), self.port)
    }

    pub fn from_str(addr: impl AsRef<str>) -> IOResult<Self> {
        let addr = addr.as_ref().trim();
        match addr.split_once(':') {
            Some((hostname, port)) => {
                let hostname = hostname.trim();
                let port = port.trim();
                if hostname.is_empty() || port.is_empty() || port.contains(':') {
                    return err_box!(
                        "Address {} failed to resolve, format should be host:port",
                        addr
                    );
                }
                Ok(Self::new(hostname, port.parse()?))
            }
            None => err_box!(
                "Address {} failed to resolve, format should be host:port",
                addr
            ),
        }
    }

    /// Parse a comma-separated `host:port` list, trimming whitespace around
    /// each token. Empty tokens (from trailing or doubled commas) are skipped.
    pub fn parse_list(addrs: impl AsRef<str>) -> IOResult<Vec<Self>> {
        let mut result = Vec::new();
        for node in addrs.as_ref().split(',') {
            let node = node.trim();
            if node.is_empty() {
                continue;
            }
            result.push(Self::from_str(node)?);
        }
        if result.is_empty() {
            return err_box!("Address list is empty, format should be host:port[,host:port...]");
        }
        Ok(result)
    }
}

impl ToSocketAddrs for InetAddr {
    type Iter = IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        self.as_pair().to_socket_addrs()
    }
}

impl Display for InetAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.hostname, self.port)
    }
}

impl From<SocketAddr> for InetAddr {
    fn from(value: SocketAddr) -> Self {
        Self::new(value.ip().to_string(), value.port())
    }
}

#[cfg(test)]
mod tests {
    use super::InetAddr;

    #[test]
    fn from_str_trims_hostname_and_port() {
        let addr = InetAddr::from_str(" curvine-master-02.oppo.local:8995 ").unwrap();
        assert_eq!(addr.hostname, "curvine-master-02.oppo.local");
        assert_eq!(addr.port, 8995);
    }

    #[test]
    fn new_trims_hostname() {
        let addr = InetAddr::new(" curvine-master-02.oppo.local ", 8995);
        assert_eq!(addr.hostname, "curvine-master-02.oppo.local");
        assert_eq!(addr.port, 8995);
    }

    #[test]
    fn parse_list_trims_mixed_comma_spacing() {
        let addrs = InetAddr::parse_list(
            "curvine-master-01.oppo.local:8995, curvine-master-02.oppo.local:8995,curvine-master-03.oppo.local:8995",
        )
        .unwrap();
        assert_eq!(
            addrs
                .iter()
                .map(|addr| addr.hostname.as_str())
                .collect::<Vec<_>>(),
            [
                "curvine-master-01.oppo.local",
                "curvine-master-02.oppo.local",
                "curvine-master-03.oppo.local"
            ]
        );
        assert!(addrs.iter().all(|addr| addr.port == 8995));
    }

    #[test]
    fn parse_list_skips_empty_tokens() {
        let addrs = InetAddr::parse_list("host-a:1,, host-b:2, ").unwrap();
        assert_eq!(addrs[0], InetAddr::new("host-a", 1));
        assert_eq!(addrs[1], InetAddr::new("host-b", 2));
    }
}
