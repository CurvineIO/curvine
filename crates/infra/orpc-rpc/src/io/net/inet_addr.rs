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

use crate::err_box;
use crate::io::net::NetUtils;
use crate::io::IOResult;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::net::{SocketAddr, ToSocketAddrs};
use std::vec::IntoIter;

// Create a socket address based on the hostname and port number.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct InetAddr {
    pub hostname: String,
    pub port: u16,
}

impl InetAddr {
    pub fn new<T: Into<String>>(hostname: T, port: u16) -> Self {
        Self {
            hostname: hostname.into(),
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

    pub fn from_str(addr: impl Into<String>) -> IOResult<Self> {
        let addr = addr.into();
        let list: Vec<&str> = addr.split(":").collect();
        if list.len() != 2 {
            return err_box!(
                "Address {} failed to resolve, format should be ip:port",
                addr
            );
        }
        let hostname = list[0].to_string();
        let port: u16 = list[1].parse()?;

        Ok(Self::new(hostname, port))
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
