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

use crate::common::Utils;
use crate::io::net::InetAddr;
use crate::io::IOResult;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct NodeAddr {
    // Node ID
    pub id: u64,
    // Node address.
    pub addr: InetAddr,
}

impl NodeAddr {
    pub fn new(id: u64, hostname: impl Into<String>, port: u16) -> Self {
        Self {
            id,
            addr: InetAddr::new(hostname.into(), port),
        }
    }

    pub fn from_addr(id: u64, addr: InetAddr) -> Self {
        Self::new(id, addr.hostname, addr.port)
    }

    pub fn from_str(addr: impl Into<String>) -> IOResult<Self> {
        let addr = InetAddr::from_str(addr)?;
        let id = Self::create_id(&addr.hostname, addr.port);

        Ok(Self { id, addr })
    }

    fn create_id(hostname: impl AsRef<str>, port: u16) -> u64 {
        let addr = format!("{}:{}", hostname.as_ref(), port);
        (Utils::murmur3(addr.as_bytes())) as u64
    }

    pub fn addr(&self) -> &InetAddr {
        &self.addr
    }
}

impl Display for NodeAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

impl Deref for NodeAddr {
    type Target = InetAddr;

    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

#[cfg(test)]
mod tests {
    use crate::io::net::NodeAddr;
    use crate::CommonResult;

    #[test]
    fn parse() -> CommonResult<()> {
        let addr = "127.0.0.1:1122";
        let addr = NodeAddr::from_str(addr)?;
        println!("addr = {}", addr);
        println!("addr = {:?}", addr);
        Ok(())
    }
}
