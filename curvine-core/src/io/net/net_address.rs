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

use crate::{try_err, try_option, CommonResult};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::net::{Ipv4Addr, ToSocketAddrs};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetAddress {
    hostname: String,
    ipv4: String,
    ipv6: String,
    port: u16,
}

impl NetAddress {
    pub fn with_v4<T: AsRef<str>>(hostname: T, ip: T, port: u16) -> Self {
        let ipv4 = ip.as_ref().to_owned();
        let ipv6 = Ipv4Addr::from_str(&ipv4)
            .map(|x| x.to_string())
            .unwrap_or("".to_string());

        NetAddress {
            hostname: hostname.as_ref().to_owned(),
            ipv4,
            ipv6,
            port,
        }
    }

    pub fn with_host<T: AsRef<str>>(host: T, port: u16) -> CommonResult<Self> {
        let hostname = format!("{}:{}", host.as_ref(), port);
        // Try to get one ipv4 address.
        let ip = try_err!(hostname.to_socket_addrs())
            .filter(|x| x.is_ipv4())
            .next();
        let ipv4 = try_option!(ip).ip().to_string();

        Ok(Self::with_v4(host.as_ref(), ipv4.as_ref(), port))
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn ip(&self) -> &str {
        if !self.ipv4.is_empty() {
            &self.ipv4
        } else {
            &self.ipv6
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn ip_v4(&self) -> &str {
        &self.ipv4
    }

    pub fn ip_v6(&self) -> &str {
        &self.ipv6
    }

    pub fn addr_v4(&self) -> String {
        let host = if !self.hostname.is_empty() {
            &self.hostname
        } else {
            &self.ipv4
        };
        format!("{}:{}", host, self.port)
    }
}

impl Display for NetAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}:{}", self.hostname, self.ip(), self.port)
    }
}
