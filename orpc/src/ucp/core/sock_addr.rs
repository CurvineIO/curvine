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

use crate::io::net::InetAddr;
use crate::ucp::bindings::{sockaddr_storage, ucs_sock_addr};
use crate::CommonError;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Debug)]
pub struct SockAddr {
    inner: socket2::SockAddr,
}

impl SockAddr {
    pub fn new(inner: socket2::SockAddr) -> Self {
        Self { inner }
    }

    pub fn as_ucs_sock_addr(&self) -> ucs_sock_addr {
        ucs_sock_addr {
            addr: self.inner.as_ptr() as _,
            addrlen: self.inner.len() as _,
        }
    }

    /// 转换为标准 SocketAddr（如果可能）
    pub fn as_socket_addr(&self) -> Option<SocketAddr> {
        self.inner.as_socket()
    }

    /// 获取 IP 地址字符串
    pub fn ip_string(&self) -> String {
        match self.inner.as_socket() {
            Some(addr) => addr.ip().to_string(),
            None => String::from("unknown"),
        }
    }

    /// 获取端口号
    pub fn port(&self) -> u16 {
        match self.inner.as_socket() {
            Some(addr) => addr.port(),
            None => 0,
        }
    }

    pub fn to_inet_addr(&self) -> InetAddr {
        InetAddr::new(self.ip_string(), self.port())
    }
}

impl From<sockaddr_storage> for SockAddr {
    fn from(value: sockaddr_storage) -> Self {
        let addr =  unsafe {
            socket2::SockAddr::new(std::mem::transmute(value), 8)
        };
        Self::new(addr)
    }
}

impl From<&str> for SockAddr {
    fn from(value: &str) -> Self {
        let addr = SocketAddr::from_str(value).unwrap();
        let sockaddr = socket2::SockAddr::from(addr);
        Self::new(sockaddr)
    }
}

impl TryFrom<&InetAddr> for SockAddr {
    type Error = CommonError;

    fn try_from(value: &InetAddr) -> Result<Self, Self::Error> {
        let addr = SocketAddr::from_str(value.to_string().as_str())?;
        let sockaddr = socket2::SockAddr::from(addr);
        Ok(Self::new(sockaddr))
    }
}

impl fmt::Display for SockAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.inner.as_socket() {
            Some(addr) => write!(f, "{}", addr),
            None => write!(f, "unknown"),
        }
    }
}
