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

use std::net::SocketAddr;
use std::str::FromStr;
use crate::ucp::bindings::ucs_sock_addr;

#[derive(Debug)]
pub struct SockAddr {
    inner: socket2::SockAddr
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
}

impl From<&str> for SockAddr {
    fn from(value: &str) -> Self {
        let addr = SocketAddr::from_str(value).unwrap();
        let sockaddr = socket2::SockAddr::from(addr);
        Self { inner: sockaddr }
    }
}