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

#[derive(Debug, Clone, Default)]
pub struct ConnState {
    pub remote_addr: InetAddr,
    pub local_addr: InetAddr,
}

impl ConnState {
    pub fn new(remote_addr: InetAddr, local_addr: InetAddr) -> Self {
        Self {
            remote_addr,
            local_addr,
        }
    }
}
