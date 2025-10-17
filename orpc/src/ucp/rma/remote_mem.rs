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

use crate::ucp::bindings::ucp_rkey;
use crate::ucp::rma::RKey;

pub struct RemoteMem {
    ep_id: u64,
    addr: u64,
    len: usize,
    rkey: RKey,
}

impl RemoteMem {
    pub fn new(ep_id: u64, addr: u64, len: usize, rkey: RKey) -> Self {
        RemoteMem {
            ep_id,
            addr,
            len,
            rkey,
        }
    }

    pub fn addr(&self) -> u64 {
        self.addr
    }

    pub fn rkey(&self) -> &RKey {
        &self.rkey
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn rkey_mut_ptr(&self) -> *mut ucp_rkey {
        self.rkey.as_mut_ptr()
    }

    pub fn ep_id(&self) -> u64 {
        self.ep_id
    }
}
