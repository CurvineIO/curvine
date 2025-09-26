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

use crate::err_ucs;
use crate::io::IOResult;
use crate::sys::RawPtr;
use crate::ucp::bindings::*;
use crate::ucp::SockAddr;
use std::mem::MaybeUninit;

pub struct ConnRequest {
    inner: RawPtr<ucp_conn_request>,
}

impl ConnRequest {
    pub fn new(ptr: ucp_conn_request_h) -> Self {
        Self {
            inner: RawPtr::from_raw(ptr),
        }
    }
    pub fn as_ptr(&self) -> *const ucp_conn_request {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_conn_request {
        self.inner.as_mut_ptr()
    }

    pub fn remote_addr(&self) -> IOResult<SockAddr> {
        let mut attr = ucp_conn_request_attr {
            field_mask: ucp_conn_request_attr_field::UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR.0
                as u64,
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        let status = unsafe { ucp_conn_request_query(self.as_mut_ptr(), &mut attr) };
        err_ucs!(status)?;

        let sock_addr =
            unsafe { socket2::SockAddr::new(std::mem::transmute(attr.client_address), 8) };
        Ok(SockAddr::new(sock_addr))
    }
}

unsafe impl Send for ConnRequest {}
