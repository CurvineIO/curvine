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

use std::mem::MaybeUninit;
use crate::err_ucs;
use crate::io::IOResult;
use crate::sys::RawPtr;
use crate::ucp::bindings::*;
use crate::ucp::core::Endpoint;

// 远程内存访问的秘钥
pub struct RKey {
    inner: RawPtr<ucp_rkey>
}

impl RKey {
    pub fn unpack(endpoint: &Endpoint, rkey_buffer: &[u8]) -> IOResult<Self> {
        let mut inner = MaybeUninit::<*mut ucp_rkey>::uninit();
        let status = unsafe {
            ucp_ep_rkey_unpack(
                endpoint.as_mut_ptr(),
                rkey_buffer.as_ptr() as _,
                inner.as_mut_ptr(),
            )
        };
        err_ucs!(status)?;
        Ok(Self {
            inner: RawPtr::from_uninit(inner),
        })
    }


    pub fn as_ptr(&self) -> *const ucp_rkey {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_rkey {
        self.inner.as_mut_ptr()
    }
}

unsafe impl Send for RKey {}

unsafe impl Sync for RKey {}

impl Drop for RKey {
    fn drop(&mut self) {
        unsafe {
            ucp_rkey_destroy(self.as_mut_ptr());
        }
    }
}