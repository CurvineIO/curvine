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
use std::os::raw::c_void;
use std::sync::Arc;
use bytes::BytesMut;
use log::warn;
use crate::err_ucs;
use crate::io::IOResult;
use crate::sys::{RawPtr, RawVec};
use crate::ucp::bindings::*;
use crate::ucp::core::Context;
use crate::ucp::rma::RKeyBuffer;

pub struct Memory {
    inner: RawPtr<ucp_mem>,
    context: Arc<Context>,
    buffer: BytesMut,
}

impl Memory {
    pub fn new(context: Arc<Context>, buffer: BytesMut) -> IOResult<Self> {
        let params = ucp_mem_map_params_t {
            field_mask: (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_ADDRESS
                | ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_LENGTH)
                .0 as u64,
            address: buffer.as_ptr() as _,
            length: buffer.len() as _,
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };

        let mut inner  = MaybeUninit::<*mut ucp_mem>::uninit();
        let status = unsafe {
            ucp_mem_map(context.as_mut_ptr(), &params, inner.as_mut_ptr())
        };
        err_ucs!(status)?;

        Ok(Self {
            inner: RawPtr::from_uninit(inner),
            context,
            buffer,
        })
    }

    pub fn as_ptr(&self) -> *const ucp_mem {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_mem {
        self.inner.as_mut_ptr()
    }

    pub fn buffer_addr(&self) -> u64 {
        self.buffer.as_ptr() as u64
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }


    pub fn pack(&self) -> IOResult<RKeyBuffer> {
        let mut buf = MaybeUninit::<*mut c_void>::uninit();
        let mut len = MaybeUninit::<usize>::uninit();

        let status = unsafe {
            ucp_rkey_pack(
                self.context.as_mut_ptr(),
                self.as_mut_ptr(),
                buf.as_mut_ptr(),
                len.as_mut_ptr(),
            )
        };
        err_ucs!(status)?;

        let rkey_len = unsafe { len.assume_init() };
        let rkey_buf = unsafe { buf.assume_init() };
        let vec =  {
            RawVec::new(
                rkey_buf as _,
                rkey_len,
            )
        };
        Ok(RKeyBuffer::new(vec))
    }
}

impl Drop for Memory {
    fn drop(&mut self) {
        unsafe {
            let status = ucp_mem_unmap(self.context.as_mut_ptr(), self.as_mut_ptr());
            if let Err(e) = err_ucs!(status) {
                warn!("ucp_mem_unmap: {}", e);
            }
        }
    }
}
