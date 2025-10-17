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

use crate::ucp::bindings::*;
use std::mem;
use std::os::raw::c_void;

pub struct RequestParam {
    inner: ucp_request_param_t,
}

impl Default for RequestParam {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestParam {
    pub fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }

    pub fn as_ptr(&self) -> *const ucp_request_param_t {
        &self.inner as *const _
    }

    pub fn send_cb(mut self, cb: ucp_send_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed() };
        ty1.send = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn recv_tag_cb(mut self, cb: ucp_tag_recv_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed() };
        ty1.recv = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn recv_stream_cb(mut self, cb: ucp_stream_recv_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed() };
        ty1.recv_stream = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn recv_am_cb(mut self, cb: ucp_am_recv_data_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed() };
        ty1.recv_am = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn enable_iov(mut self) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32;
        self.inner.datatype = ucp_dt_type::UCP_DATATYPE_IOV as _;
        self
    }

    fn set_flag(&mut self) {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32;
    }

    pub fn set_flag_eager(mut self) -> Self {
        self.set_flag();
        self.inner.flags |= ucp_send_am_flags::UCP_AM_SEND_FLAG_EAGER.0;
        self
    }

    pub fn set_flag_rndv(mut self) -> Self {
        self.set_flag();
        self.inner.flags |= ucp_send_am_flags::UCP_AM_SEND_FLAG_RNDV.0;
        self
    }

    pub fn set_flag_reply(mut self) -> Self {
        self.set_flag();
        self.inner.flags |= ucp_send_am_flags::UCP_AM_SEND_FLAG_REPLY.0;
        self
    }

    pub fn user_data(mut self, user_data: *mut c_void) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32;
        self.inner.user_data = user_data;
        self
    }
}

unsafe impl Send for RequestParam {}

unsafe impl Sync for RequestParam {}
