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
use futures::task::AtomicWaker;
use std::future::Future;
use std::mem;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::os::raw::c_void;
use std::pin::Pin;
use std::task::Poll;
use crate::err_ucs;
use crate::io::IOResult;
use crate::sys::RawPtr;
use crate::ucp::SockAddr;

#[derive(Default)]
pub struct RequestWaker(AtomicWaker);

impl RequestWaker {
    pub unsafe extern "C" fn init(request: *mut c_void) {
        (request as *mut Self).write(RequestWaker::default());
    }

    pub unsafe extern "C" fn cleanup(request: *mut c_void) {
        std::ptr::drop_in_place(request as *mut Self)
    }
}

impl Deref for RequestWaker {
    type Target = AtomicWaker;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
            unsafe { socket2::SockAddr::new(mem::transmute(attr.client_address), 8) };
        Ok(SockAddr::new(sock_addr))
    }
}

unsafe impl Send for ConnRequest {}

pub struct RequestFuture<T> {
    ptr: ucs_status_ptr_t,
    poll_fn: unsafe fn(ucs_status_ptr_t) -> Poll<T>,
}

impl<T> RequestFuture<T> {
    pub fn new(ptr: ucs_status_ptr_t, poll_fn: unsafe fn(ucs_status_ptr_t) -> Poll<T>) -> Self {
        Self { ptr, poll_fn }
    }
}

unsafe impl<T> Send for RequestFuture<T> {}

unsafe impl<T> Sync for RequestFuture<T> {}

impl<T> Future for RequestFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        let ret = unsafe { (self.poll_fn)(self.ptr) };
        match ret {
            Poll::Ready(v) => Poll::Ready(v),
            Poll::Pending => {
                let request = unsafe { &mut *(self.ptr as *mut RequestWaker) };
                request.register(cx.waker());
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for RequestFuture<T> {
    fn drop(&mut self) {
        unsafe { ucp_request_free(self.ptr as _) };
    }
}

pub struct RequestParam {
    inner: ucp_request_param_t
}

impl RequestParam {
    pub fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() }
        }
    }

    pub fn as_ptr(&self) -> *const ucp_request_param_t {
        &self.inner as *const _
    }

    pub fn send_cb(mut self, cb: ucp_send_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed()};
        ty1.send = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn recv_tag_cb(mut self, cb: ucp_tag_recv_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed()};
        ty1.recv = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn recv_stream_cb(mut self, cb: ucp_stream_recv_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed()};
        ty1.recv_stream = cb;
        self.inner.cb = ty1;

        self
    }

    pub fn recv_am_cb(mut self, cb: ucp_am_recv_data_nbx_callback_t) -> Self {
        self.inner.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;

        let mut ty1: ucp_request_param_t__bindgen_ty_1 = unsafe { mem::zeroed()};
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
}