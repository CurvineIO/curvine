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

use std::future::Future;
use std::os::raw::c_void;
use std::pin::Pin;
use std::task::Poll;
use futures::future::ok;
use futures::task::AtomicWaker;
use crate::err_ucs;
use crate::io::IOResult;
use crate::ucp::bindings::{ucp_request_check_status, ucp_request_free, ucp_stream_recv_request_test, ucs_numa_distance_t, ucs_status_ptr_t, ucs_status_t};
use crate::ucp::UcpUtils;

#[derive(Default)]
pub struct Request {
    pub waker: AtomicWaker
}

impl Request {
    pub unsafe extern "C" fn init<'a>(request: *mut c_void) {
        (request as *mut Self).write(Request::default());
    }

    pub unsafe extern "C" fn cleanup(request: *mut c_void) {
        std::ptr::drop_in_place(request as *mut Self)
    }
}

pub struct RequestFuture<T> {
    ptr: ucs_status_ptr_t,
    poll_fn: unsafe fn(ucs_status_ptr_t) -> Poll<T>,
}

impl <T> RequestFuture<T> {
    pub fn new(ptr: ucs_status_ptr_t, poll_fn: unsafe fn(ucs_status_ptr_t) -> Poll<T>) -> Self {
        Self {
            ptr,
            poll_fn,
        }
    }
}

impl <T> Future for RequestFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        let ret = unsafe { (self.poll_fn)(self.ptr) };
        match ret {
            Poll::Ready(v) => Poll::Ready(v),
            Poll::Pending => {
                let request = unsafe { &mut *(self.ptr as *mut Request) };
                request.waker.register(cx.waker());
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
