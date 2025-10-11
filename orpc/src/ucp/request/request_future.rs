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
use std::pin::Pin;
use std::task::Poll;
use crate::ucp::bindings::{ucp_request_free, ucs_status_ptr_t};
use crate::ucp::request::RequestWaker;

pub struct RequestFuture<T> {
    ptr: ucs_status_ptr_t,
    poll_fn: unsafe fn(ucs_status_ptr_t) -> Poll<T>,
}

impl<T> RequestFuture<T> {
    pub fn new(ptr: ucs_status_ptr_t, poll_fn: fn(ucs_status_ptr_t) -> Poll<T>) -> Self {
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
