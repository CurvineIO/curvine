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

use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::task::Poll;
use crate::err_ucs;
use crate::io::IOResult;
use crate::ucp::bindings::{ucs_status_ptr_t, ucs_status_t};
use crate::ucp::request::RequestFuture;
use crate::ucp::UcpUtils;

pub enum RequestStatus<T> {
    Ready(IOResult<T>),
    Pending(RequestFuture<IOResult<T>>)
}

impl <T> RequestStatus<T> {
    pub fn new(
        status: *mut c_void,
        immediate: MaybeUninit<T>,
        poll_fn: fn(ucs_status_ptr_t) -> Poll<IOResult<T>>,
    ) -> Self {
        if UcpUtils::ucs_ptr_raw_status(status) == ucs_status_t::UCS_OK {
            Self::Ready(Ok(unsafe { immediate.assume_init() }))
        } else if UcpUtils::ucs_ptr_is_err(status) {
            match err_ucs!(UcpUtils::ucs_ptr_raw_status(status)) {
                Err(err) => Self::Ready(Err(err)),
                Ok(_) => Self::Ready(Ok(unsafe { immediate.assume_init() }))
            }
        } else {
            Self::Pending(RequestFuture::new(status, poll_fn))
        }
    }
}