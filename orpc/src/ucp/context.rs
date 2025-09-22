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

use std::{mem, ptr};
use std::mem::MaybeUninit;
use crate::{err_box, err_ucs};
use crate::io::IOResult;
use crate::sys::RawPtr;
use crate::ucp::bindings::*;
use crate::ucp::{Config, stderr};
use crate::ucp::Request;

#[derive(Debug)]
pub struct Context {
    inner: RawPtr<ucp_context>,
}

impl Context {
    pub fn with_config(conf: &Config) -> IOResult<Self> {
        let features = ucp_feature::UCP_FEATURE_RMA
            | ucp_feature::UCP_FEATURE_TAG
            | ucp_feature::UCP_FEATURE_STREAM
            | ucp_feature::UCP_FEATURE_WAKEUP;


        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP
                | ucp_params_field::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .0 as u64,
            features: features.0 as u64,
            request_size: size_of::<Request>(),
            request_init: Some(Request::init),
            request_cleanup: Some(Request::cleanup),
            mt_workers_shared: 1,
            ..unsafe { mem::zeroed() }
        };
        let mut context_ptr = MaybeUninit::<*mut ucp_context>::uninit();
        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                conf.as_ptr(),
                context_ptr.as_mut_ptr(),
            )
        };
        err_ucs!(status)?;

        Ok(Self {
            inner: RawPtr::from_uninit(context_ptr)
        })
    }

    pub fn as_ptr(&self) -> *const ucp_context {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_context {
        self.inner.as_mut_ptr()
    }

    pub fn print(&self) {
        unsafe {
            ucp_context_print_info(self.as_mut_ptr(), stderr)
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::with_config(&Config::default()).unwrap()
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            ucp_cleanup(self.as_mut_ptr());
        }
    }
}

unsafe impl Send for Context {}

unsafe impl Sync for Context {}
