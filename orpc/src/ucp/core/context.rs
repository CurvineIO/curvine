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
use crate::ucp::stderr;
use std::mem;
use std::mem::MaybeUninit;
use std::sync::Arc;
use crate::ucp::core::{Config, Worker};
use crate::ucp::request::RequestWaker;

#[derive(Debug)]
pub struct Context {
    inner: RawPtr<ucp_context>,
    config: Config,
}

impl Context {
    pub fn with_config(config: Config) -> IOResult<Self> {
        let features = ucp_feature::UCP_FEATURE_RMA
            | ucp_feature::UCP_FEATURE_TAG
            | ucp_feature::UCP_FEATURE_STREAM
            | ucp_feature::UCP_FEATURE_WAKEUP
            | ucp_feature::UCP_FEATURE_AM;

        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP
                | ucp_params_field::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .0 as u64,
            features: features.0 as u64,
            request_size: size_of::<RequestWaker>(),
            request_init: Some(RequestWaker::init),
            request_cleanup: Some(RequestWaker::cleanup),
            mt_workers_shared: 1,
            ..unsafe { mem::zeroed() }
        };
        let mut context_ptr = MaybeUninit::<*mut ucp_context>::uninit();
        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                config.as_ptr(),
                context_ptr.as_mut_ptr(),
            )
        };
        err_ucs!(status)?;

        Ok(Self {
            inner: RawPtr::from_uninit(context_ptr),
            config,
        })
    }

    pub fn as_ptr(&self) -> *const ucp_context {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_context {
        self.inner.as_mut_ptr()
    }

    pub fn print(&self) {
        unsafe { ucp_context_print_info(self.as_mut_ptr(), stderr) }
    }

    pub fn create_worker(self: &Arc<Self>) -> IOResult<Worker> {
        Worker::new(self.clone())
    }

    pub fn get_attr(&self) -> IOResult<ucp_context_attr> {
        let mut attr = MaybeUninit::<ucp_context_attr>::uninit();
        let status = unsafe {
            ucp_context_query(self.as_mut_ptr(), attr.as_mut_ptr())
        };
        err_ucs!(status)?;
        Ok(unsafe { attr.assume_init() })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::with_config(Config::default()).unwrap()
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
