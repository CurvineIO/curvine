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
use std::ptr;
use std::sync::Arc;
use crate::err_ucs;
use crate::io::IOResult;
use crate::sys::{CString, RawPtr};
use crate::ucp::bindings::*;
use crate::ucp::{Context, stderr};

#[derive(Debug)]
pub struct Config {
    inner: RawPtr<ucp_config_t>,
    pub name: String,
    pub threads: usize,
}

impl Config {
    pub fn as_ptr(&self) -> *const ucp_config_t {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_config_t {
        self.inner.as_mut_ptr()
    }

    pub fn print(&self) {
        let flags = ucs_config_print_flags_t::UCS_CONFIG_PRINT_CONFIG
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_DOC
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HEADER
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HIDDEN;
        let title = CString::new("UCP conf").expect("Not a valid CStr");
        unsafe { ucp_config_print(self.as_ptr(), stderr, title.as_ptr(), flags) };
    }

    pub fn create_context(&self) -> IOResult<Context> {
        Context::with_config(self)
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut inner = MaybeUninit::<*mut ucp_config>::uninit();
        let status = unsafe {
            ucp_config_read(ptr::null(), ptr::null(), inner.as_mut_ptr())
        };
        err_ucs!(status).unwrap();

        Self {
            inner: RawPtr::from_uninit(inner),
            name: "orpc-udp".to_string(),
            threads: 32,
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        unsafe {
            ucp_config_release(self.as_mut_ptr())
        }
    }
}