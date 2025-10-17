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

#![allow(clippy::missing_safety_doc, clippy::missing_transmute_annotations)]

use crate::{err_box, err_ext};
use crate::error::ErrorExt;
use crate::ucp::bindings::{FILE, ucs_status_t};
use crate::ucp::bindings::ucs_status_t::UCS_OK;
use crate::ucp::ucp_error::UcpError;

pub const HANDSHAKE_MAGIC: u64 = 0xFEEDFACE;

pub const HANDSHAKE_VERSION: u32 = 1;

pub const HANDSHAKE_HEADER_LEN: u32 = 32;

pub const HANDSHAKE_LEN_BYTES: usize = 4;

pub const HANDSHAKE_MAX_LEN: usize = 1024;

pub mod bindings;
pub mod core;
pub mod request;
pub mod rma;
pub mod reactor;

mod ucp_utils;
mod ucp_error;

pub use self::ucp_utils::UcpUtils;

extern "C" {
    pub static stderr: *mut FILE;
}

#[macro_export]
macro_rules! err_ucs {
    ($e:expr) => {{
        match $crate::ucp::UcpError::from_status($e) {
            Err(e) => {
                use $crate::error::ErrorExt;
                let ctx: String = format!("({}:{}) {:?}", file!(), line!(), $e);
                let err: $crate::io::IOError = e.into();
                Err(err.ctx(ctx))
            }
            Ok(()) => Ok(())
        }
    }};
}

#[macro_export]
macro_rules! poll_status {
    ($status:expr, $immediate:expr, $poll_fn:expr) => {{
        let request = $crate::ucp::request::RequestStatus::new($status, $immediate, $poll_fn);
        match request {
            $crate::ucp::request::RequestStatus::Ready(v) => v,
            $crate::ucp::request::RequestStatus::Pending(f) => f.await,
        }
    }};

    ($status:expr, $poll_fn:expr) => {{
        let request = $crate::ucp::request::RequestStatus::new($status, (), $poll_fn);
        match request {
            $crate::ucp::request::RequestStatus::Ready(v) => v,
            $crate::ucp::request::RequestStatus::Pending(f) => f.await,
        }
    }};
}

