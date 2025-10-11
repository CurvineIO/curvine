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

use crate::ucp::bindings::FILE;

pub mod bindings;
pub mod core;
pub mod request;
pub mod rma;

mod ucp_utils;
pub use self::ucp_utils::UcpUtils;

mod ucp_executor;
pub use self::ucp_executor::UcpExecutor;

pub mod ucp_runtime;
mod ucp_frame;

pub use self::ucp_runtime::UcpRuntime;

extern "C" {
    pub static stderr: *mut FILE;
}

#[macro_export]
macro_rules! err_ucs {
    ($e:expr) => {{
        if $e != $crate::ucp::bindings::ucs_status_t::UCS_OK {
            let ctx = format!("errno: {}({}:{})", $e as i8, file!(), line!());
            Err($crate::io::IOError::create(ctx))
        } else {
            Ok(())
        }
    }};
}

#[macro_export]
macro_rules! poll_status {
    ($status:expr, $init_value:expr, $poll_fn:expr) => {{
        let request = $crate::ucp::request::RequestStatus::new($status, $init_value, $poll_fn);
        match request {
            $crate::ucp::request::RequestStatus::Ready(v) => v,
            $crate::ucp::request::RequestStatus::Pending(f) => f.await,
        }
    }};

    ($status:expr, $poll_fn:expr) => {{
        let request = $crate::ucp::request::RequestStatus::new($status, std::mem::MaybeUninit::uninit(), $poll_fn);
        match request {
            $crate::ucp::request::RequestStatus::Ready(v) => v,
            $crate::ucp::request::RequestStatus::Pending(f) => f.await,
        }
    }};
}

