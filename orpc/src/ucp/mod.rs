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

use crate::ucp::bindings::FILE;

// #[cfg(not(target_os = "linux"))]
pub mod bindings;

mod config;
pub use self::config::Config;

mod request;
pub use self::request::*;

mod context;
pub use self::context::Context;

mod sock_addr;
pub use self::sock_addr::SockAddr;

mod worker;
pub use self::worker::Worker;

mod endpoint;
pub use self::endpoint::Endpoint;

mod ucp_utils;
pub use self::ucp_utils::UcpUtils;

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
