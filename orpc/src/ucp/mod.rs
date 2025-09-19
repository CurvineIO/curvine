use crate::ucp::bindings::FILE;

// #[cfg(not(target_os = "linux"))]
pub mod bindings;

mod config;
pub use self::config::Config;

mod request;
pub use self::request::Request;

mod context;
pub use self::context::Context;

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
            Ok($e)
        }
    }};
}
