

// #[cfg(not(target_os = "linux"))]
pub mod bindings;

mod config;
pub use self::config::Config;

mod ucp_conf;
pub use self::ucp_conf::UcpConf;

mod context;
pub use context::Context;
use crate::ucp::bindings::FILE;

mod ucp_worker;
pub use self::ucp_worker::UcpWorker;

mod ucs_sock_addr;
pub use self::ucs_sock_addr::UcsSockAddr;

mod ucp_endpoint;
pub use self::ucp_endpoint::UcpEndpoint;

mod ucp_listener;
pub use self::ucp_listener::UcpListener;

pub mod ucp_server;

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
