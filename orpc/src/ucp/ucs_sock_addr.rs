use std::ffi::CString;
use std::net::SocketAddr;
use socket2::SockAddr;
use crate::{CommonResult, err_box};
use crate::io::IOResult;
use crate::io::net::InetAddr;
use crate::ucp::bindings::{sockaddr, ucp_address_t, ucs_sock_addr, ucs_sock_addr_t};

pub struct UcsSockAddr {
    sockaddr: libc::sockaddr_in,
}

impl UcsSockAddr {
    pub fn new(addr: SocketAddr) -> IOResult<Self> {
        // 设置 socket address
        let mut sockaddr: libc::sockaddr_in = unsafe {
            std::mem::zeroed()
        };
        sockaddr.sin_family = libc::AF_INET as u16;
        sockaddr.sin_port = addr.port().to_be();
        sockaddr.sin_addr.s_addr = match addr.ip() {
            std::net::IpAddr::V4(ipv4) => u32::from_be_bytes(ipv4.octets()).to_be(),
            std::net::IpAddr::V6(_) => return err_box!("暂不支持 IPv6"),
        };

        Ok(Self {
            sockaddr,
        })
    }

    pub fn handle(&self) -> *const ucp_address_t {
        &self.sockaddr as *const _ as *const ucp_address_t
    }
}
