use std::mem;
use std::net::{SocketAddr, IpAddr};
use crate::{CommonResult, err_box};
use crate::io::IOResult;
use crate::ucp::bindings::{ucs_sock_addr, ucs_sock_addr_t, socklen_t, sockaddr, sa_family_t};

pub struct UcsSockAddr {
    // 使用Box确保地址稳定
    sockaddr_storage: Box<SockAddrStorage>,
    // UCX需要的ucs_sock_addr结构
    ucs_addr: ucs_sock_addr,
    // 记录地址类型以便正确访问
    is_ipv6: bool,
}

// 联合体存储不同类型的sockaddr
#[repr(C)]
union SockAddrStorage {
    v4: libc::sockaddr_in,
    v6: libc::sockaddr_in6,
    // 确保足够的存储空间和对齐
    _padding: [u8; 128], // 128字节，足够存储任何sockaddr
}

impl UcsSockAddr {
    pub fn new(addr: SocketAddr) -> IOResult<Self> {
        // 创建Box确保地址稳定
        let mut storage = Box::new(SockAddrStorage {
            _padding: [0; 128]
        });
        
        let (addr_len, family, is_ipv6) = unsafe {
            match addr {
                SocketAddr::V4(v4_addr) => {
                    // 构建IPv4地址结构
                    let mut sockaddr_v4 = mem::zeroed::<libc::sockaddr_in>();
                    sockaddr_v4.sin_family = libc::AF_INET as u16;
                    sockaddr_v4.sin_port = v4_addr.port().to_be();
                    sockaddr_v4.sin_addr.s_addr = u32::from_be_bytes(v4_addr.ip().octets()).to_be();
                    
                    storage.v4 = sockaddr_v4;
                    (mem::size_of::<libc::sockaddr_in>() as socklen_t, libc::AF_INET, false)
                }
                SocketAddr::V6(v6_addr) => {
                    // 构建IPv6地址结构
                    let mut sockaddr_v6 = mem::zeroed::<libc::sockaddr_in6>();
                    sockaddr_v6.sin6_family = libc::AF_INET6 as u16;
                    sockaddr_v6.sin6_port = v6_addr.port().to_be();
                    sockaddr_v6.sin6_addr.s6_addr = v6_addr.ip().octets();
                    sockaddr_v6.sin6_flowinfo = v6_addr.flowinfo();
                    sockaddr_v6.sin6_scope_id = v6_addr.scope_id();
                    
                    storage.v6 = sockaddr_v6;
                    (mem::size_of::<libc::sockaddr_in6>() as socklen_t, libc::AF_INET6, true)
                }
            }
        };
        
        // 验证地址族设置正确
        if family == 0 {
            return err_box!("Invalid address family");
        }
        
        // 获取稳定的指针（Box确保地址不变）
        let addr_ptr = unsafe {
            if is_ipv6 {
                &storage.v6 as *const libc::sockaddr_in6 as *const sockaddr
            } else {
                &storage.v4 as *const libc::sockaddr_in as *const sockaddr
            }
        };
        
        // 创建ucs_sock_addr
        let ucs_addr = ucs_sock_addr {
            addr: addr_ptr,
            addrlen: addr_len,
        };
        
        Ok(Self {
            sockaddr_storage: storage,
            ucs_addr,
            is_ipv6,
        })
    }
    
    /// 返回正确的ucs_sock_addr_t结构
    pub fn handle(&self) -> ucs_sock_addr_t {
        self.ucs_addr
    }
    
    /// 调试用：获取地址族
    pub fn debug_address_family(&self) -> u16 {
        unsafe {
            if self.is_ipv6 {
                // IPv6地址，访问sockaddr_in6的sin6_family
                self.sockaddr_storage.as_ref().v6.sin6_family
            } else {
                // IPv4地址，访问sockaddr_in的sin_family
                self.sockaddr_storage.as_ref().v4.sin_family
            }
        }
    }
    
    /// 调试用：获取地址长度
    pub fn debug_address_len(&self) -> socklen_t {
        self.ucs_addr.addrlen
    }
    
    /// 调试用：从UCX指针直接读取地址族（验证UCX收到什么）
    pub fn debug_ucx_address_family(&self) -> u16 {
        unsafe {
            // 直接从UCX指针读取地址族
            let family_ptr = self.ucs_addr.addr as *const sa_family_t;
            *family_ptr
        }
    }
}

// 确保内存安全
unsafe impl Send for UcsSockAddr {}
unsafe impl Sync for UcsSockAddr {}

impl std::fmt::Debug for UcsSockAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UcsSockAddr")
            .field("stored_address_family", &self.debug_address_family())
            .field("ucx_address_family", &self.debug_ucx_address_family())
            .field("address_len", &self.debug_address_len())
            .field("is_ipv6", &self.is_ipv6)
            .finish()
    }
}
