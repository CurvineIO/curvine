use std::net::{SocketAddr, IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use orpc::CommonResult;
use orpc::ucp::UcsSockAddr;

#[test]
fn test_ipv4_address_family() -> CommonResult<()> {
    let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let ucs_addr = UcsSockAddr::new(ipv4_addr)?;
    
    println!("IPv4 Address: {:?}", ucs_addr);
    
    // AF_INET = 2
    assert_eq!(ucs_addr.debug_address_family(), libc::AF_INET as u16);
    assert_eq!(ucs_addr.debug_address_family(), 2);
    
    println!("✅ IPv4地址族验证成功: {}", ucs_addr.debug_address_family());
    Ok(())
}

#[test] 
fn test_ipv6_address_family() -> CommonResult<()> {
    let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 8080);
    let ucs_addr = UcsSockAddr::new(ipv6_addr)?;
    
    println!("IPv6 Address: {:?}", ucs_addr);
    
    // AF_INET6 = 10 (在大多数系统上)
    assert_eq!(ucs_addr.debug_address_family(), libc::AF_INET6 as u16);
    assert_eq!(ucs_addr.debug_address_family(), 10);
    
    println!("✅ IPv6地址族验证成功: {}", ucs_addr.debug_address_family());
    Ok(())
}

#[test]
fn test_address_from_string() -> CommonResult<()> {
    // 测试字符串解析
    let addr1 = SocketAddr::from_str("192.168.1.100:9000")?;
    let ucs_addr1 = UcsSockAddr::new(addr1)?;
    
    let addr2 = SocketAddr::from_str("[::1]:9000")?;
    let ucs_addr2 = UcsSockAddr::new(addr2)?;
    
    println!("192.168.1.100:9000 -> 地址族: {}", ucs_addr1.debug_address_family());
    println!("[::1]:9000 -> 地址族: {}", ucs_addr2.debug_address_family());
    
    assert_eq!(ucs_addr1.debug_address_family(), 2); // AF_INET
    assert_eq!(ucs_addr2.debug_address_family(), 10); // AF_INET6
    
    println!("✅ 字符串解析地址族验证成功");
    Ok(())
}
