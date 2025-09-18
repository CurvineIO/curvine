use std::net::SocketAddr;
use std::str::FromStr;
use orpc::CommonResult;
use orpc::ucp::UcsSockAddr;

#[test]
fn test_pointer_fix() -> CommonResult<()> {
    println!("=== 测试指针修复 ===");
    
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    println!("地址族: {}", ucs_addr.debug_address_family());
    println!("地址长度: {}", ucs_addr.debug_address_len());
    println!("是否IPv6: {:?}", ucs_addr);
    
    // 验证地址族正确
    assert_eq!(ucs_addr.debug_address_family(), 2); // AF_INET
    assert_eq!(ucs_addr.debug_address_len(), 16); // sizeof(sockaddr_in)
    
    println!("✅ 指针修复验证成功");
    Ok(())
}
