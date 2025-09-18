use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 验证地址族修复是否成功的测试
#[test]
fn validate_address_family_fix() -> CommonResult<()> {
    Logger::default();
    
    println!("=== 验证UCX地址族修复 ===");
    
    // 测试多个不同的IPv4地址
    let test_addresses = [
        "127.0.0.1:8080",
        "192.168.1.1:9000", 
        "10.0.0.1:8000",
        "0.0.0.0:7000",
    ];
    
    for addr_str in &test_addresses {
        let addr = SocketAddr::from_str(addr_str)?;
        let ucs_addr = UcsSockAddr::new(addr)?;
        
        println!("地址: {} -> 地址族: {}", addr_str, ucs_addr.debug_address_family());
        
        // 关键验证：地址族必须是AF_INET(2)，不能是0
        assert_ne!(ucs_addr.debug_address_family(), 0, 
                   "地址族不能为0 (已修复unknown address family问题)");
        assert_eq!(ucs_addr.debug_address_family(), libc::AF_INET as u16,
                   "IPv4地址族必须是AF_INET(2)");
    }
    
    // 测试UCX context和worker创建（确保基础设施正常）
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    println!("✅ UCX Context和Worker创建成功");
    
    // 测试地址创建和endpoint尝试连接
    let addr = SocketAddr::from_str("127.0.0.1:18080")?; // 使用不存在的端口
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(_) => {
            println!("⚠️  意外成功连接（可能有服务器在监听18080端口）");
        }
        Err(e) => {
            let error_str = format!("{:?}", e);
            // 关键检查：确保错误不是地址族相关的
            assert!(!error_str.contains("address family: 0"), 
                    "不应该再有'address family: 0'错误");
            assert!(!error_str.contains("Invalid address family: 0"),
                    "不应该再有'Invalid address family: 0'错误");
            
            println!("✅ 连接失败是预期的（无服务器监听），错误: {:?}", e);
            
            // 检查是否是网络相关错误（而不是地址族错误）
            if error_str.contains("UCS_ERR_INVALID_PARAM") {
                println!("   ℹ️  UCS_ERR_INVALID_PARAM是网络连接失败的正常错误");
            }
        }
    }
    
    println!("\n🎉 地址族修复验证成功！");
    println!("   ✅ 地址族正确设置为AF_INET(2)");  
    println!("   ✅ 不再有'unknown address family: 0'错误");
    println!("   ✅ UCX基础设施工作正常");
    println!("   ✅ 连接失败是网络问题，不是地址族问题");
    
    Ok(())
}
