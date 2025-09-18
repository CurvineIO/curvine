use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

//
// cargo test --test ucp_test::test
#[test]
fn test() -> CommonResult<()> {
    Logger::default();
    
    // 设置UCX环境变量以使用TCP传输
    unsafe { std::env::set_var("UCX_TLS", "tcp"); }
    unsafe { std::env::set_var("UCX_NET_DEVICES", "lo"); }
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    // 调试信息：验证地址族正确设置
    info!("UCS Address: {:?}", ucs_addr);
    info!("我们存储的地址族: {}", ucs_addr.debug_address_family());
    info!("UCX指针的地址族: {}", ucs_addr.debug_ucx_address_family());
    info!("Address length: {}", ucs_addr.debug_address_len());
    
    // 验证我们的存储正确
    assert_ne!(ucs_addr.debug_address_family(), 0, "Address family should not be 0");
    assert_eq!(ucs_addr.debug_address_family(), libc::AF_INET as u16, "Should be AF_INET");
    
    // 验证UCX收到的地址族也正确
    assert_eq!(ucs_addr.debug_ucx_address_family(), ucs_addr.debug_address_family(), 
               "UCX should receive the same address family as we stored");
    
    // 尝试连接（如果没有服务器会失败，但不应该是address family错误）
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            info!("Connection successful!");
            
            // 使用改进的发送方法
            println!("=== 发送消息 ===");
            let message = b"Hello from UCX! Address family fix works!\n";
            println!("准备发送数据: {:?}", std::str::from_utf8(message).unwrap_or("invalid utf8"));
            
            match ep.stream_send_and_wait(message, &worker) {
                Ok(_) => println!("✅ 消息发送成功!"),
                Err(e) => println!("❌ 消息发送失败: {:?}", e),
            }

            // 最终确保所有操作完成
            info!("Final cleanup progress...");
            for i in 0..50 {
                let count = worker.progress();
                if count > 0 {
                    info!("Final progress: {} operations (round {})", count, i + 1);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            
            info!("All messages sent, connection will close...");
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        Err(e) => {
            info!("Connection failed (expected without server): {:?}", e);
            // 确保不是address family错误
            let error_str = format!("{:?}", e);
            assert!(!error_str.contains("address family: 0"), 
                    "Should not have 'address family: 0' error anymore");
        }
    }

    Ok(())
}