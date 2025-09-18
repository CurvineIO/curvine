use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::io::{self, Write};
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

// 交互式UCX测试 - 需要手动启动nc服务器: nc -l -k -p 8080
// 运行命令: cargo test --test ucp_interactive_test -- --nocapture
#[test]
#[ignore] // 默认忽略，需要手动运行
fn interactive_test() -> CommonResult<()> {
    Logger::default();
    
    println!("=== UCX 交互式连接测试 ===");
    println!("请先在另一个终端启动: nc -l -k -p 8080");
    println!("然后按Enter键继续...");
    
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    
    // 设置UCX环境变量
    std::env::set_var("UCX_TLS", "tcp");
    std::env::set_var("UCX_NET_DEVICES", "lo");
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    info!("地址族: {}", ucs_addr.debug_address_family());
    
    // 进行多次连接测试
    for i in 1..=3 {
        println!("\n--- 连接测试 {} ---", i);
        
        match UcpEndpoint::connect(&worker, &ucs_addr) {
            Ok(ep) => {
                info!("连接 {} 成功!", i);
                
                let message = format!("UCX连接测试 #{} - 地址族修复成功!\n", i);
                ep.stream_send(message.into());
                
                // 保持连接一段时间
                std::thread::sleep(std::time::Duration::from_millis(500));
                
                ep.stream_send(format!("连接 {} 即将关闭...\n", i).into());
                std::thread::sleep(std::time::Duration::from_millis(500));
                
                info!("连接 {} 关闭", i);
            }
            Err(e) => {
                println!("连接 {} 失败: {:?}", i, e);
                return Err(e);
            }
        }
        
        // 连接间隔
        if i < 3 {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    
    println!("\n✅ 所有连接测试完成！nc服务器应该接收到了3条消息。");
    Ok(())
}

// 简单的自动化测试
#[test]
fn automated_test() -> CommonResult<()> {
    Logger::default();
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:18080")?; // 使用不同端口避免冲突
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    // 验证地址族修复
    assert_ne!(ucs_addr.debug_address_family(), 0);
    assert_eq!(ucs_addr.debug_address_family(), libc::AF_INET as u16);
    
    // 尝试连接不存在的服务（应该失败，但不是address family错误）
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(_) => {
            panic!("不应该连接成功，因为没有服务器在18080端口");
        }
        Err(e) => {
            let error_str = format!("{:?}", e);
            assert!(!error_str.contains("address family: 0"));
            info!("预期的连接失败（无服务器）: {:?}", e);
        }
    }
    
    println!("✅ 地址族修复验证通过！");
    Ok(())
}
