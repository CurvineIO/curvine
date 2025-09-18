use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 验证数据传输的测试
/// 使用方法：
/// 1. 终端1: nc -l -k -p 8080
/// 2. 终端2: cargo test --package orpc --test ucp_data_transfer_test -- --nocapture
#[test]
#[ignore] // 默认忽略，需要手动运行
fn test_data_transfer() -> CommonResult<()> {
    Logger::default();
    
    println!("=== UCX数据传输测试 ===");
    println!("请确保nc服务器在运行: nc -l -k -p 8080");
    
    // 设置UCX环境变量以使用TCP传输
    std::env::set_var("UCX_TLS", "tcp");
    std::env::set_var("UCX_NET_DEVICES", "lo");
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    info!("地址族验证通过: {}", ucs_addr.debug_address_family());
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            info!("连接成功！开始发送数据...");
            
            let messages = [
                "=== UCX数据传输测试开始 ===\n",
                "消息1: Hello from Rust UCX!\n", 
                "消息2: 地址族修复成功!\n",
                "消息3: 数据传输正常工作!\n",
                "消息4: 测试即将结束...\n",
                "=== UCX数据传输测试结束 ===\n",
            ];
            
            for (i, message) in messages.iter().enumerate() {
                info!("发送消息 {}: {}", i + 1, message.trim());
                ep.stream_send((*message).into());
                
                // 推进worker确保消息发送
                for j in 0..50 {
                    let count = worker.progress();
                    if count > 0 {
                        info!("消息 {} progress: {} operations (round {})", i + 1, count, j + 1);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                
                // 短暂暂停让用户看清每条消息
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            
            info!("所有消息已发送，等待传输完成...");
            std::thread::sleep(std::time::Duration::from_secs(2));
            
            info!("测试完成！");
        }
        Err(e) => {
            println!("连接失败: {:?}", e);
            println!("请确保nc服务器正在运行: nc -l -k -p 8080");
            return Err(e);
        }
    }
    
    Ok(())
}

/// 自动化测试：验证地址族但不需要服务器
#[test]
fn test_address_family_only() -> CommonResult<()> {
    Logger::default();
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:18080")?; 
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    // 验证地址族修复
    assert_eq!(ucs_addr.debug_address_family(), 2); // AF_INET
    assert_eq!(ucs_addr.debug_ucx_address_family(), 2); // UCX应该看到相同值
    
    info!("✅ 地址族验证成功: 存储={}, UCX={}", 
          ucs_addr.debug_address_family(), 
          ucs_addr.debug_ucx_address_family());
    
    // 尝试连接（预期失败）
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(_) => panic!("不应该连接成功"),
        Err(e) => {
            let error_str = format!("{:?}", e);
            assert!(!error_str.contains("address family"));
            info!("✅ 连接失败预期，但不是地址族错误: {:?}", e);
        }
    }
    
    Ok(())
}
