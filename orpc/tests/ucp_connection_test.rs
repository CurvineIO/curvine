use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 专注于解决连接和数据传输的基本测试
#[test]
#[ignore]
fn basic_connection_test() -> CommonResult<()> {
    Logger::default();
    
    println!("🔧 UCX基础连接测试");
    println!("=====================================");
    
    // 环境设置
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
        std::env::set_var("UCX_LOG_LEVEL", "error"); // 减少UCX内部日志
    }
    
    // 创建UCX组件
    println!("🏗️  创建UCX组件...");
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    println!("✅ UCX组件创建成功");
    println!("   地址族: {}", ucs_addr.debug_address_family());
    
    // 尝试连接
    println!("\n🔌 尝试连接到 127.0.0.1:8080...");
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            println!("✅ 连接成功!");
            
            // 发送测试数据
            let test_message = b"UCX Connection Test: SUCCESS!\n";
            println!("\n📤 发送测试消息...");
            println!("   消息: {:?}", std::str::from_utf8(test_message).unwrap());
            
            // 使用改进的发送方法
            match ep.stream_send_and_wait(test_message, &worker) {
                Ok(_) => {
                    println!("✅ 消息发送完成!");
                }
                Err(e) => {
                    println!("❌ 消息发送失败: {:?}", e);
                }
            }
            
            // 给更多时间让数据到达
            println!("\n⏳ 等待3秒让数据完全传输...");
            std::thread::sleep(std::time::Duration::from_secs(3));
            
            println!("✅ 测试完成 - 请检查nc服务器输出");
        }
        Err(e) => {
            println!("❌ 连接失败: {:?}", e);
            println!("\n💡 排查建议:");
            println!("   1. 确保nc服务器在运行: nc -l -k -p 8080");
            println!("   2. 检查防火墙设置");
            println!("   3. 确认UCX库正确安装");
            return Err(e);
        }
    }
    
    println!("=====================================");
    Ok(())
}

/// 压力测试：发送多条消息
#[test]
#[ignore] 
fn stress_test() -> CommonResult<()> {
    Logger::default();
    
    println!("🚀 UCX压力测试");
    println!("=====================================");
    
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
    }
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            println!("✅ 连接成功，开始发送10条消息...");
            
            for i in 1..=10 {
                let message = format!("Message #{}: Hello from UCX stress test!\n", i);
                println!("📤 发送消息 {}/10: {}", i, message.trim());
                
                match ep.stream_send_and_wait(message.as_bytes(), &worker) {
                    Ok(_) => println!("   ✅ 消息 {} 发送成功", i),
                    Err(e) => {
                        println!("   ❌ 消息 {} 发送失败: {:?}", i, e);
                        break;
                    }
                }
                
                // 消息间隔
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            
            println!("\n✅ 压力测试完成!");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        Err(e) => {
            println!("❌ 连接失败: {:?}", e);
            return Err(e);
        }
    }
    
    println!("=====================================");
    Ok(())
}
