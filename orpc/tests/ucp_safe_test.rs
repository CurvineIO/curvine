use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 安全的UCX测试 - 重点解决pending request问题
#[test]
#[ignore]
fn safe_ucx_test() -> CommonResult<()> {
    Logger::default();
    
    println!("🛡️  UCX安全测试 - 解决pending request问题");
    println!("================================================");
    
    // 设置UCX环境
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
        std::env::set_var("UCX_LOG_LEVEL", "error");
    }
    
    println!("🏗️  创建UCX组件...");
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    println!("✅ UCX组件创建成功");
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            println!("✅ 连接建立成功!");
            
            // 测试1: 单条消息
            println!("\n📤 测试1: 发送单条消息");
            let msg1 = b"Safe UCX Test: Message 1\n";
            match ep.stream_send_and_wait(msg1, &worker) {
                Ok(_) => println!("   ✅ 消息1发送成功"),
                Err(e) => println!("   ❌ 消息1发送失败: {:?}", e),
            }
            
            // 等待一下
            std::thread::sleep(std::time::Duration::from_millis(500));
            
            // 测试2: 第二条消息 
            println!("\n📤 测试2: 发送第二条消息");
            let msg2 = b"Safe UCX Test: Message 2\n";
            match ep.stream_send_and_wait(msg2, &worker) {
                Ok(_) => println!("   ✅ 消息2发送成功"),
                Err(e) => println!("   ❌ 消息2发送失败: {:?}", e),
            }
            
            // 测试3: 稍长的消息
            println!("\n📤 测试3: 发送较长消息");
            let msg3 = b"Safe UCX Test: This is a longer message to test if the improved UCX implementation can handle various message sizes without leaving pending requests that cause crashes.\n";
            match ep.stream_send_and_wait(msg3, &worker) {
                Ok(_) => println!("   ✅ 长消息发送成功"),
                Err(e) => println!("   ❌ 长消息发送失败: {:?}", e),
            }
            
            println!("\n⏳ 等待所有传输完成...");
            std::thread::sleep(std::time::Duration::from_secs(2));
            
            println!("✅ 所有测试完成，准备安全关闭连接");
            
            // 连接将在drop时安全关闭
            
        }
        Err(e) => {
            println!("❌ 连接失败: {:?}", e);
            println!("💡 请确保nc服务器在运行: nc -l -k -p 8080");
            return Err(e);
        }
    }
    
    println!("🛡️  安全测试完成 - 检查是否还有UCX bug");
    println!("================================================");
    Ok(())
}

/// 极简测试 - 最小化的实现来避免bug
#[test]
#[ignore]
fn minimal_safe_test() -> CommonResult<()> {
    Logger::default();
    
    println!("🔬 极简UCX测试");
    println!("========================");
    
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
            println!("✅ 连接成功");
            
            // 只发送一条非常简单的消息
            let simple_msg = b"OK\n";
            println!("📤 发送简单消息: {:?}", std::str::from_utf8(simple_msg).unwrap());
            
            match ep.stream_send_and_wait(simple_msg, &worker) {
                Ok(_) => println!("✅ 成功!"),
                Err(e) => println!("❌ 失败: {:?}", e),
            }
            
            println!("⏳ 等待3秒...");
            std::thread::sleep(std::time::Duration::from_secs(3));
            
            println!("🔚 测试结束");
        }
        Err(e) => {
            println!("❌ 连接失败: {:?}", e);
            return Err(e);
        }
    }
    
    println!("========================");
    Ok(())
}
