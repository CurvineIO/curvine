use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 超简单的UCX测试 - 只发送一条消息
#[test]
#[ignore]
fn simple_ucx_test() -> CommonResult<()> {
    Logger::default();
    
    println!("========================================");
    println!("        超简单UCX数据传输测试");
    println!("========================================");
    
    // 设置UCX环境
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
    }
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    println!("🔗 尝试连接到 127.0.0.1:8080...");
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            println!("✅ 连接成功！");
            
            // 发送一条非常简单的消息
            let message = b"SIMPLE TEST\n";
            println!("\n📤 发送消息:");
            println!("   文本: {:?}", std::str::from_utf8(message).unwrap());
            println!("   字节: {:02x?}", message);
            println!("   长度: {} 字节", message.len());
            
            // 发送数据
            ep.stream_send(message);
            
            println!("\n🔄 执行worker progress...");
            let mut total_progress = 0;
            for i in 0..50 {
                let count = worker.progress();
                total_progress += count;
                if count > 0 {
                    println!("   第{}轮: {} 个操作完成", i + 1, count);
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            
            println!("\n📊 总计完成 {} 个UCX操作", total_progress);
            println!("✅ 测试完成！请检查nc服务器的输出");
            
            // 短暂等待
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        Err(e) => {
            println!("❌ 连接失败: {:?}", e);
            println!("💡 请确保nc服务器在运行: nc -l -k -p 8080");
            return Err(e.into());
        }
    }
    
    println!("========================================");
    Ok(())
}

/// 对比测试：TCP vs UCX
#[test]
fn compare_tcp_vs_ucx() -> CommonResult<()> {
    use std::net::TcpStream;
    use std::io::Write;
    
    Logger::default();
    
    println!("========================================");
    println!("        TCP vs UCX 对比测试");
    println!("========================================");
    
    let test_data = b"COMPARE TEST: ABC123\n";
    println!("测试数据: {:?}", std::str::from_utf8(test_data).unwrap());
    println!("字节: {:02x?}", test_data);
    
    // 1. TCP测试
    println!("\n🌐 TCP测试:");
    match TcpStream::connect("127.0.0.1:8080") {
        Ok(mut stream) => {
            println!("✅ TCP连接成功");
            stream.write_all(test_data)?;
            stream.flush()?;
            println!("✅ TCP数据发送完成");
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        Err(e) => {
            println!("❌ TCP连接失败: {:?}", e);
        }
    }
    
    // 短暂等待
    std::thread::sleep(std::time::Duration::from_secs(2));
    
    // 2. UCX测试
    println!("\n🚀 UCX测试:");
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
            println!("✅ UCX连接成功");
            ep.stream_send(test_data);
            
            for i in 0..50 {
                let count = worker.progress();
                if count > 0 {
                    println!("UCX progress {}: {}", i, count);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            
            println!("✅ UCX数据发送完成");
        }
        Err(e) => {
            println!("❌ UCX连接失败: {:?}", e);
        }
    }
    
    println!("\n🔍 请比较nc服务器收到的两条消息:");
    println!("   第1条 (TCP): 应该显示 'COMPARE TEST: ABC123'");
    println!("   第2条 (UCX): 如果是乱码说明UCX有问题");
    
    println!("========================================");
    Ok(())
}
