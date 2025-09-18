use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

#[test]
fn test_debug_output() -> CommonResult<()> {
    println!("========================================");
    println!("🔧 UCX调试输出测试");
    println!("========================================");
    
    Logger::default();
    
    // 设置UCX环境变量
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
    }
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    println!("📊 地址族验证:");
    println!("   存储的地址族: {}", ucs_addr.debug_address_family());
    println!("   UCX的地址族: {}", ucs_addr.debug_ucx_address_family());
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            println!("✅ 连接成功!");
            
            // 测试数据
            let test_data = b"DEBUG TEST\n";
            println!("\n📤 准备发送数据:");
            println!("   内容: {:?}", std::str::from_utf8(test_data).unwrap());
            println!("   字节: {:02x?}", test_data);
            
            // 使用改进的发送方法
            println!("\n🚀 调用 stream_send_and_wait...");
            match ep.stream_send_and_wait(test_data, &worker) {
                Ok(_) => println!("✅ 数据发送成功!"),
                Err(e) => println!("❌ 数据发送失败: {:?}", e),
            }
            
            println!("\n🔄 开始worker progress...");
            let mut success_count = 0;
            let mut total_progress = 0;
            
            for i in 0..100 {
                let count = worker.progress();
                total_progress += count;
                
                if count > 0 {
                    success_count += 1;
                    println!("   Progress轮次 {}: {} 操作完成", i + 1, count);
                }
                
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            
            println!("\n📊 统计信息:");
            println!("   有效progress轮次: {}/100", success_count);
            println!("   总操作数: {}", total_progress);
            
            if total_progress == 0 {
                println!("⚠️  警告: 没有任何UCX操作被推进!");
            }
            
            // 最后等待
            println!("\n⏳ 等待2秒让数据传输完成...");
            std::thread::sleep(std::time::Duration::from_secs(2));
            
        }
        Err(e) => {
            println!("❌ 连接失败: {:?}", e);
            let error_str = format!("{:?}", e);
            if error_str.contains("address family") {
                println!("🔴 地址族错误!");
            } else {
                println!("🔵 其他连接错误 (可能没有服务器)");
            }
            return Err(e);
        }
    }
    
    println!("========================================");
    Ok(())
}
