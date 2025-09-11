use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread;
use anyhow::Result;

// 使用我们的 UCX 封装
use orpc::ucp::{UcxContext, UcxWorker, UcxEndpoint};

/// 简单的 UCX TCP 客户端示例
fn main() -> Result<()> {
    println!("=== UCX 简单 TCP 客户端示例 ===");
    
    // 1. 创建 UCX 上下文
    println!("1. 创建 UCX 上下文...");
    let context = UcxContext::new()?;
    println!("   ✓ UCX 上下文创建成功");

    // 2. 创建工作器
    println!("2. 创建 UCX 工作器...");
    let worker = UcxWorker::new(context)?;
    println!("   ✓ UCX 工作器创建成功");

    // 3. 连接到服务器
    let server_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    println!("3. 连接到服务器: {}", server_addr);
    
    let endpoint = match worker.connect(server_addr) {
        Ok(ep) => {
            println!("   ✓ 连接成功");
            ep
        }
        Err(e) => {
            println!("   ✗ 连接失败: {}", e);
            println!("   提示: 请确保服务器在 {} 上运行", server_addr);
            return Err(e);
        }
    };

    // 4. 交互式消息发送
    println!("4. 进入交互模式 (输入 'quit' 退出):");
    println!("   您可以输入任何消息发送到服务器");
    
    let mut buffer = [0u8; 1024];
    loop {
        print!("客户端> ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let message = input.trim();
        
        if message.is_empty() {
            continue;
        }
        
        if message == "quit" {
            break;
        }
        
        // 发送消息
        let start_time = Instant::now();
        match endpoint.send_tag(0, message.as_bytes()) {
            Ok(_) => {
                println!("   → 发送消息: \"{}\" ({} 字节)", message, message.len());
                
                // 推进工作器进度确保消息发送
                for _ in 0..10 {
                    worker.progress();
                    thread::sleep(Duration::from_millis(1));
                }
                
                // 尝试接收响应 (等待最多 5 秒)
                let mut received = false;
                let timeout = Duration::from_secs(5);
                let recv_start = Instant::now();
                
                while recv_start.elapsed() < timeout {
                    worker.progress();
                    
                    match endpoint.receive_tag(0, &mut buffer) {
                        Ok(bytes_received) if bytes_received > 0 => {
                            let response = String::from_utf8_lossy(&buffer[..bytes_received]);
                            let rtt = start_time.elapsed();
                            println!("   ← 收到响应: \"{}\" (RTT: {:?})", response, rtt);
                            received = true;
                            break;
                        }
                        Ok(_) => {
                            // 没有数据，继续等待
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => {
                            println!("   ✗ 接收响应失败: {}", e);
                            break;
                        }
                    }
                }
                
                if !received {
                    println!("   ⚠ 未收到服务器响应 (超时)");
                }
            }
            Err(e) => {
                println!("   ✗ 发送消息失败: {}", e);
            }
        }
        
        println!();
    }
    
    println!("5. 关闭连接...");
    // endpoint 会在 drop 时自动关闭
    println!("   ✓ 客户端已退出");
    
    Ok(())
}

/// 性能测试模式
#[allow(dead_code)]
fn performance_test(worker: &UcxWorker, endpoint: &UcxEndpoint) -> Result<()> {
    println!("\n=== 性能测试模式 ===");
    
    let message_count = 100;
    let message_size = 1024;
    let test_data = vec![0x42u8; message_size];
    let mut buffer = vec![0u8; message_size * 2];
    
    println!("发送 {} 条消息，每条 {} 字节", message_count, message_size);
    
    let start_time = Instant::now();
    let mut successful_sends = 0;
    let mut total_rtt = Duration::ZERO;
    
    for i in 0..message_count {
        let send_start = Instant::now();
        
        // 发送消息
        if let Err(e) = endpoint.send_tag(i, &test_data) {
            println!("发送消息 {} 失败: {}", i, e);
            continue;
        }
        
        // 推进进度
        worker.progress();
        
        // 等待响应
        let mut received = false;
        let timeout_start = Instant::now();
        while timeout_start.elapsed() < Duration::from_secs(1) {
            worker.progress();
            
            if let Ok(bytes) = endpoint.receive_tag(i, &mut buffer) {
                if bytes > 0 {
                    let rtt = send_start.elapsed();
                    total_rtt += rtt;
                    successful_sends += 1;
                    received = true;
                    break;
                }
            }
            
            thread::sleep(Duration::from_micros(100));
        }
        
        if !received {
            println!("消息 {} 超时", i);
        }
        
        if i % 10 == 0 {
            println!("完成: {}/{}", i + 1, message_count);
        }
    }
    
    let total_time = start_time.elapsed();
    let avg_rtt = if successful_sends > 0 {
        total_rtt / successful_sends
    } else {
        Duration::ZERO
    };
    
    println!("\n=== 性能测试结果 ===");
    println!("总时间: {:?}", total_time);
    println!("成功发送: {}/{}", successful_sends, message_count);
    println!("成功率: {:.2}%", (successful_sends as f64 / message_count as f64) * 100.0);
    println!("平均 RTT: {:?}", avg_rtt);
    println!("吞吐量: {:.2} 消息/秒", successful_sends as f64 / total_time.as_secs_f64());
    println!("带宽: {:.2} MB/秒", 
             (successful_sends * message_size) as f64 / (1024.0 * 1024.0) / total_time.as_secs_f64());
    
    Ok(())
}
