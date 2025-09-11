use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread;
use anyhow::Result;

// 使用我们的 UCX 封装
use orpc::ucp::{UcxContext, UcxWorker};

/// 简单的 UCX TCP 服务器示例
fn main() -> Result<()> {
    println!("=== UCX 简单 TCP 服务器示例 ===");
    
    // 1. 创建 UCX 上下文
    println!("1. 创建 UCX 上下文...");
    let context = UcxContext::new()?;
    println!("   ✓ UCX 上下文创建成功");

    // 2. 创建工作器
    println!("2. 创建 UCX 工作器...");
    let worker = UcxWorker::new(context)?;
    println!("   ✓ UCX 工作器创建成功");

    // 3. 开始监听 (这里简化实现，实际的监听需要更多 UCX API)
    let bind_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    println!("3. 准备在 {} 上提供服务", bind_addr);
    println!("   注意: 这是一个简化的服务器示例");
    println!("   实际的 UCX 服务器需要使用 ucp_worker_create_listener API");

    // 4. 主服务循环 (简化版本)
    println!("4. 服务器运行中... (按 Ctrl+C 退出)");
    
    let start_time = Instant::now();
    let mut message_count = 0;
    
    loop {
        // 推进工作器进度
        let progress = worker.progress();
        
        if progress > 0 {
            message_count += progress;
            println!("   处理了 {} 个操作 (总计: {})", progress, message_count);
        }
        
        // 每10秒打印一次状态
        if start_time.elapsed().as_secs() % 10 == 0 {
            println!("   服务器运行时间: {:?}, 总操作数: {}", start_time.elapsed(), message_count);
            thread::sleep(Duration::from_secs(1)); // 避免重复打印
        }
        
        thread::sleep(Duration::from_millis(10));
    }
}
