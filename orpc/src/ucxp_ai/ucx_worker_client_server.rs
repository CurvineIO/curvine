use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use orpc::ucp::{UcxWorker, WorkerConfig, ThreadMode};

/// UCX Worker 客户端-服务器示例
fn main() -> Result<()> {
    println!("=== UCX Worker 客户端-服务器示例 ===");
    
    // 启动服务器线程
    let server_handle = thread::spawn(|| {
        if let Err(e) = run_server() {
            eprintln!("服务器错误: {}", e);
        }
    });
    
    // 等待服务器启动
    thread::sleep(Duration::from_millis(500));
    
    // 启动客户端线程
    let client_handle = thread::spawn(|| {
        if let Err(e) = run_client() {
            eprintln!("客户端错误: {}", e);
        }
    });
    
    // 等待两个线程完成
    client_handle.join().unwrap();
    server_handle.join().unwrap();
    
    Ok(())
}

/// 运行服务器
fn run_server() -> Result<()> {
    println!("[服务器] 启动 UCX Worker 服务器...");
    
    // 创建服务器配置
    let config = WorkerConfig {
        thread_mode: ThreadMode::Single,
        buffer_size: 32 * 1024,
        enable_tag: true,
        enable_stream: false,
        enable_rma: false,
        progress_interval_us: 50,
    };
    
    // 创建服务器 Worker
    let mut server_worker = UcxWorker::new(config)?;
    println!("[服务器] Worker 创建成功");
    
    // 创建监听器
    let bind_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    server_worker.create_listener(bind_addr)?;
    println!("[服务器] 监听器创建成功: {}", bind_addr);
    
    let mut buffer = vec![0u8; 1024];
    let mut message_count = 0;
    
    println!("[服务器] 等待客户端连接和消息...");
    
    // 服务器主循环 (简化版本)
    for _i in 0..1000 {
        server_worker.progress();
        
        // 检查所有端点的消息
        let endpoint_ids = server_worker.get_endpoint_ids();
        for endpoint_id in endpoint_ids {
            // 尝试接收消息
            if let Ok(Some(bytes_received)) = server_worker.try_receive_tag(&endpoint_id, 0, &mut buffer) {
                let message = String::from_utf8_lossy(&buffer[..bytes_received]);
                message_count += 1;
                
                println!("[服务器] 收到消息 #{}: \"{}\" 来自 {}", message_count, message, endpoint_id);
                
                // 发送响应
                let response = format!("服务器响应 #{}: 已收到 '{}'", message_count, message);
                if let Err(e) = server_worker.send_tag(&endpoint_id, 1, response.as_bytes()) {
                    eprintln!("[服务器] 发送响应失败: {}", e);
                } else {
                    println!("[服务器] 发送响应到 {}: \"{}\"", endpoint_id, response);
                }
            }
        }
        
        thread::sleep(Duration::from_millis(10));
    }
    
    println!("[服务器] 服务器统计信息:");
    server_worker.stats().print();
    
    Ok(())
}

/// 运行客户端
fn run_client() -> Result<()> {
    println!("[客户端] 启动 UCX Worker 客户端...");
    
    // 创建客户端配置
    let config = WorkerConfig {
        thread_mode: ThreadMode::Single,
        buffer_size: 32 * 1024,
        enable_tag: true,
        enable_stream: false,
        enable_rma: false,
        progress_interval_us: 50,
    };
    
    // 创建客户端 Worker
    let client_worker = UcxWorker::new(config)?;
    println!("[客户端] Worker 创建成功");
    
    // 连接到服务器
    let server_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let endpoint_id = client_worker.connect(server_addr)?;
    println!("[客户端] 连接到服务器成功: {}", endpoint_id);
    
    // 等待连接建立
    thread::sleep(Duration::from_millis(200));
    
    let mut buffer = vec![0u8; 1024];
    
    // 发送测试消息
    for i in 1..=5 {
        let message = format!("测试消息 #{}", i);
        
        println!("[客户端] 发送消息: \"{}\"", message);
        client_worker.send_tag(&endpoint_id, 0, message.as_bytes())?;
        
        // 推进进度确保消息发送
        for _j in 0..10 {
            client_worker.progress();
            thread::sleep(Duration::from_millis(1));
        }
        
        // 等待并接收响应
        let mut received = false;
        for _attempt in 0..100 {
            client_worker.progress();
            
            if let Ok(Some(bytes_received)) = client_worker.try_receive_tag(&endpoint_id, 1, &mut buffer) {
                let response = String::from_utf8_lossy(&buffer[..bytes_received]);
                println!("[客户端] 收到响应: \"{}\"", response);
                received = true;
                break;
            }
            
            thread::sleep(Duration::from_millis(10));
        }
        
        if !received {
            println!("[客户端] 警告: 消息 {} 未收到响应", i);
        }
        
        // 消息间隔
        thread::sleep(Duration::from_millis(500));
    }
    
    // 测试广播功能
    println!("[客户端] 测试广播功能...");
    if let Err(e) = client_worker.broadcast_tag(99, b"广播测试消息") {
        println!("[客户端] 广播失败 (这是正常的，因为只有一个连接): {}", e);
    } else {
        println!("[客户端] 广播成功");
    }
    
    println!("[客户端] 客户端统计信息:");
    client_worker.stats().print();
    
    // 断开连接
    client_worker.disconnect(&endpoint_id)?;
    println!("[客户端] 断开连接: {}", endpoint_id);
    
    Ok(())
}
