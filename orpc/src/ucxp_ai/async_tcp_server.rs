use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{interval, timeout, sleep};

use orpc::ucp::{AsyncUcxManager, AsyncUcxClient, AsyncMessage};

/// 异步 UCX TCP 服务器示例
#[tokio::main]
async fn main() -> Result<()> {
    println!("=== 异步 UCX TCP 服务器示例 ===");
    
    // 1. 创建异步 UCX 管理器
    println!("1. 初始化异步 UCX 管理器...");
    let manager = AsyncUcxManager::new()?;
    println!("   ✓ 管理器创建成功");
    
    // 2. 创建服务器统计信息
    let stats = Arc::new(Mutex::new(ServerStats::new()));
    let stats_clone = Arc::clone(&stats);
    
    // 3. 启动统计打印任务
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let stats = stats_clone.lock().await;
            stats.print();
        }
    });
    
    // 4. 启动模拟服务器
    let bind_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    println!("2. 启动异步服务器在: {}", bind_addr);
    
    // 创建客户端处理器
    let client_handler = ClientHandler::new(Arc::clone(&stats));
    
    // 由于 UCX 监听 API 的复杂性，这里我们创建一个模拟的服务器
    // 在实际应用中，您需要使用完整的 UCX 监听器 API
    
    println!("3. 服务器运行中... (按 Ctrl+C 退出)");
    println!("   注意: 这是一个演示服务器，用于展示异步消息处理");
    
    // 模拟客户端连接处理
    simulate_server(&manager, client_handler).await?;
    
    // 关闭管理器
    manager.shutdown().await?;
    println!("   ✓ 异步服务器已停止");
    
    Ok(())
}

/// 服务器统计信息
#[derive(Debug)]
struct ServerStats {
    start_time: Instant,
    connections: u64,
    messages_received: u64,
    messages_sent: u64,
    bytes_received: u64,
    bytes_sent: u64,
    ping_count: u64,
    echo_count: u64,
    error_count: u64,
}

impl ServerStats {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            connections: 0,
            messages_received: 0,
            messages_sent: 0,
            bytes_received: 0,
            bytes_sent: 0,
            ping_count: 0,
            echo_count: 0,
            error_count: 0,
        }
    }
    
    fn record_connection(&mut self) {
        self.connections += 1;
    }
    
    fn record_message_received(&mut self, bytes: usize) {
        self.messages_received += 1;
        self.bytes_received += bytes as u64;
    }
    
    fn record_message_sent(&mut self, bytes: usize) {
        self.messages_sent += 1;
        self.bytes_sent += bytes as u64;
    }
    
    fn record_ping(&mut self) {
        self.ping_count += 1;
    }
    
    fn record_echo(&mut self) {
        self.echo_count += 1;
    }
    
    fn record_error(&mut self) {
        self.error_count += 1;
    }
    
    fn print(&self) {
        let uptime = self.start_time.elapsed();
        let msg_rate = self.messages_received as f64 / uptime.as_secs_f64();
        let throughput = (self.bytes_received as f64 / (1024.0 * 1024.0)) / uptime.as_secs_f64();
        
        println!("\n=== 服务器统计 (运行时间: {:?}) ===", uptime);
        println!("连接数: {}", self.connections);
        println!("接收消息: {} ({:.2} msg/s)", self.messages_received, msg_rate);
        println!("发送消息: {}", self.messages_sent);
        println!("接收字节: {} ({:.2} MB/s)", self.bytes_received, throughput);
        println!("发送字节: {}", self.bytes_sent);
        println!("Ping: {}, Echo: {}, 错误: {}", self.ping_count, self.echo_count, self.error_count);
    }
}

/// 客户端处理器
struct ClientHandler {
    stats: Arc<Mutex<ServerStats>>,
}

impl ClientHandler {
    fn new(stats: Arc<Mutex<ServerStats>>) -> Self {
        Self { stats }
    }
    
    /// 处理客户端连接
    async fn handle_client(&self, mut client: AsyncUcxClient, client_id: String) -> Result<()> {
        println!("   + 客户端 {} 连接", client_id);
        
        {
            let mut stats = self.stats.lock().await;
            stats.record_connection();
        }
        
        // 客户端消息处理循环
        let mut buffer_size = 4096;
        let mut tag = 0u64;
        
        loop {
            // 尝试接收消息 (带超时)
            let message_result = timeout(
                Duration::from_secs(30),
                client.receive_message(tag, buffer_size)
            ).await;
            
            match message_result {
                Ok(Ok(data)) => {
                    let message = String::from_utf8_lossy(&data);
                    println!("   <- 客户端 {} 消息: \"{}\"", client_id, message);
                    
                    {
                        let mut stats = self.stats.lock().await;
                        stats.record_message_received(data.len());
                    }
                    
                    // 处理不同类型的消息
                    if let Err(e) = self.process_message(&client, &message, tag).await {
                        eprintln!("   ! 处理消息失败: {}", e);
                        let mut stats = self.stats.lock().await;
                        stats.record_error();
                    }
                    
                    tag += 1;
                }
                Ok(Err(e)) => {
                    eprintln!("   ! 客户端 {} 接收错误: {}", client_id, e);
                    let mut stats = self.stats.lock().await;
                    stats.record_error();
                    break;
                }
                Err(_) => {
                    // 超时，检查客户端是否仍然连接
                    println!("   ? 客户端 {} 超时，检查连接状态", client_id);
                    // 这里可以发送心跳消息检查连接
                    continue;
                }
            }
        }
        
        println!("   - 客户端 {} 断开连接", client_id);
        Ok(())
    }
    
    /// 处理具体的消息
    async fn process_message(&self, client: &AsyncUcxClient, message: &str, tag: u64) -> Result<()> {
        if message.trim() == "PING" {
            // 处理 Ping 消息
            let response = AsyncMessage::text(tag, "PONG");
            client.send_message(response.tag, &response.data).await?;
            
            let mut stats = self.stats.lock().await;
            stats.record_ping();
            stats.record_message_sent(response.data.len());
            
            println!("   -> 发送 PONG 响应");
            
        } else if message.starts_with("ECHO:") {
            // 处理 Echo 消息
            let echo_content = &message[5..]; // 去掉 "ECHO:" 前缀
            let response = AsyncMessage::text(tag, &format!("ECHO_REPLY:{}", echo_content));
            client.send_message(response.tag, &response.data).await?;
            
            let mut stats = self.stats.lock().await;
            stats.record_echo();
            stats.record_message_sent(response.data.len());
            
            println!("   -> 发送 Echo 响应: \"{}\"", echo_content);
            
        } else if message.starts_with("BURST_MESSAGE_") {
            // 处理批量消息
            let response = AsyncMessage::text(tag, &format!("BURST_ACK:{}", message));
            client.send_message(response.tag, &response.data).await?;
            
            let mut stats = self.stats.lock().await;
            stats.record_message_sent(response.data.len());
            
        } else {
            // 处理其他消息
            let response = AsyncMessage::text(tag, &format!("RECEIVED:{}", message));
            client.send_message(response.tag, &response.data).await?;
            
            let mut stats = self.stats.lock().await;
            stats.record_message_sent(response.data.len());
            
            println!("   -> 发送通用响应");
        }
        
        Ok(())
    }
}

/// 模拟服务器运行
async fn simulate_server(manager: &AsyncUcxManager, handler: ClientHandler) -> Result<()> {
    let handler = Arc::new(handler);
    let mut client_counter = 0u32;
    let mut client_tasks = Vec::new();
    
    // 模拟定期接受新连接
    let mut connection_interval = interval(Duration::from_secs(5));
    
    loop {
        tokio::select! {
            _ = connection_interval.tick() => {
                // 模拟新客户端连接 (20% 概率)
                if rand::random::<f64>() < 0.2 {
                    client_counter += 1;
                    let client_id = format!("client_{}", client_counter);
                    let client = manager.create_client();
                    let handler_clone = Arc::clone(&handler);
                    
                    let task = tokio::spawn(async move {
                        // 模拟客户端处理
                        if let Err(e) = handler_clone.handle_client(client, client_id.clone()).await {
                            eprintln!("客户端 {} 处理失败: {}", client_id, e);
                        }
                    });
                    
                    client_tasks.push(task);
                }
            }
            
            // 清理已完成的客户端任务
            _ = sleep(Duration::from_secs(1)) => {
                client_tasks.retain(|task| !task.is_finished());
            }
        }
    }
}
