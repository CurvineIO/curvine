use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Duration;
use anyhow::Result;
use tokio::time::{timeout, sleep};

use orpc::ucp::{AsyncUcxManager, AsyncMessage};

/// 异步 UCX TCP 客户端示例
#[tokio::main]
async fn main() -> Result<()> {
    println!("=== 异步 UCX TCP 客户端示例 ===");
    
    // 1. 创建异步 UCX 管理器
    println!("1. 初始化异步 UCX 管理器...");
    let manager = AsyncUcxManager::new()?;
    println!("   ✓ 管理器创建成功");
    
    // 2. 创建异步客户端
    println!("2. 创建异步客户端...");
    let mut client = manager.create_client();
    println!("   ✓ 客户端创建成功");
    
    // 3. 连接到服务器
    let server_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    println!("3. 连接到服务器: {}", server_addr);
    
    match timeout(Duration::from_secs(10), client.connect(server_addr)).await {
        Ok(Ok(_)) => println!("   ✓ 连接成功"),
        Ok(Err(e)) => {
            println!("   ✗ 连接失败: {}", e);
            println!("   提示: 请确保服务器在 {} 上运行", server_addr);
            return Ok(());
        }
        Err(_) => {
            println!("   ✗ 连接超时");
            return Ok(());
        }
    }
    
    // 4. 交互式异步消息发送
    println!("4. 进入异步交互模式 (输入 'quit' 退出):");
    println!("   支持的命令:");
    println!("   - ping: 发送 ping 消息");
    println!("   - echo <消息>: 发送回显消息");
    println!("   - burst <数量>: 发送批量消息");
    println!("   - quit: 退出");
    
    let mut message_id = 0u64;
    
    loop {
        print!("异步客户端> ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        tokio::task::spawn_blocking(|| {
            io::stdin().read_line(&mut input)
        }).await??;
        
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        
        match parts[0] {
            "quit" => break,
            
            "ping" => {
                println!("   → 发送异步 Ping...");
                match send_ping(&client, message_id).await {
                    Ok(rtt) => println!("   ← Pong 收到! RTT: {:?}", rtt),
                    Err(e) => println!("   ✗ Ping 失败: {}", e),
                }
                message_id += 1;
            }
            
            "echo" => {
                if parts.len() > 1 {
                    let message = parts[1..].join(" ");
                    println!("   → 发送异步回显: \"{}\"", message);
                    match send_echo(&client, message_id, &message).await {
                        Ok(response) => println!("   ← 回显响应: \"{}\"", response),
                        Err(e) => println!("   ✗ 回显失败: {}", e),
                    }
                    message_id += 1;
                } else {
                    println!("   使用: echo <消息内容>");
                }
            }
            
            "burst" => {
                if parts.len() > 1 {
                    if let Ok(count) = parts[1].parse::<u32>() {
                        println!("   → 发送 {} 条批量消息...", count);
                        match send_burst(&client, &mut message_id, count).await {
                            Ok(stats) => {
                                println!("   ← 批量发送完成:");
                                println!("      发送: {}/{}", stats.successful, count);
                                println!("      平均RTT: {:?}", stats.avg_rtt);
                                println!("      总用时: {:?}", stats.total_time);
                            }
                            Err(e) => println!("   ✗ 批量发送失败: {}", e),
                        }
                    } else {
                        println!("   使用: burst <数量>");
                    }
                } else {
                    println!("   使用: burst <数量>");
                }
            }
            
            _ => {
                // 作为普通消息发送
                let message = input.trim();
                println!("   → 发送异步消息: \"{}\"", message);
                match send_message(&client, message_id, message).await {
                    Ok(_) => println!("   ← 消息发送成功"),
                    Err(e) => println!("   ✗ 消息发送失败: {}", e),
                }
                message_id += 1;
            }
        }
        
        println!();
    }
    
    println!("5. 关闭连接...");
    manager.shutdown().await?;
    println!("   ✓ 异步客户端已退出");
    
    Ok(())
}

/// 发送 Ping 消息
async fn send_ping(client: &orpc::ucp::AsyncUcxClient, id: u64) -> Result<Duration> {
    let start = std::time::Instant::now();
    
    let ping_msg = AsyncMessage::text(id, "PING");
    client.send_message(ping_msg.tag, &ping_msg.data).await?;
    
    let response = client.receive_message(id, 1024).await?;
    let response_str = String::from_utf8_lossy(&response);
    
    if response_str.contains("PONG") {
        Ok(start.elapsed())
    } else {
        Err(anyhow::anyhow!("未收到有效的 PONG 响应: {}", response_str))
    }
}

/// 发送回显消息
async fn send_echo(client: &orpc::ucp::AsyncUcxClient, id: u64, message: &str) -> Result<String> {
    let echo_msg = AsyncMessage::text(id, &format!("ECHO:{}", message));
    client.send_message(echo_msg.tag, &echo_msg.data).await?;
    
    let response = client.receive_message(id, 2048).await?;
    let response_str = String::from_utf8_lossy(&response).to_string();
    
    Ok(response_str)
}

/// 发送普通消息
async fn send_message(client: &orpc::ucp::AsyncUcxClient, id: u64, message: &str) -> Result<()> {
    let msg = AsyncMessage::text(id, message);
    client.send_message(msg.tag, &msg.data).await?;
    Ok(())
}

/// 批量发送消息的统计信息
#[derive(Debug)]
struct BurstStats {
    successful: u32,
    avg_rtt: Duration,
    total_time: Duration,
}

/// 发送批量消息
async fn send_burst(client: &orpc::ucp::AsyncUcxClient, message_id: &mut u64, count: u32) -> Result<BurstStats> {
    let start_time = std::time::Instant::now();
    let mut successful = 0;
    let mut total_rtt = Duration::ZERO;
    
    // 并发发送消息
    let mut tasks = Vec::new();
    
    for i in 0..count {
        let id = *message_id + i as u64;
        let msg = AsyncMessage::text(id, &format!("BURST_MESSAGE_{}", i));
        
        let task = async {
            let send_start = std::time::Instant::now();
            
            // 发送消息
            if let Err(_) = client.send_message(msg.tag, &msg.data).await {
                return None;
            }
            
            // 等待响应
            match timeout(Duration::from_secs(5), client.receive_message(id, 1024)).await {
                Ok(Ok(_)) => Some(send_start.elapsed()),
                _ => None,
            }
        };
        
        tasks.push(task);
    }
    
    // 等待所有任务完成
    let results = futures::future::join_all(tasks).await;
    
    for result in results {
        if let Some(rtt) = result {
            successful += 1;
            total_rtt += rtt;
        }
    }
    
    *message_id += count as u64;
    
    let avg_rtt = if successful > 0 {
        total_rtt / successful
    } else {
        Duration::ZERO
    };
    
    Ok(BurstStats {
        successful,
        avg_rtt,
        total_time: start_time.elapsed(),
    })
}
