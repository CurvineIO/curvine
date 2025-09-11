use anyhow::Result;
use log::info;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use rust_ucx_example::{Context, ContextBuilder, Worker, WorkerBuilder};

use rust_ucx_example::{serialize_message, deserialize_message, Message};

/// 简单的ping-pong示例
/// 演示UCX的基本通信功能

const SERVER_PORT: u16 = 9999;
const MESSAGE_SIZE: usize = 64;
const PING_COUNT: u64 = 5;

async fn run_server() -> Result<()> {
    info!("启动Ping-Pong服务器");

    // 创建UCX上下文和工作器
    let mut context_builder = ContextBuilder::new();
    context_builder.request_tag_feature();
    let context = context_builder.build()?;
    let worker = WorkerBuilder::new(&context).build()?;

    // 创建监听器
    let bind_addr = format!("127.0.0.1:{}", SERVER_PORT).parse()?;
    let listener = worker.create_listener(bind_addr).await?;
    let actual_addr = listener.socket_address()?;
    info!("服务器监听地址: {}", actual_addr);

    // 等待客户端连接
    info!("等待客户端连接...");
    let mut endpoint = None;
    
    while endpoint.is_none() {
        worker.progress();
        
        if let Some(conn_request) = listener.try_accept()? {
            endpoint = Some(conn_request.accept()?);
            info!("客户端连接成功");
        }
        
        sleep(Duration::from_millis(10)).await;
    }
    
    let endpoint = endpoint.unwrap();
    let mut buffer = vec![0u8; 4096];
    let mut ping_count = 0;

    // 处理ping消息循环
    info!("开始处理Ping消息...");
    loop {
        worker.progress();
        
        // 尝试接收ping消息
        if let Some(received) = endpoint.try_receive_tag(0, &mut buffer)? {
            let data = &buffer[..received];
            
            match deserialize_message(data) {
                Ok(Message::Ping { seq, data: ping_data }) => {
                    ping_count += 1;
                    info!("收到Ping #{}, 数据大小: {} bytes", seq, ping_data.len());
                    
                    // 发送pong响应
                    let pong = Message::Pong { seq };
                    let pong_data = serialize_message(&pong)?;
                    endpoint.send_tag(0, &pong_data)?;
                    
                    info!("发送Pong #{}", seq);
                    
                    // 如果收到足够的ping，就退出
                    if ping_count >= PING_COUNT {
                        info!("处理完所有Ping消息，退出服务器");
                        break;
                    }
                }
                Ok(Message::Disconnect) => {
                    info!("收到断开连接消息");
                    break;
                }
                Ok(other) => {
                    info!("收到其他消息: {:?}", other);
                }
                Err(e) => {
                    log::warn!("解析消息失败: {}", e);
                }
            }
        }
        
        sleep(Duration::from_millis(1)).await;
    }

    info!("Ping-Pong服务器退出");
    Ok(())
}

async fn run_client() -> Result<()> {
    info!("启动Ping-Pong客户端");

    // 等待服务器启动
    sleep(Duration::from_millis(500)).await;

    // 创建UCX上下文和工作器
    let mut context_builder = ContextBuilder::new();
    context_builder.request_tag_feature();
    let context = context_builder.build()?;
    let worker = WorkerBuilder::new(&context).build()?;

    // 连接到服务器
    let server_addr = format!("127.0.0.1:{}", SERVER_PORT).parse()?;
    let endpoint = worker.connect(server_addr).await?;
    info!("连接到服务器: {}", server_addr);

    // 等待连接建立
    sleep(Duration::from_millis(100)).await;

    let mut buffer = vec![0u8; 4096];
    let mut successful_pings = 0;
    let mut total_latency = Duration::ZERO;

    // 发送ping消息
    for seq in 0..PING_COUNT {
        let ping_data = vec![42u8; MESSAGE_SIZE];
        let ping = Message::Ping { seq, data: ping_data };
        
        let start_time = Instant::now();
        
        // 发送ping
        let ping_msg = serialize_message(&ping)?;
        endpoint.send_tag(0, &ping_msg)?;
        info!("发送Ping #{}", seq);
        
        // 推进进度确保消息发送
        worker.progress();
        
        // 等待pong响应
        let timeout = Duration::from_secs(5);
        let pong_start = Instant::now();
        let mut received_pong = false;
        
        while pong_start.elapsed() < timeout {
            worker.progress();
            
            if let Some(received) = endpoint.try_receive_tag(0, &mut buffer)? {
                let data = &buffer[..received];
                
                match deserialize_message(data) {
                    Ok(Message::Pong { seq: pong_seq }) => {
                        if pong_seq == seq {
                            let latency = start_time.elapsed();
                            total_latency += latency;
                            successful_pings += 1;
                            received_pong = true;
                            info!("收到Pong #{}, RTT: {:?}", pong_seq, latency);
                            break;
                        }
                    }
                    Ok(other) => {
                        info!("收到意外消息: {:?}", other);
                    }
                    Err(e) => {
                        log::warn!("解析响应失败: {}", e);
                    }
                }
            }
            
            sleep(Duration::from_millis(1)).await;
        }
        
        if !received_pong {
            log::warn!("Ping #{} 超时", seq);
        }
        
        // 间隔1秒发送下一个ping
        if seq < PING_COUNT - 1 {
            sleep(Duration::from_secs(1)).await;
        }
    }

    // 发送断开连接消息
    let disconnect = Message::Disconnect;
    let disconnect_msg = serialize_message(&disconnect)?;
    endpoint.send_tag(0, &disconnect_msg)?;
    worker.progress();

    // 打印统计信息
    let avg_latency = if successful_pings > 0 {
        total_latency / successful_pings as u32
    } else {
        Duration::ZERO
    };

    info!("Ping-Pong测试完成:");
    info!("  总计: {} pings", PING_COUNT);
    info!("  成功: {} pings", successful_pings);
    info!("  失败: {} pings", PING_COUNT - successful_pings);
    info!("  成功率: {:.1}%", (successful_pings as f64 / PING_COUNT as f64) * 100.0);
    info!("  平均RTT: {:?}", avg_latency);
    info!("  消息大小: {} bytes", MESSAGE_SIZE);

    info!("Ping-Pong客户端退出");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();

    info!("=== UCX Ping-Pong 示例 ===");

    // 并发运行服务器和客户端
    let server_handle = tokio::spawn(run_server());
    let client_handle = tokio::spawn(run_client());

    // 等待两个任务完成
    let (server_result, client_result) = tokio::join!(server_handle, client_handle);

    match server_result {
        Ok(Ok(())) => info!("服务器正常退出"),
        Ok(Err(e)) => log::error!("服务器出错: {}", e),
        Err(e) => log::error!("服务器任务失败: {}", e),
    }

    match client_result {
        Ok(Ok(())) => info!("客户端正常退出"),
        Ok(Err(e)) => log::error!("客户端出错: {}", e),
        Err(e) => log::error!("客户端任务失败: {}", e),
    }

    info!("Ping-Pong示例结束");
    Ok(())
}
