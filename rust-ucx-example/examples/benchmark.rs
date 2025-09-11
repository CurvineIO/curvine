use anyhow::Result;
use log::info;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use rust_ucx_example::{Context, ContextBuilder, Worker, WorkerBuilder};

use rust_ucx_example::{serialize_message, deserialize_message, Message, PerfStats};

/// UCX性能基准测试
/// 测试不同消息大小下的延迟和吞吐量

const SERVER_PORT: u16 = 9998;
const WARMUP_ITERATIONS: u64 = 100;
const BENCHMARK_ITERATIONS: u64 = 1000;

// 测试用的不同消息大小 (字节)
const MESSAGE_SIZES: &[usize] = &[
    64,       // 小消息
    512,      // 中等消息  
    1024,     // 1KB
    4096,     // 4KB
    16384,    // 16KB
    65536,    // 64KB
    262144,   // 256KB
    1048576,  // 1MB
];

#[derive(Debug)]
struct BenchmarkResult {
    message_size: usize,
    iterations: u64,
    total_time: Duration,
    avg_latency: Duration,
    min_latency: Duration,
    max_latency: Duration,
    throughput_mbps: f64,
    message_rate: f64,
}

async fn run_benchmark_server() -> Result<()> {
    info!("启动性能测试服务器");

    let mut context_builder = ContextBuilder::new();
    context_builder.request_tag_feature();
    let context = context_builder.build()?;
    let worker = WorkerBuilder::new(&context).build()?;

    let bind_addr = format!("127.0.0.1:{}", SERVER_PORT).parse()?;
    let listener = worker.create_listener(bind_addr).await?;
    info!("服务器监听: {}", listener.socket_address()?);

    // 等待客户端连接
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
    let mut buffer = vec![0u8; 2 * 1024 * 1024]; // 2MB缓冲区
    let mut total_messages = 0u64;

    info!("开始处理基准测试消息...");
    
    loop {
        worker.progress();
        
        if let Some(received) = endpoint.try_receive_tag(0, &mut buffer)? {
            let data = &buffer[..received];
            
            match deserialize_message(data) {
                Ok(Message::Ping { seq, data: ping_data }) => {
                    total_messages += 1;
                    
                    // 立即回复pong
                    let pong = Message::Pong { seq };
                    let pong_data = serialize_message(&pong)?;
                    endpoint.send_tag(0, &pong_data)?;
                    
                    if total_messages % 1000 == 0 {
                        info!("已处理 {} 条消息", total_messages);
                    }
                }
                Ok(Message::Disconnect) => {
                    info!("收到断开连接，总计处理 {} 条消息", total_messages);
                    break;
                }
                Ok(_) => {
                    // 忽略其他消息类型
                }
                Err(e) => {
                    log::warn!("解析消息失败: {}", e);
                }
            }
        }
        
        sleep(Duration::from_nanos(100)).await; // 减少CPU使用
    }

    info!("性能测试服务器退出");
    Ok(())
}

async fn run_benchmark_client() -> Result<()> {
    info!("启动性能测试客户端");
    
    // 等待服务器启动
    sleep(Duration::from_millis(1000)).await;

    let mut context_builder = ContextBuilder::new();
    context_builder.request_tag_feature();
    let context = context_builder.build()?;
    let worker = WorkerBuilder::new(&context).build()?;

    let server_addr = format!("127.0.0.1:{}", SERVER_PORT).parse()?;
    let endpoint = worker.connect(server_addr).await?;
    info!("连接到服务器: {}", server_addr);

    // 等待连接稳定
    sleep(Duration::from_millis(200)).await;

    let mut buffer = vec![0u8; 2 * 1024 * 1024]; // 2MB缓冲区
    let mut results = Vec::new();

    for &message_size in MESSAGE_SIZES {
        info!("开始测试消息大小: {} bytes", message_size);
        
        // 预热阶段
        info!("预热阶段: {} 次迭代", WARMUP_ITERATIONS);
        for seq in 0..WARMUP_ITERATIONS {
            let ping_data = vec![42u8; message_size];
            let ping = Message::Ping { seq, data: ping_data };
            
            let ping_msg = serialize_message(&ping)?;
            endpoint.send_tag(0, &ping_msg)?;
            worker.progress();
            
            // 等待响应
            let mut received_response = false;
            let timeout = Duration::from_millis(1000);
            let start = Instant::now();
            
            while start.elapsed() < timeout && !received_response {
                worker.progress();
                
                if let Some(received) = endpoint.try_receive_tag(0, &mut buffer)? {
                    if let Ok(Message::Pong { .. }) = deserialize_message(&buffer[..received]) {
                        received_response = true;
                    }
                }
                
                if !received_response {
                    sleep(Duration::from_nanos(100)).await;
                }
            }
        }
        
        info!("预热完成，开始正式测试: {} 次迭代", BENCHMARK_ITERATIONS);
        
        // 正式测试阶段
        let mut latencies = Vec::with_capacity(BENCHMARK_ITERATIONS as usize);
        let test_start = Instant::now();
        
        for seq in 0..BENCHMARK_ITERATIONS {
            let ping_data = vec![42u8; message_size];
            let ping = Message::Ping { seq, data: ping_data };
            
            let request_start = Instant::now();
            
            // 发送ping
            let ping_msg = serialize_message(&ping)?;
            endpoint.send_tag(0, &ping_msg)?;
            worker.progress();
            
            // 等待pong响应
            let mut received_response = false;
            let timeout = Duration::from_millis(5000);
            let response_start = Instant::now();
            
            while response_start.elapsed() < timeout && !received_response {
                worker.progress();
                
                if let Some(received) = endpoint.try_receive_tag(0, &mut buffer)? {
                    if let Ok(Message::Pong { seq: pong_seq }) = deserialize_message(&buffer[..received]) {
                        if pong_seq == seq {
                            let latency = request_start.elapsed();
                            latencies.push(latency);
                            received_response = true;
                        }
                    }
                }
                
                if !received_response {
                    sleep(Duration::from_nanos(100)).await;
                }
            }
            
            if !received_response {
                log::warn!("序列号 {} 超时", seq);
            }
            
            // 打印进度
            if seq % (BENCHMARK_ITERATIONS / 10) == 0 && seq > 0 {
                info!("进度: {}/{}", seq, BENCHMARK_ITERATIONS);
            }
        }
        
        let total_time = test_start.elapsed();
        
        // 计算统计信息
        if !latencies.is_empty() {
            let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;
            let min_latency = *latencies.iter().min().unwrap();
            let max_latency = *latencies.iter().max().unwrap();
            
            let total_bytes = (message_size * latencies.len() * 2) as f64; // 双向传输
            let throughput_mbps = (total_bytes / (1024.0 * 1024.0)) / total_time.as_secs_f64();
            let message_rate = latencies.len() as f64 / total_time.as_secs_f64();
            
            let result = BenchmarkResult {
                message_size,
                iterations: latencies.len() as u64,
                total_time,
                avg_latency,
                min_latency,
                max_latency,
                throughput_mbps,
                message_rate,
            };
            
            results.push(result);
            
            info!("消息大小 {} bytes 测试完成:", message_size);
            info!("  成功迭代: {}/{}", latencies.len(), BENCHMARK_ITERATIONS);
            info!("  平均延迟: {:?}", avg_latency);
            info!("  最小延迟: {:?}", min_latency);
            info!("  最大延迟: {:?}", max_latency);
            info!("  吞吐量: {:.2} MB/s", throughput_mbps);
            info!("  消息速率: {:.2} msg/s", message_rate);
        }
        
        // 测试间隔
        sleep(Duration::from_millis(500)).await;
    }

    // 发送断开连接消息
    let disconnect = Message::Disconnect;
    let disconnect_msg = serialize_message(&disconnect)?;
    endpoint.send_tag(0, &disconnect_msg)?;
    worker.progress();

    // 打印汇总结果
    print_benchmark_summary(&results);

    info!("性能测试客户端退出");
    Ok(())
}

fn print_benchmark_summary(results: &[BenchmarkResult]) {
    println!("\n=== UCX 性能基准测试结果 ===");
    println!();
    println!("{:>10} {:>12} {:>12} {:>12} {:>12} {:>12} {:>15}",
             "消息大小", "迭代次数", "平均延迟", "最小延迟", "最大延迟", "吞吐量", "消息速率");
    println!("{:>10} {:>12} {:>12} {:>12} {:>12} {:>12} {:>15}",
             "(bytes)", "(次)", "(μs)", "(μs)", "(μs)", "(MB/s)", "(msg/s)");
    println!("{:-<10} {:-<12} {:-<12} {:-<12} {:-<12} {:-<12} {:-<15}",
             "", "", "", "", "", "", "");

    for result in results {
        println!("{:>10} {:>12} {:>12.1} {:>12.1} {:>12.1} {:>12.2} {:>15.0}",
                 result.message_size,
                 result.iterations,
                 result.avg_latency.as_micros() as f64,
                 result.min_latency.as_micros() as f64,
                 result.max_latency.as_micros() as f64,
                 result.throughput_mbps,
                 result.message_rate);
    }
    
    println!();
    
    // 找出最佳性能点
    if let Some(best_latency) = results.iter().min_by_key(|r| r.avg_latency) {
        println!("🚀 最低延迟: {} bytes @ {:.1} μs", 
                 best_latency.message_size, 
                 best_latency.avg_latency.as_micros() as f64);
    }
    
    if let Some(best_throughput) = results.iter().max_by(|a, b| a.throughput_mbps.partial_cmp(&b.throughput_mbps).unwrap()) {
        println!("🔥 最高吞吐量: {} bytes @ {:.2} MB/s", 
                 best_throughput.message_size, 
                 best_throughput.throughput_mbps);
    }
    
    if let Some(best_rate) = results.iter().max_by(|a, b| a.message_rate.partial_cmp(&b.message_rate).unwrap()) {
        println!("⚡ 最高消息速率: {} bytes @ {:.0} msg/s", 
                 best_rate.message_size, 
                 best_rate.message_rate);
    }
    
    println!();
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();

    println!("=== UCX 性能基准测试 ===");
    println!("测试配置:");
    println!("  预热迭代: {}", WARMUP_ITERATIONS);
    println!("  测试迭代: {}", BENCHMARK_ITERATIONS);
    println!("  消息大小: {:?} bytes", MESSAGE_SIZES);
    println!();

    // 并发运行服务器和客户端
    let server_handle = tokio::spawn(run_benchmark_server());
    let client_handle = tokio::spawn(run_benchmark_client());

    // 等待客户端完成，然后等待服务器
    match client_handle.await {
        Ok(Ok(())) => info!("客户端测试完成"),
        Ok(Err(e)) => log::error!("客户端出错: {}", e),
        Err(e) => log::error!("客户端任务失败: {}", e),
    }

    // 给服务器一点时间清理
    sleep(Duration::from_millis(500)).await;

    match server_handle.await {
        Ok(Ok(())) => info!("服务器正常退出"),
        Ok(Err(e)) => log::error!("服务器出错: {}", e),
        Err(e) => log::error!("服务器任务失败: {}", e),
    }

    println!("基准测试完成！");
    Ok(())
}
