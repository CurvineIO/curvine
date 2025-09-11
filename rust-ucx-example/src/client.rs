use anyhow::Result;
use clap::Parser;
use log::{error, info, warn};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use rust_ucx_example::{Context, ContextBuilder, Endpoint, Worker, WorkerBuilder};

use rust_ucx_example::{
    deserialize_message, serialize_message, ConnectionState, Message, PerfStats, UcxConfig,
};

/// UCX客户端命令行参数
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 服务器地址
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    server: SocketAddr,

    /// 传输协议
    #[arg(short, long, default_value = "tcp")]
    transport: String,

    /// 运行模式
    #[arg(short, long, default_value = "ping")]
    mode: String,

    /// 消息数量 (仅ping模式)
    #[arg(short, long, default_value = "10")]
    count: u64,

    /// 消息大小 (字节)
    #[arg(long, default_value = "1024")]
    size: usize,

    /// 发送间隔 (毫秒)
    #[arg(long, default_value = "1000")]
    interval: u64,

    /// 缓冲区大小
    #[arg(long, default_value = "65536")]
    buffer_size: usize,

    /// 是否启用详细日志
    #[arg(short, long)]
    verbose: bool,
}

/// UCX客户端实现
pub struct UcxClient {
    context: Context,
    worker: Worker,
    endpoint: Option<Endpoint>,
    config: UcxConfig,
    state: ConnectionState,
    stats: PerfStats,
}

impl UcxClient {
    /// 创建新的UCX客户端
    pub fn new(config: UcxConfig) -> Result<Self> {
        info!("初始化UCX客户端，配置: {:?}", config);

        // 创建UCX上下文
        let mut context_builder = ContextBuilder::new();
        context_builder.request_tag_feature();
        context_builder.request_stream_feature();
        
        let context = context_builder.build()?;
        info!("UCX上下文创建成功");

        // 创建工作器
        let worker = WorkerBuilder::new(&context).build()?;
        info!("UCX工作器创建成功");

        Ok(Self {
            context,
            worker,
            endpoint: None,
            config,
            state: ConnectionState::Disconnected,
            stats: PerfStats::new(),
        })
    }

    /// 连接到服务器
    pub async fn connect(&mut self, server_addr: SocketAddr) -> Result<()> {
        info!("连接到服务器: {}", server_addr);
        self.state = ConnectionState::Connecting;

        // 创建连接到服务器的端点
        let endpoint = self.worker.connect(server_addr).await?;
        info!("端点创建成功");

        // 等待连接建立
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 10;
        
        while attempts < MAX_ATTEMPTS {
            self.worker.progress();
            
            // 检查连接状态（这里简化处理）
            if attempts > 3 {
                break;
            }
            
            attempts += 1;
            sleep(Duration::from_millis(100)).await;
        }

        self.endpoint = Some(endpoint);
        self.state = ConnectionState::Connected;
        info!("连接到服务器成功");
        Ok(())
    }

    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        if self.state == ConnectionState::Connected {
            if let Some(endpoint) = &self.endpoint {
                // 发送断开连接消息
                let disconnect_msg = Message::Disconnect;
                self.send_message(disconnect_msg).await?;
            }
        }
        
        self.endpoint = None;
        self.state = ConnectionState::Disconnected;
        info!("已断开连接");
        Ok(())
    }

    /// 发送消息
    pub async fn send_message(&mut self, message: Message) -> Result<()> {
        if let Some(endpoint) = &self.endpoint {
            let data = serialize_message(&message)?;
            endpoint.send_tag(0, &data)?;
            self.stats.record_send(data.len());
            
            // 推进进度确保消息发送
            self.worker.progress();
            Ok(())
        } else {
            Err(anyhow::anyhow!("未连接到服务器"))
        }
    }

    /// 接收消息
    pub async fn receive_message(&mut self) -> Result<Option<Message>> {
        if let Some(endpoint) = &self.endpoint {
            let mut buffer = vec![0u8; self.config.buffer_size];
            
            // 推进进度
            self.worker.progress();
            
            // 尝试接收消息
            if let Some(received) = endpoint.try_receive_tag(0, &mut buffer)? {
                let data = &buffer[..received];
                self.stats.record_receive(data.len());
                
                match deserialize_message(data) {
                    Ok(message) => Ok(Some(message)),
                    Err(e) => {
                        warn!("反序列化消息失败: {}", e);
                        Ok(None)
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            Err(anyhow::anyhow!("未连接到服务器"))
        }
    }

    /// Ping测试模式
    pub async fn ping_test(&mut self, count: u64, message_size: usize, interval: Duration) -> Result<()> {
        info!("开始Ping测试: count={}, size={}, interval={:?}", count, message_size, interval);
        
        let mut successful_pings = 0;
        let mut total_latency = Duration::ZERO;
        
        for seq in 0..count {
            let ping_data = vec![0u8; message_size];
            let ping_msg = Message::Ping { seq, data: ping_data };
            
            let start_time = Instant::now();
            
            // 发送Ping
            self.send_message(ping_msg).await?;
            info!("发送Ping #{}", seq);
            
            // 等待Pong响应
            let mut received_pong = false;
            let timeout = Duration::from_secs(5);
            let pong_start = Instant::now();
            
            while pong_start.elapsed() < timeout {
                if let Some(message) = self.receive_message().await? {
                    match message {
                        Message::Pong { seq: pong_seq } => {
                            if pong_seq == seq {
                                let latency = start_time.elapsed();
                                total_latency += latency;
                                successful_pings += 1;
                                received_pong = true;
                                info!("收到Pong #{}, 延迟: {:?}", pong_seq, latency);
                                break;
                            }
                        }
                        other => {
                            info!("收到其他消息: {:?}", other);
                        }
                    }
                }
                
                // 短暂休眠
                sleep(Duration::from_millis(1)).await;
            }
            
            if !received_pong {
                warn!("Ping #{} 超时", seq);
            }
            
            // 等待下一次发送
            if seq < count - 1 {
                sleep(interval).await;
            }
        }
        
        // 打印统计信息
        let avg_latency = if successful_pings > 0 {
            total_latency / successful_pings as u32
        } else {
            Duration::ZERO
        };
        
        info!("Ping测试完成:");
        info!("  成功: {}/{}", successful_pings, count);
        info!("  平均延迟: {:?}", avg_latency);
        info!("  成功率: {:.2}%", (successful_pings as f64 / count as f64) * 100.0);
        
        Ok(())
    }

    /// 数据传输模式
    pub async fn data_transfer(&mut self, file_path: &str) -> Result<()> {
        info!("开始数据传输: {}", file_path);
        
        // 读取文件（这里简化为生成测试数据）
        let test_data = vec![0u8; 1024 * 1024]; // 1MB测试数据
        
        let data_msg = Message::Data {
            id: file_path.to_string(),
            payload: test_data,
        };
        
        let start_time = Instant::now();
        self.send_message(data_msg).await?;
        
        // 等待确认
        let timeout = Duration::from_secs(30);
        let ack_start = Instant::now();
        
        while ack_start.elapsed() < timeout {
            if let Some(message) = self.receive_message().await? {
                match message {
                    Message::Data { id, payload } => {
                        if id.starts_with("ack_") {
                            let transfer_time = start_time.elapsed();
                            info!("数据传输完成，耗时: {:?}, 响应: {}", 
                                  transfer_time, String::from_utf8_lossy(&payload));
                            return Ok(());
                        }
                    }
                    other => {
                        info!("收到其他消息: {:?}", other);
                    }
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
        
        warn!("数据传输超时");
        Ok(())
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> &PerfStats {
        &self.stats
    }

    /// 打印统计信息
    pub fn print_stats(&self) {
        println!("\n=== 客户端统计信息 ===");
        println!("发送消息数: {}", self.stats.messages_sent);
        println!("接收消息数: {}", self.stats.messages_received);
        println!("发送字节数: {}", self.stats.bytes_sent);
        println!("接收字节数: {}", self.stats.bytes_received);
        println!("吞吐量: {:.2} MB/s", self.stats.throughput_mbps());
        println!("消息速率: {:.2} msg/s", self.stats.message_rate());
        println!("连接状态: {:?}", self.state);
        println!("运行时间: {:?}", self.stats.start_time.elapsed());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // 初始化日志
    if args.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    }

    info!("启动UCX客户端");

    // 创建客户端配置
    let config = UcxConfig {
        bind_addr: "0.0.0.0:0".parse().unwrap(), // 客户端使用随机端口
        transport: args.transport,
        buffer_size: args.buffer_size,
        ..Default::default()
    };

    // 创建客户端
    let mut client = UcxClient::new(config)?;

    // 连接到服务器
    client.connect(args.server).await?;

    // 根据模式执行不同操作
    match args.mode.as_str() {
        "ping" => {
            let interval = Duration::from_millis(args.interval);
            client.ping_test(args.count, args.size, interval).await?;
        }
        "data" => {
            client.data_transfer("test_file.dat").await?;
        }
        "interactive" => {
            // 交互模式
            info!("进入交互模式，输入'quit'退出");
            loop {
                println!("请输入命令 (ping/data/stats/quit):");
                
                let mut input = String::new();
                if std::io::stdin().read_line(&mut input).is_err() {
                    break;
                }
                
                match input.trim() {
                    "ping" => {
                        client.ping_test(1, 1024, Duration::from_millis(0)).await?;
                    }
                    "data" => {
                        client.data_transfer("interactive_data").await?;
                    }
                    "stats" => {
                        client.print_stats();
                    }
                    "quit" => {
                        break;
                    }
                    _ => {
                        println!("未知命令");
                    }
                }
            }
        }
        _ => {
            error!("未知模式: {}", args.mode);
            return Err(anyhow::anyhow!("不支持的运行模式"));
        }
    }

    // 断开连接
    client.disconnect().await?;
    
    // 打印最终统计信息
    client.print_stats();

    info!("UCX客户端已停止");
    Ok(())
}
