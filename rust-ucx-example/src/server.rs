use anyhow::Result;
use clap::Parser;
use log::{error, info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use rust_ucx_example::{Context, ContextBuilder, Endpoint, Listener, Worker, WorkerBuilder};

use rust_ucx_example::{deserialize_message, serialize_message, Message, PerfStats, UcxConfig};

/// UCX服务器命令行参数
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 监听地址
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    /// 传输协议
    #[arg(short, long, default_value = "tcp")]
    transport: String,

    /// 缓冲区大小 (字节)
    #[arg(long, default_value = "65536")]
    buffer_size: usize,

    /// 是否启用详细日志
    #[arg(short, long)]
    verbose: bool,
}

/// 客户端连接信息
struct ClientConnection {
    endpoint: Endpoint,
    addr: SocketAddr,
    stats: PerfStats,
}

/// UCX服务器实现
pub struct UcxServer {
    context: Context,
    worker: Worker,
    listener: Option<Listener>,
    config: UcxConfig,
    clients: Arc<Mutex<HashMap<String, ClientConnection>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl UcxServer {
    /// 创建新的UCX服务器
    pub async fn new(config: UcxConfig) -> Result<Self> {
        info!("初始化UCX服务器，配置: {:?}", config);

        // 创建UCX上下文
        let mut context_builder = ContextBuilder::new();
        
        // 配置传输层
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
            listener: None,
            config,
            clients: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx: None,
        })
    }

    /// 启动服务器监听
    pub async fn start(&mut self) -> Result<()> {
        info!("启动服务器监听: {}", self.config.bind_addr);

        // 创建监听器
        let listener = self.worker.create_listener(self.config.bind_addr).await?;
        info!("监听器创建成功: {}", listener.socket_address()?);

        self.listener = Some(listener);

        // 创建关闭信号通道
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // 主服务循环
        loop {
            tokio::select! {
                // 检查关闭信号
                _ = shutdown_rx.recv() => {
                    info!("收到关闭信号，停止服务器");
                    break;
                }
                
                // 处理新连接
                result = self.accept_connection() => {
                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            error!("接受连接失败: {}", e);
                        }
                    }
                }
                
                // 处理现有连接的消息
                _ = self.process_client_messages() => {}
            }

            // 推进UCX进度
            self.worker.progress();
            
            // 短暂休眠避免CPU占用过高
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }

        Ok(())
    }

    /// 接受新的客户端连接
    async fn accept_connection(&self) -> Result<()> {
        if let Some(listener) = &self.listener {
            // 检查是否有新连接
            if let Some(conn_request) = listener.try_accept()? {
                let endpoint = conn_request.accept()?;
                let addr = endpoint.peer_address()?;
                
                let client_id = format!("{}:{}", addr.ip(), addr.port());
                info!("接受新客户端连接: {}", client_id);

                let client = ClientConnection {
                    endpoint,
                    addr,
                    stats: PerfStats::new(),
                };

                let mut clients = self.clients.lock().await;
                clients.insert(client_id.clone(), client);
                
                info!("当前连接数: {}", clients.len());
            }
        }
        Ok(())
    }

    /// 处理所有客户端的消息
    async fn process_client_messages(&self) {
        let mut clients = self.clients.lock().await;
        let mut to_remove = Vec::new();

        for (client_id, client) in clients.iter_mut() {
            match self.handle_client_messages(client_id, client).await {
                Ok(_) => {}
                Err(e) => {
                    error!("处理客户端 {} 消息失败: {}", client_id, e);
                    to_remove.push(client_id.clone());
                }
            }
        }

        // 移除断开的客户端
        for client_id in to_remove {
            clients.remove(&client_id);
            info!("客户端 {} 已断开连接", client_id);
        }
    }

    /// 处理单个客户端的消息
    async fn handle_client_messages(
        &self,
        client_id: &str,
        client: &mut ClientConnection,
    ) -> Result<()> {
        let mut buffer = vec![0u8; self.config.buffer_size];

        // 尝试接收消息
        if let Some(received) = client.endpoint.try_receive_tag(0, &mut buffer)? {
            let data = &buffer[..received];
            client.stats.record_receive(data.len());

            match deserialize_message(data) {
                Ok(message) => {
                    info!("收到客户端 {} 消息: {:?}", client_id, message);
                    self.handle_message(client_id, client, message).await?;
                }
                Err(e) => {
                    warn!("解析客户端 {} 消息失败: {}", client_id, e);
                }
            }
        }

        Ok(())
    }

    /// 处理具体的消息类型
    async fn handle_message(
        &self,
        client_id: &str,
        client: &mut ClientConnection,
        message: Message,
    ) -> Result<()> {
        match message {
            Message::Ping { seq, data } => {
                info!("收到客户端 {} Ping: seq={}, data_len={}", client_id, seq, data.len());
                
                // 发送Pong响应
                let pong = Message::Pong { seq };
                self.send_message_to_client(client, pong).await?;
            }
            
            Message::Data { id, payload } => {
                info!("收到客户端 {} 数据: id={}, size={} bytes", client_id, id, payload.len());
                
                // 可以在这里处理数据，例如存储、转发等
                // 发送确认消息
                let ack = Message::Data {
                    id: format!("ack_{}", id),
                    payload: b"acknowledged".to_vec(),
                };
                self.send_message_to_client(client, ack).await?;
            }
            
            Message::Disconnect => {
                info!("客户端 {} 请求断开连接", client_id);
                return Err(anyhow::anyhow!("客户端断开连接"));
            }
            
            _ => {
                warn!("未知消息类型来自客户端 {}", client_id);
            }
        }
        Ok(())
    }

    /// 发送消息给指定客户端
    async fn send_message_to_client(
        &self,
        client: &mut ClientConnection,
        message: Message,
    ) -> Result<()> {
        let data = serialize_message(&message)?;
        client.endpoint.send_tag(0, &data)?;
        client.stats.record_send(data.len());
        Ok(())
    }

    /// 广播消息给所有客户端
    pub async fn broadcast_message(&self, message: Message) -> Result<()> {
        let data = serialize_message(&message)?;
        let mut clients = self.clients.lock().await;
        
        for (client_id, client) in clients.iter_mut() {
            match client.endpoint.send_tag(0, &data) {
                Ok(_) => {
                    client.stats.record_send(data.len());
                    info!("广播消息到客户端: {}", client_id);
                }
                Err(e) => {
                    error!("广播消息到客户端 {} 失败: {}", client_id, e);
                }
            }
        }
        Ok(())
    }

    /// 获取服务器统计信息
    pub async fn get_stats(&self) -> HashMap<String, PerfStats> {
        let clients = self.clients.lock().await;
        clients.iter().map(|(id, client)| (id.clone(), client.stats.clone())).collect()
    }

    /// 关闭服务器
    pub async fn shutdown(&self) -> Result<()> {
        if let Some(tx) = &self.shutdown_tx {
            tx.send(()).await?;
        }
        info!("服务器关闭");
        Ok(())
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

    info!("启动UCX服务器");

    // 创建服务器配置
    let config = UcxConfig {
        bind_addr: args.bind,
        transport: args.transport,
        buffer_size: args.buffer_size,
        ..Default::default()
    };

    // 创建并启动服务器
    let mut server = UcxServer::new(config).await?;
    
    // 设置Ctrl+C信号处理
    let server_shutdown = Arc::new(Mutex::new(None));
    let server_shutdown_clone = server_shutdown.clone();
    
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("监听Ctrl+C信号失败");
        info!("收到Ctrl+C信号，开始关闭服务器...");
        if let Some(server) = server_shutdown_clone.lock().await.as_ref() {
            server.shutdown().await.expect("关闭服务器失败");
        }
    });

    // 启动服务器
    server.start().await?;

    info!("UCX服务器已停止");
    Ok(())
}
