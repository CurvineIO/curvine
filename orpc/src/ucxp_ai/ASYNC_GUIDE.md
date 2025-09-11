# UCX + Tokio 异步编程指南

这个指南展示了如何将 UCX (Unified Communication X) 与 Tokio 异步运行时结合使用，实现高性能的异步网络通信。

## 🚀 快速开始

### 1. 基本设置

```rust
use orpc::ucx::{AsyncUcxManager, AsyncUcxClient, AsyncMessage};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 创建异步 UCX 管理器
    let manager = AsyncUcxManager::new()?;
    
    // 创建客户端
    let mut client = manager.create_client();
    
    // 连接到服务器
    client.connect("127.0.0.1:8080".parse()?).await?;
    
    // 发送消息
    let message = AsyncMessage::text(0, "Hello, async UCX!");
    client.send_message(message.tag, &message.data).await?;
    
    // 接收响应
    let response = client.receive_message(0, 1024).await?;
    println!("收到: {}", String::from_utf8_lossy(&response));
    
    // 关闭管理器
    manager.shutdown().await?;
    Ok(())
}
```

### 2. 运行示例

```bash
# 运行异步客户端
cargo run --example async_tcp_client

# 运行异步服务器
cargo run --example async_tcp_server
```

## 🏗️ 核心架构

### AsyncUcxManager
- **作用**: 管理 UCX 上下文和后台进度任务
- **特点**: 自动处理 UCX 的进度轮询
- **生命周期**: 整个应用程序期间保持活跃

```rust
let manager = AsyncUcxManager::new()?;
// 管理器自动启动后台任务处理 UCX 进度
```

### AsyncUcxClient
- **作用**: 异步 UCX 客户端连接
- **特点**: 提供 async/await API
- **操作**: 连接、发送、接收消息

```rust
let mut client = manager.create_client();
await client.connect(server_addr)?;
```

### AsyncMessage
- **作用**: 消息封装
- **特点**: 包含标签、数据和时间戳
- **工厂方法**: `text()` 创建文本消息

```rust
let msg = AsyncMessage::text(123, "Hello World");
```

## 🔄 异步模式

### 1. 发送并等待响应

```rust
async fn ping_pong(client: &AsyncUcxClient) -> Result<Duration> {
    let start = Instant::now();
    
    // 发送 ping
    client.send_message(1, b"PING").await?;
    
    // 等待 pong
    let response = client.receive_message(1, 1024).await?;
    
    if response == b"PONG" {
        Ok(start.elapsed())
    } else {
        Err(anyhow!("无效响应"))
    }
}
```

### 2. 并发发送多条消息

```rust
async fn burst_send(client: &AsyncUcxClient, count: u32) -> Result<()> {
    let mut tasks = Vec::new();
    
    for i in 0..count {
        let task = async move {
            client.send_message(i as u64, &format!("Message {}", i).as_bytes()).await
        };
        tasks.push(task);
    }
    
    // 等待所有发送完成
    let results = futures::future::join_all(tasks).await;
    
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(_) => println!("消息 {} 发送成功", i),
            Err(e) => println!("消息 {} 发送失败: {}", i, e),
        }
    }
    
    Ok(())
}
```

### 3. 带超时的操作

```rust
use tokio::time::{timeout, Duration};

async fn send_with_timeout(client: &AsyncUcxClient) -> Result<()> {
    let operation = client.send_message(1, b"Hello");
    
    match timeout(Duration::from_secs(5), operation).await {
        Ok(Ok(_)) => println!("发送成功"),
        Ok(Err(e)) => println!("发送失败: {}", e),
        Err(_) => println!("操作超时"),
    }
    
    Ok(())
}
```

## 🏢 服务器模式

### 1. 基本服务器结构

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let manager = AsyncUcxManager::new()?;
    
    // 启动服务器 (简化版)
    let server = AsyncUcxServer::new(
        Arc::new(manager.worker.clone()),
        "127.0.0.1:8080".parse()?
    );
    
    server.start(|client| async move {
        handle_client(client).await
    }).await?;
    
    Ok(())
}

async fn handle_client(client: AsyncUcxClient) -> Result<()> {
    loop {
        let data = client.receive_message(0, 1024).await?;
        let message = String::from_utf8_lossy(&data);
        
        match message.as_ref() {
            "PING" => {
                client.send_message(0, b"PONG").await?;
            }
            msg if msg.starts_with("ECHO:") => {
                let response = format!("REPLY:{}", &msg[5..]);
                client.send_message(0, response.as_bytes()).await?;
            }
            _ => {
                let response = format!("RECEIVED:{}", message);
                client.send_message(0, response.as_bytes()).await?;
            }
        }
    }
}
```

## ⚡ 性能优化

### 1. 批量操作

```rust
async fn batch_operations(client: &AsyncUcxClient) -> Result<()> {
    const BATCH_SIZE: usize = 100;
    let mut send_tasks = Vec::with_capacity(BATCH_SIZE);
    
    // 批量发送
    for i in 0..BATCH_SIZE {
        let data = format!("Batch message {}", i);
        let task = client.send_message(i as u64, data.as_bytes());
        send_tasks.push(task);
    }
    
    // 等待所有发送完成
    let _results = futures::future::try_join_all(send_tasks).await?;
    
    println!("批量发送 {} 条消息完成", BATCH_SIZE);
    Ok(())
}
```

### 2. 连接池

```rust
use std::sync::Arc;
use tokio::sync::Semaphore;

struct UcxConnectionPool {
    manager: AsyncUcxManager,
    semaphore: Arc<Semaphore>,
    max_connections: usize,
}

impl UcxConnectionPool {
    pub fn new(max_connections: usize) -> Result<Self> {
        Ok(Self {
            manager: AsyncUcxManager::new()?,
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_connections,
        })
    }
    
    pub async fn get_client(&self) -> Result<(AsyncUcxClient, tokio::sync::SemaphorePermit)> {
        let permit = self.semaphore.acquire().await.unwrap();
        let client = self.manager.create_client();
        Ok((client, permit))
    }
}
```

### 3. 消息优先级

```rust
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
enum MessagePriority {
    High,
    Normal,
    Low,
}

struct PriorityMessage {
    priority: MessagePriority,
    tag: u64,
    data: Vec<u8>,
}

async fn priority_sender(client: &AsyncUcxClient) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<PriorityMessage>();
    
    // 消息分发任务
    tokio::spawn(async move {
        let mut high_priority = Vec::new();
        let mut normal_priority = Vec::new();
        let mut low_priority = Vec::new();
        
        while let Some(msg) = rx.recv().await {
            match msg.priority {
                MessagePriority::High => high_priority.push(msg),
                MessagePriority::Normal => normal_priority.push(msg),
                MessagePriority::Low => low_priority.push(msg),
            }
            
            // 优先发送高优先级消息
            if let Some(high_msg) = high_priority.pop() {
                let _ = client.send_message(high_msg.tag, &high_msg.data).await;
            } else if let Some(normal_msg) = normal_priority.pop() {
                let _ = client.send_message(normal_msg.tag, &normal_msg.data).await;
            } else if let Some(low_msg) = low_priority.pop() {
                let _ = client.send_message(low_msg.tag, &low_msg.data).await;
            }
        }
    });
    
    Ok(())
}
```

## 🛠️ 错误处理

### 1. 重试机制

```rust
use tokio::time::{sleep, Duration};

async fn send_with_retry(
    client: &AsyncUcxClient, 
    tag: u64, 
    data: &[u8], 
    max_retries: u32
) -> Result<()> {
    let mut attempts = 0;
    
    loop {
        match client.send_message(tag, data).await {
            Ok(_) => return Ok(()),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                println!("发送失败，重试 {}/{}: {}", attempts, max_retries, e);
                sleep(Duration::from_millis(100 * attempts as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 2. 连接监控

```rust
async fn connection_monitor(client: &AsyncUcxClient) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        
        // 发送心跳消息
        match timeout(Duration::from_secs(5), client.send_message(999, b"HEARTBEAT")).await {
            Ok(Ok(_)) => println!("心跳正常"),
            Ok(Err(e)) => {
                println!("心跳失败: {}", e);
                // 可以在这里触发重连逻辑
                break;
            }
            Err(_) => {
                println!("心跳超时");
                break;
            }
        }
    }
    
    Ok(())
}
```

## 📊 监控和统计

### 1. 性能指标

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct PerformanceMetrics {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    errors: AtomicU64,
}

impl PerformanceMetrics {
    fn record_send(&self, bytes: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    
    fn record_receive(&self, bytes: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    
    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    
    fn print_stats(&self) {
        println!("发送消息: {}", self.messages_sent.load(Ordering::Relaxed));
        println!("接收消息: {}", self.messages_received.load(Ordering::Relaxed));
        println!("发送字节: {}", self.bytes_sent.load(Ordering::Relaxed));
        println!("接收字节: {}", self.bytes_received.load(Ordering::Relaxed));
        println!("错误次数: {}", self.errors.load(Ordering::Relaxed));
    }
}
```

## 🔧 配置和调优

### 1. UCX 参数配置

```rust
// 在创建 UCX 上下文时可以设置环境变量
std::env::set_var("UCX_NET_DEVICES", "eth0");  // 指定网络设备
std::env::set_var("UCX_TLS", "tcp");           // 指定传输层
std::env::set_var("UCX_TCP_RX_BUFS", "16k");   // 设置接收缓冲区
```

### 2. Tokio 运行时配置

```rust
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    // 使用多线程运行时，4个工作线程
    // 对于 I/O 密集型应用效果更好
}

// 或者手动创建运行时
fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("ucx-async")
        .enable_all()
        .build()?;
        
    rt.block_on(async_main())
}
```

## 🚨 注意事项

1. **资源管理**: 始终在适当的时候调用 `manager.shutdown()`
2. **标签匹配**: 发送和接收操作必须使用相同的标签
3. **缓冲区大小**: 接收时提供足够大的缓冲区
4. **错误处理**: 所有异步操作都应该适当处理错误
5. **超时设置**: 使用 `tokio::time::timeout` 避免无限等待
6. **背压处理**: 在高负载情况下考虑流量控制

## 📚 更多资源

- [UCX 官方文档](https://openucx.org/)
- [Tokio 官方指南](https://tokio.rs/)
- [Rust 异步编程书](https://rust-lang.github.io/async-book/)

这个异步封装提供了在 Rust 中使用 UCX 进行高性能网络编程的完整解决方案，结合了 UCX 的性能优势和 Tokio 的异步编程模型。
