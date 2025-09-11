# UCX Worker 封装使用指南

这个 UCX Worker 封装提供了一个完整、易用的 UCX 网络通信接口，支持高性能的消息传递和连接管理。

## 🚀 快速开始

### 基本使用

```rust
use orpc::ucx::{UcxWorker, WorkerConfig, ThreadMode};

fn main() -> anyhow::Result<()> {
    // 1. 创建配置
    let config = WorkerConfig {
        thread_mode: ThreadMode::Single,
        buffer_size: 64 * 1024,
        enable_tag: true,
        enable_stream: true,
        enable_rma: false,
        progress_interval_us: 100,
    };
    
    // 2. 创建 Worker
    let mut worker = UcxWorker::new(config)?;
    
    // 3. 连接到远程地址
    let endpoint_id = worker.connect("127.0.0.1:8080".parse()?)?;
    
    // 4. 发送消息
    worker.send_tag(&endpoint_id, 123, b"Hello, UCX!")?;
    
    // 5. 接收消息
    let mut buffer = vec![0u8; 1024];
    if let Ok(Some(bytes)) = worker.try_receive_tag(&endpoint_id, 123, &mut buffer) {
        println!("收到: {}", String::from_utf8_lossy(&buffer[..bytes]));
    }
    
    Ok(())
}
```

## 🏗️ 核心组件

### 1. WorkerConfig - 配置结构

```rust
let config = WorkerConfig {
    thread_mode: ThreadMode::Single,      // 线程模式
    buffer_size: 64 * 1024,              // 缓冲区大小 (字节)
    enable_tag: true,                     // 启用标签消息
    enable_stream: true,                  // 启用流功能
    enable_rma: false,                    // 启用远程内存访问
    progress_interval_us: 100,            // 进度轮询间隔 (微秒)
};
```

### 2. UcxWorker - 主要工作器

核心功能：
- **连接管理**: `connect()`, `disconnect()`, `is_connected()`
- **消息传递**: `send_tag()`, `receive_tag()`, `try_receive_tag()`
- **广播**: `broadcast_tag()`
- **监听**: `create_listener()`
- **进度处理**: `progress()`, `run_progress_loop()`
- **统计信息**: `stats()`

### 3. WorkerStats - 统计信息

自动跟踪：
- 消息发送/接收数量
- 字节传输量
- 连接数
- 错误数
- 吞吐量和消息速率

## 📋 API 详解

### 连接管理

```rust
// 连接到远程地址
let endpoint_id = worker.connect("192.168.1.100:9000".parse()?)?;

// 检查连接状态
if worker.is_connected(&endpoint_id) {
    println!("连接活跃");
}

// 获取所有连接
let endpoints = worker.get_endpoint_ids();
for endpoint in endpoints {
    println!("活跃连接: {}", endpoint);
}

// 断开特定连接
worker.disconnect(&endpoint_id)?;

// 清理所有连接
worker.cleanup_all();
```

### 消息传递

```rust
// 发送标签消息
let tag = 100;
let message = b"Hello World";
worker.send_tag(&endpoint_id, tag, message)?;

// 接收标签消息 (阻塞)
let mut buffer = vec![0u8; 1024];
let bytes_received = worker.receive_tag(&endpoint_id, tag, &mut buffer)?;

// 非阻塞接收
if let Some(bytes) = worker.try_receive_tag(&endpoint_id, tag, &mut buffer)? {
    let message = String::from_utf8_lossy(&buffer[..bytes]);
    println!("收到: {}", message);
}

// 广播消息到所有连接
worker.broadcast_tag(200, b"广播消息")?;
```

### 进度处理

```rust
// 单次进度处理
let operations_processed = worker.progress();

// 运行进度循环
let total_ops = worker.run_progress_loop(Some(Duration::from_secs(5)));
println!("5秒内处理了 {} 个操作", total_ops);
```

### 监听服务器

```rust
// 创建监听器
let bind_addr = "0.0.0.0:8080".parse()?;
worker.create_listener(bind_addr)?;
println!("监听: {}", bind_addr);

// 在循环中处理连接和消息
loop {
    worker.progress();
    
    // 处理现有连接的消息
    let endpoints = worker.get_endpoint_ids();
    for endpoint_id in endpoints {
        if let Ok(Some(bytes)) = worker.try_receive_tag(&endpoint_id, 0, &mut buffer) {
            // 处理收到的消息
            handle_message(&worker, &endpoint_id, &buffer[..bytes]);
        }
    }
    
    std::thread::sleep(Duration::from_millis(1));
}
```

## 📊 统计和监控

```rust
// 获取统计信息
let stats = worker.stats();

// 打印详细统计
stats.print();

// 获取特定指标
println!("吞吐量: {:.2} MB/s", stats.throughput_mbps());
println!("消息速率: {:.2} msg/s", stats.message_rate());
println!("发送消息数: {}", stats.messages_sent.load(Ordering::Relaxed));
```

## 🎯 使用场景

### 1. 客户端应用

```rust
fn create_client() -> Result<()> {
    let config = WorkerConfig::default();
    let worker = UcxWorker::new(config)?;
    
    // 连接到服务器
    let server_endpoint = worker.connect("server.example.com:8080".parse()?)?;
    
    // 发送请求
    worker.send_tag(&server_endpoint, 1, b"GET /api/data")?;
    
    // 等待响应
    let mut buffer = vec![0u8; 4096];
    let response_size = worker.receive_tag(&server_endpoint, 1, &mut buffer)?;
    
    println!("服务器响应: {}", String::from_utf8_lossy(&buffer[..response_size]));
    Ok(())
}
```

### 2. 服务器应用

```rust
fn create_server() -> Result<()> {
    let config = WorkerConfig {
        buffer_size: 128 * 1024, // 更大的缓冲区
        ..Default::default()
    };
    let mut worker = UcxWorker::new(config)?;
    
    // 创建监听器
    worker.create_listener("0.0.0.0:8080".parse()?)?;
    
    let mut buffer = vec![0u8; worker.config().buffer_size];
    
    // 服务器主循环
    loop {
        worker.progress();
        
        // 处理所有客户端消息
        for endpoint_id in worker.get_endpoint_ids() {
            if let Ok(Some(bytes)) = worker.try_receive_tag(&endpoint_id, 1, &mut buffer) {
                let request = String::from_utf8_lossy(&buffer[..bytes]);
                
                // 处理请求并发送响应
                let response = process_request(&request);
                worker.send_tag(&endpoint_id, 1, response.as_bytes())?;
            }
        }
        
        std::thread::sleep(Duration::from_millis(1));
    }
}
```

### 3. 点对点通信

```rust
fn peer_to_peer() -> Result<()> {
    let worker = UcxWorker::new(WorkerConfig::default())?;
    
    // 同时作为客户端和服务器
    worker.create_listener("0.0.0.0:8080".parse()?)?;
    let peer_endpoint = worker.connect("peer.example.com:8080".parse()?)?;
    
    // 发送和接收消息
    worker.send_tag(&peer_endpoint, 100, b"Hello Peer")?;
    
    let mut buffer = vec![0u8; 1024];
    if let Ok(Some(bytes)) = worker.try_receive_tag(&peer_endpoint, 100, &mut buffer) {
        println!("收到对等节点消息: {}", String::from_utf8_lossy(&buffer[..bytes]));
    }
    
    Ok(())
}
```

## 🛠️ 运行示例

### 交互式示例

```bash
cargo run --example ucx_worker_example
```

这个示例提供了一个交互式命令行界面，支持：
- `connect <地址:端口>` - 连接到远程地址
- `send <endpoint_id> <tag> <消息>` - 发送消息
- `receive <endpoint_id> <tag>` - 接收消息
- `broadcast <tag> <消息>` - 广播消息
- `stats` - 显示统计信息
- `benchmark` - 运行性能测试

### 客户端-服务器示例

```bash
cargo run --example ucx_worker_client_server
```

这个示例演示了完整的客户端-服务器通信，包括：
- 服务器监听和消息处理
- 客户端连接和消息发送
- 双向通信和响应处理

## ⚡ 性能优化技巧

### 1. 配置优化

```rust
let config = WorkerConfig {
    thread_mode: ThreadMode::Multi,       // 多线程模式 (如果支持)
    buffer_size: 256 * 1024,             // 更大的缓冲区
    progress_interval_us: 50,             // 更频繁的进度检查
    enable_rma: true,                     // 启用 RMA 以获得更好性能
    ..Default::default()
};
```

### 2. 进度处理优化

```rust
// 在高频循环中调用 progress
loop {
    let progress_count = worker.progress();
    
    if progress_count == 0 {
        // 没有操作时稍微休眠
        std::thread::sleep(Duration::from_micros(10));
    }
    
    // 处理业务逻辑
    handle_messages(&worker);
}
```

### 3. 批量操作

```rust
// 批量发送消息
let messages = vec!["msg1", "msg2", "msg3"];
for (i, msg) in messages.iter().enumerate() {
    worker.send_tag(&endpoint_id, i as u64, msg.as_bytes())?;
}

// 一次性推进所有进度
worker.progress();
```

## 🚨 注意事项

1. **进度处理**: 必须定期调用 `progress()` 来处理网络操作
2. **标签匹配**: 发送和接收操作必须使用相同的标签
3. **缓冲区大小**: 接收缓冲区必须足够大以容纳消息
4. **错误处理**: 所有操作都返回 `Result`，请适当处理错误
5. **资源管理**: Worker 在 Drop 时自动清理资源
6. **线程安全**: 当前实现主要面向单线程使用

## 🔧 故障排除

### 连接问题
- 检查网络连接和防火墙设置
- 确保目标地址正确且可达
- 验证端口没有被其他程序占用

### 消息接收问题
- 确保发送方和接收方使用相同的标签
- 检查缓冲区大小是否足够
- 定期调用 `progress()` 来处理网络事件

### 性能问题
- 调整 `progress_interval_us` 参数
- 增加缓冲区大小
- 考虑使用多线程模式 (如果支持)

## 📚 更多资源

- [UCX 官方文档](https://openucx.org/)
- [UCX API 参考](https://openucx.readthedocs.io/)
- [Rust FFI 指南](https://doc.rust-lang.org/nomicon/ffi.html)

这个 UCX Worker 封装为您提供了一个完整、高性能的网络通信解决方案，简化了 UCX 的使用并提供了 Rust 风格的安全接口。
