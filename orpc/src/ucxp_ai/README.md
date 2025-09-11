# UCX TCP 客户端使用指南

这个目录包含了使用您的 UCX bindings 创建简单 TCP 客户端的示例。

## 文件说明

- `simple_tcp_client.rs` - 简单的 TCP 客户端示例
- `simple_tcp_server.rs` - 简单的 TCP 服务器示例  
- `ucx_wrapper.rs` - UCX 的 Rust 封装层

## 如何使用

### 1. 添加依赖

在您的 `Cargo.toml` 中添加必要的依赖：

```toml
[dependencies]
orpc = { path = "." }  # 引用本地的 orpc 包
libc = "0.2"
anyhow = "1.0"
```

### 2. 运行示例

#### 运行客户端：

```bash
# 在 curvine 项目根目录下
cd orpc
cargo run --example simple_tcp_client
```

#### 运行服务器：

```bash
# 在另一个终端中
cd orpc  
cargo run --example simple_tcp_server
```

### 3. 基本用法

```rust
use orpc::ucx::{UcxContext, UcxWorker, UcxEndpoint};
use std::net::SocketAddr;

fn main() -> anyhow::Result<()> {
    // 1. 创建 UCX 上下文
    let context = UcxContext::new()?;
    
    // 2. 创建工作器
    let worker = UcxWorker::new(context)?;
    
    // 3. 连接到服务器
    let server_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let endpoint = worker.connect(server_addr)?;
    
    // 4. 发送消息
    let message = b"Hello, UCX!";
    endpoint.send_tag(0, message)?;
    
    // 5. 推进工作器进度
    worker.progress();
    
    // 6. 接收响应
    let mut buffer = [0u8; 1024];
    if let Ok(bytes_received) = endpoint.receive_tag(0, &mut buffer) {
        if bytes_received > 0 {
            let response = String::from_utf8_lossy(&buffer[..bytes_received]);
            println!("收到响应: {}", response);
        }
    }
    
    Ok(())
}
```

## API 说明

### UcxContext
- `UcxContext::new()` - 创建新的 UCX 上下文
- 自动启用 TAG 和 STREAM 功能

### UcxWorker  
- `UcxWorker::new(context)` - 创建新的工作器
- `worker.progress()` - 推进工作器进度，返回处理的操作数
- `worker.connect(addr)` - 连接到指定地址

### UcxEndpoint
- `endpoint.send_tag(tag, data)` - 发送带标签的消息
- `endpoint.receive_tag(tag, buffer)` - 接收带标签的消息
- 自动在 Drop 时关闭连接

## 重要提示

1. **进度推进**: UCX 是基于轮询的，需要定期调用 `worker.progress()` 来处理网络操作

2. **错误处理**: 所有操作都返回 `Result` 类型，请适当处理错误

3. **内存管理**: 所有资源都实现了 RAII，会在 Drop 时自动清理

4. **线程安全**: 当前实现使用单线程模式 (`UCS_THREAD_MODE_SINGLE`)

5. **网络协议**: 目前支持 TCP 连接，可以扩展支持其他协议

## 故障排除

1. **连接失败**: 确保目标服务器正在运行并监听指定端口

2. **编译错误**: 确保 UCX 库已正确安装并可链接

3. **运行时错误**: 检查防火墙设置和网络连接

4. **性能问题**: 调整 `progress()` 调用频率和缓冲区大小

## 扩展功能

您可以基于这个基础封装添加：

- 异步 I/O 支持 (使用 tokio)
- 连接池管理
- 更复杂的消息协议
- 性能监控和统计
- 多线程支持
- 流式数据传输
