use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use log::{info, debug, error};
use crate::CommonResult;
use crate::ucp::{UcpContext, UcpWorker, UcpListener, UcsSockAddr};

/// 简单的UCX Stream服务器
/// 功能：
/// 1. 监听指定端口的连接
/// 2. 接收客户端发送的stream消息
/// 3. 输出接收到的数据内容（字符串形式）
/// 4. 返回"res: 原始消息"给客户端
pub struct UcpServer {
    context: Arc<UcpContext>,
    pub worker: UcpWorker,
    listener: Option<UcpListener>,
    addr: SocketAddr,
}

impl UcpServer {
    /// 创建新的UCX服务器
    pub fn new(addr: SocketAddr) -> CommonResult<Self> {
        info!("🏗️  创建UCX服务器，监听地址: {}", addr);
        
        // 设置UCX环境变量
        unsafe {
            std::env::set_var("UCX_TLS", "tcp");
            std::env::set_var("UCX_NET_DEVICES", "lo");
            std::env::set_var("UCX_LOG_LEVEL", "warn"); // 减少UCX内部日志
        }
        
        // 创建UCX组件
        let context = Arc::new(UcpContext::new()?);
        let worker = UcpWorker::new(context.clone())?;
        
        Ok(Self {
            context,
            worker,
            listener: None,
            addr,
        })
    }
    
    /// 启动服务器
    pub fn start(&mut self) -> CommonResult<()> {
        info!("🚀 启动UCX服务器...");
        
        // 创建socket地址
        let ucs_addr = UcsSockAddr::new(self.addr)?;
        info!("📍 绑定地址: {:?}", ucs_addr);
        
        // 创建监听器
        let listener = UcpListener::new(&self.worker, &ucs_addr)?;
        self.listener = Some(listener);
        
        info!("✅ UCX服务器启动成功，等待客户端连接...");
        info!("💡 你可以使用以下命令连接测试:");
        info!("   cargo test --package orpc --test ucp_test -- --nocapture");
        
        Ok(())
    }
    
    /// 运行服务器事件循环
    pub fn run(&self) -> CommonResult<()> {
        info!("🔄 进入服务器事件循环...");
        
        let mut iteration = 0;
        loop {
            // 推进worker以处理网络事件
            let progress_count = self.worker.progress();
            
            if progress_count > 0 {
                debug!("🔄 处理了 {} 个网络事件 (迭代 {})", progress_count, iteration);
            }
            
            // 每1000次迭代显示一次心跳
            if iteration % 1000 == 0 {
                debug!("💓 服务器运行中... (迭代 {})", iteration);
            }
            
            iteration += 1;
            
            // 短暂休眠避免CPU占用过高
            std::thread::sleep(Duration::from_millis(1)); // 减少休眠时间，提高响应性
        }
    }
    
    /// 运行服务器指定时间（用于测试）
    pub fn run_for(&self, duration_secs: u64) -> CommonResult<()> {
        info!("🔄 运行服务器 {} 秒", duration_secs);
        
        let start_time = std::time::Instant::now();
        let mut iteration = 0;
        
        while start_time.elapsed().as_secs() < duration_secs {
            // 推进worker以处理网络事件
            let progress_count = self.worker.progress();
            
            if progress_count > 0 {
                info!("🔄 处理了 {} 个网络事件 (迭代 {})", progress_count, iteration);
            }
            
            iteration += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        
        info!("⏰ 服务器运行时间到，处理了 {} 次迭代", iteration);
        Ok(())
    }
    
    /// 停止服务器
    pub fn stop(&mut self) {
        info!("🛑 停止UCX服务器...");
        self.listener = None;
        info!("✅ UCX服务器已停止");
    }
}

impl Drop for UcpServer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Logger;
    use std::str::FromStr;
    use std::thread;
    
    /// 测试UCX服务器的基本功能
    #[test]
    #[ignore] // 需要手动运行
    fn test_ucx_server() -> CommonResult<()> {
        Logger::default();
        
        println!("🎯 UCX服务器测试");
        println!("================");
        
        let addr = SocketAddr::from_str("127.0.0.1:8080")?;
        let mut server = UcpServer::new(addr)?;
        
        // 启动服务器
        server.start()?;
        
        println!("🎧 服务器已启动，监听 {}:8080", "127.0.0.1");
        println!("📨 等待客户端连接和消息...");
        println!();
        println!("💡 在另一个终端运行客户端测试:");
        println!("   cargo test --package orpc --test ucp_test -- --nocapture");
        println!();
        println!("⌨️  按 Ctrl+C 停止服务器");
        
        // 运行服务器（这会无限循环）
        server.run()
    }
}

/// 简单的服务器启动器，用于快速测试
pub fn run_simple_server() -> CommonResult<()> {
    use crate::common::Logger;
    use std::str::FromStr;
    
    Logger::default();
    
    println!();
    println!("🎯 Curvine UCX简单服务器");
    println!("========================");
    println!("功能:");
    println!("  • 接收UCX stream消息");
    println!("  • 输出接收到的数据内容");
    println!("  • 返回 'res: 原始消息' 响应");
    println!();
    
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let mut server = UcpServer::new(addr)?;
    
    server.start()?;
    server.run()
}
