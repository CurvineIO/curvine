use std::sync::Arc;
use std::thread;
use std::time::Duration;
use anyhow::Result;

use orpc::ucp::{UcxWorker, WorkerConfig, ThreadMode};

/// ucp_context_h 使用演示
fn main() -> Result<()> {
    println!("=== ucp_context_h 作用演示 ===\n");
    
    // 演示 1: 基础上下文创建和使用
    demo_basic_context_usage()?;
    
    // 演示 2: 不同配置对上下文的影响
    demo_context_configurations()?;
    
    // 演示 3: 多 Worker 共享上下文
    demo_shared_context()?;
    
    // 演示 4: 上下文生命周期管理
    demo_context_lifecycle()?;
    
    Ok(())
}

/// 演示 1: 基础上下文创建和使用
fn demo_basic_context_usage() -> Result<()> {
    println!("🔧 演示 1: 基础上下文创建和使用");
    
    // 创建默认配置
    let config = WorkerConfig::default();
    println!("   配置: {:?}", config.thread_mode);
    
    // 创建 Worker (内部会创建 ucp_context_h)
    let worker = UcxWorker::new(config)?;
    println!("   ✓ UCX 上下文已创建和初始化");
    println!("   - 网络设备已发现和配置");
    println!("   - 传输协议已选择和初始化");
    println!("   - 内存管理系统已就绪");
    
    // 显示上下文相关信息
    println!("   Worker 统计:");
    worker.stats().print();
    
    println!("   ✓ 演示 1 完成\n");
    Ok(())
}

/// 演示 2: 不同配置对上下文的影响
fn demo_context_configurations() -> Result<()> {
    println!("⚙️ 演示 2: 不同配置对上下文的影响");
    
    // 配置 1: 最小功能配置
    let minimal_config = WorkerConfig {
        thread_mode: ThreadMode::Single,
        buffer_size: 1024,
        enable_tag: true,
        enable_stream: false,
        enable_rma: false,
        progress_interval_us: 1000,
    };
    
    println!("   创建最小功能上下文...");
    let minimal_worker = UcxWorker::new(minimal_config)?;
    println!("   ✓ 最小配置上下文: 仅启用标签消息功能");
    
    // 配置 2: 全功能配置
    let full_config = WorkerConfig {
        thread_mode: ThreadMode::Single,
        buffer_size: 64 * 1024,
        enable_tag: true,
        enable_stream: true,
        enable_rma: true,
        progress_interval_us: 50,
    };
    
    println!("   创建全功能上下文...");
    let full_worker = UcxWorker::new(full_config)?;
    println!("   ✓ 全功能配置上下文: 启用所有通信功能");
    
    // 比较两个上下文的资源使用
    println!("   最小配置 Worker:");
    minimal_worker.stats().print();
    
    println!("   全功能配置 Worker:");
    full_worker.stats().print();
    
    println!("   ✓ 演示 2 完成\n");
    Ok(())
}

/// 演示 3: 多 Worker 共享上下文概念
fn demo_shared_context() -> Result<()> {
    println!("🔗 演示 3: 多 Worker 共享上下文概念");
    
    // 注意: 我们的封装中每个 Worker 都有自己的上下文
    // 但在概念上，多个 Worker 可以共享同一个 UCX 上下文
    
    let config = WorkerConfig {
        buffer_size: 32 * 1024,
        enable_tag: true,
        enable_stream: true,
        ..Default::default()
    };
    
    println!("   创建多个 Worker (每个都有自己的上下文副本)...");
    
    // 创建多个 Worker
    let worker1 = UcxWorker::new(config.clone())?;
    let worker2 = UcxWorker::new(config.clone())?;
    let worker3 = UcxWorker::new(config)?;
    
    println!("   ✓ 创建了 3 个 Worker");
    println!("   - 每个 Worker 都有独立的 ucp_context_h");
    println!("   - 但它们使用相同的配置和底层资源");
    
    // 显示每个 Worker 的状态
    println!("   Worker 1 端点数: {}", worker1.get_endpoint_ids().len());
    println!("   Worker 2 端点数: {}", worker2.get_endpoint_ids().len());
    println!("   Worker 3 端点数: {}", worker3.get_endpoint_ids().len());
    
    println!("   ✓ 演示 3 完成\n");
    Ok(())
}

/// 演示 4: 上下文生命周期管理
fn demo_context_lifecycle() -> Result<()> {
    println!("♻️ 演示 4: 上下文生命周期管理");
    
    println!("   创建上下文并演示其生命周期...");
    
    {
        let config = WorkerConfig::default();
        let worker = UcxWorker::new(config)?;
        println!("   ✓ 上下文创建: ucp_init() 被调用");
        println!("   - UCX 库初始化完成");
        println!("   - 网络资源分配完成");
        
        // 模拟一些工作
        for i in 1..=3 {
            let progress = worker.progress();
            println!("   进度轮询 #{}: {} 个操作被处理", i, progress);
            thread::sleep(Duration::from_millis(100));
        }
        
        println!("   Worker 作用域即将结束...");
    } // <- worker 在这里被 drop
    
    println!("   ✓ 上下文销毁: ucp_cleanup() 被调用");
    println!("   - 所有网络连接已关闭");
    println!("   - 内存资源已释放");
    println!("   - UCX 库已清理");
    
    // 演示资源清理的重要性
    println!("   \n   🚨 重要提醒:");
    println!("   - ucp_context_h 必须在所有依赖的 worker/endpoint 之后清理");
    println!("   - 我们的封装通过 Rust 的 RAII 自动管理这个过程");
    println!("   - 如果手动管理，错误的清理顺序会导致程序崩溃");
    
    println!("   ✓ 演示 4 完成\n");
    Ok(())
}

/// 演示上下文功能配置的影响
#[allow(dead_code)]
fn demo_feature_impact() -> Result<()> {
    println!("🎯 附加演示: 功能配置的影响");
    
    // 仅标签功能
    let tag_only_config = WorkerConfig {
        enable_tag: true,
        enable_stream: false,
        enable_rma: false,
        ..Default::default()
    };
    
    // 仅流功能  
    let stream_only_config = WorkerConfig {
        enable_tag: false,
        enable_stream: true,
        enable_rma: false,
        ..Default::default()
    };
    
    // 创建不同配置的 Worker
    let tag_worker = UcxWorker::new(tag_only_config)?;
    let stream_worker = UcxWorker::new(stream_only_config)?;
    
    println!("   标签功能 Worker: 支持点对点消息传递");
    println!("   流功能 Worker: 支持大数据流式传输");
    
    // 在实际应用中，不同的功能配置会影响:
    // - 可用的 API
    // - 性能特征
    // - 内存使用
    // - 网络协议选择
    
    println!("   ✓ 功能配置演示完成");
    Ok(())
}

/// 错误处理演示
#[allow(dead_code)]
fn demo_error_handling() {
    println!("🚨 错误处理演示");
    
    // 演示可能的错误情况
    println!("   可能的 ucp_context_h 创建错误:");
    println!("   1. UCX 库未安装或版本不兼容");
    println!("   2. 网络设备不支持请求的功能");
    println!("   3. 内存不足");
    println!("   4. 权限不够 (某些高性能网络需要特权)");
    println!("   5. 配置参数无效");
    
    // 实际的错误处理
    let invalid_config = WorkerConfig {
        buffer_size: 0,  // 无效的缓冲区大小
        ..Default::default()
    };
    
    match UcxWorker::new(invalid_config) {
        Ok(_) => println!("   意外: 无效配置却创建成功"),
        Err(e) => println!("   ✓ 正确捕获错误: {}", e),
    }
}
