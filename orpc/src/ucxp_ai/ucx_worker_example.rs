use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Duration;
use std::thread;

use anyhow::Result;
use orpc::ucp::{UcxWorker, WorkerConfig, ThreadMode};

/// UCX Worker 使用示例
fn main() -> Result<()> {
    println!("=== UCX Worker 封装示例 ===");
    
    // 1. 创建 Worker 配置
    println!("1. 配置 UCX Worker...");
    let config = WorkerConfig {
        thread_mode: ThreadMode::Single,
        buffer_size: 64 * 1024,
        enable_tag: true,
        enable_stream: true,
        enable_rma: false,
        progress_interval_us: 100,
    };
    
    // 2. 创建 UCX Worker
    println!("2. 创建 UCX Worker...");
    let mut worker = UcxWorker::new(config)?;
    println!("   ✓ Worker 创建成功");
    
    // 3. 显示配置信息
    println!("3. Worker 配置:");
    println!("   线程模式: {:?}", worker.config().thread_mode);
    println!("   缓冲区大小: {} KB", worker.config().buffer_size / 1024);
    println!("   启用标签: {}", worker.config().enable_tag);
    println!("   进度间隔: {} μs", worker.config().progress_interval_us);
    
    // 4. 运行交互式模式
    println!("4. 进入交互模式 (输入 help 查看命令):");
    run_interactive_mode(&mut worker)?;
    
    // 5. 显示最终统计信息
    println!("5. 最终统计信息:");
    worker.stats().print();
    
    Ok(())
}

/// 交互式模式
fn run_interactive_mode(worker: &mut UcxWorker) -> Result<()> {
    let mut buffer = vec![0u8; worker.config().buffer_size];
    
    loop {
        print!("UCX Worker> ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        
        if parts.is_empty() {
            continue;
        }
        
        match parts[0] {
            "help" => show_help(),
            
            "stats" => {
                worker.stats().print();
            }
            
            "progress" => {
                let count = if parts.len() > 1 {
                    parts[1].parse::<u32>().unwrap_or(10)
                } else {
                    10
                };
                
                println!("   执行 {} 次进度轮询...", count);
                let mut total = 0;
                for _ in 0..count {
                    total += worker.progress();
                    thread::sleep(Duration::from_micros(worker.config().progress_interval_us));
                }
                println!("   总计处理 {} 个操作", total);
            }
            
            "connect" => {
                if parts.len() > 1 {
                    match parts[1].parse::<SocketAddr>() {
                        Ok(addr) => {
                            match worker.connect(addr) {
                                Ok(endpoint_id) => {
                                    println!("   ✓ 连接成功: {}", endpoint_id);
                                }
                                Err(e) => {
                                    println!("   ✗ 连接失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("   ✗ 地址格式错误: {}", e);
                        }
                    }
                } else {
                    println!("   使用: connect <地址:端口>");
                }
            }
            
            "disconnect" => {
                if parts.len() > 1 {
                    match worker.disconnect(parts[1]) {
                        Ok(_) => println!("   ✓ 断开连接: {}", parts[1]),
                        Err(e) => println!("   ✗ 断开连接失败: {}", e),
                    }
                } else {
                    println!("   使用: disconnect <endpoint_id>");
                }
            }
            
            "list" => {
                let endpoints = worker.get_endpoint_ids();
                if endpoints.is_empty() {
                    println!("   没有活跃连接");
                } else {
                    println!("   活跃连接 ({}):", endpoints.len());
                    for endpoint in endpoints {
                        println!("     - {}", endpoint);
                    }
                }
            }
            
            "send" => {
                if parts.len() >= 4 {
                    let endpoint_id = parts[1];
                    match parts[2].parse::<u64>() {
                        Ok(tag) => {
                            let message = parts[3..].join(" ");
                            match worker.send_tag(endpoint_id, tag, message.as_bytes()) {
                                Ok(_) => {
                                    println!("   ✓ 发送消息到 {}, 标签: {}, 内容: \"{}\"", 
                                             endpoint_id, tag, message);
                                }
                                Err(e) => {
                                    println!("   ✗ 发送失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("   ✗ 标签格式错误: {}", e);
                        }
                    }
                } else {
                    println!("   使用: send <endpoint_id> <tag> <消息内容>");
                }
            }
            
            "receive" => {
                if parts.len() >= 3 {
                    let endpoint_id = parts[1];
                    match parts[2].parse::<u64>() {
                        Ok(tag) => {
                            match worker.try_receive_tag(endpoint_id, tag, &mut buffer) {
                                Ok(Some(bytes_received)) => {
                                    let message = String::from_utf8_lossy(&buffer[..bytes_received]);
                                    println!("   ✓ 接收消息 ({} 字节): \"{}\"", bytes_received, message);
                                }
                                Ok(None) => {
                                    println!("   没有可用消息 (标签: {})", tag);
                                }
                                Err(e) => {
                                    println!("   ✗ 接收失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("   ✗ 标签格式错误: {}", e);
                        }
                    }
                } else {
                    println!("   使用: receive <endpoint_id> <tag>");
                }
            }
            
            "broadcast" => {
                if parts.len() >= 3 {
                    match parts[1].parse::<u64>() {
                        Ok(tag) => {
                            let message = parts[2..].join(" ");
                            match worker.broadcast_tag(tag, message.as_bytes()) {
                                Ok(_) => {
                                    println!("   ✓ 广播消息 (标签: {}): \"{}\"", tag, message);
                                }
                                Err(e) => {
                                    println!("   ✗ 广播失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("   ✗ 标签格式错误: {}", e);
                        }
                    }
                } else {
                    println!("   使用: broadcast <tag> <消息内容>");
                }
            }
            
            "listen" => {
                if parts.len() > 1 {
                    match parts[1].parse::<SocketAddr>() {
                        Ok(addr) => {
                            match worker.create_listener(addr) {
                                Ok(_) => {
                                    println!("   ✓ 开始监听: {}", addr);
                                }
                                Err(e) => {
                                    println!("   ✗ 监听失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("   ✗ 地址格式错误: {}", e);
                        }
                    }
                } else {
                    println!("   使用: listen <地址:端口>");
                }
            }
            
            "benchmark" => {
                run_benchmark(worker);
            }
            
            "clear" => {
                worker.cleanup_all();
                println!("   ✓ 已清理所有连接");
            }
            
            "quit" | "exit" => {
                println!("   再见!");
                break;
            }
            
            _ => {
                println!("   未知命令: {}，输入 help 查看帮助", parts[0]);
            }
        }
        
        println!();
    }
    
    Ok(())
}

/// 显示帮助信息
fn show_help() {
    println!("   可用命令:");
    println!("     help                            - 显示此帮助");
    println!("     stats                           - 显示统计信息");
    println!("     progress [次数]                 - 执行进度轮询");
    println!("     connect <地址:端口>             - 连接到远程地址");
    println!("     disconnect <endpoint_id>        - 断开连接");
    println!("     list                            - 列出所有连接");
    println!("     send <endpoint_id> <tag> <消息> - 发送标签消息");
    println!("     receive <endpoint_id> <tag>     - 接收标签消息");
    println!("     broadcast <tag> <消息>          - 广播消息");
    println!("     listen <地址:端口>              - 创建监听器");
    println!("     benchmark                       - 运行性能测试");
    println!("     clear                           - 清理所有连接");
    println!("     quit/exit                       - 退出程序");
    println!("   ");
    println!("   示例:");
    println!("     connect 127.0.0.1:8080");
    println!("     send 127.0.0.1:8080 123 Hello World");
    println!("     receive 127.0.0.1:8080 123");
}

/// 运行性能测试
fn run_benchmark(worker: &UcxWorker) {
    println!("   开始性能测试...");
    
    let start_time = std::time::Instant::now();
    let progress_count = worker.run_progress_loop(Some(Duration::from_secs(5)));
    let elapsed = start_time.elapsed();
    
    println!("   性能测试结果 (运行 {:?}):", elapsed);
    println!("     总进度操作: {}", progress_count);
    println!("     操作/秒: {:.2}", progress_count as f64 / elapsed.as_secs_f64());
    println!("     平均延迟: {:?}", elapsed / progress_count.max(1) as u32);
    
    worker.stats().print();
}
