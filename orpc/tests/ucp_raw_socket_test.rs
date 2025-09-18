use std::net::{TcpStream, SocketAddr};
use std::str::FromStr;
use std::io::Write;
use orpc::CommonResult;

/// 对比测试：使用原生TCP socket发送相同数据
/// 这可以帮助确定是UCX问题还是环境问题
#[test]
#[ignore]
fn tcp_socket_comparison() -> CommonResult<()> {
    println!("=== TCP Socket对比测试 ===");
    
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    
    match TcpStream::connect(addr) {
        Ok(mut stream) => {
            let test_data = "TCP Test: ABC\n123\nHello World\n";
            println!("通过TCP socket发送: {:?}", test_data);
            println!("字节: {:02x?}", test_data.as_bytes());
            
            stream.write_all(test_data.as_bytes())?;
            stream.flush()?;
            
            println!("TCP发送完成");
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        Err(e) => {
            println!("TCP连接失败: {:?}", e);
            println!("请确保nc服务器在运行: nc -l -k -p 8080");
        }
    }
    
    Ok(())
}

/// 测试不同UCX传输配置
#[test]
#[ignore]
fn test_ucx_transport_configs() -> CommonResult<()> {
    use std::sync::Arc;
    use orpc::common::Logger;
    use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};
    
    Logger::default();
    
    let transport_configs = [
        ("tcp", "TCP传输"),
        ("rc", "RC传输"), 
        ("ud", "UD传输"),
        ("shm", "共享内存传输"),
    ];
    
    for (tls, desc) in transport_configs.iter() {
        println!("\n=== 测试 {} ({}) ===", desc, tls);
        
        unsafe {
            std::env::set_var("UCX_TLS", tls);
            std::env::set_var("UCX_NET_DEVICES", "lo");
        }
        
        match test_with_config(tls) {
            Ok(_) => println!("✅ {} 测试完成", desc),
            Err(e) => println!("❌ {} 测试失败: {:?}", desc, e),
        }
    }
    
    Ok(())
}

fn test_with_config(tls: &str) -> CommonResult<()> {
    use std::sync::Arc;
    use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            let test_msg = format!("UCX {} test: OK\n", tls);
            println!("发送: {:?}", test_msg);
            
            ep.send_string(&test_msg);
            
            // 简化的progress循环
            for _ in 0..50 {
                worker.progress();
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
        Err(e) => {
            println!("连接失败: {:?}", e);
            return Err(e);
        }
    }
    
    Ok(())
}
