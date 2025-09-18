use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 最小化调试测试 - 发送简单数据并显示详细信息
#[test]
#[ignore]
fn minimal_debug() -> CommonResult<()> {
    Logger::default();
    
    println!("=== 最小化UCX调试 ===");
    
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
    }
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            // 测试最简单的数据
            let test_data = "ABC\n";
            println!("准备发送: {:?}", test_data);
            println!("字节表示: {:?}", test_data.as_bytes());
            println!("十六进制: {:02x?}", test_data.as_bytes());
            
            ep.send_string(test_data);
            
            // 大量progress调用
            for i in 0..200 {
                let count = worker.progress();
                if count > 0 {
                    println!("Progress round {}: {} operations", i, count);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            
            println!("发送完成，请检查nc输出");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        Err(e) => {
            println!("连接失败: {:?}", e);
            return Err(e);
        }
    }
    
    Ok(())
}

/// 测试原始字节发送
#[test] 
#[ignore]
fn test_raw_bytes() -> CommonResult<()> {
    Logger::default();
    
    println!("=== 原始字节发送测试 ===");
    
    unsafe {
        std::env::set_var("UCX_TLS", "tcp");
        std::env::set_var("UCX_NET_DEVICES", "lo");
    }
    
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    
    match UcpEndpoint::connect(&worker, &ucs_addr) {
        Ok(ep) => {
            // 发送已知的字节模式
            let test_bytes: Vec<u8> = vec![
                0x41, 0x42, 0x43, 0x0A,  // "ABC\n"
                0x31, 0x32, 0x33, 0x0A,  // "123\n"  
                0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x0A,  // "Hello\n"
            ];
            
            println!("发送字节: {:02x?}", test_bytes);
            println!("应该显示为: ABC\\n123\\nHello\\n");
            
            // 使用更底层的发送方法
            use orpc::ucp::bindings::*;
            
            unsafe extern "C" fn callback(request: *mut std::ffi::c_void, status: ucs_status_t) {
                println!("Raw send callback: status={:?}", status);
            }
            
            let status = unsafe {
                ucp_stream_send_nb(
                    ep.raw_handle(),
                    test_bytes.as_ptr() as *const std::ffi::c_void,
                    test_bytes.len(),
                    ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t,
                    Some(callback),
                    0
                )
            };
            
            println!("Send status: {:?}", status);
            
            for i in 0..100 {
                let count = worker.progress();
                if count > 0 {
                    println!("Raw progress {}: {}", i, count);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            
            println!("原始字节发送完成");
        }
        Err(e) => {
            println!("连接失败: {:?}", e);
            return Err(e);
        }
    }
    
    Ok(())
}
