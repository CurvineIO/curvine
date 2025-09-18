use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

/// 专门用于调试数据传输乱码问题的测试
/// 使用方法：
/// 1. 终端1: nc -l -k -p 8080
/// 2. 终端2: cargo test --package orpc --test ucp_debug_test -- --nocapture --ignored
#[test]
#[ignore]
fn debug_data_transmission() -> CommonResult<()> {
    Logger::default();
    
    println!("=== UCX数据传输调试测试 ===");
    
    // 设置UCX环境变量
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
            info!("连接成功！开始数据传输调试...");
            
            // 测试简单ASCII字符
            info!("\n--- 测试1: 简单ASCII ---");
            ep.send_string("Test1: Hello World\n");
            for _ in 0..50 { worker.progress(); std::thread::sleep(std::time::Duration::from_millis(10)); }
            
            // 测试数字
            info!("\n--- 测试2: 数字 ---");
            ep.send_string("Test2: 123456789\n");
            for _ in 0..50 { worker.progress(); std::thread::sleep(std::time::Duration::from_millis(10)); }
            
            // 测试特殊字符
            info!("\n--- 测试3: 特殊字符 ---");
            ep.send_string("Test3: !@#$%^&*()_+\n");
            for _ in 0..50 { worker.progress(); std::thread::sleep(std::time::Duration::from_millis(10)); }
            
            // 测试中文字符
            info!("\n--- 测试4: 中文字符 ---");
            ep.send_string("Test4: 你好世界\n");
            for _ in 0..50 { worker.progress(); std::thread::sleep(std::time::Duration::from_millis(10)); }
            
            // 测试长字符串
            info!("\n--- 测试5: 长字符串 ---");
            ep.send_string("Test5: This is a very long string to test if the UCX transmission can handle longer text data without corruption. ABCDEFGHIJKLMNOPQRSTUVWXYZ\n");
            for _ in 0..50 { worker.progress(); std::thread::sleep(std::time::Duration::from_millis(10)); }
            
            // 测试多行
            info!("\n--- 测试6: 多行文本 ---");
            ep.send_string("Test6 Line1: First line\nTest6 Line2: Second line\nTest6 Line3: Third line\n");
            for _ in 0..50 { worker.progress(); std::thread::sleep(std::time::Duration::from_millis(10)); }
            
            info!("\n=== 所有测试完成 ===");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        Err(e) => {
            println!("连接失败: {:?}", e);
            println!("请确保nc服务器正在运行: nc -l -k -p 8080");
            return Err(e);
        }
    }
    
    Ok(())
}

/// 测试DataSlice转换
#[test]
fn test_data_slice_conversion() -> CommonResult<()> {
    use orpc::sys::DataSlice;
    
    let test_strings = [
        "Hello World",
        "123456789", 
        "!@#$%^&*()",
        "你好世界",
        "Mixed: Hello 世界 123",
    ];
    
    for test_str in &test_strings {
        println!("\n--- 测试字符串: '{}' ---", test_str);
        
        // 通过From<&str>转换
        let data_slice: DataSlice = test_str.into();
        let bytes = data_slice.as_slice();
        
        println!("原始字符串: {:?}", test_str);
        println!("字节长度: {}", bytes.len());
        println!("字节内容(hex): {:02x?}", bytes);
        println!("反转换字符串: {:?}", String::from_utf8_lossy(bytes));
        
        // 验证转换是否正确
        assert_eq!(test_str.as_bytes(), bytes);
        assert_eq!(test_str, String::from_utf8_lossy(bytes));
    }
    
    println!("\n✅ DataSlice转换测试全部通过");
    Ok(())
}
