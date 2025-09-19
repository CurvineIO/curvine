use orpc::CommonResult;
use orpc::ucp::ucp_server::run_simple_server;

/// UCX服务器示例程序
/// 
/// 这个程序演示了如何创建一个简单的UCX stream服务器:
/// 1. 监听127.0.0.1:8080端口
/// 2. 接收客户端发送的stream消息  
/// 3. 输出接收到的数据内容
/// 4. 返回"res: 原始消息"给客户端
///
/// 使用方法:
/// ```bash
/// cargo run --package orpc --example ucx_server_example
/// ```
///
/// 在另一个终端测试:
/// ```bash
/// cargo test --package orpc --test ucp_test -- --nocapture
/// ```
fn main() -> CommonResult<()> {
    run_simple_server()
}
