use orpc::common::Logger;
use orpc::CommonResult;
use orpc::runtime::RpcRuntime;
use orpc::server::ServerConf;
use orpc::test::file::file_handler::FileService;
use orpc::ucp::reactor::UcpServer;

// cargo run --example file_server
fn main() -> CommonResult<()> {
    Logger::default();
    let mut conf = ServerConf::default();
    conf.hostname = "127.0.0.1".to_string();
    conf.port = 1122;
    let dirs = vec![
        String::from("../testing/orpc-d1"),
    ];

    let service = FileService::new(dirs);
    let server = UcpServer::new(conf, service);
    let rt = server.clone_rt();
    let mut status_receiver = UcpServer::run_server(server);
    rt.block_on(status_receiver.wait_stop()).unwrap();

    Ok(())
}