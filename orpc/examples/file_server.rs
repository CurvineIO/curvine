use orpc::common::Logger;
use orpc::runtime::RpcRuntime;
use orpc::server::{RpcServer, ServerConf};
use orpc::test::file::file_handler::FileService;
use orpc::CommonResult;

// cargo run --example file_server
fn main() -> CommonResult<()> {
    Logger::default();
    let mut conf = ServerConf::default();
    conf.hostname = "127.0.0.1".to_string();
    conf.port = 1122;
    conf.use_ucp = true;
    let dirs = vec![String::from("../testing/orpc-d1")];

    let service = FileService::new(dirs);
    let server = RpcServer::new(conf, service);
    let rt = server.clone_rt();
    let mut status_receiver = RpcServer::run_server(server);
    rt.block_on(status_receiver.wait_stop()).unwrap();

    Ok(())
}
