use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use orpc::common::Logger;
use orpc::CommonResult;
use orpc::ucp::{UcpContext, UcpEndpoint, UcpWorker, UcsSockAddr};

//
// cargo test --test ucp_test::test
#[test]
fn test() -> CommonResult<()> {
    Logger::default();
    let context = Arc::new(UcpContext::new()?);
    let worker = UcpWorker::new(context)?;
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let ucs_addr = UcsSockAddr::new(addr)?;
    let ep = UcpEndpoint::connect(&worker, &ucs_addr)?;


    info!("xxx");
    ep.stream_send("123".into());
    info!("xxx");

    Ok(())
}