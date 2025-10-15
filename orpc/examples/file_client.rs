use std::mem::transmute;
use std::sync::Arc;
use bytes::{Buf, BufMut, BytesMut};
use log::info;
use tokio::io::AsyncReadExt;
use orpc::client::ClientFactory;
use orpc::common::{Logger, Utils};
use orpc::CommonResult;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::ServerConf;
use orpc::sys::DataSlice;
use orpc::test::file::file_handler::{FileService, RpcCode};
use orpc::ucp::reactor::UcpRuntime;
use orpc::ucp::rma::RKey;

// cargo run --example file_client
fn main() -> CommonResult<()> {
    Logger::default();
    let addr = InetAddr::new("127.0.0.1", 1122);

    let ucp_rt = Arc::new(UcpRuntime::default());

    let factory = ClientFactory::default();
    let client = factory.create_sync_ucp(ucp_rt.clone(), &addr)?;

    let req_id = Utils::req_id();
    let open_msg = open_message(RpcCode::Write, req_id);

    let _ = client.rpc(open_msg)?;

    let str = Utils::rand_str(64 * 1024);
    let mut checksum: u64 = 0;
    for i in 0..100 {
        let data = BytesMut::from(str.as_str());
        checksum += Utils::crc32(&data) as u64;

        let msg = Builder::new()
            .code(RpcCode::Write)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(i)
            .data(DataSlice::Buffer(data.clone()))
            .build();

        let _ = client.rpc(msg).unwrap();
    }

    Ok(())
}


fn open_message(op: RpcCode, req_id: i64) -> Message {
    let id: u64 = 123455;
    let mut bytes = BytesMut::new();
    bytes.put_u64(id);

    Builder::new()
        .code(op)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .header(bytes)
        .build()
}