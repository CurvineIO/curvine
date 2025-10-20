use bytes::{BufMut, BytesMut};
use log::info;
use orpc::client::ClientFactory;
use orpc::common::{Logger, Utils};
use orpc::error::CommonErrorExt;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::test::file::file_handler::RpcCode;
use orpc::ucp::reactor::UcpRuntime;
use orpc::CommonResult;
use std::sync::Arc;

// cargo run --example file_client
fn main() -> CommonResult<()> {
    Logger::default();
    let addr = InetAddr::new("127.0.0.1", 1122);

    let ucp_rt = Arc::new(UcpRuntime::default());

    let write_ck = file_write(&addr, ucp_rt.clone())?;
    let read_ck = file_read(&addr, ucp_rt.clone())?;
    info!("write_ck {}, read_ck {}", write_ck, read_ck);
    assert_eq!(write_ck, read_ck);

    Ok(())
}

fn file_write(addr: &InetAddr, rt: Arc<UcpRuntime>) -> CommonResult<u64> {
    let factory = ClientFactory::default();
    let client = factory.create_sync_ucp(rt, addr)?;

    let req_id = Utils::req_id();
    let open_msg = open_message(RpcCode::Write, req_id);

    tracing::info!("open");
    let _ = client.rpc(open_msg)?;
    tracing::info!("open");

    let mut checksum: u64 = 0;
    for i in 0..100 {
        let str = Utils::rand_str(64 * 1024);
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

    Ok(checksum)
}

fn file_read(addr: &InetAddr, rt: Arc<UcpRuntime>) -> CommonResult<u64> {
    let factory = ClientFactory::default();
    let client = factory.create_sync_ucp(rt, addr)?;

    let req_id = Utils::req_id();
    let open_msg = open_message(RpcCode::Read, req_id);
    let _ = client.rpc(open_msg).unwrap();

    let mut checksum: u64 = 0;
    let mut seq_id: i32 = 0;
    loop {
        let msg = Builder::new()
            .request(RequestStatus::Running)
            .code(RpcCode::Read)
            .req_id(req_id)
            .seq_id(seq_id)
            .build();

        seq_id += 1;

        match client.rpc(msg) {
            Err(_) => break,

            Ok(r) if r.not_empty() => {
                if r.is_success() {
                    checksum += Utils::crc32(r.data.as_slice()) as u64;
                } else {
                    print!("warn {:?}", r.check_error_ext::<CommonErrorExt>());
                    break;
                }
            }

            _ => break,
        };
    }

    Ok(checksum)
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
