// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::{BufMut, BytesMut};
use orpc::client::ClientFactory;
use orpc::common::{Logger, Utils};
use orpc::error::CommonErrorExt;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::runtime::RpcRuntime;
use orpc::server::{RpcServer, ServerConf};
use orpc::sys::DataSlice;
use orpc::test::file::dir_location::DirLocation;
use orpc::test::file::file_handler::{FileService, RpcCode};
use orpc::ucp::{UcpRuntime, UcpServer};
use orpc::CommonResult;
use std::sync::Arc;
use tracing::info;

#[test]
fn dir_location() {
    let dirs = vec![
        String::from("../testing/orpc-1"),
        String::from("../testing/orpc-2"),
    ];

    let location = DirLocation::new(dirs);

    let mut f1 = location.get_or_create(1, true).unwrap();
    let mut f2 = location.get_or_create(2, true).unwrap();

    f1.write_all("123".as_bytes()).unwrap();
    f2.write_all("abc".as_bytes()).unwrap();
}

#[test]
fn file_server() -> CommonResult<()> {
    Logger::default();
    let mut conf = ServerConf::default();
    conf.hostname = "127.0.0.1".to_string();
    // conf.port = 1133;
    let dirs = vec![
        String::from("../testing/orpc-d1"),
        String::from("../testing/orpc-d2"),
    ];
    let addr = conf.bind_addr();

    let service = FileService::new(dirs);
    let server = UcpServer::new(conf, service);
    let rt = server.clone_rt();
    let ucp_rt = server.rt.clone();
    let mut status_receiver = UcpServer::run_server(server);
    info!("xxx");
    rt.block_on(status_receiver.wait_running())?;
    info!("yyy");

    let write_ck = file_write(&addr, ucp_rt.clone())?;
    //let read_ck = file_read(&addr)?;
    // assert_eq!(write_ck, read_ck);
    Ok(())
}

fn file_write(addr: &InetAddr, rt: Arc<UcpRuntime>) -> CommonResult<u64> {
    let factory = ClientFactory::default();
    let client = factory.create_sync_ucp(rt, addr)?;

    let req_id = Utils::req_id();
    let open_msg = open_message(RpcCode::Write, req_id);

    info!("open");
    let _ = client.rpc(open_msg)?;
    info!("open");

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

    Ok(checksum)
}

fn file_read(addr: &InetAddr) -> CommonResult<u64> {
    let factory = ClientFactory::default();
    let client = factory.create_sync(addr)?;

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
                    checksum += Utils::crc32(r.data_bytes().unwrap()) as u64;
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
