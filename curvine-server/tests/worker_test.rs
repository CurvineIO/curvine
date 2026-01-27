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

use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse,
};
use curvine_common::state::{ExtendedBlock, FileType, StorageType};
use curvine_common::utils::ProtoUtils;
use curvine_server::worker::Worker;
use orpc::common::Utils;
use orpc::io::net::NetUtils;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice::Buffer;
use orpc::CommonResult;
use prost::bytes::BytesMut;
use std::thread;

const CHUNK_SIZE: i32 = 1024;
const LOOP_NUM: i32 = 100;

// Test the worker interface function.
fn start_worker() -> ClusterConf {
    let mut conf = ClusterConf::default();
    conf.worker.rpc_port = NetUtils::get_available_port();
    conf.worker.web_port = NetUtils::get_available_port();
    conf.worker.data_dir = vec!["[MEM:10MB]../testing/worker-test".to_owned()];
    conf.client.init().unwrap();

    let server = Worker::with_conf(conf.clone()).unwrap();
    thread::spawn(move || server.start_standalone());
    conf
}

#[test]
fn test_worker_block_write_and_read_with_checksum_validation() -> CommonResult<()> {
    let conf = start_worker();

    let block_id = Utils::req_id().abs();
    let write_ck = block_write(block_id, &conf)?;
    let read_ck = block_read(block_id, &conf)?;

    assert_eq!(write_ck, read_ck);
    Ok(())
}

fn block_write(id: i64, conf: &ClusterConf) -> CommonResult<u64> {
    let block_size = (CHUNK_SIZE * LOOP_NUM) as i64;
    let block = ExtendedBlock::new(id, block_size, StorageType::Disk, FileType::File);
    let request = BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block),
        off: 0,
        block_size,
        short_circuit: false,
        client_name: "test".to_string(),
        chunk_size: CHUNK_SIZE,
        pipeline_stream: vec![],
    };

    let req_id = Utils::req_id();
    let mut seq_id = -1;
    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Open)
        .req_id(req_id)
        .seq_id(seq_id)
        .proto_header(request)
        .build();

    let client = conf.worker_sync_client()?;

    let response: BlockWriteResponse = client.rpc(msg)?.parse_header()?;

    assert_eq!(response.off, 0);
    seq_id += 1;

    let mut checksum: u64 = 0;
    for _ in 0..LOOP_NUM {
        let bytes = BytesMut::from(Utils::rand_str(CHUNK_SIZE as usize).as_str());
        checksum += Utils::crc32(&bytes) as u64;

        // write data
        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .data(Buffer(bytes))
            .build();

        let _ = client.rpc(msg)?;
        seq_id += 1;
    }

    let msg = Builder::new()
        .code(RpcCode::WriteBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(seq_id)
        .build();

    let _: Message = client.rpc(msg)?;

    Ok(checksum)
}

fn block_read(id: i64, conf: &ClusterConf) -> CommonResult<u64> {
    let request = BlockReadRequest {
        id,
        off: 0,
        len: (CHUNK_SIZE * LOOP_NUM) as i64,
        chunk_size: CHUNK_SIZE,
        short_circuit: false,
        ..Default::default()
    };

    let req_id = Utils::req_id();
    let mut seq_id = -1;
    let msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .req_id(req_id)
        .seq_id(seq_id)
        .request(RequestStatus::Open)
        .proto_header(request)
        .build();

    let client = conf.worker_sync_client()?;
    seq_id += 1;
    let rep: BlockReadResponse = client.rpc_check(msg)?.parse_header()?;
    println!("read-reap: {:#?}", rep);

    let mut start = 0;
    let mut check_sum: u64 = 0;
    while start < rep.len {
        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .req_id(req_id)
            .seq_id(seq_id)
            .request(RequestStatus::Running)
            .build();
        seq_id += 1;
        let rep = client.rpc_check(msg)?;
        println!("rep {}", rep.data.len());
        if rep.data_len() == 0 {
            break;
        } else {
            start += rep.data_len() as i64;
            check_sum += Utils::crc32(rep.data_bytes().unwrap()) as u64;
        }
    }

    let msg = Builder::new()
        .code(RpcCode::ReadBlock)
        .request(RequestStatus::Complete)
        .req_id(req_id)
        .seq_id(seq_id)
        .build();

    let _: Message = client.rpc(msg)?;

    Ok(check_sum)
}
