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

use crate::proto::raft::{SnapshotDownloadRequest, SnapshotDownloadResponse};
use crate::raft::snapshot::FileReader;
use crate::raft::{RaftError, RaftResult, RaftUtils};
use log::{debug, error};
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::{err_box, try_option_mut};

// Snapshot read processor.
pub struct SnapshotDownloadHandler {
    reader: Option<FileReader>,
    chunk_size: usize,
}

impl SnapshotDownloadHandler {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            reader: None,
            chunk_size,
        }
    }

    //@todo Add offset to support disconnection and retransmission.
    pub fn open(&mut self, msg: &Message) -> RaftResult<Message> {
        if self.reader.is_some() {
            return err_box!("The reader has been initialized");
        }

        let req: SnapshotDownloadRequest = msg.parse_header()?;
        let path = RaftUtils::snapshot_file_path(&req);
        let reader = FileReader::from_file(&path, req.offset, self.chunk_size)?;
        if (reader.len() as u64) > req.snapshot_file.len {
            return err_box!(
                "Snapshot file {} has an abnormal length, expected {}, actual {}",
                path,
                reader.len(),
                req.snapshot_file.len
            );
        }

        debug!(
            "sender opening snapshot file for transfer: {}, len: {}",
            path,
            reader.len()
        );

        self.reader = Some(reader);
        let rep = Builder::success(msg)
            .proto_header(SnapshotDownloadResponse::default())
            .build();
        Ok(rep)
    }

    pub fn read_chunk(&mut self, req: &Message) -> RaftResult<Message> {
        let reader = try_option_mut!(self.reader);
        let builder = if !reader.has_remaining() {
            let checksum = reader.checksum();
            debug!(
                "sender snapshot file transfer complete, checksum: {}, len: {}",
                checksum,
                reader.len()
            );
            let header = SnapshotDownloadResponse {
                is_last: true,
                checksum,
            };
            let _ = self.reader.take();
            Builder::success(req).proto_header(header)
        } else {
            let chunk = reader.read_chunk()?;
            Builder::success(req).data(DataSlice::Buffer(chunk))
        };

        Ok(builder.build())
    }

    pub fn complete(&mut self, msg: &Message) -> RaftResult<Message> {
        let _ = self.reader.take();
        Ok(msg.success())
    }
}

impl MessageHandler for SnapshotDownloadHandler {
    type Error = RaftError;

    fn handle(&mut self, msg: &Message) -> RaftResult<Message> {
        let res = match msg.request_status() {
            RequestStatus::Open => self.open(msg),

            RequestStatus::Running => self.read_chunk(msg),

            RequestStatus::Complete => self.complete(msg),

            _ => err_box!("Unsupported request type"),
        };

        match res {
            Err(e) => {
                error!("handler error: {}", e);
                Ok(msg.error_ext(&e))
            }

            ok => Ok(ok?),
        }
    }
}
