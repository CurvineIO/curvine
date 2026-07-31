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

use crate::proto::raft::{SnapshotDownloadRequest, SnapshotDownloadResponse, SnapshotFileInfo};
use crate::raft::{RaftCode, RaftError, RaftResult};
use orpc::client::SyncClient;
use orpc::common::Utils;
use orpc::message::{Builder, Message, RequestStatus};

// Snapshot data download client service.
// Change the customer service side to synchronous operation.
pub struct SnapshotClient {
    dir: String,
    file: SnapshotFileInfo,
    req_id: i64,
    client: SyncClient,
}

impl SnapshotClient {
    pub fn new(dir: impl Into<String>, file: SnapshotFileInfo, client: SyncClient) -> Self {
        Self {
            dir: dir.into(),
            file,
            req_id: Utils::req_id(),
            client,
        }
    }

    // Request to download the snapshot file.
    pub fn open_file(&self, seq_id: i32, offset: u64) -> RaftResult<SnapshotDownloadResponse> {
        let header = SnapshotDownloadRequest {
            dir: self.dir.clone(),
            snapshot_file: self.file.clone(),
            offset,
        };
        let msg = Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Open)
            .req_id(self.req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let rep_msg = self.client.rpc(msg)?;
        rep_msg.check_error_ext::<RaftError>()?;
        let rep_header: SnapshotDownloadResponse = rep_msg.parse_header()?;
        Ok(rep_header)
    }

    // Read snapshot data.
    pub fn read_data(&self, seq_id: i32) -> RaftResult<Message> {
        let msg = Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Running)
            .req_id(self.req_id)
            .seq_id(seq_id)
            .build();

        let rep_msg = self.client.rpc(msg)?;
        rep_msg.check_error_ext::<RaftError>()?;
        Ok(rep_msg)
    }

    // Snapshot file reading is completed
    pub fn close_file(&self, seq_id: i32) -> RaftResult<()> {
        let msg = Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Complete)
            .req_id(self.req_id)
            .seq_id(seq_id)
            .build();

        let rep_msg = self.client.rpc(msg)?;
        rep_msg.check_error_ext::<RaftError>()?;
        Ok(())
    }

    pub fn file_len(&self) -> u64 {
        self.file.len
    }

    pub fn file_path(&self) -> &str {
        &self.file.path
    }
}
