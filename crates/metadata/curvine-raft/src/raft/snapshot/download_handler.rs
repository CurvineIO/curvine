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
use crate::raft::snapshot::{DownloadJob, FileReader};
use crate::raft::{RaftResult, RaftUtils};
use curvine_core_error::{err_box, try_option_mut};
use curvine_io::DataSlice;
use curvine_rpc::message::{Builder, Message, RequestStatus};
use log::{debug, error};

/// Directory of the currently registered snapshot, or `None` when the node has
/// none. Served files must belong to it.
pub type RegisteredSnapshotDir = std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>;

// Snapshot read processor.
pub struct SnapshotDownloadHandler {
    reader: Option<FileReader>,
    chunk_size: usize,
    registered_dir: RegisteredSnapshotDir,
}

impl SnapshotDownloadHandler {
    pub fn new(chunk_size: usize, registered_dir: RegisteredSnapshotDir) -> Self {
        Self {
            reader: None,
            chunk_size,
            registered_dir,
        }
    }

    /// The request dir and file name are attacker-controlled. Only files under
    /// the currently registered snapshot directory may be served.
    fn check_request(&self, req: &SnapshotDownloadRequest) -> RaftResult<()> {
        let Some(registered) = (self.registered_dir)() else {
            return err_box!("snapshot download rejected: no registered snapshot");
        };
        if req.dir != registered {
            return err_box!(
                "snapshot download rejected: dir {:?} is not the registered snapshot {:?}",
                req.dir,
                registered
            );
        }
        DownloadJob::validate_files(std::slice::from_ref(&req.snapshot_file))
    }

    //@todo Add offset to support disconnection and retransmission.
    pub fn open(&mut self, msg: &Message) -> RaftResult<Message> {
        if self.reader.is_some() {
            return err_box!("The reader has been initialized");
        }

        let req: SnapshotDownloadRequest = msg.parse_header()?;
        self.check_request(&req)?;
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

    pub fn handle(&mut self, msg: &Message) -> RaftResult<Message> {
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
