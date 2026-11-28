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
use crate::raft::{RaftResult, RaftUtils};
use curvine_core_error::{err_box, try_option_mut};
use curvine_io::DataSlice;
use curvine_rpc::message::{Builder, Message, RequestStatus};
use log::{debug, error};
use std::path::{Path, PathBuf};

// Snapshot read processor.
pub struct SnapshotDownloadHandler {
    reader: Option<FileReader>,
    chunk_size: usize,
    root: Option<PathBuf>,
}

impl SnapshotDownloadHandler {
    pub fn new(chunk_size: usize) -> Self {
        Self::with_root(chunk_size, None)
    }

    pub fn with_root(chunk_size: usize, root: Option<PathBuf>) -> Self {
        Self {
            reader: None,
            chunk_size,
            root,
        }
    }

    //@todo Add offset to support disconnection and retransmission.
    pub fn open(&mut self, msg: &Message) -> RaftResult<Message> {
        if self.reader.is_some() {
            return err_box!("The reader has been initialized");
        }

        let req: SnapshotDownloadRequest = msg.parse_header()?;
        let root = match self.root.as_ref() {
            Some(root) => std::fs::canonicalize(root)?,
            None => return err_box!("Snapshot files are not enabled for this store"),
        };
        let file = Path::new(&req.snapshot_file.path);
        RaftUtils::validate_snapshot_file_path(&req.snapshot_file.path)?;
        let dir = std::fs::canonicalize(&req.dir)?;
        if dir == root || !dir.starts_with(&root) {
            return err_box!("Snapshot directory is outside the configured checkpoint root");
        }
        let path = std::fs::canonicalize(dir.join(file))?;
        if !path.starts_with(&dir) || !path.is_file() {
            return err_box!("Snapshot file is outside its checkpoint directory");
        }
        let path = match path.to_str() {
            Some(path) => path,
            None => return err_box!("Snapshot file path is not UTF-8"),
        };
        if req.offset > req.snapshot_file.len {
            return err_box!("Snapshot offset exceeds file length");
        }
        let reader = FileReader::from_file(path, req.offset, self.chunk_size)?;
        if (reader.len() as u64) != req.snapshot_file.len {
            return err_box!(
                "Snapshot file {} has an abnormal length, expected {}, actual {}",
                path,
                req.snapshot_file.len,
                reader.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::raft::SnapshotFileInfo;
    use crate::raft::{RaftCode, RaftError};
    use curvine_runtime::common::Utils;

    fn open(dir: &Path, file: &str) -> Message {
        Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Open)
            .proto_header(SnapshotDownloadRequest {
                dir: dir.to_string_lossy().into_owned(),
                snapshot_file: SnapshotFileInfo {
                    path: file.into(),
                    len: 4,
                    mtime: 0,
                    ctime: 0,
                },
                offset: 0,
            })
            .build()
    }

    #[test]
    fn snapshot_open_is_confined_to_checkpoint_root() {
        let temp = std::env::temp_dir().join(format!("raft-snapshot-path-{}", Utils::rand_id()));
        let root = temp.join("checkpoints");
        let checkpoint = root.join("ck-1");
        std::fs::create_dir_all(&checkpoint).unwrap();
        std::fs::write(checkpoint.join("data"), b"good").unwrap();
        std::fs::write(temp.join("secret"), b"nope").unwrap();

        let mut handler = SnapshotDownloadHandler::with_root(1024, Some(root.clone()));
        let valid = handler.handle(&open(&checkpoint, "data")).unwrap();
        valid.check_error_ext::<RaftError>().unwrap();
        handler.reader = None;
        let absolute_secret = temp.join("secret");
        for (dir, file) in [
            (checkpoint.as_path(), "../../secret"),
            (checkpoint.as_path(), absolute_secret.to_str().unwrap()),
            (temp.as_path(), "secret"),
        ] {
            assert!(handler
                .handle(&open(dir, file))
                .unwrap()
                .check_error_ext::<RaftError>()
                .is_err());
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(temp.join("secret"), checkpoint.join("link")).unwrap();
            assert!(handler
                .handle(&open(&checkpoint, "link"))
                .unwrap()
                .check_error_ext::<RaftError>()
                .is_err());
        }
        let mut disabled = SnapshotDownloadHandler::new(1024);
        assert!(disabled
            .handle(&open(&checkpoint, "data"))
            .unwrap()
            .check_error_ext::<RaftError>()
            .is_err());
        std::fs::remove_dir_all(temp).unwrap();
    }
}
