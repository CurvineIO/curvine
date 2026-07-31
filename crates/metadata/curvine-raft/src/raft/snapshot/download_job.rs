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

use crate::conf::JournalConf;
use crate::proto::raft::{SnapshotDownloadResponse, SnapshotFileInfo, SnapshotFileList};
use crate::raft::snapshot::{FileWriter, SnapshotClient};
use crate::raft::{NodeId, RaftClient, RaftResult};
use log::{debug, error, info};
use orpc::common::{ByteUnit, FileUtils, TimeSpent};
use orpc::err_box;
use orpc::runtime::GroupExecutor;
use orpc::sys::DataSlice;
use std::path::PathBuf;
use std::sync::Arc;

// @todo Consider developing concurrent download snapshot files.
pub struct DownloadJob {
    _executor: Arc<GroupExecutor>,
    node_id: NodeId,
    chunk_size: usize,
    snapshot_dir: String,
    files: SnapshotFileList,
    client: RaftClient,
}

impl DownloadJob {
    pub fn new<T: AsRef<str>>(
        _executor: Arc<GroupExecutor>,
        node_id: NodeId,
        snapshot_dir: T,
        files: SnapshotFileList,
        conf: &JournalConf,
        client: RaftClient,
    ) -> Self {
        let chunk_size = conf.snapshot_read_chunk_size;
        Self {
            _executor,
            node_id,
            chunk_size,
            snapshot_dir: snapshot_dir.as_ref().to_string(),
            files,
            client,
        }
    }

    pub fn run(&mut self) -> RaftResult<String> {
        let dir = self.snapshot_dir.clone();
        match self.run0() {
            Ok(_) => Ok(dir),

            Err(e) => {
                let _ = FileUtils::delete_path(dir, true);
                Err(e)
            }
        }
    }

    fn get_write_path(&self, file: &SnapshotFileInfo) -> RaftResult<String> {
        let mut path = PathBuf::from(&self.snapshot_dir);
        path.push(&file.path);

        // If the parent directory does not exist, it needs to be created automatically.
        if let Some(p) = path.parent() {
            if !p.exists() {
                FileUtils::create_dir(p, true)?;
            }
        }

        Ok(format!("{}", path.display()))
    }

    // @todo Will parallel download be considered in the later stage?
    fn run0(&mut self) -> RaftResult<()> {
        for file in &self.files.files {
            // Create writer
            let path = self.get_write_path(file)?;
            let writer = FileWriter::from_file(path, self.chunk_size)?;

            let client = self.client.create_snapshot_client(self.node_id)?;
            let client = SnapshotClient::new(&self.files.dir, file.clone(), client);

            Self::download_file(client, 0, writer)?;
        }

        Ok(())
    }

    pub fn download_file(
        client: SnapshotClient,
        offset: u64,
        mut writer: FileWriter,
    ) -> RaftResult<()> {
        let mut seq_id = 0;
        let spend = TimeSpent::new();

        let _ = client.open_file(seq_id, offset)?;
        seq_id += 1;
        loop {
            if writer.write_len() > client.file_len() {
                return err_box!(
                    "Snapshot file {} abnormal read length, expected {}, actual {}",
                    client.file_path(),
                    client.file_len(),
                    writer.write_len()
                );
            }

            let msg = client.read_data(seq_id)?;
            seq_id += 1;

            // Check whether the read is finished.
            if msg.header_len() > 0 {
                let header: SnapshotDownloadResponse = msg.parse_header()?;
                if header.is_last {
                    // Check check_sum
                    let receiver_checksum = writer.checksum();
                    let sender_checksum = header.checksum;
                    if sender_checksum != receiver_checksum {
                        error!(
                            "receiver snapshot file {} checksum mismatch! \
                            receiver_checksum: {}, sender_checksum: {}, \
                            written_len: {}, expected_len: {}",
                            client.file_path(),
                            receiver_checksum,
                            sender_checksum,
                            writer.write_len(),
                            client.file_len()
                        );
                        return err_box!(
                            "Snapshot file {}, checksum verification failed, \
                            receiver_checksum: {}, sender_checksum: {}",
                            client.file_path(),
                            receiver_checksum,
                            sender_checksum
                        );
                    }
                    debug!(
                        "receiver snapshot file {} checksum verified, checksum: {}",
                        client.file_path(),
                        receiver_checksum
                    );
                    break;
                } else {
                    return err_box!("Status Error");
                }
            }

            // Data is written to the file.
            match msg.data {
                DataSlice::Buffer(buf) => writer.write_chunk(&buf[..])?,
                _ => return err_box!("Unsupported data transfer method"),
            }
        }

        info!(
            "Snapshot file {} download complete, len {}, cost {} ms",
            client.file_path(),
            ByteUnit::new(client.file_len()),
            spend.used_ms()
        );

        Ok(())
    }
}
