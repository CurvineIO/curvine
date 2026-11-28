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
use crate::raft::{NodeId, RaftClient, RaftError, RaftResult};
use curvine_core_error::err_box;
use curvine_io::DataSlice;
use curvine_runtime::common::{ByteUnit, FileUtils, TimeSpent};
use log::{debug, error, info};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub struct DownloadJob {
    node_id: NodeId,
    chunk_size: usize,
    parallel: usize,
    snapshot_dir: String,
    files: SnapshotFileList,
    client: RaftClient,
}

impl DownloadJob {
    pub fn new(
        node_id: NodeId,
        snapshot_dir: String,
        files: SnapshotFileList,
        conf: &JournalConf,
        client: RaftClient,
    ) -> Self {
        let chunk_size = conf.snapshot_read_chunk_size;
        Self {
            node_id,
            chunk_size,
            parallel: conf.snapshot_download_parallel,
            snapshot_dir,
            files,
            client,
        }
    }

    pub async fn run(&mut self) -> RaftResult<String> {
        let dir = self.snapshot_dir.clone();
        match self.run0().await {
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

    fn effective_parallel(parallel: usize, total: usize) -> usize {
        parallel.max(1).min(total.max(1))
    }

    async fn run0(&mut self) -> RaftResult<()> {
        let total = self.files.files.len();
        let parallel = Self::effective_parallel(self.parallel, total);
        let spend = TimeSpent::new();
        info!(
            "Snapshot {} download start, files {}, parallel {}, owner node {}",
            self.files.dir, total, parallel, self.node_id
        );

        let semaphore = Arc::new(Semaphore::new(parallel));
        let mut tasks: JoinSet<RaftResult<()>> = JoinSet::new();

        for file in self.files.files.iter().cloned() {
            let path = self.get_write_path(&file)?;

            let semaphore = semaphore.clone();
            let client = self.client.clone();
            let dir = self.files.dir.clone();
            let node_id = self.node_id;
            let chunk_size = self.chunk_size;

            tasks.spawn(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .map_err(|e| RaftError::from(format!("Snapshot download cancelled: {}", e)))?;

                let writer = FileWriter::from_file(&path, chunk_size)?;
                let client = client.create_snapshot_client(node_id).await?;
                let client = SnapshotClient::new(dir, file, client);

                Self::download_file(client, 0, writer).await
            });
        }

        let mut failure: Option<RaftError> = None;
        while let Some(joined) = tasks.join_next().await {
            let err = match joined {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => e,
                Err(e) => RaftError::from(e),
            };

            tasks.abort_all();
            if failure.is_none() {
                failure = Some(err);
            }
        }

        match failure {
            Some(e) => Err(e),
            None => {
                info!(
                    "Snapshot {} download complete, files {}, cost {} ms",
                    self.files.dir,
                    total,
                    spend.used_ms()
                );
                Ok(())
            }
        }
    }

    pub async fn download_file(
        client: SnapshotClient,
        offset: u64,
        mut writer: FileWriter,
    ) -> RaftResult<()> {
        let mut seq_id = 0;
        let spend = TimeSpent::new();

        let _ = client.open_file(seq_id, offset).await?;
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

            let msg = client.read_data(seq_id).await?;
            seq_id += 1;

            // Check whether the read is finished.
            if msg.header_len() > 0 {
                let header: SnapshotDownloadResponse = msg.parse_header()?;
                if header.is_last {
                    let receiver_checksum = writer.checksum();
                    let sender_checksum = header.checksum;
                    if writer.write_len() != client.file_len() {
                        error!(
                            "receiver snapshot file {} length mismatch! \
                            written_len: {}, expected_len: {}, \
                            receiver_checksum: {}, sender_checksum: {}",
                            client.file_path(),
                            writer.write_len(),
                            client.file_len(),
                            receiver_checksum,
                            sender_checksum
                        );
                        return err_box!(
                            "Snapshot file {}, length verification failed, \
                            written_len: {}, expected_len: {}",
                            client.file_path(),
                            writer.write_len(),
                            client.file_len()
                        );
                    }
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
            "Snapshot file {} from {} download complete, len {}, cost {} ms",
            client.file_path(),
            client.remote_addr(),
            ByteUnit::new(client.file_len()),
            spend.used_ms()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::DownloadJob;

    #[test]
    fn effective_parallel_is_bounded_by_the_file_count() {
        assert_eq!(DownloadJob::effective_parallel(8, 3), 3);
        assert_eq!(DownloadJob::effective_parallel(8, 100), 8);
    }

    #[test]
    fn effective_parallel_never_drops_to_zero() {
        // A zero would create an empty semaphore and block every transfer.
        assert_eq!(DownloadJob::effective_parallel(0, 4), 1);
        assert_eq!(DownloadJob::effective_parallel(8, 0), 1);
    }
}
