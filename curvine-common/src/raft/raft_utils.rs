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

use crate::proto::raft::{
    FsmState, SnapshotData, SnapshotDownloadRequest, SnapshotFileInfo, SnapshotFileList,
};
use crate::raft::RaftResult;
use crate::rocksdb::DBEngine;
use orpc::common::{FileUtils, LocalTime};
use std::path::PathBuf;

// Some tools and methods of raft.
pub struct RaftUtils;

impl RaftUtils {
    // Create a snapshot based on the file directory.
    pub fn create_file_snapshot(
        dir: impl AsRef<str>,
        node_id: u64,
        fsm_state: FsmState,
    ) -> RaftResult<SnapshotData> {
        let dir = dir.as_ref();
        let files = FileUtils::list_files(dir, false)?;
        let mut list = SnapshotFileList {
            dir: dir.to_string(),
            files: vec![],
        };

        for path in files {
            let join_path = PathBuf::from(dir).join(&path);
            let meta = FileUtils::metadata(&join_path)?;
            list.files.push(SnapshotFileInfo {
                path,
                mtime: FileUtils::mtime(&meta).unwrap_or(0),
                ctime: FileUtils::ctime(&meta).unwrap_or(0),
                len: meta.len(),
            })
        }

        let data = SnapshotData {
            snapshot_id: fsm_state.applied.index,
            node_id,
            create_time: LocalTime::mills(),
            bytes_data: None,
            files_data: Some(list),
            fsm_state,
        };

        Ok(data)
    }

    pub fn apply_rocks_snapshot(db: &mut DBEngine, files: &SnapshotFileList) -> RaftResult<()> {
        db.restore(&files.dir)?;
        Ok(())
    }

    pub fn snapshot_file_path(req: &SnapshotDownloadRequest) -> String {
        let mut path = PathBuf::from(&req.dir);
        path.push(&req.snapshot_file.path);
        format!("{}", path.display())
    }

    pub fn zlib_read_full<R: std::io::Read>(reader: &mut R, buf: &mut [u8]) -> RaftResult<usize> {
        let mut off = 0;
        let mut len = 0;
        loop {
            let r = reader.read(&mut buf[off..])?;
            if r == 0 {
                break;
            } else {
                off += r;
                len += r
            }
        }

        Ok(len)
    }
}
