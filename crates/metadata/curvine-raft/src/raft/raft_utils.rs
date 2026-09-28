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
use bytes::BytesMut;
use curvine_core_error::err_box;
use curvine_runtime::common::{FileUtils, LocalTime};
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

    /// Upper bound on zlib output for `len` bytes of input (`compressBound`).
    pub(crate) fn zlib_compress_bound(len: usize) -> usize {
        len.saturating_add(len >> 12)
            .saturating_add(len >> 14)
            .saturating_add(len >> 25)
            .saturating_add(13)
    }

    /// Read a zlib stream until EOF, rejecting output longer than `max_len`.
    ///
    /// A fixed buffer that is merely "usually big enough" cannot be used.
    /// `Read::read` on a full slice returns `Ok(0)`, which is indistinguishable
    /// from EOF, so the rest of the stream is dropped. The reader stops after
    /// `max_len` bytes and checks one more byte so a hostile or oversized
    /// stream fails before it is buffered.
    pub(crate) fn zlib_read_limited<R: std::io::Read>(
        reader: &mut R,
        max_len: usize,
    ) -> RaftResult<BytesMut> {
        let mut out = BytesMut::with_capacity(max_len.min(64 * 1024));
        let mut tmp = [0u8; 64 * 1024];
        loop {
            if out.len() >= max_len {
                let extra = reader.read(&mut tmp[..1])?;
                if extra == 0 {
                    break;
                }
                return err_box!("zlib output exceeds limit {}", max_len);
            }
            let room = max_len - out.len();
            let take = room.min(tmp.len());
            let n = reader.read(&mut tmp[..take])?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&tmp[..n]);
        }
        Ok(out)
    }
}
