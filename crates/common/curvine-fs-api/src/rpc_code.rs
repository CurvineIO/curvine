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

use num_enum::{FromPrimitive, IntoPrimitive};
use std::fmt;

pub const GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS: usize = 64;
pub const GET_BLOCK_LOCATIONS_BATCH_MAX_ENCODED_PATH_BYTES: usize = 1024 * 1024;
pub const GET_BLOCK_LOCATIONS_BATCH_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const MASTER_CAPABILITY_GET_BLOCK_LOCATIONS_BATCH: &str = "get-block-locations-batch";
pub const MASTER_CAPABILITY_GET_CONTENT_SUMMARY_SNAPSHOT: &str = "get-content-summary-snapshot-v1";

#[repr(i8)]
#[derive(Debug, IntoPrimitive, FromPrimitive, PartialEq, Eq, Hash, Copy, Clone)]
pub enum RpcCode {
    #[num_enum(default)]
    Undefined = 0,
    Heartbeat = 1,

    // filesystem API
    Mkdir = 2,
    Delete = 3,
    CreateFile = 4,
    OpenFile = 5,
    AppendFile = 6,
    FileStatus = 7,
    ListStatus = 8,
    Exists = 9,
    Rename = 10,
    AddBlock = 11,
    CompleteFile = 12,
    GetBlockLocations = 13,
    GetFilesystemInfo = 14,
    SetAttr = 15,
    Symlink = 16,
    Link = 17,
    ResizeFile = 18,
    AssignWorker = 19,
    GetLock = 20,
    SetLock = 21,
    ListLock = 22,
    CreateFilesBatch = 23,
    AddBlocksBatch = 24,
    CompleteFilesBatch = 25,
    Free = 26,
    ListOptions = 27,
    GetCvMetadataSnapshotPage = 28,
    // 29: retired (GetCvMetadataDeltaPage), do not reuse.

    // manager interface.
    Mount = 30,
    UnMount = 31,
    UpdateMount = 32,
    GetMountTable = 33,
    GetMountInfo = 34,

    SubmitJob = 35,
    GetJobStatus = 36,
    CancelJob = 37,
    ReportTask = 38,
    SubmitTask = 39,
    SubmitTransfer = 46,
    GetTransferStatus = 47,
    CancelTransfer = 48,
    ReportTransferTask = 49,
    QueryTransferTask = 50,
    WatchTransfer = 51,
    ListTransfers = 52,
    ListTransferTenants = 53,
    RetryTransfer = 54,
    GetFileBlockDetails = 59,
    WorkerHeartbeat = 40,
    WorkerBlockReport = 41,

    SubmitBlockReplicationJob = 42,
    ReportBlockReplicationResult = 43,
    RequestReplacementWorker = 44,
    ReportUnderReplicatedBlocks = 45,

    SetWorkerStatus = 55,
    ListJobStatus = 56,
    ReportTransferTaskBatch = 57,
    CloseFile = 58,

    PrepareReplication = 65,
    ReconcileReplication = 66,
    SubmitFencedReplication = 67,
    GetContentSummary = 68,
    GetDirSpaceStatus = 69,

    // recycle bin (70-72: main-next uses 61-64 for batch/runtime RPCs).
    ListTrash = 70,
    RestoreTrash = 71,
    EmptyTrash = 72,

    MetricsReport = 60,
    GetBlockLocationsBatch = 61,
    UpdateRuntimeConfig = 62,
    GetRuntimeConfig = 63,
    ResetRuntimeConfig = 64,

    CreateLeaderSnapshot = 70,

    // block interface.
    WriteBlock = 80,
    ReadBlock = 81,
    WriteBlocksBatch = 82,
    WriteCommitsBatch = 83,
    WriteReplicationBlock = 84,
}

impl fmt::Display for RpcCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn content_summary_rpc_code_test() {
        // Dir-space content summary uses the next free master RPC code 68.
        assert_eq!(RpcCode::GetContentSummary as i8, 68);
        assert_eq!(RpcCode::from(68), RpcCode::GetContentSummary);
        // The wire round-trip must not collide with any existing code.
        assert_eq!(RpcCode::from(68i8).to_string(), "GetContentSummary");
    }

    #[test]
    pub fn content_summary_capability_test() {
        assert_eq!(
            MASTER_CAPABILITY_GET_CONTENT_SUMMARY_SNAPSHOT,
            "get-content-summary-snapshot-v1"
        );
    }
}
