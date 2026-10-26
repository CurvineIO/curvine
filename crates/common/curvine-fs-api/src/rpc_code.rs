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

use curvine_model::OpenFlags;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::fmt;

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
    GetFileBlockDetails = 55,
    WorkerHeartbeat = 40,
    WorkerBlockReport = 41,

    SubmitBlockReplicationJob = 42,
    ReportBlockReplicationResult = 43,
    RequestReplacementWorker = 44,
    ReportUnderReplicatedBlocks = 45,

    PrepareReplication = 56,
    ReconcileReplication = 57,
    SubmitFencedReplication = 58,

    MetricsReport = 60,

    // recycle bin.
    ListTrash = 61,
    RestoreTrash = 62,
    EmptyTrash = 63,

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

impl RpcCode {
    /// Returns whether this RPC is an explicitly permitted follower metadata read.
    ///
    /// This is deliberately a closed allowlist. New RPC codes, including trash
    /// operations, remain leader-only until they are reviewed and added here.
    pub fn is_follower_read(&self) -> bool {
        matches!(
            self,
            Self::FileStatus
                | Self::ListStatus
                | Self::Exists
                | Self::GetBlockLocations
                | Self::ListOptions
                | Self::GetMountTable
                | Self::GetMountInfo
        )
    }

    /// Checks the follower-read allowlist, including OpenFile's encoded flags.
    ///
    /// `OpenFile` is allowed only for a valid, exactly read-only flag set. A
    /// missing or malformed flag word is denied, as are create, truncate,
    /// append, and exclusive-create flags. Non-OpenFile RPCs do not use the
    /// optional flag word.
    pub fn allows_follower_read(&self, open_file_flags: Option<u32>) -> bool {
        if *self == Self::OpenFile {
            let Some(flags) = open_file_flags.and_then(OpenFlags::from_bits) else {
                return false;
            };
            return flags.read_only()
                && !flags.intersects(
                    OpenFlags::CREAT | OpenFlags::EXCL | OpenFlags::TRUNC | OpenFlags::APPEND,
                );
        }

        self.is_follower_read()
    }
}

#[cfg(test)]
mod tests {
    use super::RpcCode;
    use curvine_model::OpenFlags;

    #[test]
    fn follower_read_allowlist_is_explicit_and_excludes_trash_and_unknown_codes() {
        for code in [
            RpcCode::FileStatus,
            RpcCode::ListStatus,
            RpcCode::Exists,
            RpcCode::GetBlockLocations,
            RpcCode::ListOptions,
            RpcCode::GetMountTable,
            RpcCode::GetMountInfo,
        ] {
            assert!(code.allows_follower_read(None), "{code:?}");
        }

        for code in [
            RpcCode::Undefined,
            RpcCode::OpenFile,
            RpcCode::GetFilesystemInfo,
            RpcCode::GetFileBlockDetails,
            RpcCode::ListTrash,
            RpcCode::RestoreTrash,
            RpcCode::EmptyTrash,
            RpcCode::Mkdir,
            RpcCode::Delete,
            RpcCode::Rename,
        ] {
            assert!(!code.allows_follower_read(None), "{code:?}");
        }

        assert!(!RpcCode::from(127).allows_follower_read(None));
    }

    #[test]
    fn follower_open_file_requires_valid_read_only_flags() {
        assert!(RpcCode::OpenFile.allows_follower_read(Some(0)));
        assert!(RpcCode::OpenFile.allows_follower_read(Some(OpenFlags::RDONLY.bits())));

        for flags in [
            OpenFlags::WRONLY.bits(),
            OpenFlags::RDWR.bits(),
            OpenFlags::CREAT.bits(),
            OpenFlags::EXCL.bits(),
            OpenFlags::TRUNC.bits(),
            OpenFlags::APPEND.bits(),
            OpenFlags::RDONLY.bits() | (1 << 31),
        ] {
            assert!(
                !RpcCode::OpenFile.allows_follower_read(Some(flags)),
                "flags {flags:#x} must be denied"
            );
        }

        assert!(!RpcCode::OpenFile.allows_follower_read(None));
    }
}
