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
    GetMasterInfo = 14,
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
    WorkerHeartbeat = 40,
    WorkerBlockReport = 41,

    SubmitBlockReplicationJob = 42,
    ReportBlockReplicationResult = 43,

    MetricsReport = 60,

    // block interface.
    WriteBlock = 80,
    ReadBlock = 81,
    WriteBlocksBatch = 82,
    WriteCommitsBatch = 83,
}

impl fmt::Display for RpcCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
