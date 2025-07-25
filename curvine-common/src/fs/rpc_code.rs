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
    Undefined,

    // filesystem API
    Mkdir,
    Delete,
    CreateFile,
    OpenFile,
    AppendFile,
    FileStatus,
    ListStatus,
    Exists,
    Rename,
    AddBlock,
    CompleteFile,
    GetBlockLocations,
    GetMasterInfo,

    Mount,
    UnMount,
    UpdateMount,
    GetMountTable,
    GetMountInfo,

    // Heartbeat interface.
    Heartbeat,

    // block interface.
    WriteBlock,
    ReadBlock,

    // master report interface
    WorkerHeartbeat,
    WorkerBlockReport,

    // Load the task interface
    SubmitLoadJob,
    GetLoadStatus,
    CancelLoadJob,
    ReportLoadTask,
    SubmitLoadTask,
}

impl fmt::Display for RpcCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
