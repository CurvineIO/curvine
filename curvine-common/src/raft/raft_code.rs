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

#[repr(i8)]
#[derive(Debug, Copy, Clone, IntoPrimitive, FromPrimitive, Eq, PartialEq)]
pub enum RaftCode {
    #[num_enum(default)]
    Propose = 1,
    ConfChange = 2,
    Ping = 3,
    Unreachable = 4,
    Raft = 5,
    SnapshotDownload = 6,
}
