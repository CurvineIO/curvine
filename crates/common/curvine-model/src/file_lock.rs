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
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Default,
)]
pub enum LockType {
    #[default]
    ReadLock = 0,
    WriteLock = 1,
    UnLock = 2,
}

#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Default,
)]
pub enum LockFlags {
    #[default]
    Plock = 0, // POSIX lock (fcntl)
    Flock = 1, // BSD lock (flock)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct FileLock {
    pub client_id: String,
    pub owner_id: u64,
    pub pid: u32,
    pub acquire_time: u64,
    pub lock_type: LockType,
    pub lock_flags: LockFlags,
    pub start: u64,
    pub end: u64,
    /// The owner keeps a client session lease alive (it sends `ClientHeartbeat`),
    /// so this lock is reclaimed by session expiry instead of `lock_expire_time`.
    ///
    /// Set by the client at the `SetLock` boundary; a client that does not send
    /// heartbeats leaves it `false` and keeps the legacy TTL behaviour.
    #[serde(default)]
    pub session_owned: bool,
}
