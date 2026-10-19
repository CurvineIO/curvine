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

use curvine_codec::{CodecError, CodecResult, Decode, Encode, Reader, Writer};
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

impl Encode for LockType {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i8(*self as i8)
    }
}

impl Decode for LockType {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let raw = reader.get_i8()?;
        let value = Self::from(raw as u8);
        if value as u8 != raw as u8 {
            return Err(CodecError::invalid_tag("LockType", raw as i64));
        }
        Ok(value)
    }
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

impl Encode for LockFlags {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i8(*self as i8)
    }
}

impl Decode for LockFlags {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let raw = reader.get_i8()?;
        let value = Self::from(raw as u8);
        if value as u8 != raw as u8 {
            return Err(CodecError::invalid_tag("LockFlags", raw as i64));
        }
        Ok(value)
    }
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

impl Encode for FileLock {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        self.client_id.encode(writer)?;
        writer.put_u64(self.owner_id)?;
        writer.put_u32(self.pid)?;
        writer.put_u64(self.acquire_time)?;
        self.lock_type.encode(writer)?;
        self.lock_flags.encode(writer)?;
        writer.put_u64(self.start)?;
        writer.put_u64(self.end)?;
        writer.put_u8(u8::from(self.session_owned))
    }
}

impl Decode for FileLock {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let client_id = String::decode(reader)?;
        let owner_id = reader.get_u64()?;
        let pid = reader.get_u32()?;
        let acquire_time = reader.get_u64()?;
        let lock_type = LockType::decode(reader)?;
        let lock_flags = LockFlags::decode(reader)?;
        let start = reader.get_u64()?;
        let end = reader.get_u64()?;
        let session_owned = if reader.is_empty() {
            false
        } else {
            reader.get_u8()? != 0
        };
        Ok(Self {
            client_id,
            owner_id,
            pid,
            acquire_time,
            lock_type,
            lock_flags,
            start,
            end,
            session_owned,
        })
    }
}
