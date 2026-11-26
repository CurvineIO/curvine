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

use crate::state::{StorageType, TtlAction};
use curvine_codec::{CodecError, CodecResult, Decode, Encode, Reader, Writer};
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

#[repr(i8)]
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    IntoPrimitive,
    FromPrimitive,
    Eq,
    Default,
    Hash,
)]
pub enum StorageState {
    #[default]
    Cv = 1,
    Ufs = 2,
    Both = 3,
}

impl Encode for StorageState {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i8(*self as i8)
    }
}

impl Decode for StorageState {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let raw = reader.get_i8()?;
        let value = Self::from(raw);
        if value as i8 != raw {
            return Err(CodecError::invalid_tag("StorageState", raw as i64));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoragePolicy {
    pub storage_type: StorageType,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,
    pub ufs_mtime: i64,
    pub state: StorageState,
}

impl StoragePolicy {
    pub fn with_cv(new_policy: StoragePolicy) -> Self {
        Self {
            ufs_mtime: 0,
            state: StorageState::Cv,
            ..new_policy
        }
    }

    pub fn with_ufs(new_policy: StoragePolicy) -> Self {
        Self {
            ufs_mtime: new_policy.ufs_mtime,
            state: StorageState::Ufs,
            ..new_policy
        }
    }

    pub fn new_ufs() -> Self {
        Self {
            storage_type: StorageType::Disk,
            ttl_ms: 0,
            ttl_action: TtlAction::None,
            ufs_mtime: 0,
            state: StorageState::Ufs,
        }
    }

    pub fn overwrite(&mut self, new_policy: StoragePolicy) {
        self.storage_type = new_policy.storage_type;
        self.ttl_ms = new_policy.ttl_ms;
        self.ttl_action = new_policy.ttl_action;

        self.detach_ufs();
    }

    /// On write: detach from UFS (close link); state→Cv, ufs_mtime=0.
    pub fn detach_ufs(&mut self) {
        self.state = StorageState::Cv;
        self.ufs_mtime = 0;
    }

    /// Drops an unusable Curvine cache copy while retaining the UFS source metadata.
    ///
    /// This is used when the master knows that every readable replica for a cache
    /// block has been lost. Unlike `detach_ufs`, it deliberately preserves
    /// `ufs_mtime` so the file can be re-admitted from its authoritative source.
    pub fn invalidate_cache(&mut self) -> bool {
        if !self.both_exists() {
            return false;
        }

        self.state = StorageState::Ufs;
        true
    }

    pub fn free(&mut self) -> bool {
        match self.ttl_action {
            TtlAction::Free if self.both_exists() => {
                self.state = StorageState::Ufs;
                true
            }

            TtlAction::Delete => {
                if self.both_exists() {
                    self.state = StorageState::Ufs;
                }

                self.ufs_mtime = 0;
                true
            }

            _ => false,
        }
    }

    pub fn save_ufs(&mut self, ufs_mtime: i64) {
        if ufs_mtime > 0 {
            self.ufs_mtime = ufs_mtime;
            self.state = StorageState::Both;
        }
    }

    pub fn ufs_exists(&self) -> bool {
        matches!(self.state, StorageState::Ufs | StorageState::Both)
    }

    pub fn cv_exists(&self) -> bool {
        matches!(self.state, StorageState::Cv | StorageState::Both)
    }

    pub fn ufs_only(&self) -> bool {
        self.state == StorageState::Ufs
    }

    pub fn cv_only(&self) -> bool {
        self.state == StorageState::Cv
    }

    pub fn both_exists(&self) -> bool {
        self.state == StorageState::Both
    }
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Disk,
            ttl_ms: 0,
            ttl_action: TtlAction::None,
            ufs_mtime: 0,
            state: StorageState::Cv,
        }
    }
}

impl Encode for StoragePolicy {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        self.storage_type.encode(writer)?;
        writer.put_i64(self.ttl_ms)?;
        self.ttl_action.encode(writer)?;
        writer.put_i64(self.ufs_mtime)?;
        self.state.encode(writer)
    }
}

impl Decode for StoragePolicy {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            storage_type: StorageType::decode(reader)?,
            ttl_ms: reader.get_i64()?,
            ttl_action: TtlAction::decode(reader)?,
            ufs_mtime: reader.get_i64()?,
            state: StorageState::decode(reader)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_state_tags_round_trip_and_unknown_tags_are_rejected() {
        for raw in [1u8, 2, 3] {
            let bytes = [raw];
            let mut reader = Reader::new(&bytes, 1).unwrap();
            let value = StorageState::decode(&mut reader).unwrap();
            let mut writer = Writer::new(1).unwrap();
            value.encode(&mut writer).unwrap();
            assert_eq!(writer.into_inner(), vec![raw]);
        }
        for raw in [0u8, 4, 100, 255] {
            let bytes = [raw];
            let mut reader = Reader::new(&bytes, 1).unwrap();
            let err = StorageState::decode(&mut reader).unwrap_err();
            assert!(
                err.to_string().contains("invalid StorageState tag"),
                "tag {raw}: {err}"
            );
        }
    }

    #[test]
    fn storage_policy_matches_frozen_layout() {
        let policy = StoragePolicy {
            storage_type: StorageType::Disk,
            ttl_ms: 5,
            ttl_action: TtlAction::Free,
            ufs_mtime: 9,
            state: StorageState::Both,
        };
        let mut writer = Writer::new(1).unwrap();
        policy.encode(&mut writer).unwrap();
        let bytes = writer.into_inner();
        assert_eq!(
            bytes,
            vec![4, 5, 0, 0, 0, 0, 0, 0, 0, 2, 9, 0, 0, 0, 0, 0, 0, 0, 3]
        );
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert_eq!(StoragePolicy::decode(&mut reader).unwrap(), policy);
    }
}
