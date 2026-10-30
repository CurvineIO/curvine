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

#![allow(clippy::derivable_impls)]

use crate::{FaultKind, HealthState};
use clap::ValueEnum;
use curvine_codec::{CodecError, CodecResult, Decode, Encode, Reader, Writer};
use curvine_core_error::{err_box, CommonError};
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

#[repr(i32)]
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
    ValueEnum,
)]
pub enum StorageType {
    Mem = 0,

    Ssd = 1,

    Hdd = 2,

    Ufs = 3,

    #[num_enum(default)]
    Disk = 4,

    SpdkDisk = 5,
}

impl StorageType {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StorageType::Mem => "MEM",
            StorageType::Ssd => "SSD",
            StorageType::Hdd => "HDD",
            StorageType::Ufs => "UFS",
            StorageType::Disk => "DISK",
            StorageType::SpdkDisk => "SPDK_DISK",
        }
    }

    pub fn from_str_name(value: &str) -> Self {
        Self::try_from(value).unwrap_or(StorageType::Disk)
    }
}

impl TryFrom<&str> for StorageType {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let typ = match value.to_uppercase().as_str() {
            "MEM" => Self::Mem,
            "SSD" => Self::Ssd,
            "HDD" => Self::Hdd,
            "UFS" => Self::Ufs,
            "DISK" => Self::Disk,
            "SPDK" | "SPDK_DISK" => Self::SpdkDisk,

            _ => return err_box!("invalid storage type: {}", value),
        };

        Ok(typ)
    }
}

impl Default for StorageType {
    fn default() -> Self {
        StorageType::Disk
    }
}

impl Encode for StorageType {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i8(*self as i8)
    }
}

impl Decode for StorageType {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let raw = reader.get_i8()?;
        let value = Self::from(raw as i32);
        if value as i8 != raw {
            return Err(CodecError::invalid_tag("StorageType", raw as i64));
        }
        Ok(value)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StorageInfo {
    pub dir_id: u32,
    pub storage_id: String,
    pub failed: bool,
    pub capacity: i64,
    pub fs_used: i64,
    pub non_fs_used: i64,
    pub available: i64,
    pub reserved_bytes: i64,
    pub storage_type: StorageType,
    pub block_num: i64,
    pub dir_path: String,

    /// Health state reported by the worker. `failed` stays for older peers.
    pub health_state: HealthState,

    pub fault_kind: FaultKind,

    /// When the directory became unhealthy (ms since epoch, 0 while healthy).
    pub unhealthy_since_ms: i64,
}

impl StorageInfo {
    /// Whether this directory may take new writes.
    ///
    /// `failed` is checked as well as `health_state`: an older worker only
    /// reports the boolean, and a mixed-version cluster must not treat its
    /// failed directory as healthy just because the newer fields are absent.
    pub fn is_usable(&self) -> bool {
        !self.failed && self.health_state.is_usable()
    }
}

#[cfg(test)]
mod health_compat_test {
    use super::*;
    use crate::ProtoUtils;
    use curvine_proto::StorageInfoProto;

    fn proto(failed: bool, health_state: Option<u32>) -> StorageInfoProto {
        StorageInfoProto {
            dir_id: 1,
            storage_id: "d1".to_string(),
            failed,
            capacity: 100,
            available: 100,
            fs_used: 0,
            non_fs_used: 0,
            reserved_bytes: 0,
            storage_type: StorageType::Disk.into(),
            block_num: 0,
            dir_path: "/data".to_string(),
            health_state,
            fault_kind: None,
            unhealthy_since_ms: None,
        }
    }

    #[test]
    fn older_worker_failed_flag_still_means_unusable() {
        // An older worker sends only `failed`; the newer fields are absent.
        let info = ProtoUtils::storage_info_from_pb(proto(true, None));
        assert_eq!(info.health_state, HealthState::Healthy);
        assert!(!info.is_usable(), "legacy failed dir must stay unusable");
    }

    #[test]
    fn newer_worker_health_state_is_used() {
        let info = ProtoUtils::storage_info_from_pb(proto(
            false,
            Some(u8::from(HealthState::Unusable) as u32),
        ));
        assert!(!info.is_usable());
    }

    #[test]
    fn healthy_directory_is_usable() {
        let info = ProtoUtils::storage_info_from_pb(proto(false, Some(0)));
        assert!(info.is_usable());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn storage_type_spdk_disk_roundtrip() {
        // Verify SpdkDisk variant parses from string and serializes back
        let typ = StorageType::try_from("SPDK_DISK").unwrap();
        assert_eq!(typ, StorageType::SpdkDisk);
        assert_eq!(typ.as_str_name(), "SPDK_DISK");

        // Case-insensitive
        let typ2 = StorageType::try_from("spdk_disk").unwrap();
        assert_eq!(typ2, StorageType::SpdkDisk);

        // Backward-compatible alias used by early SPDK configurations.
        let alias = StorageType::try_from("spdk").unwrap();
        assert_eq!(alias, StorageType::SpdkDisk);

        // Numeric value
        let val: i32 = StorageType::SpdkDisk.into();
        assert_eq!(val, 5);
    }
}
