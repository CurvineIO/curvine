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

use clap::ValueEnum;
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
}

impl StorageType {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StorageType::Mem => "MEM",
            StorageType::Ssd => "SSD",
            StorageType::Hdd => "HDD",
            StorageType::Ufs => "UFS",
            StorageType::Disk => "DISK",
        }
    }

    pub fn from_str_name(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "MEM" => Self::Mem,
            "SSD" => Self::Ssd,
            "HDD" => Self::Hdd,
            "UFS" => Self::Ufs,
            _ => Self::Disk,
        }
    }
}

impl Default for StorageType {
    fn default() -> Self {
        StorageType::Disk
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
    pub storage_type: StorageType,
    pub block_num: i64,
}
