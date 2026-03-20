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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn free(&mut self) -> bool {
        if self.both_exists() && self.ttl_action != TtlAction::None {
            self.state = StorageState::Ufs;
            true
        } else {
            false
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
