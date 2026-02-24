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

use crate::state::FileStatus;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const SYNC_XATTR_STATE: &str = "cv.sync.state";
pub const SYNC_XATTR_GENERATION: &str = "cv.sync.generation";
pub const SYNC_XATTR_LEASE: &str = "cv.sync.lease";
pub const SYNC_XATTR_OWNER: &str = "cv.sync.owner";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SyncLifecycleOwner {
    #[default]
    Unknown,
    Publish,
    Hydrate,
}

impl SyncLifecycleOwner {
    pub fn as_str(&self) -> Option<&'static str> {
        match self {
            SyncLifecycleOwner::Unknown => None,
            SyncLifecycleOwner::Publish => Some("publish"),
            SyncLifecycleOwner::Hydrate => Some("hydrate"),
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "publish" => Some(SyncLifecycleOwner::Publish),
            "hydrate" => Some(SyncLifecycleOwner::Hydrate),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncLifecycleState {
    Pending,
    Committing,
    Committed,
    Aborted,
    Deleted,
}

impl SyncLifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncLifecycleState::Pending => "pending",
            SyncLifecycleState::Committing => "committing",
            SyncLifecycleState::Committed => "committed",
            SyncLifecycleState::Aborted => "aborted",
            SyncLifecycleState::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(SyncLifecycleState::Pending),
            "committing" => Some(SyncLifecycleState::Committing),
            "committed" => Some(SyncLifecycleState::Committed),
            "aborted" => Some(SyncLifecycleState::Aborted),
            "deleted" => Some(SyncLifecycleState::Deleted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLifecycleMarker {
    pub state: SyncLifecycleState,
    pub generation: String,
    pub lease: String,
    pub owner: SyncLifecycleOwner,
}

impl SyncLifecycleMarker {
    pub fn new(
        state: SyncLifecycleState,
        generation: impl Into<String>,
        lease: impl Into<String>,
    ) -> Self {
        Self::with_owner(state, generation, lease, SyncLifecycleOwner::Unknown)
    }

    pub fn with_owner(
        state: SyncLifecycleState,
        generation: impl Into<String>,
        lease: impl Into<String>,
        owner: SyncLifecycleOwner,
    ) -> Self {
        Self {
            state,
            generation: generation.into(),
            lease: lease.into(),
            owner,
        }
    }

    pub fn from_x_attr(attrs: &HashMap<String, Vec<u8>>) -> Option<Self> {
        let state = decode_x_attr(attrs, SYNC_XATTR_STATE)?;
        let generation = decode_x_attr(attrs, SYNC_XATTR_GENERATION)?;
        let lease = decode_x_attr(attrs, SYNC_XATTR_LEASE)?;
        let owner = decode_x_attr(attrs, SYNC_XATTR_OWNER)
            .as_deref()
            .and_then(SyncLifecycleOwner::parse)
            .unwrap_or_default();
        Some(Self {
            state: SyncLifecycleState::parse(&state)?,
            generation,
            lease,
            owner,
        })
    }

    pub fn from_status(status: &FileStatus) -> Option<Self> {
        Self::from_x_attr(&status.x_attr)
    }

    pub fn apply_attrs(&self, out: &mut HashMap<String, Vec<u8>>) {
        out.insert(
            SYNC_XATTR_STATE.to_string(),
            self.state.as_str().as_bytes().to_vec(),
        );
        out.insert(
            SYNC_XATTR_GENERATION.to_string(),
            self.generation.as_bytes().to_vec(),
        );
        out.insert(SYNC_XATTR_LEASE.to_string(), self.lease.as_bytes().to_vec());
        if let Some(owner) = self.owner.as_str() {
            out.insert(SYNC_XATTR_OWNER.to_string(), owner.as_bytes().to_vec());
        }
    }

    pub fn as_expect_map(&self) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::with_capacity(4);
        self.apply_attrs(&mut map);
        map
    }
}

fn decode_x_attr(attrs: &HashMap<String, Vec<u8>>, key: &str) -> Option<String> {
    let bytes = attrs.get(key)?;
    String::from_utf8(bytes.clone()).ok()
}
