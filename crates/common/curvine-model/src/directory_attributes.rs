// Copyright 2026 OPPO.
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

const BASE_TAG: [u8; 4] = *b"CDA2";
const DELTA_TAG: [u8; 4] = *b"CDD3";
const ATTRIBUTE_ENCODED_LEN: usize = 32;
const DELTA_ENCODED_LEN: usize = 72;

/// Mutable attributes of a directory stored independently from its inode.
///
/// The fixed encoding is deliberately independent of serde so it can be
/// decoded safely by the RocksDB merge callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryAttributes {
    pub mtime: i64,
    pub ctime: i64,
    pub nlink: u32,
    /// Monotonically advances with every dentry mutation in this directory.
    pub version: u64,
}

impl DirectoryAttributes {
    pub fn new(mtime: i64, ctime: i64, nlink: u32) -> Self {
        Self {
            mtime,
            ctime,
            nlink,
            version: 0,
        }
    }

    pub fn encode(self) -> [u8; ATTRIBUTE_ENCODED_LEN] {
        let mut bytes = [0; ATTRIBUTE_ENCODED_LEN];
        bytes[..4].copy_from_slice(&BASE_TAG);
        bytes[4..12].copy_from_slice(&self.mtime.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.ctime.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.nlink.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.version.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ATTRIBUTE_ENCODED_LEN || bytes[..4] != BASE_TAG {
            return None;
        }

        Some(Self {
            mtime: i64::from_le_bytes(bytes[4..12].try_into().ok()?),
            ctime: i64::from_le_bytes(bytes[12..20].try_into().ok()?),
            nlink: u32::from_le_bytes(bytes[20..24].try_into().ok()?),
            version: u64::from_le_bytes(bytes[24..32].try_into().ok()?),
        })
    }

    pub fn apply(&mut self, delta: DirectoryAttributeDelta) -> Option<()> {
        let nlink = self.nlink.checked_add_signed(delta.nlink_delta)?;
        let version = self.version.checked_add(delta.version_delta)?;
        self.mtime = self.mtime.max(delta.mtime);
        self.ctime = self.ctime.max(delta.ctime);
        self.nlink = nlink;
        self.version = version;
        Some(())
    }
}

/// An ordered update for [`DirectoryAttributes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryAttributeDelta {
    pub mtime: i64,
    pub ctime: i64,
    pub nlink_delta: i32,
    pub version_delta: u64,
    base: Option<DirectoryAttributes>,
}

impl DirectoryAttributeDelta {
    pub fn new(mtime: i64, ctime: i64, nlink_delta: i32) -> Self {
        Self {
            mtime,
            ctime,
            nlink_delta,
            version_delta: 1,
            base: None,
        }
    }

    /// Supplies the immutable pre-update directory value when this delta may
    /// be the first record for a newly created directory. Multiple concurrent
    /// first deltas carry the same base; merge applies the base once and every
    /// delta once.
    pub fn with_base(mut self, base: DirectoryAttributes) -> Self {
        self.base = Some(base);
        self
    }

    pub fn base(&self) -> Option<DirectoryAttributes> {
        self.base
    }

    pub fn for_child(mtime: i64) -> Self {
        Self::new(mtime, mtime, 0)
    }

    pub fn encode(self) -> [u8; DELTA_ENCODED_LEN] {
        let mut bytes = [0; DELTA_ENCODED_LEN];
        bytes[..4].copy_from_slice(&DELTA_TAG);
        if let Some(base) = self.base {
            bytes[4] = 1;
            bytes[8..16].copy_from_slice(&base.mtime.to_le_bytes());
            bytes[16..24].copy_from_slice(&base.ctime.to_le_bytes());
            bytes[24..28].copy_from_slice(&base.nlink.to_le_bytes());
            bytes[32..40].copy_from_slice(&base.version.to_le_bytes());
        }
        bytes[40..48].copy_from_slice(&self.mtime.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.ctime.to_le_bytes());
        bytes[56..60].copy_from_slice(&self.nlink_delta.to_le_bytes());
        bytes[64..72].copy_from_slice(&self.version_delta.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != DELTA_ENCODED_LEN || bytes[..4] != DELTA_TAG {
            return None;
        }
        let base = match bytes[4] {
            0 => None,
            1 => Some(
                DirectoryAttributes::new(
                    i64::from_le_bytes(bytes[8..16].try_into().ok()?),
                    i64::from_le_bytes(bytes[16..24].try_into().ok()?),
                    u32::from_le_bytes(bytes[24..28].try_into().ok()?),
                )
                .with_version(u64::from_le_bytes(bytes[32..40].try_into().ok()?)),
            ),
            _ => return None,
        };
        Some(Self {
            mtime: i64::from_le_bytes(bytes[40..48].try_into().ok()?),
            ctime: i64::from_le_bytes(bytes[48..56].try_into().ok()?),
            nlink_delta: i32::from_le_bytes(bytes[56..60].try_into().ok()?),
            version_delta: u64::from_le_bytes(bytes[64..72].try_into().ok()?),
            base,
        })
    }

    pub fn combine(self, other: Self) -> Option<Self> {
        // Opposite signs may hide an intermediate nlink underflow or overflow.
        if (self.nlink_delta < 0 && other.nlink_delta > 0)
            || (self.nlink_delta > 0 && other.nlink_delta < 0)
        {
            return None;
        }
        let base = match (self.base, other.base) {
            (Some(left), Some(right)) if left != right => return None,
            // A base can only initialize deltas that follow it. Returning
            // `None` keeps this ordered pair for full merge, which rejects a
            // missing base instead of accepting a malformed history.
            (None, Some(_)) => return None,
            (Some(base), _) => Some(base),
            (None, None) => None,
        };
        Some(Self {
            mtime: self.mtime.max(other.mtime),
            ctime: self.ctime.max(other.ctime),
            nlink_delta: self.nlink_delta.checked_add(other.nlink_delta)?,
            version_delta: self.version_delta.checked_add(other.version_delta)?,
            base,
        })
    }
}

impl DirectoryAttributes {
    fn with_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }
}
