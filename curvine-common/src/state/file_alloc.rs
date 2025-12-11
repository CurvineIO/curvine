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

use crate::FsResult;
use bitflags::bitflags;
use orpc::err_box;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct FileAllocMode: i32 {
        /// Default: allocate space, may contain garbage data (no flags)
        const DEFAULT = 0;

        /// FALLOC_FL_KEEP_SIZE (0x01): Keep file size unchanged, only pre-allocate
        const KEEP_SIZE = 1;

        /// FALLOC_FL_PUNCH_HOLE (0x02): Deallocate space in the range, creating a hole
        const PunchHole = 2;

        /// FALLOC_FL_NO_HIDE_STALE (0x04): Don't hide stale data
        const NO_HIDE_STALE = 4;

        /// FALLOC_FL_COLLAPSE_RANGE (0x08): Remove a range of bytes from the file
        const COLLAPSE_RANGE = 8;

        /// FALLOC_FL_ZERO_RANGE (0x10): Allocate and zero the range
        const ZERO_RANGE = 16;

        /// FALLOC_FL_INSERT_RANGE (0x20): Insert a range of bytes into the file
        const INSERT_RANGE = 32;

        /// FALLOC_FL_UNSHARE_RANGE (0x40): Unshare shared blocks within the range
        const UNSHARE_RANGE = 64;
    }
}

impl Default for FileAllocMode {
    fn default() -> Self {
        FileAllocMode::DEFAULT
    }
}

impl From<i32> for FileAllocMode {
    fn from(value: i32) -> Self {
        FileAllocMode::from_bits_truncate(value)
    }
}

impl From<FileAllocMode> for i32 {
    fn from(value: FileAllocMode) -> Self {
        value.bits()
    }
}

impl Serialize for FileAllocMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as i32 number
        serializer.serialize_i32(self.bits())
    }
}

impl<'de> Deserialize<'de> for FileAllocMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize from i32 number
        let bits = i32::deserialize(deserializer)?;
        Ok(FileAllocMode::from_bits_truncate(bits))
    }
}

/// File resize and space allocation options
/// truncate: True if resize is for truncate, false if resize is for allocate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileAllocOpts {
    pub truncate: bool,
    pub off: i64,
    pub len: i64,
    pub mode: FileAllocMode,
}

impl FileAllocOpts {
    pub fn with_truncate(len: i64) -> Self {
        Self {
            truncate: true,
            off: 0,
            len,
            mode: FileAllocMode::DEFAULT,
        }
    }

    pub fn with_alloc(len: i64, mode: FileAllocMode) -> Self {
        Self {
            truncate: false,
            off: 0,
            len,
            mode,
        }
    }

    pub fn validate(&self) -> FsResult<()> {
        if self.off != 0 {
            return err_box!("off must be 0, got {}", self.off);
        }

        if self.len < 0 {
            return err_box!("len must be >= 0 for allocate operation, got {}", self.len);
        }

        if !self.truncate {
            let allowed_flags =
                FileAllocMode::DEFAULT | FileAllocMode::ZERO_RANGE | FileAllocMode::KEEP_SIZE;
            let disallowed_flags = self.mode & !allowed_flags;
            if !disallowed_flags.is_empty() {
                return err_box!(
                    "mode must be DEFAULT (0) or ZERO_RANGE (16) for allocate operation, got {:?}",
                    self.mode
                );
            }
        }

        Ok(())
    }

    pub fn clone_with_len(&self, len: i64) -> Self {
        Self {
            len,
            ..self.clone()
        }
    }
}
