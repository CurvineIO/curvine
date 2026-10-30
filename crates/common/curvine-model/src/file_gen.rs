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

//! Immutable identity of one Curvine file version.
//!
//! The asynchronous UFS export path carries a `FileGen` end to end and uses it
//! as a conditional guard: an export may only mark `ufs_mtime` when the inode
//! still carries the exact version that export read. `rename` does not change a
//! generation; `overwrite` allocates a new block id, while `append`,
//! `truncate` and `resize` change the length.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use curvine_core_error::{err_box, CommonError};

use crate::FileBlocks;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileGen {
    pub inode_id: i64,
    pub len: i64,
    pub mtime: i64,
    /// Id of the last block; `0` for an empty file.
    pub last_block_id: i64,
}

/// Object-storage user-metadata key that carries the generation written by an
/// export. Only object stores honour it (S3, OSS, COS, GCS, Azure); other UFS
/// backends have no user metadata and fall back to length + mtime.
///
/// Deliberately not prefixed with the Curvine-internal `\0`: it is a normal
/// user metadata key on the object, and it must never be mistaken for a
/// master-owned xattr.
pub const CV_GEN_METADATA_KEY: &str = "cv-gen";

impl FileGen {
    pub fn new(inode_id: i64, len: i64, mtime: i64, last_block_id: i64) -> Self {
        Self {
            inode_id,
            len,
            mtime,
            last_block_id,
        }
    }

    /// Generation of the version a block lookup returned. Status and block list
    /// come from one master response, so they describe the same version.
    pub fn of_blocks(blocks: &FileBlocks) -> Self {
        Self::new(
            blocks.status.id,
            blocks.status.len,
            blocks.status.mtime,
            blocks.block_locs.last().map(|b| b.block.id).unwrap_or(0),
        )
    }
}

impl fmt::Display for FileGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.inode_id, self.len, self.mtime, self.last_block_id
        )
    }
}

impl FromStr for FileGen {
    type Err = CommonError;

    /// Parses the wire form `{inode}:{len}:{mtime}:{block}`.
    ///
    /// Strict by design: a transfer option that cannot be parsed exactly is a
    /// bug on the submitter side, never something to repair by guessing.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = value.split(':').collect();
        if fields.len() != 4 {
            return err_box!("invalid file gen {:?}: expected 4 fields", value);
        }

        let inode_id = parse_field(value, fields[0], "inode")?;
        let len = parse_field(value, fields[1], "len")?;
        let mtime = parse_field(value, fields[2], "mtime")?;
        let last_block_id = parse_field(value, fields[3], "block")?;

        Ok(Self::new(inode_id, len, mtime, last_block_id))
    }
}

fn parse_field(whole: &str, field: &str, name: &str) -> Result<i64, CommonError> {
    field.parse::<i64>().map_err(|_| -> CommonError {
        format!("invalid file gen {:?}: {} is not i64", whole, name).into()
    })
}
