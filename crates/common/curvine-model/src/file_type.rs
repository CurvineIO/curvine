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
    Default,
)]
pub enum FileType {
    Dir = 0,

    #[default]
    File = 1,

    Link = 2,

    Stream = 3,

    Agg = 4,

    Object = 5,

    Fifo = 6,

    Char = 7,

    Block = 8,

    Socket = 9,
}

/// Extended attribute key storing the device number (little-endian u32) for special nodes.
pub const MKNOD_RDEV_XATTR: &str = "curvine.rdev";

/// Extended attribute key storing Linux FS_IOC_* file flags (little-endian u32).
pub const IFLAGS_XATTR: &str = "curvine.i_flags";

/// Linux `FS_IMMUTABLE_FL` (see `linux/fs.h`).
pub const FS_IMMUTABLE_FL: u32 = 0x0000_0010;

/// Linux `FS_APPEND_FL` (see `linux/fs.h`).
pub const FS_APPEND_FL: u32 = 0x0000_0020;

pub fn is_special_file_type(file_type: FileType) -> bool {
    matches!(
        file_type,
        FileType::Fifo | FileType::Char | FileType::Block | FileType::Socket
    )
}
