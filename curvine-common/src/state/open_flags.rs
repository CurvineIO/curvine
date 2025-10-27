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
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, IntoPrimitive, FromPrimitive,
)]
pub enum OpenMode {
    #[default]
    Read = 0,
    Write = 1,
    ReadWrite = 2,
    Accmode = 3,
    Append = 8,
    Create = 256,
    Overwrite = 512,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFlags(i32);

impl OpenFlags {
    pub fn new(value: i32) -> Self {
        Self(value)
    }

    pub fn with_read() -> Self {
        Self(OpenMode::Read as i32)
    }

    pub fn with_write() -> Self {
        Self(OpenMode::Write as i32)
    }

    pub fn set(mut self, mode: OpenMode) -> Self {
        self.0 |= mode as i32;
        self
    }

    pub fn write(&self) -> bool {
        let mode = OpenMode::from(self.0 & OpenMode::Write as i32);
        mode == OpenMode::Write || mode == OpenMode::ReadWrite
    }

    pub fn read(&self) -> bool {
        let mode = OpenMode::from(self.0 & OpenMode::Read as i32);
        mode == OpenMode::Read || mode == OpenMode::ReadWrite
    }

    pub fn read_write(&self) -> bool {
        let mode = OpenMode::from(self.0 & OpenMode::ReadWrite as i32);
        mode == OpenMode::ReadWrite
    }

    pub fn value(&self) -> i32 {
        self.0
    }
}
