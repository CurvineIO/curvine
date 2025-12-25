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

use bitflags::bitflags;

bitflags! {
    /// Open file flags (equivalent to Go's syscall.O_* flags)
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OpenFlags: u32 {
        // Access mode flags (must specify one)
        /// Read-only mode
        const RDONLY = 0x0000;

        /// Write-only mode
        const WRONLY = 0x0001;

        /// Read-write mode
        const RDWR = 0x0002;

        /// Access mode mask (O_ACCMODE)
        const ACCMODE = 0x0003;

        // File creation and status flags
        /// Create file if it doesn't exist
        const CREAT = 0x0040;

        /// Exclusive creation (fail if file exists)
        const EXCL = 0x0080;

        /// Truncate file to zero length
        const TRUNC = 0x0200;

        /// Append mode (write at end of file)
        const APPEND = 0x0400;

    }
}

impl OpenFlags {
    pub fn new(value: u32) -> Self {
        Self::from_bits_truncate(value)
    }
    pub fn access_mode(&self) -> OpenFlags {
        *self & OpenFlags::ACCMODE
    }

    pub fn read_only(&self) -> bool {
        self.access_mode() == OpenFlags::RDONLY
    }

    pub fn write_only(&self) -> bool {
        self.access_mode() == OpenFlags::WRONLY
    }

    pub fn read_write(&self) -> bool {
        self.access_mode() == OpenFlags::RDWR
    }

    pub fn write(&self) -> bool {
        let mode = self.access_mode();
        mode == OpenFlags::WRONLY || mode == OpenFlags::RDWR
    }

    pub fn read(&self) -> bool {
        let mode = self.access_mode();
        mode == OpenFlags::RDONLY || mode == OpenFlags::RDWR
    }

    pub fn append(&self) -> bool {
        self.contains(OpenFlags::APPEND)
    }

    pub fn create(&self) -> bool {
        self.contains(OpenFlags::CREAT)
    }

    pub fn truncate(&self) -> bool {
        self.contains(OpenFlags::TRUNC)
    }

    pub fn overwrite(&self) -> bool {
        self.truncate()
    }

    pub fn exclusive(&self) -> bool {
        self.contains(OpenFlags::EXCL)
    }

    pub fn set_read_only(self) -> Self {
        let other_flags = self.bits() & !OpenFlags::ACCMODE.bits();
        Self::from_bits_truncate(other_flags | OpenFlags::RDONLY.bits())
    }

    pub fn set_write_only(self) -> Self {
        let other_flags = self.bits() & !OpenFlags::ACCMODE.bits();
        Self::from_bits_truncate(other_flags | OpenFlags::WRONLY.bits())
    }

    pub fn set_read_write(self) -> Self {
        let other_flags = self.bits() & !OpenFlags::ACCMODE.bits();
        Self::from_bits_truncate(other_flags | OpenFlags::RDWR.bits())
    }

    pub fn set_append(mut self, enable: bool) -> Self {
        self.set(OpenFlags::APPEND, enable);
        self
    }

    pub fn set_create(mut self, enable: bool) -> Self {
        self.set(OpenFlags::CREAT, enable);
        self
    }

    pub fn set_truncate(mut self, enable: bool) -> Self {
        self.set(OpenFlags::TRUNC, enable);
        self
    }

    pub fn set_overwrite(mut self, enable: bool) -> Self {
        self.set(OpenFlags::TRUNC, enable);
        self
    }

    pub fn set_exclusive(mut self, enable: bool) -> Self {
        self.set(OpenFlags::EXCL, enable);
        self
    }

    pub fn new_read_only() -> Self {
        OpenFlags::RDONLY
    }

    pub fn new_write_only() -> Self {
        OpenFlags::WRONLY
    }

    pub fn new_read_write() -> Self {
        OpenFlags::RDWR
    }

    pub fn new_append() -> Self {
        Self::new_write_only().set_append(true)
    }

    pub fn new_create() -> Self {
        Self::new_write_only().set_create(true)
    }

    pub fn value(self) -> u32 {
        self.bits()
    }

    pub fn access_mark(&self) -> String {
        let mut mark = String::with_capacity(5);

        match self.access_mode() {
            OpenFlags::RDONLY => mark.push('R'),
            OpenFlags::WRONLY => mark.push('W'),
            OpenFlags::RDWR => mark.push_str("RW"),
            _ => mark.push('N'),
        }

        if self.create() {
            mark.push('C');
        }

        if self.append() {
            mark.push('A');
        } else if self.truncate() {
            mark.push('T');
        }

        mark
    }
}
