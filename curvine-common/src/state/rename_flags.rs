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
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RenameFlags: u32 {
        /// Do not replace target if it exists (RENAME_NOREPLACE)
        const NO_REPLACE = 1 << 0;

        /// Exchange source and destination (RENAME_EXCHANGE)
        const EXCHANGE = 1 << 1;

        /// Create whiteout (not supported)
        const WHITEOUT = 1 << 2;

        /// Reserved for future use
        const _RESERVED1 = 1 << 3;

        /// Reserved for future use
        const _RESERVED2 = 1 << 4;

        /// Restore from trash (internal use)
        const RESTORE = 1 << 5;
    }
}

impl RenameFlags {
    pub fn new(value: u32) -> Self {
        RenameFlags::from_bits_truncate(value)
    }

    pub fn value(&self) -> u32 {
        self.bits()
    }

    pub fn no_replace(&self) -> bool {
        self.contains(RenameFlags::NO_REPLACE)
    }

    pub fn exchange(&self) -> bool {
        self.contains(RenameFlags::EXCHANGE)
    }

    pub fn whiteout(&self) -> bool {
        self.contains(RenameFlags::WHITEOUT)
    }

    pub fn restore(&self) -> bool {
        self.contains(RenameFlags::RESTORE)
    }

    pub fn exchange_mode(&self) -> bool {
        *self == RenameFlags::EXCHANGE
    }
}
