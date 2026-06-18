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

#[repr(i32)]
#[derive(Debug, FromPrimitive, IntoPrimitive, Copy, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum FuseNotifyCode {
    #[num_enum(default)]
    FUSE_NOTIFY_UNKNOWN = 0,
    FUSE_NOTIFY_POLL = 1,
    FUSE_NOTIFY_INVAL_INODE = 2,
    FUSE_NOTIFY_INVAL_ENTRY = 3,
    FUSE_NOTIFY_STORE = 4,
    FUSE_NOTIFY_RETRIEVE = 5,
    FUSE_NOTIFY_DELETE = 6,
    FUSE_NOTIFY_RESEND = 7,
    FUSE_NOTIFY_INC_EPOCH = 8,
    FUSE_NOTIFY_CODE_MAX = 9,
}

impl FuseNotifyCode {
    /// Returns a stable, low-cardinality `&'static str` name for this notify
    /// code, suitable for the `code` label on `notify_total`. Zero-allocation.
    ///
    /// The label set is the closed enum defined in the metrics design's Label
    /// rules: `inval_inode | inval_entry | delete | store | retrieve | poll |
    /// other`. Notify codes outside that set (`unknown`, `resend`, `inc_epoch`,
    /// `code_max`) collapse to `other` so the `notify_total{code}` series stays
    /// bounded to exactly the documented values. If a new code needs its own
    /// label, add it here and to the design doc's Label rules together.
    // Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE => "inval_inode",
            FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY => "inval_entry",
            FuseNotifyCode::FUSE_NOTIFY_DELETE => "delete",
            FuseNotifyCode::FUSE_NOTIFY_STORE => "store",
            FuseNotifyCode::FUSE_NOTIFY_RETRIEVE => "retrieve",
            FuseNotifyCode::FUSE_NOTIFY_POLL => "poll",
            FuseNotifyCode::FUSE_NOTIFY_UNKNOWN
            | FuseNotifyCode::FUSE_NOTIFY_RESEND
            | FuseNotifyCode::FUSE_NOTIFY_INC_EPOCH
            | FuseNotifyCode::FUSE_NOTIFY_CODE_MAX => "other",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FuseNotifyCode;

    #[test]
    fn as_str_matches_the_closed_label_set() {
        // Full (variant, expected-label) table. Any accidental relabeling must
        // update this table and the design doc's Label rules together. Codes
        // outside the documented set fold to "other".
        let table = [
            (FuseNotifyCode::FUSE_NOTIFY_UNKNOWN, "other"),
            (FuseNotifyCode::FUSE_NOTIFY_POLL, "poll"),
            (FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, "inval_inode"),
            (FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY, "inval_entry"),
            (FuseNotifyCode::FUSE_NOTIFY_STORE, "store"),
            (FuseNotifyCode::FUSE_NOTIFY_RETRIEVE, "retrieve"),
            (FuseNotifyCode::FUSE_NOTIFY_DELETE, "delete"),
            (FuseNotifyCode::FUSE_NOTIFY_RESEND, "other"),
            (FuseNotifyCode::FUSE_NOTIFY_INC_EPOCH, "other"),
            (FuseNotifyCode::FUSE_NOTIFY_CODE_MAX, "other"),
        ];
        for (code, expected) in table {
            assert_eq!(code.as_str(), expected, "label mismatch for {:?}", code);
        }
    }
}
