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

//! Wire contract of `MountOptionsProto.read_verify_ufs` (#2104).
//!
//! The field is `optional` so a mount update can leave the stored value alone.
//! A master built before that change decodes the same message with
//! `read_verify_ufs` declared `required`; the legacy decode below pins what such
//! a master sees when the flag is absent, which is why the CLI resolves an
//! omitted `--read-verify-ufs` from the stored mount instead of relying only on
//! the master-side merge.

use curvine_proto::{MountOptionsProto, WriteTypeProto};
use prost::Message;

fn update_options(read_verify_ufs: Option<bool>) -> MountOptionsProto {
    MountOptionsProto {
        update: true,
        read_verify_ufs,
        write_type: WriteTypeProto::CacheMode as i32,
        ..Default::default()
    }
}

/// The pre-#2104 shape of the same message: `read_verify_ufs` was a required
/// `bool`, so an absent field has no "keep the stored value" meaning.
#[derive(Clone, PartialEq, Message)]
struct LegacyMountOptionsProto {
    #[prost(bool, tag = "1")]
    update: bool,
    #[prost(int64, optional, tag = "3")]
    ttl_ms: Option<i64>,
    #[prost(bool, required, tag = "5")]
    read_verify_ufs: bool,
    #[prost(enumeration = "WriteTypeProto", required, tag = "11")]
    write_type: i32,
}

#[test]
fn omitted_read_verify_ufs_stays_absent_on_the_wire() {
    let bytes = update_options(None).encode_to_vec();

    let decoded = MountOptionsProto::decode(bytes.as_slice()).unwrap();
    assert!(decoded.update);
    assert_eq!(decoded.read_verify_ufs, None);
}

#[test]
fn explicit_read_verify_ufs_survives_the_wire() {
    for value in [true, false] {
        let bytes = update_options(Some(value)).encode_to_vec();
        let decoded = MountOptionsProto::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.read_verify_ufs, Some(value));
    }
}

#[test]
fn legacy_master_reads_an_omitted_flag_as_false() {
    // A `required` bool has no presence on the wire, so an older master reads
    // the absent field as `false` and its unconditional merge clears the stored
    // verification. Keep this behavior pinned: a new CLI must send the stored
    // value instead of omitting the field.
    let bytes = update_options(None).encode_to_vec();
    let legacy = LegacyMountOptionsProto::decode(bytes.as_slice()).unwrap();

    assert!(legacy.update);
    assert!(legacy.ttl_ms.is_none());
    assert!(!legacy.read_verify_ufs);

    // The value is still carried when it is set, so echoing the stored value is
    // what keeps an older master correct.
    let bytes = update_options(Some(true)).encode_to_vec();
    let legacy = LegacyMountOptionsProto::decode(bytes.as_slice()).unwrap();
    assert!(legacy.read_verify_ufs);
}
