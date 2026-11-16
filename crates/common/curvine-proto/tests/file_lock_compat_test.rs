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

//! Wire contract of `FileLockProto.session_owned` (#1878).
//!
//! The flag is additive on tag 9, so the two rolling-upgrade directions must both
//! keep working: an old client omits it, and an old master must not choke on it.

use curvine_proto::{FileLockProto, LockFlagsProto, LockTypeProto};
use prost::Message;

fn lock(session_owned: Option<bool>) -> FileLockProto {
    FileLockProto {
        client_id: "client-uuid".to_string(),
        owner_id: 7,
        pid: 4242,
        acquire_time: 1_700_000_000_000,
        lock_type: LockTypeProto::Write as i32,
        lock_flags: LockFlagsProto::Plock as i32,
        start: 0,
        end: 4096,
        session_owned,
    }
}

#[test]
fn session_owned_round_trips() {
    for flag in [None, Some(false), Some(true)] {
        let decoded = FileLockProto::decode(lock(flag).encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.session_owned, flag);
    }
}

/// A client that predates the field sends exactly the proto2 fields below.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyFileLockProto {
    #[prost(string, required, tag = "1")]
    client_id: String,
    #[prost(uint64, required, tag = "2")]
    owner_id: u64,
    #[prost(uint32, required, tag = "3")]
    pid: u32,
    #[prost(uint64, required, tag = "4")]
    acquire_time: u64,
    #[prost(enumeration = "LockTypeProto", required, tag = "5")]
    lock_type: i32,
    #[prost(enumeration = "LockFlagsProto", required, tag = "6")]
    lock_flags: i32,
    #[prost(uint64, required, tag = "7")]
    start: u64,
    #[prost(uint64, required, tag = "8")]
    end: u64,
}

#[test]
fn old_client_frame_decodes_with_the_flag_unset() {
    let legacy = LegacyFileLockProto {
        client_id: "legacy-client".to_string(),
        owner_id: 7,
        pid: 4242,
        acquire_time: 1_700_000_000_000,
        lock_type: LockTypeProto::Write as i32,
        lock_flags: LockFlagsProto::Plock as i32,
        start: 0,
        end: 4096,
    };

    let decoded = FileLockProto::decode(legacy.encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded.client_id, "legacy-client");
    assert_eq!(
        decoded.session_owned, None,
        "an old client must not look like a session-owning one"
    );
}

#[test]
fn old_master_ignores_the_new_flag() {
    let decoded = LegacyFileLockProto::decode(lock(Some(true)).encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded.client_id, "client-uuid");
    assert_eq!(decoded.owner_id, 7);
    assert_eq!(decoded.end, 4096);
}
