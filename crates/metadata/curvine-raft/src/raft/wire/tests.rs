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

use super::*;
use std::fmt::Debug;

fn cross_version<Old, New>(old: Old)
where
    Old: prost_legacy::Message + Default + Clone + Debug + PartialEq + From<New>,
    New: prost::Message + Default + Clone + Debug + PartialEq + From<Old>,
{
    let old_bytes = prost_legacy::Message::encode_to_vec(&old);
    let decoded_new = <New as prost::Message>::decode(old_bytes.as_slice()).unwrap();
    let converted_new = New::from(old.clone());
    assert_eq!(decoded_new, converted_new);
    assert_eq!(Old::from(decoded_new), old);

    let new_bytes = prost::Message::encode_to_vec(&converted_new);
    let decoded_old = <Old as prost_legacy::Message>::decode(new_bytes.as_slice()).unwrap();
    assert_eq!(decoded_old, old);
    assert_eq!(New::from(decoded_old), converted_new);
}

fn conf_state() -> runtime::ConfState {
    runtime::ConfState {
        voters: vec![1, 2],
        learners: vec![3],
        voters_outgoing: vec![1, 4],
        learners_next: vec![4],
        auto_leave: true,
    }
}

// Golden bytes from the existing proto2 SnapshotData format, including required
// fields, negative rpc_id, optional bytes and a file snapshot. These bytes are
// also stored as the data of the raft-rs Snapshot below.
const SNAPSHOT_DATA: &[u8] = &[
    0x08, 0x0b, 0x10, 0x16, 0x18, 0x21, 0x22, 0x03, 0x01, 0x02, 0x03, 0x2a, 0x15, 0x0a, 0x05, 0x2f,
    0x73, 0x6e, 0x61, 0x70, 0x12, 0x0c, 0x0a, 0x04, 0x6d, 0x65, 0x74, 0x61, 0x10, 0x2c, 0x18, 0x37,
    0x20, 0x42, 0x32, 0x1d, 0x0a, 0x11, 0x08, 0x07, 0x10, 0x08, 0x18, 0x09, 0x20, 0xf6, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x12, 0x08, 0x08, 0x03, 0x10, 0x04, 0x18, 0x05, 0x20,
    0x06,
];

fn snapshot() -> runtime::Snapshot {
    runtime::Snapshot {
        data: SNAPSHOT_DATA.to_vec(),
        metadata: Some(runtime::SnapshotMetadata {
            conf_state: Some(conf_state()),
            index: 20,
            term: 7,
        }),
    }
}

fn message() -> runtime::Message {
    let change = runtime::ConfChangeV2 {
        transition: 2,
        changes: vec![
            runtime::ConfChangeSingle {
                change_type: 2,
                node_id: 5,
            },
            runtime::ConfChangeSingle {
                change_type: 1,
                node_id: 6,
            },
        ],
        context: vec![8, 9],
    };
    runtime::Message {
        msg_type: 7,
        to: 2,
        from: 1,
        term: 7,
        log_term: 6,
        index: 19,
        entries: vec![runtime::Entry {
            entry_type: 2,
            term: 7,
            index: 20,
            data: prost_legacy::Message::encode_to_vec(&change),
            context: vec![10, 11],
            sync_log: true,
        }],
        commit: 20,
        commit_term: 7,
        snapshot: Some(snapshot()),
        request_snapshot: 21,
        reject: true,
        reject_hint: 18,
        context: vec![12, 13],
        deprecated_priority: 23,
        priority: -42,
    }
}

#[test]
fn old_and_new_raft_messages_preserve_all_fields() {
    cross_version::<runtime::Message, wire::Message>(message());
    // Unknown enum values must not be normalized by the adapter.
    let mut unknown = message();
    unknown.msg_type = 999;
    unknown.entries[0].entry_type = 998;
    cross_version::<runtime::Message, wire::Message>(unknown);
    cross_version::<runtime::Message, wire::Message>(runtime::Message::default());
}

#[test]
fn adapters_move_entry_and_snapshot_payloads() {
    let original = message();
    let entry_data = original.entries[0].data.as_ptr();
    let snapshot_data = original.snapshot.as_ref().unwrap().data.as_ptr();

    let wire = wire::Message::from(original);
    assert_eq!(wire.entries[0].data.as_ptr(), entry_data);
    assert_eq!(wire.snapshot.as_ref().unwrap().data.as_ptr(), snapshot_data);

    let runtime = runtime::Message::from(wire);
    assert_eq!(runtime.entries[0].data.as_ptr(), entry_data);
    assert_eq!(
        runtime.snapshot.as_ref().unwrap().data.as_ptr(),
        snapshot_data
    );
}

#[test]
fn old_and_new_configuration_changes_and_storage_records_interoperate() {
    cross_version::<runtime::ConfChange, wire::ConfChange>(runtime::ConfChange {
        change_type: 2,
        node_id: 4,
        context: vec![1, 2],
        id: 3,
    });
    cross_version::<runtime::ConfChangeV2, wire::ConfChangeV2>(runtime::ConfChangeV2 {
        transition: 2,
        changes: vec![
            runtime::ConfChangeSingle {
                change_type: 0,
                node_id: 1,
            },
            runtime::ConfChangeSingle {
                change_type: 1,
                node_id: 2,
            },
        ],
        context: vec![3, 4],
    });
    cross_version::<runtime::HardState, wire::HardState>(runtime::HardState {
        term: 10,
        vote: 3,
        commit: 100,
    });
    cross_version::<runtime::ConfState, wire::ConfState>(conf_state());
    cross_version::<runtime::Entry, wire::Entry>(message().entries.remove(0));
    cross_version::<runtime::Snapshot, wire::Snapshot>(snapshot());
    cross_version::<runtime::Snapshot, wire::Snapshot>(runtime::Snapshot::default());
}

#[test]
fn snapshot_data_keeps_the_existing_persisted_format() {
    let data = <rpc::SnapshotData as prost::Message>::decode(SNAPSHOT_DATA).unwrap();
    assert_eq!(
        (data.snapshot_id, data.node_id, data.create_time),
        (11, 22, 33)
    );
    assert_eq!(data.bytes_data.as_deref(), Some(&[1, 2, 3][..]));
    let files = data.files_data.as_ref().unwrap();
    assert_eq!(files.dir, "/snap");
    assert_eq!(files.files[0].path, "meta");
    assert_eq!(
        (
            files.files[0].mtime,
            files.files[0].ctime,
            files.files[0].len
        ),
        (44, 55, 66)
    );
    assert_eq!(data.fsm_state.applied.rpc_id, -10);
    assert_eq!(data.fsm_state.ufs_applied.index, 4);
    assert_eq!(prost::Message::encode_to_vec(&data), SNAPSHOT_DATA);
}

#[test]
fn legacy_and_current_rpc_envelopes_interoperate() {
    use prost_legacy::encoding::{self, DecodeContext};

    let original = message();
    // This is the old proto2 RaftRequest encoding: a required embedded
    // raft-rs message and three optional uint64 session fields.
    let mut old_bytes = Vec::new();
    encoding::message::encode(1, &original, &mut old_bytes);
    encoding::uint64::encode(2, &0, &mut old_bytes);
    encoding::uint64::encode(3, &234, &mut old_bytes);
    encoding::uint64::encode(4, &345, &mut old_bytes);
    let decoded = <rpc::RaftRequest as prost::Message>::decode(old_bytes.as_slice()).unwrap();
    assert_eq!(
        (
            decoded.sender_session,
            decoded.receiver_session,
            decoded.leader_commit
        ),
        (Some(0), Some(234), Some(345))
    );
    assert_eq!(runtime::Message::from(decoded.message.clone()), original);

    let new_bytes = prost::Message::encode_to_vec(&decoded);
    let mut remaining = new_bytes.as_slice();
    let mut old_message = runtime::Message::default();
    let mut fields = [None; 3];
    while !remaining.is_empty() {
        let (tag, wire_type) = encoding::decode_key(&mut remaining).unwrap();
        match tag {
            1 => encoding::message::merge(
                wire_type,
                &mut old_message,
                &mut remaining,
                DecodeContext::default(),
            )
            .unwrap(),
            2..=4 => {
                let mut value = 0;
                encoding::uint64::merge(
                    wire_type,
                    &mut value,
                    &mut remaining,
                    DecodeContext::default(),
                )
                .unwrap();
                fields[(tag - 2) as usize] = Some(value);
            }
            _ => panic!("unexpected field {tag}"),
        }
    }
    assert_eq!(old_message, original);
    assert_eq!(fields, [Some(0), Some(234), Some(345)]);
}

#[test]
fn legacy_and_current_conf_change_envelopes_interoperate() {
    use prost_legacy::encoding::{self, DecodeContext};

    let original = runtime::ConfChange {
        change_type: 2,
        node_id: 42,
        context: vec![1, 3, 5],
        id: 77,
    };
    let mut old_bytes = Vec::new();
    encoding::message::encode(1, &original, &mut old_bytes);
    let decoded = <rpc::ConfChangeRequest as prost::Message>::decode(old_bytes.as_slice()).unwrap();
    assert_eq!(runtime::ConfChange::from(decoded.change.clone()), original);

    let new_bytes = prost::Message::encode_to_vec(&decoded);
    let mut remaining = new_bytes.as_slice();
    let (tag, wire_type) = encoding::decode_key(&mut remaining).unwrap();
    assert_eq!(tag, 1);
    let mut old_change = runtime::ConfChange::default();
    encoding::message::merge(
        wire_type,
        &mut old_change,
        &mut remaining,
        DecodeContext::default(),
    )
    .unwrap();
    assert!(remaining.is_empty());
    assert_eq!(old_change, original);
}
