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

use curvine_raft::proto::raft::{SnapshotDownloadRequest, SnapshotFileInfo};
use curvine_raft::raft::snapshot::SnapshotDownloadHandler;
use curvine_raft::raft::RaftCode;
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_runtime::common::Utils;
use std::sync::Arc;

#[test]
fn snapshot_download_rejects_file_outside_registered_snapshot() {
    let dir = std::env::temp_dir().join(format!("snapshot-unregistered-{}", Utils::rand_str(12)));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("unregistered"), b"test").unwrap();
    let request = SnapshotDownloadRequest {
        dir: dir.to_string_lossy().into_owned(),
        snapshot_file: SnapshotFileInfo {
            path: "unregistered".into(),
            len: 4,
            ..Default::default()
        },
        offset: 0,
    };
    let msg = Builder::new()
        .code(RaftCode::SnapshotDownload)
        .request(RequestStatus::Open)
        .proto_header(request)
        .build();
    let mut handler = SnapshotDownloadHandler::new(1024, Arc::new(|| None));
    let result = handler.open(&msg);
    drop(handler);
    std::fs::remove_dir_all(dir).unwrap();
    assert!(result.is_err(), "unregistered file must not be served");
}

#[test]
fn snapshot_download_rejects_file_outside_registered_dir() {
    let registered =
        std::env::temp_dir().join(format!("snapshot-registered-{}", Utils::rand_str(12)));
    std::fs::create_dir_all(&registered).unwrap();
    std::fs::write(registered.join("member"), b"ok").unwrap();
    let outside = std::env::temp_dir().join(format!("snapshot-outside-{}", Utils::rand_str(12)));
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret"), b"stolen").unwrap();

    let owned_dir = registered.clone();
    let handler_dir = Arc::new(move || Some(owned_dir.to_string_lossy().into_owned()));

    let open = |dir: &std::path::Path, file: &str| {
        let msg = Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Open)
            .proto_header(SnapshotDownloadRequest {
                dir: dir.to_string_lossy().into_owned(),
                snapshot_file: SnapshotFileInfo {
                    path: file.into(),
                    len: 4,
                    ..Default::default()
                },
                offset: 0,
            })
            .build();
        SnapshotDownloadHandler::new(1024, handler_dir.clone()).open(&msg)
    };

    let stolen = open(&outside, "secret");
    let traversal = open(&registered, "../snapshot-outside-secret/secret");
    let member = open(&registered, "member");

    std::fs::remove_dir_all(registered).unwrap();
    std::fs::remove_dir_all(outside).unwrap();

    assert!(stolen.is_err(), "another directory must not be served");
    assert!(traversal.is_err(), "path traversal must not be served");
    let rep = member.expect("a file inside the registered snapshot must be served");
    assert_ne!(rep.request_status(), RequestStatus::Undefined);
}
