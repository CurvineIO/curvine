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

use curvine_proto::{
    QueryTransferTaskResponse, TransferProgressProto, TransferTaskReportRequest,
    TransferTaskStateProto, TransferTaskStatusProto,
};
use prost::Message;

// The report wire schema before T12, including all existing business fields.
#[derive(Clone, PartialEq, Message)]
struct LegacyTaskReport {
    #[prost(string, required, tag = "1")]
    job_id: String,
    #[prost(uint64, required, tag = "2")]
    run_id: u64,
    #[prost(string, required, tag = "3")]
    task_id: String,
    #[prost(uint64, required, tag = "4")]
    attempt_id: u64,
    #[prost(uint32, required, tag = "5")]
    worker_id: u32,
    #[prost(string, required, tag = "6")]
    worker_session_id: String,
    #[prost(enumeration = "TransferTaskStateProto", required, tag = "7")]
    state: i32,
    #[prost(message, required, tag = "8")]
    progress: TransferProgressProto,
    #[prost(uint32, optional, tag = "9", default = "1")]
    protocol_version: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct LegacyQueryResponse {
    #[prost(bool, required, tag = "1")]
    found: bool,
    #[prost(enumeration = "TransferTaskStateProto", required, tag = "2")]
    state: i32,
    #[prost(message, required, tag = "3")]
    progress: TransferProgressProto,
}

#[derive(Clone, PartialEq, Message)]
struct LegacyTaskStatus {
    #[prost(string, required, tag = "1")]
    task_id: String,
    #[prost(uint64, required, tag = "2")]
    attempt_id: u64,
    #[prost(string, required, tag = "3")]
    source_path: String,
    #[prost(string, required, tag = "4")]
    target_path: String,
    #[prost(uint32, required, tag = "5")]
    worker_id: u32,
    #[prost(string, required, tag = "6")]
    worker_session_id: String,
    #[prost(enumeration = "TransferTaskStateProto", required, tag = "7")]
    state: i32,
    #[prost(message, required, tag = "8")]
    progress: TransferProgressProto,
    #[prost(uint32, required, tag = "9")]
    retry_count: u32,
    #[prost(int64, required, tag = "10")]
    updated_at: i64,
}

#[test]
fn task_status_results_are_backward_compatible() {
    let legacy = LegacyTaskStatus {
        task_id: "task".into(),
        attempt_id: 2,
        state: TransferTaskStateProto::TransferTaskCompleted.into(),
        ..Default::default()
    };
    let old_wire = legacy.encode_to_vec();
    let mut status = TransferTaskStatusProto::decode(old_wire.as_slice()).unwrap();
    assert_eq!(status.encode_to_vec(), old_wire);
    assert_eq!(status.result_inline, None);
    assert_eq!(status.result_ref, None);
    assert_eq!(status.result_size, None);

    status.result_ref = Some("cv:///results/task".into());
    status.result_size = Some(65_537);
    let wire = status.encode_to_vec();
    assert_eq!(
        TransferTaskStatusProto::decode(wire.as_slice()).unwrap(),
        status
    );
    assert_eq!(LegacyTaskStatus::decode(wire.as_slice()).unwrap(), legacy);
}

#[test]
fn old_server_ignores_unknown_result_fields() {
    let legacy = LegacyTaskReport {
        job_id: "job".into(),
        run_id: 3,
        task_id: "task".into(),
        attempt_id: 4,
        worker_id: 5,
        worker_session_id: "session".into(),
        state: TransferTaskStateProto::TransferTaskCompleted.into(),
        progress: TransferProgressProto {
            loaded_size: 42,
            total_size: 42,
            update_time: 123,
            message: "done".into(),
        },
        protocol_version: Some(1),
    };
    let mut report = TransferTaskReportRequest::decode(legacy.encode_to_vec().as_slice()).unwrap();
    assert_eq!(report.result_inline, None);
    assert_eq!(report.result_ref, None);
    assert_eq!(report.result_size, None);
    assert_eq!(report.encode_to_vec(), legacy.encode_to_vec());

    report.result_inline = Some(vec![0, 255]);
    report.result_size = Some(2);
    assert_eq!(
        LegacyTaskReport::decode(report.encode_to_vec().as_slice()).unwrap(),
        legacy
    );
    report.result_inline = None;
    report.result_ref = Some("cv:///results/task".into());
    report.result_size = Some(65_537);
    assert_eq!(
        LegacyTaskReport::decode(report.encode_to_vec().as_slice()).unwrap(),
        legacy
    );
}

#[test]
fn query_result_fields_are_optional_and_roundtrip() {
    let legacy = LegacyQueryResponse {
        found: true,
        state: TransferTaskStateProto::TransferTaskCompleted.into(),
        progress: TransferProgressProto::default(),
    };
    let mut response =
        QueryTransferTaskResponse::decode(legacy.encode_to_vec().as_slice()).unwrap();
    assert_eq!(response.result_inline, None);
    assert_eq!(response.result_ref, None);
    assert_eq!(response.result_size, None);

    // Present-but-empty inline output is distinct from absent output.
    response.result_inline = Some(vec![]);
    response.result_size = Some(0);
    let wire = response.encode_to_vec();
    assert_eq!(
        QueryTransferTaskResponse::decode(wire.as_slice()).unwrap(),
        response
    );
    assert_eq!(
        LegacyQueryResponse::decode(wire.as_slice()).unwrap(),
        legacy
    );

    response.result_inline = None;
    response.result_ref = Some("cv:///results/task".into());
    response.result_size = Some(65_537);
    let wire = response.encode_to_vec();
    assert_eq!(
        QueryTransferTaskResponse::decode(wire.as_slice()).unwrap(),
        response
    );
    assert_eq!(
        LegacyQueryResponse::decode(wire.as_slice()).unwrap(),
        legacy
    );
}
