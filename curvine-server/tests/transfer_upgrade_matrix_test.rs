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

use curvine_model::{TransferJobRecord, TransferKind, TransferProgress, TransferState};
use curvine_proto::{
    ComponentInfoProto, HeartbeatStatusProto, TransferProgressProto, TransferTaskReportRequest,
    TransferTaskStateProto, WorkerAddressProto, WorkerHeartbeatRequest,
};
use curvine_server::transfer::{SqliteTransferStore, TransferStore};
use prost::Message;
use rusqlite::Connection;

// Transfer-capable heartbeat projection before task-type and result fields.
#[derive(Clone, PartialEq, Message)]
struct LegacyWorkerHeartbeat {
    #[prost(string, required, tag = "1")]
    cluster_id: String,
    #[prost(uint32, required, tag = "2")]
    worker_id: u32,
    #[prost(int64, required, tag = "3")]
    fs_ctime: i64,
    #[prost(message, required, tag = "4")]
    address: WorkerAddressProto,
    #[prost(int32, required, tag = "5")]
    failed_dirs: i32,
    #[prost(enumeration = "HeartbeatStatusProto", required, tag = "6")]
    status: i32,
    #[prost(string, required, tag = "7")]
    software_version: String,
    #[prost(string, optional, tag = "11", default = "")]
    worker_session_id: Option<String>,
    #[prost(bool, optional, tag = "12", default = "false")]
    transfer_task_submit: Option<bool>,
    #[prost(bool, optional, tag = "13", default = "false")]
    transfer_report_target: Option<bool>,
    #[prost(bool, optional, tag = "14", default = "false")]
    transfer_query_task: Option<bool>,
    #[prost(bool, optional, tag = "15", default = "false")]
    transfer_attempt_safe_output: Option<bool>,
    #[prost(bool, optional, tag = "16", default = "false")]
    transfer_source_read_plan: Option<bool>,
}

// Transfer task report projection before result fields.
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

#[test]
fn newer_transfer_fields_preserve_legacy_wire_fields() {
    // These are projections of older schemas, not old handlers. The decoder
    // accepting these messages says nothing about scheduling or result retention.
    let heartbeat = WorkerHeartbeatRequest {
        cluster_id: "curvine".into(),
        worker_id: 7,
        fs_ctime: 123,
        address: WorkerAddressProto {
            worker_id: 7,
            hostname: "worker".into(),
            ip_addr: "127.0.0.1".into(),
            rpc_port: 8997,
            control_rpc_port: Some(9005),
            web_port: 9002,
        },
        failed_dirs: 0,
        status: HeartbeatStatusProto::Running as i32,
        software_version: "new-worker".into(),
        worker_session_id: Some("session".into()),
        transfer_task_submit: Some(true),
        transfer_report_target: Some(true),
        transfer_query_task: Some(true),
        transfer_attempt_safe_output: Some(true),
        transfer_source_read_plan: Some(true),
        transfer_task_types: vec![TransferKind::Load as i32, TransferKind::Export as i32],
        transfer_task_types_explicit: Some(true),
        transfer_task_result: Some(true),
        component_info: Some(ComponentInfoProto {
            component: Some("worker".into()),
            release_version: Some("new".into()),
            protocol_version: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    };
    let legacy = LegacyWorkerHeartbeat::decode(heartbeat.encode_to_vec().as_slice()).unwrap();
    assert_eq!(legacy.worker_id, 7);
    assert_eq!(legacy.software_version, "new-worker");
    assert_eq!(legacy.address.rpc_port, 8997);
    assert_eq!(legacy.worker_session_id.as_deref(), Some("session"));
    assert_eq!(legacy.transfer_task_submit, Some(true));
    assert_eq!(legacy.transfer_report_target, Some(true));
    assert_eq!(legacy.transfer_query_task, Some(true));
    assert_eq!(legacy.transfer_attempt_safe_output, Some(true));
    assert_eq!(legacy.transfer_source_read_plan, Some(true));

    let report = TransferTaskReportRequest {
        job_id: "job".into(),
        run_id: 2,
        task_id: "task".into(),
        attempt_id: 3,
        worker_id: 7,
        worker_session_id: "session".into(),
        state: TransferTaskStateProto::TransferTaskCompleted as i32,
        progress: TransferProgressProto {
            loaded_size: 10,
            total_size: 10,
            update_time: 99,
            message: "done".into(),
            ..Default::default()
        },
        protocol_version: Some(1),
        result_inline: Some(vec![1, 2, 3]),
        result_ref: None,
        result_size: Some(3),
    };
    let legacy = LegacyTaskReport::decode(report.encode_to_vec().as_slice()).unwrap();
    assert_eq!(legacy.job_id, "job");
    assert_eq!(
        legacy.state,
        TransferTaskStateProto::TransferTaskCompleted as i32
    );
    assert_eq!(legacy.progress.loaded_size, 10);
    assert_eq!(legacy.protocol_version, Some(1));

    let by_reference = TransferTaskReportRequest {
        result_inline: None,
        result_ref: Some("cv:///result".into()),
        ..report
    };
    assert_eq!(
        LegacyTaskReport::decode(by_reference.encode_to_vec().as_slice()).unwrap(),
        legacy
    );
}

fn transfer_job(id: &str, kind: TransferKind) -> TransferJobRecord {
    TransferJobRecord {
        job_id: format!("job-{id}"),
        job_key: format!("key-{id}"),
        run_id: 1,
        kind,
        source_path: if kind == TransferKind::Load {
            format!("file:///source-{id}")
        } else {
            format!("/source-{id}")
        },
        target_path: if kind == TransferKind::Load {
            format!("/target-{id}")
        } else {
            format!("file:///target-{id}")
        },
        command_json: "{}".into(),
        mount_snapshot_json: "{}".into(),
        secret_ref_json: "{}".into(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{id}"),
        submitter: "test".into(),
        tenant: "default".into(),
        created_at: 1,
        updated_at: 1,
        spec_version: 4,
        spec_blob: vec![0xaa],
    }
}

#[test]
fn legacy_sql_projection_reads_v3_load_and_export_rows() {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-upgrade-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&path).unwrap();
    store
        .create_or_get_by_request_id_checked(transfer_job("load", TransferKind::Load))
        .unwrap();
    store
        .create_or_get_by_request_id_checked(transfer_job("export", TransferKind::Export))
        .unwrap();
    drop(store);

    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row(
            "select version from transfer_schema_version where id = 1",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        3
    );

    // This is the projection used by a pre-v3 reader. New opaque columns are
    // deliberately not selected, so legacy Load/Export rows remain readable.
    let mut stmt = conn
        .prepare(
            "select job_id, kind, source_path, target_path, state, client_request_id
             from transfer_jobs order by job_id",
        )
        .unwrap();
    let rows: Vec<(String, i32, String, String, i32, String)> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "job-export");
    assert_eq!(rows[0].1, TransferKind::Export as i32);
    assert_eq!(rows[1].0, "job-load");
    assert_eq!(rows[1].1, TransferKind::Load as i32);
    assert!(rows
        .iter()
        .all(|row| row.4 == TransferState::Pending as i32));

    let _ = std::fs::remove_file(path);
}
