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

use super::cluster_cache::ClusterSnapshot;
use super::planner::transfer_job_info;
use super::store_test::{job, task};
use super::{
    ClusterMetadataCache, MemoryTransferStore, PlannedTransfer, PlannerContext, PlannerRegistry,
    TaskDispatchContext, TransferHandler, TransferPlanner, TransferPlannerPlugin,
    TransferScheduler, TransferService, TransferStore,
};
use crate::common::UfsFactory;
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, LoadTransferDefaults};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, RpcCode};
use curvine_model::{
    decode_task_envelope_for_kind, FileStatus, MountInfo, ProtoUtils, TransferCommand,
    TransferJobRecord, TransferKind, TransferSourcePlan, TransferTaskInfo, TransferTaskReportInfo,
    TransferWorkerCapabilities, WorkerAddress, WorkerInfo,
};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::{
    GetTransferStatusRequest, GetTransferStatusResponse, SubmitTaskRequest, SubmitTaskResponse,
    SubmitTransferRequest, SubmitTransferResponse, TaskEnvelopeProto, TransferStateProto,
    TransferTaskStateProto, WorkerHeartbeatRequest,
};
use curvine_rpc::{
    handler::{HandlerService, MessageHandler},
    message::{Builder, Message, MessageBuilder},
    server::{RpcServer, ServerConf},
};
use curvine_runtime::{common::SerdeUtils, runtime::Runtime};
use prost::Message as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct StubPlanner;

#[async_trait::async_trait]
impl TransferPlannerPlugin for StubPlanner {
    fn kinds(&self) -> &'static [TransferKind] {
        &[TransferKind::Load]
    }

    fn validate_options(&self, _: &TransferCommand) -> FsResult<()> {
        Ok(())
    }

    fn validate_direction(
        &self,
        _: &TransferCommand,
        source: &Path,
        target: &Path,
    ) -> FsResult<()> {
        if source.is_cv() && target.is_cv() {
            Ok(())
        } else {
            Err(FsError::common("the stub requires two Curvine paths"))
        }
    }

    fn task_type(&self, _: TransferKind) -> TransferKind {
        TransferKind::Export
    }

    async fn plan(&self, job: &TransferJobRecord, _: &PlannerContext) -> FsResult<PlannedTransfer> {
        let mut task = task(job);
        task.task_id = "planned-by-stub".into();
        Ok(PlannedTransfer {
            tasks: vec![task],
            total_size: 123,
            skipped_files: 0,
            skipped_size: 0,
            cv_metadata_epoch: None,
        })
    }
}

fn test_planner(
    fs: CurvineFileSystem,
    cache: ClusterMetadataCache,
    factory: Arc<UfsFactory>,
    conf: &ClusterConf,
) -> TransferPlanner {
    TransferPlanner::new(
        fs,
        factory,
        cache,
        conf.client.clone(),
        10,
        1,
        LoadTransferDefaults::from_job(&conf.job),
    )
}

fn payload_test_planner() -> TransferPlanner {
    let conf = ClusterConf::default();
    let rt = Arc::new(Runtime::single());
    let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
    let cache = ClusterMetadataCache::new(fs.clone());
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt));
    test_planner(fs, cache, factory, &conf)
}

#[test]
fn golden_bytes() {
    let planner = payload_test_planner();
    let mut worker = WorkerInfo::new(
        WorkerAddress {
            worker_id: 42,
            hostname: "worker-1".into(),
            ip_addr: "127.0.0.1".into(),
            rpc_port: 8997,
            web_port: 9002,
        },
        1,
    );
    worker.worker_session_id = "worker-session".into();
    let report_endpoints = vec!["transfer-1:9010".into(), "transfer-2:9010".into()];
    let conf = ClusterConf::default();
    for kind in [TransferKind::Load, TransferKind::Export] {
        let mut job = job(kind);
        if kind == TransferKind::Export {
            std::mem::swap(&mut job.source_path, &mut job.target_path);
        }
        let mut mount = MountInfo {
            cv_path: "/target".into(),
            ufs_path: "file:///source".into(),
            replicas: Some(2),
            ..Default::default()
        };
        job.mount_snapshot_json = serde_json::to_string(&mount).unwrap();
        let mut task = task(&job);
        task.run_id = 3;
        job.run_id = task.run_id;
        task.source_path += "/child";
        task.target_path += "/child";
        let source_meta = FileStatus {
            len: 4_096,
            mtime: 1_700_000_000_000,
            ..Default::default()
        };
        task.source_read_plan_json = serde_json::to_string(&TransferSourcePlan {
            source: source_meta.clone(),
            blocks: None,
        })
        .unwrap();
        let dispatch = TaskDispatchContext {
            task: &task,
            worker: &worker,
            attempt_id: 7,
            now_ms: 1_700_000_000_123,
            report_endpoints: &report_endpoints,
        };
        let request = planner.prepare_task_payload(&job).unwrap()(&dispatch).unwrap();

        // Reference path from dispatch_one + JobWorkerClient before T16.
        LoadTransferDefaults::from_job(&conf.job).apply(&mut mount.properties);
        let mut old_payload = TransferTaskInfo {
            job: transfer_job_info(&job, &mount, &conf.client),
            task_id: task.task_id.clone(),
            worker: worker.address.clone(),
            source_path: task.source_path.clone(),
            target_path: task.target_path.clone(),
            create_time: dispatch.now_ms,
            source_read_plan_json: task.source_read_plan_json.clone(),
            source_meta,
            transfer_report: Some(TransferTaskReportInfo {
                run_id: task.run_id,
                attempt_id: dispatch.attempt_id,
                worker_id: worker.worker_id(),
                worker_session_id: worker.worker_session_id.clone(),
                report_target: report_endpoints[0].clone(),
                report_endpoints: report_endpoints.clone(),
            }),
        };
        // HashMap serialization order already varied between legacy requests.
        // Compare its contents separately and normalize only that map for the byte check.
        let mut actual_payload: TransferTaskInfo =
            SerdeUtils::deserialize(&request.task_command).unwrap();
        assert_eq!(
            actual_payload.job.mount_info.properties,
            old_payload.job.mount_info.properties
        );
        actual_payload.job.mount_info.properties.clear();
        old_payload.job.mount_info.properties.clear();
        let normalized_request = SubmitTaskRequest {
            task_type: request.task_type,
            task_command: SerdeUtils::serialize(&actual_payload).unwrap(),
        };
        // The kind now travels with the payload, so an Export task is not
        // submitted as Load.
        let old_request = SubmitTaskRequest {
            task_type: kind.into(),
            task_command: SerdeUtils::serialize(&old_payload).unwrap(),
        };
        assert_eq!(
            normalized_request.encode_to_vec(),
            old_request.encode_to_vec(),
            "{kind:?}"
        );
    }
}

#[test]
fn injected_planner_uses_task_envelope_proto() {
    let mut registry = PlannerRegistry::new();
    registry.register(StubPlanner).unwrap();
    let planner = payload_test_planner().with_planner_registry(Arc::new(registry));
    let mut job = job(TransferKind::Load);
    job.spec_version = 17;
    job.spec_blob = vec![0xaa];
    let mut task = task(&job);
    task.spec_blob = vec![0, 0xff, 1];
    let mut worker = WorkerInfo::new(Default::default(), 1);
    worker.worker_session_id = "stub-worker".into();
    let report_endpoints = vec!["transfer:9010".into()];
    let dispatch = TaskDispatchContext {
        task: &task,
        worker: &worker,
        attempt_id: 3,
        now_ms: 23,
        report_endpoints: &report_endpoints,
    };
    let request = planner.prepare_task_payload(&job).unwrap()(&dispatch).unwrap();
    assert_eq!(request.task_type, TransferKind::Export as i32);
    assert_eq!(
        decode_task_envelope_for_kind(&request.task_command, request.task_type).unwrap(),
        TaskEnvelopeProto {
            job_id: job.job_id.clone(),
            task_id: task.task_id.clone(),
            run_id: job.run_id,
            attempt_id: 3,
            worker_id: Some(0),
            worker_session_id: "stub-worker".into(),
            spec_version: 17,
            task_type: Some(request.task_type),
            spec: Some(vec![0, 0xff, 1]),
            report_endpoints: report_endpoints.clone(),
        }
    );
    let mut empty_task = task.clone();
    empty_task.spec_blob.clear();
    let empty = planner.prepare_task_payload(&job).unwrap()(&TaskDispatchContext {
        task: &empty_task,
        ..dispatch
    })
    .unwrap();
    assert_eq!(
        decode_task_envelope_for_kind(&empty.task_command, empty.task_type)
            .unwrap()
            .spec,
        Some(Vec::new())
    );
    job.spec_version = 0;
    assert!(planner.prepare_task_payload(&job).unwrap()(&dispatch).is_err());
}

#[test]
fn heartbeat_without_transfer_capabilities_is_not_eligible() {
    // Pre-transfer workers sent neither the transfer flags nor a worker session.
    let heartbeat = WorkerHeartbeatRequest::default();
    let capabilities = ProtoUtils::transfer_capabilities_from_heartbeat(&heartbeat);
    assert!(!capabilities.supports_transfer());
    assert!(capabilities.task_types.is_empty());

    let conf = ClusterConf::default();
    let rt = Arc::new(Runtime::single());
    let fs = CurvineFileSystem::with_rt(conf, rt).unwrap();
    let cache = ClusterMetadataCache::new(fs);
    let mut worker = WorkerInfo::new(Default::default(), 1);
    worker.transfer_capabilities = capabilities;
    cache.set_test_snapshot(ClusterSnapshot {
        workers: vec![worker],
        ..Default::default()
    });
    for kind in [TransferKind::Load, TransferKind::Export] {
        assert!(cache.live_workers_for_task(kind).is_err());
    }
}

#[test]
fn transfer_heartbeat_without_task_types_defaults_to_load_and_export() {
    // Transfer-capable workers advertised the base flags before task types existed.
    let heartbeat = WorkerHeartbeatRequest {
        worker_session_id: Some("pre-task-types".into()),
        transfer_task_submit: Some(true),
        transfer_report_target: Some(true),
        transfer_query_task: Some(true),
        transfer_attempt_safe_output: Some(true),
        transfer_source_read_plan: Some(true),
        ..Default::default()
    };
    let conf = ClusterConf::default();
    let rt = Arc::new(Runtime::single());
    let fs = CurvineFileSystem::with_rt(conf, rt).unwrap();
    let cache = ClusterMetadataCache::new(fs);
    let mut worker = WorkerInfo::new(Default::default(), 1);
    worker.worker_session_id = heartbeat.worker_session_id.clone().unwrap();
    worker.transfer_capabilities = ProtoUtils::transfer_capabilities_from_heartbeat(&heartbeat);
    assert_eq!(
        worker.transfer_capabilities.task_types,
        vec![TransferKind::Load, TransferKind::Export]
    );
    cache.set_test_snapshot(ClusterSnapshot {
        workers: vec![worker],
        ..Default::default()
    });
    for kind in [TransferKind::Load, TransferKind::Export] {
        assert_eq!(cache.live_workers_for_task(kind).unwrap().len(), 1);
    }
}

#[test]
fn submit_handler_uses_registered_planner() {
    let store = Arc::new(MemoryTransferStore::new());
    let conf = ClusterConf::default();
    let rt = Arc::new(Runtime::single());
    let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
    let cache = ClusterMetadataCache::new(fs.clone());
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt));
    let mut registry = PlannerRegistry::new();
    registry.register(StubPlanner).unwrap();
    let planner = test_planner(fs, cache.clone(), factory.clone(), &conf)
        .with_planner_registry(Arc::new(registry));
    let service =
        TransferService::with_cache(store.clone(), cache.clone(), Duration::from_secs(60))
            .with_planner_registry(planner.registry());
    let handler = TransferHandler::new(service);
    let mut submit = SubmitTransferRequest {
        kind: TransferKind::Load as i32,
        source_path: "/input".into(),
        target_path: "/output".into(),
        client_request_id: "stub-request".into(),
        ..Default::default()
    };
    // Neither an unknown code, an unregistered kind, nor an invalid direction may create a job.
    let default_service = TransferService::new(store.clone());
    assert!(default_service
        .submit_transfer(SubmitTransferRequest {
            kind: i32::MAX,
            ..submit.clone()
        })
        .unwrap_err()
        .to_string()
        .contains("Invalid transfer kind"));
    let unregistered =
        TransferService::new(store.clone()).with_planner_registry(Arc::new(PlannerRegistry::new()));
    assert!(unregistered
        .submit_transfer(submit.clone())
        .unwrap_err()
        .to_string()
        .contains("no transfer planner registered"));
    let submit_message = |request| {
        Builder::new()
            .code(RpcCode::SubmitTransfer)
            .proto_header(request)
            .build()
    };
    submit.source_path = "file:///input".into();
    assert!(handler
        .handle(&submit_message(submit.clone()))
        .err()
        .unwrap()
        .to_string()
        .contains("the stub requires two Curvine paths"));
    assert_eq!(store.job_count(), 0);
    submit.source_path = "/input".into();
    let accepted: SubmitTransferResponse = handler
        .handle(&submit_message(submit))
        .unwrap()
        .parse_header()
        .unwrap();
    assert_eq!(accepted.state, TransferStateProto::TransferPending as i32);
    assert_eq!(store.job_count(), 1);
}

#[derive(Clone, Default)]
struct RecordingWorker(Arc<Mutex<Vec<SubmitTaskRequest>>>);

impl HandlerService for RecordingWorker {
    type Item = Self;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        self.clone()
    }
}

impl MessageHandler for RecordingWorker {
    type Error = FsError;

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        assert_eq!(msg.code(), RpcCode::SubmitTask as i8);
        self.0.lock().unwrap().push(msg.parse_header()?);
        Ok(MessageBuilder::success_with_header(
            msg,
            SubmitTaskResponse {
                task_id: "planned-by-stub".into(),
                accepted: Some(true),
                ..Default::default()
            },
        )
        .build())
    }
}

#[tokio::test]
async fn scheduler_dispatches_only_to_workers_supporting_the_planner_task_type() {
    let rt = Arc::new(Runtime::single());
    let port = NetUtils::hold_available_port();
    let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
    server_conf.io_threads = 1;
    server_conf.worker_threads = 1;
    let recording_worker = RecordingWorker::default();
    let mut listener =
        RpcServer::with_rt(rt.clone(), server_conf, recording_worker.clone()).start();
    listener.wait_startup().await.unwrap();

    let mut conf = ClusterConf::default();
    conf.transfer.lease_timeout = Duration::from_secs(60);
    let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt));
    let address = WorkerAddress {
        worker_id: 7,
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        rpc_port: port.into(),
        web_port: 0,
    };
    let mut worker = WorkerInfo::new(address, 1);
    worker.worker_session_id = "recording-worker".into();

    // The job is Load, but StubPlanner dispatches Export. Only the worker's
    // advertised task type changes between the negative and positive cases.
    for advertised in [TransferKind::Load, TransferKind::Export] {
        let store = Arc::new(MemoryTransferStore::new());
        let cache = ClusterMetadataCache::new(fs.clone());
        worker.transfer_capabilities =
            TransferWorkerCapabilities::with_task_types(vec![advertised]);
        cache.set_test_snapshot(ClusterSnapshot {
            workers: vec![worker.clone()],
            ..Default::default()
        });
        let mut registry = PlannerRegistry::new();
        registry.register(StubPlanner).unwrap();
        let planner = test_planner(fs.clone(), cache.clone(), factory.clone(), &conf)
            .with_planner_registry(Arc::new(registry));
        // Seed a versioned spec for the stub's task envelope; submit_transfer
        // currently creates legacy rows with spec_version = 0.
        let mut job = job(TransferKind::Load);
        job.spec_version = 1;
        let job = store.create_or_get_by_request_id(job).unwrap();
        let handler = TransferHandler::new(TransferService::new(store.clone()));
        let scheduler = TransferScheduler::new(
            store,
            planner,
            cache,
            factory.clone(),
            "test-scheduler".into(),
            vec![],
            conf.transfer.clone(),
        );
        scheduler.run_once().await.unwrap();

        let request = Builder::new()
            .code(RpcCode::GetTransferStatus)
            .proto_header(GetTransferStatusRequest {
                job_id: job.job_id.clone(),
                page_size: Some(1),
                page_token: None,
                include_tasks: None,
            })
            .build();
        let status: GetTransferStatusResponse =
            handler.handle(&request).unwrap().parse_header().unwrap();
        assert_eq!(status.kind, Some(TransferKind::Load as i32));
        assert_eq!(status.tasks.len(), 1, "{advertised:?}: {status:?}");
        let task = &status.tasks[0];
        assert_eq!(task.task_id, "planned-by-stub");
        let received = recording_worker.0.lock().unwrap();
        if advertised == TransferKind::Load {
            assert!(
                received.is_empty(),
                "Load-only worker received an Export RPC"
            );
            assert_eq!(status.state, TransferStateProto::TransferDispatching as i32);
            assert_eq!(
                task.state,
                TransferTaskStateProto::TransferTaskPending as i32
            );
            assert_eq!(task.attempt_id, 0);
        } else {
            assert_eq!(received.len(), 1);
            assert_eq!(received[0].task_type, TransferKind::Export as i32);
            let envelope =
                decode_task_envelope_for_kind(&received[0].task_command, received[0].task_type)
                    .unwrap();
            assert_eq!(envelope.job_id, job.job_id);
            assert_eq!(envelope.task_id, task.task_id);
            assert_eq!(envelope.attempt_id, 1);
            assert_eq!(status.state, TransferStateProto::TransferRunning as i32);
            assert_eq!(
                task.state,
                TransferTaskStateProto::TransferTaskRunning as i32
            );
            assert_eq!(task.attempt_id, 1);
        }
    }
}
