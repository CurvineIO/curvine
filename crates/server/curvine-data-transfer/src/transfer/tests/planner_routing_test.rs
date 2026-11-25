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
use super::store_test::task;
use super::{
    ClusterMetadataCache, MemoryTransferStore, PlannedTransfer, PlannerContext, PlannerRegistry,
    TransferHandler, TransferPlanner, TransferPlannerPlugin, TransferScheduler, TransferService,
};
use crate::common::UfsFactory;
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, LoadTransferDefaults};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, RpcCode};
use curvine_model::{
    TransferCommand, TransferJobInfo, TransferJobRecord, TransferKind, TransferWorkerCapabilities,
    WorkerInfo,
};
use curvine_proto::{
    GetTransferStatusRequest, GetTransferStatusResponse, SubmitTransferRequest,
    SubmitTransferResponse, TransferStateProto, TransferTaskStateProto,
};
use curvine_rpc::{handler::MessageHandler, message::Builder};
use curvine_runtime::runtime::Runtime;
use std::sync::Arc;
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

    async fn plan(
        &self,
        job: &TransferJobRecord,
        context: &PlannerContext,
    ) -> FsResult<PlannedTransfer> {
        let mut task = task(job);
        task.task_id = "planned-by-stub".into();
        Ok(PlannedTransfer {
            job_info: self.transfer_job_info(job, context)?,
            tasks: vec![task],
            total_size: 123,
            skipped_files: 0,
            skipped_size: 0,
            cv_metadata_epoch: None,
        })
    }

    fn transfer_job_info(
        &self,
        job: &TransferJobRecord,
        _: &PlannerContext,
    ) -> FsResult<TransferJobInfo> {
        Ok(transfer_job_info(
            job,
            &Default::default(),
            &Default::default(),
        ))
    }
}

#[tokio::test]
async fn injected_planner_submits_plans_and_returns_status() {
    let store = Arc::new(MemoryTransferStore::new());
    let mut conf = ClusterConf::default();
    conf.transfer.lease_timeout = Duration::from_secs(60);
    let rt = Arc::new(Runtime::single());
    let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
    let cache = ClusterMetadataCache::new(fs.clone());
    let mut worker = WorkerInfo::new(Default::default(), 1);
    worker.worker_session_id = "load-only".into();
    worker.transfer_capabilities = TransferWorkerCapabilities {
        task_submit: true,
        report_target: true,
        query_task: true,
        attempt_safe_output: true,
        source_read_plan: true,
        task_types: vec![TransferKind::Load],
        ..Default::default()
    };
    cache.set_test_snapshot(ClusterSnapshot {
        workers: vec![worker],
        ..Default::default()
    });
    assert_eq!(
        cache
            .live_workers_for_task(TransferKind::Load)
            .unwrap()
            .len(),
        1
    );
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt));
    let mut registry = PlannerRegistry::new();
    registry.register(StubPlanner).unwrap();
    let planner = TransferPlanner::new(
        fs,
        factory.clone(),
        cache.clone(),
        conf.client,
        10,
        1,
        LoadTransferDefaults::from_job(&conf.job),
    )
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

    let scheduler = TransferScheduler::new(
        store,
        planner,
        cache,
        factory,
        "test-scheduler".into(),
        vec![],
        conf.transfer,
    );
    scheduler.run_once().await.unwrap();

    let request = Builder::new()
        .code(RpcCode::GetTransferStatus)
        .proto_header(GetTransferStatusRequest {
            job_id: accepted.job_id,
            page_size: Some(1),
            page_token: None,
        })
        .build();
    let status: GetTransferStatusResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();
    assert_eq!(status.kind, Some(TransferKind::Load as i32));
    assert_eq!(status.state, TransferStateProto::TransferDispatching as i32);
    assert_eq!(status.tasks.len(), 1);
    assert_eq!(status.tasks[0].task_id, "planned-by-stub");
    assert_eq!(
        status.tasks[0].state,
        TransferTaskStateProto::TransferTaskPending as i32
    );
    assert_eq!(status.tasks[0].attempt_id, 0);
    assert!(!status.tasks[0].progress.message.contains("No live worker"));
}
