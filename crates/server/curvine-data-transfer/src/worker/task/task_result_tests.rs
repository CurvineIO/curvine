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

use super::TaskManager;
use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_model::{
    JobTaskProgress, JobTaskState, LoadJobInfo, LoadTaskInfo, TransferTaskReportInfo,
    TransferTaskResult,
};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::{
    TransferTaskReportRequest, TransferTaskReportResponse, TransferTaskStateProto,
};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Default)]
struct ReportRecorder(Arc<Mutex<Vec<TransferTaskReportRequest>>>);

impl HandlerService for ReportRecorder {
    type Item = Self;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self {
        self.clone()
    }
}

impl MessageHandler for ReportRecorder {
    type Error = FsError;

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        assert_eq!(RpcCode::from(msg.code()), RpcCode::ReportTransferTask);
        let report: TransferTaskReportRequest = msg.parse_header()?;
        // Simulate a rejected report to exercise the worker query fallback.
        let accepted = report.result_ref.is_none();
        self.0.lock().unwrap().push(report);
        Ok(Builder::success(msg)
            .proto_header(TransferTaskReportResponse { accepted })
            .build())
    }
}

pub(super) fn sample_task() -> LoadTaskInfo {
    LoadTaskInfo {
        job: LoadJobInfo {
            job_id: "job".into(),
            source_path: "source".into(),
            target_path: "target".into(),
            block_size: 4096,
            replicas: 1,
            storage_type: Default::default(),
            ttl_ms: 0,
            ttl_action: Default::default(),
            mount_info: Default::default(),
            create_time: 0,
            overwrite: None,
        },
        task_id: "task".into(),
        worker: Default::default(),
        source_path: "source".into(),
        target_path: "target".into(),
        create_time: 0,
        source_read_plan_json: String::new(),
        transfer_report: Some(TransferTaskReportInfo {
            run_id: 1,
            attempt_id: 2,
            worker_id: 7,
            worker_session_id: "session".into(),
            report_target: String::new(),
            report_endpoints: Vec::new(),
        }),
    }
}

#[test]
fn report_with_and_without_result_roundtrip() {
    let rt = Arc::new(Runtime::single());
    let port = NetUtils::hold_available_port();
    let recorder = ReportRecorder::default();
    let server = RpcServer::with_rt(
        rt.clone(),
        ServerConf::with_hostname("127.0.0.1", port),
        recorder.clone(),
    );
    let mut listener = server.new_state_listener();
    let server_task = rt.spawn(async move { server.run().await });
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), listener.wait_startup())
            .await
            .unwrap()
            .unwrap();
        let mut conf = ClusterConf::default();
        conf.transfer.endpoints = vec![format!("127.0.0.1:{port}")];
        conf.job.worker_max_inline_result_bytes = 2;
        let manager = TaskManager::with_rt(rt.clone(), &conf, "session").unwrap();
        let task = manager.tasks.insert(sample_task());
        let reporter = manager.progress_reporter(task.clone());
        let query =
            |attempt, session| manager.query_transfer_task("job", "task", 1, attempt, session);

        reporter.report(task.progress(), None).await.unwrap();
        assert_eq!(query(2, "session").unwrap().1, None);

        let inline = TransferTaskResult::Inline(vec![0, 255]);
        let completed = JobTaskProgress {
            state: JobTaskState::Completed,
            ..task.progress()
        };
        reporter
            .report(completed.clone(), Some(inline.clone()))
            .await
            .unwrap();
        let (progress, result) = query(2, "session").unwrap();
        assert_eq!(progress.state, JobTaskState::Completed);
        assert_eq!(result, Some(inline));

        let reference = TransferTaskResult::Reference {
            reference: "cv:///results/task".into(),
            size: u64::MAX,
        };
        let err = reporter
            .report(completed, Some(reference.clone()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("report rejected"));
        assert_eq!(query(2, "session").unwrap().1, Some(reference));
        assert!(query(1, "session").is_none());
        assert!(query(2, "old-session").is_none());
        let mut next_attempt = sample_task();
        next_attempt.transfer_report.as_mut().unwrap().attempt_id = 3;
        manager.tasks.insert(next_attempt);
        assert!(query(2, "session").is_none());
        assert_eq!(query(3, "session").unwrap().1, None);
    });
    server_task.abort();
    let _ = rt.block_on(server_task);

    let reports = recorder.0.lock().unwrap();
    assert_eq!(reports.len(), 3);
    assert_eq!(reports[0].result_inline, None);
    assert_eq!(reports[0].result_ref, None);
    assert_eq!(reports[0].result_size, None);
    assert_eq!(reports[1].job_id, "job");
    assert_eq!(reports[1].run_id, 1);
    assert_eq!(reports[1].attempt_id, 2);
    assert_eq!(
        reports[1].state,
        TransferTaskStateProto::TransferTaskCompleted as i32
    );
    assert_eq!(
        reports[1].result_inline.as_deref(),
        Some([0, 255].as_slice())
    );
    assert_eq!(reports[1].result_size, Some(2));
    assert_eq!(reports[2].result_inline, None);
    assert_eq!(reports[2].result_ref.as_deref(), Some("cv:///results/task"));
    assert_eq!(reports[2].result_size, Some(u64::MAX));
}

#[tokio::test]
async fn oversize_inline_result_is_rejected_on_worker() {
    let mut conf = ClusterConf::default();
    conf.job.worker_max_inline_result_bytes = 2;
    let manager = TaskManager::with_rt(Arc::new(Runtime::single()), &conf, "session").unwrap();
    let task = manager.tasks.insert(sample_task());
    let reporter = manager.progress_reporter(task.clone());
    let progress = JobTaskProgress {
        state: JobTaskState::Completed,
        ..task.progress()
    };
    let err = reporter
        .report(progress, Some(TransferTaskResult::Inline(vec![0; 3])))
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("3 bytes, exceeding worker limit 2"));
    let (progress, result) = manager
        .query_transfer_task("job", "task", 1, 2, "session")
        .unwrap();
    assert_eq!(progress.state, JobTaskState::Pending);
    assert_eq!(result, None);
}
