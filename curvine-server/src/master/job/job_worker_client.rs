use std::time::Duration;

use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::{LoadTaskInfo, WorkType};
use curvine_common::utils::{RpcUtils, SerdeUtils};
use curvine_common::FsResult;
use orpc::client::RpcClient;
use prost::Message as PMessage;

#[derive(Clone)]
pub struct JobWorkerClient {
    client: RpcClient,
    timeout: Duration,
}

impl JobWorkerClient {
    pub fn new(client: RpcClient, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        RpcUtils::proto_rpc(&self.client, self.timeout, code, header).await
    }

    pub async fn submit_load_task(&self, task: LoadTaskInfo) -> FsResult<()> {
        let request = SubmitTaskRequest {
            task_type: WorkType::Load.into(),
            task_command: SerdeUtils::serialize(&task)?,
        };

        let _: SubmitTaskResponse = self.rpc(RpcCode::SubmitTask, request).await?;
        Ok(())
    }

    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let request = CancelJobRequest {
            job_id: job_id.as_ref().to_string(),
        };

        let _: CancelJobResponse = self.rpc(RpcCode::CancelJob, request).await?;
        Ok(())
    }
}
