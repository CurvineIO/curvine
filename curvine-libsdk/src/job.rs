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

use crate::core::Session;
use curvine_client::rpc::JobMasterClient;
use curvine_common::state::{JobStatus, LoadJobCommand, LoadJobResult};
use curvine_common::FsResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct JobClient {
    session: Arc<Session>,
}

impl JobClient {
    pub(crate) fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    fn job_master(&self) -> JobMasterClient {
        JobMasterClient::new(self.session.fs_client())
    }

    pub async fn submit_load(&self, source: impl AsRef<str>) -> FsResult<LoadJobResult> {
        self.job_master().submit_load(source).await
    }

    pub async fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        self.job_master().submit_load_job(command).await
    }

    pub async fn get_status(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        self.job_master().get_job_status(job_id).await
    }

    pub async fn cancel(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        self.job_master().cancel_job(job_id).await
    }

    pub async fn wait_complete(
        &self,
        job_id: impl AsRef<str>,
        fail_if_not_found: bool,
    ) -> FsResult<()> {
        self.job_master()
            .wait_job_complete(job_id, fail_if_not_found)
            .await
    }
}
