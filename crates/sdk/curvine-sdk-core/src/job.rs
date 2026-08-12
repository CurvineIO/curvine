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

use crate::core::transfer_compat;
use crate::core::Session;
use curvine_error::FsResult;
use curvine_model::{JobStatus, LoadJobCommand, LoadJobResult};
use std::sync::Arc;

#[derive(Clone)]
pub struct JobClient {
    session: Arc<Session>,
}

impl JobClient {
    pub(crate) fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    pub async fn submit_load(&self, source: impl AsRef<str>) -> FsResult<LoadJobResult> {
        self.submit_load_job(LoadJobCommand::builder(source.as_ref()).build())
            .await
    }

    pub async fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        transfer_compat::submit_load_job(&self.session, command).await
    }

    pub async fn get_status(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        transfer_compat::get_job_status(&self.session, job_id).await
    }

    pub async fn cancel(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        transfer_compat::cancel_job(&self.session, job_id).await
    }

    pub async fn wait_complete(
        &self,
        job_id: impl AsRef<str>,
        fail_if_not_found: bool,
    ) -> FsResult<()> {
        transfer_compat::wait_job_complete(&self.session, job_id, fail_if_not_found).await
    }
}
