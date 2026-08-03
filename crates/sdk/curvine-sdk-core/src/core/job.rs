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
use orpc_runtime::runtime::RpcRuntime;

pub fn submit_load(session: &Session, source: impl AsRef<str>) -> FsResult<LoadJobResult> {
    submit_load_job(session, LoadJobCommand::builder(source.as_ref()).build())
}

pub fn submit_load_job(session: &Session, command: LoadJobCommand) -> FsResult<LoadJobResult> {
    session
        .runtime()
        .block_on(async { transfer_compat::submit_load_job(session, command).await })
}

pub fn get_job_status(session: &Session, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
    session
        .runtime()
        .block_on(async { transfer_compat::get_job_status(session, job_id).await })
}

pub fn cancel_job(session: &Session, job_id: impl AsRef<str>) -> FsResult<()> {
    session
        .runtime()
        .block_on(async { transfer_compat::cancel_job(session, job_id).await })
}

pub fn wait_job_complete(
    session: &Session,
    job_id: impl AsRef<str>,
    fail_if_not_found: bool,
) -> FsResult<()> {
    session.runtime().block_on(async {
        transfer_compat::wait_job_complete(session, job_id, fail_if_not_found).await
    })
}
