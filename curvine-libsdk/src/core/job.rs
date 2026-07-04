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
use orpc::runtime::RpcRuntime;

pub fn submit_load(session: &Session, source: impl AsRef<str>) -> FsResult<LoadJobResult> {
    let client = JobMasterClient::new(session.fs_client());
    session
        .runtime()
        .block_on(async { client.submit_load(source).await })
}

pub fn submit_load_job(session: &Session, command: LoadJobCommand) -> FsResult<LoadJobResult> {
    let client = JobMasterClient::new(session.fs_client());
    session
        .runtime()
        .block_on(async { client.submit_load_job(command).await })
}

pub fn get_job_status(session: &Session, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
    let client = JobMasterClient::new(session.fs_client());
    session
        .runtime()
        .block_on(async { client.get_job_status(job_id).await })
}

pub fn cancel_job(session: &Session, job_id: impl AsRef<str>) -> FsResult<()> {
    let client = JobMasterClient::new(session.fs_client());
    session
        .runtime()
        .block_on(async { client.cancel_job(job_id).await })
}

pub fn wait_job_complete(
    session: &Session,
    job_id: impl AsRef<str>,
    fail_if_not_found: bool,
) -> FsResult<()> {
    let client = JobMasterClient::new(session.fs_client());
    session
        .runtime()
        .block_on(async { client.wait_job_complete(job_id, fail_if_not_found).await })
}
