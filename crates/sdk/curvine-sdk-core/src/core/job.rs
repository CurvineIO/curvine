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
use curvine_proto::{GetTransferStatusResponse, SubmitTransferResponse};
use curvine_runtime::runtime::RpcRuntime;

/// Submit a load (UFS -> Curvine) transfer.
///
/// `target_path` may be `None`, in which case the Curvine counterpart of the
/// mounted UFS path is used. `overwrite` applies to existing target files.
pub fn submit_load(
    session: &Session,
    source_path: impl AsRef<str>,
    target_path: Option<&str>,
    overwrite: bool,
    replicas: Option<i32>,
) -> FsResult<SubmitTransferResponse> {
    session.runtime().block_on(async {
        transfer_compat::submit_load(
            session,
            source_path.as_ref(),
            target_path,
            overwrite,
            replicas,
        )
        .await
    })
}

/// Submit an export (Curvine -> UFS) transfer. Existing targets are overwritten.
pub fn submit_export(
    session: &Session,
    source_path: impl AsRef<str>,
) -> FsResult<SubmitTransferResponse> {
    session.runtime().block_on(async {
        transfer_compat::submit_export(session, source_path.as_ref(), true).await
    })
}

pub fn get_transfer_status(
    session: &Session,
    job_id: impl AsRef<str>,
) -> FsResult<GetTransferStatusResponse> {
    session
        .runtime()
        .block_on(async { transfer_compat::get_transfer_status(session, job_id).await })
}

pub fn cancel_transfer(session: &Session, job_id: impl AsRef<str>) -> FsResult<()> {
    session
        .runtime()
        .block_on(async { transfer_compat::cancel_transfer(session, job_id).await })
}

pub fn retry_transfer(session: &Session, job_id: impl AsRef<str>) -> FsResult<String> {
    session
        .runtime()
        .block_on(async { transfer_compat::retry_transfer(session, job_id).await })
}

pub fn wait_transfer_complete(
    session: &Session,
    job_id: impl AsRef<str>,
    fail_if_not_found: bool,
) -> FsResult<()> {
    session.runtime().block_on(async {
        transfer_compat::wait_transfer_complete(session, job_id, fail_if_not_found).await
    })
}
