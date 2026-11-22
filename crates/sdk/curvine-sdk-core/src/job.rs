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
use std::sync::Arc;

/// Async client for the transfer service.
///
/// Every operation maps 1:1 onto the transfer protocol; load/export only add the
/// path-resolution convenience (`target_path == None` resolves the mounted peer).
#[derive(Clone)]
pub struct JobClient {
    session: Arc<Session>,
}

impl JobClient {
    pub(crate) fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    /// Submit a load (UFS -> Curvine) transfer.
    pub async fn submit_load(
        &self,
        source_path: impl AsRef<str>,
        target_path: Option<&str>,
        overwrite: bool,
        replicas: Option<i32>,
    ) -> FsResult<SubmitTransferResponse> {
        transfer_compat::submit_load(
            &self.session,
            source_path.as_ref(),
            target_path,
            overwrite,
            replicas,
        )
        .await
    }

    /// Submit an export (Curvine -> UFS) transfer. Existing targets are overwritten.
    pub async fn submit_export(
        &self,
        source_path: impl AsRef<str>,
    ) -> FsResult<SubmitTransferResponse> {
        transfer_compat::submit_export(&self.session, source_path.as_ref(), true).await
    }

    pub async fn get_transfer_status(
        &self,
        job_id: impl AsRef<str>,
    ) -> FsResult<GetTransferStatusResponse> {
        transfer_compat::get_transfer_status(&self.session, job_id).await
    }

    pub async fn cancel_transfer(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        transfer_compat::cancel_transfer(&self.session, job_id).await
    }

    pub async fn retry_transfer(&self, job_id: impl AsRef<str>) -> FsResult<String> {
        transfer_compat::retry_transfer(&self.session, job_id).await
    }

    pub async fn wait_transfer_complete(
        &self,
        job_id: impl AsRef<str>,
        fail_if_not_found: bool,
    ) -> FsResult<()> {
        transfer_compat::wait_transfer_complete(&self.session, job_id, fail_if_not_found).await
    }
}
