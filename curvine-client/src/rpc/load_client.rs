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

use crate::file::FsClient;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelLoadRequest, CancelLoadResponse, GetLoadStatusRequest, GetLoadStatusResponse,
    LoadJobRequest, LoadJobResponse, MountPointInfo,
};
use orpc::CommonResult;
use std::sync::Arc;

/// Master RPC client
pub struct LoadClient {
    // The underlying RPC client
    rpc_client: Arc<FsClient>,
}

impl LoadClient {
    pub fn new(rpc_client: Arc<FsClient>) -> CommonResult<Self> {
        Ok(Self {
            rpc_client: rpc_client.clone(),
        })
    }

    // Submit loading task
    pub async fn submit_load(
        &self,
        path: &str,
        ttl: Option<String>,
        recursive: Option<bool>,
    ) -> CommonResult<LoadJobResponse> {
        // Create a request
        let req = LoadJobRequest {
            path: path.to_string(),
            ttl,
            recursive,
        };
        let rep: LoadJobResponse = self.rpc_client.rpc(RpcCode::SubmitLoadJob, req).await?;
        Ok(rep)
    }

    /// Get loading task status according to the path
    pub async fn get_load_status(&self, job_id: &str) -> CommonResult<GetLoadStatusResponse> {
        // Create a request
        let req = GetLoadStatusRequest {
            job_id: job_id.to_string(),
            verbose: Option::from(false),
        };

        // Send a request
        let rep: GetLoadStatusResponse = self.rpc_client.rpc(RpcCode::GetLoadStatus, req).await?;

        Ok(rep)
    }

    /// Cancel the loading task
    pub async fn cancel_load(&self, job_id: &str) -> CommonResult<CancelLoadResponse> {
        // Create a request
        let req = CancelLoadRequest {
            job_id: job_id.to_string(),
        };

        let rep: CancelLoadResponse = self.rpc_client.rpc(RpcCode::CancelLoadJob, req).await?;

        Ok(rep)
    }
    pub async fn get_mount_point(&self, path: &str) -> CommonResult<Option<MountPointInfo>> {
        let rep = self.rpc_client.get_mount_point(path).await?;
        Ok(rep)
    }
}
