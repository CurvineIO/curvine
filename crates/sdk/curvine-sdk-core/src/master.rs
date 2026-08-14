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
use curvine_error::FsResult;
use curvine_model::FilesystemInfo;
use std::sync::Arc;

#[derive(Clone)]
pub struct MasterClient {
    session: Arc<Session>,
}

impl MasterClient {
    pub(crate) fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    pub async fn get_filesystem_info(&self) -> FsResult<FilesystemInfo> {
        self.session.unified().get_filesystem_info().await
    }

    /// Returns total and available capacity in a single RPC.
    pub async fn capacity_stats(&self) -> FsResult<(i64, i64)> {
        let info = self.get_filesystem_info().await?;
        Ok((info.capacity, info.available))
    }

    /// Returns total cluster capacity. Issues its own RPC; prefer [`Self::capacity_stats`]
    /// or [`Self::get_filesystem_info`] when multiple fields are needed.
    pub async fn capacity(&self) -> FsResult<i64> {
        Ok(self.get_filesystem_info().await?.capacity)
    }

    /// Returns available cluster capacity. Issues its own RPC; prefer [`Self::capacity_stats`]
    /// or [`Self::get_filesystem_info`] when multiple fields are needed.
    pub async fn available(&self) -> FsResult<i64> {
        Ok(self.get_filesystem_info().await?.available)
    }
}
