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

use crate::client::RpcClient;
use crate::err_box;
use crate::io::retry::TimeBondedRetry;
use crate::io::IOResult;
use crate::message::Message;
use crate::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;
use std::time::Duration;

/// A synchronous client service that blocks access.
pub struct SyncClient {
    rt: Arc<Runtime>,
    inner: RpcClient,
}

impl SyncClient {
    pub fn new(rt: Arc<Runtime>, client: RpcClient) -> SyncClient {
        SyncClient { rt, inner: client }
    }

    pub fn rpc(&self, msg: Message) -> IOResult<Message> {
        self.rt.block_on(self.inner.rpc(msg))
    }

    pub fn rpc_check(&self, msg: Message) -> IOResult<Message> {
        let rep = self.rt.block_on(self.inner.rpc(msg))?;
        if !rep.is_success() {
            err_box!(rep.to_error_msg())
        } else {
            Ok(rep)
        }
    }

    pub fn retry_rpc(
        &self,
        dur: Duration,
        policy: TimeBondedRetry,
        msg: Message,
    ) -> IOResult<Message> {
        self.rt.block_on(self.inner.retry_rpc(dur, policy, msg))
    }
}
