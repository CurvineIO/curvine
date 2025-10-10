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

use crate::io::IOResult;
use crate::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use crate::sync::channel::CallChannel;
use log::error;
use std::sync::Arc;
use bytes::BytesMut;
use crate::ucp::core::{Context, Worker};
use crate::ucp::rma::Memory;

#[derive(Clone)]
pub struct UcpExecutor {
    worker: Arc<Worker>,
    rt: Arc<Runtime>,
}

impl UcpExecutor {
    pub fn new(name: impl AsRef<str>, context: &Arc<Context>) -> IOResult<Self> {
        let rt = Arc::new(AsyncRuntime::new(name, 1, 0));
        let context = context.clone();

        let (tx, rx) = CallChannel::channel();
        rt.spawn(async move {
            let worker = Arc::new(context.create_worker().expect("create worker"));
            tx.send(worker.clone()).expect("send ucp worker");
            if let Err(e) = worker.event_poll().await {
                error!("worker thread error: {}", e)
            }
        });

        let worker = rt.block_on(rx.receive()).expect("receive  ucp worker");

        Ok(Self { worker, rt })
    }

    pub fn rt(&self) -> &Arc<Runtime> {
        &self.rt
    }

    pub fn worker(&self) -> &Arc<Worker> {
        &self.worker
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn clone_worker(&self) -> Arc<Worker> {
        self.worker.clone()
    }

    pub fn register_memory(&self, buffer: BytesMut) -> IOResult<Memory> {
        Memory::new(self.worker.context().clone(), buffer)
    }
}
