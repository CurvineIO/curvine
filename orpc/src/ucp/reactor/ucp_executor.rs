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
use crate::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, BlockingChannel, CallChannel};
use crate::ucp::core::{Context, Worker};
use crate::ucp::request::OpRequest;
use crate::ucp::rma::LocalMem;
use log::error;
use std::sync::Arc;

pub struct UcpExecutor {
    worker: Arc<Worker>,
    rt: Arc<Runtime>,
    sender: AsyncSender<OpRequest>,
}

impl UcpExecutor {
    pub fn new(name: impl AsRef<str>, context: &Arc<Context>) -> IOResult<Self> {
        let (sender, receiver) = AsyncChannel::new(0).split();
        let rt = Arc::new(AsyncRuntime::new(name, 1, 0));
        let context = context.clone();

        // 创建worker。必须使用绑定的线程创建worker。
        let (tx, mut rx) = BlockingChannel::new(1).split();
        rt.spawn(async move {
            let res = match context.create_worker() {
                Ok(worker) => Ok(Arc::new(worker)),
                Err(e) => Err(e),
            };
            tx.send(res).expect("send ucp worker");
        });
        let worker = rx.recv().unwrap().expect("get ucp worker");

        // 启动事件轮询
        Self::event_loop(receiver, worker.clone(), rt.clone());

        Ok(Self { worker, rt, sender })
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn worker(&self) -> &Arc<Worker> {
        &self.worker
    }

    pub fn register_memory(&self, len: usize) -> IOResult<LocalMem> {
        self.worker.register_memory(len)
    }

    pub fn clone_sender(&self) -> AsyncSender<OpRequest> {
        self.sender.clone()
    }

    // @todo 使用select优化
    fn event_loop(mut receiver: AsyncReceiver<OpRequest>, worker: Arc<Worker>, rt: Arc<Runtime>) {
        rt.spawn(async move {
            worker.event_poll().await.unwrap();
        });
        rt.spawn(async move {
            while let Some(request) = receiver.recv().await {
                if let Err(e) = request.run().await {
                    error!("ucp request error: {}", e);
                }
            }
        });
    }
}
