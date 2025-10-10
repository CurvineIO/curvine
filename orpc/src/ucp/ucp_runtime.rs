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
use crate::sync::AtomicLen;
use crate::ucp::UcpExecutor;
use std::sync::Arc;
use bytes::BytesMut;
use crate::ucp::core::{Config, Context, Endpoint, Listener, SockAddr};
use crate::ucp::request::ConnRequest;
use crate::ucp::rma::Memory;

pub struct UcpRuntime {
    pub boss: UcpExecutor,
    pub workers: Vec<UcpExecutor>,
    pub context: Arc<Context>,
    index: AtomicLen,
}

impl UcpRuntime {
    pub fn with_conf(conf: Config) -> IOResult<Self> {
        let context = Arc::new(conf.create_context()?);

        let boss = UcpExecutor::new(format!("{}-boss", &conf.name), &context)?;

        let mut workers = vec![];
        for i in 0..conf.threads {
            let wr = UcpExecutor::new(format!("{}-worker-{}", &conf.name, i), &context)?;
            workers.push(wr);
        }

        Ok(Self {
            boss,
            workers,
            context,
            index: AtomicLen::new(0),
        })
    }

    pub fn bind(&self, addr: &SockAddr) -> IOResult<Listener> {
        Listener::bind(self.boss.clone(), addr)
    }

    pub fn worker_executor(&self) -> &UcpExecutor {
        let index = self.index.next();
        &self.workers[index % self.workers.len()]
    }

    pub fn boss_executor(&self) -> &UcpExecutor {
        &self.boss
    }

    pub fn accept(&self, conn: ConnRequest) -> IOResult<Endpoint> {
        Endpoint::accept(self.boss.clone(), conn)
    }

    pub async fn connect(&self, addr: &SockAddr) -> IOResult<Endpoint> {
        Endpoint::connect(self.worker_executor().clone(), addr)
    }

    pub fn register_memory(&self, buffer: BytesMut) -> IOResult<Memory> {
        Memory::new(self.context.clone(), buffer)
    }
}

impl Default for UcpRuntime {
    fn default() -> Self {
        Self::with_conf(Config::default()).unwrap()
    }
}
