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

use crate::io::net::InetAddr;
use crate::io::IOResult;
use crate::sync::AtomicLen;
use crate::ucp::core::{Config, Context, Listener, SockAddr};
use crate::ucp::reactor::{AsyncEndpoint, RmaEndpoint, UcpExecutor};
use crate::ucp::request::ConnRequest;
use std::sync::Arc;
use crate::client::ClientConf;
use crate::server::ServerConf;

pub struct UcpRuntime {
    pub boss: Arc<UcpExecutor>,
    pub workers: Vec<Arc<UcpExecutor>>,
    pub context: Arc<Context>,
    index: AtomicLen,
}

impl UcpRuntime {
    pub fn new(context: Arc<Context>) -> IOResult<Self> {
        let boss = Arc::new(UcpExecutor::new(
            format!("{}-boss", &context.config().name),
            &context,
        )?);

        let mut workers = vec![];
        for i in 0..context.config().threads {
            let name = format!("{}-worker-{}", &context.config().name, i);
            let worker = UcpExecutor::new(name, &context)?;
            workers.push(Arc::new(worker));
        }

        Ok(Self {
            boss,
            workers,
            context,
            index: AtomicLen::new(0),
        })
    }

    pub fn with_server(conf: &ServerConf) -> IOResult<Self> {
        let conf = Config::new(&conf.name, conf.io_threads)?;
        let context = Arc::new(Context::with_config(conf)?);
        Self::new(context)
    }

    pub fn with_client(conf: &ClientConf) -> IOResult<Self> {
        let conf = Config::new("orpc-ucp", conf.io_threads)?;
        let context = Arc::new(Context::with_config(conf)?);
        Self::new(context)
    }

    pub fn bind(&self, addr: &SockAddr) -> IOResult<Listener> {
        Listener::bind(self.boss.worker().clone(), addr)
    }

    pub fn select_executor(&self) -> Arc<UcpExecutor> {
        let index = self.index.next();
        self.workers[index % self.workers.len()].clone()
    }

    pub fn boss_executor(&self) -> &UcpExecutor {
        &self.boss
    }

    pub fn accept_rma(&self, conn: ConnRequest) -> IOResult<RmaEndpoint> {
        let executor = self.select_executor();
        let endpoint = RmaEndpoint::accept(executor, conn)?;
        Ok(endpoint)
    }

    pub fn accept_async(&self, conn: ConnRequest) -> IOResult<AsyncEndpoint> {
        let ep = self.accept_rma(conn)?;
        Ok(AsyncEndpoint::new(ep))
    }

    pub fn connect_rma(&self, addr: &InetAddr) -> IOResult<RmaEndpoint> {
        let sockaddr = SockAddr::try_from(addr)?;
        let executor = self.select_executor();
        let endpoint = RmaEndpoint::connect(executor, &sockaddr)?;
        Ok(endpoint)
    }

    pub fn connect_async(&self, addr: &InetAddr) -> IOResult<AsyncEndpoint> {
        let ep = self.connect_rma(addr)?;
        Ok(AsyncEndpoint::new(ep))
    }
}
