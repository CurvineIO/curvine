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

use crate::handler::{HandlerService, MessageHandler, RpcFrame};
use crate::io::net::InetAddr;
use crate::runtime::{RpcRuntime, Runtime};
use crate::server::{RpcServer, ServerConf, ServerMonitor, ServerStateListener};
use crate::sync::StateCtl;
use crate::CommonResult;
use log::*;
use socket2::SockRef;
use std::sync::{Arc, Mutex};
use std::{env, thread};
use futures::future::err;
use tokio::net::TcpListener;
use crate::io::IOResult;
use crate::ucp::core::SockAddr;
use crate::ucp::rma::LocalMem;
use crate::ucp::reactor::{AsyncEndpoint, RmaEndpoint, UcpFrame, UcpRuntime};

pub struct UcpServer<S> {
    pub rt: Arc<UcpRuntime>,
    service: S,
    conf: ServerConf,
    addr: InetAddr,
    monitor: ServerMonitor,
    shutdown_hook: Mutex<Vec<Box<dyn FnOnce() + Send + Sync + 'static>>>,
}

impl<S> UcpServer<S>
    where
        S: HandlerService,
        S::Item: MessageHandler,
{
    pub fn new(conf: ServerConf, service: S) -> Self {
        let rt = Arc::new(UcpRuntime::default());
        Self::with_rt(rt, conf, service)
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.rt.boss_executor().clone_rt()
    }

    pub fn with_rt(rt: Arc<UcpRuntime>, conf: ServerConf, service: S) -> Self {
        let addr = InetAddr::new(conf.hostname.clone(), conf.port);

        UcpServer {
            rt,
            service,
            conf,
            addr,
            monitor: ServerMonitor::new(),
            shutdown_hook: Mutex::new(vec![]),
        }
    }

    pub fn run_server(server: UcpServer<S>) -> ServerStateListener {
        let rt = server.rt.boss_executor().clone_rt();
        let listener = server.monitor.new_listener();

        rt.spawn(async move { server.start0().await });

        listener
    }


    pub fn start(self) -> ServerStateListener {
        Self::run_server(self)
    }

    // Start server asynchronously
    async fn start0(&self) {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(target_os = "linux")]
        {
            use tokio::signal::unix::{signal, SignalKind};
            // kill -p pid will send libc::SIGTERM signal (15).
            let mut unix_sig = signal(SignalKind::terminate()).unwrap();

            tokio::select! {
                res = self.run() => {
                    if let Err(err) = res {
                        error!("failed to accept, cause = {:?}", err);
                    }
                }

                _ = ctrl_c => {
                    info!("Receive ctrl_c signal, shutting down {}", self.conf.name);
                }

                _ = unix_sig.recv()  => {
                      info!("Received SIGTERM, shutting down {} gracefully...", self.conf.name);
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            tokio::select! {
                res = self.run() => {
                    if let Err(err) = res {
                        error!("failed to accept, cause = {:?}", err);
                    }
                }

                _ = ctrl_c => {
                    info!("Receive ctrl_c signal, shutting down {}", self.conf.name);
                }
            }
        }

        self.monitor.advance_shutdown();

        self.monitor.advance_stop();

        info!("The server has stopped")
    }

    pub async fn run(&self) -> IOResult<()> {
        let bind_addr = self.get_bind_addr();
        let mut listener = self.rt.bind(&bind_addr)?;
        self.monitor.advance_running();
        info!("running");
        loop {
            let req = listener.accept().await?;

            let endpoint = self.rt.accept_async(req, self.conf.buffer_size)?;
            let rt = endpoint.clone_rt();
            let frame = UcpFrame::new(endpoint);

            let mut handler = self
                .service
                .get_stream_handler(rt.clone(), frame, &self.conf);
            rt.spawn(async move {
                handler.frame_mut().handshake_response().await.unwrap();
                if let Err(e) = handler.run().await {
                    error!("error")
                }
            });
        }
    }

    pub fn service(&self) -> &S {
        &self.service
    }

    pub fn service_mut(&mut self) -> &mut S {
        &mut self.service
    }

    pub fn bind_addr(&self) -> &InetAddr {
        &self.addr
    }

    pub fn new_state_ctl(&self) -> StateCtl {
        self.monitor.read_ctl()
    }

    pub fn new_state_listener(&self) -> ServerStateListener {
        self.monitor.new_listener()
    }

    fn get_bind_addr(&self) -> SockAddr {
        let str = format!("{}:{}", self.addr.hostname, self.addr.port);
        println!("str {}", str);
        SockAddr::from(str.as_str())
    }
}
