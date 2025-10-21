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
use crate::server::{ServerConf, ServerMonitor, ServerStateListener};
use crate::sync::StateCtl;
use crate::{CommonError, CommonResult};
use log::*;
use socket2::SockRef;
use std::sync::{Arc, Mutex};
use std::{env, thread};
use tokio::net::TcpListener;
use crate::io::IOResult;

pub struct RpcServer<S> {
    rt: Arc<Runtime>,
    service: S,
    conf: ServerConf,
    addr: InetAddr,
    monitor: ServerMonitor,
    shutdown_hook: Mutex<Vec<Box<dyn FnOnce() + Send + Sync + 'static>>>,
}

impl<S> RpcServer<S>
where
    S: HandlerService,
    S::Item: MessageHandler,
{
    pub const ORPC_BIND_HOSTNAME: &'static str = "ORPC_BIND_HOSTNAME";

    pub fn new(conf: ServerConf, service: S) -> Self {
        let rt = Arc::new(conf.create_runtime());
        Self::with_rt(rt, conf, service)
    }

    pub fn with_rt(rt: Arc<Runtime>, conf: ServerConf, service: S) -> Self {
        let addr = InetAddr::new(conf.hostname.clone(), conf.port);

        RpcServer {
            rt,
            service,
            conf,
            addr,
            monitor: ServerMonitor::new(),
            shutdown_hook: Mutex::new(vec![]),
        }
    }

    pub fn run_server(server: RpcServer<S>) -> ServerStateListener {
        let rt = server.rt.clone();
        let listener = server.monitor.new_listener();

        rt.spawn(async move { server.start0().await });

        listener
    }

    // Blocking start server
    pub fn block_on_start(&self) {
        self.rt.block_on(self.start0())
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

        // Perform a cleanup operation.
        self.do_shutdown_hook();

        self.monitor.advance_stop();

        info!("The server has stopped")
    }

    pub async fn run(&self) -> CommonResult<()> {
        #[cfg(feature = "ucp")] {
            if self.conf.use_ucp {
                self.run_ucp().await
            } else {
                self.run_tcp().await
            }
        }

        #[cfg(not(feature = "ucp"))] {
            if self.conf.use_ucp {
                warn!("UCP is not supported in compilation, will use TCP instead")
            }
            self.run_tcp().await
        }
    }

    pub async fn run_tcp(&self) -> CommonResult<()> {
        let bind_addr = self.get_bind_addr();
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(
            "Tcp server [{}] start successfully, bind address: {}, io threads: {}, worker threads: {}",
            self.conf.name,
            bind_addr,
            self.rt.io_threads(),
            self.rt.worker_threads()
        );
        self.monitor.advance_running();

        loop {
            let (stream, client_addr) = listener.accept().await?;

            // Set the tcp parameter through socket2.
            let sock_ref = SockRef::from(&stream);
            sock_ref.set_keepalive(true)?;
            sock_ref.set_nodelay(true)?;

            let frame = RpcFrame::with_server(stream, &self.conf);
            let bind_addr = self.bind_addr().clone();
            let mut handler = self
                .service
                .get_stream_handler(self.rt.clone(), frame, &self.conf);
            self.rt.spawn(async move {
                if let Err(e) = handler.run().await {
                    error!("Connection[{} -> {}]: {}", bind_addr, client_addr, e);
                }
            });
        }
    }

    #[cfg(feature = "ucp")]
    pub async fn run_ucp(&self) -> CommonResult<()> {
        use crate::ucp::reactor::UcpRuntime;
        use crate::ucp::core::Listener;
        use crate::ucp::core::SockAddr;
        use crate::ucp::reactor::UcpFrame;

        // Create ucp runtime
        let conf = self.conf.clone();
        let ucp_rt  = thread::spawn(move || {
            let rt = UcpRuntime::with_server(&conf)?;
            Ok::<Arc<UcpRuntime>, CommonError>(Arc::new(rt))
        }).join().unwrap().expect("Failed to create ucp runtime");

        let bind_addr = SockAddr::try_from(self.get_bind_addr().as_str())?;
        let mut listener = ucp_rt.bind(&bind_addr)?;
        info!(
            "Ucp server [{}] start successfully, bind address: {}, io threads: {}, worker threads: {}",
            self.conf.name,
            bind_addr,
            self.rt.io_threads(),
            self.rt.worker_threads()
        );
        self.monitor.advance_running();

        loop {
            let req = listener.accept().await?;
            let endpoint = ucp_rt.accept_async(req)?;
            let (bind_addr, client_addr) = endpoint.conn_sockaddr()?;

            let frame = UcpFrame::with_server(endpoint, &self.conf);
            let mut handler = self
                .service
                .get_stream_handler(self.rt.clone(), frame, &self.conf);
            self.rt.spawn(async move {
                if let Err(e) = handler.frame_mut().handshake_response().await {
                    error!("Handshake[{} -> {}]: {}", bind_addr, client_addr, e);
                    return;
                }

                if let Err(e) = handler.run().await {
                    error!("Connection[{} -> {}]: {}", bind_addr, client_addr, e);
                }
            });
        }
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.rt.clone()
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

    pub fn wait_shutdown(&self, listener: &mut ServerStateListener) -> CommonResult<()> {
        self.rt.block_on(listener.wait_shutdown())
    }

    pub fn wait_stop(&self, listener: &mut ServerStateListener) -> CommonResult<()> {
        self.rt.block_on(listener.wait_stop())
    }

    pub fn new_state_ctl(&self) -> StateCtl {
        self.monitor.read_ctl()
    }

    pub fn new_state_listener(&self) -> ServerStateListener {
        self.monitor.new_listener()
    }

    pub fn add_shutdown_hook<T: FnOnce() + Send + Sync + 'static>(&self, hook: T) {
        let mut state = self.shutdown_hook.lock().unwrap();
        state.push(Box::new(hook));
    }

    fn do_shutdown_hook(&self) {
        let mut state = self.shutdown_hook.lock().unwrap();
        let mut hooks = vec![];
        while let Some(func) = state.pop() {
            hooks.push(func);
        }
        thread::spawn(move || {
            for func in hooks {
                func()
            }
        })
        .join()
        .unwrap()
    }

    fn get_bind_addr(&self) -> String {
        let hostname = env::var(Self::ORPC_BIND_HOSTNAME).unwrap_or(self.addr.hostname.to_string());
        format!("{}:{}", hostname, self.addr.port)
    }
}
