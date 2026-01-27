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
use crate::server::{ServerConf, ServerMonitor, ServerState, ServerStateListener};
use crate::sync::StateCtl;
use crate::CommonResult;
use log::*;
use socket2::SockRef;
use std::sync::{Arc, Mutex};
use std::{env, thread};
use tokio::net::TcpListener;

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
        let addr = InetAddr::new(conf.hostname.clone(), conf.port);
        let rt = Arc::new(conf.create_runtime());

        RpcServer {
            rt,
            service,
            conf,
            addr,
            monitor: ServerMonitor::new(),
            shutdown_hook: Mutex::new(vec![]),
        }
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
        let mut shutdown_listener = self.monitor.new_listener();
        let shutdown_requested = self.monitor.state() >= ServerState::Shutdown;

        #[cfg(target_os = "linux")]
        {
            use tokio::signal::unix::{signal, SignalKind};
            // kill -p pid will send libc::SIGTERM signal (15).
            let mut unix_sig = signal(SignalKind::terminate()).unwrap();

            if !shutdown_requested {
                tokio::select! {
                    res = self.run() => {
                        if let Err(err) = res {
                            error!("failed to accept, cause = {:?}", err);
                        }
                    }

                    _ = ctrl_c => {
                        info!("Receive ctrl_c signal, shutting down {}", self.conf.name);
                        self.monitor.advance_shutdown();
                    }

                    _ = unix_sig.recv()  => {
                        info!("Received SIGTERM, shutting down {} gracefully...", self.conf.name);
                        self.monitor.advance_shutdown();
                    }

                    _ = shutdown_listener.wait_shutdown() => {
                        info!("Receive shutdown signal, shutting down {}", self.conf.name);
                    }
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            if !shutdown_requested {
                tokio::select! {
                    res = self.run() => {
                        if let Err(err) = res {
                            error!("failed to accept, cause = {:?}", err);
                        }
                    }

                    _ = ctrl_c => {
                        info!("Receive ctrl_c signal, shutting down {}", self.conf.name);
                        self.monitor.advance_shutdown();
                    }

                    _ = shutdown_listener.wait_shutdown() => {
                        info!("Receive shutdown signal, shutting down {}", self.conf.name);
                    }
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
        let bind_addr = self.get_bind_addr();
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(
            "Rpc server [{}] start successfully, bind address: {}, hostname: {}, thread_name: {}, io threads: {}, worker threads: {}",
            self.conf.name,
            bind_addr,
            self.addr.hostname,
            self.rt.thread_name(),
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
            let mut handler = if self.conf.enable_shutdown_listener {
                let shutdown_ctl = self.monitor.read_ctl();
                let shutdown_listener = self.monitor.new_listener();
                self.service.get_stream_handler_with_shutdown(
                    self.rt.clone(),
                    frame,
                    &self.conf,
                    shutdown_ctl,
                    shutdown_listener,
                )
            } else {
                self.service
                    .get_stream_handler(self.rt.clone(), frame, &self.conf)
            };
            self.rt.spawn(async move {
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

    pub fn shutdown(&self) {
        self.monitor.advance_shutdown();
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
