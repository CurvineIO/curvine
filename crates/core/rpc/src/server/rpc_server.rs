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
use crate::server::{ServerConf, ServerMonitor, ServerStateListener};
use curvine_core_error::CommonResult;
use curvine_net::net::{InetAddr, NetUtils};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::StateCtl;
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
        self.do_shutdown_hook().await;

        self.monitor.advance_stop();

        info!("The server has stopped")
    }

    pub async fn run(&self) -> CommonResult<()> {
        let bind_addr = self.get_bind_addr();
        // Prefer a pre-bound listener from the test port reservation map.
        // This eliminates the TOCTOU race between port discovery and actual bind
        // when parallel test processes (cargo nextest) run simultaneously.
        let listener = match NetUtils::take_held_listener(self.addr.port) {
            Some(std_listener) => {
                std_listener.set_nonblocking(true)?;
                TcpListener::from_std(std_listener)?
            }
            None => TcpListener::bind(&bind_addr).await?,
        };
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
            let (stream, client_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(
                        "accept error on {}: {}. Possible system resource exhaustion; \
                        please check open file descriptors/handles limits, \
                        ephemeral port range and usage, process/thread limits, and OS socket backlog limits",
                        bind_addr,
                        e
                    );
                    continue;
                }
            };

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

    async fn do_shutdown_hook(&self) {
        let hooks = {
            let mut state = self.shutdown_hook.lock().unwrap();
            std::mem::take(&mut *state)
        };
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        // Hooks may block on this server's runtime to finish network cleanup.
        // Await their completion instead of joining on an I/O thread, which
        // deadlocks a single-thread runtime needed by those same hooks.
        thread::spawn(move || {
            for func in hooks.into_iter().rev() {
                func()
            }
            let _ = done_tx.send(());
        });
        done_rx.await.expect("server shutdown hook thread panicked");
    }

    fn get_bind_addr(&self) -> String {
        let hostname = env::var(Self::ORPC_BIND_HOSTNAME).unwrap_or(self.addr.hostname.to_string());
        format!("{}:{}", hostname, self.addr.port)
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use crate::handler::MessageHandler;
    use crate::message::Message;
    use curvine_core_error::{CommonErrorExt, CommonResultExt};
    use std::sync::mpsc;
    use std::time::Duration;

    struct Echo;

    impl MessageHandler for Echo {
        type Error = CommonErrorExt;

        fn handle(&self, message: &Message) -> CommonResultExt<Message> {
            Ok(message.success())
        }
    }

    impl HandlerService for Echo {
        type Item = Echo;

        fn get_message_handler(&self, _: Option<curvine_net::net::ConnState>) -> Self::Item {
            Echo
        }
    }

    #[test]
    fn shutdown_hooks_leave_single_io_thread_available_and_keep_lifo_order() {
        let rt = Arc::new(Runtime::single());
        let server = Arc::new(RpcServer::with_rt(
            rt.clone(),
            ServerConf::with_hostname("127.0.0.1", 0),
            Echo,
        ));
        let order = Arc::new(Mutex::new(Vec::new()));
        let first_order = order.clone();
        server.add_shutdown_hook(move || first_order.lock().unwrap().push(1));
        let second_order = order.clone();
        let hook_rt = rt.clone();
        server.add_shutdown_hook(move || {
            let (send, receive) = mpsc::channel();
            hook_rt.spawn(async move {
                // Real registration cleanup needs the same I/O runtime to
                // keep polling while the synchronous shutdown hook waits.
                tokio::task::yield_now().await;
                send.send(()).unwrap();
            });
            receive
                .recv_timeout(Duration::from_secs(2))
                .expect("shutdown blocked the only I/O thread");
            second_order.lock().unwrap().push(2);
        });

        let task = rt.spawn(async move { server.do_shutdown_hook().await });
        rt.block_on(task).unwrap();
        assert_eq!(*order.lock().unwrap(), [2, 1]);
    }
}
