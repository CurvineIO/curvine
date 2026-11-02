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

use crate::conf::JournalConf;
use crate::raft::snapshot::SnapshotDownloadHandler;
use crate::raft::{RaftCode, RaftError, RaftResult};
use curvine_core_error::{err_box, try_option, CommonResult};
use curvine_net::net::{ConnState, NetUtils};
use curvine_rpc::client::dispatch::Envelope;
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::Message;
use curvine_rpc::server::{ServerMonitor, ServerStateListener};
use curvine_runtime::common::DurationUnit;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastMutex;
use log::warn;
use mini_moka::sync::{Cache, CacheBuilder};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub struct RaftServer {
    rt: Arc<Runtime>,
    service: RaftService,
    hostname: String,
    port: u16,
    grpc: curvine_rpc::grpc::GrpcConfig,
}

impl RaftServer {
    pub fn with_rt(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        Self {
            rt,
            service: RaftService::new(conf),
            hostname: conf.hostname.clone(),
            port: conf.rpc_port,
            grpc: conf.grpc.clone(),
        }
    }

    pub async fn start(self) -> CommonResult<ServerStateListener> {
        let tls = self.grpc.server_tls()?;
        // Each journal is also a Raft client; reject invalid outbound TLS
        // settings before advertising the listener as ready.
        let client_tls = self.grpc.client_tls(&self.hostname)?;
        let _ = curvine_rpc::grpc::TransportConfig::default().endpoint(
            self.grpc.uri(&self.hostname, self.port),
            client_tls.as_ref(),
        )?;
        let transport = curvine_rpc::grpc::TransportConfig::default();
        let mut grpc_server = transport.server(tls.as_ref())?;
        let reflection = if self.grpc.enable_reflection {
            Some(
                curvine_rpc::grpc::reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(crate::proto::raft::FILE_DESCRIPTOR_SET)
                    .with_service_name("raft.RaftService")
                    .build()?,
            )
        } else {
            None
        };
        let listener = if let Some(listener) = NetUtils::take_held_listener(self.port) {
            listener.set_nonblocking(true)?;
            tokio::net::TcpListener::from_std(listener)?
        } else {
            tokio::net::TcpListener::bind((self.hostname.as_str(), self.port)).await?
        };
        let monitor = Arc::new(ServerMonitor::new());
        let status = monitor.new_listener();
        let state = monitor.clone();
        let service = Arc::new(self.service);
        let (mut health, health_service) = curvine_rpc::grpc::health_reporter();
        health
            .set_serving::<crate::proto::raft::raft_service_server::RaftServiceServer<
                crate::raft::grpc_server::RaftGrpcServer,
            >>()
            .await;
        monitor.advance_running();
        self.rt.spawn(async move {
            let incoming = futures::stream::unfold(
                (listener, transport),
                |(listener, transport)| async move {
                    let accepted = listener.accept().await.and_then(|(stream, _)| {
                        transport.configure_socket(&stream)?;
                        Ok(stream)
                    });
                    Some((accepted, (listener, transport)))
                },
            );
            let service = crate::proto::raft::raft_service_server::RaftServiceServer::new(
                crate::raft::grpc_server::RaftGrpcServer::new(service),
            )
            .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
            if let Err(error) = grpc_server
                    .add_optional_service(reflection)
                    .add_service(health_service)
                .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async move {
                        shutdown_signal().await;
                        health.set_not_serving::<crate::proto::raft::raft_service_server::RaftServiceServer<crate::raft::grpc_server::RaftGrpcServer>>().await;
                    })
                .await
            {
                log::error!("Raft gRPC listener stopped: {}", error);
            }
            state.advance_shutdown();
            state.advance_stop();
        });
        Ok(status)
    }

    pub fn take_receiver(&mut self) -> CommonResult<mpsc::Receiver<Envelope>> {
        self.service.take_receiver()
    }

    pub fn new_sender(&self) -> mpsc::Sender<Envelope> {
        self.service.sender.clone()
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub struct RaftService {
    sender: mpsc::Sender<Envelope>,
    receiver: Option<mpsc::Receiver<Envelope>>,
    retry_cache: Arc<Cache<i64, ()>>,
    snapshot_chunk_size: usize,
}

impl RaftService {
    pub fn new(conf: &JournalConf) -> Self {
        let ttl = DurationUnit::from_str(&conf.raft_retry_cache_ttl).unwrap();
        let cache = CacheBuilder::new(conf.raft_retry_cache_size)
            .time_to_live(Duration::from_millis(ttl.as_millis()))
            .build();

        let (sender, receiver) = mpsc::channel(conf.message_size);
        Self {
            sender,
            receiver: Some(receiver),
            retry_cache: Arc::new(cache),
            // Leave space for protobuf and zlib overhead within the gRPC
            // message limit even when a larger disk chunk is configured.
            snapshot_chunk_size: conf.snapshot_read_chunk_size.clamp(1, 16 * 1024 * 1024),
        }
    }

    pub fn take_receiver(&mut self) -> CommonResult<mpsc::Receiver<Envelope>> {
        let rx = self.receiver.take();
        Ok(try_option!(rx))
    }
}

impl HandlerService for RaftService {
    type Item = RaftHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        RaftHandler {
            sender: self.sender.clone(),
            download_handler: FastMutex::new(SnapshotDownloadHandler::new(
                self.snapshot_chunk_size,
            )),
            retry_cache: self.retry_cache.clone(),
        }
    }
}

pub struct RaftHandler {
    sender: mpsc::Sender<Envelope>,
    download_handler: FastMutex<SnapshotDownloadHandler>,
    retry_cache: Arc<Cache<i64, ()>>,
}

impl MessageHandler for RaftHandler {
    type Error = RaftError;

    fn is_sync(&self, msg: &Message) -> bool {
        let code = RaftCode::from(msg.code());
        matches!(code, RaftCode::SnapshotDownload)
    }

    fn handle(&self, msg: &Message) -> RaftResult<Message> {
        let code = RaftCode::from(msg.code());
        match code {
            RaftCode::SnapshotDownload => self.download_handler.lock().handle(msg),

            _ => err_box!("Unsupported request type: {:?}", code),
        }
    }

    async fn async_handle(&self, msg: Message) -> Result<Message, Self::Error> {
        // Check for duplicate requests
        if RaftCode::from(msg.code()) == RaftCode::Propose {
            match self.retry_cache.get(&msg.req_id()) {
                None => self.retry_cache.insert(msg.req_id(), ()),

                Some(_) => {
                    warn!("Retry propose log request, req id {}", msg.req_id());
                    // Repeat request, return directly
                    return Ok(msg.success());
                }
            }
        }

        let (tx, rx) = oneshot::channel();
        self.sender.send(Envelope::new(msg, tx)).await?;
        Ok(rx.await??)
    }
}
