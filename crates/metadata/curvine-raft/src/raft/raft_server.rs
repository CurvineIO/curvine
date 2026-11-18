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
use curvine_core_error::{err_box, try_option, CommonResult, ErrorExt};
use curvine_net::net::{ConnState, NetUtils};
use curvine_rpc::client::dispatch::Envelope;
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::Message;
use curvine_rpc::server::{RpcServer, ServerConf, ServerMonitor, ServerStateListener};
use curvine_runtime::common::DurationUnit;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastMutex;
use log::warn;
use mini_moka::sync::{Cache, CacheBuilder};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub struct RaftServer {
    rt: Arc<Runtime>,
    service: RaftService,
    hostname: String,
    port: u16,
    grpc: curvine_rpc::grpc::GrpcConfig,
    conf: JournalConf,
    snapshot_root: Option<PathBuf>,
}

/// Dedup state of a propose/conf-change request keyed by its req_id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryState {
    /// The request was accepted but its log entry is not applied yet, so the
    /// outcome is still unknown.
    InFlight,
    /// The log entry was applied by this node: the write is committed.
    Committed,
}

pub type RetryCache = Arc<Cache<i64, RetryState>>;

impl RaftServer {
    pub fn with_rt(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        Self {
            rt,
            service: RaftService::new(conf),
            hostname: conf.hostname.clone(),
            port: conf.rpc_port,
            grpc: conf.grpc.clone(),
            conf: conf.clone(),
            snapshot_root: None,
        }
    }

    pub fn with_snapshot_root(mut self, root: Option<PathBuf>) -> Self {
        self.snapshot_root = root;
        self
    }

    pub async fn start(self) -> CommonResult<ServerStateListener> {
        self.conf.validate_snapshot_transport()?;
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
                    .build_v1alpha()?,
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

        // Snapshot transfers use a connection-local ORPC handler on their own
        // listener. Start both transports before reporting the journal ready.
        let mut snapshot_conf =
            ServerConf::with_hostname(self.hostname.clone(), self.conf.snapshot_rpc_port);
        snapshot_conf.name = "curvine-journal-snapshot".to_string();
        snapshot_conf.enable_splice = false;
        snapshot_conf.enable_send_file = false;
        snapshot_conf.pipe_pool_init_cap = 0;
        snapshot_conf.pipe_pool_max_cap = 0;
        let snapshot_server = RpcServer::with_rt(
            self.rt.clone(),
            snapshot_conf,
            SnapshotService {
                chunk_size: self.conf.snapshot_read_chunk_size,
                root: self.snapshot_root,
            },
        );
        let snapshot_shutdown = snapshot_server.shutdown_handle();
        let mut snapshot_listener = snapshot_server.start();
        snapshot_listener.wait_startup().await?;
        let monitor = Arc::new(ServerMonitor::new());
        let status = monitor.new_listener();
        let state = monitor.clone();
        let service = Arc::new(self.service);
        let (health, health_service) = curvine_rpc::grpc::health_reporter();
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
            let (shutdown, shutdown_rx) = oneshot::channel();
            let grpc = grpc_server
                .add_optional_service(reflection)
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                    health.set_not_serving::<crate::proto::raft::raft_service_server::RaftServiceServer<crate::raft::grpc_server::RaftGrpcServer>>().await;
                });
            tokio::pin!(grpc);
            let grpc_stopped = tokio::select! {
                result = &mut grpc => {
                    if let Err(error) = result {
                        log::error!("Raft gRPC listener stopped: {}", error);
                    }
                    true
                }
                result = snapshot_listener.wait_stop() => {
                    if let Err(error) = result {
                        log::error!("Raft snapshot listener stopped: {}", error);
                    }
                    false
                }
                _ = shutdown_signal() => false,
            };
            state.advance_shutdown();
            snapshot_shutdown.send_replace(true);
            if !grpc_stopped {
                let _ = shutdown.send(());
                if let Err(error) = grpc.await {
                    log::error!("Raft gRPC listener stopped: {}", error);
                }
            }
            if let Err(error) = snapshot_listener.wait_stop().await {
                log::error!("Raft snapshot shutdown failed: {}", error);
            }
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

    pub fn retry_cache(&self) -> RetryCache {
        self.service.retry_cache.clone()
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

/// ORPC is deliberately limited to snapshot downloads. In particular, a
/// plaintext connection here cannot reach the Raft control actor.
struct SnapshotService {
    chunk_size: usize,
    root: Option<PathBuf>,
}

impl HandlerService for SnapshotService {
    type Item = SnapshotHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        SnapshotHandler {
            download_handler: FastMutex::new(SnapshotDownloadHandler::with_root(
                self.chunk_size,
                self.root.clone(),
            )),
        }
    }
}

struct SnapshotHandler {
    download_handler: FastMutex<SnapshotDownloadHandler>,
}

impl MessageHandler for SnapshotHandler {
    type Error = RaftError;

    fn handle(&self, msg: &Message) -> RaftResult<Message> {
        if RaftCode::from(msg.code()) == RaftCode::SnapshotDownload {
            self.download_handler.lock().handle(msg)
        } else {
            err_box!(
                "Unsupported snapshot request type: {:?}",
                RaftCode::from(msg.code())
            )
        }
    }
}

pub struct RaftService {
    sender: mpsc::Sender<Envelope>,
    receiver: Option<mpsc::Receiver<Envelope>>,
    retry_cache: RetryCache,
    retry_wait_timeout: Duration,
}

impl RaftService {
    pub fn new(conf: &JournalConf) -> Self {
        let ttl = DurationUnit::from_str(&conf.raft_retry_cache_ttl).unwrap();
        let cache = CacheBuilder::new(conf.raft_retry_cache_size)
            .time_to_live(Duration::from_millis(ttl.as_millis()))
            .build();

        let (sender, receiver) = mpsc::channel(conf.message_size);
        // The wait budget must stay strictly below the client's rpc timeout
        // (`io_timeout_ms`) so a parked retry answers the caller with a
        // retryable error instead of a transport timeout. Half of it is the
        // budget; the driver owns the reservation, so only genuinely in-flight
        // entries can park a retry here.
        Self {
            sender,
            receiver: Some(receiver),
            retry_cache: Arc::new(cache),
            retry_wait_timeout: Duration::from_millis((conf.io_timeout_ms / 2).max(1)),
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
            retry_cache: self.retry_cache.clone(),
            retry_wait_timeout: self.retry_wait_timeout,
        }
    }
}

pub struct RaftHandler {
    sender: mpsc::Sender<Envelope>,
    retry_cache: RetryCache,
    retry_wait_timeout: Duration,
}

impl RaftHandler {
    /// Wait until the in-flight request becomes committed.
    ///
    /// Returns `false` when the wait budget (half of `io_timeout_ms`) expires
    /// while the entry is still unapplied, e.g. the entry was overwritten by a
    /// new leader and can never commit. The caller must answer a retryable
    /// error instead of a success in that case. The budget is kept strictly
    /// below the client rpc timeout so the caller sees the retryable error
    /// rather than a transport timeout.
    async fn wait_committed(&self, req_id: i64) -> bool {
        let poll = Duration::from_millis(20);
        let mut waited = Duration::ZERO;
        while waited < self.retry_wait_timeout {
            let sleep = poll.min(self.retry_wait_timeout - waited);
            tokio::time::sleep(sleep).await;
            waited += sleep;

            if self.retry_cache.get(&req_id) == Some(RetryState::Committed) {
                return true;
            }
        }

        false
    }
}

impl MessageHandler for RaftHandler {
    type Error = RaftError;

    fn is_sync(&self, msg: &Message) -> bool {
        let _ = msg;
        false
    }

    fn handle(&self, msg: &Message) -> RaftResult<Message> {
        err_box!(
            "Unsupported Raft control request type: {:?}",
            RaftCode::from(msg.code())
        )
    }

    async fn async_handle(&self, msg: Message) -> Result<Message, Self::Error> {
        // Check for duplicate requests. Propose and conf change both carry a
        // client req_id and are applied through the raft log, so both can be
        // deduplicated by the same cache.
        //
        // The handler only reads the cache. Reservations (`InFlight`) are
        // owned by the raft driver and inserted only after the entry was
        // really appended, so a pre-append rejection never leaves a marker
        // that would stall later retries; only genuinely in-flight entries
        // park a retry here. The single-threaded driver is also the only
        // writer, so `Committed` can never be overwritten by a reservation.
        let code = RaftCode::from(msg.code());
        if matches!(code, RaftCode::Propose | RaftCode::ConfChange) {
            match self.retry_cache.get(&msg.req_id()) {
                Some(RetryState::Committed) => {
                    warn!(
                        "Retry committed {:?} request, req id {}",
                        code,
                        msg.req_id()
                    );
                    // Repeat of an applied request, return directly.
                    return Ok(msg.success());
                }

                Some(RetryState::InFlight) => {
                    warn!(
                        "Retry in-flight {:?} request, req id {}",
                        code,
                        msg.req_id()
                    );
                    // The previous attempt was appended but never
                    // acknowledged, so its outcome is unknown. Wait until the
                    // entry is applied; if the wait budget expires the entry
                    // may never commit (e.g. it was overwritten by a new
                    // leader) and the caller must be told to retry instead of
                    // seeing a fake success.
                    if self.wait_committed(msg.req_id()).await {
                        return Ok(msg.success());
                    }
                    return Err(RaftError::leader_not_ready().ctx(format!(
                        "request {} result is still unknown, please retry",
                        msg.req_id()
                    )));
                }

                None => {}
            }
        }

        let (tx, rx) = oneshot::channel();
        self.sender.send(Envelope::new(msg, tx)).await?;
        Ok(rx.await??)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_rpc::message::Builder;
    use curvine_runtime::runtime::RpcRuntime;

    #[test]
    fn snapshot_service_rejects_control_messages() {
        let service = SnapshotService {
            chunk_size: 1024,
            root: None,
        };
        let handler = service.get_message_handler(None);
        for code in [
            RaftCode::Ping,
            RaftCode::Raft,
            RaftCode::ConfChange,
            RaftCode::Propose,
            RaftCode::ReportSnapshot,
        ] {
            assert!(handler.handle(&Builder::new_rpc(code).build()).is_err());
        }
    }

    fn test_handler(cache: RetryCache, wait_timeout: Duration) -> RaftHandler {
        let (sender, _receiver) = mpsc::channel(16);
        RaftHandler {
            sender,
            retry_cache: cache,
            retry_wait_timeout: wait_timeout,
        }
    }

    fn propose_msg(req_id: i64) -> Message {
        Builder::new_rpc(RaftCode::Propose).req_id(req_id).build()
    }

    // A retry of an in-flight request must wait for the commit instead of being
    // acknowledged as a success: the first attempt was never answered, so the
    // outcome is still unknown (issue #1906).
    #[test]
    fn in_flight_retry_waits_until_committed() {
        let rt = Runtime::new("retry-cache-test", 2, 2);
        let cache: RetryCache = Arc::new(
            CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(600))
                .build(),
        );
        let handler = test_handler(cache.clone(), Duration::from_secs(5));

        // The driver reserved the req_id after appending the entry; the
        // handler itself never writes the reservation.
        cache.insert(1, RetryState::InFlight);

        // A retry stays parked while the entry is unapplied. Assert with a
        // short timeout instead of an immediate is_finished check, which
        // would pass whether or not the handler actually waits.
        let retry = propose_msg(1);
        let handler = Arc::new(handler);
        let waiter = {
            let handler = Arc::clone(&handler);
            rt.spawn(async move { handler.async_handle(retry).await })
        };
        let mut waiter = waiter;
        let still_parked = rt.block_on(async {
            tokio::time::timeout(Duration::from_millis(100), &mut waiter)
                .await
                .is_err()
        });
        assert!(
            still_parked,
            "an in-flight retry must wait, not return success"
        );

        cache.insert(1, RetryState::Committed);
        let response = rt
            .block_on(async { tokio::time::timeout(Duration::from_secs(2), waiter).await })
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            response.is_success(),
            "the retried request must succeed once the entry is committed"
        );
    }

    // If the entry never commits within the wait budget, the retry must get a
    // retryable error instead of a fake success.
    #[test]
    fn in_flight_retry_times_out_as_retryable_error() {
        let rt = Runtime::new("retry-cache-test", 2, 2);
        let cache: RetryCache = Arc::new(
            CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(600))
                .build(),
        );
        let handler = test_handler(cache.clone(), Duration::from_millis(100));

        // The driver reserved the req_id after appending the entry.
        cache.insert(2, RetryState::InFlight);

        let retry = propose_msg(2);
        let err = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), handler.async_handle(retry)).await
            })
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(err, RaftError::LeaderNotReady(_)),
            "expected retryable LeaderNotReady, got {:?}",
            err
        );
        assert!(err.retry_leader(), "the timeout error must be retryable");
    }

    // A retry whose first attempt already committed is still a dedup success.
    #[test]
    fn committed_retry_returns_success_directly() {
        let rt = Runtime::new("retry-cache-test", 2, 2);
        let cache: RetryCache = Arc::new(
            CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(600))
                .build(),
        );
        let handler = test_handler(cache.clone(), Duration::from_millis(100));

        cache.insert(3, RetryState::Committed);
        let response = rt.block_on(handler.async_handle(propose_msg(3))).unwrap();
        assert!(response.is_success());

        // Conf change requests go through the same dedup cache: a Committed
        // conf change retry is answered directly, a miss is just forwarded.
        let conf = Builder::new_rpc(RaftCode::ConfChange).req_id(4).build();
        rt.block_on(handler.async_handle(conf)).unwrap_err();
        assert_eq!(cache.get(&4), None, "the handler must not reserve req_ids");
    }
}
