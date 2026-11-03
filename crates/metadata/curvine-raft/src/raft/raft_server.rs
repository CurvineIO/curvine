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
use curvine_net::net::ConnState;
use curvine_rpc::client::dispatch::Envelope;
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::Message;
use curvine_rpc::server::{RpcServer, ServerConf, ServerStateListener};
use curvine_runtime::common::DurationUnit;
use curvine_runtime::runtime::Runtime;
use curvine_runtime::sync::FastMutex;
use log::warn;
use mini_moka::sync::{Cache, CacheBuilder};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub struct RaftServer {
    server: RpcServer<RaftService>,
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
        let mut server_conf = ServerConf::with_hostname(conf.hostname.to_string(), conf.rpc_port);
        server_conf.name = "curvine-journal".to_string();
        server_conf.enable_splice = false;
        server_conf.enable_send_file = false;
        server_conf.pipe_pool_init_cap = 0;
        server_conf.pipe_pool_max_cap = 0;

        let service = RaftService::new(conf);

        let server = RpcServer::with_rt(rt, server_conf, service);
        Self { server }
    }

    pub fn start(self) -> ServerStateListener {
        RpcServer::run_server(self.server)
    }

    pub fn take_receiver(&mut self) -> CommonResult<mpsc::Receiver<Envelope>> {
        self.server.service_mut().take_receiver()
    }

    pub fn new_sender(&self) -> mpsc::Sender<Envelope> {
        self.server.service().sender.clone()
    }

    /// The dedup cache shared between the rpc handlers and the raft node.
    pub fn retry_cache(&self) -> RetryCache {
        self.server.service().retry_cache.clone()
    }
}

pub struct RaftService {
    sender: mpsc::Sender<Envelope>,
    receiver: Option<mpsc::Receiver<Envelope>>,
    retry_cache: RetryCache,
    retry_wait_timeout: Duration,
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
            retry_wait_timeout: Duration::from_millis(conf.io_timeout_ms.max(1)),
            snapshot_chunk_size: conf.snapshot_read_chunk_size,
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
            retry_wait_timeout: self.retry_wait_timeout,
        }
    }
}

pub struct RaftHandler {
    sender: mpsc::Sender<Envelope>,
    download_handler: FastMutex<SnapshotDownloadHandler>,
    retry_cache: RetryCache,
    retry_wait_timeout: Duration,
}

impl RaftHandler {
    /// Wait until the in-flight request becomes committed.
    ///
    /// Returns `false` when the wait budget (`io_timeout_ms`) expires while the
    /// entry is still unapplied, e.g. the entry was overwritten by a new leader
    /// and can never commit. The caller must answer a retryable error instead
    /// of a success in that case.
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
        // Check for duplicate requests. Propose and conf change both carry a
        // client req_id and are applied through the raft log, so both can be
        // deduplicated by the same cache.
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
                    // The previous attempt was never acknowledged, so its
                    // outcome is unknown. Wait until the entry is applied;
                    // if the wait budget expires the entry may never commit
                    // (e.g. it was overwritten by a new leader) and the caller
                    // must be told to retry instead of seeing a fake success.
                    if self.wait_committed(msg.req_id()).await {
                        return Ok(msg.success());
                    }
                    return Err(RaftError::leader_not_ready().ctx(format!(
                        "request {} result is still unknown, please retry",
                        msg.req_id()
                    )));
                }

                None => {
                    self.retry_cache.insert(msg.req_id(), RetryState::InFlight);
                }
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

    fn test_handler(cache: RetryCache, wait_timeout: Duration) -> RaftHandler {
        let (sender, _receiver) = mpsc::channel(16);
        RaftHandler {
            sender,
            download_handler: FastMutex::new(SnapshotDownloadHandler::new(1024)),
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

        let msg = propose_msg(1);
        rt.block_on(handler.async_handle(msg)).unwrap_err();
        assert_eq!(cache.get(&1), Some(RetryState::InFlight));

        // A retry stays parked while the entry is unapplied.
        let retry = propose_msg(1);
        let handler = Arc::new(handler);
        let waiter = {
            let handler = Arc::clone(&handler);
            rt.spawn(async move { handler.async_handle(retry).await })
        };
        assert!(
            !waiter.is_finished(),
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

        let msg = propose_msg(2);
        rt.block_on(handler.async_handle(msg)).unwrap_err();

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

        // Conf change requests go through the same dedup cache.
        let conf = Builder::new_rpc(RaftCode::ConfChange).req_id(4).build();
        rt.block_on(handler.async_handle(conf)).unwrap_err();
        assert_eq!(cache.get(&4), Some(RetryState::InFlight));
    }
}
