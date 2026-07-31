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
use log::warn;
use mini_moka::sync::{Cache, CacheBuilder};
use orpc::client::dispatch::Envelope;
use orpc::common::DurationUnit;
use orpc::handler::{HandlerService, MessageHandler};
use orpc::io::net::ConnState;
use orpc::message::Message;
use orpc::runtime::Runtime;
use orpc::server::{RpcServer, ServerConf, ServerStateListener};
use orpc::sync::FastMutex;
use orpc::{err_box, try_option, CommonResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub struct RaftServer {
    server: RpcServer<RaftService>,
}

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
}

pub struct RaftService {
    sender: mpsc::Sender<Envelope>,
    receiver: Option<mpsc::Receiver<Envelope>>,
    retry_cache: Arc<Cache<i64, ()>>,
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
            download_handler: FastMutex::new(SnapshotDownloadHandler::new(1024 * 1024)),
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
