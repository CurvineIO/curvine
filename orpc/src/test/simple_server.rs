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

use crate::common::Utils;
use crate::error::CommonErrorExt;
use crate::handler::{HandlerService, MessageHandler};
use crate::io::net::{ConnState, InetAddr, NetUtils};
use crate::message::Message;
use crate::runtime::Runtime;
use crate::server::{RpcServer, ServerConf};
use crate::sync::FastDashMap;
use crate::sys::DataSlice;
use crate::{err_box, CommonResultExt};
use log::info;
use std::sync::Arc;
use std::thread;

pub struct SimpleHandler {
    mock: bool,
    call_map: FastDashMap<i64, u32>,
}

impl MessageHandler for SimpleHandler {
    type Error = CommonErrorExt;

    fn handle(&mut self, msg: &Message) -> CommonResultExt<Message> {
        let id = msg.req_id();
        if self.mock && !self.call_map.contains_key(&id) {
            self.call_map.insert(id, 1);
            return err_box!("please retry");
        }

        let req_str = match msg.data_bytes() {
            None => "EMPTY".to_string(),

            Some(v) => String::from_utf8_lossy(v).to_string(),
        };

        let rep_str = req_str.to_uppercase().to_string();
        let bytes = DataSlice::from_str(&rep_str);
        info!(
            "Handler req_id {}, request: {}, response {}",
            msg.req_id(),
            req_str,
            rep_str
        );

        let rep_msg = msg.success_with_data(None, bytes);

        Ok(rep_msg)
    }
}

pub struct SimpleService;

impl HandlerService for SimpleService {
    type Item = SimpleHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        SimpleHandler {
            mock: true,
            call_map: FastDashMap::default(),
        }
    }
}

pub struct SimpleServer {
    server: RpcServer<SimpleService>,
}

impl SimpleServer {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let conf = ServerConf::with_hostname(host, port);
        let server = RpcServer::new(conf, SimpleService);

        Self { server }
    }

    pub fn start(self, sleep_ms: u64) {
        let server = self;
        thread::spawn(move || {
            if sleep_ms > 0 {
                Utils::sleep(sleep_ms)
            }
            server.server.block_on_start();
        });
    }

    pub fn bind_addr(&self) -> &InetAddr {
        self.server.bind_addr()
    }

    pub fn new_rt(&self) -> Arc<Runtime> {
        self.server.clone_rt()
    }
}

impl Default for SimpleServer {
    fn default() -> Self {
        let host = "127.0.0.1";
        let port = NetUtils::get_available_port();
        Self::new(host, port)
    }
}
