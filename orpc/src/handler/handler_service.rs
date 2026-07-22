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

use crate::handler::{Frame, MessageHandler, StreamHandler, TestMessageHandler};
use crate::io::net::ConnState;
use crate::runtime::Runtime;
use crate::server::ServerConf;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub struct LimitConf {
    pub conn_limit: usize,
    pub global_limit: Arc<Semaphore>,
}

impl LimitConf {
    pub fn new(conn_limit: usize, global_limit: usize) -> Self {
        assert!(conn_limit > 0, "connection limit must be greater than zero");
        assert!(global_limit > 0, "global limit must be greater than zero");
        Self {
            conn_limit,
            global_limit: Arc::new(Semaphore::new(global_limit)),
        }
    }
}

/// The message processor runs and manages the following functions:
/// 1. Manage the creation of MessageHandler.
/// 2. Global variables required to manage business logic.
///
/// Unlike ordinary RPC frameworks, orpc also handles stateful file read/write
/// streams. Handlers are shared through `Arc`, so connection-local mutable
/// state must use explicit interior synchronization.
pub trait HandlerService: Send + Sync + 'static {
    type Item: MessageHandler;
    // Whether to pass connection information.
    fn has_conn_state(&self) -> bool {
        false
    }

    fn get_message_handler(&self, conn_info: Option<ConnState>) -> Self::Item;

    fn get_stream_handler<F: Frame>(
        &self,
        rt: Arc<Runtime>,
        frame: F,
        conf: &ServerConf,
    ) -> StreamHandler<F, Self::Item> {
        let conn_state = if self.has_conn_state() {
            Some(frame.new_conn_state())
        } else {
            None
        };

        let handler = self.get_message_handler(conn_state);
        StreamHandler::new(rt, frame, handler, conf)
    }

    fn get_limit(&self) -> LimitConf {
        panic!("BufferHandler service must provide a shared LimitConf")
    }
}

#[allow(unused)]
#[derive(Default)]
pub struct TestService;

impl HandlerService for TestService {
    type Item = TestMessageHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        TestMessageHandler {}
    }
}

#[cfg(test)]
mod tests {
    use super::{HandlerService, LimitConf, TestService};
    use std::sync::Arc;

    #[test]
    fn cloned_limit_conf_shares_global_semaphore() {
        let limit = LimitConf::new(8, 4096);
        let cloned = limit.clone();

        assert_eq!(limit.conn_limit, 8);
        assert_eq!(limit.global_limit.available_permits(), 4096);
        assert!(Arc::ptr_eq(&limit.global_limit, &cloned.global_limit));
    }

    #[test]
    #[should_panic(expected = "connection limit must be greater than zero")]
    fn zero_connection_limit_is_rejected() {
        LimitConf::new(0, 4096);
    }

    #[test]
    #[should_panic(expected = "global limit must be greater than zero")]
    fn zero_global_limit_is_rejected() {
        LimitConf::new(8, 0);
    }

    #[test]
    #[should_panic(expected = "BufferHandler service must provide a shared LimitConf")]
    fn buffer_service_must_provide_shared_limit() {
        TestService.get_limit();
    }
}
