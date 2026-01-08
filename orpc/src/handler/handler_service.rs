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

/// The message processor runs and manages the following functions:
/// 1. Manage the creation of MessageHandler.
/// 2. Global variables required to manage business logic.
///
/// Unlike the ordinary rpc framework, orpc is used to handle file reading and writing. The handler is stateful and needs to pay attention to thread safety issues.
/// In order to save overhead and avoid ownership issues, naked pointers are used to call message processing functions.
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
