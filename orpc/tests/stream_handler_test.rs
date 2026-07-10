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

use orpc::error::CommonErrorExt;
use orpc::handler::{Frame, MessageHandler, StreamHandler};
use orpc::io::net::ConnState;
use orpc::io::IOResult;
use orpc::message::{BoxMessage, Builder, Message, RefMessage, RequestStatus};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::ServerConf;
use std::sync::Arc;

struct MemoryFrame {
    sent: Vec<BoxMessage>,
}

impl Frame for MemoryFrame {
    async fn send(&mut self, message: impl RefMessage) -> IOResult<()> {
        self.sent.push(message.into_box());
        Ok(())
    }

    async fn receive(&mut self) -> IOResult<Message> {
        Ok(Message::empty())
    }

    fn new_conn_state(&self) -> ConnState {
        ConnState::default()
    }
}

struct AsyncErrorHandler;

impl MessageHandler for AsyncErrorHandler {
    type Error = CommonErrorExt;

    fn is_sync(&self, _msg: &Message) -> bool {
        false
    }

    fn handle(&mut self, _msg: &Message) -> Result<Message, Self::Error> {
        unreachable!("test handler only uses async_handle")
    }

    async fn async_handle(&mut self, _msg: Message) -> Result<Message, Self::Error> {
        Err(CommonErrorExt::from("async handler failed".to_string()))
    }
}

#[test]
fn async_handler_error_is_sent_as_response() {
    let rt = Arc::new(Runtime::current_thread("stream-handler-test"));
    let conf = ServerConf::with_hostname("127.0.0.1", 0);
    let frame = MemoryFrame { sent: Vec::new() };
    let handler = AsyncErrorHandler;
    let mut stream_handler = StreamHandler::new(rt.clone(), frame, handler, &conf);
    let request = Builder::new()
        .code(1)
        .request(RequestStatus::Open)
        .req_id(1)
        .build();

    rt.block_on(stream_handler.call(request))
        .expect("async handler error should not close the stream");

    let sent = &stream_handler.frame_mut().sent;
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].is_success());
    let err = sent[0].check_error_ext::<CommonErrorExt>().unwrap_err();
    assert!(err.to_string().contains("async handler failed"));
}
