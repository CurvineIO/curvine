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

use crate::error::{CommonErrorExt, ErrorExt};
use crate::message::Message;
use crate::runtime::Runtime;
use crate::sys::DataSlice;
use bytes::BytesMut;
use log::info;
use std::future::Future;

/// Message Processors.
pub trait MessageHandler: Send + Sync + 'static {
    type Error: ErrorExt + Send + Sync;

    // Whether to process requests synchronously, default to true
    #[allow(unused)]
    fn is_sync(&self, msg: &Message) -> bool {
        true
    }

    // Process synchronous messages on the runtime blocking pool. Implementations
    // keep request state local or protect connection-local mutable state explicitly.
    fn handle(&self, msg: &Message) -> Result<Message, Self::Error>;

    // 1.7.5 starts to support async trait.
    // In some scenarios, messages will not be processed directly, and messages need to be sent through the channel, and asynchronous functions are required.
    #[allow(unused)]
    fn async_handle(
        &self,
        msg: Message,
    ) -> impl Future<Output = Result<Message, Self::Error>> + Send {
        async { panic!("Please implement the async_handle method") }
    }

    fn get_rt(&self, _msg: &Message) -> Option<&Runtime> {
        None
    }
}

// A message processor for testing, converting strings into capitalization.
pub struct TestMessageHandler;

impl MessageHandler for TestMessageHandler {
    type Error = CommonErrorExt;

    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        info!("request = {:?}", msg);

        let res = match msg.header_bytes() {
            None => None,

            Some(v) => {
                let str = String::from_utf8_lossy(v).to_uppercase();
                Some(BytesMut::from(str.as_bytes()))
            }
        };

        let response = msg.success_with_data(res, DataSlice::Empty);
        Ok(response)
    }
}
