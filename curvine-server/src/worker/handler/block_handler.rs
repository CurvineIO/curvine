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

use crate::worker::block::BlockStore;
use crate::worker::handler::BlockHandler::{Reader, Writer};
use crate::worker::handler::{ReadHandler, WriteHandler};
use curvine_client::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::FsResult;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use orpc::{err_box, CommonResult};
use std::sync::Arc;

pub enum BlockHandler {
    Writer(WriteHandler),
    Reader(ReadHandler),
}

impl BlockHandler {
    pub fn new(code: RpcCode, store: BlockStore, fs_context: Arc<FsContext>) -> CommonResult<Self> {
        let handler = match code {
            RpcCode::WriteBlock => Writer(WriteHandler::new(store, fs_context)),

            RpcCode::ReadBlock => Reader(ReadHandler::new(store)),

            code => return err_box!("Unsupported request type: {:?}", code),
        };

        Ok(handler)
    }
}

impl MessageHandler for BlockHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let response = match self {
            Writer(h) => h.handle(msg),
            Reader(h) => h.handle(msg),
        };

        match response {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }
}
