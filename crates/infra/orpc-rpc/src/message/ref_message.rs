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

use crate::io::IOResult;
use crate::message::{BoxMessage, Message};
use bytes::BytesMut;

/// This is to solve the rust ownership problem.
/// For example, when the exception is retrying, Message has moved, Message cannot be used, and Message needs to be wrapped in Arc or Rc to solve the problem.
/// This trait defines the message unified access interface.
pub trait RefMessage {
    fn as_ref(&self) -> &Message;

    fn as_mut(&mut self) -> &mut Message;

    fn into_box(self) -> BoxMessage;

    fn encode(&self, buf: &mut BytesMut) -> IOResult<()> {
        self.as_ref().encode(buf)
    }

    fn decode(buf: &mut BytesMut) -> IOResult<Message> {
        Message::decode(buf)
    }

    fn req_id(&self) -> i64 {
        self.as_ref().protocol.req_id
    }

    fn seq_id(&self) -> i32 {
        self.as_ref().protocol.seq_id
    }
}
