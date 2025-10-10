/*// Copyright 2025 OPPO.
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

use bytes::{BufMut, BytesMut};
use crate::io::IOResult;
use crate::message::Message;
use crate::ucp::core::Endpoint;

pub struct UcpFrame {
    endpoint: Endpoint,
    buf: BytesMut,
    remote_memory:
}

impl UcpFrame {
    pub fn new(endpoint: Endpoint) -> Self {
        UcpFrame { endpoint }
    }

    pub async fn send(&mut self, msg: &Message) -> IOResult<()> {
        let header_len = msg.header_len();
        let data_len = msg.data_len();

        // message protocol control block
        self.buf.put_i32((18 + header_len + data_len) as i32);
        self.buf.put_i32(header_len as i32);
        self.buf.put_i8(msg.code());
        self.buf.put_i8(msg.encode_status());
        self.buf.put_i64(msg.req_id());
        self.buf.put_i32(msg.seq_id());

        // message header part
        if let Some(h) = &msg.header {
            self.buf.extend_from_slice(h);
        }

        // write rma data
        self.endpoint.put()

        Ok(())
    }
}*/