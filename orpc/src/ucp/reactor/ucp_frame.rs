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

use bytes::{Buf, BufMut, BytesMut};
use log::info;
use crate::io::IOResult;
use crate::message::{BoxMessage, HEAD_SIZE, MAX_DATE_SIZE, Message, Protocol, RefMessage};
use crate::{err_box, message, sys};
use crate::handler::Frame;
use crate::io::net::ConnState;
use crate::sys::{DataSlice, RawVec};
use crate::ucp::reactor::AsyncEndpoint;

pub struct UcpFrame {
    endpoint: AsyncEndpoint,
    buf: BytesMut,
}

impl UcpFrame {
    pub fn new(endpoint: AsyncEndpoint) -> Self {
        UcpFrame {
            endpoint,
            buf: BytesMut::new(),
        }
    }

    fn get_buf(&mut self, len: usize) -> BytesMut {
        self.buf.reserve(len);
        unsafe {
            self.buf.set_len(len)
        }
        self.buf.split()
    }

    pub async fn handshake_request(&mut self) -> IOResult<()> {
        self.endpoint.handshake_request().await
    }

    pub async fn handshake_response(&mut self) -> IOResult<()> {
        self.endpoint.handshake_response().await
    }

}

impl Frame for UcpFrame {
    async fn send(&mut self, msg: &Message) -> IOResult<()> {
        msg.encode_protocol(&mut self.buf);
        if let Some(header) = &msg.header {
            self.buf.put_slice(&header);
        }

        let data = msg.data.clone();
        if !data.is_empty() {
            self.endpoint.put(data).await?;
        }

        self.endpoint.tag_send(DataSlice::Buffer(self.buf.split())).await?;

        Ok(())
    }

    async fn receive(&mut self) -> IOResult<Message> {
        loop {
            let buf = self.get_buf(1024);
            let mut buf = self.endpoint.tag_recv(buf).await?;
            if buf.len() < message::PROTOCOL_SIZE as usize {
                return err_box!("The data is smaller than the protocol header length");
            }
            let (protocol, header_size, data_size) = Message::decode_protocol(&mut buf)?;

            let header = if header_size > 0 {
                Some(buf.split_to(header_size as usize))
            } else {
                None
            };

            let data = if data_size > 0 {
                let data = self.endpoint.local_mem_slice(data_size as usize);
                DataSlice::MemSlice(RawVec::from_slice(data))
            } else {
                DataSlice::Empty
            };

            let msg = Message::new(protocol, header, data);
            if msg.is_heartbeat() {
                continue;
            } else {
                return Ok(msg);
            }
        }
    }

    fn new_conn_state(&self) -> ConnState {
        ConnState::default()
    }
}