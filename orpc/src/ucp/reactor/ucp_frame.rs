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
use crate::client::ClientConf;
use crate::handler::Frame;
use crate::io::net::ConnState;
use crate::server::ServerConf;
use crate::sys::{DataSlice, RawVec};
use crate::ucp::bindings::ucp_ep;
use crate::ucp::reactor::AsyncEndpoint;

pub struct UcpFrame {
    endpoint: AsyncEndpoint,
    buf: BytesMut,
    tag_max_len: usize,
    small_use_tag: bool,
}

impl UcpFrame {
    pub fn new(endpoint: AsyncEndpoint, tag_max_len: usize, small_use_tag: bool) -> Self {
        Self {
            endpoint,
            buf: BytesMut::new(),
            tag_max_len,
            small_use_tag
        }
    }

    pub fn with_server(endpoint: AsyncEndpoint, conf: &ServerConf) -> Self {
        Self::new(endpoint, conf.ucp_tag_max_len, conf.ucp_small_use_tag)
    }

    pub fn with_client(endpoint: AsyncEndpoint, conf: &ClientConf) -> Self {
       Self::new(endpoint, conf.ucp_tag_max_len, conf.ucp_small_use_tag)
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
    async fn send(&mut self, msg: impl RefMessage) -> IOResult<()> {
        let msg = msg.into_box();

        msg.encode_protocol(&mut self.buf);
        if let Some(header) = &msg.header {
            self.buf.put_slice(&header);
        }

        let data = match msg {
            BoxMessage::Msg(m) => m.data,
            // @todo 没有考虑好
            BoxMessage::Arc(_) => return err_box!("Not support")
        };

        let header_buf = DataSlice::Buffer(self.buf.split());

        if !data.is_empty() {
            let data_future = self.endpoint.put(data);
            if self.small_use_tag {
                let header_future = self.endpoint.tag_send(header_buf);
                tokio::try_join!(header_future, data_future)?;
            } else {
                let header_future = self.endpoint.stream_send(header_buf);
                tokio::try_join!(header_future, data_future)?;
            }
        } else {
            if self.small_use_tag {
                self.endpoint.tag_send(header_buf).await?;
            } else {
                self.endpoint.stream_send(header_buf).await?;
            }
        }

        Ok(())
    }

    async fn receive(&mut self) -> IOResult<Message> {
        loop {
            let mut proto_buf = if self.small_use_tag {
                let buf = self.get_buf(self.tag_max_len);
                self.endpoint.tag_recv(buf).await?
            } else {
                let buf = self.get_buf(message::PROTOCOL_SIZE as usize);
                self.endpoint.stream_recv(buf, true).await?
            };

            if proto_buf.len() < message::PROTOCOL_SIZE as usize {
                return err_box!("The data is smaller than the protocol header length");
            }

            let (protocol, header_size, data_size) = Message::decode_protocol(&mut proto_buf)?;

            let header = if header_size > 0 {
                if self.small_use_tag {
                    Some(proto_buf.split_to(header_size as usize))
                } else {
                    let header_buf = self.get_buf(header_size as usize);
                    Some(self.endpoint.stream_recv(header_buf, true).await?)
                }
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