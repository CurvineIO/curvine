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

use crate::handler::rpc_frame::FrameSate;
use crate::io::IOResult;
use crate::message::{Message, Protocol, MAX_DATE_SIZE};
use crate::sys::DataSlice;
use crate::{err_box, message};
use bytes::{Buf, BytesMut};
use std::mem;
use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::net::TcpStream;

// @todo and RpcFrame have duplicate code, and subsequent optimizations
pub struct ReadFrame {
    io: ReadHalf<TcpStream>,
    buf: BytesMut,
}

impl ReadFrame {
    pub(crate) fn new(io: ReadHalf<TcpStream>, buf: BytesMut) -> Self {
        Self { io, buf }
    }

    // Read data of the specified length.
    pub async fn read_full(&mut self, len: i32) -> IOResult<BytesMut> {
        let len = len as usize;
        self.buf.reserve(len);
        unsafe {
            self.buf.set_len(len);
        }
        let mut buf = self.buf.split_to(len);
        self.io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    pub async fn receive(&mut self) -> IOResult<Message> {
        let mut state = FrameSate::Head;
        loop {
            match state {
                FrameSate::Head => {
                    let mut buf = match self.read_full(message::PROTOCOL_SIZE).await {
                        Ok(v) => v,
                        Err(_) => return Ok(Message::empty()),
                    };

                    let total_size = buf.get_i32();
                    let header_size = buf.get_i32();
                    let data_size = total_size - header_size - message::HEAD_SIZE;
                    if data_size < 0 {
                        return err_box!("data length is negative");
                    } else if data_size > MAX_DATE_SIZE {
                        return err_box!("Data exceeds maximum size: {}", MAX_DATE_SIZE);
                    }

                    let protocol = Protocol::create(&mut buf);
                    let _ = mem::replace(
                        &mut state,
                        FrameSate::Data(protocol, header_size, data_size),
                    );
                }

                FrameSate::Data(protocol, header_size, data_size) => {
                    let header = if header_size > 0 {
                        let buf = self.read_full(header_size).await?;
                        Some(buf)
                    } else {
                        None
                    };

                    let data = if data_size <= 0 {
                        DataSlice::Empty
                    } else {
                        let bytes = self.read_full(data_size).await?;
                        DataSlice::Buffer(bytes)
                    };
                    let msg = Message {
                        protocol,
                        header,
                        data,
                    };

                    let _ = mem::replace(&mut state, FrameSate::Head);

                    // Heartbeat message.
                    if msg.is_heartbeat() {
                        continue;
                    } else {
                        return Ok(msg);
                    }
                }
            }
        }
    }
}
