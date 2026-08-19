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

use std::io;
use std::mem;

use bytes::BytesMut;
use log::debug;
use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::net::TcpStream;

use crate::handler::rpc_frame::FrameSate;
use crate::handler::FrameBuf;
use crate::message;
use crate::message::Message;
use curvine_io::{DataSlice, IOResult};

pub struct ReadFrame {
    io: ReadHalf<TcpStream>,
    buf: FrameBuf,
}

impl ReadFrame {
    pub(crate) fn new(io: ReadHalf<TcpStream>, buf: FrameBuf) -> Self {
        Self { io, buf }
    }

    // Read data of the specified length.
    pub async fn read_full(&mut self, len: i32) -> IOResult<BytesMut> {
        if len == 0 {
            return Ok(BytesMut::new());
        } else if len < 0 {
            return err_box!("Invalid length {}", len);
        }

        let mut buf = self.buf.take_exact(len as usize);
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
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            return Ok(Message::empty());
                        }
                        Err(e) => {
                            debug!("ReadFrame head read error: {}", e);
                            return Err(e);
                        }
                    };

                    let (protocol, header_size, data_size) = Message::decode_protocol(&mut buf)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn receive_returns_empty_on_peer_close_before_sending() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Connect and immediately drop the connection.
        let stream = TcpStream::connect(addr).await.unwrap();
        drop(stream);

        let (server_stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = tokio::io::split(server_stream);
        let mut read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let msg = read_frame.receive().await.unwrap();
        assert!(msg.is_empty(), "expected empty message on peer EOF");
    }
}
