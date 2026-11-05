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
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::net::TcpStream;

use crate::handler::rpc_frame::FrameSate;
use crate::handler::FrameBuf;
use crate::message::Message;
use crate::IOUtils;
use curvine_io::{DataSlice, IOResult};

pub struct ReadFrame {
    io: ReadHalf<TcpStream>,
    buf: FrameBuf,
    partial: bool,
}

impl ReadFrame {
    pub(crate) fn new(io: ReadHalf<TcpStream>, buf: FrameBuf) -> Self {
        Self {
            io,
            buf,
            partial: false,
        }
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
                    match IOUtils::read_head(&mut self.io, &mut self.buf, &mut self.partial).await?
                    {
                        FrameSate::Head => return Ok(Message::empty()),
                        next => {
                            self.partial = true;
                            state = next;
                        }
                    }
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
                    let mut msg = Message::new(protocol, header, data);
                    self.partial = false;

                    let _ = mem::replace(&mut state, FrameSate::Head);

                    // Heartbeat message.
                    if msg.is_heartbeat() {
                        continue;
                    } else {
                        msg.set_time();
                        return Ok(msg);
                    }
                }
            }
        }
    }

    pub async fn receive_timeout(&mut self, dur: Duration) -> IOResult<Message> {
        let res = IOUtils::timeout_io("frame receive", dur, self.receive()).await;
        match res {
            Err(e) if IOUtils::is_timeout(&e) && self.partial => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!(
                    "frame receive timed out after {dur:?} in the middle of a frame, \
                     the stream is desynced and the connection must be dropped"
                ),
            )
            .into()),

            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Builder;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn receive_returns_empty_on_peer_close_before_sending() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept first, then drop the peer side — matches the production
        // "peer closed an already-accepted socket" path and avoids the
        // reset/EOF race on some kernels.
        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();
        drop(client);

        let (read_half, _) = tokio::io::split(server_stream);
        let mut read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let msg = read_frame.receive().await.unwrap();
        assert!(msg.is_empty(), "expected empty message on peer EOF");
    }

    #[tokio::test]
    async fn receive_stamps_receipt_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        // Send one request frame exactly as a peer would.
        let mut frame = BytesMut::new();
        Builder::new_rpc(1i8).build().encode(&mut frame).unwrap();
        let (_, mut client_write) = tokio::io::split(client);
        client_write.write_all(&frame).await.unwrap();

        let (read_half, _) = tokio::io::split(server_stream);
        let mut read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let msg = read_frame.receive().await.unwrap();
        assert!(!msg.is_empty(), "expected a decoded request");
        assert!(
            msg.time.is_some(),
            "ReadFrame::receive must stamp the receipt time for handlers"
        );
    }

    #[tokio::test]
    async fn receive_errors_on_truncated_frame_head() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        // Send fewer than a full frame head, then close, to simulate a truncated head.
        let head_len = crate::message::PROTOCOL_SIZE as usize;
        let (_, mut client_write) = tokio::io::split(client);
        client_write
            .write_all(&vec![0u8; head_len - 1])
            .await
            .unwrap();
        client_write.shutdown().await.unwrap();

        let (read_half, _) = tokio::io::split(server_stream);
        let mut read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let err = match read_frame.receive().await {
            Ok(_) => panic!("truncated frame head must be an error, not a clean EOF"),
            Err(e) => e,
        };
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::UnexpectedEof,
            "truncated frame head must surface as UnexpectedEof, not {:?}",
            err.kind()
        );
        assert!(
            err.to_string().contains("frame head"),
            "expected a frame-head truncation error, got: {err}"
        );
    }
}
