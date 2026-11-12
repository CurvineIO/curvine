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

use std::io::IoSlice;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

use crate::handler::FrameBuf;
use crate::message::{self, Message, RefMessage};
use crate::IOUtils;
use curvine_io::{DataSlice, IOResult};

pub struct WriteFrame {
    io: WriteHalf<TcpStream>,
    buf: FrameBuf,
}

impl WriteFrame {
    // A frame at or below this size is coalesced into buf and written once.
    // Linux A/B (7 runs, paced, interleaved, median delta): writev loses
    // 0.1-0.3 us at 4-8 KiB (the copy is only ~0.1 us while the extra segments
    // cost more), the sign at 16 KiB depends on the run (-0.9..+0.7 us, a
    // wash), and it wins from 32 KiB up: 0.2-1.7 us at 32-64 KiB, 98-125 us at
    // 1 MiB. Break-even lies between 16 and 32 KiB.
    const MAX_COALESCE: usize = 16 * 1024;

    pub(crate) fn new(io: WriteHalf<TcpStream>, buf: FrameBuf) -> Self {
        Self { io, buf }
    }

    /// Half-close the connection. The peer reads EOF and closes its end, so
    /// the read half sees EOF too and the socket is released.
    pub async fn shutdown(&mut self) -> IOResult<()> {
        self.io.shutdown().await?;
        Ok(())
    }

    pub async fn send(&mut self, msg: &Message) -> IOResult<()> {
        let data = match &msg.data {
            DataSlice::IOSlice(_) => return err_box!("WriteFrame cannot send an IOSlice payload"),
            data => data.as_slice(),
        };
        let total = msg.total_len();

        if total <= Self::MAX_COALESCE {
            msg.encode_protocol(&mut *self.buf);
            if let Some(h) = &msg.header {
                self.buf.extend_from_slice(h);
            }
            if !data.is_empty() {
                self.buf.extend_from_slice(data);
            }
            self.io.write_all(&self.buf.split()).await?;
        } else {
            if !self.buf.is_empty() {
                self.io.write_all(&self.buf.split()).await?;
            }
            let mut head = [0u8; message::PROTOCOL_USIZE];
            msg.encode_protocol(&mut head[..]);
            let header = msg.header_bytes().unwrap_or(&[]);
            IOUtils::write_iov(
                &mut self.io,
                [
                    IoSlice::new(&head),
                    IoSlice::new(header),
                    IoSlice::new(data),
                ],
            )
            .await?;
        }

        Ok(())
    }

    pub async fn send_batch<M: RefMessage>(&mut self, msgs: &[M]) -> IOResult<()> {
        for m in msgs {
            let msg = m.as_ref();
            let data = match &msg.data {
                DataSlice::IOSlice(_) => {
                    return err_box!("WriteFrame cannot send an IOSlice payload")
                }
                data => data.as_slice(),
            };
            let total = msg.total_len();
            if total > Self::MAX_COALESCE {
                self.send(msg).await?;
                continue;
            }

            msg.encode_protocol(&mut *self.buf);
            if let Some(h) = &msg.header {
                self.buf.extend_from_slice(h);
            }
            if !data.is_empty() {
                self.buf.extend_from_slice(data);
            }
            if self.buf.len() >= Self::MAX_COALESCE {
                self.io.write_all(&self.buf.split()).await?;
            }
        }

        if !self.buf.is_empty() {
            self.io.write_all(&self.buf.split()).await?;
        }
        Ok(())
    }

    pub async fn send_timeout(&mut self, msg: &Message, dur: Duration) -> IOResult<()> {
        IOUtils::timeout_io("frame send", dur, self.send(msg)).await
    }

    pub async fn send_batch_timeout<M: RefMessage>(
        &mut self,
        msgs: &[M],
        dur: Duration,
    ) -> IOResult<()> {
        IOUtils::timeout_io("frame send", dur, self.send_batch(msgs)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::ReadFrame;
    use crate::message::Builder;
    use bytes::BytesMut;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn send_batch_large_message_uses_writev() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        // WriteFrame on the client's write half, ReadFrame on the server's
        // read half, mirroring the production client/server layout.
        let (_, client_write) = tokio::io::split(client);
        let (server_read, _) = tokio::io::split(server_stream);
        let mut write_frame = WriteFrame::new(client_write, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        // The large message cannot be coalesced: the pending small one is
        // flushed first, then the large one goes out through writev, so the
        // order is still preserved.
        let small1 = Builder::new_rpc(1i8).build();
        let large = Builder::new_rpc(2i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(
                WriteFrame::MAX_COALESCE + 1024,
            )))
            .build();
        let small2 = Builder::new_rpc(3i8).build();

        let msgs = [small1, large, small2];
        write_frame.send_batch(&msgs).await.unwrap();

        let m1 = read_frame.receive().await.unwrap();
        let m2 = read_frame.receive().await.unwrap();
        let m3 = read_frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);
        assert_eq!(m2.protocol.code, 2);
        assert_eq!(m2.data.len(), WriteFrame::MAX_COALESCE + 1024);
        assert_eq!(m3.protocol.code, 3);
    }

    #[tokio::test]
    async fn send_batch_small_messages_coalesced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (_, client_write) = tokio::io::split(client);
        let (server_read, _) = tokio::io::split(server_stream);
        let mut write_frame = WriteFrame::new(client_write, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        // Ten small messages stay below the buffer bound and are written with
        // a single call.
        let msgs: Vec<Message> = (0..10)
            .map(|i| {
                Builder::new_rpc(i as i8)
                    .data(DataSlice::Buffer(BytesMut::zeroed(64)))
                    .build()
            })
            .collect();
        write_frame.send_batch(&msgs).await.unwrap();

        for (i, _) in msgs.iter().enumerate() {
            let got = read_frame.receive().await.unwrap();
            assert_eq!(got.protocol.code, i as i8);
            assert_eq!(got.data.len(), 64);
        }
    }

    #[tokio::test]
    async fn send_batch_leading_large_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (_, client_write) = tokio::io::split(client);
        let (server_read, _) = tokio::io::split(server_stream);
        let mut write_frame = WriteFrame::new(client_write, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        // The large message comes first, so its writev goes out with an empty
        // buffer and the following small message is coalesced behind it.
        let large = Builder::new_rpc(1i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(
                WriteFrame::MAX_COALESCE + 1,
            )))
            .build();
        let small = Builder::new_rpc(2i8).build();

        let msgs = [large, small];
        write_frame.send_batch(&msgs).await.unwrap();

        let m1 = read_frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);
        assert_eq!(m1.data.len(), WriteFrame::MAX_COALESCE + 1);
        let m2 = read_frame.receive().await.unwrap();
        assert_eq!(m2.protocol.code, 2);
        assert_eq!(m2.data.len(), 0);
    }

    #[tokio::test]
    async fn send_batch_flushes_when_buffer_reaches_the_bound() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (_, client_write) = tokio::io::split(client);
        let (server_read, _) = tokio::io::split(server_stream);
        let mut write_frame = WriteFrame::new(client_write, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        // Six 3 KiB messages cross the 16 KiB bound mid-batch, so the buffer is
        // flushed before the batch ends; order and payloads must survive it.
        let msgs: Vec<Message> = (0..6)
            .map(|i| {
                Builder::new_rpc(i as i8)
                    .data(DataSlice::Buffer(BytesMut::zeroed(3 * 1024)))
                    .build()
            })
            .collect();
        write_frame.send_batch(&msgs).await.unwrap();

        for (i, _) in msgs.iter().enumerate() {
            let got = read_frame.receive().await.unwrap();
            assert_eq!(got.protocol.code, i as i8);
            assert_eq!(got.data.len(), 3 * 1024);
        }
    }

    #[tokio::test]
    async fn send_large_data_without_header() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (_, client_write) = tokio::io::split(client);
        let (server_read, _) = tokio::io::split(server_stream);
        let mut write_frame = WriteFrame::new(client_write, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        // Empty middle segment: the writev window holds head, an empty header
        // slice and the payload, which advance_slices must shrink correctly.
        let msg = Builder::new_rpc(9i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(
                WriteFrame::MAX_COALESCE + 1,
            )))
            .build();
        write_frame.send(&msg).await.unwrap();

        let got = read_frame.receive().await.unwrap();
        assert_eq!(got.protocol.code, 9);
        assert_eq!(got.header_len(), 0);
        assert_eq!(got.data.len(), WriteFrame::MAX_COALESCE + 1);
    }

    #[tokio::test]
    async fn send_large_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (_, client_write) = tokio::io::split(client);
        let (server_read, _) = tokio::io::split(server_stream);
        let mut write_frame = WriteFrame::new(client_write, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        // Both sections non-empty and the frame over the coalescing bound,
        // so all three writev segments are exercised.
        let msg = Builder::new_rpc(7i8)
            .header(BytesMut::zeroed(WriteFrame::MAX_COALESCE + 1))
            .data(DataSlice::Buffer(BytesMut::zeroed(1024)))
            .build();
        write_frame.send(&msg).await.unwrap();

        let got = read_frame.receive().await.unwrap();
        assert_eq!(got.protocol.code, 7);
        assert_eq!(got.header_len(), WriteFrame::MAX_COALESCE + 1);
        assert_eq!(got.data.len(), 1024);

        // Header-only frame: the empty payload segment must not end the
        // writev loop on a zero-length window.
        let head_only = Builder::new_rpc(8i8)
            .header(BytesMut::zeroed(WriteFrame::MAX_COALESCE + 1))
            .build();
        write_frame.send(&head_only).await.unwrap();

        let got = read_frame.receive().await.unwrap();
        assert_eq!(got.protocol.code, 8);
        assert_eq!(got.header_len(), WriteFrame::MAX_COALESCE + 1);
        assert_eq!(got.data.len(), 0);
    }
}
