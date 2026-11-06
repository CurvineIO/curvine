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

use std::time::Duration;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

use crate::handler::FrameBuf;
use crate::message::Message;
use crate::IOUtils;
use curvine_io::IOResult;

pub struct WriteFrame {
    io: WriteHalf<TcpStream>,
    buf: FrameBuf,
}

impl WriteFrame {
    // Protocol + header + data up to this size go out in one write; larger
    // payloads keep the streaming path to avoid an extra userspace copy.
    const MAX_COALESCE: usize = 64 * 1024;

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
        let mut start = self.buf.len();
        msg.encode_protocol(&mut self.buf);

        // Coalescing bound applies to header and data alike: streaming an
        // oversized section straight from the message avoids a full extra
        // memcpy and keeps the connection's write buffer from pinning the
        // largest header ever sent (master responses are header-only, e.g.
        // big ListStatus replies).
        if let Some(h) = &msg.header {
            if h.len() <= Self::MAX_COALESCE {
                self.buf.extend_from_slice(h);
            } else {
                if self.buf.len() > start {
                    self.io.write_all(&self.buf.split()).await?;
                }
                self.io.write_all(h).await?;
                start = self.buf.len();
            }
        }

        let slice = msg.data.as_slice();
        if !slice.is_empty() {
            if slice.len() <= Self::MAX_COALESCE {
                self.buf.extend_from_slice(slice);
            } else {
                if self.buf.len() > start {
                    self.io.write_all(&self.buf.split()).await?;
                }
                self.io.write_all(slice).await?;
                start = self.buf.len();
            }
        }

        if self.buf.len() > start {
            self.io.write_all(&self.buf.split()).await?;
        }

        Ok(())
    }

    /// Writes multiple messages with a single syscall. Falls back to
    /// per-message sends when any message carries a payload above
    /// [`Self::MAX_COALESCE`]; metadata RPCs (the hot path) never do.
    pub async fn send_batch(&mut self, msgs: &[&Message]) -> IOResult<()> {
        if msgs.len() <= 1 {
            return match msgs.first() {
                None => Ok(()),
                Some(msg) => self.send(msg).await,
            };
        }

        let before = self.buf.len();
        for (i, msg) in msgs.iter().enumerate() {
            let slice = msg.data.as_slice();
            let header_len = msg.header.as_ref().map_or(0, |h| h.len());
            if header_len + slice.len() > Self::MAX_COALESCE {
                // Flush the coalesced prefix, then stream the rest (including
                // the oversized message) with the per-message path.
                if self.buf.len() > before {
                    self.io.write_all(&self.buf.split()).await?;
                }
                for msg in &msgs[i..] {
                    self.send(msg).await?;
                }
                return Ok(());
            }

            msg.encode_protocol(&mut self.buf);
            if let Some(h) = &msg.header {
                self.buf.extend_from_slice(h);
            }
            if !slice.is_empty() {
                self.buf.extend_from_slice(slice);
            }
        }

        if self.buf.len() > before {
            self.io.write_all(&self.buf.split()).await?;
        }

        Ok(())
    }

    pub async fn send_timeout(&mut self, msg: &Message, dur: Duration) -> IOResult<()> {
        IOUtils::timeout_io("frame send", dur, self.send(msg)).await
    }

    pub async fn send_batch_timeout(&mut self, msgs: &[&Message], dur: Duration) -> IOResult<()> {
        IOUtils::timeout_io("frame send", dur, self.send_batch(msgs)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::ReadFrame;
    use crate::message::Builder;
    use bytes::BytesMut;
    use curvine_io::DataSlice;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn send_batch_falls_back_for_large_payloads() {
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

        let small1 = Builder::new_rpc(1i8).build();
        let large = Builder::new_rpc(2i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(
                WriteFrame::MAX_COALESCE + 1024,
            )))
            .build();
        let small2 = Builder::new_rpc(3i8).build();

        let msgs = vec![&small1, &large, &small2];
        write_frame.send_batch(&msgs).await.unwrap();

        let m1 = read_frame.receive().await.unwrap();
        let m2 = read_frame.receive().await.unwrap();
        let m3 = read_frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);
        assert_eq!(m2.protocol.code, 2);
        assert_eq!(m2.data.len(), WriteFrame::MAX_COALESCE + 1024);
        assert_eq!(m3.protocol.code, 3);
    }
}
