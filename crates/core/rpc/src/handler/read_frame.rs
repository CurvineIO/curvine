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
use std::ops::DerefMut;
use std::time::Duration;

use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::net::TcpStream;

use crate::handler::FrameBuf;
use crate::message;
use crate::message::{Message, Protocol};
use crate::IOUtils;
use curvine_io::{DataSlice, IOResult};

pub struct ReadFrame {
    io: ReadHalf<TcpStream>,
    buf: FrameBuf,
    /// Protocol head decoded but still waiting for its header/data sections;
    /// lets a frame span multiple socket reads without losing buffered bytes.
    pending: Option<(Protocol, i32, i32)>,
}

impl ReadFrame {
    pub(crate) fn new(io: ReadHalf<TcpStream>, buf: FrameBuf) -> Self {
        Self {
            io,
            buf,
            pending: None,
        }
    }

    /// Receives the next complete message. Bytes read from the socket are
    /// buffered, so one syscall can satisfy several back-to-back messages
    /// instead of the per-frame `read_exact` calls of the exact-read path.
    pub async fn receive(&mut self) -> IOResult<Message> {
        loop {
            match self.pending.take() {
                Some((protocol, header_size, data_size)) => {
                    // Both are validated non-negative i32; summing in usize
                    // cannot wrap, unlike the i32 add.
                    let need = header_size as usize + data_size as usize;
                    if self.buf.len() < need {
                        self.pending = Some((protocol, header_size, data_size));
                    } else {
                        let header = if header_size > 0 {
                            Some(self.buf.split_to(header_size as usize))
                        } else {
                            None
                        };

                        let data = if data_size > 0 {
                            DataSlice::Buffer(self.buf.split_to(data_size as usize))
                        } else {
                            DataSlice::Empty
                        };
                        let mut msg = Message::new(protocol, header, data);
                        self.buf.maybe_shrink();

                        // Heartbeat message.
                        if msg.is_heartbeat() {
                            continue;
                        } else {
                            msg.set_time();
                            return Ok(msg);
                        }
                    }
                }

                None => {
                    if self.buf.len() >= message::PROTOCOL_SIZE as usize {
                        let (protocol, header_size, data_size) =
                            Message::decode_protocol(self.buf.deref_mut())?;
                        // decode_protocol only bounds data_size; header_size is
                        // unbounded (large header-only responses such as
                        // ListStatus are legitimate). A negative header_size
                        // would wrap the usize fill target computed below into
                        // something the buffer can never reach, stalling the
                        // connection while it grows without bound, so reject it
                        // here. With header_size >= 0 the usize sum cannot wrap.
                        if header_size < 0 {
                            return err_box!(
                                "invalid frame head: negative header_size={}",
                                header_size
                            );
                        }
                        self.pending = Some((protocol, header_size, data_size));
                        continue;
                    }
                }
            }

            // Need more bytes from the socket. Reserve the exact shortfall so
            // read_buf never reports a zero-byte read (mistaken for EOF) and
            // does not grow the buffer one read at a time.
            let want = match self.pending {
                Some((_, header_size, data_size)) => {
                    header_size as usize + data_size as usize - self.buf.len()
                }
                None => message::PROTOCOL_SIZE as usize - self.buf.len(),
            };
            self.buf.reserve(want.max(1));
            let n = self.io.read_buf(self.buf.deref_mut()).await?;
            if n == 0 {
                // A clean close (no buffered bytes) keeps the empty-message
                // contract; any buffered partial frame is a truncation and
                // must surface as UnexpectedEof so callers drop the
                // connection instead of treating it as a clean EOF.
                if self.pending.is_none() && self.buf.is_empty() {
                    return Ok(Message::empty());
                }
                let what = if self.pending.is_none() {
                    "frame head"
                } else {
                    "frame body"
                };
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "connection closed mid-{} with {} buffered bytes",
                        what,
                        self.buf.len()
                    ),
                )
                .into());
            }
        }
    }

    pub async fn receive_timeout(&mut self, dur: Duration) -> IOResult<Message> {
        let res = IOUtils::timeout_io("frame receive", dur, self.receive()).await;
        match res {
            // Any buffered but unconsumed bytes mean a frame is in progress,
            // so a timeout leaves the stream desynced and the connection must
            // be dropped instead of resuming it.
            Err(e)
                if IOUtils::is_timeout(&e) && (self.pending.is_some() || !self.buf.is_empty()) =>
            {
                Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!(
                        "frame receive timed out after {dur:?} in the middle of a frame, \
                     the stream is desynced and the connection must be dropped"
                    ),
                )
                .into())
            }

            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Builder;
    use bytes::{BufMut, BytesMut};
    use curvine_io::DataSlice;
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

    async fn setup_pair() -> (ReadFrame, tokio::io::WriteHalf<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        // Write on the client's write half, read on the server's read half.
        let (_, client_write) = tokio::io::split(client);
        let (read_half, _) = tokio::io::split(server_stream);
        let read_frame = ReadFrame::new(read_half, FrameBuf::new(0));
        (read_frame, client_write)
    }

    #[tokio::test]
    async fn receive_parses_coalesced_frames_from_one_write() {
        let (mut read_frame, mut client_write) = setup_pair().await;

        let mut frames = BytesMut::new();
        Builder::new_rpc(1i8).build().encode(&mut frames).unwrap();
        Builder::new_rpc(2i8).build().encode(&mut frames).unwrap();
        client_write.write_all(&frames).await.unwrap();

        let m1 = read_frame.receive().await.unwrap();
        let m2 = read_frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);
        assert_eq!(m2.protocol.code, 2);
    }

    #[tokio::test]
    async fn receive_assembles_frame_spanning_multiple_reads() {
        let (mut read_frame, mut client_write) = setup_pair().await;

        let mut frame = BytesMut::new();
        Builder::new_rpc(1i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(1024)))
            .build()
            .encode(&mut frame)
            .unwrap();

        let rest = frame.split_off(50);
        client_write.write_all(&frame[..]).await.unwrap();

        let writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            client_write.write_all(&rest).await.unwrap();
        });

        let msg = read_frame.receive().await.unwrap();
        writer.await.unwrap();
        assert_eq!(msg.protocol.code, 1);
        assert_eq!(msg.data.len(), 1024);
    }

    #[tokio::test]
    async fn receive_rejects_invalid_header_size() {
        let (mut read_frame, mut client_write) = setup_pair().await;

        // A corrupt head: negative header_size makes
        // (header_size + data_size) as usize wrap around, which must be
        // rejected instead of stalling the connection.
        let mut frame = BytesMut::new();
        frame.put_i32(22);
        frame.put_i32(-8);
        frame.put_i8(0);
        frame.put_i8(0);
        frame.put_i64(0);
        frame.put_i32(0);
        client_write.write_all(&frame).await.unwrap();

        let res = read_frame.receive().await;
        assert!(res.is_err(), "negative header_size must be rejected");
    }

    #[tokio::test]
    async fn receive_accepts_header_above_max_date_size() {
        // Master responses are header-only protobuf, and ListStatus is
        // unpaginated, so headers larger than MAX_DATE_SIZE (the data-section
        // cap) are legitimate and must not be rejected.
        let (mut read_frame, mut client_write) = setup_pair().await;

        let mut msg = Builder::new_rpc(1i8).build();
        msg.header = Some(BytesMut::zeroed(
            message::MAX_DATE_SIZE as usize + 1024 * 1024,
        ));
        let mut frame = BytesMut::new();
        msg.encode(&mut frame).unwrap();

        // Write concurrently: an 18 MiB frame overflows the socket buffers,
        // so the writer must make progress while receive() drains.
        let writer = tokio::spawn(async move {
            client_write.write_all(&frame).await.unwrap();
        });

        let got = read_frame.receive().await.unwrap();
        writer.await.unwrap();
        let header = got.header.expect("header must round-trip");
        assert_eq!(header.len(), message::MAX_DATE_SIZE as usize + 1024 * 1024);
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
