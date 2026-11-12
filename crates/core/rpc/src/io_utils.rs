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

use crate::handler::{FrameBuf, FrameSate};
use crate::message::{self, Message};
use bytes::Buf;
use curvine_io::{DataSlice, IOError, IOResult};
use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::ops::DerefMut;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

pub struct IOUtils;

impl IOUtils {
    pub async fn timeout_io<T, F>(op: &str, dur: Duration, fut: F) -> IOResult<T>
    where
        F: Future<Output = IOResult<T>>,
    {
        timeout(dur, fut).await.unwrap_or_else(|_| {
            let err = io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{op} timed out after {dur:?}"),
            );
            Err(IOError::new(err))
        })
    }

    pub fn is_timeout(err: &IOError) -> bool {
        err.kind() == io::ErrorKind::TimedOut
    }

    pub(crate) async fn write_iov<W: AsyncWrite + Unpin, const N: usize>(
        io: &mut W,
        mut bufs: [IoSlice<'_>; N],
    ) -> IOResult<()> {
        // Drop trailing empty segments so the loop cannot end on a window of
        // empty slices, which the default write_vectored would report as 0.
        let end = bufs
            .iter()
            .rposition(|b| !b.is_empty())
            .map_or(0, |i| i + 1);
        let mut window: &mut [IoSlice<'_>] = &mut bufs[..end];
        IoSlice::advance_slices(&mut window, 0);
        while !window.is_empty() {
            let n = io.write_vectored(window).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write frame: write returned 0",
                )
                .into());
            }
            IoSlice::advance_slices(&mut window, n);
        }
        Ok(())
    }

    pub(crate) async fn read_head<R: AsyncRead + Unpin>(
        io: &mut R,
        buf: &mut FrameBuf,
        partial: &mut bool,
    ) -> IOResult<FrameSate> {
        let head_len = message::PROTOCOL_SIZE as usize;
        let mut head = buf.take_exact(head_len);
        let mut filled = 0;
        while filled < head_len {
            match io.read(&mut head[filled..]).await {
                Ok(0) if filled == 0 => return Ok(FrameSate::Head),
                Ok(0) => return Err(Self::eof_error("frame head", filled)),
                Ok(n) => {
                    filled += n;
                    if filled < head_len {
                        *partial = true;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        let (protocol, header_size, data_size) = Message::decode_protocol(&mut head)?;
        Ok(FrameSate::Data(protocol, header_size, data_size))
    }

    pub(crate) async fn receive_buffered<R: AsyncRead + Unpin>(
        io: &mut R,
        buf: &mut FrameBuf,
    ) -> IOResult<Message> {
        loop {
            if buf.len() < message::PROTOCOL_USIZE {
                if !Self::fill(io, buf, message::PROTOCOL_USIZE).await? {
                    if buf.is_empty() {
                        return Ok(Message::empty());
                    }
                    return Err(Self::eof_error("frame head", buf.len()));
                }
                continue;
            }

            let (protocol, header_size, data_size) = {
                let mut head = &buf[..];
                Message::decode_protocol(&mut head)?
            };
            let header_size = header_size as usize;
            let data_size = data_size as usize;
            let need = message::PROTOCOL_USIZE + header_size + data_size;

            while buf.len() < need {
                if !Self::fill(io, buf, need).await? {
                    return Err(Self::eof_error("frame body", buf.len()));
                }
            }

            if protocol.is_heartbeat() {
                buf.advance(need);
                buf.maybe_shrink();
                continue;
            }

            buf.advance(message::PROTOCOL_USIZE);
            let header = if header_size > 0 {
                Some(buf.split_to(header_size))
            } else {
                None
            };
            let data = if data_size > 0 {
                DataSlice::Buffer(buf.split_to(data_size))
            } else {
                DataSlice::Empty
            };
            let mut msg = Message::new(protocol, header, data);
            buf.maybe_shrink();
            msg.set_time();
            return Ok(msg);
        }
    }

    async fn fill<R: AsyncRead + Unpin>(
        io: &mut R,
        buf: &mut FrameBuf,
        need: usize,
    ) -> IOResult<bool> {
        let shortfall = need
            .saturating_sub(buf.len())
            .max(FrameBuf::DEFAULT_BUF_SIZE);
        buf.reserve(shortfall.max(1));
        Ok(io.read_buf(buf.deref_mut()).await? > 0)
    }

    fn eof_error(what: &str, buffered: usize) -> IOError {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("connection closed mid-{what} with {buffered} buffered bytes"),
        )
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::IOUtils;
    use std::io;
    use std::io::IoSlice;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    /// Accepts at most `limit` bytes per vectored write, so every call returns a
    /// partial result and the loop has to advance its window across calls.
    struct Trickle {
        written: Vec<u8>,
        limit: usize,
    }

    impl AsyncWrite for Trickle {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let me = self.get_mut();
            let n = buf.len().min(me.limit);
            me.written.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let me = self.get_mut();
            let mut left = me.limit;
            let mut n = 0;
            for b in bufs {
                if left == 0 {
                    break;
                }
                let take = b.len().min(left);
                me.written.extend_from_slice(&b[..take]);
                n += take;
                left -= take;
            }
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Never accepts a byte.
    struct Stuck;

    impl AsyncWrite for Stuck {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn write_iov_advances_through_partial_writes() {
        let head = [1u8; 5];
        let header = [2u8; 3];
        let data = [3u8; 7];
        let mut io = Trickle {
            written: Vec::new(),
            limit: 4,
        };

        IOUtils::write_iov(
            &mut io,
            [
                IoSlice::new(&head),
                IoSlice::new(&header),
                IoSlice::new(&data),
            ],
        )
        .await
        .unwrap();

        let expected: Vec<u8> = head
            .iter()
            .chain(header.iter())
            .chain(data.iter())
            .copied()
            .collect();
        assert_eq!(io.written, expected);
    }

    #[tokio::test]
    async fn write_iov_ignores_empty_segments() {
        let head = [7u8; 4];

        // Empty header and data: the window is the head alone.
        let mut io = Trickle {
            written: Vec::new(),
            limit: 3,
        };
        IOUtils::write_iov(
            &mut io,
            [IoSlice::new(&head), IoSlice::new(&[]), IoSlice::new(&[])],
        )
        .await
        .unwrap();
        assert_eq!(io.written, head.to_vec());

        // A window of empty segments only must complete, not report zero progress.
        let mut io = Trickle {
            written: Vec::new(),
            limit: 3,
        };
        IOUtils::write_iov(&mut io, [IoSlice::new(&[]), IoSlice::new(&[])])
            .await
            .unwrap();
        assert!(io.written.is_empty());
    }

    #[tokio::test]
    async fn write_iov_reports_zero_progress_as_write_zero() {
        let data = [9u8; 8];
        let err = IOUtils::write_iov(&mut Stuck, [IoSlice::new(&data)])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }

    #[tokio::test]
    async fn receive_coalesces_small_frames_into_few_reads() {
        use crate::handler::FrameBuf;
        use crate::message::{HEAD_SIZE, PROTOCOL_USIZE};
        use bytes::BufMut;
        use std::io;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::{AsyncRead, ReadBuf};

        struct Scripted {
            data: Vec<u8>,
            pos: usize,
            reads: usize,
        }

        impl AsyncRead for Scripted {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                let me = self.get_mut();
                me.reads += 1;
                let n = buf.remaining().min(me.data.len() - me.pos);
                let start = me.pos;
                buf.put_slice(&me.data[start..start + n]);
                me.pos += n;
                Poll::Ready(Ok(()))
            }
        }

        fn frame(data_len: usize) -> Vec<u8> {
            let mut out = bytes::BytesMut::with_capacity(PROTOCOL_USIZE + data_len);
            out.put_i32((HEAD_SIZE as usize + data_len) as i32);
            out.put_i32(0);
            out.put_i8(1);
            out.put_i8(1);
            out.put_i64(1);
            out.put_i32(0);
            out.extend_from_slice(&vec![7u8; data_len]);
            out.to_vec()
        }

        let frames = 200;
        let mut io = Scripted {
            data: (0..frames).flat_map(|_| frame(200)).collect(),
            pos: 0,
            reads: 0,
        };
        let mut buf = FrameBuf::new(0);
        for _ in 0..frames {
            IOUtils::receive_buffered(&mut io, &mut buf).await.unwrap();
        }

        assert!(
            io.reads < frames / 8,
            "{frames} small frames took {} reads, the spare capacity is not kept at the 8 KiB floor",
            io.reads
        );
    }
}
