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

use crate::handler::{Frame, FrameBuf, ReadFrame, WriteFrame};
use crate::message::{Message, Protocol, RefMessage, PROTOCOL_USIZE};
use crate::IOUtils;
use crate::ServerConf;
use bytes::BytesMut;
use curvine_io::{DataSlice, IOResult};
use curvine_net::net::ConnState;
use curvine_sys as sys;
use curvine_sys::RawIOSlice;
use std::io::IoSlice;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::io::{AsyncReadExt, Interest};
use tokio::net::TcpStream;

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};

pub enum FrameSate {
    Head,
    Data(Protocol, i32, i32),
}

/// Custom data frame resolution
pub struct RpcFrame {
    io: TcpStream,
    buf: FrameBuf,
    enable_splice: bool,
}

impl RpcFrame {
    fn new(io: TcpStream, buffer_size: usize, enable_splice: bool) -> Self {
        let enable_splice = if cfg!(target_os = "linux") {
            enable_splice
        } else {
            false
        };

        RpcFrame {
            io,
            buf: FrameBuf::new(buffer_size),
            enable_splice,
        }
    }

    pub fn with_client(io: TcpStream, buffer_size: usize) -> Self {
        Self::new(io, buffer_size, false)
    }

    pub fn with_server(io: TcpStream, conf: &ServerConf) -> Self {
        Self::new(io, conf.buffer_size, conf.enable_splice)
    }

    // Read data of the specified length.
    pub(crate) async fn read_full(&mut self, len: i32) -> IOResult<BytesMut> {
        if len == 0 {
            return Ok(BytesMut::new());
        } else if len < 0 {
            return err_box!("Invalid length {}", len);
        }

        let mut buf = self.buf.take_exact(len as usize);
        self.io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn read_data(&mut self, len: i32) -> IOResult<DataSlice> {
        if len <= 0 {
            return Ok(DataSlice::Empty);
        }

        if self.enable_splice {
            // Pass the file descriptor and read the data from the specific service
            let raw_io = sys::get_raw_io(&self.io)?;
            let slice = RawIOSlice::new(raw_io, None, len as usize);
            Ok(DataSlice::IOSlice(slice))
        } else {
            // Read data from the network to the buffer.
            let buf = self.read_full(len).await?;
            Ok(DataSlice::Buffer(buf))
        }
    }

    async fn receive_spliced(&mut self) -> IOResult<Message> {
        let mut partial = false;
        loop {
            let (protocol, header_size, data_size) =
                match IOUtils::read_head(&mut self.io, &mut self.buf, &mut partial).await? {
                    FrameSate::Head => return Ok(Message::empty()),
                    FrameSate::Data(protocol, header_size, data_size) => {
                        (protocol, header_size, data_size)
                    }
                };

            let header = if header_size > 0 {
                Some(self.read_full(header_size).await?)
            } else {
                None
            };
            let data = self.read_data(data_size).await?;
            let mut msg = Message::new(protocol, header, data);

            if msg.is_heartbeat() {
                continue;
            }
            msg.set_time();
            return Ok(msg);
        }
    }

    async fn send_file_full(
        &self,
        fd_in: sys::RawIO,
        mut off: Option<i64>,
        len: usize,
    ) -> IOResult<()> {
        let fd_out = sys::get_raw_io(self)?;
        let mut remaining = len;
        while remaining > 0 {
            let result = self
                .async_write(|| {
                    sys::send_file(fd_in, fd_out, off.as_mut(), remaining).map_err(Into::into)
                })
                .await;

            match result {
                Ok(0) => return err_box!("send_file returned 0"),
                Ok(transferred) => remaining -= transferred as usize,
                Err(error) if error.is_would_block() => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub fn io(&self) -> &TcpStream {
        &self.io
    }

    pub fn io_mut(&mut self) -> &mut TcpStream {
        &mut self.io
    }

    pub async fn readable(&self) -> IOResult<()> {
        self.io.readable().await?;
        Ok(())
    }

    pub async fn writable(&self) -> IOResult<()> {
        self.io.writable().await?;
        Ok(())
    }

    // Tokio's clear_readiness method cannot be called externally.
    // When using the readable configuration of the splice API, a large number of CPU idles will occur.Readable is available, but executing splice still returns a WouldBlock error.
    // readable should be used with try_io. When try_io returns a WouldBlock error, it will clean up the ready state.
    pub fn try_io<R>(&self, interest: Interest, f: impl FnOnce() -> IOResult<R>) -> IOResult<R> {
        let res = self.io.try_io(interest, || match f() {
            Ok(res) => Ok(res),
            Err(e) => Err(e.into_raw()),
        })?;

        Ok(res)
    }

    pub fn try_io_write<R>(&self, f: impl FnOnce() -> IOResult<R>) -> IOResult<R> {
        self.try_io(Interest::WRITABLE, f)
    }

    pub fn try_io_read<R>(&self, f: impl FnOnce() -> IOResult<R>) -> IOResult<R> {
        self.try_io(Interest::READABLE, f)
    }

    pub async fn async_io<R>(
        &self,
        interest: Interest,
        mut f: impl FnMut() -> IOResult<R>,
    ) -> IOResult<R> {
        let res = self
            .io
            .async_io(interest, || match f() {
                Ok(res) => Ok(res),
                Err(e) => Err(e.into_raw()),
            })
            .await?;

        Ok(res)
    }

    pub async fn async_read<R>(&self, f: impl FnMut() -> IOResult<R>) -> IOResult<R> {
        self.async_io(Interest::READABLE, f).await
    }

    pub async fn async_write<R>(&self, f: impl FnMut() -> IOResult<R>) -> IOResult<R> {
        self.async_io(Interest::WRITABLE, f).await
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for RpcFrame {
    fn as_raw_fd(&self) -> RawFd {
        self.io.as_raw_fd()
    }
}

impl Frame for RpcFrame {
    async fn send(&mut self, msg: impl RefMessage) -> IOResult<()> {
        let msg = msg.as_ref();
        let mut head = [0u8; PROTOCOL_USIZE];
        msg.encode_protocol(&mut head[..]);
        let header = msg.header_bytes().unwrap_or(&[]);

        match &msg.data {
            DataSlice::IOSlice(slice) => {
                IOUtils::write_iov(&mut self.io, [IoSlice::new(&head), IoSlice::new(header)])
                    .await?;
                self.send_file_full(slice.raw_io(), slice.off(), slice.len())
                    .await
            }

            data => {
                IOUtils::write_iov(
                    &mut self.io,
                    [
                        IoSlice::new(&head),
                        IoSlice::new(header),
                        IoSlice::new(data.as_slice()),
                    ],
                )
                .await
            }
        }
    }

    async fn receive(&mut self) -> IOResult<Message> {
        if self.enable_splice {
            self.receive_spliced().await
        } else {
            IOUtils::receive_buffered(&mut self.io, &mut self.buf).await
        }
    }

    fn new_conn_state(&self) -> ConnState {
        let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
        let client_addr = self.io.peer_addr().unwrap_or(ip);
        let local_addr = self.io.local_addr().unwrap_or(ip);
        ConnState::new(client_addr.into(), local_addr.into())
    }

    fn split(self) -> (ReadFrame, WriteFrame) {
        let (read, write) = tokio::io::split(self.io);
        let write_buf = self.buf.clone();
        let read_frame = ReadFrame::new(read, self.buf);
        let write_frame = WriteFrame::new(write, write_buf);
        (read_frame, write_frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Builder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
    use tokio::net::TcpListener;

    async fn setup_pair() -> (RpcFrame, ReadHalf<TcpStream>, WriteHalf<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();
        let (peer_read, peer_write) = tokio::io::split(client);
        let frame = RpcFrame::with_client(server_stream, 4096);
        (frame, peer_read, peer_write)
    }

    #[tokio::test]
    async fn receive_decodes_coalesced_frames_from_one_write() {
        let (mut frame, _peer_read, mut peer_write) = setup_pair().await;

        let mut frames = BytesMut::new();
        Builder::new_rpc(1i8).build().encode(&mut frames).unwrap();
        Builder::new_rpc(2i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(64)))
            .build()
            .encode(&mut frames)
            .unwrap();
        peer_write.write_all(&frames).await.unwrap();

        let m1 = frame.receive().await.unwrap();
        let m2 = frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);
        assert_eq!(m2.protocol.code, 2);
        assert_eq!(m2.data.len(), 64);
    }

    #[tokio::test]
    async fn send_does_not_flush_buffered_read_ahead() {
        let (mut frame, mut peer_read, mut peer_write) = setup_pair().await;

        let mut frames = BytesMut::new();
        Builder::new_rpc(1i8).build().encode(&mut frames).unwrap();
        Builder::new_rpc(2i8).build().encode(&mut frames).unwrap();
        peer_write.write_all(&frames).await.unwrap();

        let m1 = frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);

        // The second request is still sitting in the read buffer, so send must
        // not reuse that buffer for the protocol head or it would flush the
        // buffered request bytes back to the peer.
        frame.send(Builder::new_rpc(9i8).build()).await.unwrap();

        let mut head = [0u8; PROTOCOL_USIZE];
        peer_read.read_exact(&mut head).await.unwrap();

        // The peer keeps the connection open, so read_to_end could only report
        // the timeout and would hide a leak: one leaked byte must surface as a
        // successful read before the deadline.
        let mut extra = [0u8; 1];
        let read = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            peer_read.read(&mut extra),
        )
        .await;
        assert!(
            read.is_err(),
            "send leaked buffered read-ahead byte {:?}: {:?}",
            extra[0],
            read
        );
    }

    #[tokio::test]
    async fn send_writes_head_header_and_data_in_one_call() {
        let (mut frame, peer_read, _peer_write) = setup_pair().await;
        let mut read_frame = ReadFrame::new(peer_read, FrameBuf::new(0));

        let msg = Builder::new_rpc(7i8)
            .header(BytesMut::zeroed(1024))
            .data(DataSlice::Buffer(BytesMut::zeroed(4096)))
            .build();
        frame.send(msg).await.unwrap();

        let got = read_frame.receive().await.unwrap();
        assert_eq!(got.protocol.code, 7);
        assert_eq!(got.header_len(), 1024);
        assert_eq!(got.data.len(), 4096);

        // Header-only frame: the empty data segment must not stall the writev loop.
        let head_only = Builder::new_rpc(8i8).header(BytesMut::zeroed(256)).build();
        frame.send(head_only).await.unwrap();

        let got = read_frame.receive().await.unwrap();
        assert_eq!(got.protocol.code, 8);
        assert_eq!(got.header_len(), 256);
        assert_eq!(got.data.len(), 0);
    }

    #[tokio::test]
    async fn split_keeps_buffered_read_ahead() {
        let (mut frame, _peer_read, mut peer_write) = setup_pair().await;

        let mut frames = BytesMut::new();
        Builder::new_rpc(1i8).build().encode(&mut frames).unwrap();
        Builder::new_rpc(2i8)
            .data(DataSlice::Buffer(BytesMut::zeroed(64)))
            .build()
            .encode(&mut frames)
            .unwrap();
        peer_write.write_all(&frames).await.unwrap();

        let m1 = frame.receive().await.unwrap();
        assert_eq!(m1.protocol.code, 1);

        // The second frame is already buffered; split must hand it to the read
        // half instead of dropping it.
        let (mut read_frame, _write_frame) = frame.split();

        let m2 = read_frame.receive().await.unwrap();
        assert_eq!(m2.protocol.code, 2);
        assert_eq!(m2.data.len(), 64);
    }

    #[tokio::test]
    async fn receive_spliced_decodes_header_only_frame() {
        let (mut frame, _peer_read, mut peer_write) = setup_pair().await;
        frame.enable_splice = true;

        let mut wire = BytesMut::new();
        Builder::new_rpc(5i8)
            .header(BytesMut::zeroed(16))
            .build()
            .encode(&mut wire)
            .unwrap();
        peer_write.write_all(&wire).await.unwrap();

        let got = frame.receive().await.unwrap();
        assert_eq!(got.protocol.code, 5);
        assert_eq!(got.header_len(), 16);
        assert_eq!(got.data.len(), 0);
        assert!(
            got.time.is_some(),
            "the spliced path must stamp receipt time"
        );
    }
}
