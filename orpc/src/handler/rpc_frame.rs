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

use crate::handler::{ReadFrame, RpcCodec, WriteFrame};
use crate::io::net::ConnState;
use crate::io::IOResult;
use crate::message::{Message, Protocol, MAX_DATE_SIZE};
use crate::server::ServerConf;
use crate::sys::{DataSlice, RawIOSlice};
use crate::{err_box, message, sys};
use bytes::{Buf, BufMut, BytesMut};
use std::mem;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};

pub enum FrameSate {
    Head,
    Data(Protocol, i32, i32),
}

/// Custom data frame resolution
pub struct RpcFrame {
    io: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
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
            read_buf: BytesMut::with_capacity(buffer_size),
            write_buf: BytesMut::with_capacity(buffer_size),
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
    pub async fn read_full(&mut self, len: i32) -> IOResult<BytesMut> {
        self.read_buf.reserve(len as usize);
        unsafe {
            self.read_buf.set_len(len as usize);
        }
        let mut buf = self.read_buf.split_to(len as usize);
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
                    let data = self.read_data(data_size).await?;
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

    pub async fn send(&mut self, msg: &Message) -> IOResult<()> {
        let header_len = msg.header_len();
        let data_len = msg.data_len();

        // message protocol control block
        self.write_buf.put_i32((18 + header_len + data_len) as i32);
        self.write_buf.put_i32(header_len as i32);
        self.write_buf.put_i8(msg.code());
        self.write_buf.put_i8(msg.encode_status());
        self.write_buf.put_i64(msg.req_id());
        self.write_buf.put_i32(msg.seq_id());
        self.io.write_all(&self.write_buf.split()).await?;

        // message header part
        if let Some(h) = &msg.header {
            self.io.write_all(h).await?;
        }

        // message data section.
        self.write_region(&msg.data).await?;

        self.io.flush().await?;
        Ok(())
    }

    async fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        match region {
            DataSlice::Empty => Ok(()),

            DataSlice::Buffer(buf) => {
                self.io.write_all(buf).await?;
                Ok(())
            }

            DataSlice::IOSlice(io) => {
                sys::send_file_full(self, io.raw_io(), io.off(), io.len()).await?;
                Ok(())
            }

            DataSlice::MemSlice(buf) => {
                self.io.write_all(buf.as_slice()).await?;
                Ok(())
            }

            DataSlice::Bytes(buf) => {
                self.io.write_all(buf).await?;
                Ok(())
            }
        }
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

    pub fn into_tokio_frame(self) -> Framed<TcpStream, RpcCodec> {
        Framed::with_capacity(self.io, RpcCodec::new(), self.read_buf.capacity())
    }

    pub fn split(self) -> (ReadFrame, WriteFrame) {
        let (read, write) = tokio::io::split(self.io);
        let read_frame = ReadFrame::new(read, self.read_buf);
        let write_frame = WriteFrame::new(write, self.write_buf);
        (read_frame, write_frame)
    }

    pub fn new_conn_state(&self) -> ConnState {
        let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
        let client_addr = self.io.peer_addr().unwrap_or(ip);
        let local_addr = self.io.local_addr().unwrap_or(ip);
        ConnState::new(client_addr.into(), local_addr.into())
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for RpcFrame {
    fn as_raw_fd(&self) -> RawFd {
        self.io.as_raw_fd()
    }
}
