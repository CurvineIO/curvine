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

use bytes::BytesMut;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
use crate::io::IOResult;
use crate::message::Message;

pub struct WriteFrame {
    io: WriteHalf<TcpStream>,
    buf: BytesMut,
}

impl WriteFrame {
    pub(crate) fn new(io: WriteHalf<TcpStream>, buf: BytesMut) -> Self {
        Self { io, buf }
    }

    pub async fn send(&mut self, msg: &Message) -> IOResult<()> {
        msg.encode_protocol(&mut self.buf);
        self.io.write_all(&self.buf.split()).await?;

        // message header part
        if let Some(h) = &msg.header {
            self.io.write_all(h).await?;
        }

        // message data section.
        let slice = msg.data.as_slice();
        if !slice.is_empty() {
            self.io.write_all( slice).await?;
        }
        self.io.flush().await?;
        Ok(())
    }
}
