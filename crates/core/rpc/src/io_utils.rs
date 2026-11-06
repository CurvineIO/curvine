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
use curvine_io::{IOError, IOResult};
use std::future::Future;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
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
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "connection closed in the middle of a frame head: {}/{} bytes",
                            filled, head_len
                        ),
                    )
                    .into());
                }
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
}
