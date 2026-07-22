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

use crate::io::{IOError, IOResult};
use crate::message::RefMessage;
use crate::message::{Message, MAX_DATE_SIZE};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// RPC message codec.
/// Usually used on the customer service side.
/// It cannot use the operating system's underlying API, and the data will be copied from the network buffer to BytesMut.
pub struct RpcCodec {
    inner: LengthDelimitedCodec,
}

impl RpcCodec {
    pub fn new() -> RpcCodec {
        let inner = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_DATE_SIZE as usize)
            .length_field_length(4)
            .new_codec();

        RpcCodec { inner }
    }
}

impl<T: RefMessage> Encoder<T> for RpcCodec {
    type Error = IOError;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> IOResult<()> {
        item.encode(dst)
    }
}

impl Decoder for RpcCodec {
    type Item = Message;
    type Error = IOError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let res = self.inner.decode(src)?;
        match res {
            Some(mut data) => {
                let msg = Message::decode(&mut data)?;
                Ok(Some(msg))
            }

            _ => Ok(None),
        }
    }
}

impl Default for RpcCodec {
    fn default() -> Self {
        Self::new()
    }
}
