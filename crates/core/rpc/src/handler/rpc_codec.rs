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

use crate::message::RefMessage;
use crate::message::{Message, MAX_FRAME_SIZE};
use bytes::BytesMut;
use curvine_io::{IOError, IOResult};
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
            .max_frame_length(MAX_FRAME_SIZE as usize)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{HEAD_SIZE, MAX_DATE_SIZE};
    use bytes::BufMut;

    #[test]
    fn decode_rejects_frame_shorter_than_head() {
        let mut buf = BytesMut::from(&[0u8; 10][..]);
        assert!(Message::decode(&mut buf).is_err());
    }

    #[test]
    fn decode_rejects_header_beyond_frame_body() {
        let mut buf = BytesMut::new();
        buf.put_u32(64);
        buf.put_bytes(0, (HEAD_SIZE - 4) as usize);
        assert!(Message::decode(&mut buf).is_err());
    }

    #[test]
    fn decode_accepts_header_above_max_date_size() {
        // Large header-only responses such as ListStatus are legitimate: the
        // producer-side frame budget bounds them, not the data-section budget.
        let header = 17 * 1024 * 1024;
        let mut buf = BytesMut::new();
        buf.put_u32(header as u32);
        buf.put_bytes(0, (HEAD_SIZE - 4) as usize);
        buf.put_bytes(0, header);

        let msg = Message::decode(&mut buf).expect("a header above the data limit is legal");
        assert_eq!(msg.header_len(), header);
    }

    #[test]
    fn decode_rejects_data_section_above_max_date_size() {
        // Same frame the frame decoder rejects: no header, 17 MiB of data.
        let mut buf = BytesMut::new();
        buf.put_u32(0);
        buf.put_bytes(0, (HEAD_SIZE - 4) as usize);
        buf.put_bytes(0, MAX_DATE_SIZE as usize + 1024 * 1024);

        assert!(Message::decode(&mut buf).is_err());
    }
}
