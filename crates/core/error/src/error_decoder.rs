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

use crate::{ErrorImpl, StringError};
use bytes::{Buf, BytesMut};
use log::warn;
use serde::de::DeserializeOwned;

// Error deserializer.
pub struct ErrorDecoder {
    pub bytes: BytesMut,
    pub kind: i32,
    pub source: StringError,
}

impl ErrorDecoder {
    /// Kind reported when the payload is not a framed error at all. `0` is not a
    /// valid `ErrorKind`, so callers fall through to their `Common` default.
    const UNKNOWN_KIND: i32 = 0;
    const TRUNCATED: &'static str = "malformed error response";

    /// Reads an error frame: `kind: i32`, `message_len: u32`, `message`.
    ///
    /// The peer is not guaranteed to answer with a frame: a response can be
    /// truncated by a size limit, or be a payload the caller misclassified as an
    /// error. That used to panic the client in `get_i32`/`split_to`, killing the
    /// process on a request that only failed to decode (`cv fs count` on a huge
    /// directory did exactly that). Decode what is there instead, and keep the
    /// rest of the bytes for the caller.
    pub fn new(mut bytes: BytesMut) -> Self {
        if bytes.remaining() < size_of::<i32>() {
            return Self {
                bytes,
                kind: Self::UNKNOWN_KIND,
                source: Self::TRUNCATED.into(),
            };
        }
        let kind = bytes.get_i32();

        // Read the error message.
        if bytes.remaining() < size_of::<u32>() {
            return Self {
                bytes,
                kind,
                source: Self::TRUNCATED.into(),
            };
        }
        let msg_len = bytes.get_u32() as usize;
        let available = bytes.remaining();
        let error_msg = if msg_len == 0 {
            String::new()
        } else if msg_len <= available {
            let msg_buf = bytes.split_to(msg_len);
            String::from_utf8_lossy(&msg_buf).to_string()
        } else {
            warn!(
                "error decode: message needs {} bytes but only {} are available",
                msg_len, available
            );
            let msg_buf = bytes.split_to(available);
            format!(
                "{} [truncated: {} of {} bytes]",
                String::from_utf8_lossy(&msg_buf),
                available,
                msg_len
            )
        };

        Self {
            bytes,
            kind,
            source: error_msg.into(),
        }
    }

    pub fn get_data<D: DeserializeOwned>(&mut self) -> Option<D> {
        // Read data
        if self.bytes.remaining() < 4 {
            return None;
        }

        let data_len = self.bytes.get_u32() as usize;
        if data_len == 0 {
            return None;
        }

        if self.bytes.remaining() < data_len {
            warn!(
                "error decode: not enough bytes for data, need {}, have {}",
                data_len,
                self.bytes.remaining()
            );
            return None;
        }

        let data_buf = self.bytes.split_to(data_len);
        match bincode::deserialize(&data_buf) {
            Ok(v) => Some(v),
            Err(e) => {
                warn!("error decode: data deserialize failed: {}", e);
                None
            }
        }
    }

    pub fn into_string<D: DeserializeOwned>(mut self) -> ErrorImpl<StringError, D> {
        let data = self.get_data();
        ErrorImpl::new(self.source, data)
    }

    pub fn into_io(self) -> ErrorImpl<std::io::Error> {
        let error = std::io::Error::other(self.source);
        ErrorImpl::with_source(error)
    }

    pub fn kind<T: From<i32>>(&self) -> T {
        T::from(self.kind)
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorDecoder;
    use bytes::BytesMut;

    fn frame(kind: i32, message: &[u8]) -> BytesMut {
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&kind.to_be_bytes());
        bytes.extend_from_slice(&(message.len() as u32).to_be_bytes());
        bytes.extend_from_slice(message);
        bytes
    }

    #[test]
    fn reads_a_well_formed_frame() {
        let decoder = ErrorDecoder::new(frame(8, b"missing"));
        assert_eq!(decoder.kind, 8);
        assert_eq!(decoder.source.to_string(), "missing");
    }

    #[test]
    fn a_response_too_short_for_a_kind_does_not_panic() {
        let decoder = ErrorDecoder::new(BytesMut::from(&b"ab"[..]));
        assert_eq!(decoder.kind, ErrorDecoder::UNKNOWN_KIND);
        assert!(!decoder.source.to_string().is_empty());
    }

    #[test]
    fn a_response_too_short_for_a_length_does_not_panic() {
        let mut bytes = BytesMut::from(&8i32.to_be_bytes()[..]);
        bytes.extend_from_slice(&[0, 0]);
        let decoder = ErrorDecoder::new(bytes);
        assert_eq!(decoder.kind, 8);
        assert!(!decoder.source.to_string().is_empty());
    }

    /// The live crash: a payload that is not an error frame at all (here plain
    /// text) made the decoder split past its buffer.
    #[test]
    fn a_non_error_payload_is_reported_instead_of_panicking() {
        let decoder = ErrorDecoder::new(BytesMut::from(&b"the latest(42k items)"[..]));
        assert!(
            decoder.source.to_string().contains("truncated"),
            "a misclassified payload must be reported, got: {}",
            decoder.source
        );
    }
}
