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

use crate::error::{ErrorImpl, StringError};
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
    pub fn new(mut bytes: BytesMut) -> Self {
        let kind = bytes.get_i32();

        // Read the error message.
        let msg_len = bytes.get_u32() as usize;
        let error_msg = if msg_len > 0 {
            let msg_buf = bytes.split_to(msg_len);
            String::from_utf8_lossy(&msg_buf).to_string()
        } else {
            "".to_string()
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
