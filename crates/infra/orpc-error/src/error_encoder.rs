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

use crate::error::{ErrorExt, ErrorImpl};
use bytes::{BufMut, BytesMut};
use serde::Serialize;
use std::error::Error;

// Error serializer.
pub struct ErrorEncoder;

impl ErrorEncoder {
    pub fn encode<E, D>(kind: i32, e: &ErrorImpl<E, D>) -> BytesMut
    where
        E: Error,
        D: Serialize,
    {
        let mut bytes = BytesMut::new();
        bytes.put_i32(kind);

        // Write an error message.
        let error_msg = e.source.to_string();
        let error_bytes = error_msg.as_bytes();
        bytes.put_u32(error_bytes.len() as u32);
        bytes.put_slice(error_bytes);

        // No context is written.This is meaningless to the customer service side.
        // Write data information.
        match &e.data {
            None => bytes.put_u32(0),

            Some(v) => {
                let data = bincode::serialize(v).unwrap();
                bytes.put_u32(data.len() as u32);
                bytes.put_slice(&data)
            }
        }

        bytes
    }

    pub fn encode_ext<E: ErrorExt>(kind: i32, e: E) -> BytesMut {
        let mut bytes = BytesMut::new();
        bytes.put_i32(kind);
        bytes.extend_from_slice(&e.encode());
        bytes
    }
}
