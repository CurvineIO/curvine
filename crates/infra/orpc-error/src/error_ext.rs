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
use std::error::Error;

// Wrong extension interface
// Supports setting context, supporting serialization and deserialization.
pub trait ErrorExt: Error {
    fn ctx(self, ctx: impl Into<String>) -> Self;

    // Serialize errors to transmit.
    fn encode(&self) -> BytesMut;

    // Deserialization error.
    fn decode(byte: BytesMut) -> Self;

    // You can re-submit a request by determining whether the current error needs to be retryed.
    fn should_retry(&self) -> bool {
        false
    }

    // Whether the request should continue.
    // Retry multiple times on the network and return an error at will. This method feels whether to continue requesting.
    // For example, in a raft group, it indicates whether another node should be requested.
    fn should_continue(&self) -> bool {
        false
    }
}
