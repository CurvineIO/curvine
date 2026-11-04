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

use crate::message::{BoxMessage, Builder, RefMessage};
use bytes::BytesMut;
use curvine_core_error::{CommonError, CommonResult, ErrorExt};
use curvine_io::DataSlice;
use curvine_runtime::common::Utils;
use num_enum::{FromPrimitive, IntoPrimitive};
use prost::Message as PMessage;
use std::fmt::Debug;
use std::time::Instant;

pub const INIT_SEQ_ID: i32 = -1;

pub const END_SEQ_ID: i32 = -2;

pub const EMPTY_REQ_ID: i64 = -1;

pub const MAX_DATE_SIZE: i32 = 16 * 1024 * 1024;

#[repr(i8)]
#[derive(Debug, Copy, Clone, PartialEq, IntoPrimitive, FromPrimitive)]
pub enum RequestStatus {
    #[num_enum(default)]
    Undefined = -1,

    Heartbeat = 0,

    Rpc = 1,

    Open = 2,     //Streaming request initialization
    Running = 3,  //Streaming request data delivery
    Cancel = 4,   //Cancel request
    Complete = 5, //Request complete
}

#[repr(i8)]
#[derive(Debug, Copy, Clone, PartialEq, IntoPrimitive, FromPrimitive)]
pub enum ResponseStatus {
    #[num_enum(default)]
    Undefined = -1,

    Success = 0,
    Error = 1,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Status(pub RequestStatus, pub ResponseStatus);

impl Status {
    pub fn new_request(s: RequestStatus) -> Status {
        Status(s, ResponseStatus::Undefined)
    }
}

impl Default for Status {
    fn default() -> Self {
        Status(RequestStatus::Undefined, ResponseStatus::Undefined)
    }
}

/// Protocol control block.
/// code: Request code, usually means request type.
/// status: request or response status
/// req_id: the unique id requested
/// seq_id: the requested sequence number.
#[derive(Debug, Copy, Clone)]
pub struct Protocol {
    pub code: i8,
    pub status: Status,
    pub req_id: i64,
    pub seq_id: i32,
}

impl Protocol {
    pub fn new(code: i8, status: Status, seq_id: i32) -> Self {
        Protocol {
            code,
            status,
            req_id: Utils::req_id(),
            seq_id,
        }
    }
}

// Internal request and response shared by the gRPC adapters and handlers.
#[derive(Debug)]
pub struct RpcMessage {
    pub protocol: Protocol,
    pub header: Option<BytesMut>,
    pub data: DataSlice,
    /// Receipt time stamped by adapters that track request queue delay.
    /// Locally built messages leave it `None`; new read paths must preserve
    /// this invariant because the queue-delay metric and the audit `queueUs`
    /// field derive from it.
    pub time: Option<Instant>,
}

impl RpcMessage {
    pub fn new(protocol: Protocol, header: Option<BytesMut>, data: DataSlice) -> Self {
        Self {
            protocol,
            header,
            data,
            time: None,
        }
    }

    pub fn empty() -> Self {
        Builder::new().build()
    }

    pub fn heartbeat() -> Self {
        Builder::new().request(RequestStatus::Heartbeat).build()
    }

    // Whether it is an empty message.
    pub fn is_empty(&self) -> bool {
        self.request_status() == RequestStatus::Undefined
            && self.response_status() == ResponseStatus::Undefined
    }

    pub fn not_empty(&self) -> bool {
        !self.is_empty()
    }

    pub fn request(
        code: i8,
        status: RequestStatus,
        seq_id: i32,
        header: Option<BytesMut>,
        data: DataSlice,
    ) -> Self {
        let protocol = Protocol::new(code, Status::new_request(status), seq_id);
        Self::new(protocol, header, data)
    }

    pub fn success_with_data(&self, header: Option<BytesMut>, data: DataSlice) -> Self {
        let status = Status(self.request_status(), ResponseStatus::Success);
        self.response(status, header, data)
    }

    pub fn success_with_header(&self, header: BytesMut) -> Self {
        let status = Status(self.request_status(), ResponseStatus::Success);
        self.response(status, Some(header), DataSlice::Empty)
    }

    pub fn success(&self) -> Self {
        let status = Status(self.request_status(), ResponseStatus::Success);
        self.response(status, None, DataSlice::Empty)
    }

    pub fn error(&self, err: &CommonError) -> Self {
        let status = Status(self.request_status(), ResponseStatus::Error);
        let err_bytes = BytesMut::from(err.to_string().as_str());
        self.response(status, None, DataSlice::Buffer(err_bytes))
    }

    pub fn error_ext<T: ErrorExt>(&self, err: &T) -> Self {
        let status = Status(self.request_status(), ResponseStatus::Error);
        let err_bytes = err.encode();
        self.response(status, None, DataSlice::Buffer(err_bytes))
    }

    pub fn check_error_ext<E: ErrorExt>(&self) -> Result<(), E> {
        if self.is_success() {
            return Ok(());
        }

        let err_buf = BytesMut::from(self.data.as_slice());
        let err = E::decode(err_buf);
        Err(err)
    }

    fn response(&self, status: Status, header: Option<BytesMut>, data: DataSlice) -> Self {
        let protocol = Protocol {
            code: self.protocol.code,
            status,
            req_id: self.protocol.req_id,
            seq_id: self.protocol.seq_id,
        };

        Self::new(protocol, header, data)
    }

    pub fn header_len(&self) -> usize {
        match &self.header {
            Some(v) => v.len(),
            None => 0,
        }
    }

    pub fn header_bytes(&self) -> Option<&[u8]> {
        match &self.header {
            Some(v) => Some(v),
            None => None,
        }
    }

    pub fn req_id(&self) -> i64 {
        self.protocol.req_id
    }

    pub fn seq_id(&self) -> i32 {
        self.protocol.seq_id
    }

    pub fn code(&self) -> i8 {
        self.protocol.code
    }

    pub fn request_status(&self) -> RequestStatus {
        self.protocol.status.0
    }

    pub fn response_status(&self) -> ResponseStatus {
        self.protocol.status.1
    }

    pub fn data_len(&self) -> usize {
        self.data.len()
    }

    pub fn data_bytes(&self) -> Option<&[u8]> {
        match &self.data {
            DataSlice::Buffer(buf) => Some(buf),
            DataSlice::Bytes(buf) => Some(buf),
            _ => None,
        }
    }

    pub fn is_success(&self) -> bool {
        self.response_status() == ResponseStatus::Success
    }

    pub fn is_heartbeat(&self) -> bool {
        self.request_status() == RequestStatus::Heartbeat
    }

    pub fn to_error_msg(&self) -> String {
        self.data.to_error_msg()
    }

    pub fn data_string(&self) -> String {
        self.data.to_error_msg()
    }

    pub fn parse_header<T: PMessage + Default>(&self) -> CommonResult<T> {
        match &self.header {
            Some(bytes) => {
                let header = T::decode(&bytes[..])?;
                Ok(header)
            }
            None => Ok(T::default()),
        }
    }

    pub fn into_arc(self) -> BoxMessage {
        BoxMessage::arc(self)
    }

    pub fn set_time(&mut self) {
        self.time = Some(Instant::now());
    }
}

impl RefMessage for RpcMessage {
    fn as_ref(&self) -> &Self {
        self
    }

    fn as_mut(&mut self) -> &mut Self {
        self
    }

    fn into_box(self) -> BoxMessage {
        BoxMessage::Msg(self)
    }
}
