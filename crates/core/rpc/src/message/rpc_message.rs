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
use bytes::{Buf, BufMut, BytesMut};
use curvine_core_error::{CommonError, CommonResult, ErrorExt};
use curvine_io::{DataSlice, IOResult};
use curvine_runtime::common::Utils;
use num_enum::{FromPrimitive, IntoPrimitive};
use prost::Message as PMessage;
use std::fmt::Debug;
use std::time::Instant;

/// RPC message communication format
// Message header fixed byte length
// header length (4 bytes) + body length (4 bytes) + code (1 byte) + status (1 byte) + req_id (8 bytes) + seqId (4 bytes)
// If an error is returned, the error message is saved in data as a byte array.
pub const PROTOCOL_SIZE: i32 = 22;
pub const PROTOCOL_USIZE: usize = PROTOCOL_SIZE as usize;

// Head length
pub const HEAD_SIZE: i32 = PROTOCOL_SIZE - 4;

pub const INIT_SEQ_ID: i32 = -1;

pub const END_SEQ_ID: i32 = -2;

pub const EMPTY_REQ_ID: i64 = -1;

pub const MAX_DATE_SIZE: i32 = 16 * 1024 * 1024;

pub const MAX_FRAME_SIZE: i32 = 64 * 1024 * 1024;

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
    pub fn encode(&self) -> i8 {
        self.0 as i8 | ((self.1 as i8) << 4)
    }

    pub fn new_request(s: RequestStatus) -> Status {
        Status(s, ResponseStatus::Undefined)
    }

    pub fn from(v: i8) -> Status {
        let req = RequestStatus::from(v & 0x0f);
        let rep = ResponseStatus::from(v >> 4);

        Status(req, rep)
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

    pub fn create(buf: &mut impl Buf) -> Self {
        Protocol {
            code: buf.get_i8(),
            status: Status::from(buf.get_i8()),
            req_id: buf.get_i64(),
            seq_id: buf.get_i32(),
        }
    }

    pub fn is_heartbeat(&self) -> bool {
        self.status.0 == RequestStatus::Heartbeat
    }
}

// Network message.A one-time and continuous request protocol is uniformly defined.
#[derive(Debug)]
pub struct RpcMessage {
    pub protocol: Protocol,
    pub header: Option<BytesMut>,
    pub data: DataSlice,
    /// Receipt time, stamped only by the wire-decode paths
    /// (`RpcFrame::receive`, `ReadFrame::receive`) for non-heartbeat messages.
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

    pub fn total_len(&self) -> usize {
        PROTOCOL_USIZE + self.header_len() + self.data_len()
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

    pub fn encode_status(&self) -> i8 {
        self.protocol.status.encode()
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
            _ => None,
        }
    }

    pub fn is_success(&self) -> bool {
        self.response_status() == ResponseStatus::Success
    }

    pub fn is_heartbeat(&self) -> bool {
        self.protocol.is_heartbeat()
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

    pub fn encode_protocol(&self, mut buf: impl BufMut) {
        let header_len = self.header_len();
        let data_len = self.data_len();

        buf.put_i32((header_len + data_len) as i32 + HEAD_SIZE);
        buf.put_i32(header_len as i32);
        buf.put_i8(self.protocol.code);
        buf.put_i8(self.protocol.status.encode());
        buf.put_i64(self.protocol.req_id);
        buf.put_i32(self.protocol.seq_id);
    }

    /// Decode protocol data
    ///
    /// Parse the protocol header information from the byte buffer, including the total size, header size, and data size.
    /// Create a protocol object.
    ///
    /// # Parameters
    /// * `buf` - A mutable reference to the byte buffer containing the protocol data to be parsed
    ///
    /// # Return value
    /// Returns an IOResult-wrapped tuple containing:
    /// * Protocol - The protocol object created by parsing
    /// * i32 - Header size
    /// * i32 - Data size
    pub fn decode_protocol(buf: &mut impl Buf) -> IOResult<(Protocol, i32, i32)> {
        let total_size = buf.get_i32();
        let header_size = buf.get_i32();

        if header_size < 0 {
            return err_box!("header length is negative: {}", header_size);
        } else if total_size > MAX_FRAME_SIZE {
            return err_box!(
                "Frame exceeds maximum size: {} > {}",
                total_size,
                MAX_FRAME_SIZE
            );
        }

        let data_size = total_size
            .saturating_sub(header_size)
            .saturating_sub(HEAD_SIZE);
        if data_size < 0 {
            return err_box!("data length is negative: {}", data_size);
        } else if data_size > MAX_DATE_SIZE {
            return err_box!("Data exceeds maximum size: {}", MAX_DATE_SIZE);
        }

        let protocol = Protocol::create(buf);
        Ok((protocol, header_size, data_size))
    }

    pub fn encode(&self, buf: &mut BytesMut) -> IOResult<()> {
        let slice = match &self.data {
            DataSlice::IOSlice(_) => {
                return err_box!("Message::encode cannot encode an IOSlice payload")
            }
            data => data.as_slice(),
        };

        self.encode_protocol(&mut *buf);

        if let Some(header) = &self.header {
            buf.put_slice(header);
        }

        if !slice.is_empty() {
            buf.put_slice(slice);
        }

        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> IOResult<Self> {
        // The BytesMut returned by tokio does not contain the first length byte.
        if buf.remaining() < HEAD_SIZE as usize {
            return err_box!("frame too short: {} bytes", buf.len());
        }

        let header_size = buf.get_u32() as usize;
        let protocol = Protocol::create(buf);

        if header_size > buf.remaining() {
            return err_box!(
                "header length {} exceeds the frame body {}",
                header_size,
                buf.remaining()
            );
        }

        let data_size = buf.remaining() - header_size;
        if data_size > MAX_DATE_SIZE as usize {
            return err_box!(
                "Data exceeds maximum size: {} > {}",
                data_size,
                MAX_DATE_SIZE
            );
        }

        let header = if header_size > 0 {
            Some(buf.split_to(header_size))
        } else {
            None
        };

        let data = if buf.has_remaining() {
            DataSlice::Buffer(buf.split())
        } else {
            DataSlice::Empty
        };

        Ok(Self::new(protocol, header, data))
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
