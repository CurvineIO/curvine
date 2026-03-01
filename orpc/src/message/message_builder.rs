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

use crate::common::Utils;
use crate::message::{
    Builder, Message, Protocol, RequestStatus, ResponseStatus, Status, EMPTY_REQ_ID, INIT_SEQ_ID,
};
use crate::sys::DataSlice;
use bytes::BytesMut;
use log::error;
use prost::Message as PMessage;
use std::fmt::Debug;

pub struct MessageBuilder {
    code: i8,
    status: Status,
    req_id: i64,
    seq_id: i32,
    header: Option<BytesMut>,
    data: DataSlice,
}

impl MessageBuilder {
    pub fn new() -> Self {
        Builder {
            code: 0,
            status: Status::default(),
            req_id: EMPTY_REQ_ID,
            seq_id: INIT_SEQ_ID,
            header: None,
            data: DataSlice::Empty,
        }
    }

    // Create a RequestStatus::Rpc request.
    pub fn new_rpc<T: Into<i8>>(code: T) -> Self {
        Builder {
            code: code.into(),
            status: Status(RequestStatus::Rpc, ResponseStatus::Undefined),
            req_id: Utils::req_id(),
            seq_id: INIT_SEQ_ID,
            header: None,
            data: DataSlice::Empty,
        }
    }

    pub fn new_rpc_with_req_id<T: Into<i8>>(code: T, req_id: i64) -> Self {
        Builder {
            code: code.into(),
            status: Status(RequestStatus::Rpc, ResponseStatus::Undefined),
            req_id,
            seq_id: INIT_SEQ_ID,
            header: None,
            data: DataSlice::Empty,
        }
    }

    pub fn success(req: &Message) -> Self {
        Builder {
            code: req.code(),
            status: Status(req.request_status(), ResponseStatus::Success),
            req_id: req.req_id(),
            seq_id: req.seq_id(),
            header: None,
            data: DataSlice::Empty,
        }
    }

    pub fn success_with_header<T: PMessage + Default>(req: &Message, header: T) -> Self {
        Self::success(req).proto_header(header)
    }

    pub fn code<T: Into<i8>>(mut self, code: T) -> Self {
        self.code = code.into();
        self
    }

    pub fn request(mut self, req_status: RequestStatus) -> Self {
        self.status.0 = req_status;
        self
    }

    pub fn response(mut self, rep_status: ResponseStatus) -> Self {
        self.status.1 = rep_status;
        self
    }

    pub fn new_req_id(mut self) -> Self {
        self.req_id = Utils::req_id();
        self
    }

    pub fn req_id(mut self, req_id: i64) -> Self {
        self.req_id = req_id;
        self
    }

    pub fn seq_id(mut self, seq_id: i32) -> Self {
        self.seq_id = seq_id;
        self
    }

    pub fn header(mut self, header: BytesMut) -> Self {
        self.header = Some(header);
        self
    }

    pub fn proto_header<T: PMessage + Debug>(mut self, header: T) -> Self {
        let mut bytes = BytesMut::with_capacity(header.encoded_len());
        match header.encode(&mut bytes) {
            Ok(_) => self.header = Some(bytes),
            Err(e) => error!("proto encode {}", e),
        }

        self
    }

    pub fn proto_header_ref<T: PMessage>(mut self, header: &T) -> Self {
        let mut bytes = BytesMut::with_capacity(header.encoded_len());
        match header.encode(&mut bytes) {
            Ok(_) => self.header = Some(bytes),
            Err(e) => error!("proto encode {}", e),
        }

        self
    }

    pub fn data(mut self, data: DataSlice) -> Self {
        if !data.is_empty() {
            self.data = data;
        }
        self
    }

    pub fn build(self) -> Message {
        let protocol = Protocol {
            code: self.code,
            status: self.status,
            req_id: self.req_id,
            seq_id: self.seq_id,
        };

        Message {
            protocol,
            header: self.header,
            data: self.data,
        }
    }
}

impl Default for MessageBuilder {
    fn default() -> Self {
        Self::new()
    }
}
