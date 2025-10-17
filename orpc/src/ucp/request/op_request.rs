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
use tracing::info;
use crate::common::Utils;
use crate::io::IOResult;
use crate::sync::channel::CallSender;
use crate::sys::{DataSlice, RawPtr};
use crate::ucp::reactor::RmaEndpoint;

pub struct HandshakeRequest {
    pub ep: RawPtr<RmaEndpoint>,
    pub call: CallSender<IOResult<()>>,
}

impl HandshakeRequest {
    pub async fn run(mut self) -> IOResult<()> {
        let res = self.ep.handshake_request().await;
        self.call.send(res)
    }
}

pub struct HandshakeResponse {
    pub ep: RawPtr<RmaEndpoint>,
    pub call: CallSender<IOResult<()>>,
}

impl HandshakeResponse {
    pub async fn run(mut self) -> IOResult<()> {
        let res = self.ep.handshake_response().await;
        self.call.send(res)
    }
}


pub struct StreamSend {
    pub ep: RawPtr<RmaEndpoint>,
    pub buf: DataSlice,
    pub call: CallSender<IOResult<()>>,
}

impl StreamSend {
    pub async fn run(self) -> IOResult<()> {
        let res = self.ep.stream_send(self.buf.as_slice()).await;
        self.call.send(res)
    }
}

pub struct StreamRecv {
    pub ep: RawPtr<RmaEndpoint>,
    pub buf: BytesMut,
    pub full: bool,
    pub call: CallSender<IOResult<BytesMut>>,
}

impl StreamRecv {
    pub async fn run(mut self) -> IOResult<()> {
        let res = async {
            if self.full {
                self.ep.stream_recv_full(&mut self.buf).await?;
                Ok(self.buf)
            } else {
                let size = self.ep.stream_recv(&mut self.buf).await?;
                Ok(self.buf.split_to(size))
            }
        }.await;
        self.call.send(res)
    }
}

pub struct Flush {
    pub ep: RawPtr<RmaEndpoint>,
    pub call: CallSender<IOResult<()>>,
}

impl Flush {
    pub async fn run(self) -> IOResult<()> {
        let res = self.ep.flush().await;
        self.call.send(res)
    }
}

pub struct RmaPut {
    pub ep: RawPtr<RmaEndpoint>,
    pub buf: DataSlice,
    pub call: CallSender<IOResult<()>>,
}

impl RmaPut {
    pub async fn run(self) -> IOResult<()> {
        let res = self.ep.put(self.buf.as_slice()).await;
        self.call.send(res).unwrap();
        Ok(())
    }
}

pub struct RmaGet {
    pub ep: RawPtr<RmaEndpoint>,
    pub buf: BytesMut,
    pub call: CallSender<IOResult<BytesMut>>,
}

impl RmaGet {
    pub async fn run(mut self) -> IOResult<()> {
        let res = match self.ep.get(&mut self.buf).await {
            Ok(_) => Ok(self.buf),
            Err(e) => Err(e)
        };
        self.call.send(res)
    }
}

pub struct TagSend {
    pub ep: RawPtr<RmaEndpoint>,
    pub buf: DataSlice,
    pub call: CallSender<IOResult<()>>,
}

impl TagSend {
    pub async fn run(self) -> IOResult<()> {
        let res = self.ep.tag_send(self.buf.as_slice()).await;
        self.call.send(res)
    }
}

pub struct TagRecv {
    pub ep: RawPtr<RmaEndpoint>,
    pub buf: BytesMut,
    pub call: CallSender<IOResult<BytesMut>>,
}

impl TagRecv {
    pub async fn run(mut self) -> IOResult<()> {
        let res = match self.ep.tag_recv(&mut self.buf).await {
            Ok(v) => Ok(self.buf.split_to(v)),
            Err(e) => Err(e)
        };
        self.call.send(res)
    }
}

pub enum OpRequest {
    HandshakeRequest(HandshakeRequest),
    HandshakeResponse(HandshakeResponse),
    StreamSend(StreamSend),
    StreamRecv(StreamRecv),
    Flush(Flush),
    RmaPut(RmaPut),
    RmaGet(RmaGet),
    TagSend(TagSend),
    TagRecv(TagRecv),
}

impl OpRequest {
    pub async fn run(self) -> IOResult<()> {
        match self {
            OpRequest::HandshakeRequest(req) => req.run().await,
            OpRequest::HandshakeResponse(req) => req.run().await,
            OpRequest::StreamSend(req) => req.run().await,
            OpRequest::StreamRecv(req) => req.run().await,
            OpRequest::Flush(req) => req.run().await,
            OpRequest::RmaPut(req) => req.run().await,
            OpRequest::RmaGet(req) => req.run().await,
            OpRequest::TagSend(req) => req.run().await,
            OpRequest::TagRecv(req) => req.run().await,
        }
    }
}