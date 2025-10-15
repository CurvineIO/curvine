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

use std::sync::Arc;
use bytes::BytesMut;
use crate::io::IOResult;
use crate::runtime::Runtime;
use crate::sync::channel::{AsyncSender, CallChannel};
use crate::sys::{DataSlice, RawPtr, RawVec};
use crate::ucp::reactor::{RmaEndpoint, UcpExecutor};
use crate::ucp::request::{Flush, OpRequest, RmaGet, RmaPut, StreamRecv, StreamSend, TagRecv, TagSend};

#[derive(Clone)]
pub struct AsyncEndpoint {
    executor: Arc<UcpExecutor>,
    inner: RawPtr<RmaEndpoint>,
    sender: AsyncSender<OpRequest>
}

impl AsyncEndpoint {
    pub fn new(executor: Arc<UcpExecutor>, endpoint: RmaEndpoint) -> Self {
        let sender = executor.clone_sender();
        AsyncEndpoint {
            executor,
            inner: RawPtr::from_owned(endpoint),
            sender
        }
    }

    pub async fn handshake_request(&mut self) -> IOResult<()> {
        self.inner.handshake_request().await
    }

    pub async fn handshake_response(&mut self) -> IOResult<()> {
        self.inner.handshake_response().await
    }

    pub async fn stream_send(&self, buf: DataSlice) -> IOResult<()> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::StreamSend(StreamSend {
            ep: self.inner.clone(),
            buf,
            cb: tx
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub async fn stream_recv(&self, buf: BytesMut) -> IOResult<BytesMut> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::StreamRecv(StreamRecv {
            ep: self.inner.clone(),
            buf,
            full: false,
            cb: tx,
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub async fn put(&self, buf: DataSlice) -> IOResult<()> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::RmaPut(RmaPut {
            ep: self.inner.clone(),
            buf,
            cb: tx
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub async fn get(&self, buf: BytesMut) -> IOResult<BytesMut> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::RmaGet(RmaGet {
            ep: self.inner.clone(),
            buf,
            cb: tx
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub async fn flush(&self) -> IOResult<()> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::Flush(Flush {
            ep: self.inner.clone(),
            cb: tx
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub async fn tag_send(&self, buf: DataSlice) -> IOResult<()> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::TagSend(TagSend {
            ep: self.inner.clone(),
            buf,
            cb: tx
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub async fn tag_recv(&self, buf: BytesMut) -> IOResult<BytesMut> {
        let (tx, rx) = CallChannel::channel();
        let req = OpRequest::TagRecv(TagRecv {
            ep: self.inner.clone(),
            buf,
            cb: tx
        });
        self.sender.send(req).await?;
        rx.receive().await?
    }

    pub fn rt(&self) -> &Runtime {
        self.executor.rt()
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.executor.clone_rt()
    }

    pub fn into_inner(self) -> RawPtr<RmaEndpoint> {
        self.inner
    }

    pub fn local_mem_slice(&self, len: usize) -> &[u8] {
        &self.inner.local_mem_slice()[..len]
    }
}

