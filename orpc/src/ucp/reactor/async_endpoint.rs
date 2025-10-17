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

use std::mem;
use std::sync::Arc;
use bytes::BytesMut;
use log::info;
use crate::err_box;
use crate::io::IOResult;
use crate::runtime::{RpcRuntime, Runtime};
use crate::sync::channel::{AsyncSender, CallChannel};
use crate::sys::{DataSlice, RawPtr};
use crate::ucp::reactor::{RmaEndpoint, UcpExecutor};
use crate::ucp::request::*;

#[derive(Clone)]
pub struct AsyncEndpoint {
    inner: Option<RawPtr<RmaEndpoint>>,
    executor: Arc<UcpExecutor>,
    sender: AsyncSender<OpRequest>
}

impl AsyncEndpoint {
    pub fn new(endpoint: RmaEndpoint) -> Self {
        let sender = endpoint.executor().clone_sender();
        let executor = endpoint.executor().clone();
        AsyncEndpoint {
            inner: Some(RawPtr::from_owned(endpoint)),
            executor,
            sender
        }
    }

    fn ep(&self) -> IOResult<RawPtr<RmaEndpoint>> {
        match self.inner.as_ref() {
            Some(ep) => Ok(ep.clone()),
            None => err_box!("endpoint is closed")
        }
    }

    pub async fn handshake_request(&mut self) -> IOResult<()> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::HandshakeRequest(HandshakeRequest {
            ep: self.ep()?,
            call
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn handshake_response(&mut self) -> IOResult<()> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::HandshakeResponse(HandshakeResponse {
            ep: self.ep()?,
            call
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn stream_send(&self, buf: DataSlice) -> IOResult<()> {
        let (call, promise) = CallChannel::channel();
            let req = OpRequest::StreamSend(StreamSend {
                ep: self.ep()?,
            buf,
            call
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn stream_recv(&self, buf: BytesMut) -> IOResult<BytesMut> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::StreamRecv(StreamRecv {
            ep: self.ep()?,
            buf,
            full: false,
            call,
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn put(&self, buf: DataSlice) -> IOResult<()> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::RmaPut(RmaPut {
            ep: self.ep()?,
            buf,
            call,
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn get(&self, buf: BytesMut) -> IOResult<BytesMut> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::RmaGet(RmaGet {
            ep: self.ep()?,
            buf,
            call,
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn flush(&self) -> IOResult<()> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::Flush(Flush {
            ep: self.ep()?,
            call,
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn tag_send(&self, buf: DataSlice) -> IOResult<()> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::TagSend(TagSend {
            ep: self.ep()?,
            buf,
            call,
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub async fn tag_recv(&self, buf: BytesMut) -> IOResult<BytesMut> {
        let (call, promise) = CallChannel::channel();
        let req = OpRequest::TagRecv(TagRecv {
            ep: self.ep()?,
            buf,
            call,
        });
        self.sender.send(req).await?;
        promise.receive().await?
    }

    pub fn executor(&self) -> &UcpExecutor {
        self.inner.as_ref().unwrap().executor()
    }

    pub fn rt(&self) -> &Runtime {
        self.executor().rt()
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.executor().clone_rt()
    }

    pub fn local_mem_slice(&self, len: usize) -> &[u8] {
        &self.inner.as_ref().unwrap().local_mem_slice()[..len]
    }

}

impl Drop for AsyncEndpoint {
    fn drop(&mut self) {
        if let Some(ep) = self.inner.take() {
            self.executor.rt().spawn(async move {
                drop(ep);
            });
        }

    }
}

