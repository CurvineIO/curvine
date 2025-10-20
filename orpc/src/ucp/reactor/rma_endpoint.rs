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
use crate::err_box;
use crate::io::IOResult;
use crate::ucp::core::{Endpoint, SockAddr};
use crate::ucp::reactor::{RmaType, UcpExecutor};
use crate::ucp::request::{ConnRequest, HandshakeV1};
use crate::ucp::rma::{LocalMem, RKey, RemoteMem};
use crate::ucp::HANDSHAKE_LEN_BYTES;
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;
use log::info;

/// RMA (Remote Memory Access) endpoint that supports high-performance remote memory operations.
///
/// This endpoint enables direct memory access to remote peers using UCX's RMA capabilities.
/// It maintains local memory registration and optional remote memory information for efficient
/// data transfer operations like put/get without CPU overhead on the remote side.
///
/// Uses a dual-end memory model where both client and server hold remote memory addresses
pub struct RmaEndpoint {
    inner: Endpoint,
    executor: Arc<UcpExecutor>,
    ep_id: u64,
    local_mem: Option<LocalMem>,
    remote_mem: Option<RemoteMem>,
}

impl RmaEndpoint {
    fn new(executor: Arc<UcpExecutor>, inner: Endpoint) -> Self {
        Self {
            inner,
            ep_id: Utils::unique_id(),
            executor,
            local_mem: None,
            remote_mem: None,
        }
    }

    pub fn accept(executor: Arc<UcpExecutor>, conn: ConnRequest) -> IOResult<Self> {
        let inner = Endpoint::accept(executor.worker().clone(), conn)?;
        Ok(Self::new(executor, inner))
    }

    pub fn connect(executor: Arc<UcpExecutor>, addr: &SockAddr) -> IOResult<Self> {
        let inner = Endpoint::connect(executor.worker().clone(), addr)?;
        Ok(Self::new(executor, inner))
    }

    /// 发送握手信息
    pub async fn handshake_request(&mut self, rma_type: RmaType, mem_len: usize) -> IOResult<()> {
        info!(
            "handshake request endpoint, rma_type: {:?}, mem_len: {}, transports {:?}",
            rma_type,
            mem_len,
            self.inner.query_transports().unwrap_or_default()
        );

        // 注册本地内存。
        if rma_type.client_need() {
            let mem =self.executor.register_memory(mem_len)?;
            let _ = self.local_mem.insert(mem);
        }

        self.handshake_send(rma_type, mem_len).await?;

        // 保存远程内存信息。
        let handshake = self.handshake_recv().await?;
        if let Some(remote_mem) = handshake.get_remote_mem(&self.inner)? {
            let _ = self.remote_mem.insert(remote_mem);
        }

        Ok(())
    }

    /// 接收握手信息
    pub async fn handshake_response(&mut self) -> IOResult<()> {
        let handshake = self.handshake_recv().await?;
        info!(
            "handshake request endpoint, rma_type: {:?}, mem_len: {}, transports {:?}",
            handshake.rma_type,
            handshake.mem_len,
            self.inner.query_transports().unwrap_or_default()
        );

        // 保存ep_id和远程内存信息
        self.ep_id = handshake.ep_id;
        if let Some(remote_mem) = handshake.get_remote_mem(&self.inner)? {
            let _ = self.remote_mem.insert(remote_mem);
        }

        // 注册本地内存。
        if handshake.rma_type.client_need() {
            let mem =self.executor.register_memory(handshake.mem_len as usize)?;
            let _ = self.local_mem.insert(mem);
        }

        self.handshake_send(handshake.rma_type, handshake.mem_len as usize).await?;

        Ok(())
    }

    fn get_remote_mem(&self, len: usize) -> IOResult<&RemoteMem> {
        match self.remote_mem {
            Some(ref mem) => {
                if len > mem.len() {
                    return err_box!(
                        "Data length exceeds limit, data len {}, memory len {}",
                        len,
                        mem.len()
                    );
                }
                Ok(mem)
            }
            None => err_box!("remote memory not set"),
        }
    }

    async fn handshake_send(&mut self, rma_type: RmaType, mem_len: usize) -> IOResult<()> {
        let handshake = HandshakeV1::new(self.ep_id, rma_type, mem_len, self.local_mem.as_ref())?;
        let buf = handshake.encode()?;
        self.inner.stream_send(&buf).await
    }

    async fn handshake_recv(&mut self) -> IOResult<HandshakeV1> {
        let mut buf = BytesMut::zeroed(HANDSHAKE_LEN_BYTES);
        self.inner.stream_recv_full(&mut buf).await?;

        let total_len = buf.get_u32() as usize;
        let mut buf = BytesMut::zeroed(total_len);
        self.inner.stream_recv_full(&mut buf).await?;

        HandshakeV1::decode(buf)
    }

    pub async fn put(&self, buf: &[u8]) -> IOResult<()> {
        let mem = self.get_remote_mem(buf.len())?;
        self.inner.put(buf, mem).await
    }

    pub async fn get(&self, buf: &mut [u8]) -> IOResult<()> {
        let mem = self.get_remote_mem(buf.len())?;
        self.inner.get(buf, mem).await
    }

    pub async fn stream_send(&self, buf: &[u8]) -> IOResult<()> {
        self.inner.stream_send(buf).await
    }

    pub async fn stream_recv(&self, buf: &mut [u8]) -> IOResult<usize> {
        self.inner.stream_recv(buf).await
    }

    pub async fn stream_recv_full(&self, buf: &mut [u8]) -> IOResult<()> {
        self.inner.stream_recv_full(buf).await
    }

    pub async fn tag_recv(&self, buf: &mut [u8]) -> IOResult<usize> {
        self.inner.tag_recv(self.ep_id, buf).await
    }

    pub async fn tag_send(&self, buf: &[u8]) -> IOResult<()> {
        self.inner.tag_send(self.ep_id, buf).await
    }

    pub async fn flush(&self) -> IOResult<()> {
        self.inner.flush().await
    }

    pub fn executor(&self) -> &Arc<UcpExecutor> {
        &self.executor
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner
    }

    pub fn into_inner(self) -> Endpoint {
        self.inner
    }

    pub fn local_mem(&self) -> IOResult<&LocalMem> {
        match self.local_mem.as_ref() {
            Some(mem) => Ok(mem),
            None => err_box!("local memory not set"),
        }
    }

    pub fn local_mem_slice(&self) -> IOResult<&[u8]> {
        match self.local_mem.as_ref() {
            Some(mem) => Ok(mem.as_slice()),
            None => err_box!("local memory not set"),
        }
    }

    pub fn remote_mem(&self) -> IOResult<&RemoteMem> {
        match self.remote_mem.as_ref() {
            Some(mem) => Ok(mem),
            None => err_box!("remote memory not set"),
        }
    }
}
