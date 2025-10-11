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

use bincode::config::BigEndian;
use bytes::{Buf, BufMut, BytesMut};
use crate::common::Utils;
use crate::err_box;
use crate::io::IOResult;
use crate::sys::DataSlice;
use crate::ucp::core::{Endpoint, SockAddr};
use crate::ucp::rma::{LocalMem, RemoteMem, RKey};
use crate::ucp::UcpExecutor;

/// RMA (Remote Memory Access) endpoint that supports high-performance remote memory operations.
///
/// This endpoint enables direct memory access to remote peers using UCX's RMA capabilities.
/// It maintains local memory registration and optional remote memory information for efficient
/// data transfer operations like put/get without CPU overhead on the remote side.
///
/// Uses a dual-end memory model where both client and server hold remote memory addresses
pub struct RmaEndpoint {
    inner: Endpoint,
    ep_id: u64,
    local_mem: LocalMem,
    remote_mem: Option<RemoteMem>
}

impl RmaEndpoint {
    pub const MEM_MAGIC: u64 = 0xFEED1234;
    pub const MEM_HEADER_LEN: u32 = 28;
    pub const MEM_LEN_BYTES: usize = 4;

    pub fn new(inner: Endpoint, local_mem: LocalMem) -> Self {
        RmaEndpoint {
            inner,
            ep_id: Utils::unique_id(),
            local_mem,
            remote_mem: None,
        }
    }

    pub fn connect(executor: UcpExecutor, addr: &SockAddr, mem_len: usize) -> IOResult<Self> {
        let local_mem = executor.register_memory(mem_len)?;
        let inner = Endpoint::connect(executor, addr)?;
        Ok(Self::new(inner, local_mem))
    }

    fn encode_local_mem(&self) -> IOResult<BytesMut> {
        let mut buf = BytesMut::new();
        let pack = self.local_mem.pack()?;

        buf.put_u32(Self::MEM_HEADER_LEN + pack.as_slice().len() as u32);
        buf.put_u64(Self::MEM_MAGIC);
        buf.put_u64(self.ep_id);
        buf.put_u64(self.local_mem.addr());
        buf.put_u32(self.local_mem.len() as u32);
        buf.extend_from_slice(pack.as_slice());

        Ok(buf)
    }

    fn decode_remote_mem(&self, mut buf: BytesMut) -> IOResult<RemoteMem> {
        let magic = buf.get_u64();
        if magic != Self::MEM_MAGIC {
            return err_box!("invalid magic number: 0x{:x}", magic)
        }

        let ep_id = buf.get_u64();
        let addr = buf.get_u64();
        let len = buf.get_u32() as usize;
        let r_key = RKey::unpack(&self.inner, &buf)?;

        let remote_mem = RemoteMem::new(ep_id, addr, len, r_key);
        Ok(remote_mem)
    }

    pub fn with_capacity(inner: Endpoint, size: usize) -> IOResult<Self> {
        let local_mem = inner.executor().register_memory(size)?;

        Ok(RmaEndpoint::new(inner, local_mem))
    }

    async fn get_mem(&self) -> IOResult<RemoteMem> {
        let mut buf = BytesMut::zeroed(Self::MEM_LEN_BYTES);
        self.inner.stream_recv_full(&mut buf).await?;

        // read memory bytes
        let total_len = buf.get_u32() as usize;
        let mut buf = BytesMut::zeroed(total_len);
        self.inner.stream_recv_full(&mut buf).await?;
        self.decode_remote_mem(buf)
    }

    async fn send_mem(&self) -> IOResult<()> {
        let buf = self.encode_local_mem()?;
        self.inner.stream_send(&buf).await?;
        Ok(())
    }

    /// 客服端发起调用。
    /// 1. 发送自己的内存信息给服务端。
    /// 2. 获取服务端的内存信息。
    pub async fn exchange_mem(&mut self) -> IOResult<()> {
        self.send_mem().await?;

        let remote_mem = self.get_mem().await?;
        let _ = self.remote_mem.insert(remote_mem);

        Ok(())
    }


    /// 服务端发起调用。
    /// 1. 接收客服端的内存信息。
    /// 2. 发送自己的内存信息给客服端。
    pub async fn accept_memory(&mut self) -> IOResult<()> {
        let remote_mem = self.get_mem().await?;
        let _ = self.remote_mem.insert(remote_mem);

        self.send_mem().await?;

        Ok(())
    }

    fn get_remote_mem(&self, len: usize) -> IOResult<&RemoteMem> {
        match self.remote_mem {
            Some(ref mem) => {
                if len > mem.len() {
                    return err_box!("Data length exceeds limit, data len {}, memory len {}", len, mem.len())
                }
                Ok(mem)
            },
            None => err_box!("remote memory not set")
        }
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

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner
    }

    pub fn into_inner(self) -> Endpoint {
        self.inner
    }

    pub fn local_mem(&self) -> &LocalMem {
        &self.local_mem
    }

    pub fn local_mem_slice(&self) -> &[u8] {
        self.local_mem.as_slice()
    }

    pub fn remote_mem(&self) -> Option<&RemoteMem> {
        self.remote_mem.as_ref()
    }
}