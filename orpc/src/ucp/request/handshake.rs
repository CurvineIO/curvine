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

use crate::err_box;
use crate::io::IOResult;
use crate::ucp::rma::LocalMem;
use crate::ucp::{
    HANDSHAKE_HEADER_LEN, HANDSHAKE_LEN_BYTES, HANDSHAKE_MAGIC, HANDSHAKE_MAX_LEN,
    HANDSHAKE_VERSION,
};
use bytes::{Buf, BufMut, BytesMut};

pub struct HandshakeV1 {
    pub ep_id: u64,
    pub mem_addr: u64,
    pub mem_len: u32,
    pub rkey: BytesMut,
}

impl HandshakeV1 {
    pub fn new(ep_id: u64, mem: &LocalMem) -> IOResult<Self> {
        let rkey = mem.pack()?;
        Ok(Self {
            ep_id,
            mem_addr: mem.addr(),
            mem_len: mem.len() as u32,
            rkey: BytesMut::from(rkey.as_slice()),
        })
    }

    pub fn encode(&self) -> IOResult<BytesMut> {
        let mut buf = BytesMut::new();

        buf.put_u32(HANDSHAKE_HEADER_LEN + self.rkey.len() as u32);
        buf.put_u64(HANDSHAKE_MAGIC);
        buf.put_u32(HANDSHAKE_VERSION);

        buf.put_u64(self.ep_id);
        buf.put_u64(self.mem_addr);
        buf.put_u32(self.mem_len);
        buf.put_slice(&self.rkey);

        if buf.len() > HANDSHAKE_MAX_LEN {
            return err_box!("handshake request is too long: {}", buf.len());
        }
        Ok(buf)
    }

    pub fn decode(mut buf: BytesMut) -> IOResult<Self> {
        if buf.len() + HANDSHAKE_LEN_BYTES > HANDSHAKE_MAX_LEN {
            return err_box!("handshake request is too long: {}", buf.len());
        }
        let magic = buf.get_u64();
        if magic != HANDSHAKE_MAGIC {
            return err_box!(
                "invalid magic number: {}, expected: {}",
                magic,
                HANDSHAKE_MAGIC
            );
        }
        let version = buf.get_u32();
        if version != HANDSHAKE_VERSION {
            return err_box!(
                "invalid version: {}, expected: {}",
                version,
                HANDSHAKE_VERSION
            );
        };
        let ep_id = buf.get_u64();
        let mem_addr = buf.get_u64();
        let mem_len = buf.get_u32();
        let rkey = buf.split();

        Ok(Self {
            ep_id,
            mem_addr,
            mem_len,
            rkey,
        })
    }
}
