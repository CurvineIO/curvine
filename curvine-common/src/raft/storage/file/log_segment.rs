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

use std::hash::Hasher;
use std::io::{BufWriter, Write};
use prost::bytes::BytesMut;
use prost::Message;
use raft::eraftpb::Entry;
use tokio_util::bytes::BufMut;
use orpc::err_box;
use orpc::os::LocalFile;
use crate::raft::RaftResult;

pub struct LogSegment {
    pub(crate) path: String,
    pub(crate) start_index: u64,
    pub(crate) end_index: u64,
    pub(crate) file: Option<BufWriter<LocalFile>>,
    pub(crate) cache: Vec<Entry>,
    pub(crate) cur_index: u64,
    pub(crate) checksum: crc32fast::Hasher,
    pub(crate) write_len: u64,
}

impl LogSegment {
    pub fn get_last_entry(&self) -> Option<&Entry> {
        self.cache.last()
    }

    pub fn get_first_entry(&self) -> Option<&Entry> {
        self.cache.first()
    }

    pub fn append(&mut self, mut entry: Entry) -> RaftResult<()> {
       /* if let Some(v) = self.get_last_entry() {
            if entry.index != v.index + 1 {
                return err_box!("gap between entries {} and {}", entry.index, v.index)
            }
        }

        let len = entry.encoded_len();
        let mut bytes = BytesMut::with_capacity(len + 8);

        bytes.put_u32(len as u32);
        entry.encode(&mut bytes[4..4 + len])?;

        self.checksum.reset();
        self.checksum.write(&mut bytes[4..4 + len]);
        bytes.put_u32(self.checksum.finalize());

        let file = match self.file.as_mut() {
            None => return err_box!("Segment file {} already close", self.path),
            Some(f) => f
        };

        file.write(&bytes)?;
        self.write_len += len;
        self.end_index = entry.index;
        self.cache.push(entry);

        Ok(())*/
        Ok(())
    }

    pub fn flush(&mut self) -> RaftResult<()> {
        /*if let Some(w) = &mut self.file {
            w.flush()?;
        }*/
        Ok(())
    }

    pub fn clear(&mut self) {
        self.end_index = self.start_index - 1;
        self.cache.clear();
    }
}
