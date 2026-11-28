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

use crate::worker::WorkerMetrics;
use curvine_error::FsResult;
use curvine_metrics::Counter;
use curvine_model::ExtendedBlock;
use curvine_model::ProtoUtils;
use curvine_proto::{BlockReadRequest, BlockWriteRequest};
use curvine_rpc::message::Message;

/// Counter handles of the storage directory owning an open block. They are
/// resolved once in `open()` so the data path does not rebuild the `dir_id`
/// label and re-hash it for every written chunk.
pub struct DirWriteMetrics {
    pub(crate) dir_id: u32,
    pub(crate) write_bytes: Counter,
    pub(crate) write_time_us: Counter,
    pub(crate) write_count: Counter,
    pub(crate) slow_write_count: Counter,
}

impl DirWriteMetrics {
    pub fn new(metrics: &WorkerMetrics, dir_id: u32) -> Self {
        let dir_id_label = dir_id.to_string();
        let label = dir_id_label.as_str();
        Self {
            dir_id,
            write_bytes: metrics.write_bytes.with_label_values(&[label]),
            write_time_us: metrics.write_time_us.with_label_values(&[label]),
            write_count: metrics.write_count.with_label_values(&[label]),
            slow_write_count: metrics.slow_write_count.with_label_values(&[label]),
        }
    }
}

impl std::fmt::Debug for DirWriteMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirWriteMetrics(dir_id={})", self.dir_id)
    }
}

#[derive(Debug)]
pub struct WriteContext {
    pub block: ExtendedBlock,
    pub req_id: i64,
    pub chunk_size: i32,
    pub short_circuit: bool,
    pub off: i64,
    pub block_size: i64,
    pub dir_id: u32,
    pub dir_metrics: Option<DirWriteMetrics>,
}

impl WriteContext {
    pub fn from_req(msg: &Message) -> FsResult<Self> {
        let req: BlockWriteRequest = msg.parse_header()?;

        let context = Self {
            block: ProtoUtils::extend_block_from_pb(req.block),
            req_id: msg.req_id(),
            chunk_size: req.chunk_size,
            short_circuit: req.short_circuit,
            off: req.off,
            block_size: req.block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        Ok(context)
    }
}

pub struct ReadContext {
    pub block_id: i64,
    pub chunk_size: i32,
    pub req_id: i64,
    pub short_circuit: bool,
    pub off: i64,
    pub len: i64,
    pub enable_read_ahead: bool,
    pub read_ahead_len: i64,
    pub drop_cache_len: i64,
    pub dir_id: u32,
}

impl ReadContext {
    pub fn from_req(msg: &Message) -> FsResult<Self> {
        let req: BlockReadRequest = msg.parse_header()?;
        let context = Self {
            block_id: req.id,
            chunk_size: req.chunk_size,
            req_id: msg.req_id(),
            short_circuit: req.short_circuit,
            off: req.off,
            len: req.len,
            enable_read_ahead: req.enable_read_ahead,
            read_ahead_len: req.read_ahead_len,
            drop_cache_len: req.drop_cache_len,
            dir_id: 0,
        };

        Ok(context)
    }
}
