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

use curvine_common::proto::{ContainerBlockWriteRequest, ContainerMetadataProto};
use curvine_common::state::ExtendedBlock;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::message::Message;

#[derive(Debug)]
pub struct ContainerWriteContext {
    pub block: ExtendedBlock,
    pub req_id: i64,
    pub chunk_size: i32,
    pub short_circuit: bool,
    pub off: i64,
    pub block_size: i64,
    pub files_metadata: ContainerMetadataProto,
}

impl ContainerWriteContext {
    pub fn from_req(msg: &Message) -> FsResult<Self> {
        let req: ContainerBlockWriteRequest = msg.parse_header()?;

        let context = Self {
            block: ProtoUtils::extend_block_from_pb(req.block),
            req_id: msg.req_id(),
            chunk_size: req.chunk_size,
            short_circuit: req.short_circuit,
            off: req.off,
            block_size: req.block_size,
            files_metadata: req.files_metadata,
        };

        Ok(context)
    }
}
