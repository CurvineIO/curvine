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
