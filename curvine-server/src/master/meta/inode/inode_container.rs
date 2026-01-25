use crate::master::meta::BlockMeta;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeContainer {
    pub(crate) id: i64,
    pub(crate) files: HashMap<String, SmallFileMeta>,
    pub(crate) blocks: Vec<BlockMeta>,
    pub(crate) total_size: i64,
    pub(crate) max_file_size: i64, // Threshold for small files
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallFileMeta {
    pub(crate) offset: i64,
    pub(crate) len: i64,
    pub(crate) block_index: u32,
    pub(crate) mtime: i64,
}
