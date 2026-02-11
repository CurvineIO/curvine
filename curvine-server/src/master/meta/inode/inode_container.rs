use crate::master::meta::feature::{AclFeature, FileFeature, WriteFeature};
use crate::master::meta::inode::{Inode, EMPTY_PARENT_ID};
use crate::master::meta::{BlockMeta, InodeId};
use curvine_common::state::{CommitBlock, CreateFileOpts, FileType, StoragePolicy};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::{err_box, CommonResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeContainer {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) file_type: FileType,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,
    pub(crate) features: FileFeature,
    pub(crate) files: HashMap<String, SmallFileMeta>,
    pub(crate) block: BlockMeta, // convert to only one block
    pub(crate) replicas: u16,
    pub(crate) len: i64,
    pub(crate) block_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallFileMeta {
    pub(crate) offset: i64,
    pub(crate) len: i64,
    pub(crate) block_index: u32,
    pub(crate) mtime: i64,
}

impl InodeContainer {
    pub fn new(id: i64, time: i64) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            file_type: FileType::Container,
            mtime: time,
            atime: time,
            nlink: 1,
            storage_policy: Default::default(),
            features: FileFeature::new(),
            files: HashMap::new(),
            block: BlockMeta::new(id, 0),
            replicas: 0,
            len: 0,
            block_size: 0,
        }
    }

    pub fn with_opts(id: i64, time: i64, opts: CreateFileOpts) -> Self {
        let mut file = Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            file_type: FileType::File,
            mtime: time,
            atime: time,
            nlink: 1,
            storage_policy: opts.storage_policy,
            features: FileFeature {
                x_attr: Default::default(),
                file_write: None,
                acl: AclFeature {
                    mode: opts.mode,
                    owner: opts.owner,
                    group: opts.group,
                },
            },
            files: HashMap::new(),
            block: BlockMeta::new(id, 0),
            replicas: opts.replicas,
            len: 0,
            block_size: opts.block_size,
        };
        file.features.set_writing(opts.client_name);
        if !opts.x_attr.is_empty() {
            file.features.set_attrs(opts.x_attr);
        }

        file.features.set_mode(opts.mode);
        file
    }

    pub fn complete(
        &mut self,
        len: i64,
        commit_block: &CommitBlock,
        client_name: impl AsRef<str>,
        only_flush: bool,
        files: HashMap<String, SmallFileMeta>,
    ) -> FsResult<()> {
        self.files = files;
        self.mtime = LocalTime::mills() as i64;

        if !only_flush {
            self.features.complete_write(client_name);
        }
        // update len of inode
        self.len = self.len.max(len);
        // validate len
        if self.block.len != self.len {
            return err_box!(
                "Block len is not equal to container inode len, block len= {}, container inode len= {}",
                self.block.len,
                self.len
            );
        }
        //update block meta
        self.block.commit(commit_block);
        Ok(())
    }

    pub fn get_file(&self, name: &str) -> Option<&SmallFileMeta> {
        self.files.get(name)
    }

    pub fn files_count(&self) -> usize {
        self.files.len()
    }

    pub fn add_block(&mut self, block: BlockMeta) {
        self.block = block;
    }

    pub fn block_ids(&self) -> Vec<i64> {
        vec![self.block.id]
    }

    pub fn is_complete(&self) -> bool {
        self.features.file_write.is_none()
    }

    pub fn is_writing(&self) -> bool {
        self.features.file_write.is_some()
    }

    pub fn write_feature(&self) -> Option<&WriteFeature> {
        self.features.file_write.as_ref()
    }

    // Get current link count
    pub fn nlink(&self) -> u32 {
        self.nlink
    }

    /// Create a new block id
    /// It is composed of the inode id + block number of the file, starting from 1.
    /// inode id + serial number 0, is the file id.
    pub fn next_block_id(&mut self) -> CommonResult<i64> {
        let seq = 0 as i64;
        // don't increment seq because InodeContainer has just one block.
        InodeId::create_block_id(self.id, seq)
    }
}

impl Inode for InodeContainer {
    fn id(&self) -> i64 {
        self.id
    }

    fn parent_id(&self) -> i64 {
        self.parent_id
    }

    fn is_dir(&self) -> bool {
        false // Container is a file-like object
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }

    fn nlink(&self) -> u32 {
        self.nlink
    }
}

impl PartialEq for InodeContainer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl SmallFileMeta {
    pub fn from_proto(meta: curvine_common::proto::SmallFileMetaProto) -> Self {
        Self {
            offset: meta.offset,
            len: meta.len,
            block_index: meta.block_index as u32,
            mtime: meta.mtime,
        }
    }
    pub fn to_proto(&self) -> curvine_common::proto::SmallFileMetaProto {
        curvine_common::proto::SmallFileMetaProto {
            offset: self.offset,
            len: self.len,
            block_index: self.block_index as i32,
            mtime: self.mtime,
        }
    }
}
