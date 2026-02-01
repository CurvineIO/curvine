use crate::master::meta::feature::{AclFeature, FileFeature, WriteFeature};
use crate::master::meta::inode::{Inode, EMPTY_PARENT_ID};
use crate::master::meta::BlockMeta;
use curvine_common::state::{CommitBlock, CreateFileOpts, StoragePolicy};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeContainer {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,
    pub(crate) features: FileFeature,
    pub(crate) files: HashMap<String, SmallFileMeta>,
    pub(crate) blocks: Vec<BlockMeta>,
    pub(crate) total_size: i64,
    pub(crate) max_file_size: i64, // Threshold for small files
    pub(crate) replicas: u16,
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
            mtime: time,
            atime: time,
            nlink: 1,
            storage_policy: Default::default(),
            features: FileFeature::new(),
            files: HashMap::new(),
            blocks: Vec::new(),
            total_size: 0,
            max_file_size: 1024 * 1024, // 1MB default threshold
            replicas: 0,
        }
    }

    pub fn with_opts(id: i64, time: i64, opts: CreateFileOpts) -> Self {
        let mut file = Self {
            id,
            parent_id: EMPTY_PARENT_ID,
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
            blocks: Vec::new(),
            total_size: 0,
            max_file_size: 1024 * 1024, // 1MB default threshold
            replicas: opts.replicas,
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
        println!("DEBUG at InodeContainer, at complete, len: {:?}, commit_block {:?}, files {:?}", len, commit_block, files);
        self.files = files;
        self.mtime = LocalTime::mills() as i64;
        if !only_flush {
            self.features.complete_write(client_name);
        }
        println!(
            "DEBUG at Inode_Container: at complete function, block: {:?}",
            self.blocks
        );
        //update block meta
        let meta = self.blocks.first_mut().expect("can extract");
      
        println!(
            "DEBUG at inode_container: at complete function, meta before: {:?}",
            meta
        );
        meta.commit(commit_block);

        println!(
            "DEBUG at inode_container: at complete function, meta after: {:?}",
            meta
        );
        //update Inode container len
        // self.len = self.len.max(len);
        println!("DEBUG at InodeContainer, meta: {:?}", meta);
        Ok(())
    }

    // Container-specific methods
    pub fn add_file(&mut self, name: String, meta: SmallFileMeta) {
        self.files.insert(name, meta);
        self.update_total_size();
    }

    pub fn remove_file(&mut self, name: &str) -> Option<SmallFileMeta> {
        let result = self.files.remove(name);
        self.update_total_size();
        result
    }

    pub fn get_file(&self, name: &str) -> Option<&SmallFileMeta> {
        self.files.get(name)
    }

    pub fn files_count(&self) -> usize {
        self.files.len()
    }

    pub fn update_total_size(&mut self) {
        self.total_size = self.files.values().map(|f| f.len).sum();
    }

    pub fn should_accept_file(&self, file_size: i64) -> bool {
        file_size <= self.max_file_size
    }

    pub fn add_block(&mut self, block: BlockMeta) {
        self.blocks.push(block);
    }

    pub fn block_ids(&self) -> Vec<i64> {
        self.blocks.iter().map(|x| x.id).collect()
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
    pub fn from_proto(meta: curvine_common::proto::SmallFileMeta) -> Self {
        Self {
            offset: meta.offset,
            len: meta.len,
            block_index: meta.block_index as u32,
            mtime: meta.mtime,
        }
    }
    pub fn to_proto(&self) -> curvine_common::proto::SmallFileMeta {
        curvine_common::proto::SmallFileMeta {
            offset: self.offset,
            len: self.len,
            block_index: self.block_index as i32,
            mtime: self.mtime,
        }
    }
}
