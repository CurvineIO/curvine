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

use crate::master::fs::DeleteResult;
use crate::master::meta::inode::ttl::ttl_bucket::TtlBucketList;
use crate::master::meta::inode::ttl_types::TtlInodeMetadata;
use crate::master::meta::inode::{InodeFile, InodePtr, InodeView, ROOT_INODE_ID};
use crate::master::meta::store::{InodeWriteBatch, RocksInodeStore};
use crate::master::meta::{FileSystemStats, FsDir};
use curvine_common::rocksdb::{DBConf, RocksUtils};
use curvine_common::state::{BlockLocation, CommitBlock, MountInfo};
use orpc::common::{FileUtils, Utils};
use orpc::{try_err, try_option, CommonResult};
use std::collections::{HashMap, LinkedList};
use std::sync::Arc;

// Currently, only RockSDB is supported.
#[derive(Clone)]
pub struct InodeStore {
    pub(crate) store: Arc<RocksInodeStore>,
    pub(crate) fs_stats: Arc<FileSystemStats>,
    ttl_bucket_list: Arc<TtlBucketList>,
}

impl InodeStore {
    pub fn new(store: RocksInodeStore, ttl_bucket_list: Arc<TtlBucketList>) -> Self {
        InodeStore {
            store: Arc::new(store),
            fs_stats: Arc::new(FileSystemStats::new()),
            ttl_bucket_list,
        }
    }

    pub fn get_ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.ttl_bucket_list.clone()
    }

    pub fn apply_add(&self, parent: &InodeView, child: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(child)?;
        batch.write_inode(parent)?;
        batch.add_child(parent.id(), child.name(), child.id())?;

        batch.commit()?;

        if let Some(ttl_config) = child.ttl_config() {
            let metadata = TtlInodeMetadata::new(child.id() as u64, ttl_config.clone());
            if let Err(e) = self.ttl_bucket_list.add_inode(&metadata) {
                log::warn!(
                    "Direct ttl registration failed for inode {}: {}",
                    child.id(),
                    e
                );
            }
        }

        match child {
            InodeView::File(_) => self.fs_stats.increment_file_count(),
            InodeView::Dir(dir) => {
                // Don't count root directory
                if dir.id != ROOT_INODE_ID {
                    self.fs_stats.increment_dir_count();
                }
            }
        }

        Ok(())
    }

    pub fn apply_delete(&self, parent: &InodeView, del: &InodeView) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();
        batch.write_inode(parent)?;

        let mut stack = LinkedList::new();
        stack.push_back((parent.id(), del));
        let mut del_res = DeleteResult::new();
        let mut deleted_files = 0i64;
        let mut deleted_dirs = 0i64;

        while let Some((parent_id, inode)) = stack.pop_front() {
            // Delete inode nodes and edges
            batch.delete_inode(inode.id())?;
            batch.delete_child(parent_id, inode.name())?;
            del_res.inodes += 1;

            if let Err(e) = self.ttl_bucket_list.remove_inode(inode.id() as u64) {
                log::warn!("Direct ttl removal failed for inode {}: {}", inode.id(), e);
            }

            match inode {
                InodeView::File(file) => {
                    deleted_files += 1;
                    for meta in &file.blocks {
                        if meta.is_writing() {
                            // Uncommitted block.
                            if let Some(locs) = &meta.locs {
                                del_res.blocks.insert(meta.id, locs.clone());
                            }
                        } else {
                            let locs = self.store.get_locations(meta.id)?;
                            if !locs.is_empty() {
                                del_res.blocks.insert(meta.id, locs);
                            }
                        };
                    }
                }

                InodeView::Dir(dir) => {
                    // Don't count root directory
                    if dir.id != ROOT_INODE_ID {
                        deleted_dirs += 1;
                    }
                    for item in dir.children_iter() {
                        stack.push_back((inode.id(), item))
                    }
                }
            }
        }

        batch.commit()?;

        if deleted_files > 0 {
            self.fs_stats.add_file_count(-deleted_files);
        }
        if deleted_dirs > 0 {
            self.fs_stats.add_dir_count(-deleted_dirs);
        }

        Ok(del_res)
    }

    pub fn apply_rename(
        &self,
        src_parent: &InodeView,
        src_inode: &InodeView,
        dst_parent: &InodeView,
        dst_inode: &InodeView,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        // Delete the old node.
        batch.delete_child(src_parent.id(), src_inode.name())?;

        // Add new node.
        batch.write_inode(dst_inode)?;
        batch.add_child(dst_parent.id(), dst_inode.name(), dst_inode.id())?;

        // Update the modification time of the previous node.
        batch.write_inode(src_parent)?;
        batch.write_inode(dst_parent)?;

        batch.commit()?;

        Ok(())
    }

    pub fn apply_new_block(
        &self,
        file: &InodeView,
        previous: Option<&CommitBlock>,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(file)?;
        if let Some(commit) = previous {
            for item in &commit.locations {
                batch.add_location(commit.block_id, item)?;
            }
        }

        batch.commit()
    }

    pub fn apply_complete_file(
        &self,
        file: &InodeView,
        last: Option<&CommitBlock>,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(file)?;
        if let Some(commit) = last {
            for item in &commit.locations {
                batch.add_location(commit.block_id, item)?;
            }
        }

        batch.commit()
    }

    pub fn apply_append_file(&self, file: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.write_inode(file)?;
        batch.commit()
    }

    pub fn apply_set_attr(&self, inodes: Vec<InodePtr>) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        for inode in &inodes {
            batch.write_inode(inode.as_ref())?;
        }
        batch.commit()?;

        for inode in &inodes {
            let inode_id = inode.id() as u64;
            let _ = self.ttl_bucket_list.remove_inode(inode_id);
            if let Some(ttl_config) = inode.ttl_config() {
                let metadata = TtlInodeMetadata::new(inode_id, ttl_config.clone());
                if let Err(e) = self.ttl_bucket_list.add_inode(&metadata) {
                    log::warn!(
                        "Direct ttl re-registration failed for inode {}: {}",
                        inode_id,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    pub fn apply_symlink(&self, parent: &InodeView, new_inode: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;
        batch.write_inode(new_inode)?;
        batch.add_child(parent.id(), new_inode.name(), new_inode.id())?;

        batch.commit()?;

        Ok(())
    }

    // Restore to a directory tree from rocksdb
    pub fn create_tree(&self) -> CommonResult<(i64, InodeView)> {
        let mut root = FsDir::create_root();
        let mut stack = LinkedList::new();
        stack.push_back((root.as_ptr(), ROOT_INODE_ID));
        let mut last_inode_id = ROOT_INODE_ID;
        let mut file_count = 0i64;
        let mut dir_count = 0i64;

        while let Some((mut parent, child_id)) = stack.pop_front() {
            last_inode_id = last_inode_id.max(child_id);

            let next_parent = if child_id != ROOT_INODE_ID {
                let inode = try_option!(self.store.get_inode(child_id)?);

                // Count files and directories during tree reconstruction
                match &inode {
                    InodeView::File(_) => file_count += 1,
                    InodeView::Dir(dir) => {
                        // Don't count root directory
                        if dir.id != ROOT_INODE_ID {
                            dir_count += 1;
                        }
                    }
                }

                parent.add_child(inode)?
            } else {
                parent
            };

            // Find all child nodes in the directory.
            if next_parent.is_dir() {
                let childs_iter = self.store.edges_iter(next_parent.id())?;
                for item in childs_iter {
                    let (_, value) = try_err!(item);
                    let child_id = RocksUtils::i64_from_bytes(&value)?;
                    stack.push_back((next_parent.clone(), child_id))
                }
            }

            if let Some(ttl_config) = next_parent.ttl_config() {
                let metadata = TtlInodeMetadata::new(next_parent.id() as u64, ttl_config.clone());
                if let Err(e) = self.ttl_bucket_list.add_inode(&metadata) {
                    log::warn!(
                        "Direct ttl registration failed during tree creation for inode {}: {}",
                        next_parent.id(),
                        e
                    );
                }
            }
        }

        // Update statistics with the counts from tree reconstruction
        self.fs_stats.set_counts(file_count, dir_count);

        Ok((last_inode_id, root))
    }

    pub fn get_file_locations(
        &self,
        file: &InodeFile,
    ) -> CommonResult<HashMap<i64, Vec<BlockLocation>>> {
        let mut res = HashMap::with_capacity(file.blocks.len());
        for meta in &file.blocks {
            let locs = self.store.get_locations(meta.id)?;
            res.insert(meta.id, locs);
        }

        Ok(res)
    }

    pub fn get_block_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        self.store.get_locations(block_id)
    }

    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        self.store.get_inode(id)
    }

    pub fn cf_hash(&self, cf: &str) -> u128 {
        let iter = self.store.iter_cf(cf).unwrap();
        let mut hash = 0;
        for inode in iter {
            let kv = inode.unwrap();
            hash += Utils::crc32(kv.0.as_ref()) as u128;
            hash += Utils::crc32(kv.1.as_ref()) as u128;
        }
        hash
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.store.db.create_checkpoint(id)
    }

    pub fn restore<T: AsRef<str>>(&mut self, path: T) -> CommonResult<()> {
        let conf = self.store.db.conf().clone();

        // The database points to a temporary directory.
        let tmp_path = Utils::temp_file();
        let tmp_conf = DBConf::new(tmp_path);
        self.store = Arc::new(RocksInodeStore::new(tmp_conf, false)?);

        // Delete the original file and move the checkpoint to the data directory.
        FileUtils::delete_path(&conf.data_dir, true)?;
        FileUtils::copy_dir(path.as_ref(), &conf.data_dir)?;

        self.store = Arc::new(RocksInodeStore::new(conf, false)?);
        Ok(())
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        self.store.db.get_checkpoint_path(id)
    }

    pub fn new_batch(&self) -> InodeWriteBatch<'_> {
        self.store.new_batch()
    }

    pub fn apply_mount(&self, id: u32, info: &MountInfo) -> CommonResult<()> {
        self.store.add_mountpoint(id, info)
    }

    pub fn apply_umount(&self, id: u32) -> CommonResult<()> {
        self.store.remove_mountpoint(id)
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        self.store.get_mount_info(id)
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        self.store.get_mount_table()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        self.fs_stats.counts()
    }
}
