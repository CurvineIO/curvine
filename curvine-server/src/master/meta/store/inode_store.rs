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
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::{
    DirEntry, DirTree, InodeDir, InodeFile, InodeView, ROOT_INODE_ID,
};
use crate::master::meta::store::{InodeWriteBatch, RocksInodeStore};
use crate::master::meta::{FileSystemStats, LockMeta};
use curvine_common::rocksdb::{DBConf, RocksUtils};
use curvine_common::state::{BlockLocation, CommitBlock, FileLock, MountInfo};
use orpc::common::{FileUtils, Utils};
use orpc::{err_box, try_err, CommonResult};
use std::collections::{HashMap, LinkedList};
use std::sync::Arc;
// Currently, only RockSDB is supported.
// Note: InodeStore is intentionally NOT Clone.
// Cloning InodeStore increases Arc<RocksInodeStore> refcount, which prevents
// the RocksDB lock from being released during Raft snapshot restore.
// If you need to share InodeStore, use Arc<InodeStore> or access it via FsDir.
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

    pub fn apply_add(
        &self,
        parent: &InodeView,
        child_name: &str,
        child: &InodeView,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(child)?;
        batch.write_inode(parent)?;
        batch.add_child(parent.id(), child_name, child.id())?;

        batch.commit()?;

        self.ttl_bucket_list.add(child);

        match child {
            InodeView::File(_) => self.fs_stats.increment_file_count(),
            InodeView::Dir(d) => {
                // Don't count root directory
                if d.id != ROOT_INODE_ID {
                    self.fs_stats.increment_dir_count();
                }
            }
        }

        Ok(())
    }

    pub fn apply_delete(
        &self,
        parent: &InodeView,
        child_name: &str,
        del: &InodeView,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();
        batch.write_inode(parent)?;

        let mut stack = LinkedList::new();
        stack.push_back((parent.id(), child_name.to_string(), del.clone()));
        let mut del_res = DeleteResult::new();
        let mut deleted_files = 0i64;
        let mut deleted_dirs = 0i64;

        while let Some((parent_id, name, inode)) = stack.pop_front() {
            batch.delete_child(parent_id, &name)?;
            del_res.inodes += 1;

            match &inode {
                InodeView::Dir(dir) => {
                    batch.delete_inode(inode.id())?;
                    self.ttl_bucket_list.remove(&inode);

                    if dir.id != ROOT_INODE_ID {
                        deleted_dirs += 1;
                    }

                    let childs_iter = self.store.edges_iter(inode.id())?;
                    for item in childs_iter {
                        let (key, value) = try_err!(item);
                        let (_, child_name) = RocksUtils::i64_str_from_bytes(&key).unwrap();
                        let child_id = RocksUtils::i64_from_bytes(&value)?;

                        if let Some(child_inode) = self.store.get_inode(child_id)? {
                            stack.push_back((inode.id(), child_name.to_string(), child_inode));
                        }
                    }
                }

                _ => {
                    deleted_files += 1;
                    let res = self.decrement_inode_nlink(inode.id(), &mut batch)?;
                    del_res.blocks.extend(res.blocks);
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

    pub fn apply_free(&self, inodes: Vec<InodeView>) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        for inode in inodes {
            batch.write_inode(&inode)?;
        }
        batch.commit()?;
        Ok(())
    }

    pub fn apply_rename(
        &self,
        src_parent: &InodeView,
        src_name: &str,
        dst_parent: &InodeView,
        dst_name: &str,
        dst_inode: &InodeView,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.delete_child(src_parent.id(), src_name)?;

        batch.write_inode(dst_inode)?;
        batch.add_child(dst_parent.id(), dst_name, dst_inode.id())?;

        batch.write_inode(src_parent)?;
        batch.write_inode(dst_parent)?;

        batch.commit()?;

        Ok(())
    }

    pub fn apply_new_block(
        &self,
        file: &InodeView,
        commit_blocks: &[CommitBlock],
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(file)?;
        for commit in commit_blocks {
            for item in &commit.locations {
                batch.add_location(commit.block_id, item)?;
            }
        }

        batch.commit()
    }

    pub fn apply_complete_file(
        &self,
        file: &InodeView,
        commit_blocks: &[CommitBlock],
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(file)?;
        for commit in commit_blocks {
            for item in &commit.locations {
                batch.add_location(commit.block_id, item)?;
            }
        }

        batch.commit()
    }

    pub fn apply_overwrite_file(&self, file: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.write_inode(file)?;
        batch.commit()
    }

    pub fn apply_reopen_file(&self, file: &InodeView) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.write_inode(file)?;
        batch.commit()
    }

    pub fn apply_set_attr(&self, inodes: Vec<InodeView>) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        for inode in &inodes {
            batch.write_inode(inode)?;
            self.ttl_bucket_list.add(inode);
        }
        batch.commit()?;

        Ok(())
    }

    pub fn apply_symlink(
        &self,
        parent: &InodeView,
        name: &str,
        new_inode: &InodeView,
        is_add: bool,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;
        batch.write_inode(new_inode)?;
        batch.add_child(parent.id(), name, new_inode.id())?;

        batch.commit()?;

        if is_add {
            self.fs_stats.increment_file_count();
        }

        Ok(())
    }

    pub fn apply_replace_inode(
        &self,
        parent: &InodeView,
        name: &str,
        old_inode_id: i64,
        new_inode: &InodeView,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;
        batch.write_inode(new_inode)?;
        batch.add_child(parent.id(), name, new_inode.id())?;

        if old_inode_id != new_inode.id() {
            batch.delete_inode(old_inode_id)?;
        }

        batch.commit()?;
        self.ttl_bucket_list.add(new_inode);
        Ok(())
    }

    pub fn apply_link(
        &self,
        parent: &InodeView,
        name: &str,
        original_inode_id: i64,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;

        batch.add_child(parent.id(), name, original_inode_id)?;

        self.increment_inode_nlink(original_inode_id, &mut batch)?;

        batch.commit()?;

        self.fs_stats.increment_file_count();

        Ok(())
    }

    fn increment_inode_nlink(
        &self,
        inode_id: i64,
        batch: &mut InodeWriteBatch<'_>,
    ) -> CommonResult<()> {
        if let Some(mut inode_view) = self.get_inode(inode_id, None)? {
            match &mut inode_view {
                InodeView::File(_) => {
                    inode_view.incr_nlink();
                    batch.write_inode(&inode_view)?;
                }
                _ => {
                    return err_box!("Cannot increment nlink for non-file inode {}", inode_id);
                }
            }
        } else {
            return err_box!("Inode {} not found when incrementing nlink", inode_id);
        }
        Ok(())
    }

    pub fn apply_unlink(
        &self,
        parent: &InodeView,
        child_name: &str,
        child_id: i64,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;

        batch.delete_child(parent.id(), child_name)?;

        let del_res = self.decrement_inode_nlink(child_id, &mut batch)?;

        batch.commit()?;

        self.fs_stats.decrement_file_count();

        Ok(del_res)
    }

    pub fn apply_unlink_file_entry(
        &self,
        parent: &InodeView,
        child_name: &str,
        inode_id: i64,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;

        batch.delete_child(parent.id(), child_name)?;

        let del_res = self.decrement_inode_nlink(inode_id, &mut batch)?;

        batch.commit()?;

        self.fs_stats.decrement_file_count();

        Ok(del_res)
    }

    // Helper method to decrement nlink count of an inode
    fn decrement_inode_nlink(
        &self,
        inode_id: i64,
        batch: &mut InodeWriteBatch<'_>,
    ) -> CommonResult<DeleteResult> {
        let mut del_res = DeleteResult::new();
        // Load the inode from storage
        if let Some(mut inode_view) = self.get_inode(inode_id, None)? {
            match &mut inode_view {
                InodeView::File(f) => {
                    let remaining_links = f.decrement_nlink();
                    if remaining_links == 0 {
                        batch.delete_inode(inode_id)?;

                        // Collect block info
                        del_res.blocks.extend(f.get_locs(self)?);

                        self.ttl_bucket_list.remove(&inode_view);
                    } else {
                        // Write the updated inode back to storage
                        batch.write_inode(&inode_view)?;
                    }
                }
                _ => {
                    return err_box!("Cannot decrement nlink for non-file inode {}", inode_id);
                }
            }
        } else {
            return err_box!("Inode {} not found when decrementing nlink", inode_id);
        }
        Ok(del_res)
    }

    pub fn create_blank_tree(&self) -> CommonResult<(i64, DirTree)> {
        let root_dir = InodeDir::new(ROOT_INODE_ID, 0);
        let root_view = InodeView::new_dir(root_dir.clone());

        let mut batch = self.store.new_batch();
        batch.write_inode(&root_view)?;
        batch.commit()?;

        self.ttl_bucket_list.add(&root_view);

        let root = DirTree::new(DirEntry::new_dir(root_dir));
        self.fs_stats.set_counts(0, 0);
        Ok((ROOT_INODE_ID, root))
    }

    pub fn create_tree(&self) -> CommonResult<(i64, DirTree)> {
        let root_inode = match self.store.get_inode(ROOT_INODE_ID)? {
            Some(v) => v,
            None => {
                return err_box!("Root inode not found in store");
            }
        };

        let root_dir = root_inode.as_dir_ref()?.clone();
        let mut tree = DirTree::new(DirEntry::new_dir(root_dir));

        self.ttl_bucket_list.add(&root_inode);

        let mut stack = LinkedList::new();
        stack.push_back(tree.root_key());
        let mut last_inode_id = ROOT_INODE_ID;
        let mut file_count = 0i64;
        let mut dir_count = 0i64;

        while let Some(parent_key) = stack.pop_front() {
            let parent_id = tree.entry(parent_key)?.id();
            let childs_iter = self.store.edges_iter(parent_id)?;
            for item in childs_iter {
                let (key, value) = try_err!(item);
                let (_key_parent_id, child_name) = RocksUtils::i64_str_from_bytes(&key).unwrap();
                let child_id = RocksUtils::i64_from_bytes(&value)?;

                last_inode_id = last_inode_id.max(child_id);

                let child_inode = match self.store.get_inode(child_id)? {
                    Some(v) => v,
                    None => {
                        log::warn!(
                            "create_tree: orphaned edge detected, child_id={} has no inode, skipping",
                            child_id
                        );
                        continue;
                    }
                };

                self.ttl_bucket_list.add(&child_inode);
                match &child_inode {
                    InodeView::Dir(d) => {
                        dir_count += 1;
                        let child_key = tree.insert_dir((**d).clone());
                        tree.entry_mut(parent_key)?
                            .add_child(child_name.to_string(), child_key);
                        stack.push_back(child_key);
                    }
                    InodeView::File(_) => {
                        file_count += 1;
                    }
                }
            }
        }

        // Update statistics
        self.fs_stats.set_counts(file_count, dir_count);

        Ok((last_inode_id, tree))
    }

    pub fn list_children(&self, parent_id: i64) -> CommonResult<Vec<(String, i64)>> {
        let childs_iter = self.store.edges_iter(parent_id)?;
        let mut children = Vec::new();
        for item in childs_iter {
            let (key, value) = try_err!(item);
            let (_parent_id, child_name) = RocksUtils::i64_str_from_bytes(&key).unwrap();
            let child_id = RocksUtils::i64_from_bytes(&value)?;
            children.push((child_name.to_string(), child_id));
        }
        Ok(children)
    }

    pub fn lookup_child_id(&self, parent_id: i64, child_name: &str) -> CommonResult<Option<i64>> {
        self.store.get_child_id_exact(parent_id, child_name)
    }

    pub fn lookup_child(
        &self,
        parent_id: i64,
        child_name: &str,
    ) -> CommonResult<Option<InodeView>> {
        match self.lookup_child_id(parent_id, child_name)? {
            Some(id) => self.get_inode(id, Some(child_name)),
            None => Ok(None),
        }
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

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> CommonResult<()> {
        let mut batch = self.store.new_batch();
        batch.add_location(block_id, &location)?;
        batch.commit()?;
        Ok(())
    }

    //get_inode should return the inode with the name of the FileEntry
    //TODO refactor: remove seq name from store_inode
    pub fn get_inode(&self, id: i64, name: Option<&str>) -> CommonResult<Option<InodeView>> {
        let mut inode_view = self.store.get_inode(id)?;
        if let Some(name) = name {
            if let Some(ref mut inode) = inode_view {
                inode.change_name(name.to_string());
            }
        }
        Ok(inode_view)
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
        // Check if there are other references to the Arc, which would prevent the lock from being released
        let ref_count = Arc::strong_count(&self.store);
        if ref_count > 1 {
            return err_box!(
                "cannot restore: RocksInodeStore has {} references (expected 1). \
                Other components are still holding clones of InodeStore, \
                which prevents RocksDB lock from being released.",
                ref_count
            );
        }

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

    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        self.store.get_locations(block_id)
    }

    pub fn get_locks(&self, id: i64) -> CommonResult<LockMeta> {
        self.store.get_locks(id)
    }

    pub fn apply_set_locks(&self, id: i64, lock: &[FileLock]) -> CommonResult<()> {
        self.store.set_locks(id, lock)
    }

    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        self.store.get_rocksdb_metrics()
    }

    pub fn store(&self) -> &RocksInodeStore {
        &self.store
    }

    pub fn has_root_inode(&self) -> CommonResult<bool> {
        Ok(self.store.get_inode(ROOT_INODE_ID)?.is_some())
    }
}
