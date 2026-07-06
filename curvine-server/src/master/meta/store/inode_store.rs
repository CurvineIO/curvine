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
use crate::master::meta::inode::{Inode, InodeFile, InodePath, InodePtr, InodeView, ROOT_INODE_ID};
use crate::master::meta::store::{InodeWriteBatch, RocksInodeStore};
use crate::master::meta::{FileSystemStats, FsDir, LockMeta};
use curvine_common::rocksdb::{DBConf, RocksUtils};
use curvine_common::state::{BlockLocation, CommitBlock, FileLock, FileStatus, MountInfo};
use curvine_common::utils::SerdeUtils;
use log::info;
use orpc::common::{FileUtils, Utils};
use orpc::{err_box, try_err, try_option, CommonResult};
use std::collections::{HashMap, HashSet, LinkedList, VecDeque};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Private helper structs for bulk-load snapshot restore (Issue #964)
// ---------------------------------------------------------------------------

/// Bulk-loaded metadata from RocksDB, used as input for in-memory tree assembly.
struct SnapshotData {
    /// parent_id -> list of (child_name, child_id) edges
    edges: HashMap<i64, Vec<(String, i64)>>,
    /// inode_id -> deserialized InodeView
    inodes: HashMap<i64, InodeView>,
}

/// Lightweight phase-timing / counter collector for create_tree restore.
struct RestoreTimer {
    phases: Vec<(&'static str, u64)>,
    edge_count: usize,
    inode_count: usize,
    orphaned_count: usize,
    repair_count: usize,
    file_count: i64,
    dir_count: i64,
    last_mark: std::time::Instant,
}

impl RestoreTimer {
    fn new() -> Self {
        Self {
            phases: Vec::new(),
            edge_count: 0,
            inode_count: 0,
            orphaned_count: 0,
            repair_count: 0,
            file_count: 0,
            dir_count: 0,
            last_mark: std::time::Instant::now(),
        }
    }

    fn mark(&mut self, name: &'static str) {
        let elapsed = self.last_mark.elapsed().as_millis() as u64;
        self.phases.push((name, elapsed));
        self.last_mark = std::time::Instant::now();
    }

    fn log_summary(&self) {
        let phase_str: Vec<String> = self
            .phases
            .iter()
            .map(|(name, ms)| format!("{}={}ms", name, ms))
            .collect();
        info!(
            "create_tree: edges={}, inodes={}, orphaned={}, repairs={}, files={}, dirs={}, phases=[{}]",
            self.edge_count,
            self.inode_count,
            self.orphaned_count,
            self.repair_count,
            self.file_count,
            self.dir_count,
            phase_str.join(", "),
        );
    }
}

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
    pub fn new(store: RocksInodeStore, ttl_bucket_list: Arc<TtlBucketList>) -> CommonResult<Self> {
        Ok(InodeStore {
            store: Arc::new(store),
            fs_stats: Arc::new(FileSystemStats::new()?),
            ttl_bucket_list,
        })
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

        self.ttl_bucket_list.add(child);

        match child {
            InodeView::File(_) => self.fs_stats.increment_file_count(),
            InodeView::Dir(d) => {
                // Don't count root directory
                if d.id != ROOT_INODE_ID {
                    self.fs_stats.increment_dir_count();
                }
            }
            InodeView::FileEntry(..) => self.fs_stats.increment_file_count(),
        }

        Ok(())
    }

    pub fn apply_delete(
        &self,
        parent: &InodeView,
        del: &InodeView,
        del_name: &str,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();
        batch.write_inode(parent)?;

        let mut stack = LinkedList::new();
        stack.push_back((parent.id(), del_name.to_string(), del.clone()));
        let mut del_res = DeleteResult::new();
        let mut deleted_files = 0i64;
        let mut deleted_dirs = 0i64;

        while let Some((parent_id, edge_name, inode)) = stack.pop_front() {
            // Delete inode edges
            batch.delete_child(parent_id, &edge_name)?;
            del_res.inodes += 1;

            match &inode {
                InodeView::Dir(dir) => {
                    batch.delete_inode(inode.id())?;
                    self.ttl_bucket_list.remove(&inode);

                    // Don't count root directory
                    if dir.id != ROOT_INODE_ID {
                        deleted_dirs += 1;
                    }
                    for item in dir.children_iter() {
                        stack.push_back((inode.id(), item.name().to_string(), item.clone()))
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
        src_inode: &InodeView,
        src_name: &str,
        dst_parent: &InodeView,
        dst_inode: &InodeView,
    ) -> CommonResult<()> {
        if src_inode.id() != dst_inode.id() {
            return err_box!(
                "rename inode id mismatch: source id {}, destination id {}",
                src_inode.id(),
                dst_inode.id()
            );
        }

        let mut batch = self.store.new_batch();

        // The edge name is the namespace key. The inode name is duplicated metadata
        // and may lag behind after older bugs or checkpoint restore.
        batch.delete_child(src_parent.id(), src_name)?;

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

    /// Persists a symlink inode and its directory edge under `parent`.
    /// `is_add`: create a new symlink dentry (adds a directory entry); bump live file count.
    /// `!is_add`: update an existing symlink dentry in place; file count unchanged.
    pub fn apply_symlink(
        &self,
        parent: &InodeView,
        new_inode: &InodeView,
        is_add: bool,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;
        batch.write_inode(new_inode)?;
        batch.add_child(parent.id(), new_inode.name(), new_inode.id())?;

        batch.commit()?;

        if is_add {
            self.fs_stats.increment_file_count();
        }

        Ok(())
    }

    pub fn apply_link(
        &self,
        parent: &InodeView,
        new_entry: &InodeView,
        original_inode_id: i64,
    ) -> CommonResult<()> {
        let mut batch = self.store.new_batch();

        batch.write_inode(parent)?;

        //link is a edge, link to same inode
        batch.add_child(parent.id(), new_entry.name(), original_inode_id)?;

        // Increment nlink count of the original inode (in the same batch for atomicity)
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
        child: &InodeView,
        child_name: &str,
    ) -> CommonResult<DeleteResult> {
        let mut batch = self.store.new_batch();

        // Write the updated parent directory (child will be removed by the caller)
        batch.write_inode(parent)?;

        // Remove the child from the parent's children list
        batch.delete_child(parent.id(), child_name)?;

        // Decrement nlink count of the file being unlinked.
        // If nlink reaches 0 the inode is also deleted and del_res.blocks will be populated.
        let del_res = if let InodeView::File(_) = child {
            self.decrement_inode_nlink(child.id(), &mut batch)?
        } else {
            DeleteResult::new()
        };

        batch.commit()?;

        self.fs_stats.decrement_file_count();

        Ok(del_res)
    }

    pub fn apply_unlink_file_entry(
        &self,
        parent: &InodeView,
        child: &InodeView,
        child_name: &str,
        inode_id: i64,
    ) -> CommonResult<DeleteResult> {
        if child.id() != inode_id {
            return err_box!(
                "unlink FileEntry->inode id mismatch: entry references inode id {}, caller passed inode id {}",
                child.id(),
                inode_id
            );
        }

        let mut batch = self.store.new_batch();

        // Write the updated parent directory
        batch.write_inode(parent)?;

        // Remove the FileEntry from the parent's children list
        batch.delete_child(parent.id(), child_name)?;

        // Decrement nlink count of the original inode.
        // If nlink reaches 0 the inode is also deleted and del_res.blocks will be populated.
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

    pub fn create_blank_tree(&self) -> CommonResult<(i64, InodeView)> {
        let root = FsDir::create_root();
        self.fs_stats.set_counts(0, 0);
        Ok((ROOT_INODE_ID, root))
    }

    // Restore to a directory tree from rocksdb.
    //
    // Two-phase bulk-load approach (Issue #964):
    //   Phase 1: Sequentially scan entire `edges` and `inodes` column families
    //            into in-memory HashMaps, eliminating O(N) point lookups.
    //            Inode deserialization is parallelised across available cores.
    //   Phase 2: Assemble the in-memory directory tree via BFS, moving InodeView
    //            values out of the HashMap (zero cloning) and applying the same
    //            repair logic as the previous per-inode-lookup implementation.
    pub fn create_tree(&self) -> CommonResult<(i64, InodeView)> {
        let mut timer = RestoreTimer::new();

        let data = self.load_snapshot_data(&mut timer)?;
        let (last_inode_id, root) = self.build_tree_from_data(data, &mut timer)?;

        self.fs_stats.set_counts(timer.file_count, timer.dir_count);
        timer.log_summary();

        Ok((last_inode_id, root))
    }

    /// Phase 1: Bulk-load all edges and inodes from RocksDB into memory.
    fn load_snapshot_data(&self, timer: &mut RestoreTimer) -> CommonResult<SnapshotData> {
        // --- 1a. Scan edges CF → group by parent_id ---
        let mut edges: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
        let edges_iter = self.store.bulk_scan_edges()?;
        for item in edges_iter {
            let (key, value) = try_err!(item);
            let (parent_id, child_name) = RocksUtils::i64_str_from_bytes(&key)?;
            let child_id = RocksUtils::i64_from_bytes(&value)?;
            edges
                .entry(parent_id)
                .or_default()
                .push((child_name.to_string(), child_id));
            timer.edge_count += 1;
        }
        timer.mark("scan_edges");

        // --- 1b. Scan inodes CF → collect raw bytes ---
        let mut raw_inodes: Vec<(i64, Vec<u8>)> = Vec::new();
        let inodes_iter = self.store.bulk_scan_inodes()?;
        for item in inodes_iter {
            let (key, value) = try_err!(item);
            let inode_id = RocksUtils::i64_from_bytes(&key)?;
            raw_inodes.push((inode_id, value.to_vec()));
        }
        timer.inode_count = raw_inodes.len();
        timer.mark("scan_inodes");

        // --- 1c. Parallel-deserialize inodes ---
        let inodes = Self::parallel_deserialize_inodes(raw_inodes)?;
        timer.mark("deserialize_inodes");

        Ok(SnapshotData { edges, inodes })
    }

    /// Parallel-deserialize raw inode bytes into a HashMap using std::thread::scope.
    /// Falls back to single-threaded for small datasets.
    fn parallel_deserialize_inodes(
        raw: Vec<(i64, Vec<u8>)>,
    ) -> CommonResult<HashMap<i64, InodeView>> {
        let total = raw.len();
        let mut inodes = HashMap::with_capacity(total);

        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(total.max(1));

        if num_threads <= 1 || total < 1000 {
            for (id, bytes) in raw {
                let inode: InodeView = SerdeUtils::deserialize(&bytes)?;
                inodes.insert(id, inode);
            }
            return Ok(inodes);
        }

        let chunk_size = total.div_ceil(num_threads);

        // Borrow raw as slices — no cloning of Vec<u8> payloads.
        // std::thread::scope guarantees all threads are joined before the
        // scope returns, so borrowing raw for the scope lifetime is safe.
        let thread_results = std::thread::scope(|s| -> CommonResult<Vec<Vec<(i64, InodeView)>>> {
            let handles: Vec<_> = raw
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || -> CommonResult<Vec<(i64, InodeView)>> {
                        let mut result = Vec::with_capacity(chunk.len());
                        for (id, bytes) in chunk {
                            let inode: InodeView = SerdeUtils::deserialize(bytes)?;
                            result.push((*id, inode));
                        }
                        Ok(result)
                    })
                })
                .collect();

            let mut all = Vec::with_capacity(handles.len());
            for h in handles {
                let inner = match h.join() {
                    Ok(result) => result,
                    Err(_) => return err_box!("thread panicked during inode deserialization"),
                };
                all.push(inner?);
            }
            Ok(all)
        })?;

        for chunk_result in thread_results {
            for (id, inode) in chunk_result {
                inodes.insert(id, inode);
            }
        }

        Ok(inodes)
    }

    /// Phase 2 + 3: Assemble the in-memory directory tree from bulk-loaded data.
    ///
    /// Uses BFS with `HashMap::remove()` to move `InodeView` values into the tree
    /// (zero cloning). Repair logic (orphaned edges, duplicate directory edges,
    /// name/parent mismatches) is preserved exactly from the previous implementation.
    fn build_tree_from_data(
        &self,
        mut data: SnapshotData,
        timer: &mut RestoreTimer,
    ) -> CommonResult<(i64, InodeView)> {
        // Load root metadata from the bulk-loaded inodes (or create default).
        let mut root = data
            .inodes
            .remove(&ROOT_INODE_ID)
            .unwrap_or_else(FsDir::create_root);

        // BFS stack: (parent_ptr, parent_id)
        let mut stack: VecDeque<(InodePtr, i64)> = VecDeque::new();
        stack.push_back((root.as_ptr(), ROOT_INODE_ID));

        let mut last_inode_id = ROOT_INODE_ID;
        let mut dir_edges: HashMap<i64, (i64, String)> = HashMap::new();
        let mut seen_ids: HashSet<i64> = HashSet::new();
        let mut repair_batch = self.store.new_batch();
        let mut has_repairs = false;

        while let Some((mut parent, parent_id)) = stack.pop_front() {
            // Look up children for this directory (O(1) HashMap — no RocksDB access).
            let children = match data.edges.get(&parent_id) {
                Some(c) => c,
                None => continue,
            };

            for (edge_name, child_id) in children {
                last_inode_id = last_inode_id.max(*child_id);

                // Check for duplicate directory edge BEFORE removing from inodes.
                // A directory can only have one parent; if we've already seen this
                // directory id, the current edge is a duplicate and must be deleted.
                if let Some((old_parent_id, old_name)) = dir_edges.get(child_id) {
                    log::warn!(
                        "create_tree: directory inode {} has multiple parent edges: keeping parent {} name '{}', dropping parent {} name '{}'",
                        child_id, old_parent_id, old_name, parent_id, edge_name
                    );
                    repair_batch.delete_child(parent_id, edge_name)?;
                    has_repairs = true;
                    timer.repair_count += 1;
                    continue;
                }

                // Check if this is a hard-linked file (already consumed for another edge).
                // Files can have multiple parent edges; each edge gets its own FileEntry.
                if seen_ids.contains(child_id) {
                    timer.file_count += 1;
                    let entry = InodeView::new_entry(edge_name.clone(), *child_id);
                    parent.add_child(entry)?;
                    continue;
                }

                // Move the inode out of the HashMap — zero cloning.
                // seen_ids is updated only after a successful lookup so that orphaned
                // edges (missing inode) are not recorded as "seen". Otherwise a second
                // edge to the same missing inode would be misclassified as a hard-link
                // and added to the tree as a FileEntry pointing at a non-existent inode.
                let store_inode = match data.inodes.remove(child_id) {
                    Some(v) => {
                        seen_ids.insert(*child_id);
                        v
                    }
                    None => {
                        // Orphaned edge: inode was deleted but edge was not cleaned up
                        // by older buggy metadata updates. Drop the edge durably so
                        // recovery does not keep replaying the same broken namespace entry.
                        log::warn!(
                            "create_tree: orphaned edge detected, parent_id={}, edge_name='{}', child_id={} has no inode, dropping edge",
                            parent_id,
                            edge_name,
                            child_id
                        );
                        repair_batch.delete_child(parent_id, edge_name)?;
                        has_repairs = true;
                        timer.orphaned_count += 1;
                        timer.repair_count += 1;
                        continue;
                    }
                };

                let inode = match store_inode {
                    InodeView::Dir(_) => {
                        let mut store_inode = store_inode;
                        dir_edges.insert(*child_id, (parent_id, edge_name.clone()));

                        // Name / parent_id mismatch repair
                        if store_inode.name() != edge_name.as_str()
                            || store_inode.as_dir_ref()?.parent_id() != parent_id
                        {
                            store_inode.change_name(edge_name.clone());
                            store_inode.set_parent_id(parent_id);
                            repair_batch.write_inode(&store_inode)?;
                            has_repairs = true;
                            timer.repair_count += 1;
                        }
                        self.ttl_bucket_list.add(&store_inode);
                        // Don't count root directory
                        if *child_id != ROOT_INODE_ID {
                            timer.dir_count += 1;
                        }
                        store_inode
                    }
                    _ => {
                        self.ttl_bucket_list.add(&store_inode);
                        timer.file_count += 1;
                        // Use lightweight FileEntry instead of the full inode
                        InodeView::new_entry(edge_name.clone(), *child_id)
                    }
                };

                let child_ptr = parent.add_child(inode)?;

                // If directory, push onto stack for its children
                if child_ptr.is_dir() {
                    stack.push_back((child_ptr, *child_id));
                }
            }
        }

        // Commit repairs atomically
        if has_repairs {
            repair_batch.commit()?;
        }
        timer.mark("build_tree");

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

    pub fn batched_get_inodes(
        &self,
        inp: &InodePath,
        list: Vec<&InodeView>,
    ) -> CommonResult<Vec<FileStatus>> {
        if list.is_empty() {
            return Ok(vec![]);
        }

        let mut out = vec![FileStatus::default(); list.len()];
        let mut scan_inodes = Vec::with_capacity(list.len());

        for (index, inode) in list.iter().enumerate() {
            if inode.is_file_entry() {
                scan_inodes.push((index, RocksUtils::i64_to_bytes(inode.id())));
            } else {
                let child_path = inp.child_path(inode.name());
                out[index] = inode.to_file_status(&child_path)?;
            }
        }

        if scan_inodes.is_empty() {
            return Ok(out);
        }

        let keys = scan_inodes.iter().map(|x| &x.1);
        let batch_res = self.store.batched_multi_get_inodes(keys, false)?;
        for (i, item) in batch_res.into_iter().enumerate() {
            let index = try_option!(
                scan_inodes.get(i),
                "batched_get_inodes: missing scan entry for batch result index {}",
                i
            )
            .0;
            let file_entry = try_option!(
                list.get(index),
                "batched_get_inodes: file entry index {} out of range, list len {}",
                index,
                list.len()
            );

            if let Some(bytes) = try_err!(item) {
                let mut inode: InodeView = SerdeUtils::deserialize(bytes.as_ref())?;
                if inode.id() != file_entry.id() {
                    return err_box!(
                        "batched_get_inodes: inode id mismatch for key index {} (expected id {}, stored inode id {})",
                        i,
                        file_entry.id(),
                        inode.id()
                    );
                }

                inode.change_name(file_entry.name().to_owned());
                let child_path = inp.child_path(file_entry.name());
                out[index] = inode.to_file_status(&child_path)?;
            } else {
                return err_box!(
                    "batched_get_inodes: inode missing in store path {}, id {}",
                    inp.child_path(file_entry.name()),
                    file_entry.id()
                );
            }
        }

        Ok(out)
    }

    pub fn cf_hash(&self, cf: &str) -> CommonResult<u128> {
        let iter = self.store.iter_cf(cf)?;
        let mut hash = 0;
        for inode in iter {
            let kv = inode?;
            hash += Utils::crc32(kv.0.as_ref()) as u128;
            hash += Utils::crc32(kv.1.as_ref()) as u128;
        }
        Ok(hash)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::meta::inode::{Inode, InodeDir, InodeFile, ROOT_INODE_ID};
    use crate::master::Master;

    fn new_store(name: &str) -> CommonResult<InodeStore> {
        Master::init_test_metrics();
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "inode-store-test/{}-{}",
            name,
            Utils::rand_str(6)
        )));
        let rocks = RocksInodeStore::new(conf, true)?;
        InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000)?))
    }

    #[test]
    fn apply_rename_deletes_source_edge_name_not_directory_inode_name() -> CommonResult<()> {
        let store = new_store("rename-source-edge-name")?;
        let root = FsDir::create_root();
        let src_parent = root.clone();
        let dst_parent = root.clone();
        let src_inode = InodeView::new_dir("inode-name".to_string(), InodeDir::new(2001, 0));
        let mut dst_inode = src_inode.clone();
        dst_inode.change_name("renamed".to_string());

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&src_inode)?;
            batch.add_child(ROOT_INODE_ID, "edge-name", src_inode.id())?;
            batch.commit()?;
        }

        store.apply_rename(
            &src_parent,
            &src_inode,
            "edge-name",
            &dst_parent,
            &dst_inode,
        )?;

        let (_, restored) = store.create_tree()?;
        let names: Vec<&str> = restored
            .children()
            .into_iter()
            .map(|child| child.name())
            .collect();
        assert_eq!(names, vec!["renamed"]);

        Ok(())
    }

    #[test]
    fn create_tree_drops_duplicate_directory_edges() -> CommonResult<()> {
        let store = new_store("drop-dir-alias")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("dir".to_string(), InodeDir::new(2002, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "a", dir.id())?;
            batch.add_child(ROOT_INODE_ID, "b", dir.id())?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let names: Vec<&str> = restored.children().into_iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["a"]);

        let edge_count = store.store.edges_iter(ROOT_INODE_ID)?.count();
        assert_eq!(edge_count, 1);

        Ok(())
    }

    #[test]
    fn create_tree_persists_directory_edge_hydration() -> CommonResult<()> {
        let store = new_store("persist-dir-edge-hydration")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("stale-name".to_string(), InodeDir::new(2003, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "edge-name", dir.id())?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let child = restored
            .get_child("edge-name")
            .expect("edge name should be hydrated into restored tree");
        assert_eq!(child.name(), "edge-name");

        let persisted = store
            .store
            .get_inode(dir.id())?
            .expect("directory inode should still exist");
        assert_eq!(persisted.name(), "edge-name");
        assert_eq!(persisted.as_dir_ref()?.parent_id(), ROOT_INODE_ID);

        Ok(())
    }

    /// Regression test for the seen_ids-before-lookup bug (PR #978 review):
    /// Two edges point to the same missing inode. The first edge takes the
    /// orphan path; the second must NOT be misclassified as a hard-link.
    #[test]
    fn create_tree_handles_duplicate_orphaned_edges() -> CommonResult<()> {
        let store = new_store("dup-orphan-edges")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("real-dir".to_string(), InodeDir::new(2020, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "real-dir", dir.id())?;
            // Two orphan edges pointing to the same missing inode (9999).
            batch.add_child(ROOT_INODE_ID, "orphan1", 9999)?;
            batch.add_child(ROOT_INODE_ID, "orphan2", 9999)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let names: Vec<&str> = restored.children().into_iter().map(|c| c.name()).collect();
        // Only the real directory should be in the tree; both orphaned edges are skipped.
        assert_eq!(names, vec!["real-dir"]);

        Ok(())
    }

    #[test]
    fn create_tree_handles_orphaned_edge() -> CommonResult<()> {
        let store = new_store("orphaned-edge")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("real-dir".to_string(), InodeDir::new(2010, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.add_child(ROOT_INODE_ID, "real-dir", dir.id())?;
            // Add an orphaned edge: edge points to an inode that does not exist.
            batch.add_child(ROOT_INODE_ID, "orphan", 9999)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        let names: Vec<&str> = restored.children().into_iter().map(|c| c.name()).collect();
        // Only the real directory should be in the tree; orphaned edge is skipped.
        assert_eq!(names, vec!["real-dir"]);

        Ok(())
    }

    #[test]
    fn create_tree_large_tree_counts() -> CommonResult<()> {
        let store = new_store("large-tree-counts")?;
        let root = FsDir::create_root();

        let num_dirs = 100;
        let files_per_dir = 10;
        let mut next_id: i64 = 3000;

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;

            for i in 0..num_dirs {
                let dir_id = next_id;
                next_id += 1;
                let dir_name = format!("d{}", i);
                let dir = InodeView::new_dir(dir_name.clone(), InodeDir::new(dir_id, 0));
                batch.write_inode(&dir)?;
                batch.add_child(ROOT_INODE_ID, &dir_name, dir_id)?;

                for j in 0..files_per_dir {
                    let file_id = next_id;
                    next_id += 1;
                    let file_name = format!("f{}", j);
                    let file = InodeView::new_file(file_name.clone(), InodeFile::new(file_id, 0));
                    batch.write_inode(&file)?;
                    batch.add_child(dir_id, &file_name, file_id)?;
                }
            }
            batch.commit()?;
        }

        let (_, _restored) = store.create_tree()?;
        // get_file_counts() returns (dir_count, file_count)
        let (dir_count, file_count) = store.get_file_counts();

        assert_eq!(file_count, (num_dirs * files_per_dir) as i64);
        assert_eq!(dir_count, num_dirs as i64);

        Ok(())
    }

    #[test]
    fn load_snapshot_data_correctness() -> CommonResult<()> {
        let store = new_store("snapshot-data-correctness")?;
        let root = FsDir::create_root();
        let dir = InodeView::new_dir("subdir".to_string(), InodeDir::new(5001, 0));
        let file = InodeView::new_file("note.txt".to_string(), InodeFile::new(5002, 0));

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.write_inode(&dir)?;
            batch.write_inode(&file)?;
            batch.add_child(ROOT_INODE_ID, "subdir", dir.id())?;
            batch.add_child(dir.id(), "note.txt", file.id())?;
            batch.commit()?;
        }

        let mut timer = RestoreTimer::new();
        let data = store.load_snapshot_data(&mut timer)?;

        // Verify edges
        assert_eq!(data.edges.len(), 2);
        let root_children = data.edges.get(&ROOT_INODE_ID).expect("root edges");
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].0, "subdir");
        assert_eq!(root_children[0].1, dir.id());

        let dir_children = data.edges.get(&dir.id()).expect("dir edges");
        assert_eq!(dir_children.len(), 1);
        assert_eq!(dir_children[0].0, "note.txt");
        assert_eq!(dir_children[0].1, file.id());

        // Verify inodes
        assert_eq!(data.inodes.len(), 3);
        assert!(data.inodes.contains_key(&ROOT_INODE_ID));
        assert!(data.inodes.contains_key(&dir.id()));
        assert!(data.inodes.contains_key(&file.id()));

        Ok(())
    }

    /// Regression benchmark: 100 dirs × 1000 files = ~100K inodes.
    /// Run with: cargo test -p curvine-server --lib --release -- \
    ///   master::meta::store::inode_store::tests::bench_create_tree_large --ignored --nocapture
    #[test]
    #[ignore]
    fn bench_create_tree_large() -> CommonResult<()> {
        let store = new_store("bench-large")?;
        let root = FsDir::create_root();

        let num_dirs = 100;
        let files_per_dir = 1000;
        let mut next_id: i64 = 100000;

        // Write all inodes and edges in a single batch for speed.
        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;

            for i in 0..num_dirs {
                let dir_id = next_id;
                next_id += 1;
                let dir_name = format!("d{}", i);
                let dir = InodeView::new_dir(dir_name.clone(), InodeDir::new(dir_id, 0));
                batch.write_inode(&dir)?;
                batch.add_child(ROOT_INODE_ID, &dir_name, dir_id)?;

                for j in 0..files_per_dir {
                    let file_id = next_id;
                    next_id += 1;
                    let file_name = format!("f{}", j);
                    let file = InodeView::new_file(file_name.clone(), InodeFile::new(file_id, 0));
                    batch.write_inode(&file)?;
                    batch.add_child(dir_id, &file_name, file_id)?;
                }
            }
            batch.commit()?;
        }

        let total_inodes = 1 + num_dirs + num_dirs * files_per_dir;
        println!(
            "bench: {} inodes ({} dirs × {} files + root)",
            total_inodes, num_dirs, files_per_dir
        );

        let start = std::time::Instant::now();
        let (last_inode_id, _root) = store.create_tree()?;
        let elapsed_ms = start.elapsed().as_millis();

        let (dir_count, file_count) = store.get_file_counts();
        println!(
            "bench: create_tree total={} ms, last_inode_id={}, dir_count={}, file_count={}",
            elapsed_ms, last_inode_id, dir_count, file_count
        );

        assert_eq!(file_count, (num_dirs * files_per_dir) as i64);
        assert_eq!(dir_count, num_dirs as i64);

        // Assert total < 10 seconds for ~100K inodes.
        assert!(
            elapsed_ms < 10_000,
            "create_tree took {} ms (> 10s) for {} inodes",
            elapsed_ms,
            total_inodes
        );

        Ok(())
    }
}
