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

use crate::master::fs::{BlockInodeState, DeleteResult};
use crate::master::journal::{
    CreateFileEntry, DeleteEntry, JournalEntry, JournalWriter, MkdirEntry, RenameEntry,
};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::*;
use crate::master::meta::store::{
    InodeStore, InodeStoreSnapshot, PurgeItem, PurgeTarget, RocksInodeStore,
};
use crate::master::meta::{trash, BlockMeta, FileSystemStats, InodeId};
use crate::master::quota::eviction::evictor::Evictor;
use curvine_config::ClusterConf;
use curvine_core_error::CommonError;
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    BlockLocation, CommitBlock, CreateFileOpts, DirectoryAttributeDelta, ExtendedBlock,
    FileAllocOpts, FileGen, FileLock, FileStatus, FreeResult, LeaderSnapshotInfo, ListOptions,
    MkdirOpts, MountInfo, RenameFlags, SetAttrOpts, StorageState, TrashEntry, TtlAction, TtlType,
    WorkerAddress, INTERNAL_CTIME_XATTR, INTERNAL_TTL_TYPE_XATTR,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, LinkedList};
use std::sync::Arc;

const MODE_SETGID: u32 = 0o2000;

/// In-memory inode and namespace mutations require either the exclusive
/// filesystem guard or the inode locks owned by the sharded write path.
pub struct FsDir {
    /// Live root directory. Held as `Arc<DirNode>` for the same reason every
    /// other directory edge is: the node owns its children index and
    /// attribute state, so readers and writers never need a projected view.
    pub(crate) root: Arc<DirNode>,
    pub(crate) inode_id: Arc<InodeId>,
    pub(crate) store: InodeStore,
    pub(crate) journal_writer: Arc<JournalWriter>,
    pub(crate) evictor: Arc<dyn Evictor>,
    pub(crate) op_id: Arc<AtomicCounter>,
    /// Max time `restore()` waits for active metadata scan leases to release
    /// the store before aborting (leaves the original DB/tree untouched).
    restore_wait_timeout: std::time::Duration,
}

#[derive(Default)]
pub(crate) struct CacheInvalidationResult {
    pub delete_result: DeleteResult,
    pub invalidated_block_ids: HashSet<i64>,
}

impl CacheInvalidationResult {
    pub(crate) fn extend(&mut self, other: Self) {
        self.delete_result.inodes += other.delete_result.inodes;
        for (block_id, locations) in other.delete_result.blocks {
            self.delete_result
                .blocks
                .entry(block_id)
                .or_default()
                .extend(locations);
        }
        self.invalidated_block_ids
            .extend(other.invalidated_block_ids);
    }
}

// One path claimed by a purge batch. Kept until the store batch commits so the
// in-memory tree can be updated with the same state that was persisted.
// The claimed child is not kept as a pointer: `InodePath::resolve` hydrates a
// terminal `FileEntry` into an owned inode that is freed when the path drops.
struct PurgeClaim {
    parent_handle: ParentMut,
    parent: InodeView,
    child_id: i64,
    name: String,
    is_dir: bool,
    dir_meta: Option<InodeView>,
}

// One page of a trash listing. `next` is the sort key of the last scanned
// entry and is set only when more names remain, so the caller can resume
// without rescanning what it already received.
pub struct TrashListing {
    pub entries: Vec<TrashEntry>,
    pub next: Option<trash::TrashCursor>,
}

#[derive(Debug)]
pub struct RestoredTrashEntry {
    pub dst: String,
    pub status: FileStatus,
    pub replaced: Option<DeleteResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicationBlockState {
    pub replicas: u8,
    pub locations: Vec<BlockLocation>,
}

/// One bounded page of a low-cost trash scan. Only expired entry paths are
/// materialized; `next_after` is the cursor for the next page, or `None` when
/// the bin has been fully scanned.
#[derive(Debug, Default)]
pub struct TrashScanPage {
    pub paths: Vec<String>,
    pub next_after: Option<String>,
}

/// set_attr publication target: directories publish through the live
/// `DirNode` (ArcSwap + attributes), files through the owned view the
/// caller holds. A projected view never reaches the tree.
enum SetAttrTarget {
    /// Live node directory publication plus the caller's projected box
    /// (refreshed in place after publish so status reads see the new acl).
    Dir(Arc<DirNode>),
    View(InodePtr),
}

impl SetAttrTarget {}

enum ParentMut {
    Live(Arc<DirNode>),
    View(InodePtr),
}

impl ParentMut {
    fn get(inp: &InodePath, pos: i32) -> FsResult<Self> {
        if let Some(node) = inp.get_dir_node(pos) {
            return Ok(Self::Live(node));
        }
        let ptr = inp
            .get_inode(pos)
            .ok_or_else(|| FsError::common("parent path not resolved"))?;
        Ok(Self::View(ptr))
    }

    fn id(&self) -> i64 {
        match self {
            Self::Live(node) => node.id,
            Self::View(ptr) => ptr.id(),
        }
    }

    fn mtime(&self) -> i64 {
        match self {
            Self::Live(node) => node.directory_attributes().mtime,
            Self::View(ptr) => ptr.mtime(),
        }
    }

    fn ctime(&self) -> i64 {
        match self {
            Self::Live(node) => node.directory_attributes().ctime,
            Self::View(ptr) => ptr.ctime(),
        }
    }

    /// Detached persistence view for store batch inputs. Writes on it never
    /// reach the tree; live updates go through the mutation methods. `name`
    /// must be the edge name of this directory so store writes keep the
    /// record's identity (the node itself carries no name).
    fn view(&self, name: &str) -> FsResult<InodeView> {
        match self {
            Self::Live(node) => Ok(InodeDir::from_dir_node(name.to_string(), node.clone())),
            Self::View(ptr) => Ok(ptr.as_ref().clone()),
        }
    }

    /// Caller-computed delta with the live node's base decision: Live reads
    /// the node's persisted flag and base; View applies the pre-S3 projection
    /// semantics (base from the borrowed view's attribute state).
    fn directory_attribute_delta(
        &self,
        raw: DirectoryAttributeDelta,
    ) -> FsResult<DirectoryAttributeDelta> {
        match self {
            // Live node: base/persisted decisions belong to DirNode's state
            // (persisted=true → raw delta; persisted=false → fixed base).
            Self::Live(node) => Ok(node.directory_attribute_delta(raw)),
            Self::View(ptr) => Ok(ptr.as_dir_ref()?.directory_attribute_delta(raw)),
        }
    }

    fn children_handle(&self) -> FsResult<Arc<DirectoryChildren>> {
        match self {
            Self::Live(node) => Ok(node.children.clone()),
            Self::View(ptr) => Ok(ptr.as_dir_ref()?.children_handle()),
        }
    }

    /// Live mtime/nlink/ctime updates must go through [`Self::apply_delta`]
    /// so the live state matches the persisted merge semantics. This shim
    /// stays for the legacy `View` path only.
    // Legacy per-field shims: only the `View` arm still performs a write.
    // The `Live` arm routes through the same atomic delta as the direct
    // publish path, and errors propagate so callers fence via
    // fail_metadata_publication instead of silently losing the update.
    fn update_mtime(&mut self, mtime: i64) -> FsResult<()> {
        match self {
            Self::Live(node) => {
                let delta = DirectoryAttributeDelta::new(mtime, mtime, 0);
                node.apply_attribute_delta(delta)
                    .map_err(|e| FsError::common(e.to_string()))
            }
            Self::View(ptr) => {
                ptr.as_mut().update_mtime(mtime);
                Ok(())
            }
        }
    }

    fn add_child(&self, inode: InodeView) -> CommonResult<ChildAddResult> {
        match self {
            Self::Live(node) => node.add_child_edge(inode),
            Self::View(ptr) => ptr.as_mut().add_child(inode),
        }
    }

    fn delete_child(&self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        match self {
            Self::Live(node) => node.delete_child_edge(child_id, child_name),
            Self::View(ptr) => ptr.as_mut().delete_child(child_id, child_name),
        }
    }

    fn publish_directory_delta(&self, delta: DirectoryAttributeDelta) -> CommonResult<()> {
        match self {
            Self::Live(node) => node.apply_attribute_delta(delta),
            Self::View(ptr) => ptr.as_dir_ref()?.apply_directory_attribute_delta(delta),
        }
    }

    fn mark_persisted(&self) -> CommonResult<()> {
        match self {
            Self::Live(node) => {
                node.mark_attributes_persisted();
                Ok(())
            }
            Self::View(ptr) => {
                ptr.as_dir_ref()?.mark_directory_attributes_persisted();
                Ok(())
            }
        }
    }

    fn validate_child_name(&self, name: &str) -> FsResult<()> {
        Ok(self.children_handle()?.validate_child_name(name)?)
    }
}

impl FsDir {
    fn publish_metadata<T>(&self, result: CommonResult<T>) -> CommonResult<T> {
        self.publish_metadata_impl(result)
    }

    fn publish_metadata_impl<T>(&self, result: CommonResult<T>) -> CommonResult<T> {
        result.inspect_err(|_error| {
            self.journal_writer.fail_metadata_publication();
        })
    }

    pub fn new(
        conf: &ClusterConf,
        journal_writer: Arc<JournalWriter>,
        ttl_bucket_list: Arc<TtlBucketList>,
        evictor: Arc<dyn Evictor>,
    ) -> FsResult<Self> {
        InodeChildren::set_container(conf.master.children_container)?;
        info!(
            "master children container: {:?}",
            conf.master.children_container
        );

        let db_conf = conf.db_conf();

        let store = RocksInodeStore::new(db_conf, conf.format_master)?;
        let state = InodeStore::new(store, ttl_bucket_list)?;
        state.migrate_directory_attributes()?;
        state.initialize_root_directory_attributes()?;
        // Rebuild the live tree from the local store; create_tree supplies a
        // blank root for a fresh format. Store rows and the journal cursor are
        // persisted independently, so this is not a proof of crash consistency.
        let (last_inode_id, root_view) = state.create_tree()?;
        root_view.as_dir_ref()?;

        let fs_dir = Self {
            root: DirNode::from_view(&root_view),
            inode_id: Arc::new(InodeId::new()),
            store: state,
            journal_writer,
            evictor,
            op_id: Arc::new(AtomicCounter::new(0)),
            restore_wait_timeout: std::time::Duration::from_millis(
                conf.master.dir_space.restore_wait_timeout_ms(),
            ),
        };
        fs_dir.update_last_inode_id(last_inode_id)?;

        Ok(fs_dir)
    }

    // Create root directory
    pub fn create_root() -> InodeView {
        InodeView::new_dir(ROOT_INODE_NAME.to_string(), InodeDir::new(ROOT_INODE_ID, 0))
    }

    /// Live root handle. Every root write goes through this node, so the
    /// namespace never hands out a projected root pointer.
    pub fn root_node(&self) -> Arc<DirNode> {
        self.root.clone()
    }

    /// Detached attribute projection of the root for DTO-shaped callers
    /// (metadata snapshot, tree dump). Attribute writes stay local to the
    /// projection; child mutations still reach the shared live container, so
    /// this is read-only by contract.
    pub fn root_view(&self) -> InodeView {
        InodeDir::from_dir_node_live(ROOT_INODE_NAME.to_string(), self.root.clone())
    }

    fn next_inode_id(&self) -> FsResult<i64> {
        let id = self.inode_id.next()?;
        Ok(id)
    }

    pub fn next_op_id(&self) -> u64 {
        self.op_id.next()
    }

    /// Rebuild the absolute path of an inode by walking parent_id links.
    ///
    /// Used when an RPC addresses a file by inode id: the caller-supplied path may be
    /// stale after rename, but journal / FileStatus / UFS export still need the current path.
    pub fn get_inode_path(&self, inode_id: i64) -> FsResult<String> {
        self.store.ensure_available()?;
        if inode_id == ROOT_INODE_ID {
            return Ok("/".to_string());
        }
        // The caller holds the namespace guard, keeping the store alive during restore.
        let snapshot = InodeStoreSnapshot::new(self.store.checked_read_snapshot()?);
        Self::inode_path_in_snapshot(&snapshot, inode_id)
    }

    pub(crate) fn inode_path_in_snapshot(
        snapshot: &InodeStoreSnapshot<'_>,
        inode_id: i64,
    ) -> FsResult<String> {
        if inode_id == ROOT_INODE_ID {
            return Ok("/".to_string());
        }
        let mut current_id = inode_id;
        let mut components = Vec::new();
        let mut visited = Vec::new();

        while current_id != ROOT_INODE_ID {
            if visited.contains(&current_id) {
                return err_box!("Cycle detected while resolving inode path {}", inode_id);
            }
            visited.push(current_id);

            let inode_view = match snapshot.get_inode(current_id)? {
                Some(inode_view) => inode_view,
                None => {
                    return err_ext!(FsError::file_not_found(format!(
                        "inode_id={} (missing ancestor {})",
                        inode_id, current_id
                    )));
                }
            };

            match &inode_view {
                File(f) => {
                    components.push(f.name.clone());
                    current_id = f.parent_id();
                }
                Dir(d) => {
                    components.push(d.name.clone());
                    current_id = d.parent_id();
                }
                FileEntry(e) => {
                    // Top-level inode CF should not store FileEntry rows; a truncated
                    // name-only path would be unsafe for UFS mount selection.
                    return err_box!(
                        "Cannot resolve path for inode {}: unexpected FileEntry '{}'",
                        inode_id,
                        e.name
                    );
                }
            }
        }

        components.reverse();
        Ok(format!("/{}", components.join("/")))
    }

    pub fn update_op_id(&self, op_id: u64) {
        Self::advance_op_id(&self.op_id, op_id);
    }

    /// Store an opaque journal cursor row. Metadata and cursor are separate
    /// writes, so sharing a RocksDB does not guarantee crash consistency.
    pub fn store_journal_applied(&self, data: &[u8]) -> CommonResult<()> {
        self.store
            .store
            .put_common_kv(RocksInodeStore::JOURNAL_APPLIED_KEY, data)
    }

    pub fn load_journal_applied(&self) -> CommonResult<Option<Vec<u8>>> {
        self.store
            .store
            .get_common_kv(RocksInodeStore::JOURNAL_APPLIED_KEY)
    }

    pub(crate) fn advance_op_id(counter: &AtomicCounter, op_id: u64) {
        let mut current = counter.get();
        while op_id > current {
            if counter.compare_and_set(current, op_id) {
                return;
            }
            current = counter.get();
        }
    }

    pub fn get_ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.store.get_ttl_bucket_list()
    }

    pub fn mkdir(&self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        // Create parent directory
        inp = self.create_parent_dir(inp, opts.parent_opts())?;

        // Create the final directory.
        inp = self.create_single_dir(inp, opts)?;
        Ok(inp)
    }

    // Create the first subdirectory that does not exist.
    // 1. If all directories on the path already exist, skip and return successful.
    // 2. If the parent directory does not exist, an error is returned.
    fn create_single_dir(&self, mut inp: InodePath, mut opts: MkdirOpts) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() - 1;
        let name = inp.get_component(pos + 1)?.to_string();

        if let Some(group) = Self::setgid_parent_group(&inp)? {
            opts.group = group;
            opts.mode |= MODE_SETGID;
        }

        let dir = InodeDir::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);

        inp = self.add_last_inode(inp, InodeView::new_dir(name.clone(), dir.clone()))?;

        let parent_path = inp.get_valid_parent_path();
        self.journal_writer.send_and_wait(self, || {
            JournalEntry::Mkdir(MkdirEntry {
                op_id: self.next_op_id(),
                rpc_id: 0,
                path: parent_path,
                dir,
            })
        })?;

        Ok(inp)
    }

    fn setgid_parent_group(inp: &InodePath) -> FsResult<Option<String>> {
        if inp.existing_len() == 0 {
            return Ok(None);
        }
        let parent_pos = inp.existing_len() as i32 - 1;
        let parent = match inp.get_inode(parent_pos) {
            Some(parent) => parent,
            None => return Ok(None),
        };
        let acl = parent.as_ref().acl()?;
        if acl.mode & MODE_SETGID != 0 {
            Ok(Some(acl.group.clone()))
        } else {
            Ok(None)
        }
    }

    fn mkdir_opts_for_intermediate_parent(
        base: &MkdirOpts,
        mount_table: &[MountInfo],
        cv_path: &str,
    ) -> MkdirOpts {
        let mut opts = base.parent_opts();
        if let Some(mount) = MountInfo::find_for_cv_path(mount_table, cv_path) {
            if let Some(policy) = mount.intermediate_dir_storage_policy() {
                opts.storage_policy = policy;
            }
        }
        opts
    }

    // Create all previous directories that may be missing on the path.
    fn create_parent_dir(&self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        let mut index = inp.existing_len();

        // The parent directory already exists and does not need to be created.
        if inp.is_full() || index + 1 >= inp.len() {
            return Ok(inp);
        }

        let mount_table = self.get_mount_table()?;

        while index <= inp.len() - 2 {
            let cv_path = inp.get_path(inp.existing_len() + 1);
            let parent_opts =
                Self::mkdir_opts_for_intermediate_parent(&opts, &mount_table, &cv_path);
            inp = self.create_single_dir(inp, parent_opts)?;
            index += 1;
        }

        Ok(inp)
    }

    // Delete files or directories
    pub fn delete(&self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(inp.path()));
        }

        if !inp.is_empty_dir() && !recursive {
            return err_ext!(FsError::dir_not_empty(inp.path()));
        }

        let del_res = self.unprotected_delete(inp, op_ms as i64)?;
        self.journal_writer.send_and_wait(self, || {
            JournalEntry::Delete(DeleteEntry {
                op_id: self.next_op_id(),
                rpc_id: 0,
                path: inp.path().to_string(),
                mtime: op_ms as i64,
            })
        })?;

        Ok(del_res)
    }

    /// Parent-side mutation surface: live `DirNode` when the edge stores one,
    /// otherwise the legacy borrowed `InodePtr` view. Container-switch shim;
    /// the `View` arm disappears with the `Box<InodeView>` container.
    pub(crate) fn unprotected_delete(&self, inp: &InodePath, mtime: i64) -> FsResult<DeleteResult> {
        let target = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("Path not exists: {}", inp.path()),
        };

        let mut parent = ParentMut::get(inp, -2)?;
        let parent_name = inp.get_component_neg(-2)?.to_string();
        let child = target.as_ref();
        let child_name = inp.name();

        // An already-expired parent TTL must keep its mtime: the checker can
        // delete child files first, and bumping mtime would un-expire the
        // directory for the rest of the same cleanup pass. Stage every live
        // mutation until the namespace batch has committed.
        // One projection is enough: it is a pure read of the parent, and the
        // expiry check below is the only thing the first one was for. Building
        // it twice allocated the children/attribute Arcs and cloned the features
        // and storage policy twice inside the serialised write path.
        //
        // KNOWN UNRESOLVED: a test was written for this decision (an
        // already-expired parent must keep its mtime) and it FAILED against the
        // original code too (mtime moved 841 -> 862), both with the default
        // TtlType and with TtlType::Atime, so the expiry is not reaching the
        // condition as the comment above claims. The cause was not found: the
        // path from MkdirOpts.x_attr through DirNode.x_attr to
        // expiration_ms() -> ttl_base_ms() still needs tracing. This may be a
        // pre-existing defect. The merge below is safe because both views were
        // pure reads of the same value, but the TTL behaviour itself is NOT
        // verified by a test and must not be reported as working.
        let parent_view_value = parent.view(&parent_name)?;
        let update_parent_mtime = !matches!(
            parent_view_value.expiration_ms(),
            Ok(Some(exp)) if mtime > exp
        );
        let write_parent = parent.id() == ROOT_INODE_ID
            || child.is_dir()
            || (update_parent_mtime && mtime > parent.mtime());
        let mut staged_parent = write_parent.then(|| parent_view_value.clone_metadata());
        if let Some(staged) = &mut staged_parent {
            if update_parent_mtime {
                staged.update_mtime(mtime);
            }
            if child.is_dir() {
                staged.dec_nlink(mtime);
            }
        }
        let persisted_parent: &InodeView = staged_parent.as_ref().unwrap_or(&parent_view_value);

        // The dir-attr merge takes `max(mtime)`, so handing it the deletion
        // timestamp would refresh a parent that is already past its own TTL —
        // the same un-expiry the row mtime guard above prevents. Keep the
        // delta on the guarded value.
        let delta_mtime = if update_parent_mtime {
            mtime
        } else {
            parent.mtime()
        };
        let parent_delta = parent.directory_attribute_delta(
            DirectoryAttributeDelta::new(delta_mtime, delta_mtime, -i32::from(child.is_dir()))
                .for_child_removal(),
        )?;
        let del_res = match child {
            File(f) => {
                if f.nlink() > 1 {
                    self.store.apply_unlink(
                        persisted_parent,
                        child,
                        child_name,
                        mtime,
                        parent_delta,
                    )?
                } else {
                    // This is the last link, delete the inode
                    self.store.apply_delete(
                        persisted_parent,
                        child,
                        child_name,
                        mtime,
                        write_parent,
                        parent_delta,
                    )?
                }
            }
            FileEntry(e) => {
                // This is a link entry, just remove the directory entry
                // The actual inode's nlink count should be decremented
                self.store.apply_unlink_file_entry(
                    persisted_parent,
                    child,
                    child_name,
                    e.id,
                    mtime,
                    parent_delta,
                )?
            }
            Dir(_) => {
                // Directories are always deleted
                self.store.apply_delete(
                    persisted_parent,
                    child,
                    child_name,
                    mtime,
                    write_parent,
                    parent_delta,
                )?
            }
        };

        // R8: real publication-failure injection at the true boundary —
        // the store batch has committed (durable above) and NO live
        // mutation has been applied yet. The fail_metadata_publication
        // records the real publication source; the caller's guard then
        // terminalizes the holder while the locks are still held.
        crate::fault_point! {
            sync,
            name: "master.metadata.recursive.publish_after_commit",
            description: "Inject a publication failure after the durable commit, before live publication",
            context: { "child_name" => child_name },
            return_error: |fault| {
                self.journal_writer.fail_metadata_publication();
                Err(fault.message.into())
            },
        }
        if update_parent_mtime {
            self.publish_metadata(parent.update_mtime(mtime).map_err(CommonError::from))?;
        }
        if matches!(child, File(f) if f.nlink() > 1) {
            if let File(file) = target.as_mut() {
                file.decrement_nlink(mtime);
            }
        }

        let delta = DirectoryAttributeDelta::new(
            parent.mtime(),
            parent.ctime(),
            -i32::from(child.is_dir()),
        )
        .for_child_removal();
        self.publish_metadata(parent.publish_directory_delta(delta))?;
        self.publish_metadata(parent.mark_persisted())?;

        // After deletion occurs, the target address cannot be used.
        let _ = self.publish_metadata(parent.delete_child(child.id(), child_name))?;
        Ok(del_res)
    }

    /// Removes the given paths without journaling one record per path.
    ///
    /// The trash purger supplies bounded, leaf-first batches, so every listed
    /// path is a file or a directory the earlier paths of the same batch empty.
    /// The whole batch is staged in one store write batch: either all listed
    /// paths are removed or none is, so a failure never leaves paths durable
    /// without the block locations they returned.
    ///
    /// Paths that no longer resolve are skipped, which keeps a replayed batch
    /// idempotent. A directory that still holds children after the paths
    /// already claimed in this batch is skipped as well, so a purge never
    /// removes inodes the batch did not list.
    pub fn purge_paths(&self, paths: &[String], mtime: i64) -> FsResult<DeleteResult> {
        let mut claims: Vec<PurgeClaim> = Vec::new();
        // (parent id, name) pairs already claimed, so a duplicated path is not
        // counted twice.
        let mut claimed: HashSet<(i64, String)> = HashSet::new();
        // Children claimed per directory: a later directory entry in the same
        // batch is empty once these paths are gone.
        let mut pending: HashMap<i64, HashSet<String>> = HashMap::new();
        // Parent metadata staged once per directory, with this batch's mtime
        // and nlink updates applied.
        let mut parents: HashMap<i64, InodeView> = HashMap::new();

        for path in paths {
            // A missing path resolves to a truncated InodePath and is skipped
            // below; an error here is a store failure, not a missing path, and
            // must fail replay instead of advancing past an undeleted inode.
            let inp = InodePath::resolve(self.root_node(), path, &self.store)?;

            let Some(child) = inp.get_last_inode() else {
                debug!("purge: skip vanished path {}", path);
                continue;
            };
            let parent_handle = ParentMut::get(&inp, -2)?;
            let parent = parent_handle.view(inp.get_component_neg(-2)?)?;
            let name = inp.name().to_string();
            if claimed.contains(&(parent.id(), name.clone())) {
                continue;
            }

            let is_dir = child.as_ref().is_dir();
            if let Dir(dir) = child.as_ref() {
                let removed = pending.get(&child.id()).map_or(0, HashSet::len);
                if dir.children_len() > removed {
                    warn!(
                        "purge: skip non-empty directory {} ({} unlisted children)",
                        path,
                        dir.children_len() - removed
                    );
                    continue;
                }
            }

            let parent_meta = parents
                .entry(parent.id())
                .or_insert_with(|| parent.clone_metadata());
            let expired = matches!(parent_meta.expiration_ms(), Ok(Some(exp)) if mtime > exp);
            if !expired {
                parent_meta.update_mtime(mtime);
            }
            if is_dir {
                parent_meta.dec_nlink(mtime);
            }

            let dir_meta = if is_dir {
                Some(child.as_ref().clone_metadata())
            } else {
                None
            };

            claimed.insert((parent.id(), name.clone()));
            pending.entry(parent.id()).or_default().insert(name.clone());
            claims.push(PurgeClaim {
                parent_handle,
                parent,
                child_id: child.id(),
                name,
                is_dir,
                dir_meta,
            });
        }

        if claims.is_empty() {
            return Ok(DeleteResult::new());
        }

        let items = claims
            .iter()
            .map(|claim| PurgeItem {
                parent: parents[&claim.parent.id()].clone_metadata(),
                target: match claim.dir_meta {
                    Some(ref dir) => PurgeTarget::Dir(dir.clone()),
                    None => PurgeTarget::File(claim.child_id),
                },
                name: claim.name.clone(),
            })
            .collect();

        // The store batch is the only durable change; the in-memory tree and
        // file counts are published only after it commits.
        // Deltas are computed from the live nodes (base + persisted decisions
        // belong to DirNode), keyed by parent id for the store batch.
        let mut parent_deltas: HashMap<i64, DirectoryAttributeDelta> = HashMap::new();
        for claim in claims.iter() {
            let delta = claim
                .parent_handle
                .directory_attribute_delta(
                    DirectoryAttributeDelta::new(mtime, mtime, -i32::from(claim.is_dir))
                        .for_child_removal(),
                )
                .map_err(|e| CommonError::from(e.to_string()))?;
            parent_deltas.insert(claim.parent_handle.id(), delta);
        }
        let total = self.store.apply_purge(items, mtime, parent_deltas)?;

        for claim in claims {
            let parent = claim.parent_handle;

            let delta = DirectoryAttributeDelta::new(
                parent.mtime(),
                parent.ctime(),
                -i32::from(claim.is_dir),
            )
            .for_child_removal();
            self.publish_metadata(parent.publish_directory_delta(delta))?;
            self.publish_metadata(parent.mark_persisted())?;
            // Do not journal or release blocks when live publication fails:
            // recovery may restore the still-uncommitted journal operation.
            // This can leave orphaned blocks that require recovery cleanup.
            // The produce closure is bound before it is called: the fault
            // handler's early `return` must leave this step only, not the
            // whole purge function (an inline IIFE trips the lint while a
            // plain block would change the return target).
            let produce = || {
                crate::fault_point! {
                    sync,
                    name: "master.metadata.purge.before_child_publication",
                    description: "Before publishing a committed purge to the live tree",
                    context: { "child_name" => claim.name.as_str() },
                    return_error: |fault| Err(fault.message.into()),
                }
                parent.delete_child(claim.child_id, &claim.name)
            };
            let removed = produce();
            self.publish_metadata(removed)?;
        }

        Ok(total)
    }

    // Move an entry into /.Trash instead of removing it. Blocks stay on the
    // workers so the entry can be restored until retention expires.
    pub fn trash(&self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills() as i64;

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(inp.path()));
        }

        if !inp.is_empty_dir() && !recursive {
            return err_ext!(FsError::dir_not_empty(inp.path()));
        }

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        // Unlinking one name of a hard-linked file must not move the shared
        // inode, and unresolved entries cannot be renamed, so both keep hard
        // unlink semantics.
        if inode.is_file_entry() || matches!(inode.as_ref(), File(f) if f.nlink() > 1) {
            return self.delete(inp, recursive);
        }

        self.ensure_trash_root()?;

        let entry_name = self.unique_trash_entry_name(op_ms, inode.id())?;
        let dst_path = trash::trash_entry_path(&entry_name);
        let dst_inp = InodePath::resolve(self.root_node(), &dst_path, &self.store)?;

        // The TRASH rename records the origin and the deletion time on the
        // moved inode as part of the same persisted mutation, so a hard delete
        // still costs one journal record instead of SetAttr + Rename, and a
        // failure can never leave markers on a live inode.
        self.rename_with_mtime(
            inp,
            &dst_inp,
            RenameFlags::NO_REPLACE | RenameFlags::TRASH,
            op_ms,
        )?;

        debug!("moved {} to trash as {}", inp.path(), dst_path);
        Ok(DeleteResult {
            trashed: true,
            ..DeleteResult::new()
        })
    }

    /// Moves `inp` into the asynchronous staging directory `/.Trash/.pending`.
    ///
    /// This is the `async_delete` path: the client gets the same immediate
    /// namespace effect as a soft delete, while the purger reclaims the entry
    /// without waiting for `trash_retention`. It is independent of
    /// `trash_enabled` because `/.Trash` is reserved either way.
    pub fn trash_pending(&self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills() as i64;

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(inp.path()));
        }

        if !inp.is_empty_dir() && !recursive {
            return err_ext!(FsError::dir_not_empty(inp.path()));
        }

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        // A shared inode must be unlinked rather than moved, and unresolved
        // entries cannot be renamed; both stay synchronous and bounded because
        // they only remove a single name.
        if inode.is_file_entry() || matches!(inode.as_ref(), File(f) if f.nlink() > 1) {
            return self.delete(inp, recursive);
        }

        self.ensure_trash_root()?;
        self.ensure_pending_root()?;

        let entry_name = self.unique_trash_entry_name(op_ms, inode.id())?;
        let dst_path = trash::pending_entry_path(&entry_name);
        let dst_inp = InodePath::resolve(self.root_node(), &dst_path, &self.store)?;

        self.rename_with_mtime(
            inp,
            &dst_inp,
            RenameFlags::NO_REPLACE | RenameFlags::TRASH,
            op_ms,
        )?;
        debug!("staged {} for async purge as {}", inp.path(), dst_path);
        Ok(DeleteResult {
            trashed: true,
            purge_pending: true,
            ..DeleteResult::new()
        })
    }

    /// Claims a retention-expired trash entry for reclamation by moving it from
    /// `/.Trash/<name>` to `/.Trash/.pending/<name>` in one journaled rename.
    ///
    /// The purger releases the namespace lock between batches; an entry left
    /// directly under `/.Trash` would stay restorable while its subtree is
    /// partially deleted. Once claimed, `RestoreTrash` rejects the path (it is
    /// no longer a direct trash child), so a restore either wins before the
    /// claim or fails cleanly. Returns the pending path, or `None` when the
    /// entry is already gone (restored or emptied after the scan).
    pub fn claim_trash_entry_for_purge(&self, trash_path: &str) -> FsResult<Option<String>> {
        if !trash::is_trash_entry_path(trash_path) {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is not a trash entry",
                trash_path
            )));
        }

        let src_inp = InodePath::resolve(self.root_node(), trash_path, &self.store)?;
        if src_inp.get_last_inode().is_none() {
            return Ok(None);
        }

        self.ensure_pending_root()?;
        let dst_path = trash::pending_entry_path(src_inp.name());
        let dst_inp = InodePath::resolve(self.root_node(), &dst_path, &self.store)?;

        // Trash markers are kept: the entry stays a trash entry, only no longer
        // a restorable one.
        self.rename_with_mtime(
            &src_inp,
            &dst_inp,
            RenameFlags::NO_REPLACE,
            LocalTime::mills() as i64,
        )?;
        debug!("claimed {} for purge as {}", trash_path, dst_path);
        Ok(Some(dst_path))
    }

    fn ensure_trash_root(&self) -> FsResult<()> {
        self.ensure_trash_dir(trash::TRASH_ROOT)
    }

    fn ensure_pending_root(&self) -> FsResult<()> {
        self.ensure_trash_dir(trash::TRASH_PENDING_ROOT)
    }

    fn ensure_trash_dir(&self, path: &str) -> FsResult<()> {
        let inp = InodePath::resolve(self.root_node(), path, &self.store)?;
        match inp.get_last_inode() {
            Some(inode) if inode.is_dir() => Ok(()),
            Some(_) => err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                path
            ))),
            None => {
                self.mkdir(inp, MkdirOpts::with_create(false))?;
                Ok(())
            }
        }
    }

    fn unique_trash_entry_name(&self, delete_ms: i64, inode_id: i64) -> FsResult<String> {
        let base = trash::trash_entry_name(delete_ms, inode_id);
        for seq in 0..1000 {
            let name = if seq == 0 {
                base.clone()
            } else {
                format!("{}-{}", base, seq)
            };
            let mut candidates = vec![trash::trash_entry_path(&name)];
            // Pending entries share the trash-root name space by name only, not
            // by directory, so both locations must be free to reuse a name.
            candidates.push(trash::pending_entry_path(&name));
            if candidates.iter().all(|path| {
                InodePath::resolve(self.root_node(), path, &self.store)
                    .map(|inp| inp.get_last_inode().is_none())
                    .unwrap_or(false)
            }) {
                return Ok(name);
            }
        }

        err_box!(
            "failed to allocate a unique trash entry name for inode {}",
            inode_id
        )
    }

    pub fn list_trash(
        &self,
        after: Option<&trash::TrashCursor>,
        limit: usize,
    ) -> FsResult<TrashListing> {
        let inp = InodePath::resolve(self.root_node(), trash::TRASH_ROOT, &self.store)?;
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                return Ok(TrashListing {
                    entries: vec![],
                    next: None,
                })
            }
        };

        let dir = match inode.as_ref() {
            Dir(d) => d,
            _ => {
                return err_ext!(FsError::invalid_argument(format!(
                    "{} exists and is not a directory",
                    trash::TRASH_ROOT
                )))
            }
        };

        // Entry names start with their delete time, so ordering names is enough
        // to resume a listing; only the entries inside the page load their
        // inode and build a FileStatus.
        let mut names: Vec<(i64, String)> = dir
            .children_iter()
            .filter_map(|child| {
                let name = child.name();
                Some((trash::parse_entry_delete_ms(name)?, name.to_string()))
            })
            .collect();
        names.sort();

        let start = match after {
            Some(cursor) => {
                let cursor_name = cursor
                    .path
                    .strip_prefix(trash::TRASH_ROOT)
                    .and_then(|rest| rest.strip_prefix(PATH_SEPARATOR))
                    .filter(|name| !name.is_empty() && !name.contains(PATH_SEPARATOR))
                    .ok_or_else(|| {
                        FsError::invalid_argument(format!(
                            "invalid trash cursor path: {}",
                            cursor.path
                        ))
                    })?;
                names.partition_point(|(delete_ms, name)| {
                    (*delete_ms, name.as_str()) <= (cursor.delete_ms, cursor_name)
                })
            }
            None => 0,
        };

        let limit = limit.max(1);
        let mut entries = Vec::new();
        let mut index = start;
        while index < names.len() && entries.len() < limit {
            let (delete_ms, name) = &names[index];
            index += 1;

            let path = trash::trash_entry_path(name);
            // Directory children can be unresolved FileEntry pointers; resolve
            // each entry so the full inode (and its trash xattrs) is loaded.
            let child_inp = InodePath::resolve(self.root_node(), &path, &self.store)?;
            let Some(child) = child_inp.get_last_inode() else {
                continue;
            };
            let Ok(x_attr) = child.x_attr() else {
                continue;
            };
            let (Some(origin), Some(stored_delete_ms)) = (
                trash::decode_origin(x_attr),
                trash::decode_delete_ms(x_attr),
            ) else {
                continue;
            };
            // A name that disagrees with its metadata is not a valid entry;
            // skipping it keeps the name-derived cursor consistent.
            if stored_delete_ms != *delete_ms {
                continue;
            }

            entries.push(TrashEntry {
                status: child.to_file_status(&path)?,
                path,
                origin,
                delete_ms: stored_delete_ms,
            });
        }

        // Only reachable after filling the page, so the last scanned name is
        // the final returned entry and a valid resume key.
        let next = if index < names.len() {
            let (delete_ms, name) = &names[index - 1];
            Some(trash::TrashCursor {
                delete_ms: *delete_ms,
                path: trash::trash_entry_path(name),
            })
        } else {
            None
        };

        Ok(TrashListing { entries, next })
    }

    fn resolve_restore_dst(&self, src_inp: &InodePath, dst: Option<&str>) -> FsResult<String> {
        let inode = match src_inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
        };

        let x_attr = inode.x_attr()?;
        let Some(origin) = trash::decode_origin(x_attr) else {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is not a trash entry",
                src_inp.path()
            )));
        };

        let dst = dst
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
            .unwrap_or(origin);
        if !dst.starts_with(PATH_SEPARATOR) || trash::is_reserved_path(&dst) {
            return err_ext!(FsError::invalid_argument(format!(
                "invalid restore destination: {}",
                dst
            )));
        }
        Ok(dst)
    }

    // Resolve the effective restore destination without moving anything, so
    // the master can validate mount destinations before an RPC restore.
    pub fn restore_dst(&self, trash_path: &str, dst: Option<&str>) -> FsResult<String> {
        if !trash::is_trash_entry_path(trash_path) {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is not a trash entry",
                trash_path
            )));
        }
        let src_inp = InodePath::resolve(self.root_node(), trash_path, &self.store)?;
        self.resolve_restore_dst(&src_inp, dst)
    }

    /// Scans at most `limit` trash children after `start_after` and returns
    /// only the expired entry paths. This avoids building full `FileStatus`
    /// values and sorting the whole bin, and lets the caller release the
    /// namespace lock between pages.
    pub fn scan_expired_trash(
        &self,
        cutoff_ms: i64,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let limit = limit.max(1);
        let inp = InodePath::resolve(self.root_node(), trash::TRASH_ROOT, &self.store)?;
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return Ok(TrashScanPage::default()),
        };
        let dir = match inode.as_ref() {
            Dir(d) => d,
            _ => {
                return err_ext!(FsError::invalid_argument(format!(
                    "{} exists and is not a directory",
                    trash::TRASH_ROOT
                )))
            }
        };

        let mut children = dir.list_options(&ListOptions {
            limit: Some(limit.saturating_add(1)),
            start_after: start_after.map(str::to_string),
        });
        let has_more = children.len() > limit;
        children.truncate(limit);
        let next_after = if has_more {
            children.last().map(|child| child.name().to_string())
        } else {
            None
        };

        let mut paths = Vec::new();
        for child in children {
            // Files are stored as unresolved FileEntry pointers in directories;
            // load the full inode so the trash xattrs are available.
            let delete_ms = if child.is_file_entry() {
                match self.store.get_inode(child.id(), Some(child.name())) {
                    Ok(Some(inode)) => inode.x_attr().ok().and_then(trash::decode_delete_ms),
                    _ => None,
                }
            } else {
                child.x_attr().ok().and_then(trash::decode_delete_ms)
            };

            if matches!(delete_ms, Some(delete_ms) if delete_ms <= cutoff_ms) {
                paths.push(trash::trash_entry_path(child.name()));
            }
        }

        Ok(TrashScanPage { paths, next_after })
    }

    /// Returns one bounded page of `/.Trash/.pending` entry paths. Every entry
    /// here is due for immediate reclamation; there is no retention cutoff.
    pub fn scan_pending_trash(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let limit = limit.max(1);
        let inp = InodePath::resolve(self.root_node(), trash::TRASH_PENDING_ROOT, &self.store)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(TrashScanPage::default());
        };
        let Dir(dir) = inode.as_ref() else {
            return err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                trash::TRASH_PENDING_ROOT
            )));
        };

        let mut children = dir.list_options(&ListOptions {
            limit: Some(limit.saturating_add(1)),
            start_after: start_after.map(str::to_string),
        });
        let has_more = children.len() > limit;
        children.truncate(limit);
        let next_after = if has_more {
            children.last().map(|child| child.name().to_string())
        } else {
            None
        };

        let paths = children
            .into_iter()
            .map(|child| trash::pending_entry_path(child.name()))
            .collect();

        Ok(TrashScanPage { paths, next_after })
    }

    pub fn restore_trash(
        &mut self,
        trash_path: &str,
        dst: Option<&str>,
        overwrite: bool,
    ) -> FsResult<RestoredTrashEntry> {
        if !trash::is_trash_entry_path(trash_path) {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is not a trash entry",
                trash_path
            )));
        }

        let src_inp = InodePath::resolve(self.root_node(), trash_path, &self.store)?;
        let default_dst = dst.is_none() || dst.is_some_and(|value| value.is_empty());
        let dst = self.resolve_restore_dst(&src_inp, dst)?;

        let dst_inp = InodePath::resolve(self.root_node(), &dst, &self.store)?;
        if dst_inp.get_last_inode().is_some() && !overwrite {
            return err_ext!(FsError::file_exists(&dst));
        }
        if !dst_inp
            .get_inode(-2)
            .map(|parent| parent.is_dir())
            .unwrap_or(false)
        {
            if default_dst {
                // The origin's parent may have been removed independently;
                // name the action that unblocks the restore instead of only
                // reporting a parent path the caller never mentioned.
                return err_ext!(FsError::invalid_argument(format!(
                    "cannot restore {} to its origin {}: parent directory {} does not exist; restore the parent directory first",
                    trash_path,
                    dst,
                    dst_inp.get_parent_path()
                )));
            }
            return err_ext!(FsError::file_not_found(dst_inp.get_parent_path()));
        }

        // The RESTORE rename clears the trash markers as part of the same
        // persisted mutation, so a crash can never leave an entry inside the
        // bin without its origin.
        let mut flags = RenameFlags::RESTORE;
        if !overwrite {
            flags |= RenameFlags::NO_REPLACE;
        }

        let replaced = self.rename(&src_inp, &dst_inp, flags)?;

        let restored_inp = InodePath::resolve(self.root_node(), &dst, &self.store)?;
        let status = match restored_inp.get_last_inode() {
            Some(inode) => inode.to_file_status(&dst)?,
            None => return err_ext!(FsError::file_not_found(&dst)),
        };

        debug!("restored {} to {}", trash_path, dst);
        Ok(RestoredTrashEntry {
            dst,
            status,
            replaced,
        })
    }

    pub fn trash_entry_names(&self) -> FsResult<Vec<String>> {
        let inp = InodePath::resolve(self.root_node(), trash::TRASH_ROOT, &self.store)?;
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let dir = match inode.as_ref() {
            Dir(d) => d,
            _ => {
                return err_ext!(FsError::invalid_argument(format!(
                    "{} exists and is not a directory",
                    trash::TRASH_ROOT
                )))
            }
        };
        Ok(dir
            .children_iter()
            .filter_map(|child| {
                let name = child.name();
                // Keep the purge scope identical to the listing scope: a child
                // whose name is not a trash entry name is invisible to
                // ListTrash, so EmptyTrash must not delete it silently.
                if trash::parse_entry_delete_ms(name).is_none() {
                    warn!(
                        "skipping foreign child under {}: {}",
                        trash::TRASH_ROOT,
                        name
                    );
                    return None;
                }
                Some(name.to_string())
            })
            .collect())
    }

    // Purge one batch of names. Keeping the loop in the caller lets the
    // write lock be released between batches instead of blocking every
    // metadata mutation for the whole purge.
    /// Moves the named recycle-bin entries into the async staging directory.
    ///
    /// Renaming a whole entry is O(1) and keeps its origin/delete-time xattrs,
    /// so `EmptyTrash` can acknowledge immediately and let the purger reclaim the
    /// subtrees in bounded batches. Entries that disappeared since the snapshot
    /// (restored, purged, or staged by another node) are skipped, which keeps a
    /// retried empty idempotent.
    pub fn stage_trash_entries(&self, names: &[String]) -> FsResult<u64> {
        if names.is_empty() {
            return Ok(0);
        }
        self.ensure_pending_root()?;

        let mtime = LocalTime::mills() as i64;
        let mut staged = 0;
        for name in names {
            let src_path = trash::trash_entry_path(name);
            let dst_path = trash::pending_entry_path(name);

            let src = match InodePath::resolve(self.root_node(), &src_path, &self.store) {
                Ok(inp) => inp,
                Err(e) => {
                    debug!("stage: skip unresolvable trash entry {}: {}", src_path, e);
                    continue;
                }
            };
            if src.get_last_inode().is_none() {
                continue;
            }

            let dst = InodePath::resolve(self.root_node(), &dst_path, &self.store)?;
            if dst.get_last_inode().is_some() {
                debug!("stage: {} is already staged", dst_path);
                continue;
            }

            // No TRASH flag: the entry already carries its origin and delete
            // time, and re-stamping them would rewrite the user-visible origin.
            self.rename_with_mtime(&src, &dst, RenameFlags::NO_REPLACE, mtime)?;
            staged += 1;
        }

        Ok(staged)
    }

    pub fn free(&self, inp: &InodePath, recursive: bool) -> FsResult<FreeResult> {
        let op_ms = LocalTime::mills() as i64;

        if inp.is_root() {
            return err_box!("The root is not allowed to be free");
        }

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let free_res = self.unprotected_free(inode, op_ms, recursive)?;
        self.journal_writer
            .log_free(self, inp.path(), op_ms, recursive)?;

        Ok(free_res)
    }

    pub fn free_file(&self, inp: &InodePath, mtime: i64) -> FsResult<FreeResult> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };
        let file = inode.as_file_ref()?;
        let locs = file.get_locs(&self.store)?;
        let bytes = file.get_locs_bytes(&locs);

        let mut updated = inode.as_ref().clone_metadata();
        let freed = updated.as_file_mut()?.free(mtime);
        if freed {
            self.store.apply_free(vec![updated.clone()])?;
            *inode.as_mut() = updated;
        }
        self.journal_writer
            .log_free(self, inp.path(), mtime, false)?;

        let mut free_res = FreeResult::default();
        if freed {
            free_res.add(bytes, locs);
        }
        Ok(free_res)
    }

    /// Release one fs_mode file whose UFS object was verified out of lock.
    ///
    /// Re-resolves the inode by id and releases it only if it still carries the
    /// verified version, is still `Both` and still records `ufs_mtime`, so a
    /// rewrite, a newer mark or a rename onto the path between verification and
    /// release cannot release an unverified file. Logs a non-recursive `Free`,
    /// so followers release exactly this file. `None` when nothing was released.
    pub fn free_verified(
        &self,
        inode_id: i64,
        expected_path: &str,
        gen: FileGen,
        ufs_mtime: i64,
    ) -> FsResult<Option<FreeResult>> {
        let path = match self.get_inode_path(inode_id) {
            Ok(path) => path,
            Err(FsError::FileNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        if path != expected_path {
            return Ok(None);
        }
        let inp = InodePath::resolve(self.root_node(), &path, &self.store)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(None);
        };
        let still_verified = match inode.as_ref() {
            File(f) => {
                f.id == inode_id
                    && f.gen() == gen
                    && f.storage_policy.state == StorageState::Both
                    && f.storage_policy.ufs_mtime == ufs_mtime
            }
            _ => false,
        };
        if !still_verified {
            return Ok(None);
        }

        let op_ms = LocalTime::mills() as i64;
        let free_res = self.unprotected_free(inode, op_ms, false)?;
        self.journal_writer
            .log_free(self, inp.path(), op_ms, false)?;
        Ok(Some(free_res))
    }

    pub(crate) fn unprotected_free(
        &self,
        inode: InodePtr,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<FreeResult> {
        let mut free_res = FreeResult::default();
        let mut change_inodes = vec![];
        // R6/F7: hard-link dedup is a RECURSIVE concern — non-recursive
        // single-file frees process exactly one inode, so the HashSet is
        // only allocated when the traversal can actually fan out.
        let mut seen_inodes: Option<std::collections::HashSet<i64>> = if recursive {
            Some(std::collections::HashSet::new())
        } else {
            None
        };

        let mut stack = LinkedList::new();
        stack.push_back(inode);
        while let Some(inode) = stack.pop_front() {
            match inode.as_ref() {
                FileEntry(e) => {
                    // An alias whose inode was already reclaimed (another
                    // name processed it first) is already accounted for;
                    // otherwise its store row carries the real inode.
                    if seen_inodes
                        .as_ref()
                        .is_some_and(|seen| seen.contains(&e.id))
                    {
                        continue;
                    }
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back(InodePtr::from_owned(store_inode));
                    }
                }

                Dir(d) => {
                    if recursive {
                        for child in d.children_vec() {
                            stack.push_back(InodePtr::from_owned(child));
                        }
                    }
                }

                File(f) => {
                    // R6: two names of one inode inside the same subtree
                    // must be reclaimed once. The FIRST name processes the
                    // inode (free + accounting); every later alias of the
                    // same id sees the seen-marker and stops.
                    // G1: only skip when Some AND already seen — a None
                    // seen set (non-recursive free) must still process the
                    // file normally.
                    if seen_inodes
                        .as_ref()
                        .is_some_and(|seen| seen.contains(&f.id))
                    {
                        continue;
                    }
                    if let Some(seen) = seen_inodes.as_mut() {
                        seen.insert(f.id);
                    }
                    let locs = f.get_locs(&self.store)?;
                    let bytes = f.get_locs_bytes(&locs);
                    if inode.as_mut().as_file_mut()?.free(mtime) {
                        free_res.add(bytes, locs);
                        change_inodes.push(inode.as_ref().clone());
                    }
                }
            }
        }

        self.store.apply_free(change_inodes)?;
        Ok(free_res)
    }

    pub fn rename(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        flags: RenameFlags,
    ) -> FsResult<Option<DeleteResult>> {
        let op_ms = LocalTime::mills() as i64;
        self.rename_with_mtime(src_inp, dst_inp, flags, op_ms)
    }

    /// Rename with an explicit operation timestamp. Internal moves that store
    /// the timestamp on the inode (trash) or derive a name from it reuse the
    /// same value that the journal records, so replay is bit-identical.
    fn rename_with_mtime(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        flags: RenameFlags,
        op_ms: i64,
    ) -> FsResult<Option<DeleteResult>> {
        let exchange_pre_swap_ids = if flags.exchange_mode() {
            let src_id = src_inp
                .get_last_inode()
                .map(|inode| inode.id())
                .unwrap_or(0);
            let dst_id = dst_inp
                .get_last_inode()
                .map(|inode| inode.id())
                .unwrap_or(0);
            Some((src_id, dst_id))
        } else {
            None
        };
        let res = self.unprotected_rename(src_inp, dst_inp, op_ms, flags, exchange_pre_swap_ids)?;
        let (src_inode_id, dst_inode_id) = exchange_pre_swap_ids.unwrap_or((0, 0));
        self.journal_writer.send_and_wait(self, || {
            JournalEntry::Rename(RenameEntry {
                op_id: self.next_op_id(),
                rpc_id: 0,
                src: src_inp.path().to_string(),
                dst: dst_inp.path().to_string(),
                mtime: op_ms,
                flags: flags.value(),
                src_inode_id,
                dst_inode_id,
            })
        })?;
        Ok(res)
    }

    pub(crate) fn unprotected_rename(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        flags: RenameFlags,
        exchange_pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<Option<DeleteResult>> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        if flags.exchange_mode() {
            self.unprotected_exchange(src_inp, dst_inp, mtime, exchange_pre_swap_ids)?;
            return Ok(None);
        }

        let src_parent = ParentMut::get(src_inp, -2)?;
        let dst_parent = ParentMut::get(dst_inp, -2)?;
        dst_parent.validate_child_name(dst_inp.name())?;

        let mut replaced = None;
        let mut del_res = if let Some(dst_inode) = dst_inp.get_last_inode() {
            let src_is_file = src_inode.is_file();
            let dst_is_file = dst_inode.is_file();
            if flags.no_replace() {
                return err_ext!(FsError::file_exists(dst_inp.path()));
            }
            if src_is_file && !dst_is_file {
                return err_ext!(FsError::is_a_directory(dst_inp.path()));
            }
            if !src_is_file && dst_is_file {
                return err_ext!(FsError::not_a_directory(dst_inp.path()));
            }
            if !src_is_file && !dst_is_file && !dst_inp.is_empty_dir() {
                return err_ext!(FsError::dir_not_empty(dst_inp.path()));
            }
            if src_inode.id() == dst_inode.id() {
                // Preserve the historical journal replay behavior for old entries.
                Some(self.unprotected_delete(dst_inp, mtime)?)
            } else {
                replaced = Some(dst_inode);
                None
            }
        } else {
            None
        };

        let new_name = dst_inp.name().to_string();
        // /1.log -> /a/b : b is the new inode name and /a is the destination parent.
        // For replace semantics, keep the original destination parent and name;
        // the destination removal is committed together with the new edge.
        // Modify the time and name of the rename node.
        let mut new_inode = src_inode.as_ref().clone_metadata();
        new_inode.change_name(new_name.clone());
        new_inode.set_parent_id(dst_parent.id());
        new_inode.update_ctime(mtime);

        // Trash and restore renames rewrite the markers on the moved inode in
        // the same persisted mutation as the rename. Journal replay runs this
        // path with the same flags and timestamp, so a crash can neither leave
        // an entry inside the bin without its origin nor restore an entry that
        // keeps stale markers, and a soft delete still costs one journal
        // record.
        if !new_inode.is_file_entry() {
            if flags.trash() {
                let x_attr = new_inode.x_attr_mut()?;
                x_attr.insert(
                    trash::TRASH_XATTR_ORIGIN.to_string(),
                    src_inp.path().as_bytes().to_vec(),
                );
                x_attr.insert(
                    trash::TRASH_XATTR_DELETE_MS.to_string(),
                    mtime.to_le_bytes().to_vec(),
                );
            } else if flags.restore() {
                let x_attr = new_inode.x_attr_mut()?;
                x_attr.remove(trash::TRASH_XATTR_ORIGIN);
                x_attr.remove(trash::TRASH_XATTR_DELETE_MS);
            }
        }
        // A directory edge stores its stable live node. Attach it before the
        // store batch commits so the only post-commit publication is the
        // staged DTO whose metadata matches the persisted inode record.
        if new_inode.is_dir() {
            let node = src_inp
                .get_dir_node(-1)
                .ok_or_else(|| FsError::common("source directory lost its live node"))?;
            new_inode.as_dir_mut()?.node = Some(node);
        }

        // Stage parent metadata separately: a failed store write must not
        // publish a new mtime or link count into the live namespace.
        // A plain same-directory rename stages one parent copy: both sides
        // only get the same mtime bump, so the two clones would be equal.
        let same_parent_plain =
            src_parent.id() == dst_parent.id() && replaced.is_none() && !src_inode.is_dir();
        let mut new_src_parent = src_parent
            .view(src_inp.get_component_neg(-2)?)?
            .clone_metadata();
        new_src_parent.update_mtime(mtime);
        let new_dst_parent = if same_parent_plain {
            new_src_parent.clone_metadata()
        } else {
            let mut cloned = dst_parent
                .view(dst_inp.get_component_neg(-2)?)?
                .clone_metadata();
            cloned.update_mtime(mtime);
            cloned
        };
        let mut new_dst_parent = new_dst_parent;
        let move_dir = src_parent.id() != dst_parent.id() && src_inode.is_dir();
        if move_dir {
            new_src_parent.dec_nlink(mtime);
            new_dst_parent.incr_nlink(mtime);
        }
        let replaced_dir = replaced.as_ref().is_some_and(|inode| inode.is_dir());
        if replaced_dir {
            new_dst_parent.dec_nlink(mtime);
        }

        // One raw delta per parent, shared by the persisted batch and the
        // live publication so the merge result and live state cannot drift.
        // Matrix (HEAD store semantics):
        //   plain   same : src (0)
        //   plain   cross: src (-move_dir, children -1), dst (+move_dir, children +1)
        //   replace same : src (-replaced_dir, children -1)
        //   replace cross: src (-move_dir, children -1), dst (move_dir - replaced_dir)
        let move_nlink = i32::from(move_dir);
        let replaced_nlink = i32::from(replaced_dir);
        let same_parent = src_parent.id() == dst_parent.id();
        let (src_raw, dst_raw) = match (replaced.is_some(), same_parent) {
            (false, true) => (DirectoryAttributeDelta::new(mtime, mtime, 0), None),
            (false, false) => (
                DirectoryAttributeDelta::new(mtime, mtime, -move_nlink).for_child_removal(),
                Some(DirectoryAttributeDelta::new(mtime, mtime, move_nlink).for_child_entry()),
            ),
            (true, true) => (
                DirectoryAttributeDelta::new(mtime, mtime, -replaced_nlink).for_child_removal(),
                None,
            ),
            (true, false) => (
                DirectoryAttributeDelta::new(mtime, mtime, -move_nlink).for_child_removal(),
                Some(DirectoryAttributeDelta::new(
                    mtime,
                    mtime,
                    move_nlink - replaced_nlink,
                )),
            ),
        };
        let src_delta = src_parent.directory_attribute_delta(src_raw)?;
        let dst_delta = match dst_raw {
            Some(raw) => dst_parent.directory_attribute_delta(raw)?,
            None => src_delta,
        };
        if let Some(target) = replaced.as_ref() {
            del_res = Some(self.store.apply_replace_rename(
                &new_src_parent,
                src_inode.as_ref(),
                src_inp.name(),
                &new_dst_parent,
                &new_inode,
                target.as_ref(),
                mtime,
                src_delta,
                dst_delta,
            )?);
        } else {
            self.store.apply_rename(
                &new_src_parent,
                src_inode.as_ref(),
                src_inp.name(),
                &new_dst_parent,
                &new_inode,
                same_parent_plain,
                src_delta,
                dst_delta,
            )?;
        }

        self.publish_metadata(src_parent.publish_directory_delta(src_raw))?;
        self.publish_metadata(src_parent.mark_persisted())?;
        if let Some(raw) = dst_raw {
            self.publish_metadata(dst_parent.publish_directory_delta(raw))?;
            self.publish_metadata(dst_parent.mark_persisted())?;
        }
        // Update memory status. Same-directory renames mutate the child index
        // inside a single rename-mutation window so a concurrent listing never
        // observes the deleted-but-not-yet-added middle state.
        if src_parent.id() == dst_parent.id() {
            let children =
                self.publish_metadata(src_parent.children_handle().map_err(CommonError::from))?;
            let mut mutation = children.begin_rename_mutation(src_inp.name(), dst_inp.name());
            self.publish_metadata(
                mutation.rename_to(
                    src_inode.id(),
                    src_inp.name(),
                    replaced
                        .as_ref()
                        .map(|target| (target.id(), dst_inp.name())),
                    new_inode,
                ),
            )?;
            return Ok(del_res);
        }

        if let Some(target) = replaced {
            let _ = self.publish_metadata(dst_parent.delete_child(target.id(), dst_inp.name()))?;
        }

        // Cross-directory renames publish the same staged DTO that was
        // committed above. Its live-node backlink preserves the subtree and
        // `DirNode::for_edge` installs its parent and feature metadata.
        let _ = self.publish_metadata(src_parent.delete_child(src_inode.id(), src_inp.name()))?;
        self.publish_metadata(dst_parent.add_child(new_inode))?;

        Ok(del_res)
    }

    fn unprotected_exchange(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<()> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        let dst_inode = match dst_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(dst_inp.path())),
            Some(v) => v,
        };

        let src_id = src_inode.id();
        let dst_id = dst_inode.id();

        if src_id == dst_id {
            return Ok(());
        }

        if let Some((expected_src, expected_dst)) = pre_swap_ids {
            if expected_src != 0 && expected_dst != 0 {
                if src_id == expected_dst && dst_id == expected_src {
                    return Ok(());
                }
                if src_id != expected_src || dst_id != expected_dst {
                    warn!(
                        "Exchange replay inode id mismatch at {} and {}: current ({}, {}), expected ({}, {})",
                        src_inp.path(),
                        dst_inp.path(),
                        src_id,
                        dst_id,
                        expected_src,
                        expected_dst
                    );
                    return Ok(());
                }
            }
        }

        let src_parent = ParentMut::get(src_inp, -2)?;
        let dst_parent = ParentMut::get(dst_inp, -2)?;

        let src_name = src_inp.name().to_string();
        let dst_name = dst_inp.name().to_string();

        let mut at_src = dst_inode.as_ref().clone_metadata();
        at_src.change_name(src_name.clone());
        at_src.set_parent_id(src_parent.id());

        let mut at_dst = src_inode.as_ref().clone_metadata();
        at_dst.change_name(dst_name.clone());
        at_dst.set_parent_id(dst_parent.id());

        // Exchange keeps child counts unchanged on both sides; only nlink
        // moves when a directory swaps with a file across parents. One raw
        // delta per parent feeds both the store batch and the live state.
        let nlink = if src_parent.id() != dst_parent.id() {
            i32::from(dst_inode.is_dir()) - i32::from(src_inode.is_dir())
        } else {
            0
        };
        let src_raw = DirectoryAttributeDelta::new(mtime, mtime, nlink);
        let dst_raw = DirectoryAttributeDelta::new(mtime, mtime, -nlink);
        let mut new_src_parent = src_parent
            .view(src_inp.get_component_neg(-2)?)?
            .clone_metadata();
        let mut new_dst_parent = dst_parent
            .view(dst_inp.get_component_neg(-2)?)?
            .clone_metadata();
        new_src_parent.update_exchange_delta(mtime, nlink as i8)?;
        new_dst_parent.update_exchange_delta(mtime, -nlink as i8)?;
        let src_delta = src_parent.directory_attribute_delta(src_raw)?;
        let dst_delta = dst_parent.directory_attribute_delta(dst_raw)?;
        self.store.apply_exchange(
            &new_src_parent,
            &src_name,
            &new_dst_parent,
            &dst_name,
            &at_src,
            &at_dst,
            src_delta,
            dst_delta,
        )?;
        self.publish_metadata(src_parent.publish_directory_delta(src_raw))?;
        self.publish_metadata(src_parent.mark_persisted())?;
        if src_parent.id() != dst_parent.id() {
            self.publish_metadata(dst_parent.publish_directory_delta(dst_raw))?;
            self.publish_metadata(dst_parent.mark_persisted())?;
        }

        let detached_src = self.publish_metadata(src_parent.delete_child(src_id, &src_name))?;
        let detached_dst = self.publish_metadata(dst_parent.delete_child(dst_id, &dst_name))?;
        // Move the live node handles: the detached views carry the original
        // Arc<DirNode> backlink, so each parent slot stores the identical
        // handle and both subtrees keep their shared children index.
        let mut moved_src = detached_dst;
        let mut moved_dst = detached_src;
        moved_src.change_name(src_name.clone());
        moved_dst.change_name(dst_name.clone());
        self.publish_metadata(src_parent.add_child(moved_src))?;
        self.publish_metadata(dst_parent.add_child(moved_dst))?;

        Ok(())
    }

    pub fn create_file(&self, inp: InodePath, opts: CreateFileOpts) -> FsResult<InodePath> {
        Ok(self.create_file_with_status(inp, opts)?.0)
    }

    /// `create_file` for a new path that also returns the status of the file it
    /// committed, so the caller does not project the live node again.
    ///
    /// The status is built from `clone_last_file()`, the same copy the journal
    /// persists, while the parent write lock is still held.
    pub fn create_file_with_status(
        &self,
        mut inp: InodePath,
        mut opts: CreateFileOpts,
    ) -> FsResult<(InodePath, FileStatus)> {
        if inp.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Create a directory that does not exist.
        inp = self.create_parent_dir(inp, opts.dir_opts())?;

        if let Some(group) = Self::setgid_parent_group(&inp)? {
            opts.group = group;
        }

        let name = inp.name().to_string();

        // Create an inode file node.
        let file = InodeFile::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
        inp = self.add_last_inode(inp, InodeView::new_file(name.clone(), file))?;
        let last_file = inp.clone_last_file()?;
        let status = Self::committed_file_status(&name, inp.path(), &last_file);
        self.journal_writer.send_and_wait(self, || {
            JournalEntry::CreateFile(CreateFileEntry {
                op_id: self.next_op_id(),
                rpc_id: 0,
                path: inp.path().to_string(),
                file: last_file.clone(),
            })
        })?;

        Ok((inp, status))
    }

    /// Status of a file that was just committed, from the node copy the journal
    /// persists. A file has no children and its directory-ctime marker does not
    /// apply, so this needs no child count and no attribute-state read.
    fn committed_file_status(name: &str, path: &str, file: &InodeFile) -> FileStatus {
        let acl = &file.features.acl;
        FileStatus {
            id: file.id,
            path: path.to_owned(),
            name: name.to_owned(),
            is_dir: false,
            mtime: file.mtime,
            atime: file.atime,
            children_num: 0,
            is_complete: file.is_complete(),
            len: file.len,
            replicas: file.replicas as i32,
            block_size: file.block_size as i64,
            file_type: file.file_type,
            x_attr: file.features.x_attr.clone(),
            storage_policy: file.storage_policy.clone(),
            owner: acl.owner.clone(),
            group: acl.group.clone(),
            mode: acl.mode,
            nlink: file.nlink(),
            target: file.target.clone(),
        }
    }

    pub(crate) fn add_last_inode(
        &self,
        mut inp: InodePath,
        mut child: InodeView,
    ) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() as i32;

        // parent must be an existing directory.
        let parent = ParentMut::get(&inp, pos - 1)?;
        parent.validate_child_name(child.name())?;
        // Persist against detached metadata first. Publishing to the live
        // child index before the batch commits would leave the namespace tree
        // visible after a RocksDB failure.
        child.set_parent_id(parent.id());
        // A non-root directory already has an inode row. If its fields did
        // not change, the edge and attribute delta are the only writes needed.
        // Root may not have an inode row until its first child is created.
        let parent_view = parent.view(inp.get_component_neg(pos - 1)?)?;
        let write_parent =
            parent.id() == ROOT_INODE_ID || child.is_dir() || child.mtime() > parent_view.mtime();
        let mut staged_parent = write_parent.then(|| parent_view.clone_metadata());
        if let Some(staged) = &mut staged_parent {
            staged.update_mtime(child.mtime());
            if child.is_dir() {
                staged.incr_nlink(child.mtime());
            }
        }
        let persisted_parent: &InodeView = staged_parent.as_ref().unwrap_or(&parent_view);
        let add_delta = parent
            .directory_attribute_delta(
                DirectoryAttributeDelta::new(
                    child.mtime(),
                    child.mtime(),
                    i32::from(child.is_dir()),
                )
                .for_child_entry(),
            )
            .map_err(CommonError::from)?;
        self.store
            .apply_add(persisted_parent, &child, write_parent, add_delta)?;

        let delta =
            DirectoryAttributeDelta::new(child.mtime(), child.mtime(), i32::from(child.is_dir()))
                .for_child_entry();
        self.publish_metadata(parent.publish_directory_delta(delta))?;
        self.publish_metadata(parent.mark_persisted())?;
        let added = self.publish_metadata(parent.add_child(child))?;
        self.publish_metadata(inp.append(added.into_entry()))?;

        Ok(inp)
    }

    pub fn file_status(&self, inp: &InodePath) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let status = match inode.as_ref() {
            File(..) | Dir(..) => inode.to_file_status(inp.path())?,
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        Ok(status)
    }

    pub fn list_status(&self, inp: &InodePath) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        match inode.as_ref() {
            File(_) => Ok(vec![inode.to_file_status(inp.path())?]),

            Dir(d) => {
                let iter = d.children_handle().list_options(&ListOptions::default());
                let res = self
                    .store
                    .batched_get_inodes(Self::list_parent(inp), iter)?;
                Ok(res)
            }

            FileEntry(e) => {
                let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
                match inode_opt {
                    Some(inode_view) => Ok(vec![inode_view.to_file_status(inp.path())?]),
                    None => err_ext!(FsError::file_not_found(inp.path())),
                }
            }
        }
    }

    /// Parent path for child statuses: a root `InodePath` lists as "/"
    /// whatever spelling resolved it.
    fn list_parent(inp: &InodePath) -> &str {
        if inp.is_root() {
            PATH_SEPARATOR
        } else {
            inp.path()
        }
    }

    fn list_single_file(status: FileStatus, opts: &ListOptions) -> Vec<FileStatus> {
        if matches!(opts.limit, Some(0)) {
            return vec![];
        }
        if let Some(sa) = opts.start_after.as_deref() {
            if status.name.as_str() <= sa {
                return vec![];
            }
        }
        vec![status]
    }

    pub fn list_options(&self, inp: &InodePath, opts: &ListOptions) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        match inode.as_ref() {
            File(_) => {
                let status = inode.to_file_status(inp.path())?;
                Ok(Self::list_single_file(status, opts))
            }

            Dir(d) => {
                let children = d.list_options(opts);
                let res = self
                    .store
                    .batched_get_inodes(Self::list_parent(inp), children)?;
                Ok(res)
            }

            FileEntry(e) => {
                let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
                match inode_opt {
                    Some(inode_view) => {
                        let status = inode_view.to_file_status(inp.path())?;
                        Ok(Self::list_single_file(status, opts))
                    }
                    None => err_ext!(FsError::file_not_found(inp.path())),
                }
            }
        }
    }

    pub fn acquire_new_block(
        &self,
        path: impl AsRef<str>,
        inode: InodePtr,
        commit_blocks: Vec<CommitBlock>,
        choose_workers: &[WorkerAddress],
        file_len: i64,
    ) -> FsResult<ExtendedBlock> {
        let mut updated = inode.as_ref().clone_metadata();
        let file = updated.as_file_mut()?;

        let new_block_id = file.next_block_id()?;

        // flush file and commit block
        file.complete(file_len, &commit_blocks, "", true)?;

        // create block.
        file.add_block(BlockMeta::with_pre(new_block_id, choose_workers));

        let block = ExtendedBlock {
            id: new_block_id,
            len: 0,
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
            alloc_opts: None,
        };

        // state add block.
        self.store.apply_new_block(&updated, &commit_blocks)?;
        *inode.as_mut() = updated;
        self.journal_writer
            .log_add_block(self, path, inode.as_file_ref()?, commit_blocks)?;
        Ok(block)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file(
        &self,
        path: impl AsRef<str>,
        inode: &mut InodePtr,
        len: i64,
        commit_block: Vec<CommitBlock>,
        client_name: impl AsRef<str>,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<bool> {
        let mut updated = inode.as_ref().clone_metadata();
        let file = updated.as_file_mut()?;
        let id = file.id();
        file.complete(len, &commit_block, client_name, only_flush)?;

        // Only apply set_attr_opts on real complete/close, not on flush.
        // Flush semantics should remain narrow (durability only).
        if !only_flush {
            if let Some(mut opts) = set_attr_opts {
                // `complete_file` is a client-facing mutation: apply the same
                // internal-xattr guard and ctime normalization as `set_attr`.
                Self::check_not_internal_x_attr(&opts)?;
                Self::normalize_internal_x_attrs(&mut opts);
                updated.set_attr(opts)?;
            }
        }

        self.store.apply_complete_file(&updated, &commit_block)?;
        *inode.as_mut() = updated;
        self.evictor.on_access(id);
        self.journal_writer
            .log_complete_file(self, path, inode.as_file_ref()?, commit_block)?;

        Ok(true)
    }

    pub fn get_file_locations(
        &self,
        file: &InodeFile,
    ) -> FsResult<HashMap<i64, Vec<BlockLocation>>> {
        let locs = self.store.get_file_locations(file)?;
        self.evictor.on_access(file.id());
        Ok(locs)
    }

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> FsResult<()> {
        self.store.ensure_available()?;
        self.store.add_block_location(block_id, location)?;
        Ok(())
    }

    pub fn get_block_locations(&self, block_id: i64) -> FsResult<Vec<BlockLocation>> {
        self.store.ensure_available()?;
        Ok(self.store.get_block_locations(block_id)?)
    }

    /// Returns replication state only when the block still belongs to the current file version.
    pub(crate) fn replication_block_state(
        &self,
        block_id: i64,
    ) -> FsResult<Option<ReplicationBlockState>> {
        self.store.ensure_available()?;
        let inode_id = InodeId::get_id(block_id);
        let Some(File(file)) = self.store.get_inode(inode_id, None)? else {
            return Ok(None);
        };
        if !file.blocks.iter().any(|block| block.id == block_id) {
            return Ok(None);
        }
        Ok(Some(ReplicationBlockState {
            replicas: file.replicas,
            locations: self.store.get_block_locations(block_id)?,
        }))
    }

    /// Adds a replication result only if the block is still current and needs a replica.
    /// The namespace guard excludes overwrite/deletion; the caller must also
    /// hold the block and worker locks through validation and insertion.
    pub(crate) fn add_replication_location_if_needed(
        &self,
        block_id: i64,
        location: BlockLocation,
    ) -> FsResult<bool> {
        self.store.ensure_available()?;
        let inode_id = InodeId::get_id(block_id);
        let Some(File(file)) = self.store.get_inode(inode_id, None)? else {
            return Ok(false);
        };
        if !file.blocks.iter().any(|block| block.id == block_id) {
            return Ok(false);
        }

        let locations = self.store.get_block_locations(block_id)?;
        if locations
            .iter()
            .any(|current| current.worker_id == location.worker_id)
            || locations.len() >= file.replicas as usize
        {
            return Ok(false);
        }

        self.store.add_block_location(block_id, location)?;
        Ok(true)
    }

    pub fn reopen_file(
        &self,
        inp: &InodePath,
        client_name: impl AsRef<str>,
    ) -> FsResult<FileStatus> {
        let inode_ptr = match inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(inp.path())),
            Some(v) => v,
        };

        let mut updated = match inode_ptr.as_ref() {
            File(..) => inode_ptr.as_ref().clone_metadata(),
            Dir(..) => {
                let err_msg = format!("Cannot append to already exists {} directory", inp.path());
                return err_ext!(FsError::file_exists(err_msg));
            }
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        let file = updated.as_file_mut()?;
        let _ = file.reopen(client_name);
        let status = updated.to_file_status(inp.path())?;

        self.store.apply_reopen_file(&updated)?;
        *inode_ptr.as_mut() = updated;
        self.journal_writer
            .log_reopen_file(self, inp.path(), inode_ptr.as_file_ref()?)?;

        Ok(status)
    }

    /// Release one writer record registered by reopen. Idempotent: missing
    /// files, directories and unknown clients succeed. Only the RocksDB
    /// state is updated (no journal entry): complete carries the
    /// authoritative snapshot, so a crash before complete replays to the
    /// correct unfinished state either way.
    pub fn close_writer(&self, inp: &InodePath, client_name: &str) -> FsResult<()> {
        let inode = match inp.get_last_inode() {
            Some(v) => v.clone(),
            None => return Ok(()),
        };
        if inode.as_file_ref().is_err() {
            return Ok(());
        }
        let mut updated = inode.as_ref().clone_metadata();
        updated.as_file_mut()?.features.complete_write(client_name);
        self.store.apply_reopen_file(&updated)?;
        *inode.as_mut() = updated;
        Ok(())
    }

    pub(crate) fn block_inode_state(&self, block_id: i64) -> FsResult<BlockInodeState> {
        let file_id = InodeId::get_id(block_id);
        let inode = self.store.get_inode(file_id, None)?;
        match inode {
            None => Ok(BlockInodeState::Missing),
            Some(v) => {
                if v.is_file() {
                    Ok(BlockInodeState::File)
                } else {
                    Ok(BlockInodeState::NotFile)
                }
            }
        }
    }

    /// Overwrite a file by cleaning all blocks and updating metadata.
    /// If file doesn't exist, create a new one.
    /// Returns DeleteResult containing blocks that need to be removed from workers.
    pub fn overwrite_file(&self, inp: &InodePath, opts: CreateFileOpts) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();
        let mut delete_result = DeleteResult::new();

        match inp.get_last_inode() {
            Some(inode) => {
                if !inode.is_file() {
                    return err_box!("Path is not a file: {}", inp.path());
                }

                let mut updated = inode.as_ref().clone_metadata();
                let file = updated.as_file_mut()?;
                for block_meta in &file.blocks {
                    if let Ok(locations) = self.get_block_locations(block_meta.id) {
                        delete_result.blocks.insert(block_meta.id, locations);
                    }
                }
                file.overwrite(opts, op_ms as i64);

                // Publish the live inode only after the durable overwrite has
                // committed. A failed RocksDB write must leave both views at
                // the old block set.
                self.store.apply_overwrite_file(&updated)?;
                *inode.as_mut() = updated;
            }
            None => {
                return err_ext!(FsError::file_not_found(inp.path()));
            }
        }

        // Log the operation
        self.journal_writer.log_overwrite_file(self, inp)?;

        Ok(delete_result)
    }

    pub fn print_tree(&self) {
        self.root_view().print_tree()
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        self.store.ensure_available()?;
        let mut tree_hash = self.root_view().sum_hash()?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_INODES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_EDGES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_LOCATION)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_BLOCK)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_DIR_ATTRS)?;
        Ok(tree_hash)
    }

    pub fn last_inode_id(&self) -> i64 {
        self.inode_id.current()
    }

    /// Stable inode-id counter reference; callers that must report metrics
    /// even while the view is closed reuse this same instance.
    pub(crate) fn inode_id_handle(&self) -> Arc<InodeId> {
        self.inode_id.clone()
    }

    /// Same store stats instance the live InodeStore owns (counts gauges).
    pub(crate) fn fs_stats_handle(&self) -> Arc<FileSystemStats> {
        self.store.fs_stats_handle()
    }

    pub fn update_last_inode_id(&self, new_value: i64) -> CommonResult<()> {
        self.inode_id.advance_to(new_value);
        Ok(())
    }

    // Read data from rocksdb to build a directory tree
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        self.store.ensure_available()?;
        self.store.create_tree().map(|x| x.1)
    }

    // Restore in-memory tree from RocksDB without checkpoint (for testing only).
    // In production, use restore() with checkpoint path via Raft snapshot.
    pub fn restore_from_rocksdb(&mut self) -> CommonResult<()> {
        // Same restore boundary as `restore()`: the restore guard drains and
        // cancels scans across the rebuild. Clear-before-build; on failure the
        // old tree is never replaced and the TTL list may be empty, partially
        // rebuilt, or fully rebuilt. DB migration and repair writes are not
        // rolled back. Executors read current inodes, so the entries present
        // never cause wrong deletions; the only cost is TTL-deletion timing.
        // This matches the pre-existing `restore()` semantics.
        let _restore_guard = self.store.begin_restore(self.restore_wait_timeout)?;
        self.store.ensure_available()?;
        self.store.migrate_directory_attributes()?;
        self.store.initialize_root_directory_attributes()?;
        self.store.get_ttl_bucket_list().clear();
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        root_dir.as_dir_ref()?;
        self.root = DirNode::from_view(&root_dir);
        self.update_last_inode_id(last_inode_id)?;
        Ok(())
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.store.create_checkpoint(id)
    }

    pub(crate) fn create_snapshot(&self, force: bool) -> FsResult<LeaderSnapshotInfo> {
        self.journal_writer.create_snapshot(self, force)
    }

    pub fn restore<T: AsRef<str>>(&mut self, path: T, checkpoint_size: u64) -> CommonResult<()> {
        let mut spend = TimeSpent::new();
        let path = path.as_ref();

        // Cancel any active metadata snapshot scan lease and wait (bounded) for
        // scanners to release the store before touching anything. A timeout
        // leaves the original RocksDB and in-memory tree untouched and
        // queryable.
        let restore_guard = self.store.begin_restore(self.restore_wait_timeout)?;

        // Keep the old tree until a replacement can be built. A failure before
        // replacing RocksDB must not discard the still-valid live namespace.
        // Reset rocksdb
        if let Err(error) = self.store.restore_with_guard(&restore_guard, path) {
            if !self.store.cursor_store_available() {
                self.journal_writer.fail_metadata_restore();
            }
            return Err(error);
        }
        self.store.get_ttl_bucket_list().clear();
        let result = (|| -> CommonResult<(i64, InodeView)> {
            self.store.migrate_directory_attributes()?;
            self.store.initialize_root_directory_attributes()?;
            self.store.create_tree()
        })();
        let (last_inode_id, root_dir) = result.inspect_err(|_error| {
            self.store.invalidate_cursor_store();
            self.journal_writer.fail_metadata_restore();
        })?;
        let time1 = spend.used_ms();
        spend.reset();

        // Update the directory tree
        root_dir.as_dir_ref().inspect_err(|_error| {
            self.store.invalidate_cursor_store();
            self.journal_writer.fail_metadata_restore();
        })?;
        self.root = DirNode::from_view(&root_dir);
        self.update_last_inode_id(last_inode_id)
            .inspect_err(|_error| {
                self.store.invalidate_cursor_store();
                self.journal_writer.fail_metadata_restore();
            })?;
        let time2 = spend.used_ms();

        info!(
            "restore from {}, checkpoint_size={} bytes, restore_and_build_tree={} ms, \
        publish_tree={} ms (see create_tree log for sub-phase breakdown), \
        last_inode_id={}",
            path, checkpoint_size, time1, time2, last_inode_id
        );
        Ok(())
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        self.store.get_checkpoint_path(id)
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        self.store.get_file_counts()
    }

    pub fn block_report(&mut self, blocks: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        self.block_report_locked(blocks)
    }

    /// Apply location changes while the caller holds the namespace guard and
    /// the corresponding block/worker locks. No inode state is mutated here.
    pub(crate) fn block_report_locked(
        &self,
        blocks: Vec<(bool, i64, BlockLocation)>,
    ) -> FsResult<()> {
        self.store.ensure_available()?;
        let mut batch = self.store.new_batch();
        for (add, id, loc) in blocks {
            if add {
                batch.add_location(id, &loc)?;
            } else {
                batch.delete_location(id, loc.worker_id)?;
            }
        }

        batch.commit()?;
        Ok(())
    }

    pub fn get_rocks_store(&self) -> &RocksInodeStore {
        &self.store.store
    }

    /// Read-only access to the inode store for snapshot scan coordination.
    pub fn store_ref(&self) -> &InodeStore {
        &self.store
    }

    /// Acquire a metadata snapshot scan lease against the current store.
    ///
    /// The lease holds an `Arc` clone of the RocksDB store so the scan reads
    /// one consistent generation; a pending Raft snapshot restore cancels the
    /// lease (or rejects acquisition) so it never blocks restore indefinitely.
    pub fn begin_snapshot_scan(&self) -> CommonResult<crate::master::space::MetadataScanLease> {
        self.store.begin_snapshot_scan()
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        self.store.ensure_available()?;
        let block_ids = self.store.store.delete_locations(worker_id)?;
        Ok(block_ids)
    }

    pub fn block_ids_for_worker(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        self.store.ensure_available()?;
        Ok(self.store.store.get_block_ids(worker_id)?)
    }

    /// Invalidate UFS-backed files whose cache is no longer readable after a
    /// worker's locations are removed. A single missing block makes the whole
    /// cache copy unusable, even when other blocks or replicas still exist.
    ///
    /// The returned locations belong to the discarded cache copies and should
    /// be scheduled for deletion from any surviving workers.
    pub(crate) fn invalidate_lost_cache_files(
        &self,
        block_ids: &[i64],
    ) -> FsResult<CacheInvalidationResult> {
        let inode_ids: HashSet<_> = block_ids.iter().map(|id| InodeId::get_id(*id)).collect();
        let mut result = CacheInvalidationResult::default();
        let mut changed_inodes = Vec::new();

        for inode_id in inode_ids {
            let Some(mut inode) = self.store.get_inode(inode_id, None)? else {
                continue;
            };
            let File(file) = &mut inode else {
                continue;
            };

            // Cache-mode load jobs use the Delete TTL action. Files in fs-mode
            // use Free instead and retain their normal replica-recovery path.
            if !file.storage_policy.both_exists()
                || file.storage_policy.ttl_action != TtlAction::Delete
            {
                continue;
            }

            // `get_locs` purposefully omits empty location lists, so inspect
            // each file block directly to find cache blocks with no replicas.
            let mut cache_unreadable = false;
            for block in &file.blocks {
                if self.store.get_block_locations(block.id)?.is_empty() {
                    cache_unreadable = true;
                    break;
                }
            }
            if !cache_unreadable {
                continue;
            }

            let locations = file.get_locs(&self.store)?;
            result
                .invalidated_block_ids
                .extend(file.blocks.iter().map(|block| block.id));
            if file.invalidate_cache() {
                result.delete_result.blocks.extend(locations);
                changed_inodes.push(inode);
            }
        }

        let journal_inodes = changed_inodes.clone();
        self.store.apply_cache_invalidations(changed_inodes)?;
        self.journal_writer
            .log_cache_invalidations(self, journal_inodes)?;
        Ok(result)
    }

    pub fn get_worker_block_ids(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        Ok(self.store.store.get_block_ids(worker_id)?)
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        self.journal_writer.take_entries()
    }

    pub fn store_mount(&self, info: MountInfo, send_log: bool) -> FsResult<()> {
        self.store.store.add_mountpoint(info.mount_id, &info)?;

        if send_log {
            self.journal_writer.log_mount(self, info)?;
        }

        Ok(())
    }

    /// Persists the row deletion; `send_log` selects the journal echo exactly
    /// like [`FsDir::store_mount`].
    pub fn unmount_with_log(&self, id: u32, send_log: bool) -> FsResult<()> {
        self.store.store.remove_mountpoint(id)?;
        if send_log {
            self.journal_writer.log_unmount(self, id)?;
        }
        Ok(())
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        self.store.get_mount_table()
    }

    pub fn save_mount_table_version(&self, version: u64) -> FsResult<()> {
        self.store.save_mount_table_version(version)?;
        Ok(())
    }

    pub fn get_mount_table_version(&self) -> CommonResult<u64> {
        self.store.get_mount_table_version()
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        self.store.get_mount_point(id)
    }

    /// Refresh a file's atime for atime-based TTL expiration.
    ///
    /// Called on the read-open path. To bound journal writes, the update is
    /// throttled: it fires only when `now - atime >= ttl_ms / 2` (relatime
    /// style). The inode id and restore epoch prevent a re-resolved path from
    /// refreshing a replacement or an inode from a different store generation.
    /// Returns whether the atime was updated.
    pub fn touch_atime_for_ttl(
        &self,
        inp: &InodePath,
        expected_id: i64,
        expected_epoch: u64,
    ) -> FsResult<bool> {
        if self.store.scan_control().cancel_epoch() != expected_epoch {
            return Ok(false);
        }
        let inode = match inp.get_last_inode() {
            Some(v) if v.id() == expected_id => v,
            _ => return Ok(false),
        };

        if !inode.is_file() {
            return Ok(false);
        }

        // Only files with an active atime-based TTL policy need refreshes.
        let sp = &inode.as_file_ref()?.storage_policy;
        if sp.ttl_ms <= 0 || sp.ttl_action == TtlAction::None {
            return Ok(false);
        }

        let ttl_type = TtlType::from_xattr_value(
            inode
                .x_attr()?
                .get(INTERNAL_TTL_TYPE_XATTR)
                .map(|v| v.as_slice()),
        );
        if ttl_type != TtlType::Atime {
            return Ok(false);
        }

        let now = LocalTime::mills() as i64;
        if now.saturating_sub(inode.atime()) < sp.ttl_ms / 2 {
            return Ok(false);
        }

        let opts = SetAttrOpts {
            recursive: false,
            atime: Some(now),
            ..Default::default()
        };
        let mut updated = inode.as_ref().clone_metadata();
        updated.set_attr(opts.clone())?;
        self.store.apply_set_attr(std::slice::from_ref(&updated))?;
        *inode.as_mut() = updated;
        self.journal_writer.log_set_attr(self, inp, opts)?;
        Ok(true)
    }

    pub fn set_attr(&self, inp: InodePath, mut opts: SetAttrOpts) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        Self::check_not_internal_x_attr(&opts)?;

        // Internal metadata is master-owned. Persist the operation timestamp in the
        // journal so replay restores it without changing the bincode inode layout.
        // The TTL-type marker is likewise master-owned (create/mount path); user
        // SetAttr requests must not flip a file between atime and mtime TTL.
        Self::normalize_internal_x_attrs(&mut opts);

        // Conditional UFS mark: an fs_mode export names the version it copied,
        // and the mark only lands while the inode still carries it. A miss is a
        // safe no-op (the file stays `Cv`, Free reconciliation re-exports it).
        // `take()` keeps the guard out of the journal so replay is unconditional.
        // Reports without a guard keep the original behaviour.
        if let Some(expected) = opts.expect_gen.take() {
            if opts.ufs_mtime.is_some() && !Self::ufs_mark_applicable(inode.as_ref(), expected) {
                return Ok(inode.to_file_status(inp.path())?);
            }
        }

        self.unprotected_set_attr(inode.clone(), opts.clone())?;
        self.journal_writer.log_set_attr(self, &inp, opts)?;
        // A directory target publishes through its live DirNode; the caller's
        // projected box is a read-only view and keeps the pre-call acl, so
        // the returned status must be rebuilt from the node when available.
        if let Some(node) = inp.inodes.last().and_then(|entry| entry.dir_node()) {
            let name = inp.name().to_string();
            let projected = InodeDir::from_dir_node_live(name, node.clone());
            return Ok(projected.to_file_status(inp.path())?);
        }
        Ok(inode.to_file_status(inp.path())?)
    }

    /// Whether `inode` may still claim that `expected` is the version sitting on
    /// UFS. Failing any condition means the export raced with a newer version, a
    /// delete, or a free: marking `Both` then would let TTL Free drop Curvine
    /// data that UFS does not actually hold.
    fn ufs_mark_applicable(inode: &InodeView, expected: FileGen) -> bool {
        Self::ufs_mark_candidate(inode).is_some_and(|file| file.gen() == expected)
    }

    /// Only a complete, quiescent file that still owns a Curvine copy may
    /// receive a UFS mark.
    fn ufs_mark_candidate(inode: &InodeView) -> Option<&InodeFile> {
        let File(file) = inode else {
            return None;
        };

        if !file.is_complete() || file.is_writing() {
            return None;
        }

        // `Ufs` means the Curvine copy was already released: `save_ufs` would
        // flip it back to `Both` with no data behind the claim.
        if !matches!(
            file.storage_policy.state,
            StorageState::Cv | StorageState::Both
        ) {
            return None;
        }

        Some(file)
    }

    /// Master-owned xattrs (ctime, trash markers) are never client-writable.
    /// Every client-facing mutation that forwards a `SetAttrOpts` must call
    /// this, so a new caller cannot silently gain write access to them.
    pub(crate) fn check_not_internal_x_attr(opts: &SetAttrOpts) -> FsResult<()> {
        let reserved = opts
            .add_x_attr
            .keys()
            .map(String::as_str)
            .chain(opts.remove_x_attr.iter().map(String::as_str))
            .find(|key| key.starts_with('\0'));

        match reserved {
            Some(key) => err_ext!(FsError::invalid_argument(format!(
                "xattr {} is reserved for internal master metadata",
                key.escape_default()
            ))),
            None => Ok(()),
        }
    }

    /// Clients may not set ctime or TTL-type markers, and every mutation refreshes ctime.
    /// Shared by `set_attr` and `complete_file` so both paths behave identically.
    fn normalize_internal_x_attrs(opts: &mut SetAttrOpts) {
        opts.add_x_attr.remove(INTERNAL_CTIME_XATTR);
        opts.add_x_attr.remove(INTERNAL_TTL_TYPE_XATTR);
        opts.remove_x_attr
            .retain(|key| key != INTERNAL_CTIME_XATTR && key != INTERNAL_TTL_TYPE_XATTR);
        let ctime = LocalTime::mills() as i64;
        opts.add_x_attr.insert(
            INTERNAL_CTIME_XATTR.to_string(),
            ctime.to_le_bytes().to_vec(),
        );
    }

    pub fn unprotected_set_attr(&self, inode: InodePtr, opts: SetAttrOpts) -> FsResult<()> {
        if inode.is_file_entry() {
            return err_box!("set_attr is not supported on unresolved FileEntry inodes; resolve/load the full inode before calling set_attr");
        }

        // Every directory write publishes through its live node. The root has
        // no parent edge, so its handle comes from `FsDir` itself.
        enum Element {
            Dir {
                name: String,
                node: Arc<DirNode>,
            },
            File {
                view: InodeView,
            },
            /// Borrowed live view (files): publication writes it.
            Borrowed(InodePtr),
        }

        let root_node = inode
            .as_ref()
            .as_dir_ref()
            .ok()
            .and_then(|dir| dir.node.clone())
            .or_else(|| (inode.id() == ROOT_INODE_ID).then(|| self.root.clone()));
        let root_name = inode.name().to_string();
        let mut stack = LinkedList::new();
        if inode.is_dir() {
            match root_node {
                Some(node) => stack.push_back(Element::Dir {
                    name: root_name,
                    node,
                }),
                None => {
                    return err_box!(
                        "set_attr: directory {} has no live node handle; cannot publish (id={})",
                        inode.name(),
                        inode.id()
                    );
                }
            }
        } else {
            // Keep the caller's pointer: its box backs the projected view and
            // the publication must write it for the returned status.
            stack.push_back(Element::Borrowed(inode.clone()));
        }

        let child_opts = opts.child_opts();
        let mut change_inodes = Vec::new();
        let mut targets: Vec<Option<SetAttrTarget>> = Vec::new();

        while let Some(element) = stack.pop_front() {
            let (cur_id, cur_node, cur_view, cur_live) = match element {
                Element::Dir { name, node } => {
                    let id = node.id;
                    let view = InodeDir::from_dir_node_live(name, node.clone());
                    let children = node.children.clone();
                    if opts.recursive {
                        // children.list_options returns owned views; directory
                        // views carry their node backlink (live projection).
                        for child in children.list_options(&ListOptions::default()) {
                            let name = child.name().to_string();
                            if child.is_dir() {
                                let node = child
                                    .as_dir_ref()?
                                    .node
                                    .clone()
                                    .expect("live projection must carry its node");
                                stack.push_back(Element::Dir { name, node });
                            } else {
                                let full = self
                                    .store
                                    .get_inode(child.id(), Some(&name))?
                                    .ok_or_else(|| {
                                        FsError::common(format!(
                                            "set_attr: directory entry '{}' references missing inode {}",
                                            name,
                                            child.id()
                                        ))
                                    })?;
                                stack.push_back(Element::File { view: full });
                            }
                        }
                    }
                    (id, Some(node), view, None)
                }
                Element::File { view } => (view.id(), None, view, None),
                Element::Borrowed(ptr) => (ptr.id(), None, ptr.as_ref().clone(), Some(ptr)),
            };
            let set_opts = if cur_id != inode.id() {
                child_opts.clone()
            } else {
                opts.clone()
            };
            // set_attr assigns mtime exactly (it may move backwards); the
            // attribute state is the live/persisted authority, so mirror the
            // requested value into it. Inode::mtime() on a directory reads that
            // state, so take the request value, not `updated.mtime()`.
            let assigned_mtime = set_opts.mtime;
            let mut updated = cur_view.clone_metadata();
            updated.set_attr(set_opts)?;
            if updated.is_dir() {
                let Some(node) = &cur_node else {
                    return err_box!(
                        "set_attr: directory {} is missing its live node handle (id={})",
                        cur_view.name(),
                        cur_id
                    );
                };
                let mut attributes = node.directory_attributes();
                if let Some(mtime) = assigned_mtime {
                    attributes.mtime = mtime;
                }
                attributes.ctime = updated
                    .as_dir_ref()?
                    .marker_ctime()
                    .unwrap_or(attributes.ctime);
                updated.set_directory_attributes(attributes);
            }
            change_inodes.push(updated);
            let target = match (&cur_node, &cur_live) {
                (Some(node), _) => Some(SetAttrTarget::Dir(node.clone())),
                (None, Some(live)) => Some(SetAttrTarget::View(live.clone())),
                (None, None) => Some(SetAttrTarget::View(InodePtr::from_owned(cur_view.clone()))),
            };
            targets.push(target);
        }

        self.store.apply_set_attr(&change_inodes)?;
        for (target, updated) in targets.into_iter().zip(change_inodes) {
            match target {
                Some(SetAttrTarget::Dir(node)) => {
                    // Precise assignment (mtime may move backwards): mirror
                    // the assigned fields into the live attribute state.
                    let mut attributes = node.directory_attributes();
                    attributes.mtime = updated.mtime();
                    attributes.ctime = updated.ctime();
                    attributes.nlink = updated.nlink().unwrap_or(2);
                    node.set_attribute_values(attributes);
                    let updated_dir = updated.as_dir_ref()?;
                    node.store_meta(updated_dir.to_dir_meta());
                }
                Some(SetAttrTarget::View(ptr)) => {
                    // Files are owned views backed by the caller's pointer:
                    // publishing is a plain in-place overwrite.
                    *ptr.as_mut() = updated;
                }
                None => {}
            }
        }
        Ok(())
    }

    pub fn symlink(
        &self,
        target: String,
        link: InodePath,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        let new_inode = InodeFile::with_link(
            self.inode_id.next()?,
            op_ms as i64,
            target,
            mode,
            owner,
            group,
        );

        let link = self.unprotected_symlink(link, new_inode.clone(), force)?;
        self.journal_writer
            .log_symlink(self, link.path(), new_inode, force)?;
        Ok(())
    }

    pub fn unprotected_symlink(
        &self,
        mut link: InodePath,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<InodePath> {
        // check parent
        let parent = ParentMut::get(&link, -2)?;

        let old_inode = if let Some(v) = link.get_last_inode() {
            if !v.is_link() || (v.is_link() && !force) {
                return err_ext!(FsError::file_exists(link.path()));
            } else {
                Some(v)
            }
        } else {
            None
        };

        let name = link.name().to_string();
        parent.validate_child_name(&name)?;
        let is_add = old_inode.is_none();
        let new_inode = InodeView::new_file(name, new_inode);
        // Replay may deliver the same inode twice; a distinct id means the old
        // link inode loses one hard link in the same store batch.
        if old_inode
            .as_ref()
            .is_some_and(|inode| inode.id() == new_inode.id())
        {
            return Ok(link);
        }
        if let Some(inode) = old_inode.as_ref() {
            // A link inode never owns blocks; any block list means it is a
            // regular file, not a replaceable symlink.
            if !inode.as_file_ref()?.block_ids().is_empty() {
                return err_ext!(FsError::file_exists(link.path()));
            }
        }
        let mut persisted_parent = parent.view(link.get_component_neg(-2)?)?.clone_metadata();
        persisted_parent.update_mtime(new_inode.mtime());
        let link_delta = parent
            .directory_attribute_delta(
                DirectoryAttributeDelta::for_child(new_inode.mtime()).for_child_entry(),
            )
            .map_err(CommonError::from)?;
        self.store.apply_symlink(
            &persisted_parent,
            &new_inode,
            old_inode.as_ref().map(|inode| inode.id()),
            link_delta,
        )?;

        let delta = DirectoryAttributeDelta::for_child(new_inode.mtime());
        if is_add {
            self.publish_metadata(parent.publish_directory_delta(delta.for_child_entry()))?;
        } else {
            self.publish_metadata(parent.publish_directory_delta(delta))?;
        }
        self.publish_metadata(parent.mark_persisted())?;
        match old_inode {
            Some(v) => {
                self.publish_metadata(parent.delete_child(v.id(), link.name()))?;
                let added = self.publish_metadata(parent.add_child(new_inode))?;
                *link
                    .inodes
                    .last_mut()
                    .expect("existing symlink has an inode") = added.into_entry();
            }
            None => {
                let added = self.publish_metadata(parent.add_child(new_inode))?;
                self.publish_metadata(link.append(added.into_entry()))?;
            }
        }
        Ok(link)
    }

    // Create a link to an existing file
    pub fn link(&self, src_path: InodePath, dst_path: InodePath) -> FsResult<()> {
        let op_ms = LocalTime::mills();

        // Get the original inode ID and update nlink in memory if it's a direct File
        let (original_inode_id, mut original_inode_ptr) = match src_path.get_last_inode() {
            Some(inode) => match inode.as_ref() {
                File(file) => {
                    // Hard links to regular files and symlinks are valid; directories are not.
                    if !matches!(
                        file.file_type,
                        curvine_model::FileType::File | curvine_model::FileType::Link
                    ) {
                        return err_ext!(FsError::common("Cannot create link to non-regular file"));
                    }
                    (file.id, Some(inode.clone()))
                }
                FileEntry(e) => (e.id, None), // FileEntry already points to an inode
                Dir(_) => return err_ext!(FsError::common("Cannot create link to directory")),
            },
            None => return err_ext!(FsError::file_not_found(src_path.path())),
        };

        // Create the link
        let dst_path_str = dst_path.path().to_string();
        self.unprotected_link(dst_path, original_inode_id, op_ms)?;

        if let Some(ref mut inode_ptr) = original_inode_ptr {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(op_ms as i64);
            }
        }

        // Log the operation
        self.journal_writer
            .log_link(self, src_path.path(), &dst_path_str, op_ms as i64)?;

        Ok(())
    }

    pub fn unprotected_link(
        &self,
        mut new_path: InodePath,
        original_inode_id: i64,
        op_ms: u64,
    ) -> FsResult<InodePath> {
        // Check if the new path already exists
        if new_path.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(new_path.path()));
        }

        // Create parent directory if needed
        new_path = self.create_parent_dir(new_path, MkdirOpts::with_create(true))?;

        // Get the parent directory
        let parent = ParentMut::get(&new_path, -2)?;

        // Create a FileEntry that points to the original inode
        let name = new_path.name().to_string();
        parent.validate_child_name(&name)?;
        let file_entry = InodeView::new_entry(name.clone(), original_inode_id);

        // Persist before publishing the new dentry to the live child index.
        let mut persisted_parent = parent
            .view(new_path.get_component_neg(-2)?)?
            .clone_metadata();
        persisted_parent.update_mtime(op_ms as i64);
        let link_delta = parent
            .directory_attribute_delta(
                DirectoryAttributeDelta::for_child(op_ms as i64).for_child_entry(),
            )
            .map_err(CommonError::from)?;
        self.store.apply_link(
            &persisted_parent,
            &file_entry,
            original_inode_id,
            op_ms as i64,
            link_delta,
        )?;
        let delta = DirectoryAttributeDelta::for_child(op_ms as i64).for_child_entry();
        self.publish_metadata(parent.publish_directory_delta(delta))?;
        self.publish_metadata(parent.mark_persisted())?;
        let added = self.publish_metadata(parent.add_child(file_entry))?;
        self.publish_metadata(new_path.append(added.into_entry()))?;

        Ok(new_path)
    }

    /// Resize a file to the specified length.
    ///
    /// This method changes the file size by either extending or truncating it.
    /// - If the new size is larger than the current size, new blocks are allocated.
    /// - If the new size is smaller, blocks beyond the new size are marked for deletion.
    ///
    /// # Arguments
    /// * `inp` - The inode path of the file to resize
    /// * `opts` - File allocation options containing the target length and allocation mode
    ///
    /// # Returns
    /// * `DeleteResult` - Contains blocks that need to be deleted from workers
    ///
    /// # Process
    /// 1. Resize the file metadata (extend or truncate blocks)
    /// 2. Complete the file operation to update metadata state
    /// 3. Collect locations of blocks to be deleted
    /// 4. Persist changes to store and write journal entry
    pub fn resize(&mut self, inp: &InodePath, opts: FileAllocOpts) -> FsResult<DeleteResult> {
        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };
        self.resize_inode(inp.path(), &mut inode, opts)
    }

    pub fn resize_inode(
        &self,
        audit_path: &str,
        inode: &mut InodePtr,
        opts: FileAllocOpts,
    ) -> FsResult<DeleteResult> {
        let mut updated = inode.as_ref().clone_metadata();
        let file = updated.as_file_mut()?;

        if file.len == opts.len {
            return Ok(DeleteResult::new());
        }
        let del_blocks = file.resize(opts.clone())?;
        debug!("resize file {} success, opts: {:?}", audit_path, opts);

        file.complete(file.len, &[], "", true)?;
        let mut del_res = DeleteResult::new();
        for meta in del_blocks {
            let locs = self.get_locations(&meta)?;
            if !locs.is_empty() {
                del_res.blocks.insert(meta.id, locs);
            }
        }

        self.store.apply_complete_file(&updated, &[])?;
        *inode.as_mut() = updated;
        self.journal_writer
            .log_complete_file(self, audit_path, inode.as_file_ref()?, vec![])?;

        Ok(del_res)
    }

    pub fn assign_worker(
        &self,
        inp: InodePath,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<ExtendedBlock> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        self.assign_worker_inode(inp.path(), &mut inode, block_id, workers)
            .map(|(block, _assigned)| block)
    }

    pub fn assign_worker_inode(
        &self,
        audit_path: &str,
        inode: &mut InodePtr,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<(ExtendedBlock, bool)> {
        let mut updated = inode.as_ref().clone_metadata();
        let file = updated.as_file_mut()?;

        let block = file.search_block_mut_check(block_id)?;
        let assigned = block.assign_worker(workers);
        let block = ExtendedBlock {
            id: block.id,
            len: block.len as i64,
            alloc_opts: block.alloc_opts.clone(),
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
        };

        if assigned {
            self.store.apply_new_block(&updated, &[])?;
            *inode.as_mut() = updated;
            self.journal_writer
                .log_add_block(self, audit_path, inode.as_file_ref()?, vec![])?;
        }

        Ok((block, assigned))
    }

    pub fn get_locations(&self, meta: &BlockMeta) -> CommonResult<Vec<BlockLocation>> {
        if let Some(locs) = &meta.locs {
            Ok(locs.clone())
        } else {
            self.store.get_locations(meta.id)
        }
    }

    pub fn get_lock(
        &self,
        inp: InodePath,
        lock: &FileLock,
        expire_ms: u64,
    ) -> FsResult<Option<FileLock>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let mut meta = self.store.get_locks(inode.id())?;
        let conflict = meta.check_conflict(lock, expire_ms);
        Ok(conflict)
    }

    pub fn set_lock(
        &self,
        inp: InodePath,
        lock: FileLock,
        expire_ms: u64,
    ) -> FsResult<Option<FileLock>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let mut meta = self.store.get_locks(inode.id())?;
        let (conflict, changed) = meta.set_lock_with_change(lock, expire_ms);

        if !changed {
            return Ok(conflict);
        }

        let locks = meta.to_vec();
        self.store.apply_set_locks(inode.id(), &locks)?;
        self.journal_writer.log_set_locks(self, inode.id(), locks)?;

        Ok(conflict)
    }
}
