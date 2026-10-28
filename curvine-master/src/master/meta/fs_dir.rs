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
use crate::master::journal::{JournalEntry, JournalWriter};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::*;
use crate::master::meta::store::{InodeStore, PurgeItem, PurgeTarget, RocksInodeStore};
use crate::master::meta::{trash, BlockMeta, InodeId};
use crate::master::quota::eviction::evictor::Evictor;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    BlockLocation, CommitBlock, CreateFileOpts, ExtendedBlock, FileAllocOpts, FileLock, FileStatus,
    FreeResult, LeaderSnapshotInfo, ListOptions, MkdirOpts, MountInfo, RenameFlags, SetAttrOpts,
    TrashEntry, TtlAction, WorkerAddress, INTERNAL_CTIME_XATTR,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, LinkedList};
use std::mem;
use std::sync::Arc;

const MODE_SETGID: u32 = 0o2000;

/// Note: The modification operation uses &mut self, which is a necessary improvement. We use the unsafe API to perform modifications.
pub struct FsDir {
    pub(crate) root_dir: InodeView,
    pub(crate) inode_id: InodeId,
    pub(crate) store: InodeStore,
    pub(crate) journal_writer: Arc<JournalWriter>,
    pub(crate) evictor: Arc<dyn Evictor>,
    pub(crate) op_id: AtomicCounter,
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
    parent: InodePtr,
    child_id: i64,
    name: String,
    expired: bool,
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

impl FsDir {
    pub fn new(
        conf: &ClusterConf,
        journal_writer: Arc<JournalWriter>,
        ttl_bucket_list: Arc<TtlBucketList>,
        evictor: Arc<dyn Evictor>,
    ) -> FsResult<Self> {
        let db_conf = conf.db_conf();

        let store = RocksInodeStore::new(db_conf, conf.format_master)?;
        let state = InodeStore::new(store, ttl_bucket_list)?;
        let (last_inode_id, root_dir) = state.create_blank_tree()?;

        let fs_dir = Self {
            root_dir,
            inode_id: InodeId::new(),
            store: state,
            journal_writer,
            evictor,
            op_id: AtomicCounter::new(0),
        };
        fs_dir.update_last_inode_id(last_inode_id)?;

        Ok(fs_dir)
    }

    // Create root directory
    pub fn create_root() -> InodeView {
        InodeView::new_dir(ROOT_INODE_NAME.to_string(), InodeDir::new(ROOT_INODE_ID, 0))
    }

    pub fn root_ptr(&self) -> InodePtr {
        InodePtr::from_ref(&self.root_dir)
    }

    pub fn root_dir(&self) -> &InodeView {
        &self.root_dir
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

            let inode_view = match self.store.get_inode(current_id, None)? {
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
        if op_id > self.op_id.get() {
            self.op_id.set(op_id);
        }
    }

    pub fn get_ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.store.get_ttl_bucket_list()
    }

    pub fn mkdir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        // Create parent directory
        inp = self.create_parent_dir(inp, opts.parent_opts())?;

        // Create the final directory.
        inp = self.create_single_dir(inp, opts)?;
        Ok(inp)
    }

    // Create the first subdirectory that does not exist.
    // 1. If all directories on the path already exist, skip and return successful.
    // 2. If the parent directory does not exist, an error is returned.
    fn create_single_dir(
        &mut self,
        mut inp: InodePath,
        mut opts: MkdirOpts,
    ) -> FsResult<InodePath> {
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
        self.journal_writer.log_mkdir(self, &parent_path, &dir)?;

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

    // Create all previous directories that may be missing on the path.
    fn create_parent_dir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        let mut index = inp.existing_len();

        // The parent directory already exists and does not need to be created.
        if inp.is_full() || index + 1 >= inp.len() {
            return Ok(inp);
        }

        while index <= inp.len() - 2 {
            inp = self.create_single_dir(inp, opts.clone())?;
            index += 1;
        }

        Ok(inp)
    }

    // Delete files or directories
    pub fn delete(&mut self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
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
        self.journal_writer
            .log_delete(self, inp.path(), op_ms as i64)?;

        Ok(del_res)
    }

    pub(crate) fn unprotected_delete(
        &mut self,
        inp: &InodePath,
        mtime: i64,
    ) -> FsResult<DeleteResult> {
        let target = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("Path not exists: {}", inp.path()),
        };

        let mut parent = match inp.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Abnormal data status"),
        };
        let child = target.as_ref();
        let child_name = inp.name();

        // Handle different types of nodes.
        // An already-expired parent TTL must keep its mtime: the checker can
        // delete child files first, and bumping mtime would un-expire the
        // directory for the rest of the same cleanup pass.
        // Evaluate expiry at this delete's `mtime` so journal replay matches the
        // leader. `expiration_ms` can fail (e.g. FileEntry); never fail the delete.
        match parent.as_ref().expiration_ms() {
            Ok(Some(exp)) if mtime > exp => {}
            _ => parent.update_mtime(mtime),
        }

        let del_res = match child {
            File(f) => {
                if f.nlink() > 1 {
                    let target_inode = target.clone();
                    if let File(ref mut nf) = target_inode.as_mut() {
                        nf.decrement_nlink(mtime);
                    }
                    self.store
                        .apply_unlink(parent.as_ref(), child, child_name, mtime)?
                } else {
                    // This is the last link, delete the inode
                    self.store
                        .apply_delete(parent.as_ref(), child, child_name, mtime)?
                }
            }
            FileEntry(e) => {
                // This is a link entry, just remove the directory entry
                // The actual inode's nlink count should be decremented
                self.store.apply_unlink_file_entry(
                    parent.as_ref(),
                    child,
                    child_name,
                    e.id,
                    mtime,
                )?
            }
            Dir(_) => {
                parent.dec_nlink(mtime);
                // Directories are always deleted
                self.store
                    .apply_delete(parent.as_ref(), child, child_name, mtime)?
            }
        };

        // After deletion occurs, the target address cannot be used.
        let _ = parent.delete_child(child.id(), child_name)?;
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
    pub fn purge_paths(&mut self, paths: &[String], mtime: i64) -> FsResult<DeleteResult> {
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
            let inp = InodePath::resolve(self.root_ptr(), path, &self.store)?;

            let Some(child) = inp.get_last_inode() else {
                debug!("purge: skip vanished path {}", path);
                continue;
            };
            let Some(parent) = inp.get_inode(-2) else {
                return err_box!("purge: path {} has no parent directory", path);
            };
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
                .or_insert_with(|| parent.as_ref().clone_metadata());
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
                parent,
                child_id: child.id(),
                name,
                expired,
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
        let total = self.store.apply_purge(items, mtime)?;

        for claim in claims {
            let parent = claim.parent.as_mut();
            // Re-read the live child: a hydrated `FileEntry` copy was already
            // freed with its InodePath, and only a tree-resident file inode
            // holds the shared nlink.
            if let Some(child) = parent.get_child_ptr(&claim.name) {
                // Files keep their shared inode when other names still link to it.
                if matches!(child.as_ref(), File(f) if f.nlink() > 1) {
                    child.as_mut().dec_nlink(mtime);
                }
            }

            if !claim.expired {
                parent.update_mtime(mtime);
            }
            if claim.is_dir {
                parent.dec_nlink(mtime);
            }
            // The store batch already committed, so the child is gone from the
            // durable tree. Aborting here would drop `total`, and the block
            // locations it carries could then never be released; report the
            // in-memory divergence instead of losing the reclamation.
            if let Err(e) = parent.delete_child(claim.child_id, &claim.name) {
                warn!(
                    "purge: {} is deleted from the store but could not be dropped from the in-memory tree: {}",
                    claim.name, e
                );
            }
        }

        Ok(total)
    }

    // Move an entry into /.Trash instead of removing it. Blocks stay on the
    // workers so the entry can be restored until retention expires.
    pub fn trash(&mut self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
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
        let dst_inp = InodePath::resolve(self.root_ptr(), &dst_path, &self.store)?;

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
    pub fn trash_pending(&mut self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
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
        let dst_inp = InodePath::resolve(self.root_ptr(), &dst_path, &self.store)?;

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
    pub fn claim_trash_entry_for_purge(&mut self, trash_path: &str) -> FsResult<Option<String>> {
        if !trash::is_trash_entry_path(trash_path) {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is not a trash entry",
                trash_path
            )));
        }

        let src_inp = InodePath::resolve(self.root_ptr(), trash_path, &self.store)?;
        if src_inp.get_last_inode().is_none() {
            return Ok(None);
        }

        self.ensure_pending_root()?;
        let dst_path = trash::pending_entry_path(src_inp.name());
        let dst_inp = InodePath::resolve(self.root_ptr(), &dst_path, &self.store)?;

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

    fn ensure_trash_root(&mut self) -> FsResult<()> {
        self.ensure_trash_dir(trash::TRASH_ROOT)
    }

    fn ensure_pending_root(&mut self) -> FsResult<()> {
        self.ensure_trash_dir(trash::TRASH_PENDING_ROOT)
    }

    fn ensure_trash_dir(&mut self, path: &str) -> FsResult<()> {
        let inp = InodePath::resolve(self.root_ptr(), path, &self.store)?;
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
                InodePath::resolve(self.root_ptr(), path, &self.store)
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
        let inp = InodePath::resolve(self.root_ptr(), trash::TRASH_ROOT, &self.store)?;
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
            let child_inp = InodePath::resolve(self.root_ptr(), &path, &self.store)?;
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
        let src_inp = InodePath::resolve(self.root_ptr(), trash_path, &self.store)?;
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
        let inp = InodePath::resolve(self.root_ptr(), trash::TRASH_ROOT, &self.store)?;
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
        let inp = InodePath::resolve(self.root_ptr(), trash::TRASH_PENDING_ROOT, &self.store)?;
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

        let src_inp = InodePath::resolve(self.root_ptr(), trash_path, &self.store)?;
        let default_dst = dst.is_none() || dst.is_some_and(|value| value.is_empty());
        let dst = self.resolve_restore_dst(&src_inp, dst)?;

        let dst_inp = InodePath::resolve(self.root_ptr(), &dst, &self.store)?;
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

        let restored_inp = InodePath::resolve(self.root_ptr(), &dst, &self.store)?;
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
        let inp = InodePath::resolve(self.root_ptr(), trash::TRASH_ROOT, &self.store)?;
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
    pub fn stage_trash_entries(&mut self, names: &[String]) -> FsResult<u64> {
        if names.is_empty() {
            return Ok(0);
        }
        self.ensure_pending_root()?;

        let mtime = LocalTime::mills() as i64;
        let mut staged = 0;
        for name in names {
            let src_path = trash::trash_entry_path(name);
            let dst_path = trash::pending_entry_path(name);

            let src = match InodePath::resolve(self.root_ptr(), &src_path, &self.store) {
                Ok(inp) => inp,
                Err(e) => {
                    debug!("stage: skip unresolvable trash entry {}: {}", src_path, e);
                    continue;
                }
            };
            if src.get_last_inode().is_none() {
                continue;
            }

            let dst = InodePath::resolve(self.root_ptr(), &dst_path, &self.store)?;
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

    pub fn free(&mut self, inp: &InodePath, recursive: bool) -> FsResult<FreeResult> {
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

    pub(crate) fn unprotected_free(
        &mut self,
        inode: InodePtr,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<FreeResult> {
        let mut free_res = FreeResult::default();
        let mut change_inodes = vec![];

        let mut stack = LinkedList::new();
        stack.push_back(inode);
        while let Some(inode) = stack.pop_front() {
            match inode.as_mut() {
                FileEntry(e) => {
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back(InodePtr::from_owned(store_inode));
                    }
                }

                Dir(d) => {
                    if recursive {
                        for child in d.children_iter() {
                            stack.push_back(InodePtr::from_ref(child));
                        }
                    }
                }

                File(f) => {
                    let locs = f.get_locs(&self.store)?;
                    let bytes = f.get_locs_bytes(&locs);
                    if f.free(mtime) {
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
        &mut self,
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
        &mut self,
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
        self.journal_writer.log_rename(
            self,
            src_inp.path(),
            dst_inp.path(),
            op_ms,
            flags,
            exchange_pre_swap_ids,
        )?;
        Ok(res)
    }

    pub(crate) fn unprotected_rename(
        &mut self,
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

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };

        let del_res = if let Some(dst_inode) = dst_inp.get_last_inode() {
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
            Some(self.unprotected_delete(dst_inp, mtime)?)
        } else {
            None
        };

        let new_name = dst_inp.name().to_string();
        // /1.log -> /a/b : b is the new inode name and /a is the destination parent.
        // For replace semantics (dst exists), dst has been deleted above and we still
        // rename to dst's original parent with dst's original name.
        let mut dst_parent = match dst_inp.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent {} does not exist", dst_inp.get_parent_path()),
        };

        // Modify the time and name of the rename node.
        let mut new_inode = src_inode.as_ref().clone_metadata();
        new_inode.change_name(new_name);
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

        // Update the parent directory for the last modification time.
        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        // A directory is a link from each parent; cross-parent moves must
        // transfer it so live execution and journal replay agree.
        if src_parent.id() != dst_parent.id() && src_inode.is_dir() {
            src_parent.dec_nlink(mtime);
            dst_parent.incr_nlink(mtime);
        }

        // Update state.
        self.store.apply_rename(
            src_parent.as_ref(),
            src_inode.as_ref(),
            src_inp.name(),
            dst_parent.as_ref(),
            &new_inode,
        )?;

        // Update memory status.
        // step 1: Delete the original node.
        // step 2: Add a new node.
        let detached = src_parent.delete_child(src_inode.id(), src_inode.name())?;
        // Persist first, then transfer the subtree under the same namespace lock.
        new_inode.adopt_directory_children(detached);
        let _ = dst_parent.add_child(new_inode)?;

        Ok(del_res)
    }

    fn unprotected_exchange(
        &mut self,
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

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };
        let mut dst_parent = match dst_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", dst_inp.path()),
            Some(v) => v,
        };

        let src_name = src_inp.name().to_string();
        let dst_name = dst_inp.name().to_string();

        let mut at_src = dst_inode.as_ref().clone_metadata();
        at_src.change_name(src_name.clone());
        at_src.set_parent_id(src_parent.id());

        let mut at_dst = src_inode.as_ref().clone_metadata();
        at_dst.change_name(dst_name.clone());
        at_dst.set_parent_id(dst_parent.id());

        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        if src_parent.id() != dst_parent.id() {
            let src_was_dir = src_inode.is_dir();
            let dst_was_dir = dst_inode.is_dir();
            if src_was_dir && !dst_was_dir {
                src_parent.dec_nlink(mtime);
            } else if !src_was_dir && dst_was_dir {
                src_parent.incr_nlink(mtime);
            }
            if dst_was_dir && !src_was_dir {
                dst_parent.dec_nlink(mtime);
            } else if !dst_was_dir && src_was_dir {
                dst_parent.incr_nlink(mtime);
            }
        }

        self.store.apply_exchange(
            src_parent.as_ref(),
            &src_name,
            dst_parent.as_ref(),
            &dst_name,
            &at_src,
            &at_dst,
        )?;

        let detached_src = src_parent.delete_child(src_id, &src_name)?;
        let detached_dst = dst_parent.delete_child(dst_id, &dst_name)?;
        at_src.adopt_directory_children(detached_dst);
        at_dst.adopt_directory_children(detached_src);
        let _ = src_parent.add_child(at_src)?;
        let _ = dst_parent.add_child(at_dst)?;

        Ok(())
    }

    pub fn create_file(
        &mut self,
        mut inp: InodePath,
        mut opts: CreateFileOpts,
    ) -> FsResult<InodePath> {
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
        inp = self.add_last_inode(inp, InodeView::new_file(name, file))?;
        self.journal_writer.log_create_file(self, &inp)?;

        Ok(inp)
    }

    pub(crate) fn add_last_inode(
        &mut self,
        mut inp: InodePath,
        child: InodeView,
    ) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() as i32;

        // parent must be an existing directory.
        let mut parent = match inp.get_inode(pos - 1) {
            Some(v) => {
                if !v.is_dir() {
                    return err_box!("Parent path is not a directory: {}", inp.get_parent_path());
                } else {
                    v
                }
            }

            None => return err_box!("Parent path not exists: {}", inp.get_parent_path()),
        };

        // Update the parent directory for the last modification time.
        parent.update_mtime(child.mtime());
        if child.is_dir() {
            parent.incr_nlink(child.mtime());
        }

        let added = parent.add_child(child)?;
        self.store.apply_add(parent.as_ref(), added.as_ref())?;
        inp.append(added)?;

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
                let iter = d.children_iter().collect();
                let res = self.store.batched_get_inodes(inp, iter)?;
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
                let res = self.store.batched_get_inodes(inp, children)?;
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
        &mut self,
        path: impl AsRef<str>,
        mut inode: InodePtr,
        commit_blocks: Vec<CommitBlock>,
        choose_workers: &[WorkerAddress],
        file_len: i64,
    ) -> FsResult<ExtendedBlock> {
        let file = inode.as_file_mut()?;

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
        self.store.apply_new_block(inode.as_ref(), &commit_blocks)?;
        self.journal_writer
            .log_add_block(self, path, inode.as_file_ref()?, commit_blocks)?;
        Ok(block)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file(
        &mut self,
        path: impl AsRef<str>,
        inode: &mut InodePtr,
        len: i64,
        commit_block: Vec<CommitBlock>,
        client_name: impl AsRef<str>,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<bool> {
        let file = inode.as_file_mut()?;
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
                inode.set_attr(opts)?;
            }
        }

        self.evictor.on_access(id);

        self.store
            .apply_complete_file(inode.as_ref(), &commit_block)?;
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
        self.store.add_block_location(block_id, location)?;
        Ok(())
    }

    pub fn get_block_locations(&self, block_id: i64) -> FsResult<Vec<BlockLocation>> {
        Ok(self.store.get_block_locations(block_id)?)
    }

    /// Returns replication state only when the block still belongs to the current file version.
    pub(crate) fn replication_block_state(
        &self,
        block_id: i64,
    ) -> FsResult<Option<ReplicationBlockState>> {
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

    /// Adds a replication result only if the block is still current and still needs a replica.
    /// The caller holds the filesystem write lock, so validation and insertion are atomic with
    /// respect to overwrite and deletion.
    pub(crate) fn add_replication_location_if_needed(
        &mut self,
        block_id: i64,
        location: BlockLocation,
    ) -> FsResult<bool> {
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
        &mut self,
        inp: &InodePath,
        client_name: impl AsRef<str>,
    ) -> FsResult<FileStatus> {
        let inode_ptr = match inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(inp.path())),
            Some(v) => v,
        };

        let mut inode = match inode_ptr.as_ref() {
            File(..) => inode_ptr.as_ref().clone(),
            Dir(..) => {
                let err_msg = format!("Cannot append to already exists {} directory", inp.path());
                return err_ext!(FsError::file_exists(err_msg));
            }
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        let file = inode.as_file_mut()?;
        let _ = file.reopen(client_name);
        let status = inode.to_file_status(inp.path())?;

        self.store.apply_reopen_file(&inode)?;
        self.journal_writer
            .log_reopen_file(self, inp.path(), inode.as_file_ref()?)?;

        Ok(status)
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
    pub fn overwrite_file(
        &mut self,
        inp: &InodePath,
        opts: CreateFileOpts,
    ) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();
        let mut delete_result = DeleteResult::new();

        match inp.get_last_inode() {
            Some(inode) => {
                if !inode.is_file() {
                    return err_box!("Path is not a file: {}", inp.path());
                }

                let file = inode.as_mut().as_file_mut()?;
                for block_meta in &file.blocks {
                    if let Ok(locations) = self.get_block_locations(block_meta.id) {
                        delete_result.blocks.insert(block_meta.id, locations);
                    }
                }
                file.overwrite(opts, op_ms as i64);

                self.store.apply_overwrite_file(inode.as_ref())?;
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
        self.root_dir.print_tree()
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let mut tree_hash = self.root_dir.sum_hash()?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_INODES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_EDGES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_LOCATION)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_BLOCK)?;
        Ok(tree_hash)
    }

    pub fn last_inode_id(&self) -> i64 {
        self.inode_id.current()
    }

    pub fn update_last_inode_id(&self, new_value: i64) -> CommonResult<()> {
        let old_value = self.last_inode_id();
        if new_value > old_value {
            self.inode_id.reset(new_value)
        } else {
            Ok(())
        }
    }

    // Read data from rocksdb to build a directory tree
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        self.store.create_tree().map(|x| x.1)
    }

    // Restore in-memory tree from RocksDB without checkpoint (for testing only).
    // In production, use restore() with checkpoint path via Raft snapshot.
    pub fn restore_from_rocksdb(&mut self) -> CommonResult<()> {
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        self.root_dir = root_dir;
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

        // Set to other value first to facilitate memory recycling.
        self.root_dir = Self::create_root();

        // Reset rocksdb
        self.store.restore(path)?;
        let time1 = spend.used_ms();
        spend.reset();

        // Update the directory tree
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        self.root_dir = root_dir;
        self.update_last_inode_id(last_inode_id)?;
        let time2 = spend.used_ms();

        info!(
            "restore from {}, checkpoint_size={} bytes, restore_rocksdb={} ms, \
        build_tree={} ms (see create_tree log for sub-phase breakdown), \
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

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        let block_ids = self.store.store.delete_locations(worker_id)?;
        Ok(block_ids)
    }

    /// Invalidate UFS-backed files whose cache is no longer readable after a
    /// worker's locations are removed. A single missing block makes the whole
    /// cache copy unusable, even when other blocks or replicas still exist.
    ///
    /// The returned locations belong to the discarded cache copies and should
    /// be scheduled for deletion from any surviving workers.
    pub(crate) fn invalidate_lost_cache_files(
        &mut self,
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

    pub fn store_mount(&mut self, info: MountInfo, send_log: bool) -> FsResult<()> {
        self.store.store.add_mountpoint(info.mount_id, &info)?;

        if send_log {
            self.journal_writer.log_mount(self, info)?;
        }

        Ok(())
    }

    pub fn unprotected_store_mount(&mut self, info: MountInfo) -> FsResult<()> {
        self.store.store.add_mountpoint(info.mount_id, &info)?;
        Ok(())
    }

    pub fn unmount(&mut self, id: u32) -> FsResult<()> {
        self.store.store.remove_mountpoint(id)?;
        self.journal_writer.log_unmount(self, id)?;
        Ok(())
    }

    pub fn unprotected_unmount(&mut self, id: u32) -> FsResult<()> {
        self.store.store.remove_mountpoint(id)?;
        Ok(())
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        self.store.get_mount_table()
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        self.store.get_mount_point(id)
    }

    pub fn set_attr(&mut self, inp: InodePath, mut opts: SetAttrOpts) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        // Internal metadata is master-owned. Persist the operation timestamp in the
        // journal so replay restores it without changing the bincode inode layout.
        Self::normalize_internal_x_attrs(&mut opts);
        self.unprotected_set_attr(inode.clone(), opts.clone())?;
        self.journal_writer.log_set_attr(self, &inp, opts)?;
        Ok(inode.to_file_status(inp.path())?)
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

    /// Clients may not set ctime, and every mutation refreshes it. Shared by
    /// `set_attr` and `complete_file` so both paths behave identically.
    fn normalize_internal_x_attrs(opts: &mut SetAttrOpts) {
        opts.add_x_attr.remove(INTERNAL_CTIME_XATTR);
        opts.remove_x_attr.retain(|key| key != INTERNAL_CTIME_XATTR);
        let ctime = LocalTime::mills() as i64;
        opts.add_x_attr.insert(
            INTERNAL_CTIME_XATTR.to_string(),
            ctime.to_le_bytes().to_vec(),
        );
    }

    pub fn unprotected_set_attr(&mut self, inode: InodePtr, opts: SetAttrOpts) -> FsResult<()> {
        if inode.is_file_entry() {
            return err_box!("set_attr is not supported on unresolved FileEntry inodes; resolve/load the full inode before calling set_attr");
        }

        let mut change_inodes = vec![];
        let mut stack = LinkedList::new();
        stack.push_back(inode.clone());
        let child_opts = opts.child_opts();
        while let Some(cur_inode) = stack.pop_front() {
            if !cur_inode.is_file_entry() {
                let set_opts = if cur_inode.id() != inode.id() {
                    child_opts.clone()
                } else {
                    opts.clone()
                };
                cur_inode.as_mut().set_attr(set_opts)?;
                change_inodes.push(cur_inode.as_ref().clone_metadata());
            }

            match cur_inode.as_mut() {
                Dir(dir) if opts.recursive => {
                    for child in dir.children_iter() {
                        stack.push_back(InodePtr::from_ref(child));
                    }
                }

                FileEntry(e) => {
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back(InodePtr::from_owned(store_inode));
                    } else {
                        warn!(
                            "unprotected_set_attr: missing inode {} for FileEntry '{}'",
                            e.id, e.name
                        );
                    }
                }

                _ => (),
            }
        }

        self.store.apply_set_attr(change_inodes)?;
        Ok(())
    }

    pub fn symlink(
        &mut self,
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
        &mut self,
        mut link: InodePath,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<InodePath> {
        // check parent
        let mut parent = match link.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Directory does not exist"),
        };

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
        parent.update_mtime(new_inode.mtime);
        let is_add = old_inode.is_none();
        let new_inode_ptr = match old_inode {
            Some(v) => {
                let _ = mem::replace(v.as_mut(), InodeView::new_file(name, new_inode));
                v
            }
            None => {
                let added = parent.add_child(InodeView::new_file(name, new_inode))?;
                link.append(added.clone())?;
                added
            }
        };

        self.store
            .apply_symlink(parent.as_ref(), new_inode_ptr.as_ref(), is_add)?;
        Ok(link)
    }

    // Create a link to an existing file
    pub fn link(&mut self, src_path: InodePath, dst_path: InodePath) -> FsResult<()> {
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

        // If we have the original inode in memory, increment its nlink count
        if let Some(ref mut inode_ptr) = original_inode_ptr {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(op_ms as i64);
            }
        }

        // Create the link
        let dst_path_str = dst_path.path().to_string();
        self.unprotected_link(dst_path, original_inode_id, op_ms)?;

        // Log the operation
        self.journal_writer
            .log_link(self, src_path.path(), &dst_path_str, op_ms as i64)?;

        Ok(())
    }

    pub fn unprotected_link(
        &mut self,
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
        let mut parent = match new_path.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent directory does not exist"),
        };

        // Create a FileEntry that points to the original inode
        let name = new_path.name().to_string();
        let file_entry = InodeView::new_entry(name.clone(), original_inode_id);

        // Update parent directory
        parent.update_mtime(op_ms as i64);
        let added = parent.add_child(file_entry)?;
        new_path.append(added.clone())?;

        // Apply changes to storage - this creates an edge pointing to the original inode
        self.store.apply_link(
            parent.as_ref(),
            added.as_ref(),
            original_inode_id,
            op_ms as i64,
        )?;

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
        &mut self,
        audit_path: &str,
        inode: &mut InodePtr,
        opts: FileAllocOpts,
    ) -> FsResult<DeleteResult> {
        let file = inode.as_file_mut()?;

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

        self.store.apply_complete_file(inode.as_ref(), &[])?;
        self.journal_writer
            .log_complete_file(self, audit_path, inode.as_file_ref()?, vec![])?;

        Ok(del_res)
    }

    pub fn assign_worker(
        &mut self,
        inp: InodePath,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<ExtendedBlock> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        self.assign_worker_inode(inp.path(), &mut inode, block_id, workers)
            .map(|(block, _assigned)| block)
    }

    pub fn assign_worker_inode(
        &mut self,
        audit_path: &str,
        inode: &mut InodePtr,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<(ExtendedBlock, bool)> {
        let file = inode.as_file_mut()?;

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
            self.store.apply_new_block(inode.as_ref(), &[])?;
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
