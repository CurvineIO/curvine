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
use crate::master::meta::inode::InodeView::{Dir, File};
use crate::master::meta::inode::*;
use crate::master::meta::{trash, FsDir, RestoredTrashEntry, TrashListing, TrashScanPage};
use curvine_core_error::{err_box, err_ext};
use curvine_error::{FsError, FsResult};
use curvine_model::{ListOptions, MkdirOpts, RenameFlags, TrashEntry};
use curvine_runtime::common::LocalTime;
use log::{debug, warn};

// One scannable container of recycle-bin entries, in listing order: a
// checkpoint directory, or a pre-checkpoint entry that still sits directly
// under the trash root.
struct TrashSource {
    name: String,
    checkpoint: bool,
}

// Where a listing or scan page resumes: at the head of the bin, after a whole
// source, or inside a checkpoint after one of its children.
enum TrashResume {
    Start,
    After(String),
    InCheckpoint { checkpoint: String, entry: String },
}

// How a walk over one checkpoint directory ended.
enum CheckpointWalk {
    // Every child was examined.
    Exhausted,
    // The walk stopped early; `last` is the last child examined.
    Stopped { last: String, more_children: bool },
}

impl FsDir {
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

        let checkpoint = trash::checkpoint_name(op_ms, self.trash_checkpoint_ms);
        self.ensure_trash_dir(&trash::checkpoint_path(&checkpoint))?;
        let entry_name = self.unique_trash_entry_name(op_ms, inode.id())?;
        let dst_path = trash::checkpoint_entry_path(&checkpoint, &entry_name);
        let dst_inp = InodePath::resolve(self.root_ptr(), &dst_path, &self.store)?;

        // The TRASH rename records the origin and the deletion time on the
        // moved inode as part of the same persisted mutation, so a soft delete
        // needs no separate SetAttr + Rename for its markers (only one extra
        // Mkdir when the delete opens a new checkpoint), and a failure can
        // never leave markers on a live inode.
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

    /// Moves a subtree being permanently deleted to
    /// `/.Trash/.purging/<name>` in one journaled rename and returns the staged
    /// path (#1864).
    ///
    /// The caller purges the staged entry in bounded batches after releasing
    /// the namespace lock, so a large recursive delete no longer removes the
    /// whole subtree under one lock hold and one store batch. The path vanishes
    /// from the namespace at once, exactly like a direct delete. Hard links and
    /// unresolved entries are not staged; the caller deletes them directly.
    pub fn stage_for_purge(&mut self, inp: &InodePath) -> FsResult<String> {
        let op_ms = LocalTime::mills() as i64;
        let inode = match inp.get_last_inode() {
            Some(v) if !inp.is_root() => v,
            _ => return err_ext!(FsError::file_not_found(inp.path())),
        };

        self.ensure_trash_root()?;
        self.ensure_trash_dir(trash::TRASH_PURGING_ROOT)?;

        let entry_name = self.unique_trash_entry_name(op_ms, inode.id())?;
        let dst_path = trash::purging_entry_path(&entry_name);
        let dst_inp = InodePath::resolve(self.root_ptr(), &dst_path, &self.store)?;

        // No TRASH flag: the entry gets no origin or delete-time markers, so it
        // can never be listed, restored or swept by the retention pass. PURGE
        // keeps an expired source parent's mtime, matching a direct delete.
        self.rename_with_mtime(
            inp,
            &dst_inp,
            RenameFlags::NO_REPLACE | RenameFlags::PURGE,
            op_ms,
        )?;
        debug!("staged {} for bounded purge as {}", inp.path(), dst_path);
        Ok(dst_path)
    }

    /// Claims a retention-expired trash entry for reclamation by moving it into
    /// `/.Trash/.pending/<name>` in one journaled rename.
    ///
    /// The purger releases the namespace lock between batches; an entry left in
    /// its checkpoint would stay restorable while its subtree is partially
    /// deleted. Once claimed, `RestoreTrash` rejects the path (the staging area
    /// is not an entry), so a restore either wins before the claim or fails
    /// cleanly. Returns the pending path, or `None` when the entry is already
    /// gone (restored or emptied after the scan).
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
        let checkpoint = trash::checkpoint_name(delete_ms, self.trash_checkpoint_ms);
        for seq in 0..1000 {
            let name = if seq == 0 {
                base.clone()
            } else {
                format!("{}-{}", base, seq)
            };
            let mut candidates = vec![trash::checkpoint_entry_path(&checkpoint, &name)];
            // Staged entries share the trash-root name space by name only, not
            // by directory, so every location must be free to reuse a name.
            candidates.push(trash::pending_entry_path(&name));
            candidates.push(trash::purging_entry_path(&name));
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

    // Scannable containers of trash entries in listing order: checkpoint
    // directories and the pre-checkpoint entries still directly under the root.
    // Children iterate in name order and checkpoint names sort chronologically,
    // so this order is also the listing order.
    fn trash_source_of(child: &InodeView) -> Option<TrashSource> {
        let name = child.name();
        if name == trash::TRASH_PENDING_NAME || name == trash::TRASH_PURGING_NAME {
            return None;
        }
        if child.is_dir() && trash::is_checkpoint_name(name) {
            return Some(TrashSource {
                name: name.to_string(),
                checkpoint: true,
            });
        }
        trash::parse_entry_delete_ms(name).map(|_| TrashSource {
            name: name.to_string(),
            checkpoint: false,
        })
    }

    fn trash_root(&self) -> FsResult<Option<InodePtr>> {
        let inp = InodePath::resolve(self.root_ptr(), trash::TRASH_ROOT, &self.store)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(None);
        };
        if !inode.is_dir() {
            return err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                trash::TRASH_ROOT
            )));
        }
        Ok(Some(inode))
    }

    // Materializes every source. Only EmptyTrash and the empty-directory prune
    // need the whole list; listing and scanning walk the root one source at a
    // time, so a large pre-checkpoint bin is not copied on every page.
    fn trash_sources(&self) -> FsResult<Vec<TrashSource>> {
        let Some(root) = self.trash_root()? else {
            return Ok(vec![]);
        };
        let Dir(dir) = root.as_ref() else {
            return Ok(vec![]);
        };

        Ok(dir
            .children_iter()
            .filter_map(Self::trash_source_of)
            .collect())
    }

    // Next source strictly after `after` (from the start when `None`). Skipped
    // non-source children cost one seek each, so walking a flat pre-checkpoint
    // bin stays O(log n) per source instead of rebuilding the source list.
    fn next_trash_source(dir: &InodeDir, after: Option<&str>) -> Option<TrashSource> {
        let mut after = after.map(str::to_string);
        loop {
            let child = dir
                .list_options(&ListOptions {
                    limit: Some(1),
                    start_after: after.clone(),
                })
                .into_iter()
                .next()?;
            after = Some(child.name().to_string());
            if let Some(source) = Self::trash_source_of(child) {
                return Some(source);
            }
        }
    }

    fn has_later_trash_source(dir: &InodeDir, after: &str) -> bool {
        Self::next_trash_source(dir, Some(after)).is_some()
    }

    fn trash_cursor(rel_path: &str) -> trash::TrashCursor {
        trash::TrashCursor {
            path: trash::trash_path(rel_path),
        }
    }

    // Parses a trash-relative resume path: `<name>` resumes after a whole
    // source, `<checkpoint>/<entry>` resumes inside a checkpoint. The child
    // name is a position marker, not an entry reference: a page can stop after
    // a child the walk skips, and that cursor must still resume.
    fn parse_trash_resume(rel: &str) -> Option<TrashResume> {
        let mut parts = rel.split(PATH_SEPARATOR);
        match (parts.next(), parts.next(), parts.next()) {
            (Some(name), None, None) if trash::parse_entry_delete_ms(name).is_some() => {
                Some(TrashResume::After(name.to_string()))
            }
            (Some(checkpoint), Some(entry), None)
                if trash::is_checkpoint_name(checkpoint) && !entry.is_empty() =>
            {
                Some(TrashResume::InCheckpoint {
                    checkpoint: checkpoint.to_string(),
                    entry: entry.to_string(),
                })
            }
            _ => None,
        }
    }

    fn trash_resume(cursor: Option<&trash::TrashCursor>) -> FsResult<TrashResume> {
        let Some(cursor) = cursor else {
            return Ok(TrashResume::Start);
        };
        cursor
            .path
            .strip_prefix(trash::TRASH_ROOT)
            .and_then(|rest| rest.strip_prefix(PATH_SEPARATOR))
            .and_then(Self::parse_trash_resume)
            .ok_or_else(|| {
                FsError::invalid_argument(format!("invalid trash cursor path: {}", cursor.path))
            })
    }

    fn trash_scan_resume(rel: &str) -> FsResult<TrashResume> {
        Self::parse_trash_resume(rel)
            .ok_or_else(|| FsError::invalid_argument(format!("invalid trash scan cursor: {}", rel)))
    }

    fn load_trash_entry(&self, path: &str, name_delete_ms: i64) -> FsResult<Option<TrashEntry>> {
        // Directory children can be unresolved FileEntry pointers; resolve each
        // entry so the full inode (and its trash xattrs) is loaded.
        let child_inp = InodePath::resolve(self.root_ptr(), path, &self.store)?;
        let Some(child) = child_inp.get_last_inode() else {
            return Ok(None);
        };
        let Ok(x_attr) = child.x_attr() else {
            return Ok(None);
        };
        let (Some(origin), Some(stored_delete_ms)) = (
            trash::decode_origin(x_attr),
            trash::decode_delete_ms(x_attr),
        ) else {
            return Ok(None);
        };
        // A name that disagrees with its metadata is not a valid entry;
        // skipping it keeps the name-derived cursor consistent.
        if stored_delete_ms != name_delete_ms {
            return Ok(None);
        }

        Ok(Some(TrashEntry {
            status: child.to_file_status(path)?,
            path: path.to_string(),
            origin,
            delete_ms: stored_delete_ms,
        }))
    }

    pub fn list_trash(
        &self,
        after: Option<&trash::TrashCursor>,
        limit: usize,
    ) -> FsResult<TrashListing> {
        let Some(root) = self.trash_root()? else {
            return Ok(TrashListing {
                entries: vec![],
                next: None,
            });
        };
        let Dir(dir) = root.as_ref() else {
            return err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                trash::TRASH_ROOT
            )));
        };

        // Entry names start with their delete time, so ordering names is enough
        // to resume a listing; only the entries inside the page load their inode
        // and build a FileStatus. Sources are read page by page, so a listing
        // never materializes the whole bin.
        let limit = limit.max(1);
        let mut entries = Vec::new();
        let mut after_name: Option<String>;

        match Self::trash_resume(after)? {
            TrashResume::InCheckpoint { checkpoint, entry } => {
                match self.append_checkpoint_entries(
                    &checkpoint,
                    Some(&entry),
                    limit,
                    &mut entries,
                )? {
                    CheckpointWalk::Exhausted => after_name = Some(checkpoint),
                    CheckpointWalk::Stopped {
                        last,
                        more_children,
                    } => {
                        let rel = format!("{}{}{}", checkpoint, PATH_SEPARATOR, last);
                        let next = (more_children
                            || Self::has_later_trash_source(dir, &checkpoint))
                        .then(|| Self::trash_cursor(&rel));
                        return Ok(TrashListing { entries, next });
                    }
                }
            }
            TrashResume::After(name) => after_name = Some(name),
            TrashResume::Start => after_name = None,
        }

        while let Some(source) = Self::next_trash_source(dir, after_name.as_deref()) {
            after_name = Some(source.name.clone());

            if source.checkpoint {
                match self.append_checkpoint_entries(
                    &source.name,
                    None,
                    limit - entries.len(),
                    &mut entries,
                )? {
                    CheckpointWalk::Exhausted => {}
                    CheckpointWalk::Stopped {
                        last,
                        more_children,
                    } => {
                        let rel = format!("{}{}{}", source.name, PATH_SEPARATOR, last);
                        let next = (more_children
                            || Self::has_later_trash_source(dir, &source.name))
                        .then(|| Self::trash_cursor(&rel));
                        return Ok(TrashListing { entries, next });
                    }
                }
            } else {
                let Some(delete_ms) = trash::parse_entry_delete_ms(&source.name) else {
                    continue;
                };
                let path = trash::trash_path(&source.name);
                if let Some(entry) = self.load_trash_entry(&path, delete_ms)? {
                    entries.push(entry);
                    if entries.len() >= limit {
                        let next = Self::has_later_trash_source(dir, &source.name)
                            .then(|| Self::trash_cursor(&source.name));
                        return Ok(TrashListing { entries, next });
                    }
                }
            }
        }

        Ok(TrashListing {
            entries,
            next: None,
        })
    }

    // Appends up to `room` entries from one checkpoint directory, resuming
    // after `start_after`. `Stopped.last` is the last child examined, so a page
    // that fills mid-directory can resume right after it.
    fn append_checkpoint_entries(
        &self,
        checkpoint: &str,
        start_after: Option<&str>,
        room: usize,
        entries: &mut Vec<TrashEntry>,
    ) -> FsResult<CheckpointWalk> {
        let inp = InodePath::resolve(
            self.root_ptr(),
            trash::checkpoint_path(checkpoint),
            &self.store,
        )?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(CheckpointWalk::Exhausted);
        };
        let Dir(dir) = inode.as_ref() else {
            return Ok(CheckpointWalk::Exhausted);
        };

        let start_len = entries.len();
        let mut after_name = start_after.map(str::to_string);
        loop {
            let remaining = room.saturating_sub(entries.len() - start_len);
            let mut children = dir.list_options(&ListOptions {
                limit: Some(remaining.saturating_add(1)),
                start_after: after_name.clone(),
            });
            let has_more = children.len() > remaining;
            if has_more {
                children.truncate(remaining);
            }

            for child in &children {
                let name = child.name();
                let Some(delete_ms) = trash::parse_entry_delete_ms(name) else {
                    continue;
                };
                let path = trash::checkpoint_entry_path(checkpoint, name);
                if let Some(entry) = self.load_trash_entry(&path, delete_ms)? {
                    entries.push(entry);
                }
            }

            let Some(last) = children.last() else {
                return Ok(CheckpointWalk::Exhausted);
            };
            after_name = Some(last.name().to_string());
            if entries.len() - start_len >= room {
                return Ok(CheckpointWalk::Stopped {
                    last: last.name().to_string(),
                    more_children: has_more,
                });
            }
            if !has_more {
                return Ok(CheckpointWalk::Exhausted);
            }
        }
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

    // Files are stored as unresolved FileEntry pointers in directories; load
    // the full inode so the trash xattrs are available.
    fn trash_child_delete_ms(&self, child: &InodeView) -> Option<i64> {
        if child.is_file_entry() {
            match self.store.get_inode(child.id(), Some(child.name())) {
                Ok(Some(inode)) => inode.x_attr().ok().and_then(trash::decode_delete_ms),
                _ => None,
            }
        } else {
            child.x_attr().ok().and_then(trash::decode_delete_ms)
        }
    }

    fn trash_entry_delete_ms(&self, path: &str) -> Option<i64> {
        let inp = InodePath::resolve(self.root_ptr(), path, &self.store).ok()?;
        let inode = inp.get_last_inode()?;
        inode.x_attr().ok().and_then(trash::decode_delete_ms)
    }

    /// Scans at most `limit` trash children after `start_after` and returns
    /// only the expired entry paths. This avoids building full `FileStatus`
    /// values and sorting the whole bin, and lets the caller release the
    /// namespace lock between pages. `start_after` is a trash-relative entry
    /// path (`<checkpoint>/<name>` or a pre-checkpoint `<name>`).
    pub fn scan_expired_trash(
        &self,
        cutoff_ms: i64,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let limit = limit.max(1);
        let Some(root) = self.trash_root()? else {
            return Ok(TrashScanPage {
                paths: vec![],
                next_after: None,
            });
        };
        let Dir(dir) = root.as_ref() else {
            return err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                trash::TRASH_ROOT
            )));
        };

        let mut paths = Vec::new();
        let mut budget = limit;
        let mut after_name: Option<String>;

        let resume = match start_after {
            Some(rel) => Self::trash_scan_resume(rel)?,
            None => TrashResume::Start,
        };
        // Every entry in a source is at least as new as the source name: a
        // checkpoint entry's delete time falls inside the checkpoint window,
        // and a pre-checkpoint entry's name starts with its delete time. Later
        // sources are newer, so the sweep can stop at the first past the
        // cutoff.
        match resume {
            TrashResume::InCheckpoint { checkpoint, entry } => {
                if Self::source_past_cutoff(&checkpoint, true, cutoff_ms) {
                    return Ok(TrashScanPage {
                        paths,
                        next_after: None,
                    });
                }
                match self.collect_expired_checkpoint_entries(
                    &checkpoint,
                    Some(&entry),
                    cutoff_ms,
                    &mut budget,
                    &mut paths,
                )? {
                    CheckpointWalk::Exhausted => after_name = Some(checkpoint),
                    CheckpointWalk::Stopped {
                        last,
                        more_children,
                    } => {
                        let rel = format!("{}{}{}", checkpoint, PATH_SEPARATOR, last);
                        let next = (more_children
                            || Self::has_later_trash_source(dir, &checkpoint))
                        .then_some(rel);
                        return Ok(TrashScanPage {
                            paths,
                            next_after: next,
                        });
                    }
                }
            }
            TrashResume::After(name) => after_name = Some(name),
            TrashResume::Start => after_name = None,
        }

        loop {
            if budget == 0 {
                break;
            }
            let Some(source) = Self::next_trash_source(dir, after_name.as_deref()) else {
                break;
            };
            after_name = Some(source.name.clone());
            if Self::source_past_cutoff(&source.name, source.checkpoint, cutoff_ms) {
                return Ok(TrashScanPage {
                    paths,
                    next_after: None,
                });
            }

            if source.checkpoint {
                match self.collect_expired_checkpoint_entries(
                    &source.name,
                    None,
                    cutoff_ms,
                    &mut budget,
                    &mut paths,
                )? {
                    CheckpointWalk::Exhausted => {}
                    CheckpointWalk::Stopped {
                        last,
                        more_children,
                    } => {
                        let rel = format!("{}{}{}", source.name, PATH_SEPARATOR, last);
                        let next = (more_children
                            || Self::has_later_trash_source(dir, &source.name))
                        .then_some(rel);
                        return Ok(TrashScanPage {
                            paths,
                            next_after: next,
                        });
                    }
                }
            } else {
                let path = trash::trash_path(&source.name);
                if matches!(
                    self.trash_entry_delete_ms(&path),
                    Some(ms) if ms <= cutoff_ms
                ) {
                    paths.push(path);
                }
                budget -= 1;
                if budget == 0 {
                    let next = Self::has_later_trash_source(dir, &source.name)
                        .then(|| source.name.clone());
                    return Ok(TrashScanPage {
                        paths,
                        next_after: next,
                    });
                }
            }
        }

        Ok(TrashScanPage {
            paths,
            next_after: None,
        })
    }

    fn source_past_cutoff(name: &str, checkpoint: bool, cutoff_ms: i64) -> bool {
        let source_ms = if checkpoint {
            trash::parse_checkpoint_ms(name)
        } else {
            trash::parse_entry_delete_ms(name)
        };
        source_ms.is_some_and(|ms| ms > cutoff_ms)
    }

    // Collects the expired entries of one checkpoint, resuming after
    // `start_after`, and charges every examined child against `budget`.
    fn collect_expired_checkpoint_entries(
        &self,
        checkpoint: &str,
        start_after: Option<&str>,
        cutoff_ms: i64,
        budget: &mut usize,
        paths: &mut Vec<String>,
    ) -> FsResult<CheckpointWalk> {
        let inp = InodePath::resolve(
            self.root_ptr(),
            trash::checkpoint_path(checkpoint),
            &self.store,
        )?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(CheckpointWalk::Exhausted);
        };
        let Dir(dir) = inode.as_ref() else {
            return Ok(CheckpointWalk::Exhausted);
        };

        let mut after_name = start_after.map(str::to_string);
        loop {
            let mut children = dir.list_options(&ListOptions {
                limit: Some(budget.saturating_add(1)),
                start_after: after_name.clone(),
            });
            let has_more = children.len() > *budget;
            if has_more {
                children.truncate(*budget);
            }

            for child in &children {
                let name = child.name();
                if trash::parse_entry_delete_ms(name).is_none() {
                    continue;
                }
                if matches!(self.trash_child_delete_ms(child), Some(ms) if ms <= cutoff_ms) {
                    paths.push(trash::checkpoint_entry_path(checkpoint, name));
                }
            }

            let Some(last) = children.last() else {
                return Ok(CheckpointWalk::Exhausted);
            };
            *budget -= children.len();
            after_name = Some(last.name().to_string());
            if *budget == 0 {
                return Ok(CheckpointWalk::Stopped {
                    last: last.name().to_string(),
                    more_children: has_more,
                });
            }
            if !has_more {
                return Ok(CheckpointWalk::Exhausted);
            }
        }
    }

    /// Returns one bounded page of `/.Trash/.pending` entry paths. Every entry
    /// here is due for immediate reclamation; there is no retention cutoff.
    pub fn scan_pending_trash(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        self.scan_staging_dir(trash::TRASH_PENDING_ROOT, limit, start_after)
    }

    /// Returns one bounded page of `/.Trash/.purging` entry paths. The caller
    /// must skip entries a synchronous delete is still purging.
    pub fn scan_purging_trash(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        self.scan_staging_dir(trash::TRASH_PURGING_ROOT, limit, start_after)
    }

    fn scan_staging_dir(
        &self,
        root: &str,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let limit = limit.max(1);
        let inp = InodePath::resolve(self.root_ptr(), root, &self.store)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(TrashScanPage::default());
        };
        let Dir(dir) = inode.as_ref() else {
            return err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                root
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
            .map(|child| format!("{}{}{}", root, PATH_SEPARATOR, child.name()))
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

    // Trash-relative paths of every purgeable entry, in listing order:
    // `<checkpoint>/<entry>` for the checkpoint layout and `<entry>` for
    // pre-checkpoint entries that still sit directly under the trash root.
    pub fn trash_entry_rel_paths(&self) -> FsResult<Vec<String>> {
        let mut rel_paths = Vec::new();
        for source in self.trash_sources()? {
            if !source.checkpoint {
                rel_paths.push(source.name);
                continue;
            }
            let dir_path = trash::checkpoint_path(&source.name);
            let inp = InodePath::resolve(self.root_ptr(), &dir_path, &self.store)?;
            let Some(dir_inode) = inp.get_last_inode() else {
                continue;
            };
            let Dir(dir) = dir_inode.as_ref() else {
                continue;
            };
            for child in dir.children_iter() {
                let name = child.name().to_string();
                // Keep the purge scope identical to the listing scope: a child
                // whose name is not a trash entry name is invisible to
                // ListTrash, so EmptyTrash must not delete it silently.
                if trash::parse_entry_delete_ms(&name).is_none() {
                    warn!("skipping foreign child under {}: {}", dir_path, name);
                    continue;
                }
                rel_paths.push(format!("{}{}{}", source.name, PATH_SEPARATOR, name));
            }
        }
        Ok(rel_paths)
    }

    // Purge one batch of names. Keeping the loop in the caller lets the
    // write lock be released between batches instead of blocking every
    // metadata mutation for the whole purge.
    /// Moves the named recycle-bin entries into the async staging directory.
    ///
    /// `rel_paths` are trash-relative entry paths: `<checkpoint>/<entry>` for
    /// the checkpoint layout, or `<entry>` for pre-checkpoint entries. Renaming
    /// a whole entry is O(1) and keeps its origin/delete-time xattrs, so
    /// `EmptyTrash` can acknowledge immediately and let the purger reclaim the
    /// subtrees in bounded batches. Entries that disappeared since the snapshot
    /// (restored, purged, or staged by another node) are skipped, which keeps a
    /// retried empty idempotent.
    pub fn stage_trash_entries(&mut self, rel_paths: &[String]) -> FsResult<u64> {
        if rel_paths.is_empty() {
            return Ok(0);
        }
        self.ensure_pending_root()?;

        let mtime = LocalTime::mills() as i64;
        let mut staged = 0;
        for rel_path in rel_paths {
            let Some((_, name)) = trash::split_entry_rel_path(rel_path) else {
                continue;
            };
            let src_path = trash::trash_path(rel_path);
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

    /// Removes checkpoint directories that no longer hold any entry, so the
    /// trash root keeps one directory per live delete window instead of one per
    /// window ever used. Legacy entries directly under the root are untouched.
    /// Examines at most `limit` trash sources after `start_after` and removes
    /// the empty checkpoint directories among them. Returns the number removed
    /// and the name of the last source examined, or `None` once the root has
    /// been fully walked. Bounding the walk keeps the namespace lock hold short
    /// when the bin holds thousands of drained checkpoints.
    pub fn prune_empty_trash_checkpoints(
        &mut self,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<(u64, Option<String>)> {
        let limit = limit.max(1);
        let Some(root) = self.trash_root()? else {
            return Ok((0, None));
        };
        let Dir(dir) = root.as_ref() else {
            return err_ext!(FsError::invalid_argument(format!(
                "{} exists and is not a directory",
                trash::TRASH_ROOT
            )));
        };

        let mut removed = 0;
        let mut after = start_after.map(str::to_string);
        for _ in 0..limit {
            let Some(source) = Self::next_trash_source(dir, after.as_deref()) else {
                return Ok((removed, None));
            };
            after = Some(source.name.clone());
            if !source.checkpoint {
                continue;
            }
            let path = trash::checkpoint_path(&source.name);
            let inp = InodePath::resolve(self.root_ptr(), &path, &self.store)?;
            let Some(inode) = inp.get_last_inode() else {
                continue;
            };
            if inode.is_dir() && inode.child_len() == 0 {
                self.delete(&inp, false)?;
                removed += 1;
            }
        }
        Ok((removed, after))
    }
}
