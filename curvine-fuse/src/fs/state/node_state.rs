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

use crate::fs::dcache::DirTree;
use crate::fs::state::file_handle::FileHandle;
use crate::fs::state::DirHandle;
use crate::fs::{FuseReader, FuseWriter};
use crate::fuse_metrics::{
    StateStageTimer, CACHE_RESULT_HIT, CACHE_RESULT_MISS, CACHE_RESULT_PUT, CACHE_STATUS,
    STATE_KIND_DIR_HANDLES, STATE_KIND_FILE_HANDLES, STATE_KIND_NODE_MAP, STATE_STAGE_DIR_HANDLES,
    STATE_STAGE_FILE_HANDLES, STATE_STAGE_NODE_MAP,
};
use crate::raw::fuse_abi::{fuse_attr, fuse_forget_one};
use crate::{
    err_fuse, FuseError, FuseMetrics, FuseResult, FuseUtils, FUSE_CURRENT_DIR, FUSE_PARENT_DIR,
    FUSE_ROOT_ID, STATE_FILE_MAGIC, STATE_FILE_VERSION,
};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::{ClientConf, ClusterConf, FuseConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, ListStream, Path, StateReader, StateWriter};
use curvine_common::state::{
    CreateFileOpts, FileAllocOpts, FileStatus, ListOptions, MkdirOpts, OpenFlags, SetAttrOpts,
};
use futures::stream::{self, StreamExt};
use log::{debug, error, info, warn};
use orpc::common::FastHashMap;
use orpc::err_box;
use orpc::sync::{AsyncMutex, AsyncSharedMap, AtomicCounter, RwLockHashMap};
use orpc::sys::RawPtr;
use std::borrow::Cow;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct NodeState {
    dir_tree: RwLock<DirTree>,
    writers: AsyncSharedMap<u64, FuseWriter>,
    handles: RwLockHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
    dir_handles: RwLockHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
    path_locks: Vec<AsyncMutex<()>>,
    fh_creator: AtomicCounter,
    fs: UnifiedFileSystem,
    conf: FuseConf,
    enable_meta_cache: bool,
    meta_cache_ttl: u64,
}

impl NodeState {
    pub fn new(fs: UnifiedFileSystem) -> FuseResult<Self> {
        let conf = fs.conf().fuse.clone();
        let meta_cache_ttl = conf.meta_cache_ttl.as_millis() as u64;
        let enable_meta_cache = conf.enable_meta_cache;

        let state = Self {
            dir_tree: RwLock::new(DirTree::new(conf.clone())),
            writers: AsyncSharedMap::default(),
            handles: RwLockHashMap::default(),
            dir_handles: RwLockHashMap::default(),
            path_locks: (0..conf.path_lock_stripes)
                .map(|_| AsyncMutex::new(()))
                .collect(),
            fh_creator: AtomicCounter::new(0),
            fs,
            conf,
            enable_meta_cache,
            meta_cache_ttl,
        };
        FuseMetrics::with(|m| {
            m.inode_num.set(1);
            m.inode_count.set(1);
        });
        Ok(state)
    }

    pub fn dir_write(&self) -> RwLockWriteGuard<'_, DirTree> {
        self.dir_tree.write().unwrap()
    }

    pub fn dir_read(&self) -> RwLockReadGuard<'_, DirTree> {
        self.dir_tree.read().unwrap()
    }

    pub fn client_conf(&self) -> &ClientConf {
        &self.fs.conf().client
    }

    pub fn cluster_conf(&self) -> &ClusterConf {
        self.fs.conf()
    }

    /// Invalidate the cached entry for `(ino, name)` on a mutation and record the
    /// `user_meta_cache_invalidations_total{cache,reason}` counter. Mirrors the
    /// pre-refactor `invalidate_cache`: a no-op (and no metric) when the metadata
    /// cache is disabled, and the metric is additionally gated on `metrics_enabled`.
    /// `reason` is a static call-site reason; it does not change what is invalidated.
    pub fn invalid_cache(&self, ino: u64, name: Option<&str>, reason: &'static str) {
        if !self.enable_meta_cache {
            return;
        }

        {
            let mut dir = self.dir_write();
            if let Some(inode) = dir.get_inode_mut(ino, name) {
                inode.invalid_cache();
            }
        }

        if self.conf.metrics_enabled {
            // A named entry always has a parent directory; an inode-only target has
            // one unless it is the FUSE root (matches the old `path.parent()` check).
            let has_parent = name.is_some() || ino != FUSE_ROOT_ID;
            FuseMetrics::with(|m| m.record_invalidation(reason, has_parent));
        }
    }

    pub fn update_status(&self, ino: u64, name: Option<&str>, status: &FileStatus) -> bool {
        let mut lock = self.dir_write();
        let inode = match lock.get_inode_mut(ino, name) {
            Some(inode) => inode,
            None => return false,
        };

        let is_changed = inode.mtime != status.mtime || status.len != inode.len;
        inode.update_status(status.clone());

        is_changed
    }

    pub fn keep_cache(&self, ino: u64, status: &FileStatus) -> bool {
        let is_changed = self.update_status(ino, None, status);
        !is_changed
    }

    pub fn get_parent_ino(&self, ino: u64) -> FuseResult<u64> {
        let dir = self.dir_read();
        let inode = dir.get_inode_check(ino, None)?;
        Ok(inode.parent)
    }

    pub fn get_path_common(&self, parent: u64, name: Option<&str>) -> FuseResult<Path> {
        self.dir_read().get_path_common(parent, name)
    }

    pub fn get_path_name(&self, parent: u64, name: &str) -> FuseResult<Path> {
        self.dir_read().get_path_name(parent, name)
    }

    pub fn get_path(&self, ino: u64) -> FuseResult<Path> {
        self.dir_read().get_path(ino)
    }

    pub fn get_path2(
        &self,
        ino1: u64,
        name1: &str,
        ino2: u64,
        name2: &str,
    ) -> FuseResult<(Path, Path)> {
        let dir = self.dir_read();
        let path1 = dir.get_path_name(ino1, name1)?;
        let path2 = dir.get_path_name(ino2, name2)?;
        Ok((path1, path2))
    }

    pub fn next_fh(&self) -> u64 {
        self.fh_creator.next()
    }

    pub fn current_fh(&self) -> u64 {
        self.fh_creator.get()
    }

    pub fn next_ino(&self, status: &FileStatus) -> u64 {
        self.dir_read().next_id(status.id)
    }

    pub async fn lock_path(&self, path: &Path) -> tokio::sync::MutexGuard<'_, ()> {
        let idx = (fxhash::hash32(path.full_path()) as usize) % self.path_locks.len();
        self.path_locks[idx].lock().await
    }

    pub fn lookup_status(
        &self,
        parent: u64,
        name: &str,
        status: &FileStatus,
    ) -> FuseResult<fuse_attr> {
        self.do_lookup(parent, name, status)
    }

    pub fn do_lookup(&self, parent: u64, name: &str, status: &FileStatus) -> FuseResult<fuse_attr> {
        self.do_lookup_recorded(parent, name, status, false, false)
    }

    pub fn do_lookup_recorded(
        &self,
        parent: u64,
        name: &str,
        status: &FileStatus,
        is_real_lookup: bool,
        metrics_enabled: bool,
    ) -> FuseResult<fuse_attr> {
        let record = is_real_lookup && metrics_enabled;

        let mut dir = self.dir_write();
        let cache_hit = record && dir.get_inode(parent, Some(name)).is_some();
        let before = dir.inode_lens();
        let attr = {
            let inode = dir.lookup(parent, name, status.clone())?;
            inode.to_attr(&self.conf)?
        };
        let after = dir.inode_lens();
        drop(dir);

        for _ in 0..after.saturating_sub(before) {
            FuseMetrics::with(|m| {
                Self::inc_gauges_lockstep(&m.inode_num, &m.inode_count);
            });
        }

        if record {
            let result = if cache_hit {
                CACHE_RESULT_HIT
            } else {
                CACHE_RESULT_MISS
            };
            FuseMetrics::with(|m| m.record_node_cache_lookup(result));
        }
        Ok(attr)
    }

    pub fn clear(&self) -> FuseResult<()> {
        let mut dir = self.dir_write();
        dir.clear(|ino| self.has_open_handles(ino));
        Ok(())
    }

    pub async fn lookup_common(&self, parent: u64, name: &str) -> FuseResult<fuse_attr> {
        let status = self.fs_stat(parent, Some(name)).await?;
        self.lookup_status(parent, name, &status)
    }

    pub async fn lookup_link(
        &self,
        parent: u64,
        name: &str,
        link_id: u64,
    ) -> FuseResult<fuse_attr> {
        let status = self.fs_stat(parent, Some(name)).await?;

        let mut dir = self.dir_write();
        let inode = dir.link(link_id, parent, name, status)?;
        inode.to_attr(&self.conf)
    }

    pub fn get_ino(&self, parent: u64, name: Option<&str>) -> Option<u64> {
        if name.is_none() {
            Some(parent)
        } else {
            let dir = self.dir_read();
            let inode = dir.get_inode(parent, name)?;
            Some(inode.ino)
        }
    }

    pub fn inode_exists(&self, ino: u64, name: Option<&str>) -> bool {
        let dir = self.dir_read();
        dir.get_inode(ino, name).is_some()
    }

    pub fn unlink(&self, ino: u64, name: &str, mark_delete: bool) -> FuseResult<()> {
        let before = self.dir_read().inode_lens();
        let mut dir = self.dir_write();
        dir.unlink(ino, name, mark_delete)?;
        let after = dir.inode_lens();
        drop(dir);
        for _ in 0..before.saturating_sub(after) {
            FuseMetrics::with(|m| {
                Self::dec_gauges_lockstep(&m.inode_num, &m.inode_count);
            });
        }
        Ok(())
    }

    pub fn forget(&self, ino: u64, n_lookup: u64) -> FuseResult<()> {
        let before = self.dir_read().inode_lens();
        self.dir_write().forget(ino, n_lookup)?;
        let after = self.dir_read().inode_lens();
        for _ in 0..before.saturating_sub(after) {
            FuseMetrics::with(|m| {
                Self::dec_gauges_lockstep(&m.inode_num, &m.inode_count);
            });
        }
        Ok(())
    }

    pub fn batch_forget(&self, nodes: &[&fuse_forget_one]) -> FuseResult<()> {
        let before = self.dir_read().inode_lens();
        let mut dir = self.dir_write();
        for node in nodes {
            if let Err(e) = dir.forget(node.nodeid, node.nlookup) {
                warn!("batch_forget {:?}: {}", node, e);
            }
        }
        let after = dir.inode_lens();
        drop(dir);
        for _ in 0..before.saturating_sub(after) {
            FuseMetrics::with(|m| {
                Self::dec_gauges_lockstep(&m.inode_num, &m.inode_count);
            });
        }
        Ok(())
    }

    pub fn rename(
        &self,
        old_id: u64,
        old_name: &str,
        new_id: u64,
        new_name: &str,
    ) -> FuseResult<()> {
        self.dir_write().rename(old_id, old_name, new_id, new_name)
    }

    pub async fn find_writer(&self, ino: u64) -> Option<Arc<FuseWriter>> {
        self.writers.get(&ino).await
    }

    // Get or create the writer registered under `ino`. The caller is
    // responsible for resolving the inode first (see `new_handle`), so that the
    // writer-map key and the handle ino are guaranteed to be the same value.
    pub async fn get_or_create_writer(
        &self,
        ino: u64,
        path: &Path,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FuseWriter>> {
        self.writers
            .get_or_create(ino, async {
                let writer = self.fs.open_with_opts(path, opts, flags).await?;
                let writer = FuseWriter::new(&self.conf, self.fs.clone_runtime(), writer);
                Ok(Arc::new(writer))
            })
            .await
    }

    pub async fn new_writer(
        &self,
        path: &Path,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FuseWriter>> {
        let writer = self.fs.open_with_opts(path, opts, flags).await?;
        Ok(Arc::new(FuseWriter::new(
            &self.conf,
            self.fs.clone_runtime(),
            writer,
        )))
    }

    pub async fn new_reader(&self, path: &Path) -> FuseResult<FuseReader> {
        let reader = self.fs.open(path).await?;
        let reader = FuseReader::new(&self.conf, self.fs.clone_runtime(), reader);
        Ok(reader)
    }

    pub async fn flush_writer(&self, ino: u64) -> FuseResult<()> {
        if let Some(existing_writer) = self.find_writer(ino).await {
            existing_writer.flush(None).await?;
        }
        Ok(())
    }

    pub async fn new_handle(
        &self,
        ino: Option<u64>,
        path: &Path,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        let mode = flags.access_mode();

        // Read-only: only a reader, no writer to register in the writers map.
        if mode == OpenFlags::RDONLY {
            let reader = self.new_reader(path).await?;
            let mut status = reader.status().clone();
            let ino = ino.unwrap_or(self.next_ino(&status));
            status.id = ino as i64;
            let handle = self
                .insert_handle_with_writer(ino, Some(RawPtr::from_owned(reader)), None, status)
                .await;
            return Ok(handle);
        }

        if mode != OpenFlags::WRONLY && mode != OpenFlags::RDWR {
            return err_fuse!(libc::EINVAL, "Invalid access mode: {:?}", mode);
        }

        // Write modes (WRONLY / RDWR): resolve the inode ONCE and register the
        // writer under that same ino, so the writer-map key always equals the
        // handle ino. When `ino` is provided (open / restore) we reuse it; when
        // it is None (create) we open the writer first, derive the ino from its
        // status a single time, then insert it under that key. This avoids the
        // previous double `next_ino` call that could diverge for a freshly
        // created file whose backend id was not yet assigned.
        let (ino, writer) = match ino {
            Some(ino) => {
                let writer = self.get_or_create_writer(ino, path, flags, opts).await?;
                (ino, writer)
            }
            None => {
                let writer = self.new_writer(path, flags, opts).await?;
                let ino = self.next_ino(writer.status());
                let writer = self.writers.insert::<FuseError>(ino, writer).await?;
                (ino, writer)
            }
        };

        let reader = if mode == OpenFlags::RDWR {
            if writer.is_ufs() {
                warn!(
                    "ufs {} -> {} does not support read-write mode for file opening, reader will be None",
                    path,
                    writer.path().full_path()
                );
                None
            } else {
                let reader = self.new_reader(path).await?;
                Some(RawPtr::from_owned(reader))
            }
        } else {
            None
        };

        let mut status = writer.status().clone();
        status.id = ino as i64;

        let handle = self
            .insert_handle_with_writer(ino, reader, Some(writer), status)
            .await;

        Ok(handle)
    }

    async fn insert_handle_with_writer(
        &self,
        ino: u64,
        reader: Option<RawPtr<FuseReader>>,
        writer: Option<Arc<FuseWriter>>,
        status: FileStatus,
    ) -> Arc<FileHandle> {
        let handle = Arc::new(FileHandle::new_backend(
            ino,
            self.next_fh(),
            reader,
            writer,
            status,
        ));

        let mut lock = self.handles.write();
        Self::insert_file_handle_locked(&mut lock, handle.ino(), handle.fh(), handle.clone());
        handle
    }

    fn insert_file_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
        ino: u64,
        fh: u64,
        handle: Arc<FileHandle>,
    ) {
        if Self::map_insert_handle(lock, ino, fh, handle) {
            FuseMetrics::with(|m| {
                Self::inc_gauges_lockstep(&m.file_handle_num, &m.file_handle_count)
            });
        }
    }

    fn remove_file_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
        ino: u64,
        fh: u64,
    ) -> Option<Arc<FileHandle>> {
        let (handle, removed) = Self::map_remove_handle(lock, ino, fh);
        if removed {
            FuseMetrics::with(|m| {
                Self::dec_gauges_lockstep(&m.file_handle_num, &m.file_handle_count)
            });
        }
        handle
    }

    fn map_insert_handle<H>(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<H>>>,
        ino: u64,
        fh: u64,
        handle: Arc<H>,
    ) -> bool {
        lock.entry(ino).or_default().insert(fh, handle).is_none()
    }

    fn map_remove_handle<H>(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<H>>>,
        ino: u64,
        fh: u64,
    ) -> (Option<Arc<H>>, bool) {
        if let Some(map) = lock.get_mut(&ino) {
            let handle = map.remove(&fh);
            let removed = handle.is_some();
            if map.is_empty() {
                lock.remove(&ino);
            }
            (handle, removed)
        } else {
            (None, false)
        }
    }

    fn insert_dir_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
        ino: u64,
        fh: u64,
        handle: Arc<DirHandle>,
    ) {
        if Self::map_insert_handle(lock, ino, fh, handle) {
            FuseMetrics::with(|m| {
                Self::inc_gauges_lockstep(&m.dir_handle_num, &m.dir_handle_count)
            });
        }
    }

    fn remove_dir_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
        ino: u64,
        fh: u64,
    ) -> Option<Arc<DirHandle>> {
        let (handle, removed) = Self::map_remove_handle(lock, ino, fh);
        if removed {
            FuseMetrics::with(|m| {
                Self::dec_gauges_lockstep(&m.dir_handle_num, &m.dir_handle_count)
            });
        }
        handle
    }

    pub fn find_handle(&self, ino: u64, fh: u64) -> FuseResult<Arc<FileHandle>> {
        let lock = self.handles.read();
        if let Some(v) = lock.get(&ino) {
            if let Some(handle) = v.get(&fh) {
                return Ok(handle.clone());
            }
        }
        err_fuse!(
            libc::EBADF,
            "node_id {} file_handle {}  not found handle",
            ino,
            fh
        )
    }

    pub fn remove_handle(&self, ino: u64, fh: u64) -> Option<Arc<FileHandle>> {
        let mut lock = self.handles.write();
        Self::remove_file_handle_locked(&mut lock, ino, fh)
    }

    pub async fn release_handle(
        &self,
        ino: u64,
        fh: u64,
    ) -> FuseResult<(Arc<FileHandle>, FuseResult<()>)> {
        // Find the handle without removing it yet.  The handle stays in
        // self.handles while complete()/flush() runs so that
        // has_open_handles() keeps returning true during the commit —
        // otherwise a concurrent unlink or deferred-delete could delete the
        // file before the data upload finishes.
        let handle = self.find_handle(ino, fh)?;

        let close_result: FuseResult<()> = match handle.as_ref() {
            FileHandle::Backend(_) if handle.has_writer() => {
                let cleanup_handle = handle.clone();
                self.writers
                    .release_with_cleanup(handle.ino(), move |last| async move {
                        if last {
                            cleanup_handle.complete(None).await
                        } else {
                            cleanup_handle.flush(None).await
                        }
                    })
                    .await
                    .map(|_| ())
            }

            _ => Ok(()),
        };

        // A failed close keeps both the handle and its shared-writer reference
        // so cleanup state is not silently discarded. FUSE normally sends
        // RELEASE only once; automatic retry of retained state is tracked by
        // #1221.
        if close_result.is_ok() {
            let _ = self.remove_handle(ino, fh);
        }

        Ok((handle, close_result))
    }

    pub fn has_open_handles(&self, ino: u64) -> bool {
        let lock = self.handles.read();
        if let Some(map) = lock.get(&ino) {
            !map.is_empty()
        } else {
            false
        }
    }

    pub fn clear_mark_delete(&self, ino: u64) -> FuseResult<()> {
        self.dir_write().clear_mark_delete(ino)
    }

    pub fn complete_deferred_delete(
        &self,
        ino: u64,
        delete_result: Result<(), FsError>,
    ) -> FuseResult<()> {
        // Keep the mark observable after failure. The background retry needed
        // to reclaim it without another FUSE request is tracked by #1221.
        match delete_result {
            Ok(()) | Err(FsError::FileNotFound(_)) => self.clear_mark_delete(ino),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn fs_unlink(&self, parent: u64, name: &str) -> FuseResult<()> {
        let (ino, path) = {
            let dir = self.dir_read();
            let inode = dir.get_inode_check(parent, Some(name))?;
            (inode.ino, dir.get_path_name(parent, name)?)
        };

        // Always mark for deletion and remove the directory entry first,
        // and check has_open_handles inside the same dir_write critical
        // section.  While we hold dir_write, no concurrent fs_open/fs_create
        // can acquire dir_read, so no new handles can be registered — the
        // has_open_handles result is accurate.
        let has_handles = {
            let mut dir = self.dir_write();
            dir.unlink(parent, name, true)?;
            self.has_open_handles(ino)
        };

        if has_handles {
            debug!("unlink ino={}, path={}: open handles, deferring", ino, path);
            return Ok(());
        }

        // Re-check for a concurrent fs_open that may have registered a handle
        // while we waited.  If so, defer — release() will delete via
        // deferred_delete_ready.
        if self.has_open_handles(ino) {
            debug!(
                "unlink ino={}, path={}: handle appeared, deferring",
                ino, path
            );
            return Ok(());
        }

        match self.fs.delete(&path, false).await {
            Ok(()) => (),
            Err(FsError::FileNotFound(_)) => (),
            Err(e) => {
                self.clear_mark_delete(ino)?;
                return Err(e.into());
            }
        }

        // Clear mark_delete + deleted_child entry now that the server-side
        // delete has completed.  clear_mark_delete handles both in one lock.
        self.clear_mark_delete(ino)?;

        Ok(())
    }

    pub async fn deferred_delete_ready(&self, ino: u64) -> FuseResult<bool> {
        // Early exit if there are open handles or no pending delete.
        if self.has_open_handles(ino) || !self.dir_read().pending_delete(ino) {
            return Ok(false);
        }

        // NOTE: Do NOT clear mark_delete here.  The mark stays set until
        // after self.fs.delete() completes in release().  This ensures
        // that a concurrent fs_open() during the delete RPC sees
        // pending_delete=true and rejects the open, preventing a handle
        // from being registered for a file that is about to be deleted.
        Ok(true)
    }

    pub fn find_dir_handle(&self, ino: u64, fh: u64) -> FuseResult<Arc<DirHandle>> {
        let lock = self.dir_handles.read();
        if let Some(v) = lock.get(&ino) {
            if let Some(handle) = v.get(&fh) {
                return Ok(handle.clone());
            }
        }

        err_fuse!(
            libc::EBADF,
            "node_id {} dir_handle {}  not found dir handle",
            ino,
            fh
        )
    }

    pub fn remove_dir_handle(&self, ino: u64, fh: u64) -> Option<Arc<DirHandle>> {
        let mut lock = self.dir_handles.write();
        Self::remove_dir_handle_locked(&mut lock, ino, fh)
    }

    pub async fn new_dir_handle(&self, ino: u64, path: &Path) -> FuseResult<Arc<DirHandle>> {
        let stream = self.list_stream(path).await?;
        let handle = Arc::new(DirHandle::new(
            ino,
            self.next_fh(),
            path,
            self.conf.list_limit,
            stream,
        ));
        let mut lock = self.dir_handles.write();
        Self::insert_dir_handle_locked(&mut lock, ino, handle.fh, handle.clone());

        Ok(handle)
    }

    pub fn all_handles(&self) -> Vec<Arc<FileHandle>> {
        let lock = self.handles.read();
        lock.values()
            .flat_map(|v| v.values().cloned())
            .collect::<Vec<_>>()
    }

    pub fn all_dir_handles(&self) -> Vec<Arc<DirHandle>> {
        let lock = self.dir_handles.read();
        lock.values()
            .flat_map(|v| v.values().cloned())
            .collect::<Vec<_>>()
    }

    pub fn file_handles_len(&self) -> usize {
        let lock = self.handles.read();
        lock.values().map(|m| m.len()).sum()
    }

    pub fn dir_handles_len(&self) -> usize {
        let lock = self.dir_handles.read();
        lock.values().map(|m| m.len()).sum()
    }

    fn sync_legacy_gauges(&self) {
        FuseMetrics::with(|m| {
            let inode_len = self.dir_read().inode_lens() as i64;
            let file_len = self.file_handles_len() as i64;
            let dir_len = self.dir_handles_len() as i64;
            m.inode_num.set(inode_len);
            m.inode_count.set(inode_len);
            m.file_handle_num.set(file_len);
            m.file_handle_count.set(file_len);
            m.dir_handle_num.set(dir_len);
            m.dir_handle_count.set(dir_len);
        });
    }

    fn inc_gauges_lockstep(legacy: &orpc::common::Gauge, alias: &orpc::common::Gauge) {
        legacy.inc();
        alias.inc();
    }

    fn dec_gauges_lockstep(legacy: &orpc::common::Gauge, alias: &orpc::common::Gauge) {
        legacy.dec();
        alias.dec();
    }

    pub fn writers_len(&self) -> usize {
        self.writers.len()
    }

    pub fn writer_keys(&self) -> Vec<u64> {
        self.writers.keys()
    }

    /// Emit `user_meta_cache_total{cache=status,status}`, gated on the
    /// `metrics_enabled` observation kill-switch (separate from the
    /// `enable_meta_cache` feature gate). No-op when metrics are disabled.
    fn record_status_cache(&self, status: &'static str) {
        if self.conf.metrics_enabled {
            FuseMetrics::with(|m| m.record_user_meta_cache(CACHE_STATUS, status));
        }
    }

    /// `user_meta_cache_total{cache=status,status=put}`, emitted when a backend
    /// status is materialized into the dcache (e.g. readdir populating child
    /// inodes). Only counts when the status/attr cache is actually in use.
    pub fn record_status_put(&self) {
        if self.enable_meta_cache {
            self.record_status_cache(CACHE_RESULT_PUT);
        }
    }

    fn get_cached_status(
        &self,
        ino: u64,
        name: Option<&str>,
        add_lookup: bool,
    ) -> FuseResult<Option<FileStatus>> {
        if !self.enable_meta_cache {
            self.dir_read().check_deleted_child(ino, name)?;
            return Ok(None);
        }

        let status = if add_lookup {
            let mut dir = self.dir_write();
            dir.check_deleted_child(ino, name)?;

            dir.lookup_valid_inode_mut(ino, name, self.meta_cache_ttl)
                .map(|inode| inode.status.clone())
        } else {
            let dir = self.dir_read();
            dir.check_deleted_child(ino, name)?;

            dir.get_valid_inode(ino, name, self.meta_cache_ttl)
                .map(|inode| inode.status.clone())
        };

        // Hit/miss recorded the moment the cache read resolves (before any backend
        // fetch), so a backend error/ENOENT after a miss stays in the denominator.
        self.record_status_cache(if status.is_some() {
            CACHE_RESULT_HIT
        } else {
            CACHE_RESULT_MISS
        });

        Ok(status)
    }

    pub async fn fs_lookup(&self, ino: u64, name: &str) -> FuseResult<fuse_attr> {
        let (ino, cow_name) = if name == FUSE_CURRENT_DIR {
            let dir = self.dir_read();
            let inode = dir.get_inode_check(ino, None)?;
            (inode.parent, Cow::Owned(inode.name.to_owned()))
        } else if name == FUSE_PARENT_DIR {
            let dir = self.dir_read();
            let parent_inode = dir.get_inode_check(ino, None)?;
            let inode = dir.get_inode_check(parent_inode.parent, None)?;
            (inode.parent, Cow::Owned(inode.name.to_owned()))
        } else {
            (ino, Cow::Borrowed(name))
        };

        let name = cow_name.as_ref();

        if let Some(status) = self.get_cached_status(ino, Some(name), true)? {
            if self.conf.metrics_enabled {
                FuseMetrics::with(|m| m.record_node_cache_lookup(CACHE_RESULT_HIT));
            }
            return FuseUtils::status_to_attr(&self.conf, &status);
        }

        let path = {
            let dir = self.dir_read();

            if self.enable_meta_cache && dir.dir_scan_valid(ino, self.meta_cache_ttl) {
                return err_fuse!(libc::ENOENT, "inode {} {} not found", ino, name);
            }

            dir.get_path_name(ino, name)?
        };

        let status = self.fs.get_status(&path).await?;
        // Real FUSE Lookup path: record `node_cache_total{operation=lookup}` via the
        // existing `do_lookup_recorded` hit/miss machinery (is_real_lookup=true).
        let attr = self.do_lookup_recorded(ino, name, &status, true, self.conf.metrics_enabled)?;
        if self.enable_meta_cache {
            self.record_status_cache(CACHE_RESULT_PUT);
        }
        Ok(attr)
    }

    pub async fn update_writer_len(&self, attr: &mut fuse_attr) {
        if let Some(len) = self.get_writer_len(attr.ino).await {
            attr.size = attr.size.max(len)
        }

        if let Some(mtime) = self.get_writer_mtime(attr.ino).await {
            attr.mtime = (mtime.max(0) / 1000) as u64;
            attr.mtimensec = ((mtime.max(0) % 1000) * 1_000_000) as u32;
            attr.ctime = attr.mtime;
            attr.ctimensec = attr.mtimensec;
        }
    }

    pub async fn get_writer_len(&self, ino: u64) -> Option<u64> {
        if let Some(writer) = self.find_writer(ino).await {
            return Some(writer.len() as u64);
        }

        None
    }

    pub async fn get_writer_mtime(&self, ino: u64) -> Option<i64> {
        if let Some(writer) = self.find_writer(ino).await {
            return Some(writer.mtime());
        }

        None
    }

    pub async fn fs_stat(&self, ino: u64, name: Option<&str>) -> FuseResult<FileStatus> {
        if let Some(status) = self.get_cached_status(ino, name, false)? {
            return Ok(status);
        }

        let path = self.get_path_common(ino, name)?;
        let status = self.fs.get_status(&path).await?;

        if self.enable_meta_cache {
            let _ = self.update_status(ino, name, &status);
            self.record_status_cache(CACHE_RESULT_PUT);
        }

        Ok(status)
    }

    pub async fn fs_mkdir(&self, ino: u64, name: &str, opts: MkdirOpts) -> FuseResult<fuse_attr> {
        let path = self.get_path_name(ino, name)?;
        let status = match self.fs.mkdir_with_opts(&path, opts).await? {
            Some(status) => status,
            None => self.fs.get_status(&path).await?,
        };

        self.lookup_status(ino, name, &status)
    }

    pub async fn fs_create(
        &self,
        ino: u64,
        name: &str,
        flags: u32,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        let flags = OpenFlags::new(flags);
        let path = self.get_path_name(ino, name)?;
        let _guard = self.lock_path(&path).await;

        let handle = if flags.read_only() {
            let writer = self
                .new_writer(&path, flags.set_write_only(), opts.clone())
                .await?;
            let mut status = writer.status().clone();
            writer.complete(None).await?;

            let child_ino = self.next_ino(&status);
            status.id = child_ino as i64;
            let _ = self.lookup_status(ino, name, &status)?;
            return self.new_handle(Some(child_ino), &path, flags, opts).await;
        } else {
            self.new_handle(None, &path, flags, opts).await?
        };
        self.lookup_status(ino, name, &handle.status())?;
        Ok(handle)
    }

    pub async fn fs_open(
        &self,
        ino: u64,
        flags: u32,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        let flags = OpenFlags::new(flags);
        let path = self.get_path(ino)?;
        self.new_handle(Some(ino), &path, flags, opts).await
    }

    pub async fn fs_set_attr(&self, ino: u64, opts: SetAttrOpts) -> FuseResult<FileStatus> {
        let path = self.get_path_common(ino, None)?;
        let status = match self.fs.fuse_set_attr(&path, opts).await? {
            Some(status) => status,
            None => self.fs.get_status(&path).await?,
        };
        let _ = self.update_status(ino, None, &status);

        Ok(status)
    }

    pub async fn fs_resize(&self, ino: u64, fh: u64, opts: FileAllocOpts) -> FuseResult<()> {
        opts.validate()?;

        let path = self.get_path(ino)?;
        if fh != 0 {
            let handle = self.find_handle(ino, fh)?;
            handle.resize(opts).await?;
        } else if let Some(writer) = self.find_writer(ino).await {
            // fh-less path resize (truncate(path), fallocate paths) must stay
            // ordered with the inode's active writer; otherwise backend resize
            // bypasses buffered writer state and open fds can observe stale
            // metadata or EIO. This restores the PR #962 behavior after the
            // dcache refactor moved resize handling into NodeState.
            writer.resize(opts).await?;
        } else {
            self.fs.resize(&path, opts).await?;
        }

        Ok(())
    }

    pub async fn fs_rename(
        &self,
        old_id: u64,
        old_name: &str,
        new_id: u64,
        new_name: &str,
    ) -> FuseResult<()> {
        let (old_path, new_path) = self.get_path2(old_id, old_name, new_id, new_name)?;
        self.fs.rename(&old_path, &new_path).await?;
        self.rename(old_id, old_name, new_id, new_name)
    }

    pub async fn fs_fsync(&self, parent: u64, name: Option<&str>) -> FuseResult<()> {
        let Some(ino) = self.dir_read().get_ino(parent, name) else {
            return Ok(());
        };

        self.flush_writer(ino).await?;

        Ok(())
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        let metrics_enabled = self.conf.metrics_enabled;
        if metrics_enabled {
            let node_len = self.dir_read().inode_lens();
            let file_len = self.file_handles_len();
            let dir_len = self.dir_handles_len();
            FuseMetrics::with(|m| {
                m.set_state_handle_count(STATE_KIND_NODE_MAP, node_len);
                m.set_state_handle_count(STATE_KIND_FILE_HANDLES, file_len);
                m.set_state_handle_count(STATE_KIND_DIR_HANDLES, dir_len);
            });
        }

        writer.write_all(STATE_FILE_MAGIC)?;
        writer.write_len(STATE_FILE_VERSION)?;

        {
            let stage = StateStageTimer::start(metrics_enabled, true, STATE_STAGE_NODE_MAP);
            info!("node_state::persist: saving dir_tree");
            let dir = self.dir_read();
            dir.persist(writer)?;
            info!("node_state::persist: {} node saved", dir.inode_lens());
            if let Some(stage) = stage {
                stage.success();
            }
        }

        {
            let stage = StateStageTimer::start(metrics_enabled, true, STATE_STAGE_FILE_HANDLES);
            info!("node_state::persist: saving file_handles");
            let handles = self.all_handles();
            writer.write_len(handles.len() as u64)?;
            for handle in &handles {
                if let Err(e) = handle.persist(writer).await {
                    error!(
                        "node_state::persist: error saving file_handle ino={}, fh={}: {:?}",
                        handle.ino(),
                        handle.fh(),
                        e
                    );
                    return Err(e);
                }
            }
            info!("node_state::persist: {} file_handles saved", handles.len());
            if let Some(stage) = stage {
                stage.success();
            }
        }

        {
            let stage = StateStageTimer::start(metrics_enabled, true, STATE_STAGE_DIR_HANDLES);
            info!("node_state::persist: saving dir_handles");
            let dir_handles = self.all_dir_handles();
            writer.write_len(dir_handles.len() as u64)?;
            for dir_handle in &dir_handles {
                writer.write_struct(&**dir_handle)?;
            }
            info!(
                "node_state::persist: {} dir_handles saved",
                dir_handles.len()
            );
            writer.write_len(self.fh_creator.get())?;
            if let Some(stage) = stage {
                stage.success();
            }
        }

        Ok(())
    }

    pub async fn list_stream(&self, path: &Path) -> FuseResult<ListStream> {
        let inner = self
            .fs
            .list_stream(path, ListOptions::with_limit(self.conf.list_limit))
            .await?;

        let dots = stream::iter([
            Ok(FuseUtils::new_dot_status(FUSE_CURRENT_DIR)),
            Ok(FuseUtils::new_dot_status(FUSE_PARENT_DIR)),
        ]);

        Ok(ListStream::new(dots.chain(inner)))
    }

    pub async fn restore(&self, reader: &mut StateReader) -> FuseResult<()> {
        let metrics_enabled = self.conf.metrics_enabled;

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != STATE_FILE_MAGIC {
            return err_box!(
                "invalid magic: expected {:?}, got {:?}",
                STATE_FILE_MAGIC,
                magic
            );
        }

        let version: u64 = reader.read_len()?;
        if version != STATE_FILE_VERSION {
            return err_box!(
                "unsupported version: expected {}, got {}",
                STATE_FILE_VERSION,
                version
            );
        }

        {
            let stage = StateStageTimer::start(metrics_enabled, false, STATE_STAGE_NODE_MAP);
            info!("node_state::restore: restoring dir_tree");
            let mut dir = self.dir_write();
            dir.restore(reader)?;
            info!(
                "node_state::restore: dir_tree {} restored",
                dir.inode_lens()
            );
            if let Some(stage) = stage {
                stage.success();
            }
        }

        let result: FuseResult<()> = async {
            {
                let stage =
                    StateStageTimer::start(metrics_enabled, false, STATE_STAGE_FILE_HANDLES);
                info!("node_state::restore: restoring file_handles");
                let handles_count = reader.read_len()?;
                for i in 0..handles_count {
                    let handle = match FileHandle::restore(reader, self).await {
                        Ok(handle) => handle,
                        Err(e) => {
                            error!(
                                "failed to restore file_handle {}/{}: {}",
                                i + 1,
                                handles_count,
                                e
                            );
                            return Err(e);
                        }
                    };

                    self.handles
                        .write()
                        .entry(handle.ino())
                        .or_default()
                        .insert(handle.fh(), Arc::new(handle));
                }
                info!(
                    "node_state::restore: {} file_handles restored",
                    handles_count
                );
                if let Some(stage) = stage {
                    stage.success();
                }
            }

            {
                let stage = StateStageTimer::start(metrics_enabled, false, STATE_STAGE_DIR_HANDLES);
                info!("node_state::restore: restoring dir_handles");
                let dir_handles_count = reader.read_len()?;
                for _ in 0..dir_handles_count {
                    let mut handle = reader.read_struct::<DirHandle>()?;
                    let path = Path::from_str(&handle.path)?;
                    let stream = self.list_stream(&path).await?;
                    handle.set_stream(stream);

                    self.dir_handles
                        .write()
                        .entry(handle.ino)
                        .or_default()
                        .insert(handle.fh, Arc::new(handle));
                }
                info!(
                    "node_state::restore: {} dir_handles restored",
                    dir_handles_count
                );

                let fh_creator_value = reader.read_len()?;
                self.fh_creator.set(fh_creator_value);
                if let Some(stage) = stage {
                    stage.success();
                }
            }
            Ok(())
        }
        .await;

        self.sync_legacy_gauges();
        if result.is_ok() {
            info!("node_state::restore: state restore completed successfully");
        }
        result
    }
}

#[cfg(test)]
mod test {
    use crate::fs::state::file_handle::FileHandle;
    use crate::fs::state::{DirHandle, NodeState};
    use crate::fs::FuseWriter;
    use crate::{FuseError, FUSE_ROOT_ID};
    use bytes::Bytes;
    use curvine_client::unified::{UnifiedFileSystem, UnifiedWriter};
    use curvine_common::conf::ClusterConf;
    use curvine_common::error::FsError;
    use curvine_common::fs::local::LocalWriter;
    use curvine_common::fs::{ListStream, Path, StateReader, StateWriter, Writer};
    use curvine_common::state::FileStatus;
    use orpc::common::{FastHashMap, Utils};
    use orpc::runtime::{AsyncRuntime, RpcRuntime};
    use std::sync::Arc;

    fn file_handle(ino: u64, fh: u64) -> Arc<FileHandle> {
        Arc::new(FileHandle::new_backend(
            ino,
            fh,
            None,
            None,
            FileStatus::with_name(ino as i64, "f".to_string(), false),
        ))
    }

    fn dir_handle(ino: u64, fh: u64) -> Arc<DirHandle> {
        let path = Path::from_str("/d").unwrap();
        Arc::new(DirHandle::new(
            ino,
            fh,
            &path,
            16,
            ListStream::new(futures::stream::empty()),
        ))
    }

    #[test]
    fn file_handle_chokepoint_per_fh_invariants() {
        let mut map: FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>> = FastHashMap::default();

        assert!(NodeState::map_insert_handle(
            &mut map,
            1,
            10,
            file_handle(1, 10)
        ));
        assert!(NodeState::map_insert_handle(
            &mut map,
            1,
            11,
            file_handle(1, 11)
        ));
        assert_eq!(map.get(&1).map(|m| m.len()), Some(2));

        assert!(!NodeState::map_insert_handle(
            &mut map,
            1,
            10,
            file_handle(1, 10)
        ));
        assert_eq!(map.get(&1).map(|m| m.len()), Some(2));

        let (removed, did_remove) = NodeState::map_remove_handle(&mut map, 1, 10);
        assert!(removed.is_some() && did_remove);
        assert_eq!(map.get(&1).map(|m| m.len()), Some(1));

        let (_, did_remove) = NodeState::map_remove_handle(&mut map, 1, 11);
        assert!(did_remove);
        assert!(map.get(&1).is_none());

        let (removed, did_remove) = NodeState::map_remove_handle(&mut map, 1, 99);
        assert!(removed.is_none() && !did_remove);
    }

    #[test]
    fn dir_handle_chokepoint_per_fh_invariants() {
        let mut map: FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>> = FastHashMap::default();

        assert!(NodeState::map_insert_handle(
            &mut map,
            2,
            20,
            dir_handle(2, 20)
        ));
        assert!(NodeState::map_insert_handle(
            &mut map,
            2,
            21,
            dir_handle(2, 21)
        ));
        assert_eq!(map.get(&2).map(|m| m.len()), Some(2));

        assert!(NodeState::map_remove_handle(&mut map, 2, 20).1);
        assert!(NodeState::map_remove_handle(&mut map, 2, 21).1);
        assert!(map.get(&2).is_none());
        assert!(!NodeState::map_remove_handle(&mut map, 2, 99).1);
    }

    #[test]
    fn handle_chokepoints_inc_dec_their_own_gauge_only() {
        crate::FuseMetrics::ensure_init().unwrap();
        let mx = crate::FuseMetrics::get();

        let mut file_map: FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>> =
            FastHashMap::default();
        let file_before = mx.file_handle_num.get();
        let dir_before = mx.dir_handle_num.get();

        NodeState::insert_file_handle_locked(&mut file_map, 7, 70, file_handle(7, 70));
        assert_eq!(mx.file_handle_num.get(), file_before + 1);
        assert_eq!(mx.dir_handle_num.get(), dir_before);

        NodeState::remove_file_handle_locked(&mut file_map, 7, 70);
        assert_eq!(mx.file_handle_num.get(), file_before);

        let mut dir_map: FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>> =
            FastHashMap::default();
        let dir_before = mx.dir_handle_num.get();
        let file_before = mx.file_handle_num.get();

        NodeState::insert_dir_handle_locked(&mut dir_map, 8, 80, dir_handle(8, 80));
        assert_eq!(mx.dir_handle_num.get(), dir_before + 1);
        assert_eq!(mx.file_handle_num.get(), file_before);

        NodeState::remove_dir_handle_locked(&mut dir_map, 8, 80);
        assert_eq!(mx.dir_handle_num.get(), dir_before);
    }

    #[test]
    fn inc_dec_gauges_lockstep_move_both_together() {
        let legacy = orpc::common::Metrics::new_gauge(
            "test_lockstep_legacy_gauge_unique",
            "isolated legacy gauge",
        )
        .unwrap();
        let alias = orpc::common::Metrics::new_gauge(
            "test_lockstep_alias_gauge_unique",
            "isolated alias gauge",
        )
        .unwrap();

        NodeState::inc_gauges_lockstep(&legacy, &alias);
        NodeState::inc_gauges_lockstep(&legacy, &alias);
        assert_eq!(legacy.get(), 2);
        assert_eq!(alias.get(), 2);

        NodeState::dec_gauges_lockstep(&legacy, &alias);
        assert_eq!(legacy.get(), 1);
        assert_eq!(alias.get(), 1);
    }

    #[test]
    fn restore_returns_error_on_corrupt_file_handle_record() {
        let rt = Arc::new(AsyncRuntime::single());
        let task_rt = rt.clone();

        rt.block_on(async move {
            crate::FuseMetrics::ensure_init().unwrap();
            let fs = UnifiedFileSystem::with_rt(ClusterConf::default(), task_rt).unwrap();
            let persisted_state = NodeState::new(fs.clone()).unwrap();
            let restored_state = NodeState::new(fs).unwrap();

            let state_path = Utils::temp_file();
            let _ = std::fs::remove_file(&state_path);
            let mut writer = StateWriter::new(&state_path).unwrap();
            writer.write_all(crate::STATE_FILE_MAGIC).unwrap();
            writer.write_len(crate::STATE_FILE_VERSION).unwrap();
            persisted_state.dir_read().persist(&mut writer).unwrap();

            writer.write_len(1).unwrap();
            writer.write_struct(&FileHandle::TYPE_BACKEND).unwrap();
            writer.write_all(b"not-json\n").unwrap();

            // A restore loop that swallows the corrupt handle reads these as
            // dir_handles_count and fh_creator, then incorrectly returns Ok.
            writer.write_len(0).unwrap();
            writer.write_len(0).unwrap();
            writer.flush().unwrap();
            drop(writer);

            let mut reader = StateReader::new(&state_path).unwrap();
            let result = restored_state.restore(&mut reader).await;
            drop(reader);
            let _ = std::fs::remove_file(&state_path);

            assert!(
                result.is_err(),
                "a corrupt file-handle record must fail the whole state restore"
            );
        });
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn persist_returns_error_when_file_handle_persist_fails() {
        let rt = Arc::new(AsyncRuntime::single());
        let task_rt = rt.clone();

        rt.block_on(async move {
            crate::FuseMetrics::ensure_init().unwrap();
            let fs = UnifiedFileSystem::with_rt(ClusterConf::default(), task_rt.clone()).unwrap();
            let state = NodeState::new(fs).unwrap();

            // /dev/full deterministically fails backend writes with ENOSPC. Queue a
            // write before persisting so BackendHandle::persist's complete() sees
            // the worker failure and returns it to NodeState::persist.
            let full_path = Path::from_str("/dev/full").unwrap();
            let local_writer = LocalWriter::new(&full_path, 1).unwrap();
            let status = local_writer.status().clone();
            let fuse_writer = Arc::new(FuseWriter::new(
                &state.conf,
                task_rt,
                UnifiedWriter::Local(local_writer),
            ));
            fuse_writer
                .write(0, Bytes::from_static(b"x"), None)
                .await
                .unwrap();
            state
                .insert_handle_with_writer(1, None, Some(fuse_writer), status)
                .await;

            let state_path = Utils::temp_file();
            let _ = std::fs::remove_file(&state_path);
            let mut writer = StateWriter::new(&state_path).unwrap();
            let result = state.persist(&mut writer).await;
            drop(writer);
            let _ = std::fs::remove_file(&state_path);

            assert!(
                result.is_err(),
                "a file-handle persist failure must fail the whole state persist"
            );
        });
    }

    #[test]
    fn deferred_delete_error_retains_mark_until_success_or_not_found() {
        let rt = Arc::new(AsyncRuntime::single());
        let fs = UnifiedFileSystem::with_rt(ClusterConf::default(), rt).unwrap();
        let state = NodeState::new(fs).unwrap();
        let ino = {
            let mut dir = state.dir_write();
            let status = FileStatus::with_name(123, "pending".to_string(), false);
            let ino = dir.lookup(FUSE_ROOT_ID, "pending", status).unwrap().ino;
            dir.unlink(FUSE_ROOT_ID, "pending", true).unwrap();
            ino
        };

        assert!(state.dir_read().pending_delete(ino));
        assert!(state
            .complete_deferred_delete(ino, Err(FsError::common("delete failed")))
            .is_err());
        assert!(state.dir_read().pending_delete(ino));

        state
            .complete_deferred_delete(ino, Err(FsError::file_not_found("/pending")))
            .unwrap();
        assert!(!state.dir_read().pending_delete(ino));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn release_handle_failure_retains_handle_and_writer_for_retry() {
        let rt = Arc::new(AsyncRuntime::single());
        let task_rt = rt.clone();

        rt.block_on(async move {
            crate::FuseMetrics::ensure_init().unwrap();
            let fs = UnifiedFileSystem::with_rt(ClusterConf::default(), task_rt.clone()).unwrap();
            let state = NodeState::new(fs).unwrap();

            let full_path = Path::from_str("/dev/full").unwrap();
            let local_writer = LocalWriter::new(&full_path, 1).unwrap();
            let status = local_writer.status().clone();
            let fuse_writer = Arc::new(FuseWriter::new(
                &state.conf,
                task_rt,
                UnifiedWriter::Local(local_writer),
            ));
            state
                .writers
                .insert::<FuseError>(1, fuse_writer.clone())
                .await
                .unwrap();
            let handle = state
                .insert_handle_with_writer(1, None, Some(fuse_writer.clone()), status)
                .await;
            fuse_writer
                .write(0, Bytes::from_static(b"x"), None)
                .await
                .unwrap();

            let (_, first_result) = state.release_handle(1, handle.fh()).await.unwrap();
            assert!(first_result.is_err());
            assert!(state.find_handle(1, handle.fh()).is_ok());
            assert!(Arc::ptr_eq(
                &state.find_writer(1).await.unwrap(),
                &fuse_writer
            ));

            let (_, retry_result) = state.release_handle(1, handle.fh()).await.unwrap();
            assert!(retry_result.is_err());
            assert!(state.find_handle(1, handle.fh()).is_ok());
            assert!(Arc::ptr_eq(
                &state.find_writer(1).await.unwrap(),
                &fuse_writer
            ));
        });
    }
}
