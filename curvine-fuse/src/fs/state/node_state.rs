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

use crate::fs::state::file_handle::FileHandle;
use crate::fs::state::DirHandle;
use crate::fs::state::{NodeAttr, NodeMap};
use crate::fs::{CurvineFileSystem, FuseReader, FuseWriter};
use crate::raw::fuse_abi::{fuse_attr, fuse_forget_one};
use crate::{
    err_fuse, FuseMetrics, FuseResult, FUSE_CURRENT_DIR, FUSE_PARENT_DIR, STATE_FILE_MAGIC,
    STATE_FILE_VERSION,
};
use curvine_client::file::FsReader;
use curvine_client::unified::{UnifiedFileSystem, UnifiedReader};
use curvine_common::conf::{ClientConf, ClusterConf, FuseConf};
use curvine_common::fs::{FileSystem, ListStream, MetaCache, Path, StateReader, StateWriter};
use curvine_common::state::{CreateFileOpts, FileBlocks, FileStatus, ListOptions, OpenFlags};
use futures::stream::{self, StreamExt};
use log::{debug, error, info, warn};
use orpc::common::FastHashMap;
use orpc::err_box;
use orpc::sync::{AtomicCounter, RwLockHashMap};
use orpc::sys::RawPtr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::Mutex;

pub struct NodeState {
    node_map: RwLock<NodeMap>,
    handles: RwLockHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
    dir_handles: RwLockHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
    fh_creator: AtomicCounter,
    meta_cache: MetaCache,
    fs: UnifiedFileSystem,
    conf: FuseConf,
}

impl NodeState {
    /// Construct a `NodeState`. For the legacy `inode_num`/`*_handle_num` gauges
    /// to be correct, the caller MUST have called `FuseMetrics::ensure_init()`
    /// first (production does this in `CurvineFileSystem::new`, pinned by the
    /// `ensure_init_precedes_node_state` test). If a caller skips `ensure_init`
    /// the gauge updates here and at later mutation sites are silently no-op'd by
    /// `FuseMetrics::with`; the danger is asymmetry — constructing/inserting
    /// before init then removing after a later `ensure_init` would `dec` without
    /// a matching `inc` and drive the gauge negative. Tests that exercise
    /// `NodeState` without caring about the gauges rely on the no-op; tests that
    /// do care must `ensure_init` before the first mutation.
    pub fn new(fs: UnifiedFileSystem) -> Self {
        let conf = fs.conf().fuse.clone();
        let node_map = NodeMap::new(&conf);
        let meta_cache = MetaCache::new(conf.meta_cache_capacity, conf.meta_cache_ttl_duration);

        Self {
            node_map: RwLock::new(node_map),
            handles: RwLockHashMap::default(),
            dir_handles: RwLockHashMap::default(),
            fh_creator: AtomicCounter::new(0),
            meta_cache,
            fs,
            conf,
        }
    }

    pub fn node_write(&self) -> RwLockWriteGuard<'_, NodeMap> {
        self.node_map.write().unwrap()
    }

    pub fn node_read(&self) -> RwLockReadGuard<'_, NodeMap> {
        self.node_map.read().unwrap()
    }

    pub fn meta_cache(&self) -> &MetaCache {
        &self.meta_cache
    }

    pub fn client_conf(&self) -> &ClientConf {
        &self.fs.conf().client
    }

    pub fn cluster_conf(&self) -> &ClusterConf {
        self.fs.conf()
    }

    pub fn current_fh(&self) -> u64 {
        self.fh_creator.get()
    }

    pub fn get_node(&self, id: u64) -> FuseResult<NodeAttr> {
        self.node_read().get_check(id).cloned()
    }

    fn update_cache_state(&self, id: u64, status: &FileStatus) -> bool {
        let mut lock = self.node_write();
        let Some(attr) = lock.get_mut(id) else {
            return false;
        };

        let is_changed = status.mtime != attr.mtime || status.len != attr.len;

        attr.cache_valid = true;
        attr.mtime = status.mtime;
        attr.len = status.len;

        is_changed
    }

    pub fn clear(&self) -> FuseResult<()> {
        self.node_write().clean_cache();
        Ok(())
    }

    pub fn should_keep_cache(&self, id: u64, status: &FileStatus) -> bool {
        let is_changed = self.update_cache_state(id, status);
        !is_changed
    }

    pub async fn update_writer_len(&self, attr: &mut fuse_attr) {
        if let Some(len) = self.get_writer_len(attr.ino).await {
            attr.size = attr.size.max(len)
        }
    }

    pub async fn get_writer_len(&self, ino: u64) -> Option<u64> {
        if let Some(writer) = self.find_writer(&ino) {
            return Some(writer.lock().await.len() as u64);
        }

        None
    }

    pub fn get_path_common<T: AsRef<str>>(&self, parent: u64, name: Option<T>) -> FuseResult<Path> {
        self.node_read().get_path_common(parent, name)
    }

    pub fn get_path_name<T: AsRef<str>>(&self, parent: u64, name: T) -> FuseResult<Path> {
        self.node_read().get_path_name(parent, name)
    }

    pub fn get_path(&self, id: u64) -> FuseResult<Path> {
        self.node_read().get_path(id)
    }

    pub fn get_path2<T: AsRef<str>>(
        &self,
        id1: u64,
        name1: T,
        id2: u64,
        name2: T,
    ) -> FuseResult<(Path, Path)> {
        let map = self.node_read();
        let path1 = map.get_path_name(id1, name1)?;
        let path2 = map.get_path_name(id2, name2)?;
        Ok((path1, path2))
    }

    pub fn get_parent_id(&self, id: u64) -> FuseResult<u64> {
        self.node_read().get_check(id).map(|x| x.parent)
    }

    pub fn next_fh(&self) -> u64 {
        self.fh_creator.next()
    }

    pub fn find_node(&self, parent: u64, name: Option<&str>) -> FuseResult<NodeAttr> {
        self.node_write().find_node(parent, name).map(|x| x.clone())
    }

    // fuse.c do_lookup the equivalent implementation of the function
    // Peer implementation of fuse.c do_lookup function.
    // 1. Execute find_node, and if the node does not exist, create one automatically.Equivalent to an automatically built node cache
    // 2. Update the cache if needed.
    pub fn do_lookup<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
        status: &FileStatus,
    ) -> FuseResult<fuse_attr> {
        self.node_write().do_lookup(parent, name, status)
    }

    pub fn unlink_node<T: AsRef<str>>(&self, id: u64, name: Option<T>) -> FuseResult<()> {
        let mut map = self.node_write();
        let node = if let Some(node) = map.lookup_node_mut(id, name) {
            node.sub_lookup(1);
            node.clone()
        } else {
            return Ok(());
        };

        map.delete_name(&node)
    }

    // Peer-to-peer implementation of fuse.c forget_node
    pub fn forget_node(&self, id: u64, n_lookup: u64) -> FuseResult<()> {
        self.node_write().forget_node(id, n_lookup)
    }

    pub fn batch_forget_node(&self, nodes: &[&fuse_forget_one]) -> FuseResult<()> {
        let mut state = self.node_write();
        for node in nodes {
            if let Err(e) = state.forget_node(node.nodeid, node.nlookup) {
                warn!("batch_forget {:?}: {}", node, e);
            }
        }
        Ok(())
    }

    // fuse.c rename_node
    pub fn rename_node<T: AsRef<str>>(
        &self,
        old_id: u64,
        old_name: T,
        new_id: u64,
        new_name: T,
    ) -> FuseResult<()> {
        self.node_write()
            .rename_node(old_id, old_name, new_id, new_name)
    }

    pub fn find_link_inode(&self, curvine_ino: i64, fuse_ino: u64) -> u64 {
        self.node_write().find_link_inode(curvine_ino, fuse_ino)
    }

    fn find_writer0(
        map: &FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
        ino: &u64,
    ) -> Option<Arc<Mutex<FuseWriter>>> {
        if let Some(h) = map.get(ino) {
            for (_, handle) in h.iter() {
                if let Some(writer) = &handle.writer {
                    return Some(writer.clone());
                }
            }
        }

        None
    }

    pub fn find_writer(&self, ino: &u64) -> Option<Arc<Mutex<FuseWriter>>> {
        let map = self.handles.read();
        Self::find_writer0(&map, ino)
    }

    pub async fn new_writer(
        &self,
        ino: u64,
        path: &Path,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<Mutex<FuseWriter>>> {
        let exists_writer = {
            let lock = self.handles.read();
            Self::find_writer0(&lock, &ino)
        };

        if let Some(writer) = exists_writer {
            if !writer.lock().await.is_completed() {
                return Ok(writer);
            }
        }

        let writer = self.fs.open_with_opts(path, opts, flags).await?;
        let writer = FuseWriter::new(&self.conf, self.fs.clone_runtime(), writer);
        Ok(Arc::new(Mutex::new(writer)))
    }

    fn get_cached_blocks(&self, path: &Path) -> Option<FileBlocks> {
        if self.conf.enable_meta_cache {
            self.meta_cache.get_blocks(path)
        } else {
            None
        }
    }

    pub async fn new_reader(&self, path: &Path) -> FuseResult<FuseReader> {
        let reader = match self.get_cached_blocks(path) {
            Some(blocks) => {
                let reader = FsReader::new(path.clone(), self.fs.fs_context().clone(), blocks)?;
                UnifiedReader::Cv(reader)
            }

            None => {
                let reader = self.fs.open(path).await?;

                if self.conf.enable_meta_cache {
                    match &reader {
                        UnifiedReader::Cv(cv_reader) => {
                            self.meta_cache
                                .put_open(path, cv_reader.file_blocks().clone());
                        }
                        UnifiedReader::Fallback(fallback_reader) => {
                            self.meta_cache
                                .put_open(path, fallback_reader.file_blocks().clone());
                        }
                        _ => {}
                    };
                }

                reader
            }
        };

        let reader = FuseReader::new(&self.conf, self.fs.clone_runtime(), reader);
        Ok(reader)
    }

    pub async fn complete_writer(&self, ino: u64) -> FuseResult<()> {
        if let Some(existing_writer) = self.find_writer(&ino) {
            existing_writer.lock().await.complete(None).await?;
        }
        Ok(())
    }

    pub async fn new_handle(
        &self,
        ino: u64,
        path: &Path,
        flags: u32,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        let flags = OpenFlags::new(flags);

        let (reader, writer) = match flags.access_mode() {
            mode if mode == OpenFlags::RDONLY => {
                let reader = self.new_reader(path).await?;
                (Some(RawPtr::from_owned(reader)), None)
            }

            mode if mode == OpenFlags::WRONLY => {
                let writer = self.new_writer(ino, path, flags, opts).await?;
                (None, Some(writer))
            }

            mode if mode == OpenFlags::RDWR => {
                let writer = self.new_writer(ino, path, flags, opts).await?;
                let (is_ufs, ufs_path) = {
                    let lock = writer.lock().await;
                    (lock.is_ufs(), lock.path().full_path().to_string())
                };
                let reader = if is_ufs {
                    warn!(
                        "ufs {} -> {} does not support read-write mode for file opening, reader will be None",
                        path,
                        ufs_path
                    );
                    None
                } else {
                    let reader = self.new_reader(path).await?;
                    Some(RawPtr::from_owned(reader))
                };

                (reader, Some(writer))
            }
            _ => {
                return err_fuse!(
                    libc::EINVAL,
                    "Invalid access mode: {:?}",
                    flags.access_mode()
                );
            }
        };

        let status = if let Some(writer) = &writer {
            let lock = writer.lock().await;
            lock.status().clone()
        } else if let Some(reader) = &reader {
            reader.status().clone()
        } else {
            return err_fuse!(libc::EINVAL, "Invalid flags: {:?}", flags);
        };

        let handle = self
            .insert_handle_with_writer(ino, reader, writer, status)
            .await;

        Ok(handle)
    }

    async fn insert_handle_with_writer(
        &self,
        ino: u64,
        reader: Option<RawPtr<FuseReader>>,
        writer: Option<Arc<Mutex<FuseWriter>>>,
        status: FileStatus,
    ) -> Arc<FileHandle> {
        let mut candidate_writer = writer;
        let mut ignored_completed: Vec<Arc<Mutex<FuseWriter>>> = vec![];
        let mut reader = Some(reader);
        let mut status = Some(status);

        loop {
            let writer = match candidate_writer.take() {
                Some(writer) => writer,
                None => {
                    let handle = Arc::new(FileHandle::new(
                        ino,
                        self.next_fh(),
                        reader.take().unwrap(),
                        None,
                        status.take().unwrap(),
                    ));
                    let mut lock = self.handles.write();
                    Self::insert_file_handle_locked(
                        &mut lock,
                        handle.ino,
                        handle.fh,
                        handle.clone(),
                    );
                    return handle;
                }
            };

            let exist_writer = self.find_writer_excluding(ino, &ignored_completed);

            let Some(exist_writer) = exist_writer else {
                let handle = Arc::new(FileHandle::new(
                    ino,
                    self.next_fh(),
                    reader.take().unwrap(),
                    Some(writer),
                    status.take().unwrap(),
                ));
                let mut lock = self.handles.write();
                Self::insert_file_handle_locked(&mut lock, handle.ino, handle.fh, handle.clone());
                return handle;
            };

            let exist_guard = exist_writer.lock().await;
            if exist_guard.is_completed() {
                drop(exist_guard);
                ignored_completed.push(exist_writer);
                candidate_writer = Some(writer);
                continue;
            }

            {
                let mut lock = self.handles.write();
                let still_exists = lock.get(&ino).is_some_and(|handles| {
                    handles
                        .values()
                        .filter_map(|handle| handle.writer.as_ref())
                        .any(|writer| Arc::ptr_eq(writer, &exist_writer))
                });
                if still_exists {
                    let handle = Arc::new(FileHandle::new(
                        ino,
                        self.next_fh(),
                        reader.take().unwrap(),
                        Some(exist_writer.clone()),
                        status.take().unwrap(),
                    ));
                    Self::insert_file_handle_locked(
                        &mut lock,
                        handle.ino,
                        handle.fh,
                        handle.clone(),
                    );
                    return handle;
                }
            };

            drop(exist_guard);
            candidate_writer = Some(writer);
        }
    }

    fn find_writer_excluding(
        &self,
        ino: u64,
        ignored_writers: &[Arc<Mutex<FuseWriter>>],
    ) -> Option<Arc<Mutex<FuseWriter>>> {
        let lock = self.handles.read();
        lock.get(&ino).and_then(|handles| {
            handles
                .values()
                .filter_map(|handle| handle.writer.as_ref())
                .find(|writer| {
                    !ignored_writers
                        .iter()
                        .any(|ignored| Arc::ptr_eq(ignored, writer))
                })
                .cloned()
        })
    }

    /// Runtime chokepoint for inserting a file handle while holding the write
    /// lock. Counts by handle (fh), matching `file_handles_len()`'s semantics
    /// (sum of inner-map sizes), and only inc's on a genuinely new fh — a
    /// replace (`is_some()`) does not, in case future code can reopen/reuse an
    /// fh (today `next_fh()` is monotonic so every fh is new). The restore bulk
    /// path does NOT use this — it sets the gauge from the live count once.
    ///
    /// Guard-rail (applies to all four `*_handle_locked` chokepoints): the
    /// `FuseMetrics::with` closure runs with `handles`/`dir_handles` write-locked,
    /// so it MUST stay a single atomic `AtomicI64` inc/dec (design rule 5) — no
    /// `*Vec` label lookup, allocation, or registry traversal under the lock.
    /// In-lock update is intentional: it keeps the gauge atomically consistent
    /// with the map under concurrent FUSE workers.
    fn insert_file_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
        ino: u64,
        fh: u64,
        handle: Arc<FileHandle>,
    ) {
        if Self::map_insert_handle(lock, ino, fh, handle) {
            FuseMetrics::with(|m| m.file_handle_num.inc());
        }
    }

    /// Runtime chokepoint for removing a file handle while holding the write
    /// lock. Dec's only when an fh was actually present; pruning the now-empty
    /// outer per-inode entry must NOT dec again (the count is per-fh, not
    /// per-inode).
    fn remove_file_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
        ino: u64,
        fh: u64,
    ) -> Option<Arc<FileHandle>> {
        let (handle, removed) = Self::map_remove_handle(lock, ino, fh);
        if removed {
            FuseMetrics::with(|m| m.file_handle_num.dec());
        }
        handle
    }

    /// Pure map insert (no gauge side effect): insert `handle` at `ino`/`fh`,
    /// returning `true` iff a genuinely new fh was added (the inc condition). The
    /// gauge-free core of [`Self::insert_file_handle_locked`] / its dir twin, so
    /// the per-fh map invariants can be unit-tested WITHOUT touching the
    /// process-global handle gauges (which would couple parallel tests — see the
    /// chokepoint tests). Generic over the handle type so file/dir share it.
    fn map_insert_handle<H>(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<H>>>,
        ino: u64,
        fh: u64,
        handle: Arc<H>,
    ) -> bool {
        lock.entry(ino).or_default().insert(fh, handle).is_none()
    }

    /// Pure map remove (no gauge side effect): remove `ino`/`fh`, prune the
    /// now-empty outer entry, and return `(removed_handle, did_remove)` where
    /// `did_remove` is the dec condition (an fh was actually present). Pruning
    /// the empty inner map must NOT count as a second removal.
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

    pub fn has_open_handles(&self, ino: u64) -> bool {
        let lock = self.handles.read();
        if let Some(map) = lock.get(&ino) {
            !map.is_empty()
        } else {
            false
        }
    }

    fn find_open_handle_by_path(&self, path: &Path) -> Option<Arc<FileHandle>> {
        let path = path.full_path();
        let lock = self.handles.read();
        lock.values()
            .flat_map(|handles| handles.values())
            .find(|handle| handle.status.path == path)
            .cloned()
    }

    pub fn should_delete_now<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
    ) -> FuseResult<bool> {
        let name = name.as_ref();

        let (id, missing_path) = {
            let map = self.node_read();
            match map.lookup_node(parent, name) {
                Some(v) => (Some(v.id), None),
                None => (None, Some(map.get_path_common(parent, name)?)),
            }
        };

        let Some(id) = id else {
            let path = missing_path.expect("missing path should be set when node id is absent");
            if let Some(handle) = self.find_open_handle_by_path(&path) {
                let mut map = self.node_write();
                map.mark_pending_delete(handle.ino);
                info!(
                    "unlink {}: node cache missing but file has open handles (ino={}), marking for delayed deletion",
                    path, handle.ino
                );
                return Ok(false);
            }

            debug!(
                "unlink node cache miss for path={}; deleting backend path",
                path
            );
            return Ok(true);
        };

        if self.has_open_handles(id) {
            let mut map = self.node_write();
            map.mark_pending_delete(id);
            let path = map.get_path(id)?;
            info!(
                "unlink {}: file has open handles (ino={}), marking for delayed deletion",
                path, id
            );
            Ok(false)
        } else {
            Ok(true)
        }
    }

    pub fn remove_pending_delete(&self, ino: u64) -> bool {
        self.node_write().remove_pending_delete(ino)
    }

    pub fn is_pending_delete(&self, ino: u64) -> bool {
        self.node_read().is_pending_delete(ino)
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

    /// Dir-handle counterpart of [`Self::insert_file_handle_locked`]; same
    /// per-fh inc-on-new-key invariant.
    fn insert_dir_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
        ino: u64,
        fh: u64,
        handle: Arc<DirHandle>,
    ) {
        if Self::map_insert_handle(lock, ino, fh, handle) {
            FuseMetrics::with(|m| m.dir_handle_num.inc());
        }
    }

    /// Dir-handle counterpart of [`Self::remove_file_handle_locked`]; dec only
    /// on a real removal, empty-outer prune does not dec again.
    fn remove_dir_handle_locked(
        lock: &mut FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
        ino: u64,
        fh: u64,
    ) -> Option<Arc<DirHandle>> {
        let (handle, removed) = Self::map_remove_handle(lock, ino, fh);
        if removed {
            FuseMetrics::with(|m| m.dir_handle_num.dec());
        }
        handle
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

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        writer.write_all(STATE_FILE_MAGIC)?;
        writer.write_len(STATE_FILE_VERSION)?;

        {
            info!("node_state::persist: saving node_map");
            let node_lock = self.node_read();
            node_lock.persist(writer)?;
            info!("node_state::persist: {} node saved", node_lock.nodes_len());
        }

        info!("node_state::persist: saving file_handles");
        let handles = self.all_handles();
        writer.write_len(handles.len() as u64)?;
        for handle in &handles {
            if let Err(e) = handle.persist(writer).await {
                error!("node_state::persist: error saving file_handle {:?}", e)
            }
        }
        info!("node_state::persist: {} file_handles saved", handles.len());

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

        Ok(())
    }

    pub async fn list_stream(&self, path: &Path) -> FuseResult<ListStream> {
        let inner = self
            .fs
            .list_stream(path, ListOptions::with_limit(self.conf.list_limit))
            .await?;

        let dots = stream::iter([
            Ok(CurvineFileSystem::new_dot_status(FUSE_CURRENT_DIR)),
            Ok(CurvineFileSystem::new_dot_status(FUSE_PARENT_DIR)),
        ]);

        Ok(ListStream::new(dots.chain(inner)))
    }

    pub async fn restore(&self, reader: &mut StateReader) -> FuseResult<()> {
        // The magic/version early-returns below are gauge-safe ONLY because no
        // map has been mutated yet: restore runs at mount time before any
        // traffic, so the maps are still at cold-start and the gauges already
        // read the correct baseline (inode 1, handles 0). Keep ALL map mutation
        // after the finalizer-guarded sections (NodeMap::restore for inodes, the
        // handle-phase finalizer below); pulling a map write ahead of these
        // checks would skip the finalizer on a magic/version failure and drift
        // the gauge.
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
            info!("node_state::restore: restoring node_map");
            let mut node_lock = self.node_write();
            // inode_num is owned by NodeMap::restore (sets from live nodes.len()
            // on success and on early-?), so a failure here needs no handle
            // finalizer: the handle maps have not been touched this restore and
            // their gauges keep their old (still-correct) live values.
            node_lock.restore(reader)?;
            info!(
                "node_state::restore: node_map {}restored",
                node_lock.nodes_len()
            );
        }

        // Handle-restore phase. The bulk inserts below do NOT go through the
        // event-driven insert helpers (per-insert inc + a final set would churn
        // and complicate the partial-failure value); instead the finalizer after
        // this block sets file/dir_handle_num from the live map counts exactly
        // once. NodeState::restore does NOT clear the handle maps first, so on a
        // re-restore / non-empty state a "restored count" would be wrong — only
        // the live `file_handles_len()`/`dir_handles_len()` are correct.
        //
        // The finalizer must run on success AND on every early-`?` in this phase
        // (dir-handle read/path/list_stream, and the trailing fh_creator read),
        // because any of those can fire after the handle maps were mutated.
        let result: FuseResult<()> = async {
            info!("node_state::restore: restoring file_handles");
            let handles_count = reader.read_len()?;
            let mut restored_handles = 0;
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
                        continue;
                    }
                };

                // Drift-check allowlist: restore bulk insert (file handles).
                // Direct insert on purpose — the finalizer below owns the gauge.
                self.handles
                    .write()
                    .entry(handle.ino)
                    .or_default()
                    .insert(handle.fh, Arc::new(handle));
                restored_handles += 1;
            }
            info!(
                "node_state::restore: {}/{} file_handles restored",
                restored_handles, handles_count
            );

            info!("node_state::restore: restoring dir_handles");
            let dir_handles_count = reader.read_len()?;
            for _ in 0..dir_handles_count {
                let mut handle = reader.read_struct::<DirHandle>()?;
                let path = Path::from_str(&handle.path)?;
                let stream = self.list_stream(&path).await?;
                handle.set_stream(stream);

                // Drift-check allowlist: restore bulk insert (dir handles).
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
            Ok(())
        }
        .await;

        // Finalizer: set both handle gauges from the actual live map counts,
        // covering the Ok path and every early-? above. inode_num is not touched
        // here (NodeMap::restore already owns it).
        FuseMetrics::with(|m| {
            Self::sync_handle_gauges(
                &m.file_handle_num,
                self.file_handles_len(),
                &m.dir_handle_num,
                self.dir_handles_len(),
            )
        });

        if result.is_ok() {
            info!("node_state::restore: state restore completed successfully");
        }
        result
    }

    /// Set the file/dir handle gauges to the live map counts. Extracted as a
    /// `&Gauge` taker so the restore finalizer's "live count -> gauge" mapping is
    /// unit-testable against injected, isolated gauges (the process-global
    /// handle gauges are shared with other tests). Pins that file count goes to
    /// the file gauge and dir count to the dir gauge — a swap here would be a
    /// silent drift that the map-count restore tests cannot catch.
    fn sync_handle_gauges(
        file_gauge: &orpc::common::Gauge,
        file_len: usize,
        dir_gauge: &orpc::common::Gauge,
        dir_len: usize,
    ) {
        file_gauge.set(file_len as i64);
        dir_gauge.set(dir_len as i64);
    }
}

#[cfg(test)]
mod test {
    use crate::fs::state::{DirHandle, FileHandle, NodeState};
    use crate::FUSE_ROOT_ID;
    use curvine_client::unified::UnifiedFileSystem;
    use curvine_common::conf::{ClusterConf, FuseConf};
    use curvine_common::fs::{ListStream, Path};
    use curvine_common::state::FileStatus;
    use orpc::common::FastHashMap;
    use orpc::runtime::AsyncRuntime;
    use orpc::CommonResult;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn file_handle(ino: u64, fh: u64) -> Arc<FileHandle> {
        // reader/writer are None: the map-insertion chokepoints never touch them,
        // so this avoids any backend I/O.
        Arc::new(FileHandle::new(
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

    // The handle gauges (file_handle_num / dir_handle_num) count by fh and are
    // event-driven through the *_handle_locked chokepoints. The per-fh MAP
    // invariants are tested here via the gauge-free `map_insert_handle` /
    // `map_remove_handle` cores — deliberately NOT the `*_handle_locked`
    // wrappers, which would write the process-global gauge and couple these
    // tests to the gauge-delta tests below under parallel execution. The
    // returned bool/Option ARE the inc/dec conditions, so this still pins the
    // branch the gauge keys off. Covered: insert returns true on a new fh (would
    // inc), false on a replace (would NOT inc); remove returns (Some, true) on a
    // real fh (would dec); pruning a now-empty outer per-inode entry does NOT
    // dec again; multiple fhs under the same inode each count.

    #[test]
    fn file_handle_chokepoint_per_fh_invariants() {
        let mut map: FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>> = FastHashMap::default();

        // Two fhs under the same inode -> both new (would inc), both count.
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

        // Reinserting an existing fh -> false (would NOT inc), count unchanged.
        assert!(!NodeState::map_insert_handle(
            &mut map,
            1,
            10,
            file_handle(1, 10)
        ));
        assert_eq!(map.get(&1).map(|m| m.len()), Some(2));

        // Removing one fh leaves the other; outer entry survives, no extra prune.
        let (removed, did_remove) = NodeState::map_remove_handle(&mut map, 1, 10);
        assert!(removed.is_some() && did_remove, "real fh removal would dec");
        assert_eq!(map.get(&1).map(|m| m.len()), Some(1));

        // Removing the last fh prunes the empty outer entry; this is one dec
        // (the fh), NOT an extra dec for the pruned inode bucket.
        let (_, did_remove) = NodeState::map_remove_handle(&mut map, 1, 11);
        assert!(did_remove);
        assert!(
            map.get(&1).is_none(),
            "empty inner map prunes the outer entry"
        );

        // Removing a non-existent fh is a no-op (would NOT dec).
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
        assert!(
            map.get(&2).is_none(),
            "empty inner map prunes the outer entry"
        );
        assert!(!NodeState::map_remove_handle(&mut map, 2, 99).1);
    }

    // The per-fh invariant tests above use the gauge-free map_* cores, so an
    // inverted inc/dec or a copy-pasted wrong gauge field (e.g. file chokepoint
    // touching dir_handle_num) would pass them. These two tests close that gap by
    // reading the real process-global gauge delta around one insert+remove cycle
    // through the `*_handle_locked` wrappers, the same before/after pattern as
    // `meta_task_guard_gate`.
    //
    // Concurrency note: this is the ONLY test in this binary that writes
    // `file_handle_num`/`dir_handle_num` — the invariant tests above were moved off
    // the gauge-writing wrappers onto the gauge-free map_* cores precisely so they
    // no longer pollute these deltas (PR #941 review). Production
    // `new_handle`/`new_dir_handle` need a live backend, so no other test drives
    // them either.
    //
    // The file and dir cases are a SINGLE test (not two) on purpose: each case's
    // cross-non-interference assertion READS the OTHER gauge (the dir case asserts
    // `file_handle_num` is untouched, and vice versa), while the sibling case WRITES
    // it. As two `#[test]`s they raced under the default parallel harness — the dir
    // test could observe the file test's in-flight `+1` and see `file_handle_num !=
    // file_before` (a spurious `left:0 right:1`). Run sequentially in one test, there
    // is no concurrent writer of either gauge, so the cross-gauge reads are stable.
    // We assert deltas (not absolutes) since other tests' `ensure_init` leaves an
    // unknown start. If a future test writes these gauges, switch to an injected
    // gauge or serialize.
    #[test]
    fn handle_chokepoints_inc_dec_their_own_gauge_only() {
        crate::FuseMetrics::ensure_init().unwrap();
        let mx = crate::FuseMetrics::get();

        // --- file handle: inc/dec file_handle_num, must not touch dir_handle_num ---
        let mut file_map: FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>> =
            FastHashMap::default();
        let file_before = mx.file_handle_num.get();
        let dir_before = mx.dir_handle_num.get();

        NodeState::insert_file_handle_locked(&mut file_map, 7, 70, file_handle(7, 70));
        assert_eq!(
            mx.file_handle_num.get(),
            file_before + 1,
            "new fh must inc file_handle_num"
        );
        assert_eq!(
            mx.dir_handle_num.get(),
            dir_before,
            "file handle must NOT touch dir_handle_num"
        );

        NodeState::remove_file_handle_locked(&mut file_map, 7, 70);
        assert_eq!(
            mx.file_handle_num.get(),
            file_before,
            "removing the fh must dec file_handle_num back"
        );

        // --- dir handle: inc/dec dir_handle_num, must not touch file_handle_num ---
        // Re-read baselines: both gauges are back to their pre-file-case values now.
        let mut dir_map: FastHashMap<u64, FastHashMap<u64, Arc<DirHandle>>> =
            FastHashMap::default();
        let dir_before = mx.dir_handle_num.get();
        let file_before = mx.file_handle_num.get();

        NodeState::insert_dir_handle_locked(&mut dir_map, 8, 80, dir_handle(8, 80));
        assert_eq!(
            mx.dir_handle_num.get(),
            dir_before + 1,
            "new fh must inc dir_handle_num"
        );
        assert_eq!(
            mx.file_handle_num.get(),
            file_before,
            "dir handle must NOT touch file_handle_num"
        );

        NodeState::remove_dir_handle_locked(&mut dir_map, 8, 80);
        assert_eq!(
            mx.dir_handle_num.get(),
            dir_before,
            "removing the fh must dec dir_handle_num back"
        );
    }

    // The restore handle finalizer feeds file_handles_len()/dir_handles_len() to
    // the two gauges via `sync_handle_gauges`. The restore tests prove the live
    // counts; this proves the finalizer writes the FILE count into the FILE
    // gauge and the DIR count into the DIR gauge (a copy-paste swap would be a
    // silent drift the map-count tests miss). Injected isolated gauges, so no
    // dependence on global state.
    #[test]
    fn sync_handle_gauges_maps_each_count_to_its_gauge() {
        let file_g = orpc::common::Metrics::new_gauge(
            "test_sync_file_handle_gauge_unique",
            "isolated file gauge",
        )
        .unwrap();
        let dir_g = orpc::common::Metrics::new_gauge(
            "test_sync_dir_handle_gauge_unique",
            "isolated dir gauge",
        )
        .unwrap();
        file_g.set(111);
        dir_g.set(222);

        NodeState::sync_handle_gauges(&file_g, 3, &dir_g, 5);

        assert_eq!(file_g.get(), 3, "file count must land in the file gauge");
        assert_eq!(dir_g.get(), 5, "dir count must land in the dir gauge");
    }

    #[test]
    pub fn path() -> CommonResult<()> {
        let mut conf = ClusterConf::default();
        conf.fuse.init()?;
        let fs = UnifiedFileSystem::with_rt(conf, Arc::new(AsyncRuntime::single()))?;
        let state = NodeState::new(fs);

        let a = state.find_node(FUSE_ROOT_ID, Some("a"))?;
        println!("a = {:?}", a);
        let b = state.find_node(a.id, Some("b"))?;
        println!("b = {:?}", b);

        let path = state.get_path(a.id)?;
        println!("path = {}", path);
        assert_eq!(path.path(), "/a");

        let path = state.get_path(b.id)?;
        println!("path = {}", path);
        assert_eq!(path.path(), "/a/b");

        let path = state.get_path_common(a.id, Some("b"))?;
        println!("path = {}", path);
        assert_eq!(path.path(), "/a/b");
        Ok(())
    }

    #[test]
    pub fn ttl() -> CommonResult<()> {
        let mut conf = ClusterConf {
            fuse: FuseConf {
                node_cache_size: 2,
                node_cache_timeout: "100ms".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        conf.fuse.init()?;

        let fs = UnifiedFileSystem::with_rt(conf, Arc::new(AsyncRuntime::single()))?;
        let state = NodeState::new(fs);
        let status_a = FileStatus::with_name(2, "a".to_string(), true);
        let status_b = FileStatus::with_name(3, "b".to_string(), true);
        let status_c = FileStatus::with_name(4, "c".to_string(), true);
        let a = state.do_lookup(FUSE_ROOT_ID, Some("a"), &status_a)?;
        let b = state.do_lookup(a.ino, Some("b"), &status_b)?;
        let c = state.do_lookup(b.ino, Some("c"), &status_c)?;

        state.forget_node(c.ino, 1)?;
        state.forget_node(b.ino, 1)?;
        thread::sleep(Duration::from_secs(1));

        // Trigger cache cleaning
        let a1 = state.find_node(FUSE_ROOT_ID, Some("a"));
        assert!(a1.is_ok());

        let c1 = state.get_path_common(c.ino, Some("1.log"));
        assert!(c1.is_err());

        Ok(())
    }

    #[test]
    pub fn rename_over_existing_name_keeps_destination_unlinkable() -> CommonResult<()> {
        let mut conf = ClusterConf::default();
        conf.fuse.init()?;
        let fs = UnifiedFileSystem::with_rt(conf, Arc::new(AsyncRuntime::single()))?;
        let state = NodeState::new(fs);

        let old_dst_status = FileStatus::with_name(2, "asymbolic".to_string(), false);
        let _old_dst = state.do_lookup(FUSE_ROOT_ID, Some("asymbolic"), &old_dst_status)?;

        let src_status = FileStatus::with_name(3, "symbolic".to_string(), false);
        let _src = state.do_lookup(FUSE_ROOT_ID, Some("symbolic"), &src_status)?;

        state.rename_node(FUSE_ROOT_ID, "symbolic", FUSE_ROOT_ID, "asymbolic")?;

        assert!(state.should_delete_now(FUSE_ROOT_ID, Some("asymbolic"))?);

        state.unlink_node(FUSE_ROOT_ID, Some("asymbolic"))?;

        // A cache miss must not make unlink fail before the backend delete.
        assert!(state.should_delete_now(FUSE_ROOT_ID, Some("asymbolic"))?);

        Ok(())
    }

    #[test]
    pub fn cache_miss_with_open_handle_keeps_delayed_delete() -> CommonResult<()> {
        let mut conf = ClusterConf::default();
        conf.fuse.init()?;
        let fs = UnifiedFileSystem::with_rt(conf, Arc::new(AsyncRuntime::single()))?;
        let state = NodeState::new(fs);

        let status = FileStatus::with_name(2, "open-file".to_string(), false);
        let node = state.do_lookup(FUSE_ROOT_ID, Some("open-file"), &status)?;
        let path = state.get_path_common(FUSE_ROOT_ID, Some("open-file"))?;

        let mut handle_status = status.clone();
        handle_status.path = path.full_path().to_string();
        let handle = Arc::new(FileHandle::new(
            node.ino,
            state.next_fh(),
            None,
            None,
            handle_status,
        ));
        state
            .handles
            .write()
            .entry(node.ino)
            .or_default()
            .insert(handle.fh, handle);

        state.unlink_node(FUSE_ROOT_ID, Some("open-file"))?;

        assert!(!state.should_delete_now(FUSE_ROOT_ID, Some("open-file"))?);
        assert!(state.is_pending_delete(node.ino));

        Ok(())
    }
}
