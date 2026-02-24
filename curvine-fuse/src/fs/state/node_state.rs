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
use crate::fs::{FuseReader, FuseWriter};
use crate::raw::fuse_abi::{fuse_attr, fuse_forget_one};
use crate::{err_fuse, FuseResult, STATE_FILE_MAGIC, STATE_FILE_VERSION};
use curvine_client::unified::{UnifiedFileSystem, UnifiedReader};
use curvine_common::conf::{ClientConf, ClusterConf, FuseConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, MetaCache, Path, StateReader, StateWriter};
use curvine_common::state::{CreateFileOpts, FileStatus, OpenFlags};
use log::{debug, error, info, warn};
use orpc::common::FastHashMap;
use orpc::err_box;
use orpc::sync::{AtomicCounter, RwLockHashMap};
use orpc::sys::RawPtr;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{Mutex, Notify};

const WRITER_LOCK_STRIPES: usize = 1024;

pub struct NodeState {
    node_map: RwLock<NodeMap>,
    handles: RwLockHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
    dir_handles: RwLockHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
    writer_locks: Vec<Arc<Mutex<()>>>,
    writer_closing_barrier: RwLock<FastHashMap<u64, Arc<Notify>>>,
    fh_creator: AtomicCounter,
    meta_cache: MetaCache,
    fs: UnifiedFileSystem,
    conf: FuseConf,
}

impl NodeState {
    pub fn new(fs: UnifiedFileSystem) -> Self {
        let conf = fs.conf().fuse.clone();
        let node_map = NodeMap::new(&conf);
        let meta_cache = MetaCache::new(conf.meta_cache_capacity, conf.meta_cache_ttl_duration);
        let writer_locks = (0..WRITER_LOCK_STRIPES)
            .map(|_| Arc::new(Mutex::new(())))
            .collect();

        Self {
            node_map: RwLock::new(node_map),
            handles: RwLockHashMap::default(),
            dir_handles: RwLockHashMap::default(),
            writer_locks,
            writer_closing_barrier: RwLock::new(FastHashMap::default()),
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

    /// Update node cache state and return cache validity info.
    ///
    /// Returns (is_first_access, is_changed) where:
    /// - is_first_access: true if this is the first access (cache_valid was false)
    /// - is_changed: true if file mtime or len has changed
    ///
    /// Usage patterns:
    /// 1. For page cache (should_keep_cache):
    ///    - Cache is valid if: is_first_access || !is_changed
    ///    - First access OR unchanged mtime/len → cache is valid
    ///    - We don't use kernel notification (FUSE_NOTIFY_INVAL_INODE) as it causes deadlocks in practice
    ///
    /// 2. For attr cache (should_keep_attr):
    ///    - Cache is valid only when mtime/len is unchanged
    ///    - Any mtime/len change forces attr_valid=0 on this reply
    ///    - This prevents stale st_size/st_mtime after overwrite-heavy workloads
    fn update_cache_state(&self, id: u64, status: &FileStatus) -> FuseResult<(bool, bool)> {
        let mut lock = self.node_write();
        let attr = lock.get_mut_check(id)?;

        let is_first_access = !attr.cache_valid;
        let is_changed = status.mtime != attr.mtime || status.len != attr.len;

        attr.cache_valid = true;
        attr.mtime = status.mtime;
        attr.len = status.len;

        Ok((is_first_access, is_changed))
    }

    pub fn should_keep_cache(&self, id: u64, status: &FileStatus) -> FuseResult<bool> {
        let (is_first_access, is_changed) = self.update_cache_state(id, status)?;
        Ok(is_first_access || !is_changed)
    }

    pub fn should_keep_attr(&self, id: u64, status: &FileStatus) -> FuseResult<bool> {
        let (_, is_changed) = self.update_cache_state(id, status)?;
        Ok(!is_changed)
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

    pub fn lookup_node_id<T: AsRef<str>>(&self, parent: u64, name: Option<T>) -> Option<u64> {
        self.node_read()
            .lookup_node(parent, name.as_ref())
            .map(|node| node.id)
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

    pub fn writer_path_lock(&self, path: &Path) -> Arc<Mutex<()>> {
        let mut hasher = DefaultHasher::new();
        path.full_path().hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.writer_locks.len();
        self.writer_locks[idx].clone()
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
            return Ok(writer);
        }

        let writer = match self.fs.open_with_opts(path, opts.clone(), flags).await {
            Ok(writer) => writer,

            Err(e) if Self::should_retry_open_as_create(flags, &e) => {
                let exists = self.fs.exists(path).await?;
                if !Self::should_retry_open_as_create_with_existence(flags, &e, exists) {
                    return Err(e.into());
                }
                let retry_flags = flags.set_create(true);
                debug!(
                    "retry open with O_CREAT for {} (flags={} -> {}) after {:?}",
                    path,
                    flags.access_mark(),
                    retry_flags.access_mark(),
                    e
                );
                self.fs.open_with_opts(path, opts, retry_flags).await?
            }

            Err(e) => return Err(e.into()),
        };
        let writer = FuseWriter::new(&self.conf, self.fs.clone_runtime(), writer);
        Ok(Arc::new(Mutex::new(writer)))
    }

    fn should_retry_open_as_create(flags: OpenFlags, err: &FsError) -> bool {
        matches!(err, FsError::FileNotFound(_))
            && flags.write_only()
            && (flags.truncate() || flags.append())
            && !flags.create()
    }

    fn should_retry_open_as_create_with_existence(
        flags: OpenFlags,
        err: &FsError,
        exists: bool,
    ) -> bool {
        Self::should_retry_open_as_create(flags, err) && exists
    }

    pub async fn new_reader_with_hint(
        &self,
        path: &Path,
        prefer_cv: bool,
    ) -> FuseResult<FuseReader> {
        let reader = if prefer_cv {
            UnifiedReader::Cv(self.fs.cv().open(path).await?)
        } else {
            self.fs.open(path).await?
        };
        let reader = FuseReader::new(&self.conf, self.fs.clone_runtime(), reader);
        Ok(reader)
    }

    pub async fn new_reader(&self, path: &Path) -> FuseResult<FuseReader> {
        self.new_reader_with_hint(path, false).await
    }

    pub async fn new_handle(
        &self,
        ino: u64,
        path: &Path,
        flags: u32,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        let flags = OpenFlags::new(flags);
        let strict_write_open = flags.write() && (flags.truncate() || flags.create());

        if flags.read() || strict_write_open {
            self.wait_no_open_writer_handles(ino).await;
            self.wait_writer_release_barrier(ino).await;
        }

        if flags.read() || flags.write() {
            loop {
                // Serialize open paths per logical file (striped lock) to provide
                // close->open visibility guarantees while release finalization is in-flight.
                let path_lock = self.writer_path_lock(path);
                let _guard = path_lock.lock().await;
                if (flags.read() || strict_write_open)
                    && (self.has_open_writer_handles(ino) || self.is_writer_closing(ino))
                {
                    drop(_guard);
                    self.wait_no_open_writer_handles(ino).await;
                    self.wait_writer_release_barrier(ino).await;
                    continue;
                }
                return self.new_handle_inner(ino, path, flags, opts).await;
            }
        }
        self.new_handle_inner(ino, path, flags, opts).await
    }

    async fn new_handle_inner(
        &self,
        ino: u64,
        path: &Path,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        // Flush active writer before creating reader to reduce stale reads.
        // If a CV-backed writer is still active, prefer CV reader to preserve
        // close->open visibility while writer finalization is still in-flight.
        let active_writer_prefer_cv = if flags.read() {
            match self.find_writer(&ino) {
                Some(existing_writer) => {
                    let mut lock = existing_writer.lock().await;
                    lock.flush(None).await?;
                    Some(!lock.is_ufs())
                }
                None => None,
            }
        } else {
            None
        };

        let (reader, writer) = match flags.access_mode() {
            mode if mode == OpenFlags::RDONLY => {
                let reader = self
                    .new_reader_with_hint(path, active_writer_prefer_cv.unwrap_or(false))
                    .await?;
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
                    let reader = self.new_reader_with_hint(path, true).await?;
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

        let mut status = if let Some(writer) = &writer {
            let lock = writer.lock().await;
            lock.status().clone()
        } else if let Some(reader) = &reader {
            reader.status().clone()
        } else {
            return err_fuse!(libc::EINVAL, "Invalid flags: {:?}", flags);
        };

        // FileHandle always keeps logical Curvine path for follow-up FUSE operations.
        status.path = path.full_path().to_string();
        status.name = path.name().to_string();

        let mut lock = self.handles.write();

        // Check if writer already exists to prevent duplicate creation.
        let check_writer = if let Some(writer) = writer {
            let exist_writer = Self::find_writer0(&lock, &ino);
            if let Some(exist_writer) = exist_writer {
                Some(exist_writer)
            } else {
                Some(writer)
            }
        } else {
            None
        };

        let handle = Arc::new(FileHandle::new(
            ino,
            self.next_fh(),
            reader,
            check_writer,
            status,
        ));
        lock.entry(handle.ino)
            .or_default()
            .insert(handle.fh, handle.clone());

        Ok(handle)
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
        if let Some(map) = lock.get_mut(&ino) {
            let handle = map.remove(&fh);

            if map.is_empty() {
                lock.remove(&ino);
            }

            handle
        } else {
            None
        }
    }

    pub fn has_open_handles(&self, ino: u64) -> bool {
        let lock = self.handles.read();
        if let Some(map) = lock.get(&ino) {
            !map.is_empty()
        } else {
            false
        }
    }

    pub fn has_open_writer_handles(&self, ino: u64) -> bool {
        self.open_writer_handle_count(ino) > 0
    }

    pub async fn writer_status_snapshot(&self, ino: u64) -> Option<FileStatus> {
        let writer = self.find_writer(&ino)?;
        let lock = writer.lock().await;
        Some(lock.status().clone())
    }

    pub fn handle_status_snapshot(&self, ino: u64) -> Option<FileStatus> {
        let lock = self.handles.read();
        let map = lock.get(&ino)?;
        map.values().next().map(|handle| handle.status.clone())
    }

    pub fn open_writer_handle_count(&self, ino: u64) -> usize {
        let lock = self.handles.read();
        if let Some(map) = lock.get(&ino) {
            map.values().filter(|handle| handle.has_writer()).count()
        } else {
            0
        }
    }

    pub fn has_other_writer_handles(&self, ino: u64, exclude_fh: u64) -> bool {
        let lock = self.handles.read();
        if let Some(map) = lock.get(&ino) {
            map.iter()
                .any(|(fh, handle)| *fh != exclude_fh && handle.has_writer())
        } else {
            false
        }
    }

    pub fn should_delete_now<T: AsRef<str>>(
        &self,
        parent: u64,
        name: Option<T>,
    ) -> FuseResult<bool> {
        let name = name.as_ref();

        let id = {
            let map = self.node_read();
            match map.lookup_node(parent, name) {
                Some(v) => v.id,
                None => return err_fuse!(libc::ENOENT, "node {} not found", parent),
            }
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

    pub fn mark_writer_closing(&self, ino: u64) {
        let mut map = self.writer_closing_barrier.write().unwrap();
        map.entry(ino).or_insert_with(|| Arc::new(Notify::new()));
    }

    pub fn clear_writer_closing(&self, ino: u64) {
        let notify = {
            let mut map = self.writer_closing_barrier.write().unwrap();
            map.remove(&ino)
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    pub fn is_writer_closing(&self, ino: u64) -> bool {
        self.writer_closing_barrier
            .read()
            .unwrap()
            .contains_key(&ino)
    }

    pub async fn wait_writer_release_barrier(&self, ino: u64) {
        let timeout = Duration::from_secs(self.fs.conf().client.close_timeout_secs.max(1));
        let deadline = Instant::now() + timeout;

        loop {
            let notify = {
                self.writer_closing_barrier
                    .read()
                    .unwrap()
                    .get(&ino)
                    .cloned()
            };
            let Some(notify) = notify else {
                return;
            };

            let notified = notify.notified();
            if !self.is_writer_closing(ino) {
                return;
            }

            let now = Instant::now();
            if now >= deadline {
                warn!(
                    "timed out waiting for writer-release barrier, ino={}, timeout={:?}",
                    ino, timeout
                );
                return;
            }

            let wait_timeout = deadline.saturating_duration_since(now);
            if tokio::time::timeout(wait_timeout, notified).await.is_ok() {
                return;
            }
        }
    }

    pub async fn wait_no_open_writer_handles(&self, ino: u64) {
        if !self.has_open_writer_handles(ino) {
            return;
        }

        let timeout = Duration::from_secs(self.fs.conf().client.close_timeout_secs.max(1));
        let start = Instant::now();
        while self.has_open_writer_handles(ino) {
            if start.elapsed() >= timeout {
                warn!(
                    "timed out waiting for open writer handles to close, ino={}, timeout={:?}",
                    ino, timeout
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
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
        if let Some(map) = lock.get_mut(&ino) {
            let handle = map.remove(&fh);

            if map.is_empty() {
                lock.remove(&ino);
            }

            handle
        } else {
            None
        }
    }

    pub async fn new_dir_handle(
        &self,
        ino: u64,
        list: Vec<FileStatus>,
    ) -> FuseResult<Arc<DirHandle>> {
        let handle = Arc::new(DirHandle::new(ino, self.next_fh(), list));
        let mut lock = self.dir_handles.write();
        lock.entry(ino)
            .or_default()
            .insert(handle.fh, handle.clone());

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

    pub async fn restore(&self, reader: &mut StateReader) -> FuseResult<()> {
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
            node_lock.restore(reader)?;
            info!(
                "node_state::restore: node_map {}restored",
                node_lock.nodes_len()
            );
        }

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
            let handle = reader.read_struct::<DirHandle>()?;
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

        info!("node_state::restore: state restore completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::fs::state::NodeState;
    use crate::FUSE_ROOT_ID;
    use curvine_client::unified::UnifiedFileSystem;
    use curvine_common::conf::{ClusterConf, FuseConf};
    use curvine_common::state::FileStatus;
    use orpc::runtime::AsyncRuntime;
    use orpc::CommonResult;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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
}
