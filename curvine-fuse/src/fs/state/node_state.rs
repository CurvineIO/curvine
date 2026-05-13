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
use crate::raw::fuse_abi::{fuse_attr, fuse_forget_one};
use crate::{
    err_fuse, FuseMetrics, FuseResult, FuseUtils, FUSE_CURRENT_DIR, FUSE_PARENT_DIR,
    STATE_FILE_MAGIC, STATE_FILE_VERSION,
};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::{ClientConf, ClusterConf, FuseConf};
use curvine_common::fs::{FileSystem, ListStream, Path, StateReader, StateWriter};
use curvine_common::state::{
    CreateFileOpts, FileAllocOpts, FileStatus, ListOptions, MkdirOpts, OpenFlags, SetAttrOpts,
};
use futures::stream::{self, StreamExt};
use log::{error, info, warn};
use orpc::common::FastHashMap;
use orpc::err_box;
use orpc::sync::{AtomicCounter, RwLockHashMap};
use orpc::sys::RawPtr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct NodeState {
    dir_tree: RwLock<DirTree>,
    handles: RwLockHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
    dir_handles: RwLockHashMap<u64, FastHashMap<u64, Arc<DirHandle>>>,
    fh_creator: AtomicCounter,
    fs: UnifiedFileSystem,
    conf: FuseConf,
    enable_meta_cache: bool,
    meta_cache_ttl: u64,
}

impl NodeState {
    pub fn new(fs: UnifiedFileSystem) -> Self {
        let conf = fs.conf().fuse.clone();
        let enable_meta_cache = conf.enable_meta_cache;
        let meta_cache_ttl = conf.meta_cache_ttl.as_millis() as u64;

        Self {
            dir_tree: RwLock::new(DirTree::new(conf.clone())),
            handles: RwLockHashMap::default(),
            dir_handles: RwLockHashMap::default(),
            fh_creator: AtomicCounter::new(0),
            fs,
            conf,
            enable_meta_cache,
            meta_cache_ttl,
        }
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

    pub fn invalid_cache(&self, ino: u64, name: Option<&str>) {
        if self.enable_meta_cache {
            let mut dir = self.dir_write();
            if let Some(inode) = dir.get_inode_mut(ino, name) {
                inode.invalid_cache();
            };
        }
    }

    /// Update node cache state and return cache validity info.
    ///
    /// Returns (is_first_access, is_changed) where:
    /// - is_first_access: true if this is the first access (cache_valid was false)
    /// - is_changed: true if file mtime or len has changed
    ///
    /// Usage patterns:
    ///  For page cache (should_keep_cache):
    ///   - Cache is valid if: cache_valid || !is_changed
    ///   - First access OR unchanged mtime/len → cache is valid
    ///   - We don't use kernel notification (FUSE_NOTIFY_INVAL_INODE) as it causes deadlocks in practice
    ///
    pub fn update_status(&self, ino: u64, name: Option<&str>, status: &FileStatus) -> (bool, bool) {
        let mut lock = self.dir_write();
        let inode = match lock.get_inode_mut(ino, name) {
            Some(inode) => inode,
            None => return (false, false),
        };

        let is_changed = inode.mtime != status.mtime || status.len != inode.len;
        let cache_valid = inode.cache_valid(self.meta_cache_ttl);
        inode.update_status(status.clone());

        (cache_valid, is_changed)
    }

    pub fn keep_cache(&self, ino: u64, status: &FileStatus) -> bool {
        let (cache_valid, is_changed) = self.update_status(ino, None, status);
        !cache_valid || !is_changed
    }

    pub fn keep_attr(&self, ino: u64) -> bool {
        self.find_writer(ino).is_none()
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

    pub fn lookup_status(
        &self,
        parent: u64,
        name: &str,
        status: FileStatus,
    ) -> FuseResult<fuse_attr> {
        let mut dir = self.dir_write();
        dir.clear(|ino| self.has_open_handles(ino));

        let inode = dir.lookup(parent, name, status)?;
        inode.to_attr(&self.conf)
    }

    pub async fn lookup_common(&self, parent: u64, name: &str) -> FuseResult<fuse_attr> {
        let status = self.fs_stat(parent, Some(name)).await?;
        self.lookup_status(parent, name, status)
    }

    pub async fn lookup_link(
        &self,
        parent: u64,
        name: &str,
        link_id: u64,
    ) -> FuseResult<fuse_attr> {
        let mut status = self.fs_stat(parent, Some(name)).await?;
        status.id = link_id as i64;
        self.lookup_status(parent, name, status)
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

    pub fn unlink(&self, ino: u64, name: &str) -> FuseResult<()> {
        let mut dir = self.dir_write();
        dir.unlink(ino, name)
    }

    pub fn forget(&self, ino: u64, n_lookup: u64) -> FuseResult<()> {
        self.dir_write().forget(ino, n_lookup)
    }

    pub fn batch_forget(&self, nodes: &[&fuse_forget_one]) -> FuseResult<()> {
        let mut dir = self.dir_write();
        for node in nodes {
            if let Err(e) = dir.forget(node.nodeid, node.nlookup) {
                warn!("batch_forget {:?}: {}", node, e);
            }
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

    fn find_writer0(
        map: &FastHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
        ino: u64,
    ) -> Option<Arc<FuseWriter>> {
        if let Some(h) = map.get(&ino) {
            for (_, handle) in h.iter() {
                if let Some(writer) = &handle.writer {
                    return Some(writer.clone());
                }
            }
        }

        None
    }

    pub fn find_writer(&self, ino: u64) -> Option<Arc<FuseWriter>> {
        let map = self.handles.read();
        Self::find_writer0(&map, ino)
    }

    pub async fn new_writer(
        &self,
        path: &Path,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FuseWriter>> {
        let writer = self.fs.open_with_opts(path, opts, flags).await?;
        let writer = FuseWriter::new(&self.conf, self.fs.clone_runtime(), writer);
        Ok(Arc::new(writer))
    }

    pub async fn new_reader(&self, path: &Path) -> FuseResult<FuseReader> {
        let reader = self.fs.open(path).await?;
        let reader = FuseReader::new(&self.conf, self.fs.clone_runtime(), reader);
        Ok(reader)
    }

    pub async fn flush_writer(&self, ino: u64) -> FuseResult<()> {
        if let Some(existing_writer) = self.find_writer(ino) {
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
        // Before creating reader, flush any active writer to ensure reader gets correct file length
        // This is critical for applications like git clone that read files while they're being written
        if let Some(ino) = ino.filter(|_| flags.read()) {
            self.flush_writer(ino).await?;
        }

        let (reader, writer) = match flags.access_mode() {
            mode if mode == OpenFlags::RDONLY => {
                let reader = self.new_reader(path).await?;
                (Some(RawPtr::from_owned(reader)), None)
            }

            mode if mode == OpenFlags::WRONLY => {
                let writer = self.new_writer(path, flags, opts).await?;
                (None, Some(writer))
            }

            mode if mode == OpenFlags::RDWR => {
                let writer = self.new_writer(path, flags, opts).await?;
                let reader = if writer.is_ufs() {
                    warn!(
                        "ufs {} -> {} does not support read-write mode for file opening, reader will be None",
                        path,
                        writer.path().full_path()
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

        let mut status = if let Some(writer) = &writer {
            writer.status().clone()
        } else if let Some(reader) = &reader {
            reader.status().clone()
        } else {
            return err_fuse!(libc::EINVAL, "Invalid flags: {:?}", flags);
        };

        let ino = ino.unwrap_or(self.next_ino(&status));
        status.id = ino as i64;

        let mut lock = self.handles.write();
        // Check if writer already exists to prevent duplicate creation
        let check_writer = if let Some(writer) = writer {
            if let Some(exist_writer) = Self::find_writer0(&lock, ino) {
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

        lock.entry(handle.ino())
            .or_default()
            .insert(handle.fh(), handle.clone());

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

    pub async fn should_delete_server(&self, parent: u64, name: &str) -> FuseResult<bool> {
        let ino = self.dir_read().get_ino_check(parent, Some(name))?;

        if self.has_open_handles(ino) {
            let mut dir = self.dir_write();
            let path = dir.get_path(ino)?;
            info!(
                "unlink {}: open handles, marking for delayed deletion",
                path
            );
            dir.mark_delete(ino)?;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    pub fn deferred_delete_ready(&self, ino: u64) -> bool {
        !self.has_open_handles(ino) && self.dir_read().pending_delete(ino)
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

    pub fn file_handles_len(&self) -> usize {
        let lock = self.handles.read();
        lock.values().map(|m| m.len()).sum()
    }

    pub fn dir_handles_len(&self) -> usize {
        let lock = self.dir_handles.read();
        lock.values().map(|m| m.len()).sum()
    }

    pub fn set_metrics(&self, m: &FuseMetrics) {
        m.inode_num.set(self.dir_read().inode_lens() as i64);
        m.file_handle_num.set(self.file_handles_len() as i64);
        m.dir_handle_num.set(self.dir_handles_len() as i64);
    }

    fn get_cached_status(
        &self,
        ino: u64,
        name: Option<&str>,
        add_lookup: bool,
    ) -> Option<FileStatus> {
        if !self.enable_meta_cache {
            return None;
        }

        if add_lookup {
            self.dir_write()
                .lookup_valid_inode_mut(ino, name, self.meta_cache_ttl)
                .map(|inode| inode.status.clone())
        } else {
            self.dir_read()
                .get_valid_inode(ino, name, self.meta_cache_ttl)
                .map(|inode| inode.status.clone())
        }
    }

    pub async fn fs_lookup(&self, ino: u64, name: &str) -> FuseResult<fuse_attr> {
        if let Some(status) = self.get_cached_status(ino, Some(name), true) {
            return FuseUtils::status_to_attr(&self.conf, &status);
        }

        let path = {
            let dir = self.dir_read();
            // Parent has a full directory snapshot (scan_complete, still within TTL);
            // if the child is absent locally, return ENOENT without hitting the server.
            if self.enable_meta_cache && dir.dir_scan_valid(ino, self.meta_cache_ttl) {
                return err_fuse!(libc::ENOENT, "inode {} {} not found", ino, name);
            }
            dir.get_path_name(ino, name)?
        };

        let status = self.fs.get_status(&path).await?;
        self.lookup_status(ino, name, status)
    }

    pub async fn fs_stat(&self, ino: u64, name: Option<&str>) -> FuseResult<FileStatus> {
        if let Some(status) = self.get_cached_status(ino, name, false) {
            return Ok(status);
        }

        let path = self.get_path_common(ino, name)?;
        let status = self.fs.get_status(&path).await?;

        if self.enable_meta_cache {
            let _ = self.update_status(ino, name, &status);
        }

        Ok(status)
    }

    pub async fn fs_mkdir(&self, ino: u64, name: &str, opts: MkdirOpts) -> FuseResult<fuse_attr> {
        let path = self.get_path_name(ino, name)?;
        let status = match self.fs.mkdir_with_opts(&path, opts).await? {
            Some(status) => status,
            None => self.fs.get_status(&path).await?,
        };

        self.lookup_status(ino, name, status.clone())
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

        let handle = self.new_handle(None, &path, flags, opts).await?;
        self.lookup_status(ino, name, handle.status().clone())?;
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
        } else {
            self.fs.resize(&path, opts).await?;
        }

        Ok(())
    }

    pub async fn fs_unlink(&self, parent: u64, name: &str) -> FuseResult<()> {
        let path = self.get_path_common(parent, Some(name))?;
        if self.should_delete_server(parent, name).await? {
            self.fs.delete(&path, false).await?;
        }
        self.unlink(parent, name)
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        writer.write_all(STATE_FILE_MAGIC)?;
        writer.write_len(STATE_FILE_VERSION)?;

        {
            info!("node_state::persist: saving node_map");
            let dir = self.dir_read();
            dir.persist(writer)?;
            info!("node_state::persist: {} node saved", dir.inode_lens());
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
            Ok(FuseUtils::new_dot_status(FUSE_CURRENT_DIR)),
            Ok(FuseUtils::new_dot_status(FUSE_PARENT_DIR)),
        ]);

        Ok(ListStream::new(dots.chain(inner)))
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
            let mut dir = self.dir_write();
            dir.restore(reader)?;
            info!("node_state::restore: node_map {}restored", dir.inode_lens());
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
                .entry(handle.ino())
                .or_default()
                .insert(handle.fh(), Arc::new(handle));
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
