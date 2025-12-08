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
use crate::fs::state::{NodeAttr, NodeMap};
use crate::fs::{CurvineFileSystem, FuseReader, FuseWriter};
use crate::raw::fuse_abi::{fuse_attr, fuse_forget_one};
use crate::{err_fuse, FuseResult};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::FuseConf;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{CreateFileOpts, FileStatus, OpenFlags};
use log::{debug, warn};
use orpc::common::FastHashMap;
use orpc::sync::{AtomicCounter, RwLockHashMap};
use orpc::sys::RawPtr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::Mutex;

pub struct NodeState {
    node_map: RwLock<NodeMap>,
    handles: RwLockHashMap<u64, FastHashMap<u64, Arc<FileHandle>>>,
    fh_creator: AtomicCounter,
    fs: UnifiedFileSystem,
    conf: FuseConf,
}

impl NodeState {
    pub fn new(fs: UnifiedFileSystem) -> Self {
        let conf = fs.conf().fuse.clone();
        let node_map = NodeMap::new(&conf);

        Self {
            node_map: RwLock::new(node_map),
            handles: RwLockHashMap::default(),
            fs,
            fh_creator: AtomicCounter::new(0),
            conf,
        }
    }

    pub fn node_write(&self) -> RwLockWriteGuard<'_, NodeMap> {
        self.node_map.write().unwrap()
    }

    pub fn node_read(&self) -> RwLockReadGuard<'_, NodeMap> {
        self.node_map.read().unwrap()
    }

    pub fn get_node(&self, id: u64) -> FuseResult<NodeAttr> {
        self.node_read().get_check(id).cloned()
    }

    pub fn should_keep_cache(&self, id: u64, status: &FileStatus) -> FuseResult<bool> {
        let mut lock = self.node_write();
        let attr = lock.get_mut_check(id)?;

        let is_first_access = !attr.cache_valid;
        let is_changed = status.mtime != attr.mtime || status.len != attr.len;

        attr.cache_valid = true;
        attr.mtime = status.mtime;
        attr.len = status.len;

        Ok(is_first_access || !is_changed)
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
        let mut map = self.node_write();
        let node = match map.find_node(parent, name) {
            Ok(v) => v,
            Err(e) => return err_fuse!(libc::ENOMEM, "{}", e),
        };

        let mut attr = CurvineFileSystem::status_to_attr(&self.conf, status)?;
        attr.ino = node.id;

        Ok(attr)
    }

    // Peer-to-peer implementation of fuse.c forget_node
    pub fn forget_node(&self, id: u64, n_lookup: u64) -> FuseResult<()> {
        self.node_write().unref_node(id, n_lookup)
    }

    pub fn batch_forget_node(&self, nodes: &[fuse_forget_one]) -> FuseResult<()> {
        let mut state = self.node_write();
        for node in nodes {
            if let Err(e) = state.unref_node(node.nodeid, node.nlookup) {
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

    // Register a hard link node with specified inode ID
    pub fn link_node<T: AsRef<str>>(&self, parent: u64, name: T, ino: u64) -> FuseResult<()> {
        self.node_write().link_node(parent, name, ino)
    }

    // Register backend inode mapping
    pub fn register_linked_inode(&self, backend_ino: i64, fuse_ino: u64) {
        self.node_write()
            .register_linked_inode(backend_ino, fuse_ino)
    }

    // Lookup backend inode to find existing FUSE inode (for hard link detection)
    pub fn lookup_link_inode(&self, backend_ino: i64) -> Option<u64> {
        self.node_read().lookup_link_inode(backend_ino)
    }

    // Remove a single name mapping (parent,name) from the node map for unlink
    pub fn unlink_name<T: AsRef<str>>(&self, parent: u64, name: T) {
        self.node_write().remove_name(parent, name);
    }

    pub fn fill_ino(&self, parent: u64, mut list: Vec<FileStatus>) -> FuseResult<Vec<FileStatus>> {
        let mut map = self.node_write();
        for status in list.iter_mut() {
            if status.name == "." || status.name == ".." {
                continue;
            }

            if status.exists_links() {
                let backend_ino = status.id;

                if let Some(link_fuse_ino) = map.lookup_link_inode(backend_ino) {
                    if let Err(e) = map.link_node(parent, &status.name, link_fuse_ino) {
                        debug!(
                            "fill_ino: link_node failed (may already exist): name={}, fuse_ino={}, err={}",
                            status.name, link_fuse_ino, e
                        );
                    }
                    status.id = link_fuse_ino as i64;
                } else {
                    let node_id = {
                        let node = map.find_node(parent, Some(&status.name))?;
                        node.id
                    };
                    map.register_linked_inode(backend_ino, node_id);
                    status.id = node_id as i64;
                    debug!(
                        "fill_ino: first occurrence, register mapping backend_ino={} -> fuse_ino={}, name={}, nlink={}",
                        backend_ino, node_id, status.name, status.nlink
                    );
                }
            } else {
                let node = map.find_node(parent, Some(&status.name))?;
                status.id = node.id as i64;
            }
        }

        Ok(list)
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

        let writer = self.fs.open_with_opts(path, opts, flags).await?;
        let writer = FuseWriter::new(&self.conf, self.fs.clone_runtime(), writer);
        Ok(Arc::new(Mutex::new(writer)))
    }

    pub async fn new_reader(&self, path: &Path) -> FuseResult<FuseReader> {
        let reader = self.fs.open(path).await?;
        let reader = FuseReader::new(&self.conf, self.fs.clone_runtime(), reader);
        Ok(reader)
    }

    pub async fn new_handle(
        &self,
        ino: u64,
        path: &Path,
        flags: u32,
        opts: CreateFileOpts,
    ) -> FuseResult<Arc<FileHandle>> {
        let flags = OpenFlags::new(flags);

        // Before creating reader, flush any active writer to ensure reader gets correct file length
        // This is critical for applications like git clone that read files while they're being written
        if flags.read() {
            if let Some(existing_writer) = self.find_writer(&ino) {
                existing_writer.lock().await.flush(None).await?;
            }
        }

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
                let reader = if let Some((ufs_path, _)) = self.fs.get_mount(path).await? {
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

        let mut lock = self.handles.write();

        // Check if writer already exists to prevent duplicate creation
        let check_writer = if let Some(writer) = writer {
            if let Some(exist_writer) = Self::find_writer0(&lock, &ino) {
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
                Ok(handle.clone())
            } else {
                err_fuse!(libc::EBADF, "Ino {} fh {}  not found handle", ino, fh)
            }
        } else {
            err_fuse!(libc::EBADF, "Ino {} fh {}  not found handle", ino, fh)
        }
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
        let _ = state.do_lookup(b.ino, Some("c"), &status_c)?;

        thread::sleep(Duration::from_secs(1));

        // Trigger cache cleaning
        let a1 = state.find_node(FUSE_ROOT_ID, Some("a"));
        assert!(a1.is_ok());

        let c1 = state.get_path_common(b.ino, Some("c"));
        assert!(c1.is_err());

        Ok(())
    }
}
