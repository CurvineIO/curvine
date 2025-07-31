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

use crate::fs::state::{HandleMap, NodeAttr, NodeMap};
use crate::fs::{CurvineFileSystem, FuseFile};
use crate::raw::fuse_abi::{fuse_attr, fuse_forget_one};
use crate::{err_fuse, FuseResult};
use curvine_common::conf::FuseConf;
use curvine_common::fs::Path;
use curvine_common::state::FileStatus;
use log::warn;
use orpc::sys::RawPtr;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

pub struct NodeState {
    node_map: RwLock<NodeMap>,
    handle_map: HandleMap,
    conf: FuseConf,
}

impl NodeState {
    pub fn new(conf: &FuseConf) -> Self {
        let node_map = NodeMap::new(conf);

        Self {
            node_map: RwLock::new(node_map),
            handle_map: HandleMap::new(),
            conf: conf.clone(),
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

    pub fn add_file(&self, file: FuseFile) -> FuseResult<u64> {
        self.handle_map.add(file)
    }

    pub fn get_file(&self, fh: u64) -> Option<RawPtr<FuseFile>> {
        self.handle_map.get(fh)
    }

    pub fn get_file_check(&self, fh: u64) -> FuseResult<RawPtr<FuseFile>> {
        match self.handle_map.get(fh) {
            None => err_fuse!(libc::EBADF, "FileHandle not initialized, fh {}", fh),
            Some(v) => Ok(v),
        }
    }

    pub fn remove_file(&self, fh: u64) -> Option<RawPtr<FuseFile>> {
        self.handle_map.remove(fh)
    }

    pub fn next_handle(&self) -> u64 {
        self.handle_map.next_id()
    }

    pub fn find_node<T: AsRef<str>>(&self, parent: u64, name: Option<T>) -> FuseResult<NodeAttr> {
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

        if self.conf.auto_cache
            && node.cache_valid
            && (node.mtime.as_millis() as u64 != attr.mtime || attr.size != node.size)
        {
            node.cache_valid = false;
            node.mtime = Duration::from_millis(attr.mtime);
            node.size = attr.size;
        }
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

    pub fn fill_ino(&self, parent: u64, mut list: Vec<FileStatus>) -> FuseResult<Vec<FileStatus>> {
        let mut map = self.node_write();
        for status in list.iter_mut() {
            let attr = map.find_node(parent, Some(&status.name))?;
            status.id = attr.id as i64
        }

        Ok(list)
    }
}

#[cfg(test)]
mod test {
    use crate::fs::state::NodeState;
    use crate::FUSE_ROOT_ID;
    use curvine_common::conf::FuseConf;
    use curvine_common::state::FileStatus;
    use orpc::CommonResult;
    use std::thread;
    use std::time::Duration;

    #[test]
    pub fn path() -> CommonResult<()> {
        let mut conf = FuseConf::default();
        conf.init()?;
        let state = NodeState::new(&conf);

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
        let mut conf = FuseConf {
            node_cache_size: 2,
            node_cache_timeout: "100ms".to_string(),
            ..Default::default()
        };
        conf.init()?;
        println!("{:#?}", conf);

        let state = NodeState::new(&conf);
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
