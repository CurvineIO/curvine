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

use crate::fs::state::NodeAttr;
use crate::{err_fuse, FuseResult, FUSE_PATH_SEPARATOR, FUSE_ROOT_ID, FUSE_UNKNOWN_INO};
use curvine_common::conf::FuseConf;
use curvine_common::fs::Path;
use log::info;
use orpc::common::{FastHashMap, LocalTime};
use orpc::sync::AtomicCounter;
use std::collections::VecDeque;
use std::time::Duration;

// Cache all fuse inode data.
// It is the rust implementation of the node structure in libfuse fuse.c.
pub struct NodeMap {
    nodes: FastHashMap<u64, NodeAttr>,
    names: FastHashMap<String, u64>,
    id_creator: AtomicCounter,
    remember: bool,
    cache_ttl: u64,
    last_clean: u64,
}

impl NodeMap {
    pub fn new(conf: &FuseConf) -> Self {
        let mut nodes = FastHashMap::default();
        nodes.insert(FUSE_ROOT_ID, NodeAttr::new(FUSE_ROOT_ID, Some("/"), 0));
        Self {
            nodes,
            names: FastHashMap::default(),
            remember: conf.remember,
            id_creator: AtomicCounter::new(FUSE_ROOT_ID),
            cache_ttl: conf.node_cache_ttl.as_millis() as u64,
            last_clean: LocalTime::mills(),
        }
    }

    fn name_key<T: AsRef<str>>(id: u64, name: T) -> String {
        format!("{}{}", id, name.as_ref())
    }

    pub fn get(&self, id: u64) -> Option<&NodeAttr> {
        self.nodes.get(&id)
    }

    pub fn get_check(&self, id: u64) -> FuseResult<&NodeAttr> {
        match self.nodes.get(&id) {
            None => err_fuse!(libc::ENOMEM, "inode {} not exists", id),
            Some(v) => Ok(v),
        }
    }

    pub fn get_mut(&mut self, id: u64) -> Option<&mut NodeAttr> {
        self.nodes.get_mut(&id)
    }

    pub fn get_mut_check(&mut self, id: u64) -> FuseResult<&mut NodeAttr> {
        match self.nodes.get_mut(&id) {
            None => err_fuse!(libc::ENOMEM, "inode {} not exists", id),
            Some(v) => Ok(v),
        }
    }

    // fuse.c peer implementation of lookup_node and get_node functions
    // Query an inode
    pub fn lookup_node<T: AsRef<str>>(&self, parent: u64, name: Option<T>) -> Option<&NodeAttr> {
        match name {
            None => self.nodes.get(&parent),

            Some(v) => {
                let key = Self::name_key(parent, v);
                match self.names.get(&key) {
                    None => None,
                    Some(v) => self.nodes.get(v),
                }
            }
        }
    }

    pub fn lookup_node_mut<T: AsRef<str>>(
        &mut self,
        parent: u64,
        name: Option<T>,
    ) -> Option<&mut NodeAttr> {
        match name {
            None => self.nodes.get_mut(&parent),

            Some(v) => {
                let key = Self::name_key(parent, v);
                match self.names.get(&key) {
                    None => None,
                    Some(v) => self.nodes.get_mut(v),
                }
            }
        }
    }

    // fuse.c find_node function peer implementation
    // Query a node, and if the node does not exist, one will be automatically created.
    pub fn find_node<T: AsRef<str>>(
        &mut self,
        parent: u64,
        name: Option<T>,
    ) -> FuseResult<&mut NodeAttr> {
        self.clean_cache();

        let id = match self.lookup_node(parent, name.as_ref()) {
            Some(v) => v.id,
            None => self.add_node(parent, name),
        };

        let node = self.get_mut_check(id)?;
        node.stat_updated = Duration::from_millis(LocalTime::mills());
        Ok(node)
    }

    // Add a new node and return the id of the new node.
    fn add_node<T: AsRef<str>>(&mut self, parent: u64, name: Option<T>) -> u64 {
        let ino = self.next_id();
        let mut node = NodeAttr::new(ino, name, parent);

        if self.remember {
            node.inc_lookup();
        }

        // Update parent references.
        if let Some(p) = self.get_mut(parent) {
            p.ref_ctr += 1;
        }
        node.inc_lookup();

        let key = Self::name_key(node.parent, &node.name);
        self.names.insert(key, ino);
        self.nodes.insert(ino, node);

        ino
    }

    // is a peer implementation of the fuse.h try_get_path function.
    pub fn try_get_path<T: AsRef<str>>(&self, parent: u64, name: Option<T>) -> FuseResult<Path> {
        let mut buf = VecDeque::new();
        if let Some(v) = name.as_ref() {
            buf.push_front(v.as_ref());
        }

        let mut node = self.get_check(parent)?;
        while !node.is_root() {
            buf.push_front(&node.name);
            node = self.get_check(node.parent)?;
        }

        let path = Self::join_path(&buf);
        Ok(Path::from_str(path)?)
    }

    pub fn get_path_common<T: AsRef<str>>(&self, parent: u64, name: Option<T>) -> FuseResult<Path> {
        self.try_get_path(parent, name)
    }

    pub fn get_path(&self, id: u64) -> FuseResult<Path> {
        self.get_path_common::<String>(id, None)
    }

    pub fn get_path_name<T: AsRef<str>>(&self, parent: u64, name: T) -> FuseResult<Path> {
        self.try_get_path(parent, Some(name))
    }

    fn join_path(vec: &VecDeque<&str>) -> String {
        let total_len = vec.iter().map(|x| x.len()).sum::<usize>() + vec.len();
        let mut s = String::with_capacity(total_len);

        s.push_str(FUSE_PATH_SEPARATOR);
        for (index, item) in vec.iter().enumerate() {
            if index != 0 {
                s.push_str(FUSE_PATH_SEPARATOR);
            }
            s.push_str(item.as_ref())
        }
        s
    }

    pub fn unref_node(&mut self, id: u64, n_lookup: u64) -> FuseResult<()> {
        let node = match self.get_mut(id) {
            Some(v) => v,
            None => return Ok(()),
        };

        let name_key = Self::name_key(node.parent, &node.name);
        if node.ref_ctr <= 1 && node.n_lookup <= n_lookup {
            self.nodes.remove(&id);
            self.names.remove(&name_key);
        } else {
            node.n_lookup = node.n_lookup.saturating_sub(n_lookup);
            node.ref_ctr = node.ref_ctr.checked_sub(1).unwrap_or(0);
        }

        Ok(())
    }

    pub fn rename_node<T: AsRef<str>>(
        &mut self,
        old_id: u64,
        old_name: T,
        new_id: u64,
        new_name: T,
    ) -> FuseResult<()> {
        let (old_name, new_name) = (old_name.as_ref(), new_name.as_ref());

        // If destination node exists, remove it first to support replace semantics
        let existing_node = self.lookup_node(new_id, Some(new_name)).cloned();
        if existing_node.is_some() {
            info!(
                "Removing existing destination node for rename: parent={}, name={}",
                new_id, new_name
            );
        }

        let old_node = match self.lookup_node(old_id, Some(old_name)) {
            None => {
                return err_fuse!(
                    libc::ENOENT,
                    "src parent: {}, name: {} not exists",
                    old_id,
                    old_name
                );
            }

            Some(v) => v.clone(),
        };

        // Remove existing destination node if it exists
        if let Some(node) = existing_node {
            self.delete_node(&node);
        }

        // Remove the old node from names mapping but keep the inode
        let old_name_key = Self::name_key(old_node.parent, &old_node.name);
        self.names.remove(&old_name_key);

        // Update the old node with new parent and name, then re-add it
        let mut updated_node = old_node;
        updated_node.parent = new_id;
        updated_node.name = new_name.to_string();

        // Add the updated node back with new path mapping
        let new_name_key = Self::name_key(updated_node.parent, &updated_node.name);
        self.names.insert(new_name_key, updated_node.id);
        self.nodes.insert(updated_node.id, updated_node);

        Ok(())
    }

    // Delete the node.
    fn delete_node(&mut self, node: &NodeAttr) {
        self.names.remove(&Self::name_key(node.parent, &node.name));
        self.nodes.remove(&node.id);
    }

    pub fn clean_cache(&mut self) {
        let now = LocalTime::mills();
        if self.last_clean + self.cache_ttl > now {
            return;
        }

        let expired_nodes: Vec<_> = self
            .nodes
            .iter()
            .filter(|x| {
                !x.1.is_root() && x.1.stat_updated.as_millis() as u64 + self.cache_ttl <= now
            })
            .map(|x| x.1.clone())
            .collect();

        for node in &expired_nodes {
            self.delete_node(node);
        }

        self.last_clean = now;
        info!(
            "Clean node cache, total nodes {}, delete nodes {}, cost {} ms",
            self.nodes.len(),
            expired_nodes.len(),
            LocalTime::mills() - now
        );
    }

    pub fn next_id(&self) -> u64 {
        loop {
            let id = self.id_creator.next();
            if id == FUSE_ROOT_ID || id == FUSE_UNKNOWN_INO || self.nodes.contains_key(&id) {
                continue;
            } else {
                return id;
            }
        }
    }
}
