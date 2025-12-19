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
use log::{debug, error, info};
use orpc::common::{FastHashMap, FastHashSet, LocalTime};
use orpc::sync::AtomicCounter;
use std::collections::VecDeque;
use std::time::Duration;

// Cache all fuse inode data.
// It is the rust implementation of the node structure in libfuse fuse.c.
pub struct NodeMap {
    nodes: FastHashMap<u64, NodeAttr>,
    names: FastHashMap<String, u64>,
    // record curvine inode ID to FUSE inode ID for hard link detection when fuse restart
    linked_inode_map: FastHashMap<i64, u64>,
    pending_deletes: FastHashSet<u64>,
    id_creator: AtomicCounter,
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
            linked_inode_map: FastHashMap::default(),
            pending_deletes: FastHashSet::default(),
            id_creator: AtomicCounter::new(FUSE_ROOT_ID),
            cache_ttl: conf.node_cache_ttl.as_millis() as u64,
            last_clean: LocalTime::mills(),
        }
    }

    fn name_key<T: AsRef<str>>(id: u64, name: T) -> String {
        format!("{}\0{}", id, name.as_ref())
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

        let ino = match self.lookup_node(parent, name.as_ref()) {
            Some(v) => v.id,

            None => {
                let ino = self.next_id();
                let node = NodeAttr::new(ino, name, parent);

                if let Some(p) = self.get_mut(parent) {
                    p.add_ref(1);
                }

                let key = Self::name_key(node.parent, &node.name);
                self.names.insert(key, ino);
                self.nodes.insert(ino, node);

                ino
            }
        };

        let node = self.get_mut_check(ino)?;
        node.add_lookup(1);
        node.stat_updated = Duration::from_millis(LocalTime::mills());

        Ok(node)
    }

    // Add a hard link node with specified inode ID
    pub fn link_node<T: AsRef<str>>(&mut self, parent: u64, name: T, ino: u64) -> FuseResult<()> {
        let name = name.as_ref();
        let key = Self::name_key(parent, name);

        // Check if the name already exists
        if self.names.contains_key(&key) {
            return err_fuse!(
                libc::EEXIST,
                "Name already exists: parent={}, name={}",
                parent,
                name
            );
        }

        // Check if the target inode exists
        if !self.nodes.contains_key(&ino) {
            return err_fuse!(libc::ENOENT, "Target inode {} does not exist", ino);
        }

        // Add the name mapping to the existing inode
        self.names.insert(key, ino);

        // Increment the lookup count of the target node
        if let Some(node) = self.nodes.get_mut(&ino) {
            node.add_lookup(1);
        }

        // Update parent references
        if let Some(p) = self.get_mut(parent) {
            p.ref_ctr += 1;
        }

        debug!(
            "link_node: parent={}, name={}, ino={}, registered hard link in node map",
            parent, name, ino
        );

        Ok(())
    }

    // Register curvine inode mapping when creating a new node
    // This helps detect whether a file exists links after FUSE restart
    pub fn register_linked_inode(&mut self, curvine_ino: i64, fuse_ino: u64) {
        if curvine_ino > 0 {
            self.linked_inode_map.insert(curvine_ino, fuse_ino);
            debug!(
                "register_linked_inode: curvine_ino={}, fuse_ino={}",
                curvine_ino, fuse_ino
            );
        }
    }

    // Check if a curvine inode is already mapped (indicates a hard link)
    // Returns the FUSE inode ID if found
    pub fn lookup_link_inode(&self, curvine_ino: i64) -> Option<u64> {
        self.linked_inode_map.get(&curvine_ino).copied()
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

    pub fn delete_node(&mut self, node: &NodeAttr) -> FuseResult<()> {
        self.delete_name(node)?;
        self.nodes.remove(&node.id);

        Ok(())
    }

    pub fn delete_name(&mut self, node: &NodeAttr) -> FuseResult<()> {
        if let Some(parent) = self.get_mut(node.parent).cloned() {
            self.unref_node(parent.id)?;
        }

        self.names.remove(&Self::name_key(node.parent, &node.name));
        Ok(())
    }

    pub fn unref_node(&mut self, id: u64) -> FuseResult<()> {
        if id == FUSE_ROOT_ID {
            return Ok(());
        }

        let node = match self.get_mut(id) {
            None => return Ok(()),
            Some(v) => {
                v.sub_ref(1);
                v.clone()
            }
        };

        if node.ref_ctr == 0 && node.n_lookup == 0 {
            self.delete_node(&node)?;
        }

        Ok(())
    }

    pub fn forget_node(&mut self, id: u64, n_lookup: u64) -> FuseResult<()> {
        let node = match self.get_mut(id) {
            None => return Ok(()),
            Some(v) => v,
        };

        let cur_lookup = node.sub_lookup(n_lookup);
        if cur_lookup == 0 {
            self.unref_node(id)?;
        };

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

        let old_node = match self.lookup_node_mut(old_id, Some(old_name)) {
            None => return err_fuse!(libc::ENOENT, "inode {} {} not exists", old_id, old_name),

            Some(v) => {
                v.sub_lookup(1);
                v.clone()
            }
        };
        self.delete_name(&old_node)?;

        if let Some(exists_node) = self.lookup_node(new_id, Some(new_name)).cloned() {
            self.delete_name(&exists_node)?;
        }

        let mut new_node = old_node;
        new_node.parent = new_id;
        new_node.name = new_name.to_string();

        if let Some(parent) = self.get_mut(new_id) {
            parent.add_ref(1);
        }
        self.names
            .insert(Self::name_key(new_node.parent, &new_node.name), new_node.id);
        self.nodes.insert(new_node.id, new_node);

        Ok(())
    }

    // Remove a single name mapping (parent,name) without deleting the inode itself.
    // Used for unlink operations where only the directory entry is removed.
    // If the removed name is the primary name in the node, update it to another valid name.
    pub fn remove_name<T: AsRef<str>>(&mut self, parent: u64, name: T) {
        let name_str = name.as_ref();
        let key = Self::name_key(parent, name_str);

        // Get the nodeid before removing
        if let Some(nodeid) = self.names.get(&key).copied() {
            // Remove the name mapping
            self.names.remove(&key);

            info!(
                "remove_name: removed mapping (parent={}, name='{}') -> nodeid={}",
                parent, name_str, nodeid
            );

            // Check if this was the primary name in the node
            let needs_update = self
                .nodes
                .get(&nodeid)
                .map(|node| node.name == name_str && node.parent == parent)
                .unwrap_or(false);

            if needs_update {
                info!(
                    "remove_name: '{}' was primary name for nodeid={}, finding alternative",
                    name_str, nodeid
                );

                // Find another valid name for this nodeid
                let mut new_parent_name = None;
                for (other_key, &other_nid) in self.names.iter() {
                    if other_nid == nodeid {
                        // Parse the key to extract parent and name
                        if let Some((new_parent, new_name)) = Self::parse_name_key(other_key) {
                            new_parent_name = Some((new_parent, new_name));
                            break;
                        }
                    }
                }

                // Update the node with the new parent and name
                if let Some((new_parent, new_name)) = new_parent_name {
                    if let Some(node) = self.nodes.get_mut(&nodeid) {
                        node.parent = new_parent;
                        node.name = new_name.clone();
                        info!("remove_name: updated nodeid={} to use alternative name='{}' in parent={}", 
                              nodeid, new_name, new_parent);
                    }
                } else {
                    info!(
                        "remove_name: no alternative name found for nodeid={}, keeping stale name",
                        nodeid
                    );
                }
            }
        } else {
            info!(
                "remove_name: no mapping found for (parent={}, name='{}')",
                parent, name_str
            );
        }
    }

    // Helper function to parse a name key back into (parent_id, name)
    fn parse_name_key(key: &str) -> Option<(u64, String)> {
        // The key format is "{parent_id}{name}"
        // We need to find where the parent_id ends and name begins

        // Try to parse increasing prefixes as parent_id
        for i in 1..=key.len() {
            if let Ok(parent_id) = key[..i].parse::<u64>() {
                // Check if there's a name part after the parent_id
                if i < key.len() {
                    let name = key[i..].to_string();
                    return Some((parent_id, name));
                }
            }
        }
        None
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
                x.1.should_unref() && x.1.stat_updated.as_millis() as u64 + self.cache_ttl <= now
            })
            .map(|x| x.1.id)
            .collect();

        for node in &expired_nodes {
            if let Err(e) = self.unref_node(*node) {
                error!("clean_cache: unref_node failed: {}", e);
            }
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

    pub fn mark_pending_delete(&mut self, ino: u64) {
        self.pending_deletes.insert(ino);
    }

    pub fn remove_pending_delete(&mut self, ino: u64) -> bool {
        self.pending_deletes.remove(&ino)
    }

    pub fn is_pending_delete(&self, ino: u64) -> bool {
        self.pending_deletes.contains(&ino)
    }
}
