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
use crate::fs::CurvineFileSystem;
use crate::raw::fuse_abi::fuse_attr;
use crate::{
    err_fuse, FuseMetrics, FuseResult, FUSE_PATH_SEPARATOR, FUSE_ROOT_ID, FUSE_UNKNOWN_INO,
};
use curvine_common::conf::FuseConf;
use curvine_common::fs::{Path, StateReader, StateWriter};
use curvine_common::state::FileStatus;
use log::{error, info};
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
    conf: FuseConf,
}

impl NodeMap {
    pub fn new(conf: &FuseConf) -> Self {
        let mut nodes = FastHashMap::default();
        // Root-inode baseline insert. This is on the local `nodes` (before
        // `self` exists), so it is intentionally outside the `self.nodes.*`
        // drift scan and the `insert_node`/`remove_node` chokepoints; the
        // `set(1)` below owns the gauge for construction.
        nodes.insert(FUSE_ROOT_ID, NodeAttr::new(FUSE_ROOT_ID, Some("/"), 0));
        // Event-driven legacy `inode_num` baseline: the map starts with exactly
        // the root inode. `set(1)` (not `inc()`) because this gauge is a
        // process-global singleton: constructing several NodeMaps in tests would
        // accumulate with `inc()`, whereas the gauge tracks the single
        // production mount. Assumes one FUSE mount / one NodeState per process
        // (the CLI builds a single CurvineFileSystem); multi-mount would need a
        // per-state aggregator instead.
        FuseMetrics::with(|m| {
            m.inode_num.set(1);
            m.inode_count.set(1);
        });
        Self {
            nodes,
            names: FastHashMap::default(),
            linked_inode_map: FastHashMap::default(),
            pending_deletes: FastHashSet::default(),
            id_creator: AtomicCounter::new(FUSE_ROOT_ID),
            cache_ttl: conf.node_cache_ttl.as_millis() as u64,
            conf: conf.clone(),
        }
    }

    /// Runtime chokepoint for adding a node to `self.nodes`. ALL runtime inserts
    /// must go through here so the event-driven `inode_num` gauge stays exact;
    /// the only exceptions are the root baseline in `new()` and the restore bulk
    /// path (which sets the gauge from the live count once at the end). Inc only
    /// when a genuinely new key is added — a same-id replace (e.g. `rename_node`
    /// reinserting the same inode) returns `Some` and must NOT inc.
    ///
    /// Both this and `remove_node` run with the `NodeMap` behind a held write
    /// lock, so the `FuseMetrics::with` closure here MUST stay a single atomic
    /// `AtomicI64` inc/dec (design "Performance and Overhead Control" rule 5):
    /// no `*Vec` label lookup, no allocation, no registry traversal under the
    /// lock. Updating in-lock is intentional — it keeps the gauge atomically
    /// consistent with `self.nodes` under concurrent FUSE workers.
    fn insert_node(&mut self, id: u64, node: NodeAttr) -> Option<NodeAttr> {
        let prev = self.nodes.insert(id, node);
        if prev.is_none() {
            FuseMetrics::with(|m| {
                m.inode_num.inc();
                m.inode_count.inc();
            });
        }
        prev
    }

    /// Runtime chokepoint for removing a node from `self.nodes`. Dec only when a
    /// node was actually present — a no-op remove (e.g. the second removal of
    /// the same id along a recursive delete chain) returns `None` and must NOT
    /// dec.
    fn remove_node(&mut self, id: u64) -> Option<NodeAttr> {
        let removed = self.nodes.remove(&id);
        if removed.is_some() {
            FuseMetrics::with(|m| {
                m.inode_num.dec();
                m.inode_count.dec();
            });
        }
        removed
    }

    pub fn current_id(&self) -> u64 {
        self.id_creator.get()
    }

    fn name_key<T: AsRef<str>>(id: u64, name: T) -> String {
        format!("{}\0{}", id, name.as_ref())
    }

    pub fn nodes_len(&self) -> usize {
        self.nodes.len()
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

    pub fn do_lookup<T: AsRef<str>>(
        &mut self,
        parent: u64,
        name: Option<T>,
        status: &FileStatus,
    ) -> FuseResult<fuse_attr> {
        self.do_lookup_probed(parent, name, status, false).0
    }

    /// `do_lookup` plus an optional NodeMap-dcache hit/miss probe for
    /// `node_cache_total` (Phase 3a). When `record && name.is_some()`, the second
    /// tuple element is `Some(hit)` where `hit` reflects whether `(parent,name)`
    /// already mapped to an inode **before** `find_node` auto-creates one — decided
    /// here, inside the write lock, so it is atomic with the lookup (not a racy
    /// pre-probe). The probe is returned (not emitted) so the caller can record the
    /// metric AFTER releasing the lock, and it is returned on every path (incl. the
    /// pending-delete/error early returns) so a hit/miss is never dropped. `record`
    /// is `true` only on the real FUSE `Lookup` path; readdir / mkdir / create /
    /// link / symlink pass `false`, and `name=None` (`.`/`..`) yields `None`.
    pub fn do_lookup_probed<T: AsRef<str>>(
        &mut self,
        parent: u64,
        name: Option<T>,
        status: &FileStatus,
        record: bool,
    ) -> (FuseResult<fuse_attr>, Option<bool>) {
        let node_cache_hit = if record {
            name.as_ref()
                .map(|n| self.lookup_node(parent, Some(n.as_ref())).is_some())
        } else {
            None
        };
        (self.do_lookup_inner(parent, name, status), node_cache_hit)
    }

    fn do_lookup_inner<T: AsRef<str>>(
        &mut self,
        parent: u64,
        name: Option<T>,
        status: &FileStatus,
    ) -> FuseResult<fuse_attr> {
        let ino = match self.find_node(parent, name) {
            Ok(v) => v.id,
            Err(e) => return err_fuse!(libc::ENOMEM, "{}", e),
        };

        let mut attr = CurvineFileSystem::status_to_attr(&self.conf, status)?;
        attr.ino = if status.exists_links() {
            self.find_link_inode(status.id, ino)
        } else {
            ino
        };

        if self.is_pending_delete(attr.ino) {
            if let Some(inode) = self.get_mut(attr.ino) {
                inode.sub_lookup(1);
            }
            return err_fuse!(
                libc::ENOENT,
                "inode {} marked for deletion, suppress lookup revive",
                attr.ino
            );
        }

        Ok(attr)
    }

    // fuse.c find_node function peer implementation
    // Query a node, and if the node does not exist, one will be automatically created.
    pub fn find_node<T: AsRef<str>>(
        &mut self,
        parent: u64,
        name: Option<T>,
    ) -> FuseResult<&mut NodeAttr> {
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
                self.insert_node(ino, node);

                ino
            }
        };

        let node = self.get_mut_check(ino)?;
        node.add_lookup(1);
        node.stat_updated = Duration::from_millis(LocalTime::mills());

        Ok(node)
    }

    // Check if a curvine inode is already mapped (indicates a hard link)
    // Returns the FUSE inode ID if found
    pub fn find_link_inode(&mut self, curvine_ino: i64, fuse_ino: u64) -> u64 {
        *self.linked_inode_map.entry(curvine_ino).or_insert(fuse_ino)
    }

    fn convert_to_cv_path(&self, fuse_path: &str) -> FuseResult<Path> {
        let fs_path = &self.conf.fs_path;

        if fs_path == "/" {
            return Ok(Path::from_str(fuse_path)?);
        }

        if fuse_path == "/" {
            return Ok(Path::from_str(fs_path)?);
        }

        let fuse_path_without_slash = fuse_path.strip_prefix('/').unwrap_or(fuse_path);
        let cv_path_str = if fs_path.ends_with('/') {
            format!("{}{}", fs_path, fuse_path_without_slash)
        } else {
            format!("{}/{}", fs_path, fuse_path_without_slash)
        };

        Ok(Path::from_str(cv_path_str)?)
    }

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

        let fuse_path = Self::join_path(&buf);
        self.convert_to_cv_path(&fuse_path)
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
        self.remove_node(node.id);

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

        let old_node = match self.lookup_node(old_id, Some(old_name)) {
            None => return err_fuse!(libc::ENOENT, "inode {} {} not exists", old_id, old_name),
            Some(v) => v.clone(),
        };
        self.delete_name(&old_node)?;

        if let Some(exists_node) = self.lookup_node(new_id, Some(new_name)).cloned() {
            self.delete_name(&exists_node)?;
            if exists_node.should_unref() {
                // Go through the remove_node chokepoint (not a bare
                // `self.nodes.remove`) so both the legacy `inode_num` and the
                // namespaced `inode_count` gauges decrement with the live map.
                // The name mapping was already cleared by `delete_name` above,
                // so we must NOT call `delete_node` here (that would re-run
                // delete_name / parent-ref changes) — only the inode removal.
                self.remove_node(exists_node.id);
            }
        }

        let mut new_node = old_node;
        new_node.parent = new_id;
        new_node.name = new_name.to_string();

        if let Some(parent) = self.get_mut(new_id) {
            parent.add_ref(1);
        }
        self.names
            .insert(Self::name_key(new_node.parent, &new_node.name), new_node.id);
        // Same-id reinsert (rename keeps the inode, only changes parent/name):
        // `insert_node` sees `prev.is_some()` and does NOT inc inode_num.
        self.insert_node(new_node.id, new_node);

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

    /// Create snapshot of NodeMap state (stream-based, memory-efficient)
    /// Serializes: nodes, names, linked_inode_map, pending_deletes, id_creator
    pub fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        writer.write_len(self.id_creator.get())?;

        writer.write_len(self.nodes.len() as u64)?;
        for item in self.nodes.iter() {
            writer.write_struct(&item)?;
        }

        writer.write_len(self.names.len() as u64)?;
        for item in self.names.iter() {
            writer.write_struct(&item)?;
        }

        writer.write_struct(&self.linked_inode_map)?;
        writer.write_struct(&self.pending_deletes)?;

        writer.flush()?;
        Ok(())
    }

    /// Restore NodeMap state from snapshot (stream-based, memory-efficient)
    /// Deserializes: nodes, names, linked_inode_map, pending_deletes, id_creator
    pub fn restore(&mut self, reader: &mut StateReader) -> FuseResult<()> {
        // Bulk reload, NOT routed through the insert_node/remove_node
        // chokepoints: per-node inc/dec would be O(n) churn on the gauge and the
        // partial-failure value would be ambiguous. Instead we set inode_num
        // from the live `nodes.len()` exactly once at the end, for BOTH the Ok
        // and the early-`?` paths, via the closure-then-finalize shape below.
        //
        // This is correct regardless of where a `?` fires, because the inode
        // gauge tracks ONLY `self.nodes`, which is rebuilt in a single
        // clear-then-rebuild pass:
        //  - failure before `clear()` (id_creator/nodes_count read) -> the
        //    `nodes` map is untouched -> len() == old live count -> gauge stays
        //    correct. (Note `self.id_creator` is reassigned before `clear()`, so
        //    "before bulk reset" means the node MAP is untouched, not that the
        //    whole NodeMap is pristine — id_creator may already hold the snapshot
        //    value. That does not affect the inode gauge.)
        //  - failure after `clear()` (mid bulk-insert) -> len() == the partial
        //    count actually loaded -> gauge reflects live state.
        let result = (|| -> FuseResult<()> {
            let id_creator_value = reader.read_len()?;
            self.id_creator = AtomicCounter::new(id_creator_value);

            let nodes_count = reader.read_len()?;
            self.nodes.clear();
            self.nodes.reserve(nodes_count as usize);
            for _ in 0..nodes_count {
                let (id, node): (u64, NodeAttr) = reader.read_struct()?;
                // Drift-check allowlist point (3): restore bulk insert. Direct
                // `self.nodes.insert` on purpose — the end-of-function set below
                // owns the gauge for this path.
                self.nodes.insert(id, node);
            }

            let names_count = reader.read_len()?;
            self.names.clear();
            self.names.reserve(names_count as usize);
            for _ in 0..names_count {
                let (name, id): (String, u64) = reader.read_struct()?;
                self.names.insert(name, id);
            }

            self.linked_inode_map = reader.read_struct()?;
            self.pending_deletes = reader.read_struct()?;

            Ok(())
        })();

        // Single inode_num set from the live map count — runs on success and on
        // every early-return above (the closure captured `&mut self`, so reading
        // `self.nodes.len()` here is fine and reflects whatever was loaded).
        FuseMetrics::with(|m| {
            let live = self.nodes.len();
            Self::sync_inode_gauge(&m.inode_num, live);
            Self::sync_inode_gauge(&m.inode_count, live);
        });
        result
    }

    /// Set the inode gauge to the live node-map count. Extracted as a `&Gauge`
    /// taker (not inlined into the `with` closure) so the restore finalizer's
    /// "live count -> gauge" mapping is unit-testable against an injected,
    /// isolated gauge — the process-global `inode_num` is written by other
    /// parallel tests, so a direct assertion on it would be flaky.
    fn sync_inode_gauge(gauge: &orpc::common::Gauge, live_len: usize) {
        gauge.set(live_len as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::NodeMap;
    use crate::fs::state::NodeAttr;
    use crate::FUSE_ROOT_ID;
    use curvine_common::conf::FuseConf;
    use curvine_common::fs::{StateReader, StateWriter};
    use orpc::common::Utils;

    fn test_map() -> NodeMap {
        let conf = FuseConf::default();
        NodeMap::new(&conf)
    }

    // The event-driven inode_num gauge keys off the per-`NodeMap` `nodes`
    // map-state predicates exercised here (insert returns None on a new key,
    // remove returns Some on a real removal, a same-id reinsert returns Some).
    // We assert on the local `nodes_len()` / chokepoint return values rather than
    // the process-global gauge, which other tests (path/ttl) mutate in parallel
    // once any test calls `ensure_init` — a global delta would be flaky and a
    // standalone serialized gauge test is out of scope. These invariants are
    // exactly what make the gauge correct.

    #[test]
    fn new_map_starts_with_only_root() {
        let map = test_map();
        assert_eq!(
            map.nodes_len(),
            1,
            "fresh NodeMap holds just the root inode"
        );
        assert!(map.get(FUSE_ROOT_ID).is_some());
    }

    #[test]
    fn insert_node_inc_predicate_only_on_new_key() {
        let mut map = test_map();
        let before = map.nodes_len();

        // New key -> insert returns None (the gauge inc branch).
        let prev = map.insert_node(100, NodeAttr::new(100, Some("a"), FUSE_ROOT_ID));
        assert!(prev.is_none(), "new id must report no previous (would inc)");
        assert_eq!(map.nodes_len(), before + 1);

        // Same id again -> insert returns Some (the no-inc branch); count steady.
        let prev = map.insert_node(100, NodeAttr::new(100, Some("a2"), FUSE_ROOT_ID));
        assert!(
            prev.is_some(),
            "same id must report previous (would NOT inc)"
        );
        assert_eq!(map.nodes_len(), before + 1, "replace does not grow the map");
    }

    #[test]
    fn remove_node_dec_predicate_only_on_real_removal() {
        let mut map = test_map();
        map.insert_node(100, NodeAttr::new(100, Some("a"), FUSE_ROOT_ID));
        let len = map.nodes_len();

        // Real removal -> remove returns Some (the gauge dec branch).
        let removed = map.remove_node(100);
        assert!(
            removed.is_some(),
            "real removal must report a node (would dec)"
        );
        assert_eq!(map.nodes_len(), len - 1);

        // Repeat removal of the same id -> None (no dec); idempotent for the
        // recursive delete chain where the same id can be removed twice.
        let removed = map.remove_node(100);
        assert!(
            removed.is_none(),
            "second removal must be a no-op (would NOT dec)"
        );
        assert_eq!(map.nodes_len(), len - 1);
    }

    #[test]
    fn rename_keeps_same_inode_count() {
        // rename_node reinserts the SAME inode id under a new parent/name; the
        // chokepoint sees `prev.is_some()` and must not inc. Drive it through the
        // real rename path and assert the live count is unchanged.
        let mut map = test_map();
        // Two children under root: "src" and the rename target's parent "dst".
        let src = map.find_node(FUSE_ROOT_ID, Some("src")).unwrap().id;
        let dst_parent = map.find_node(FUSE_ROOT_ID, Some("dst")).unwrap().id;
        let before = map.nodes_len();

        map.rename_node(FUSE_ROOT_ID, "src", dst_parent, "renamed")
            .unwrap();

        assert_eq!(
            map.nodes_len(),
            before,
            "rename moves an inode; it must not change the inode count"
        );
        // The inode survives under its new parent, same id.
        assert!(map.get(src).is_some(), "renamed inode keeps its id");
    }

    #[test]
    fn rename_overwrite_unreffed_target_drops_one_inode() {
        // rename onto an EXISTING target whose `should_unref()` is true must
        // remove the target inode through the `remove_node` chokepoint, so the
        // live count drops by exactly one (and, in production, both the legacy
        // `inode_num` and the namespaced `inode_count` gauges decrement). This is
        // the path that previously did a bare `self.nodes.remove`, drifting the
        // gauges. We assert on `nodes_len()` (the value the gauges track) rather
        // than the process-global gauge, to stay parallel-test safe.
        let mut map = test_map();
        let src = map.find_node(FUSE_ROOT_ID, Some("src")).unwrap().id;
        let target = map.find_node(FUSE_ROOT_ID, Some("target")).unwrap().id;

        // Make the target collectible: drop its lookup to 0 (find_node set it to
        // 1) so `should_unref()` (n_lookup==0 && ref_ctr==0 && !root) holds.
        map.get_mut(target).unwrap().sub_lookup(1);
        assert!(
            map.get(target).unwrap().should_unref(),
            "target must be unref-able to exercise the remove branch"
        );

        let before = map.nodes_len();
        map.rename_node(FUSE_ROOT_ID, "src", FUSE_ROOT_ID, "target")
            .unwrap();

        assert_eq!(
            map.nodes_len(),
            before - 1,
            "overwriting an unreffed target removes exactly one inode"
        );
        // The overwritten target id is gone; the source inode took its name.
        assert!(
            map.get(target).is_none(),
            "overwritten target inode removed"
        );
        assert!(map.get(src).is_some(), "source inode survives the rename");
    }

    // Phase 3a node_cache_total probe: `do_lookup_probed` must report the
    // hit/miss decided BEFORE find_node auto-creates, only when record=true and
    // name is Some, and on every return path. This is parallel-safe: it asserts
    // the returned probe bool, not the process-global node_cache_total counter.
    #[test]
    fn do_lookup_probed_reports_hit_miss_only_when_recording() {
        use curvine_common::state::FileStatus;
        let mut map = test_map();
        // Distinct names per case: EVEN a record=false lookup materializes the
        // node via find_node (it is a real lookup, just not counted), so each
        // presence assertion must use a name not yet touched.
        let s_y = FileStatus::with_name(2, "y".to_string(), true);
        let s_z = FileStatus::with_name(3, "z".to_string(), true);

        // record=true, first lookup of "z": miss (not yet in the map).
        let (_r, probe) = map.do_lookup_probed(FUSE_ROOT_ID, Some("z"), &s_z, true);
        assert_eq!(probe, Some(false), "first lookup is a miss");

        // record=true, second lookup of "z": hit (find_node materialized it).
        let (_r, probe) = map.do_lookup_probed(FUSE_ROOT_ID, Some("z"), &s_z, true);
        assert_eq!(probe, Some(true), "second lookup is a hit");

        // record=false (readdir / mkdir path): never probes, even though it still
        // materializes "y".
        let (_r, probe) = map.do_lookup_probed(FUSE_ROOT_ID, Some("y"), &s_y, false);
        assert_eq!(probe, None, "record=false must not probe");
        // Confirm that record=false call DID materialize "y": a recording lookup
        // now sees a hit.
        let (_r, probe) = map.do_lookup_probed(FUSE_ROOT_ID, Some("y"), &s_y, true);
        assert_eq!(
            probe,
            Some(true),
            "record=false still materializes the node"
        );

        // record=true but name=None (`.`/`..`): not counted.
        let root_status = FileStatus::with_name(1, "/".to_string(), true);
        let (_r, probe) = map.do_lookup_probed(FUSE_ROOT_ID, None::<&str>, &root_status, true);
        assert_eq!(probe, None, "name=None must not be counted");
    }

    #[test]
    fn delete_node_drops_one_inode() {
        let mut map = test_map();
        let id = map.find_node(FUSE_ROOT_ID, Some("victim")).unwrap().id;
        let with_victim = map.nodes_len();
        let node = map.get(id).unwrap().clone();

        map.delete_node(&node).unwrap();
        assert_eq!(
            map.nodes_len(),
            with_victim - 1,
            "delete_node removes exactly one"
        );
        assert!(map.get(id).is_none());
    }

    // restore is a clear-then-rebuild bulk path: the finalizer sets inode_num
    // from the live `nodes.len()`, NOT a running "restored" counter. The key
    // case is restoring onto a NON-EMPTY map (re-restore / live-upgrade retry):
    // restore does not pre-clear from the caller's view, so only the live count
    // is correct. We assert on `nodes_len()`, which is exactly the value the
    // finalizer feeds the gauge.
    #[test]
    fn restore_onto_nonempty_map_uses_live_count() {
        // Source map with root + 3 children = 4 nodes.
        let mut src = test_map();
        for n in ["a", "b", "c"] {
            src.find_node(FUSE_ROOT_ID, Some(n)).unwrap();
        }
        assert_eq!(src.nodes_len(), 4);

        let path = Utils::temp_file();
        {
            let mut writer = StateWriter::new(&path).unwrap();
            src.persist(&mut writer).unwrap();
        }

        // Destination already holds extra nodes (root + 5). A running counter
        // would mis-add; the live count after a clear-then-rebuild must be the
        // source's 4.
        let mut dst = test_map();
        for n in ["x", "y", "z", "w", "v"] {
            dst.find_node(FUSE_ROOT_ID, Some(n)).unwrap();
        }
        assert_eq!(dst.nodes_len(), 6, "dst non-empty before restore");

        let mut reader = StateReader::new(&path).unwrap();
        dst.restore(&mut reader).unwrap();

        assert_eq!(
            dst.nodes_len(),
            4,
            "restore replaces state; live count = source count, not old+new"
        );
        let _ = std::fs::remove_file(&path);
    }

    // The restore finalizer feeds `nodes.len()` to the gauge via
    // `sync_inode_gauge`. The test above proves the live count is right; this
    // proves the finalizer actually writes that count into the gauge (catches a
    // deleted finalizer line). Uses an INJECTED isolated gauge — the global
    // `inode_num` is written by parallel tests, so a direct assertion on it
    // would be flaky.
    #[test]
    fn sync_inode_gauge_sets_live_count() {
        let g = orpc::common::Metrics::new_gauge(
            "test_sync_inode_gauge_unique",
            "isolated gauge for finalizer mapping test",
        )
        .unwrap();
        g.set(999); // stale value the finalizer must overwrite
        NodeMap::sync_inode_gauge(&g, 7);
        assert_eq!(g.get(), 7, "finalizer must set the gauge to the live count");
    }
}
