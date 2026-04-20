//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::fs::dcache::inode::Inode;
use crate::fs::dcache::DirEntry;
use crate::{
    err_fuse, FuseResult, FUSE_PATH_MAX_DEPTH, FUSE_PATH_SEPARATOR, FUSE_ROOT_ID, FUSE_UNKNOWN_INO,
};
use curvine_common::conf::FuseConf;
use curvine_common::fs::{Path, StateReader, StateWriter};
use curvine_common::state::FileStatus;
use log::info;
use orpc::common::{FastHashMap, LocalTime};
use orpc::sync::AtomicCounter;

pub struct DirTree {
    inodes: FastHashMap<u64, Inode>,
    id_creator: AtomicCounter,
    conf: FuseConf,
    cache_ttl: u64,
    last_clean: u64,
}

impl DirTree {
    pub fn new(conf: FuseConf) -> Self {
        let cache_ttl = conf.node_cache_ttl.as_millis() as u64;
        let mut tree = Self {
            inodes: FastHashMap::default(),
            id_creator: AtomicCounter::new(u64::MAX / 2),
            conf,
            cache_ttl,
            last_clean: LocalTime::mills(),
        };
        tree.inodes.insert(FUSE_ROOT_ID, Inode::new_root());
        tree
    }

    pub fn inode_lens(&self) -> usize {
        self.inodes.len()
    }

    pub fn current_id(&self) -> u64 {
        self.id_creator.get()
    }

    pub fn get_ino(&self, ino: u64, name: Option<&str>) -> Option<u64> {
        if let Some(name) = name {
            let inode = self.inodes.get(&ino)?;
            inode.dir.as_ref()?.children.get(name).cloned()
        } else {
            Some(ino)
        }
    }

    pub fn get_ino_check(&self, ino: u64, name: Option<&str>) -> FuseResult<u64> {
        match self.get_ino(ino, name) {
            None => err_fuse!(libc::ENOENT, "inode {} {:?} not exists", ino, name),
            Some(v) => Ok(v),
        }
    }

    pub fn get_inode(&self, ino: u64, name: Option<&str>) -> Option<&Inode> {
        let ino = self.get_ino(ino, name)?;
        self.inodes.get(&ino)
    }

    pub fn get_inode_check(&self, ino: u64, name: Option<&str>) -> FuseResult<&Inode> {
        match self.get_inode(ino, name) {
            None => err_fuse!(libc::ENOENT, "inode {} {:?} not exists", ino, name),
            Some(v) => Ok(v),
        }
    }

    pub fn get_inode_mut(&mut self, ino: u64, name: Option<&str>) -> Option<&mut Inode> {
        let ino = self.get_ino(ino, name)?;
        self.inodes.get_mut(&ino)
    }

    pub fn get_inode_mut_check(&mut self, ino: u64, name: Option<&str>) -> FuseResult<&mut Inode> {
        match self.get_inode_mut(ino, name) {
            None => err_fuse!(libc::ENOENT, "inode {} {:?} not exists", ino, name),
            Some(v) => Ok(v),
        }
    }

    pub fn get_dir_mut_check(&mut self, ino: u64) -> FuseResult<&mut DirEntry> {
        match self.inodes.get_mut(&ino) {
            None => err_fuse!(libc::ENOENT, "inode {} not found", ino),
            Some(inode) => match inode.dir.as_mut() {
                None => err_fuse!(libc::ENOTDIR, "inode {} is not a directory", ino),
                Some(dir) => Ok(dir),
            },
        }
    }

    pub fn get_dir_check(&self, ino: u64) -> FuseResult<&DirEntry> {
        match self.inodes.get(&ino) {
            None => err_fuse!(libc::ENOENT, "inode {} not found", ino),
            Some(inode) => match inode.dir.as_ref() {
                None => err_fuse!(libc::ENOTDIR, "inode {} is not a directory", ino),
                Some(dir) => Ok(dir),
            },
        }
    }

    fn remove_inode(&mut self, ino: u64) {
        self.inodes.remove(&ino);
    }

    pub fn next_id(&self, cv_id: i64) -> u64 {
        if cv_id > FUSE_ROOT_ID as i64 {
            return cv_id as u64;
        }

        loop {
            let id = self.id_creator.next();
            if id == FUSE_ROOT_ID || id == FUSE_UNKNOWN_INO || self.inodes.contains_key(&id) {
                continue;
            } else {
                return id;
            }
        }
    }

    // LOOKUP: create inode and parent directory entry as needed.
    pub fn lookup(&mut self, parent: u64, name: &str, status: FileStatus) -> FuseResult<&Inode> {
        let ino = match self.get_inode_mut(parent, Some(name)) {
            Some(inode) => {
                inode.add_lookup(1);
                inode.update_status(status);
                inode.ino
            }
            None => {
                let ino = self.next_id(status.id);
                self.inodes
                    .insert(ino, Inode::with_status(ino, parent, name, status));
                ino
            }
        };

        // Link child name under parent directory.
        let dir = self.get_dir_mut_check(parent)?;
        dir.children.insert(name.to_owned(), ino);

        self.get_inode_check(ino, None)
    }

    pub fn unlink(&mut self, parent: u64, name: &str) -> FuseResult<()> {
        let (ino, should_remove) = {
            let inode = self.get_inode_mut_check(parent, Some(name))?;
            inode.ensure_dir_empty()?;

            inode.sub_ref(1);
            inode.sub_link(1);
            (inode.ino, inode.should_unref())
        };
        if should_remove {
            self.remove_inode(ino);
        }

        // Remove directory entry; keep parent inode's `DirEntry` even when `children` is empty.
        let dir = self.get_dir_mut_check(parent)?;
        dir.children.remove(name);

        Ok(())
    }

    pub fn forget(&mut self, ino: u64, n_lookup: u64) -> FuseResult<()> {
        let should_unref = match self.get_inode_mut(ino, None) {
            None => return Ok(()),
            Some(inode) => {
                inode.sub_lookup(n_lookup);
                inode.should_unref()
            }
        };
        if should_unref {
            self.remove_inode(ino);
        }

        Ok(())
    }

    pub fn rename(
        &mut self,
        old_id: u64,
        old_name: &str,
        new_id: u64,
        new_name: &str,
    ) -> FuseResult<()> {
        self.get_dir_check(new_id)?;

        let old_ino = self.get_ino_check(old_id, Some(old_name))?;

        // If the target exists, unlink it first to avoid inode leaks.
        // Same inode (rename-in-place / hard-link corner cases): POSIX requires success with no-op.
        if let Some(existing_ino) = self.get_ino(new_id, Some(new_name)) {
            if existing_ino == old_ino {
                return Ok(());
            }
            let should_remove = {
                let inode = self.get_inode_mut_check(existing_ino, None)?;
                inode.sub_ref(1);
                inode.sub_link(1);
                inode.should_unref()
            };
            if should_remove {
                self.remove_inode(existing_ino);
            }
        }

        // Remove old directory entry.
        let old_dir = self.get_dir_mut_check(old_id)?;
        old_dir.children.remove(old_name);

        // Insert new directory entry.
        let new_dir = self.get_dir_mut_check(new_id)?;
        new_dir.children.insert(new_name.to_string(), old_ino);

        let inode = self.get_inode_mut_check(old_ino, None)?;
        inode.parent = new_id;
        inode.name = new_name.to_string();

        Ok(())
    }

    pub fn link(&mut self, old_id: u64, new_id: u64, new_name: &str) -> FuseResult<&Inode> {
        let new_dir = self.get_dir_mut_check(new_id)?;
        new_dir.children.insert(new_name.to_string(), old_id);

        let inode = self.get_inode_mut_check(old_id, None)?;
        inode.add_ref(1);
        inode.add_lookup(1);
        inode.add_link(1);

        Ok(inode)
    }

    pub fn try_get_path(&self, parent: u64, name: Option<&str>) -> FuseResult<Path> {
        let mut segments: Vec<&str> = Vec::with_capacity(8);
        let mut seg_bytes = 0usize;

        if let Some(v) = name {
            seg_bytes += v.len();
            segments.push(v);
        }
        let mut inode = self.get_inode_check(parent, None)?;
        while !inode.is_root() {
            seg_bytes += inode.name.len();
            segments.push(inode.name.as_str());
            inode = self.get_inode_check(inode.parent, None)?;

            if segments.len() >= FUSE_PATH_MAX_DEPTH {
                return err_fuse!(libc::ENAMETOOLONG, "too many path segments");
            }
        }

        seg_bytes += self.conf.fs_path.len();
        segments.push(&self.conf.fs_path);
        seg_bytes += segments.len();

        let mut path = String::with_capacity(seg_bytes);
        for seg in segments.iter().rev() {
            if !path.is_empty() && &path[path.len() - 1..] != FUSE_PATH_SEPARATOR {
                path.push_str(FUSE_PATH_SEPARATOR);
            }
            path.push_str(seg);
        }
        Ok(Path::from_str(path)?)
    }

    pub fn get_path_common(&self, parent: u64, name: Option<&str>) -> FuseResult<Path> {
        self.try_get_path(parent, name)
    }

    pub fn get_path(&self, ino: u64) -> FuseResult<Path> {
        self.get_path_common(ino, None)
    }

    pub fn get_path_name(&self, parent: u64, name: &str) -> FuseResult<Path> {
        self.try_get_path(parent, Some(name))
    }

    pub fn mark_delete(&mut self, ino: u64) -> FuseResult<()> {
        let inode = self.get_inode_mut_check(ino, None)?;
        inode.mark_delete = true;
        Ok(())
    }

    pub fn pending_delete(&self, ino: u64) -> bool {
        let inode = self.get_inode(ino, None);
        match inode {
            None => false,
            Some(inode) => inode.mark_delete,
        }
    }

    pub fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        writer.write_len(self.id_creator.get())?;
        writer.write_len(self.inodes.len() as u64)?;
        for (_, inode) in self.inodes.iter() {
            writer.write_struct(inode)?;
        }
        writer.flush()?;
        Ok(())
    }

    pub fn restore(&mut self, reader: &mut StateReader) -> FuseResult<()> {
        let id_creator_value = reader.read_len()?;
        self.id_creator = AtomicCounter::new(id_creator_value);

        let inodes_count = reader.read_len()?;
        self.inodes.reserve(inodes_count as usize);
        for _ in 0..inodes_count {
            let inode: Inode = reader.read_struct()?;
            self.inodes.insert(inode.ino, inode);
        }

        Ok(())
    }

    pub fn clear(&mut self, has_open_handles: impl Fn(u64) -> bool) {
        let now = LocalTime::mills();
        if self.last_clean + self.cache_ttl > now {
            return;
        }

        let to_remove: Vec<(u64, u64, String)> = self
            .inodes
            .values()
            .filter(|inode| {
                !inode.is_root()
                    && !has_open_handles(inode.ino)
                    && inode.last_access + self.cache_ttl <= now
                    && inode.dir.as_ref().is_none_or(|d| d.children.is_empty())
            })
            .map(|inode| (inode.ino, inode.parent, inode.name.clone()))
            .collect();

        let removed = to_remove.len();
        for (ino, parent, name) in &to_remove {
            if let Some(parent_inode) = self.inodes.get_mut(parent) {
                if let Some(dir) = parent_inode.dir.as_mut() {
                    dir.children.remove(name.as_str());
                }
            }
            self.inodes.remove(ino);
        }

        self.last_clean = now;
        info!(
            "DirTree::clear: evicted {} expired inodes, remaining {}, cost {} ms",
            removed,
            self.inodes.len(),
            LocalTime::mills() - now
        );
    }

    #[cfg(test)]
    pub(crate) fn set_last_clean_for_test(&mut self, last_clean: u64) {
        self.last_clean = last_clean;
    }
}

impl Default for DirTree {
    fn default() -> Self {
        Self::new(FuseConf::default())
    }
}

#[cfg(test)]
mod test {
    use crate::fs::dcache::DirTree;
    use crate::FUSE_ROOT_ID;
    use curvine_common::conf::FuseConf;
    use curvine_common::state::FileStatus;

    fn dir_st(name: &str, id: i64) -> FileStatus {
        FileStatus {
            is_dir: true,
            name: name.to_string(),
            path: format!("/{name}"),
            id,
            ..Default::default()
        }
    }

    fn file_st(name: &str, id: i64) -> FileStatus {
        FileStatus {
            is_dir: false,
            name: name.to_string(),
            id,
            ..Default::default()
        }
    }

    /// After lookup → rename → link → unlink and forget, the tree and ref counts stay consistent.
    #[test]
    fn create_lookup_rename_link_unlink_forget_keeps_tree_consistent() {
        let mut t = DirTree::default();

        t.lookup(FUSE_ROOT_ID, "d", dir_st("d", 100)).unwrap();
        assert!(t.get_inode_check(100, None).unwrap().is_dir);
        assert_eq!(t.get_inode(FUSE_ROOT_ID, Some("d")).unwrap().ino, 100);

        let f = t.lookup(FUSE_ROOT_ID, "f", file_st("f", 0)).unwrap().ino;
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, 1);
        assert_eq!(t.get_inode_check(f, None).unwrap().n_lookup, 1);

        t.rename(FUSE_ROOT_ID, "f", 100, "g").unwrap();
        assert!(t.get_inode(FUSE_ROOT_ID, Some("f")).is_none());
        assert_eq!(t.get_inode(100, Some("g")).unwrap().ino, f);

        t.link(f, FUSE_ROOT_ID, "h").unwrap();
        assert_eq!(t.get_inode(FUSE_ROOT_ID, Some("h")).unwrap().ino, f);
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, 2);

        t.unlink(FUSE_ROOT_ID, "h").unwrap();
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, 1);

        t.forget(f, 2).unwrap();
        assert!(t.get_inode(f, None::<&str>).is_some());
        assert_eq!(t.get_inode_check(f, None).unwrap().n_lookup, 0);
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, 1);
        assert_eq!(t.get_inode(100, Some("g")).unwrap().ino, f);

        t.unlink(100, "g").unwrap();
        assert!(t.get_inode(f, None::<&str>).is_none());

        assert!(t.get_inode_check(100, None).is_ok());
        assert_eq!(t.get_inode(FUSE_ROOT_ID, Some("d")).unwrap().ino, 100);
    }

    /// Single path, single lookup: after `unlink`, `n_lookup` stays 1 (kernel still holds dentry);
    /// inode must remain in dcache (deferred delete relies on this); `forget` drops `n_lookup` then removes inode.
    #[test]
    fn unlink_drops_inode_when_last_ref_forget_is_idempotent() {
        let mut t = DirTree::default();
        let f = t.lookup(FUSE_ROOT_ID, "x", file_st("x", 0)).unwrap().ino;
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, 1);
        assert_eq!(t.get_inode_check(f, None).unwrap().n_lookup, 1);
        t.unlink(FUSE_ROOT_ID, "x").unwrap();
        // ref_ctr=0 but n_lookup=1 → should_unref() false → inode kept
        assert!(t.get_inode(f, None).is_some());
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, 0);
        assert_eq!(t.get_inode_check(f, None).unwrap().n_lookup, 1);

        // forget clears n_lookup → should_unref() true → inode removed
        t.forget(f, 1).unwrap();
        assert!(t.get_inode(f, None).is_none());

        // Second forget is idempotent
        t.forget(f, 1).unwrap();
        assert!(t.get_inode(f, None).is_none());
    }

    /// After forget clears n_lookup on a looked-up file, unlink should remove the inode.
    #[test]
    fn create_then_forget_then_unlink() {
        let mut t = DirTree::default();
        t.lookup(FUSE_ROOT_ID, "c", file_st("c", 200)).unwrap();
        let n0 = t.get_inode_check(200, None).unwrap().n_lookup;
        t.forget(200, n0).unwrap();
        assert_eq!(t.get_inode_check(200, None).unwrap().n_lookup, 0);
        t.unlink(FUSE_ROOT_ID, "c").unwrap();
        assert!(t.get_inode(200, None).is_none());
    }

    /// Repeated lookup on the same name: n_lookup increases each time; ref_ctr increments only on first dirent.
    #[test]
    fn repeated_lookup_accumulates_ref_and_nlookup() {
        let mut t = DirTree::default();
        let st = file_st("p", 0);
        let i = t.lookup(FUSE_ROOT_ID, "p", st.clone()).unwrap().ino;
        t.lookup(FUSE_ROOT_ID, "p", st).unwrap();
        assert_eq!(t.get_inode_check(i, None).unwrap().ref_ctr, 1);
        assert_eq!(t.get_inode_check(i, None).unwrap().n_lookup, 2);
    }

    /// Directory lookup records child names in the directory inode's `dir.children`; children resolve under parent.
    #[test]
    fn lookup_dir_inserts_dirs_map_and_child_visible() {
        let mut t = DirTree::default();
        let d_ino = t.lookup(FUSE_ROOT_ID, "sub", dir_st("sub", 0)).unwrap().ino;
        assert!(t.get_inode_check(d_ino, None).unwrap().is_dir);
        let inner_ino = t.lookup(d_ino, "inner", file_st("inner", 0)).unwrap().ino;
        assert_eq!(t.get_inode(d_ino, Some("inner")).unwrap().ino, inner_ino);
    }

    #[test]
    fn try_get_path_root_only() {
        let t = DirTree::default();
        let p = t.try_get_path(FUSE_ROOT_ID, None).unwrap();
        assert_eq!(p.full_path(), "/");
    }

    #[test]
    fn try_get_path_root_with_tail() {
        let t = DirTree::default();
        let p = t.try_get_path(FUSE_ROOT_ID, Some("a")).unwrap();
        assert_eq!(p.full_path(), "/a");
    }

    #[test]
    fn try_get_path_dir_without_tail() {
        let mut t = DirTree::default();
        let d_ino = t.lookup(FUSE_ROOT_ID, "sub", dir_st("sub", 0)).unwrap().ino;
        let p = t.try_get_path(d_ino, None).unwrap();
        assert_eq!(p.full_path(), "/sub");
    }

    #[test]
    fn try_get_path_nested_dir_and_tail() {
        let mut t = DirTree::default();
        let d_ino = t.lookup(FUSE_ROOT_ID, "sub", dir_st("sub", 0)).unwrap().ino;
        let p = t.try_get_path(d_ino, Some("file.txt")).unwrap();
        assert_eq!(p.full_path(), "/sub/file.txt");
    }

    #[test]
    fn try_get_path_three_levels() {
        let mut t = DirTree::default();
        let a_ino = t.lookup(FUSE_ROOT_ID, "a", dir_st("a", 0)).unwrap().ino;
        let mut b = dir_st("b", 0);
        b.path = "/a/b".to_owned();
        let b_ino = t.lookup(a_ino, "b", b).unwrap().ino;
        let p = t.try_get_path(b_ino, Some("c")).unwrap();
        assert_eq!(p.full_path(), "/a/b/c");
    }

    #[test]
    fn try_get_path_prefixes_fs_path_from_conf() {
        let conf = FuseConf {
            fs_path: "s3://bucket/prefix".to_string(),
            ..Default::default()
        };
        let mut t = DirTree::new(conf);
        let d_ino = t.lookup(FUSE_ROOT_ID, "sub", dir_st("sub", 0)).unwrap().ino;
        let p = t.try_get_path(d_ino, Some("x")).unwrap();
        assert_eq!(p.full_path(), "s3://bucket/prefix/sub/x");
    }

    /// Rename within the same parent only changes the name; ino and ref count unchanged.
    #[test]
    fn rename_within_same_parent_keeps_ino_and_ref() {
        let mut t = DirTree::default();
        let i = t.lookup(FUSE_ROOT_ID, "a", file_st("a", 300)).unwrap().ino;
        let r = t.get_inode_check(i, None).unwrap().ref_ctr;
        t.rename(FUSE_ROOT_ID, "a", FUSE_ROOT_ID, "b").unwrap();
        assert_eq!(t.get_inode(FUSE_ROOT_ID, Some("b")).unwrap().ino, i);
        assert!(t.get_inode(FUSE_ROOT_ID, Some("a")).is_none());
        assert_eq!(t.get_inode_check(i, None).unwrap().ref_ctr, r);
    }

    /// Missing `(parent, name)`: `get_inode_mut_check(parent, Some(name))` fails.
    #[test]
    fn get_child_mut_check_missing_returns_err() {
        let mut t = DirTree::default();
        assert!(t
            .get_inode_mut_check(FUSE_ROOT_ID, Some("missing"))
            .is_err());
    }

    /// `forget` on a non-existent ino succeeds (idempotent).
    #[test]
    fn forget_absent_inode_ok() {
        let mut t = DirTree::default();
        t.forget(9_999_999, 1).unwrap();
    }

    /// Root inode is never removed by `forget` via `should_unref`.
    #[test]
    fn forget_root_keeps_root_inode() {
        let mut t = DirTree::default();
        t.forget(FUSE_ROOT_ID, 1).unwrap();
        assert!(t.get_inode(FUSE_ROOT_ID, None).is_some());
        assert!(t.get_inode_check(FUSE_ROOT_ID, None).unwrap().is_root());
    }

    /// `link` bumps both ref_ctr and n_lookup:
    /// fuse_entry_out gives the kernel a new lookup ref; one matching forget is required.
    #[test]
    fn link_bumps_both_ref_and_nlookup() {
        let mut t = DirTree::default();
        t.lookup(FUSE_ROOT_ID, "d", dir_st("d", 400)).unwrap();
        let f = t.lookup(FUSE_ROOT_ID, "f", file_st("f", 0)).unwrap().ino;
        let n_ref = t.get_inode_check(f, None).unwrap().ref_ctr;
        let n_lookup = t.get_inode_check(f, None).unwrap().n_lookup;
        t.link(f, 400, "hard").unwrap();
        assert_eq!(t.get_inode_check(f, None).unwrap().ref_ctr, n_ref + 1);
        assert_eq!(t.get_inode_check(f, None).unwrap().n_lookup, n_lookup + 1);
        assert_eq!(t.get_inode(400, Some("hard")).unwrap().ino, f);
    }

    /// Hard link: `link` adds ref_ctr; each `unlink` of a dirent subtracts ref_ctr; inode removed when zero (after forget if n_lookup).
    #[test]
    fn hard_link_ref_count_and_unlink_removes_inode_when_zero() {
        let mut t = DirTree::default();
        t.lookup(FUSE_ROOT_ID, "d", dir_st("d", 600)).unwrap();

        let f_ino = t.lookup(FUSE_ROOT_ID, "f", file_st("f", 0)).unwrap().ino;
        assert_eq!(t.get_inode_check(f_ino, None).unwrap().ref_ctr, 1);

        t.link(f_ino, 600, "hard").unwrap();
        assert_eq!(t.get_inode_check(f_ino, None).unwrap().ref_ctr, 2);
        assert_eq!(t.get_inode(600, Some("hard")).unwrap().ino, f_ino);

        t.unlink(FUSE_ROOT_ID, "f").unwrap();
        assert_eq!(t.get_inode_check(f_ino, None).unwrap().ref_ctr, 1);
        assert!(t.get_inode(f_ino, None).is_some());
        assert!(t.get_inode(FUSE_ROOT_ID, Some("f")).is_none());
        assert_eq!(t.get_inode(600, Some("hard")).unwrap().ino, f_ino);

        t.unlink(600, "hard").unwrap();
        // ref_ctr=0 but n_lookup=2 (lookup + link), inode stays until forget
        assert!(t.get_inode(f_ino, None).is_some());
        assert_eq!(t.get_inode_check(f_ino, None).unwrap().ref_ctr, 0);
        assert_eq!(t.get_inode_check(f_ino, None).unwrap().n_lookup, 2);

        // forget clears n_lookup; inode is removed
        t.forget(f_ino, 2).unwrap();
        assert!(t.get_inode(f_ino, None).is_none());
        assert!(t.get_inode_check(f_ino, None).is_err());
    }

    /// Renaming onto an existing target decrements the old target's ref_ctr by 1;
    /// kernel may still hold n_lookup, so inode stays until forget.
    #[test]
    fn rename_overwrites_existing_target_frees_inode() {
        let mut t = DirTree::default();

        // Create source "src" and target "dst"
        let src = t
            .lookup(FUSE_ROOT_ID, "src", file_st("src", 0))
            .unwrap()
            .ino;
        let dst = t
            .lookup(FUSE_ROOT_ID, "dst", file_st("dst", 0))
            .unwrap()
            .ino;

        assert!(t.get_inode(dst, None).is_some());

        // rename src → dst overwrites dst: ref_ctr=0, n_lookup=1, inode kept
        t.rename(FUSE_ROOT_ID, "src", FUSE_ROOT_ID, "dst").unwrap();

        // "src" dirent gone; "dst" dirent points at src's ino
        assert!(t.get_inode(FUSE_ROOT_ID, Some("src")).is_none());
        assert_eq!(t.get_inode(FUSE_ROOT_ID, Some("dst")).unwrap().ino, src);

        // old dst inode kept (n_lookup=1), ref_ctr zero
        assert!(t.get_inode(dst, None).is_some());
        assert_eq!(t.get_inode_check(dst, None).unwrap().ref_ctr, 0);
        assert_eq!(t.get_inode_check(dst, None).unwrap().n_lookup, 1);

        // forget removes inode from dcache
        t.forget(dst, 1).unwrap();
        assert!(t.get_inode(dst, None).is_none());
    }

    /// Rename-over-target: target ref_ctr hits zero but n_lookup > 0 → inode kept until forget;
    /// matches deferred delete and kernel dentry lifetime.
    #[test]
    fn rename_overwrites_target_with_active_lookup_keeps_inode() {
        let mut t = DirTree::default();

        let src = t
            .lookup(FUSE_ROOT_ID, "src", file_st("src", 0))
            .unwrap()
            .ino;
        // Two lookups on "dst": ref_ctr=1 (single dirent), n_lookup=2
        t.lookup(FUSE_ROOT_ID, "dst", file_st("dst", 0)).unwrap();
        let dst = t
            .lookup(FUSE_ROOT_ID, "dst", file_st("dst", 0))
            .unwrap()
            .ino;
        assert_eq!(t.get_inode_check(dst, None).unwrap().ref_ctr, 1);
        assert_eq!(t.get_inode_check(dst, None).unwrap().n_lookup, 2);

        t.rename(FUSE_ROOT_ID, "src", FUSE_ROOT_ID, "dst").unwrap();

        // "dst" dirent now points at src's ino
        assert_eq!(t.get_inode(FUSE_ROOT_ID, Some("dst")).unwrap().ino, src);

        // old dst: ref_ctr=0, n_lookup=2 → should_unref() false → inode kept
        assert!(t.get_inode(dst, None).is_some());
        assert_eq!(t.get_inode_check(dst, None).unwrap().ref_ctr, 0);
        assert_eq!(t.get_inode_check(dst, None).unwrap().n_lookup, 2);

        // forget clears n_lookup; inode removed
        t.forget(dst, 2).unwrap();
        assert!(t.get_inode(dst, None).is_none());
    }

    /// `clear` evicts expired inodes but skips: open handles, not yet TTL-expired entries,
    /// dirs with cached children, root. Throttled by `last_clean` until `cache_ttl` elapses.
    #[test]
    fn clear_evicts_expired_inodes_and_respects_all_constraints() {
        use std::time::Duration;

        let conf = FuseConf {
            node_cache_ttl: Duration::from_secs(60),
            ..Default::default()
        };
        let mut t = DirTree::new(conf);

        // Case 1: expired, ref_ctr=0, no handles → should be evicted
        // Simulates unlinked file (ref_ctr=0) while kernel still holds dentry (n_lookup=1):
        // should_unref() false, no FORGET yet, inode still in dcache.
        let f1 = t.lookup(FUSE_ROOT_ID, "f1", file_st("f1", 0)).unwrap().ino;
        t.unlink(FUSE_ROOT_ID, "f1").unwrap();
        assert_eq!(t.get_inode_check(f1, None).unwrap().ref_ctr, 0);
        assert_eq!(t.get_inode_check(f1, None).unwrap().n_lookup, 1);
        t.get_inode_mut(f1, None).unwrap().last_access = 0; // force past TTL

        // Case 2: still linked, fresh last_access → not evicted (per-inode TTL not expired)
        let f2 = t.lookup(FUSE_ROOT_ID, "f2", file_st("f2", 0)).unwrap().ino;
        // Do not zero last_access: clear() does not consult ref_ctr; expiry is last_access-based.

        // Case 3: expired, ref_ctr=0, but open handle → not evicted
        let f3 = t.lookup(FUSE_ROOT_ID, "f3", file_st("f3", 0)).unwrap().ino;
        t.unlink(FUSE_ROOT_ID, "f3").unwrap();
        t.get_inode_mut(f3, None).unwrap().last_access = 0;

        // Case 4: ref_ctr=0 but not expired → not evicted
        let f4 = t.lookup(FUSE_ROOT_ID, "f4", file_st("f4", 0)).unwrap().ino;
        t.unlink(FUSE_ROOT_ID, "f4").unwrap();
        // last_access is fresh; within 60s TTL

        // Case 5: expired empty dir, ref_ctr=0 → evicted
        let d1 = t.lookup(FUSE_ROOT_ID, "d1", dir_st("d1", 500)).unwrap().ino;
        t.unlink(FUSE_ROOT_ID, "d1").unwrap(); // like rmdir; DirEntry.children empty
        t.get_inode_mut(d1, None).unwrap().last_access = 0;

        // Case 6: expired dir with ref_ctr=0 but cached children → not evicted
        // Evicting would orphan cached children and break path reconstruction.
        let d2 = t.lookup(FUSE_ROOT_ID, "d2", dir_st("d2", 600)).unwrap().ino;
        t.lookup(d2, "child", file_st("child", 0)).unwrap(); // d2.children has "child"
        t.get_inode_mut(d2, None).unwrap().ref_ctr = 0; // simulate unlinked dir inode
        t.get_inode_mut(d2, None).unwrap().last_access = 0;

        // First `clear` after `new()` is skipped until `last_clean + cache_ttl <= now`.
        t.set_last_clean_for_test(0);

        // Run clear; treat f3 as having an open handle
        t.clear(|ino| ino == f3);

        assert!(
            t.get_inode(f1, None::<&str>).is_none(),
            "f1 should be evicted"
        );
        assert!(
            t.get_inode(f2, None::<&str>).is_some(),
            "f2 should stay (last_access still within TTL)"
        );
        assert!(
            t.get_inode(f3, None::<&str>).is_some(),
            "f3 should stay (open handle)"
        );
        assert!(
            t.get_inode(f4, None::<&str>).is_some(),
            "f4 should stay (not expired)"
        );
        assert!(
            t.get_inode(d1, None::<&str>).is_none(),
            "d1 should be evicted (empty dir)"
        );
        assert!(
            t.get_inode(d2, None::<&str>).is_some(),
            "d2 should stay (has cached children)"
        );
        assert!(
            t.get_inode(FUSE_ROOT_ID, None::<&str>).is_some(),
            "root must never be evicted"
        );

        // cache_ttl==0: per-inode check is `last_access + 0 <= now`; stale last_access still evicts.
        let mut t0 = DirTree::default();
        let fx = t0.lookup(FUSE_ROOT_ID, "fx", file_st("fx", 0)).unwrap().ino;
        t0.unlink(FUSE_ROOT_ID, "fx").unwrap();
        t0.get_inode_mut(fx, None).unwrap().last_access = 0;
        t0.set_last_clean_for_test(0);
        t0.clear(|_| false);
        assert!(
            t0.get_inode(fx, None::<&str>).is_none(),
            "cache_ttl=0 still evicts when last_access + ttl <= now"
        );
    }
}
