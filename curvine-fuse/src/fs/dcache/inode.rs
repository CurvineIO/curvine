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

use crate::fs::dcache::{DirEntry, Lifecycle};
use crate::raw::fuse_abi::fuse_attr;
use crate::{err_fuse, FuseResult, FuseUtils, FUSE_PATH_SEPARATOR, FUSE_ROOT_ID};
use curvine_common::conf::FuseConf;
use curvine_common::state::{FileStatus, LocatedBlock};
use orpc::common::LocalTime;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Default, Deserialize, Serialize, Clone, Debug)]
pub struct Inode {
    pub ino: u64,
    pub parent: u64,
    pub name: String,

    pub status: FileStatus,
    pub locs: Option<Box<Vec<LocatedBlock>>>,

    pub lifecycle: Lifecycle,

    pub n_lookup: u64,
    pub ref_ctr: u64,
    pub last_access: u64,

    pub dir: Option<Box<DirEntry>>,

    pub mark_delete: bool,
}

impl Inode {
    pub fn new_root() -> Self {
        let root_st = FileStatus {
            is_dir: true,
            name: FUSE_PATH_SEPARATOR.to_owned(),
            path: FUSE_PATH_SEPARATOR.to_owned(),
            nlink: 2,
            ..Default::default()
        };
        let dir = Some(Box::new(DirEntry::new()));
        Inode {
            ino: FUSE_ROOT_ID,
            parent: 0,
            name: FUSE_PATH_SEPARATOR.to_owned(),
            status: root_st,
            locs: None,
            lifecycle: Lifecycle::Invalid,
            n_lookup: 0,
            ref_ctr: 0,
            last_access: LocalTime::mills(),
            dir,
            ..Default::default()
        }
    }

    pub fn with_status(ino: u64, parent: u64, name: &str, mut status: FileStatus) -> Self {
        let dir = if status.is_dir {
            Some(Box::new(DirEntry::new()))
        } else {
            None
        };

        status.id = ino as i64;
        Inode {
            ino,
            parent,
            name: name.to_owned(),
            status,
            locs: None,
            lifecycle: Lifecycle::Cached,
            n_lookup: 1,
            ref_ctr: 1,
            last_access: LocalTime::mills(),
            dir,
            ..Default::default()
        }
    }

    pub fn update_status(&mut self, mut status: FileStatus) {
        if status.is_dir {
            let needs_fresh_dir = self.dir.is_none() || !self.status.is_dir;
            if needs_fresh_dir {
                let _ = self.dir.replace(Box::new(DirEntry::new()));
            }
        } else {
            let _ = self.dir.take();
        }

        status.id = self.ino as i64;
        self.status = status;

        self.lifecycle = Lifecycle::Cached;
        self.last_access = LocalTime::mills();
    }

    pub fn invalid_cache(&mut self) {
        if matches!(self.lifecycle, Lifecycle::Cached) {
            self.lifecycle = Lifecycle::Invalid;
        }
    }

    pub fn is_root(&self) -> bool {
        self.ino == FUSE_ROOT_ID
    }

    pub fn add_lookup(&mut self, v: u64) -> u64 {
        self.n_lookup = self.n_lookup.saturating_add(v);
        self.n_lookup
    }

    pub fn sub_lookup(&mut self, v: u64) -> u64 {
        self.n_lookup = self.n_lookup.saturating_sub(v);
        self.n_lookup
    }

    pub fn add_ref(&mut self, v: u64) -> u64 {
        self.ref_ctr = self.ref_ctr.saturating_add(v);
        self.ref_ctr
    }

    pub fn sub_ref(&mut self, v: u64) -> u64 {
        self.ref_ctr = self.ref_ctr.saturating_sub(v);
        self.ref_ctr
    }

    pub fn should_unref(&self) -> bool {
        self.n_lookup == 0 && self.ref_ctr == 0 && !self.is_root()
    }

    pub fn sub_link(&mut self, v: u32) {
        self.nlink = self.nlink.saturating_sub(v);
    }

    pub fn add_link(&mut self, v: u32) {
        self.nlink = self.nlink.saturating_add(v);
    }

    pub fn ensure_dir_empty(&self) -> FuseResult<()> {
        if !self.is_dir {
            return Ok(());
        }
        let Some(dir) = self.dir.as_ref() else {
            return err_fuse!(libc::EIO, "directory inode {} missing DirEntry", self.ino);
        };
        if !dir.children.is_empty() {
            return err_fuse!(
                libc::ENOTEMPTY,
                "directory inode {} still has cached children",
                self.ino
            );
        }
        Ok(())
    }

    pub fn can_evict(&self, ttl: u64) -> bool {
        !self.is_root()
            && !self.cache_valid(ttl)
            && self.dir.as_ref().is_none_or(|d| d.children.is_empty())
    }

    pub fn remove_child(&mut self, name: &str) {
        if let Some(dir) = &mut self.dir {
            dir.remove_child(name);
        }
    }

    pub fn cache_valid(&self, ttl: u64) -> bool {
        match self.lifecycle {
            Lifecycle::Cached => self.last_access + ttl >= LocalTime::mills(),
            Lifecycle::Dirty => true,
            Lifecycle::Invalid => false,
        }
    }

    pub fn dir_scan_valid(&self, ttl: u64) -> bool {
        let Some(dir) = &self.dir else { return false };
        dir.scan_complete && self.last_access + ttl >= LocalTime::mills()
    }

    pub fn to_attr(&self, conf: &FuseConf) -> FuseResult<fuse_attr> {
        FuseUtils::status_to_attr(conf, &self.status)
    }
}

impl Deref for Inode {
    type Target = FileStatus;

    fn deref(&self) -> &Self::Target {
        &self.status
    }
}

impl DerefMut for Inode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.status
    }
}
