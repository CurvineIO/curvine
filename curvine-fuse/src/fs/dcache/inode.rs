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

use crate::fs::dcache::{DirEntry, OpState};
use crate::{err_fuse, FuseResult, FUSE_PATH_SEPARATOR, FUSE_ROOT_ID};
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

    pub op_state: OpState,

    pub n_lookup: u64,
    pub ref_ctr: u64,
    pub last_access: u64,
    pub cache_valid: bool,

    pub dir: Option<Box<DirEntry>>,

    pub mark_delete: bool,
}

impl Inode {
    pub fn new_root() -> Self {
        let root_st = FileStatus {
            is_dir: true,
            name: FUSE_PATH_SEPARATOR.to_owned(),
            path: FUSE_PATH_SEPARATOR.to_owned(),
            ..Default::default()
        };
        let dir = Some(Box::new(DirEntry::new()));
        Inode {
            ino: FUSE_ROOT_ID,
            parent: 0,
            name: FUSE_PATH_SEPARATOR.to_owned(),
            status: root_st,
            locs: None,
            op_state: OpState::Cached,
            n_lookup: 0,
            ref_ctr: 0,
            last_access: LocalTime::mills(),
            cache_valid: false,
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
            op_state: OpState::Cached,
            n_lookup: 1,
            ref_ctr: 1,
            last_access: LocalTime::mills(),
            cache_valid: true,
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

        self.op_state = OpState::Cached;
        self.cache_valid = true;
        self.last_access = LocalTime::mills();
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
