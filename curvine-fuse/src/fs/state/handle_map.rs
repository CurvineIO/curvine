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

use crate::fs::FuseFile;
use crate::{FuseResult, FUSE_UNKNOWN_INO};
use orpc::sync::{AtomicCounter, FastDashMap};
use orpc::sys::RawPtr;

pub struct HandleMap {
    pub files: FastDashMap<u64, RawPtr<FuseFile>>,
    fh_creator: AtomicCounter,
}

impl Default for HandleMap {
    fn default() -> Self {
        Self::new()
    }
}

impl HandleMap {
    pub fn new() -> Self {
        Self {
            files: FastDashMap::default(),
            fh_creator: AtomicCounter::new(0),
        }
    }

    fn id_used(&self, id: u64) -> bool {
        self.files.contains_key(&id)
    }

    pub fn next_id(&self) -> u64 {
        loop {
            let id = self.fh_creator.next();
            if id == 0 || id == FUSE_UNKNOWN_INO || self.id_used(id) {
                continue;
            } else {
                return id;
            }
        }
    }

    pub fn add(&self, mut file: FuseFile) -> FuseResult<u64> {
        let id = self.next_id();
        file.set_fh(id);
        self.files.insert(id, RawPtr::from_owned(file));
        Ok(id)
    }

    pub fn get(&self, fh: u64) -> Option<RawPtr<FuseFile>> {
        self.files.get(&fh).map(|x| x.clone())
    }

    pub fn remove(&self, fh: u64) -> Option<RawPtr<FuseFile>> {
        self.files.remove(&fh).map(|x| x.1)
    }
}
