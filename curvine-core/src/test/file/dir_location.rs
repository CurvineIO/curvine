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

use crate::io::{IOResult, LocalFile};
use std::fs;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct DirLocation {
    dirs: Vec<String>,
    current_index: AtomicU64,
}

impl DirLocation {
    pub fn new(dirs: Vec<String>) -> Self {
        Self::init(&dirs).unwrap();
        DirLocation {
            dirs,
            current_index: AtomicU64::new(0),
        }
    }

    fn init(dirs: &Vec<String>) -> IOResult<()> {
        for dir in dirs {
            if let Ok(meta) = fs::metadata(dir) {
                if meta.is_file() {
                    return Err("Not dir".into());
                }
            } else {
                fs::create_dir_all(dir)?;
            }
        }

        Ok(())
    }

    fn next_dir(&self) -> &str {
        let index = (self.current_index.fetch_add(1, Ordering::SeqCst) as usize) % self.dirs.len();
        &(self.dirs[index])
    }

    pub fn create_block(&self, id: u64) -> String {
        format!("{}/{}", self.next_dir(), id)
    }

    pub fn get_block(&self, id: u64) -> Option<String> {
        for dir in &self.dirs {
            let path = format!("{}/{}", dir, id);
            if fs::metadata(&path).is_ok() {
                return Some(path);
            }
        }

        None
    }

    pub fn get_or_create(&self, id: u64, for_write: bool) -> IOResult<LocalFile> {
        let path = match self.get_block(id) {
            Some(v) => v,
            None => self.create_block(id),
        };

        let file = if for_write {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?
        } else {
            OpenOptions::new().read(true).open(&path)?
        };

        LocalFile::from_file(&path, file)
    }
}
