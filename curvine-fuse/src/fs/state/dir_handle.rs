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

use curvine_common::state::FileStatus;

pub struct DirHandle {
    pub ino: u64,
    pub fh: u64,

    list: Vec<FileStatus>,
}

impl DirHandle {
    pub fn new(ino: u64, fh: u64, list: Vec<FileStatus>) -> Self {
        DirHandle { ino, fh, list }
    }

    pub fn get_list(&self, skip: usize) -> impl Iterator<Item = (usize, &FileStatus)> {
        self.list.iter().enumerate().skip(skip)
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn get_all(&self) -> &[FileStatus] {
        &self.list
    }
}
