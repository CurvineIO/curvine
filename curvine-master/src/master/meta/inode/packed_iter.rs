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

use crate::master::meta::inode::ChildItem;
use crate::master::meta::inode::InodeView;
use crate::master::meta::inode::PackedChildren;
use curvine_record::Chunk;

pub struct PackedIter<'a> {
    dirs: &'a [Option<Box<InodeView>>],
    chunks: &'a [Chunk],
    ci: usize,
    off: usize,
}

impl<'a> PackedIter<'a> {
    pub(super) fn new(dirs: &'a [Option<Box<InodeView>>], chunks: &'a [Chunk]) -> Self {
        Self {
            dirs,
            chunks,
            ci: 0,
            off: 0,
        }
    }
}

impl<'a> Iterator for PackedIter<'a> {
    type Item = ChildItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let chunk = self.chunks.get(self.ci)?;
            if self.off >= chunk.used() {
                self.ci += 1;
                self.off = 0;
                continue;
            }
            let off = self.off;
            self.off += PackedChildren::record_len(chunk.buf(), off);
            return Some(PackedChildren::packed_item(self.dirs, chunk, off));
        }
    }
}
