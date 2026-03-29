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

use crate::master::meta::inode::{InodePtr, InodeView};
use curvine_common::state::ListOptions;
use glob::Pattern;
use orpc::{err_box, CommonResult};
use std::collections::btree_map::{Entry, Iter as BTreeIter};
use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Debug, Clone)]
pub enum InodeChildren {
    Map(BTreeMap<String, Box<InodeView>>),
}

#[derive(Clone, Copy)]
pub struct NamedInodeView<'a> {
    pub name: &'a str,
    pub inode: &'a InodeView,
}

impl InodeChildren {
    pub fn new_map() -> Self {
        InodeChildren::Map(BTreeMap::new())
    }

    /// Get children matching glob pattern (e.g., "*.txt", "dir*")
    pub fn get_child_by_glob_pattern<'a>(
        &'a self,
        glob_pattern: &'a Pattern,
    ) -> Option<Vec<NamedInodeView<'a>>> {
        match self {
            InodeChildren::Map(map) => {
                let mut matches = Vec::new();
                for (name, child) in map {
                    if glob_pattern.matches(name) {
                        matches.push(NamedInodeView {
                            name,
                            inode: child.as_ref(),
                        });
                    }
                }
                Some(matches)
            }
        }
    }

    pub fn get_child_ptr_by_glob_pattern(&self, glob_pattern: &Pattern) -> Option<Vec<InodePtr>> {
        self.get_child_by_glob_pattern(glob_pattern)
            .map(|children| children.iter().map(|child| InodePtr::from_ref(child.inode)).collect())
    }

    pub fn get_child(&self, name: &str) -> Option<&InodeView> {
        match self {
            InodeChildren::Map(map) => map.get(name).map(|x| x.as_ref()),
        }
    }

    pub fn get_child_ptr(&self, name: &str) -> Option<InodePtr> {
        self.get_child(name).map(InodePtr::from_ref)
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let removed = match self {
            InodeChildren::Map(map) => map.remove(child_name),
        };

        match removed {
            None => err_box!("Child {} not exists", child_name),
            Some(r) => {
                if r.id() != child_id {
                    err_box!(
                        "Inode status error, expect id {}, actually delete {}",
                        child_id,
                        r.id()
                    )
                } else {
                    Ok(*r)
                }
            }
        }
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<NamedInodeView<'_>> {
        match self {
            InodeChildren::Map(map) => {
                let range = opts.start_after.as_ref().map_or(
                    map.range::<str, _>((Bound::Unbounded, Bound::Unbounded)),
                    |a| map.range::<str, _>((Bound::Excluded(a.as_str()), Bound::Unbounded)),
                );
                let n = opts.limit.unwrap_or(usize::MAX);
                range
                    .take(n)
                    .map(|(name, inode)| NamedInodeView {
                        name,
                        inode: inode.as_ref(),
                    })
                    .collect()
            }
        }
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        match inode {
            InodeView::File(name, file) => {
                self.add_child_with_name(&name.clone(), InodeView::File(name, file))
            }
            InodeView::Dir(name, dir) => {
                self.add_child_with_name(&name.clone(), InodeView::Dir(name, dir))
            }
            InodeView::FileEntry(_) => unreachable!("FileEntry cannot be added without an edge name"),
        }
    }

    pub fn add_child_with_name(&mut self, child_name: &str, inode: InodeView) -> CommonResult<InodePtr> {
        let inode = Box::new(inode);
        match self {
            InodeChildren::Map(map) => match map.entry(child_name.to_owned()) {
                Entry::Vacant(v) => {
                    if inode.is_file() {
                        v.insert(Box::new(InodeView::FileEntry(inode.id())));
                        Ok(InodePtr::from_owned(*inode))
                    } else {
                        let inserted = v.insert(inode);
                        Ok(InodePtr::from_ref(inserted.as_ref()))
                    }
                }
                Entry::Occupied(_) => err_box!("Child {} already exists", child_name),
            },
        }
    }

    pub fn iter(&self) -> ChildrenIter<'_> {
        match self {
            InodeChildren::Map(map) => ChildrenIter {
                len: map.len(),
                inner: map.iter(),
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InodeChildren::Map(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for InodeChildren {
    fn default() -> Self {
        Self::new_map()
    }
}

pub struct ChildrenIter<'a> {
    len: usize,
    inner: BTreeIter<'a, String, Box<InodeView>>,
}

impl ChildrenIter<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> Iterator for ChildrenIter<'a> {
    type Item = NamedInodeView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(name, inode)| NamedInodeView {
            name,
            inode: inode.as_ref(),
        })
    }
}
