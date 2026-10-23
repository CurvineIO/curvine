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

#[derive(Clone, Copy)]
pub enum ChildItem<'a> {
    View(&'a InodeView),
    FileEntry { name: &'a str, id: i64 },
}

impl<'a> ChildItem<'a> {
    #[inline]
    pub fn name(&self) -> &'a str {
        match self {
            ChildItem::View(view) => view.name(),
            ChildItem::FileEntry { name, .. } => name,
        }
    }

    #[inline]
    pub fn id(&self) -> i64 {
        match self {
            ChildItem::View(view) => view.id(),
            ChildItem::FileEntry { id, .. } => *id,
        }
    }

    #[inline]
    pub fn view(&self) -> Option<&'a InodeView> {
        match self {
            ChildItem::View(view) => Some(view),
            ChildItem::FileEntry { .. } => None,
        }
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        matches!(self, ChildItem::View(InodeView::Dir(_)))
    }

    #[inline]
    pub fn is_file_entry(&self) -> bool {
        matches!(self, ChildItem::FileEntry { .. })
    }

    #[inline]
    pub fn into_view(self) -> InodeView {
        match self {
            ChildItem::View(view) => view.clone(),
            ChildItem::FileEntry { name, id } => InodeView::new_entry(name.to_string(), id),
        }
    }

    #[inline]
    pub fn as_ptr(self) -> InodePtr {
        match self {
            ChildItem::View(view) => InodePtr::from_ref(view),
            ChildItem::FileEntry { name, id } => {
                InodePtr::from_owned(InodeView::new_entry(name.to_string(), id))
            }
        }
    }
}
