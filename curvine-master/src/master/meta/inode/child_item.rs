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

use crate::master::meta::inode::{DirNode, InodeView};
use std::sync::Arc;

/// One resolved child edge. Directory children carry their stable
/// [`DirNode`](crate::master::meta::inode::DirNode) handle; file children are
/// either a borrowed view (List/Map containers) or an inline name+id edge
/// (Packed). The borrow variant exists only for containers that still store
/// file views and disappears once those containers are directory-node only.
#[derive(Clone, Copy)]
pub enum ChildItem<'a> {
    View(&'a InodeView),
    DirNode {
        name: &'a str,
        node: &'a Arc<DirNode>,
    },
    FileEntry {
        name: &'a str,
        id: i64,
    },
}

impl<'a> ChildItem<'a> {
    #[inline]
    pub fn name(&self) -> &'a str {
        match self {
            ChildItem::View(view) => view.name(),
            ChildItem::DirNode { name, .. } => name,
            ChildItem::FileEntry { name, .. } => name,
        }
    }

    /// Whether the edge is a directory (a live node handle or a full
    /// `InodeView` directory in the borrow variant).
    #[inline]
    pub fn is_dir(&self) -> bool {
        match self {
            ChildItem::DirNode { .. } => true,
            ChildItem::View(view) => view.is_dir(),
            ChildItem::FileEntry { .. } => false,
        }
    }

    #[inline]
    pub fn id(&self) -> i64 {
        match self {
            ChildItem::View(view) => view.id(),
            // Immutable identity: safe to read without any lock.
            ChildItem::DirNode { node, .. } => node.id,
            ChildItem::FileEntry { id, .. } => *id,
        }
    }

    pub fn dir_node(&self) -> Option<&Arc<DirNode>> {
        match self {
            ChildItem::DirNode { node, .. } => Some(node),
            _ => None,
        }
    }

    /// Whether the edge resolves to a file (inline id or a borrowed file
    /// view). Directory edges always carry a live node handle.
    #[inline]
    pub fn is_file_entry(&self) -> bool {
        !self.is_dir()
    }

    #[inline]
    pub(crate) fn into_view(self) -> InodeView {
        match self {
            ChildItem::View(view) => view.clone(),
            ChildItem::DirNode { name, node } => {
                InodeView::from_dir_node_live(name.to_owned(), (*node).clone())
            }
            ChildItem::FileEntry { name, id } => InodeView::new_entry(name.to_owned(), id),
        }
    }
}
