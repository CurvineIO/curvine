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

use crate::master::meta::inode::{InodeView, ROOT_INODE_ID};
use crate::master::meta::store::InodeStoreSnapshot;
use crate::master::meta::FsDir;
use curvine_core_error::{err_box, CommonResult};

/// An owned path view from one RocksDB snapshot. It is not a write lock:
/// writers must revalidate its edges after acquiring their object locks.
pub struct StoreResolvedPath {
    components: Vec<String>,
    inodes: Vec<InodeView>,
}

impl StoreResolvedPath {
    pub fn is_full(&self) -> bool {
        self.components.len() == self.inodes.len()
    }

    pub fn components(&self) -> &[String] {
        &self.components
    }

    pub fn inodes(&self) -> &[InodeView] {
        &self.inodes
    }

    /// Compare path edges, including the first missing edge. Callers must also
    /// validate the restore epoch and reread inode contents under their locks.
    pub fn same_resolution(&self, other: &Self) -> bool {
        self.components == other.components
            && self.inodes.len() == other.inodes.len()
            && self
                .inodes
                .iter()
                .zip(&other.inodes)
                .all(|(a, b)| a.id() == b.id())
    }

    pub fn target(&self) -> Option<&InodeView> {
        self.is_full().then(|| self.inodes.last()).flatten()
    }

    pub fn parent(&self) -> Option<&InodeView> {
        self.components
            .len()
            .checked_sub(2)
            .and_then(|i| self.inodes.get(i))
    }
}

impl InodeStoreSnapshot<'_> {
    /// Resolve every edge and inode from this snapshot, retaining partial paths
    /// exactly as `InodePath::resolve` does. Dangling edges are errors.
    pub fn resolve_path(&self, path: &str) -> CommonResult<StoreResolvedPath> {
        let components = InodeView::path_components(path)?;
        if components.last().is_none_or(String::is_empty) {
            return err_box!("Path {} is invalid", path);
        }
        let root = self
            .get_inode(ROOT_INODE_ID)?
            .unwrap_or_else(FsDir::create_root);
        let mut inodes = Vec::with_capacity(components.len());
        inodes.push(root);
        for component in components.iter().skip(1) {
            let parent = inodes.last().expect("resolved path contains root");
            if !parent.is_dir() {
                break;
            }
            match self.get_child(parent.id(), component)? {
                Some(child) => inodes.push(child),
                None => break,
            }
        }
        Ok(StoreResolvedPath { components, inodes })
    }
}
