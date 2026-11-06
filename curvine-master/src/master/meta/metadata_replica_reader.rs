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

//! Lock-free-at-the-filesystem-level namespace topology reader.
//!
//! The reader follows the canonical per-directory child indexes. It returns
//! only owned topology metadata and version tokens; in particular, no
//! [`InodePtr`](crate::master::meta::inode::InodePtr) can outlive a directory
//! read guard through this API.

use crate::master::meta::inode::{DirectoryChildren, DirectoryChildrenSharedVersion, InodeView};
use curvine_core_error::CommonResult;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A path resolved through shared directory handles. It is usable only after
/// [`MetadataReplicaReader::validate`] succeeds.
pub(crate) struct MetadataReplicaPath {
    component_count: usize,
    resolved_components: usize,
    epoch: u64,
    versions: Vec<DirectoryChildrenSharedVersion>,
}

impl MetadataReplicaPath {
    pub(crate) fn is_full(&self) -> bool {
        self.resolved_components == self.component_count
    }
}

/// Owns the stable root handle for topology-only reads. Root replacement uses
/// an epoch so no path from a pre-restore tree can validate afterwards.
pub(crate) struct MetadataReplicaReader {
    root: RwLock<Arc<DirectoryChildren>>,
    epoch: AtomicU64,
}

impl MetadataReplicaReader {
    pub(crate) fn new(root: &InodeView) -> CommonResult<Self> {
        Ok(Self {
            root: RwLock::new(root.as_dir_ref()?.children_handle()),
            epoch: AtomicU64::new(0),
        })
    }

    pub(crate) fn replace_root(&self, root: &InodeView) -> CommonResult<()> {
        let directory = root.as_dir_ref()?.children_handle();
        self.epoch.fetch_add(1, Ordering::AcqRel);
        *self.root.write() = directory;
        self.epoch.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub(crate) fn resolve(&self, path: &str) -> CommonResult<MetadataReplicaPath> {
        let components = InodeView::path_components(path)?;
        let (mut directory, epoch) = self.root_at_epoch();
        let mut resolved_components = 1;
        let mut versions = Vec::with_capacity(components.len().saturating_sub(1));

        for name in components.iter().skip(1) {
            let (child, version) = directory.read_child_shared(name);
            versions.push(version);
            let Some(child) = child else {
                break;
            };
            resolved_components += 1;
            if resolved_components == components.len() || !child.is_dir() {
                break;
            }
            directory = child.as_dir_ref()?.children_handle();
        }

        Ok(MetadataReplicaPath {
            component_count: components.len(),
            resolved_components,
            epoch,
            versions,
        })
    }

    pub(crate) fn validate(&self, path: &MetadataReplicaPath) -> bool {
        let epoch = self.epoch.load(Ordering::Acquire);
        epoch.is_multiple_of(2)
            && epoch == path.epoch
            && path.versions.iter().all(|version| version.is_current())
    }

    fn root_at_epoch(&self) -> (Arc<DirectoryChildren>, u64) {
        loop {
            let epoch = self.epoch.load(Ordering::Acquire);
            if !epoch.is_multiple_of(2) {
                std::thread::yield_now();
                continue;
            }
            let root = self.root.read().clone();
            if self.epoch.load(Ordering::Acquire) == epoch {
                return (root, epoch);
            }
        }
    }
}
