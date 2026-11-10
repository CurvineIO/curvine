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

use crate::master::meta::feature::{AclFeature, DirFeature};
use crate::master::meta::inode::ChildAddResult;
use crate::master::meta::inode::DirectoryChildren;
use crate::master::meta::inode::{Inode, InodeFile, InodeView, EMPTY_PARENT_ID};
use arc_swap::ArcSwap;
use curvine_codec::{CodecResult, Decode, Encode, Reader, Writer};
use curvine_core_error::{CommonError, CommonResult};
use curvine_model::{
    DirectoryAttributeDelta, DirectoryAttributes, ListOptions, MkdirOpts, StoragePolicy,
    INTERNAL_CTIME_XATTR,
};
use glob::Pattern;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub(crate) struct DirectoryAttributeState {
    update_lock: Mutex<()>,
    initialized: AtomicBool,
    persisted: AtomicBool,
    mtime: AtomicI64,
    ctime: AtomicI64,
    nlink: AtomicU32,
    children: AtomicI64,
    version: AtomicU64,
    base: OnceLock<DirectoryAttributes>,
}

impl Default for DirectoryAttributeState {
    fn default() -> Self {
        Self {
            update_lock: Mutex::new(()),
            initialized: AtomicBool::new(false),
            persisted: AtomicBool::new(false),
            mtime: AtomicI64::new(0),
            ctime: AtomicI64::new(0),
            nlink: AtomicU32::new(0),
            children: AtomicI64::new(-1),
            version: AtomicU64::new(0),
            base: OnceLock::new(),
        }
    }
}

impl DirectoryAttributeState {
    pub(crate) fn new(attributes: DirectoryAttributes) -> Self {
        let state = Self::default();
        state.mtime.store(attributes.mtime, Ordering::Release);
        state.ctime.store(attributes.ctime, Ordering::Release);
        state.nlink.store(attributes.nlink, Ordering::Release);
        state.children.store(
            attributes.children.map(i64::from).unwrap_or(-1),
            Ordering::Release,
        );
        state.initialized.store(true, Ordering::Release);
        state.persisted.store(false, Ordering::Release);
        state
    }

    pub(crate) fn mark_persisted(&self) {
        self.persisted.store(true, Ordering::Release);
    }

    /// Field-level mtime update on a detached DTO mirrors into an already
    /// initialized state so reads keep a single view of the value.
    pub(crate) fn set_mtime_if_initialized(&self, time: i64) {
        if self.initialized.load(Ordering::Acquire) {
            self.mtime.store(time, Ordering::Release);
        }
    }

    /// Marks the dir-attrs record as absent (external row deletion, e.g. a
    /// legacy migration test). The next delta re-carries the fixed base so
    /// the merge can rebuild the record.
    pub fn invalidate_persisted(&self) {
        let _guard = self.update_lock.lock();
        self.persisted.store(false, Ordering::Release);
        self.initialized.store(true, Ordering::Release);
    }

    /// Initializes the state in place when it is still absent (a decoded DTO
    /// that has not seen `set_directory_attributes` yet). Values and base are
    /// set together, and `persisted` stays false — the record is not in the
    /// dir-attrs CF until the first delta commits.
    pub(crate) fn init_if_absent(&self, attributes: DirectoryAttributes) {
        let _guard = self.update_lock.lock();
        if self.initialized.load(Ordering::Acquire) {
            return;
        }
        self.mtime.store(attributes.mtime, Ordering::Release);
        self.ctime.store(attributes.ctime, Ordering::Release);
        self.nlink.store(attributes.nlink, Ordering::Release);
        self.children
            .store(attributes.children.map_or(-1, i64::from), Ordering::Release);
        self.version.store(attributes.version, Ordering::Release);
        let _ = self.base.set(attributes);
        self.initialized.store(true, Ordering::Release);
    }

    pub(crate) fn current(&self) -> Option<DirectoryAttributes> {
        let _guard = self.update_lock.lock();
        self.current_locked()
    }

    fn current_locked(&self) -> Option<DirectoryAttributes> {
        self.initialized.load(Ordering::Acquire).then(|| {
            let mut attributes = DirectoryAttributes::new(
                self.mtime.load(Ordering::Acquire),
                self.ctime.load(Ordering::Acquire),
                self.nlink.load(Ordering::Acquire),
            );
            attributes.version = self.version.load(Ordering::Acquire);
            attributes.children = match self.children.load(Ordering::Acquire) {
                children if children >= 0 => i32::try_from(children).ok(),
                _ => None,
            };
            attributes
        })
    }

    fn set_persisted(&self, attributes: DirectoryAttributes) {
        let _guard = self.update_lock.lock();
        self.mtime.store(attributes.mtime, Ordering::Release);
        self.ctime.store(attributes.ctime, Ordering::Release);
        self.nlink.store(attributes.nlink, Ordering::Release);
        self.children
            .store(attributes.children.map_or(-1, i64::from), Ordering::Release);
        self.version.store(attributes.version, Ordering::Release);
        let _ = self.base.set(attributes);
        self.initialized.store(true, Ordering::Release);
        self.persisted.store(true, Ordering::Release);
    }

    fn metadata_copy(&self) -> Self {
        let _guard = self.update_lock.lock();
        let attributes = self.current_locked();
        let persisted = self.persisted.load(Ordering::Acquire);
        match attributes {
            Some(attributes) => Self {
                update_lock: Mutex::new(()),
                initialized: AtomicBool::new(true),
                persisted: AtomicBool::new(persisted),
                mtime: AtomicI64::new(attributes.mtime),
                ctime: AtomicI64::new(attributes.ctime),
                nlink: AtomicU32::new(attributes.nlink),
                children: AtomicI64::new(attributes.children.map_or(-1, i64::from)),
                version: AtomicU64::new(attributes.version),
                base: OnceLock::from(attributes),
            },
            None => Self::default(),
        }
    }

    fn delta(
        &self,
        fallback: DirectoryAttributes,
        delta: DirectoryAttributeDelta,
    ) -> DirectoryAttributeDelta {
        if self.persisted.load(Ordering::Acquire) {
            delta
        } else {
            delta.with_base(*self.base.get_or_init(|| fallback))
        }
    }

    fn apply(&self, delta: DirectoryAttributeDelta) -> CommonResult<()> {
        let _guard = self.update_lock.lock();
        let nlink = self
            .nlink
            .load(Ordering::Acquire)
            .checked_add_signed(delta.nlink_delta)
            .ok_or_else(|| CommonError::from("directory nlink update overflow"))?;
        let children = self.children.load(Ordering::Acquire);
        let children = match (children, delta.children_delta) {
            (children, Some(change)) if children >= 0 => children
                .checked_add(i64::from(change))
                .filter(|children| *children >= 0 && *children <= i64::from(i32::MAX))
                .ok_or_else(|| CommonError::from("directory child count update overflow"))?,
            (children, _) => children,
        };
        let version = self
            .version
            .load(Ordering::Acquire)
            .checked_add(delta.version_delta)
            .ok_or_else(|| CommonError::from("directory attributes version overflow"))?;
        self.nlink.store(nlink, Ordering::Release);
        self.children.store(children, Ordering::Release);
        // All accesses to these state timestamps hold update_lock.
        self.mtime.store(
            self.mtime.load(Ordering::Relaxed).max(delta.mtime),
            Ordering::Relaxed,
        );
        self.ctime.store(
            self.ctime.load(Ordering::Relaxed).max(delta.ctime),
            Ordering::Relaxed,
        );
        self.version.store(version, Ordering::Release);
        Ok(())
    }
}

/// Directory metadata replaced only by non-hot-path updates. Hot directory
/// timestamps, link count, and child count are owned by
/// [`DirectoryAttributeState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirMeta {
    pub(crate) parent_id: i64,
    pub(crate) atime: i64,
    pub(crate) storage_policy: StoragePolicy,
    pub(crate) features: DirFeature,
}

/// Live directory node. `id` and `children` are stable for the node's
/// lifetime. A reader that combines `meta` and `attributes` must hold the
/// directory inode lock until both values have been read.
#[derive(Debug)]
pub struct DirNode {
    pub(crate) id: i64,
    pub(crate) children: Arc<DirectoryChildren>,
    pub(crate) attributes: Arc<DirectoryAttributeState>,
    pub(crate) meta: ArcSwap<DirMeta>,
}

impl DirNode {
    /// Marks the dir-attrs record as absent (external row deletion, e.g. a
    /// legacy migration test). The next delta re-carries the fixed base so
    /// the merge can rebuild the record.
    pub fn invalidate_persisted(&self) {
        self.attributes.invalidate_persisted();
    }

    pub(crate) fn meta(&self) -> Arc<DirMeta> {
        self.meta.load_full()
    }

    // ---- live mutation API ----
    // Publication contract (S3-B): the caller commits the store batch first,
    // then calls these under the directory inode write lock. A publication
    // error must go through `fail_metadata_publication` at the call site.
    // Attribute changes apply one `DirectoryAttributeDelta` atomically (same
    // merge semantics as the RocksDB dir-attr operator); there are no
    // per-field writers.

    /// Applies one attribute delta to the live state.
    pub(crate) fn apply_attribute_delta(&self, delta: DirectoryAttributeDelta) -> CommonResult<()> {
        self.attributes.apply(delta)
    }

    /// Installs persisted attribute values (restore / store callback).
    pub(crate) fn set_attribute_values(&self, attributes: DirectoryAttributes) {
        self.attributes.set_persisted(attributes);
    }

    pub(crate) fn mark_attributes_persisted(&self) {
        self.attributes.mark_persisted();
    }

    /// Live attribute snapshot for delta baselines. The constructor
    /// invariant guarantees initialization; a missing value is a bug, not a
    /// fallback.
    pub(crate) fn directory_attributes(&self) -> DirectoryAttributes {
        self.attributes
            .current()
            .expect("DirNode attributes must be initialized")
    }

    pub(crate) fn directory_attribute_delta(
        &self,
        delta: DirectoryAttributeDelta,
    ) -> DirectoryAttributeDelta {
        self.attributes.delta(self.directory_attributes(), delta)
    }

    /// Adds a child edge after the store batch committed. Must be called
    /// under the directory inode write lock.
    pub fn add_child_edge(&self, mut inode: InodeView) -> CommonResult<ChildAddResult> {
        inode.set_parent_id(self.id);
        self.children.add_child(inode)
    }

    /// Removes a child edge after the store batch committed. Must be called
    /// under the directory inode write lock.
    pub(crate) fn delete_child_edge(
        &self,
        child_id: i64,
        child_name: &str,
    ) -> CommonResult<InodeView> {
        self.children.delete_child(child_id, child_name)
    }

    pub(crate) fn store_meta(&self, meta: DirMeta) {
        self.meta.store(Arc::new(meta));
    }

    /// Returns the live node to retain in a directory edge. A moved directory
    /// already carries its node; publish the staged metadata before reusing it.
    pub(crate) fn for_edge(inode: &InodeView) -> Arc<Self> {
        let dir = inode
            .as_dir_ref()
            .expect("DirNode::for_edge requires a directory");
        if let Some(node) = &dir.node {
            node.store_meta(dir.to_dir_meta());
            node.clone()
        } else {
            Self::from_view(inode)
        }
    }

    /// Builds the live node for a fresh directory (mkdir / root).
    ///
    /// Invariant: `attributes` must already be initialized (mtime/ctime/
    /// nlink seeded from the persisted record, or from the `InodeDir` value
    /// when no dir-attr row exists — persisted=false, base set to the same
    /// value). Projection reads panic rather than silently reporting zeros.
    /// Builds the live node for an `InodeView` edge arriving at a tree
    /// boundary (mkdir / restore / replay). Attributes are initialized from
    /// the record's persisted values (or its plain fields when no dir-attr
    /// state exists), so the constructor invariant holds.
    pub fn from_view(view: &InodeView) -> Arc<Self> {
        let dir = match view {
            InodeView::Dir(named) => &named.dir,
            other => panic!("from_view requires a directory, got {:?}", other.is_dir()),
        };
        // The state Arc is the single owner of persisted/base semantics; it
        // must be the very Arc the view carries, never a fresh copy. A decoded
        // DTO carries an uninitialized state: initialize it in place from the
        // record fields, keeping persisted=false.
        dir.attributes
            .init_if_absent(DirectoryAttributes::new(dir.mtime, dir.ctime(), dir.nlink));
        Arc::new(Self::from_parts(dir.id, dir, dir.attributes.clone()))
    }

    pub(crate) fn from_parts(
        id: i64,
        dir: &InodeDir,
        attributes: Arc<DirectoryAttributeState>,
    ) -> Self {
        debug_assert!(
            attributes.current().is_some(),
            "DirNode attributes must be initialized before construction"
        );
        Self {
            id,
            children: dir.children_handle(),
            attributes,
            meta: ArcSwap::from_pointee(dir.to_dir_meta()),
        }
    }
}

impl InodeDir {
    /// Projects a live `DirNode` plus its edge name into a detached
    /// persistence DTO for store/journal/list boundaries. Every field is a
    /// snapshot value: `attributes` is a freshly initialized state copied
    /// from the live values, not the live `Arc`, so calling attribute
    /// writers on the projection cannot reach the tree. Directory writes go
    /// through [`DirNode`]. The attributes state must be initialized
    /// (constructor invariant), so a missing value is a bug, not a fallback.
    /// Live-children projection: topology readers (snapshot walk, recursive
    /// free/set_attr, trash purge) keep the shared child index. Writes on
    /// the projection still never reach the tree; live writes go through
    /// `DirNode`.
    pub(crate) fn from_dir_node_live(name: String, node: Arc<DirNode>) -> InodeView {
        Self::project(name, node, true, None)
    }

    /// Persistence DTO: detached children and attribute state.
    pub(crate) fn from_dir_node(name: String, node: Arc<DirNode>) -> InodeView {
        Self::project(name, node, false, None)
    }

    fn project(
        name: String,
        node: Arc<DirNode>,
        live_children: bool,
        fallback: Option<DirectoryAttributes>,
    ) -> InodeView {
        let meta = node.meta();
        node.attributes.init_if_absent(
            fallback.unwrap_or_else(|| DirectoryAttributes::new(meta.atime, meta.atime, 2)),
        );
        let attributes = node
            .attributes
            .current()
            .expect("DirNode attributes must be initialized");
        let projected = DirectoryAttributeState::new(attributes);
        // The marker inside meta.features is the persisted snapshot and is
        // kept as-is; Inode::ctime() on this projection reads
        // attributes.ctime via the state, never derived from atime.
        let dir = InodeDir {
            id: node.id,
            parent_id: meta.parent_id,
            mtime: attributes.mtime,
            atime: meta.atime,
            nlink: attributes.nlink,
            storage_policy: meta.storage_policy.clone(),
            features: meta.features.clone(),
            children: if live_children {
                node.children.clone()
            } else {
                Arc::new(DirectoryChildren::new())
            },
            attributes: Arc::new(projected),
            node: live_children.then(|| node.clone()),
        };
        InodeView::new_dir(name, dir)
    }
}

/// Persisted directory record shape used by store and journal boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeDir {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,

    pub(crate) features: DirFeature,

    #[serde(skip)]
    children: Arc<DirectoryChildren>,

    #[serde(skip, default)]
    attributes: Arc<DirectoryAttributeState>,

    /// Live-node backlink set only by the live projection
    /// ([`InodeDir::from_dir_node_live`]); persistence DTOs leave it empty.
    /// Namespace state reads/writes must go through this handle when set.
    #[serde(skip, default)]
    pub(crate) node: Option<Arc<DirNode>>,
}

impl InodeDir {
    /// Codec-boundary view of the mutable metadata. Mirror of `DirMeta` for
    /// the record's codec fields; store and journal boundaries convert via
    /// [`Self::to_dir_meta`] / [`Self::with_dir_meta`].
    pub(crate) fn to_dir_meta(&self) -> DirMeta {
        DirMeta {
            parent_id: self.parent_id,
            atime: self.atime,
            storage_policy: self.storage_policy.clone(),
            features: self.features.clone(),
        }
    }

    /// Copy persisted directory fields. The copy gets a fresh, empty child
    /// index: children live in the edges, not the inode record, and the
    /// caller must adopt the live handle before publishing the copy.
    pub(crate) fn clone_metadata(&self) -> Self {
        let Self {
            id,
            parent_id,
            mtime,
            atime,
            nlink,
            storage_policy,
            features,
            children: _,
            attributes: _,
            node: _,
        } = self;
        Self {
            id: *id,
            parent_id: *parent_id,
            mtime: *mtime,
            atime: *atime,
            nlink: *nlink,
            storage_policy: storage_policy.clone(),
            features: features.clone(),
            children: Arc::new(DirectoryChildren::new()),
            attributes: Arc::new(self.attributes.metadata_copy()),
            node: None,
        }
    }

    pub fn new(id: i64, time: i64) -> Self {
        Self::with_children(id, time, Arc::new(DirectoryChildren::new()))
    }

    pub fn with_children(id: i64, time: i64, children: Arc<DirectoryChildren>) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: Default::default(),
            features: Default::default(),
            children,
            attributes: Arc::new(DirectoryAttributeState::new(DirectoryAttributes::new(
                time, time, 2,
            ))),
            node: None,
        }
    }

    pub fn with_opts(id: i64, time: i64, opts: MkdirOpts) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: opts.storage_policy,
            features: DirFeature {
                acl: AclFeature {
                    mode: opts.mode,
                    owner: opts.owner,
                    group: opts.group,
                },
                x_attr: opts.x_attr,
            },
            children: Arc::new(DirectoryChildren::new()),
            attributes: Arc::new(DirectoryAttributeState::new(DirectoryAttributes::new(
                time, time, 2,
            ))),
            node: None,
        }
    }

    /// Live attribute snapshot. The attribute state is the single live
    /// authority once initialized (S3-B); plain fields remain only for the
    /// persistence DTO projection.
    pub(crate) fn directory_attributes(&self) -> DirectoryAttributes {
        if let Some(current) = self.attributes.current() {
            return current;
        }
        DirectoryAttributes::new(self.mtime, self.ctime(), self.nlink)
    }

    pub(crate) fn directory_attribute_delta(
        &self,
        delta: DirectoryAttributeDelta,
    ) -> DirectoryAttributeDelta {
        self.attributes.delta(self.directory_attributes(), delta)
    }

    pub(crate) fn mark_directory_attributes_persisted(&self) {
        self.attributes.persisted.store(true, Ordering::Release);
    }

    pub(crate) fn apply_directory_attribute_delta(
        &self,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        self.attributes.apply(delta)
    }

    pub(crate) fn set_directory_attributes(&self, attributes: DirectoryAttributes) {
        self.attributes.set_persisted(attributes);
    }

    /// Add a word node and return a reference to that word node.
    pub fn add_child(&mut self, mut inode: InodeView) -> CommonResult<ChildAddResult> {
        inode.set_parent_id(self.id);
        self.children.add_child(inode)
    }

    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodeView> {
        self.children.get_child_by_glob_pattern(glob_pattern)
    }

    pub fn update_mtime(&mut self, time: i64) {
        if time > self.mtime {
            self.mtime = time;
            self.update_ctime(time);
            // A detached DTO owns its state copy: field-level updates must
            // flow into the initialized projection or reads drift behind.
            self.attributes.set_mtime_if_initialized(time);
        }
    }

    pub fn update_ctime(&mut self, time: i64) {
        if time > self.ctime() {
            self.features.x_attr.insert(
                INTERNAL_CTIME_XATTR.to_string(),
                time.to_le_bytes().to_vec(),
            );
        }
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        self.children.delete_child(child_id, child_name)
    }

    /// Shared child index handle for versioned readers.
    pub fn children_handle(&self) -> Arc<DirectoryChildren> {
        self.children.clone()
    }

    pub fn print_child(&self) {
        for child in self.children_vec() {
            let t = if child.is_dir() { "dir" } else { "file" };

            println!("{} {} {}", t, child.id(), child.name());
        }
    }

    pub fn children_iter(&self) -> impl Iterator<Item = InodeView> {
        self.children_vec().into_iter()
    }

    pub fn list_options(&self, options: &ListOptions) -> Vec<InodeView> {
        self.children.list_options(options)
    }

    pub fn children_vec(&self) -> Vec<InodeView> {
        self.children.read_all()
    }

    pub(crate) fn apply_nlink_delta(&mut self, delta: i32, ctime: i64) -> CommonResult<()> {
        if delta > 0 {
            self.nlink = self
                .nlink
                .checked_add(delta as u32)
                .ok_or_else(|| CommonError::from("directory nlink overflow"))?;
        } else {
            self.nlink = self.nlink.saturating_sub(delta.unsigned_abs());
        }
        self.update_ctime(ctime);
        Ok(())
    }

    pub fn children_len(&self) -> usize {
        self.children.iter_len()
    }

    pub fn add_file_child(&mut self, name: &str, file: InodeFile) -> CommonResult<ChildAddResult> {
        self.add_child(InodeView::new_file(name.to_string(), file))
    }

    pub fn add_dir_child(&mut self, name: &str, dir: InodeDir) -> CommonResult<ChildAddResult> {
        self.add_child(InodeView::new_dir(name.to_string(), dir))
    }
}

impl InodeDir {
    /// ctime carried by the persisted codec marker, if present. `set_attr`
    /// refreshes this marker (normalize_internal_x_attrs), so it is the
    /// requested ctime of the mutation, independent of the live state.
    pub(crate) fn marker_ctime(&self) -> Option<i64> {
        self.features
            .x_attr
            .get(INTERNAL_CTIME_XATTR)
            .and_then(|bytes| bytes.as_slice().try_into().ok())
            .map(i64::from_le_bytes)
    }
}

impl Inode for InodeDir {
    fn id(&self) -> i64 {
        self.id
    }

    fn parent_id(&self) -> i64 {
        self.parent_id
    }

    fn is_dir(&self) -> bool {
        true
    }

    fn nlink(&self) -> u32 {
        // Live authority is the attribute state (dec/inc update it); the
        // field is the codec value of a detached DTO.
        if let Some(current) = self.attributes.current() {
            return current.nlink;
        }
        self.nlink
    }

    fn mtime(&self) -> i64 {
        if let Some(current) = self.attributes.current() {
            return current.mtime;
        }
        self.mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }

    fn ctime(&self) -> i64 {
        // Live authority is the attribute state; the codec marker is only a
        // persisted snapshot used when the state is absent (pure DTO).
        if let Some(current) = self.attributes.current() {
            return current.ctime;
        }
        self.features
            .x_attr
            .get(INTERNAL_CTIME_XATTR)
            .and_then(|bytes| bytes.as_slice().try_into().ok())
            .map(i64::from_le_bytes)
            .unwrap_or(self.mtime)
    }
}

impl PartialEq for InodeDir {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Encode for InodeDir {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i64(self.id)?;
        writer.put_i64(self.parent_id)?;
        writer.put_i64(self.mtime)?;
        writer.put_i64(self.atime)?;
        writer.put_u32(self.nlink)?;
        self.storage_policy.encode(writer)?;
        self.features.encode(writer)
    }
}

impl Decode for InodeDir {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            id: reader.get_i64()?,
            parent_id: reader.get_i64()?,
            mtime: reader.get_i64()?,
            atime: reader.get_i64()?,
            nlink: reader.get_u32()?,
            storage_policy: StoragePolicy::decode(reader)?,
            features: DirFeature::decode(reader)?,
            // Children are persisted as edges, not inside the inode record.
            children: Arc::new(DirectoryChildren::new()),
            node: None,
            attributes: Arc::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{StorageState, StorageType, TtlAction};
    use std::collections::HashMap;

    #[test]
    fn inode_dir_round_trips_all_fields() {
        let dir = InodeDir {
            id: 101,
            parent_id: 7,
            mtime: 1_000,
            atime: 2_000,
            nlink: 3,
            storage_policy: StoragePolicy {
                storage_type: StorageType::Disk,
                ttl_ms: 60_000,
                ttl_action: TtlAction::Delete,
                ufs_mtime: 1_234,
                state: StorageState::Cv,
            },
            features: DirFeature {
                x_attr: HashMap::from([("user.k".to_string(), vec![1u8, 2, 3])]),
                acl: AclFeature {
                    owner: "owner".to_string(),
                    group: "group".to_string(),
                    mode: 0o755,
                },
            },
            children: Arc::new(DirectoryChildren::new()),
            attributes: Arc::default(),
            node: None,
        };

        let mut writer = Writer::new(1).unwrap();
        dir.encode(&mut writer).unwrap();
        let bytes = writer.into_inner();
        let mut reader = Reader::new(&bytes, 1).unwrap();
        let decoded = InodeDir::decode(&mut reader).unwrap();
        assert!(reader.is_empty());

        assert_eq!(decoded.id, dir.id);
        assert_eq!(decoded.parent_id, dir.parent_id);
        assert_eq!(decoded.mtime, dir.mtime);
        assert_eq!(decoded.atime, dir.atime);
        assert_eq!(decoded.nlink, dir.nlink);
        assert_eq!(decoded.storage_policy, dir.storage_policy);
        assert_eq!(decoded.features.x_attr, dir.features.x_attr);
        assert_eq!(decoded.features.acl.owner, dir.features.acl.owner);
        assert_eq!(decoded.features.acl.group, dir.features.acl.group);
        assert_eq!(decoded.features.acl.mode, dir.features.acl.mode);
        // Children are stored as edges outside the inode record, never encoded.
        assert_eq!(decoded.children_len(), 0);
    }
}
