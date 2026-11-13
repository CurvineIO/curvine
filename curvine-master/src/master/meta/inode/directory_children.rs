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

//! Shared, versioned child index for one directory.
//!
//! `InodeDir` owns an `Arc<DirectoryChildren>` instead of an inline
//! container: readers can hold the handle (and a version) past the point
//! where the tree moves, and validate afterwards via `version_is`. Large or
//! contended directories promote to per-name shards lazily; readers and
//! writers re-check `sharded()` after taking each fallback lock, matching the
//! Astra promotion protocol.

use super::sharded_children::{min_sharded_children, ShardedChildren};
use super::{ChildAddResult, ChildItem, DirNode, InodeChildren, InodeView};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::ListOptions;
use glob::Pattern;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// Atomically tracks a directory-wide generation and in-flight writers.
/// Different shards can mutate simultaneously, so parity alone is insufficient.
/// The low 16 bits count writers; the upper 48 bits advance on every entry and
/// exit. Overflow fails closed instead of letting an old version become valid.
#[derive(Debug, Default)]
pub(crate) struct SequenceState {
    value: AtomicU64,
}

impl SequenceState {
    const WRITER_BITS: u32 = 16;
    const WRITER_MASK: u64 = (1 << Self::WRITER_BITS) - 1;
    const GENERATION_STEP: u64 = 1 << Self::WRITER_BITS;

    pub(crate) fn begin(&self) {
        self.value
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                if value & Self::WRITER_MASK == Self::WRITER_MASK {
                    return None;
                }
                value.checked_add(Self::GENERATION_STEP + 1)
            })
            .expect("directory sequence writer count or generation overflow");
    }

    pub(crate) fn finish(&self) {
        self.value
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                if value & Self::WRITER_MASK == 0 {
                    return None;
                }
                value.checked_add(Self::GENERATION_STEP - 1)
            })
            .expect("directory sequence writer underflow or generation overflow");
    }

    pub(crate) fn version(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    pub(crate) fn is_current(&self, version: u64) -> bool {
        version & Self::WRITER_MASK == 0 && self.value.load(Ordering::Acquire) == version
    }
}

/// RAII write window: bumps the sequence on entry and again on drop so a
/// concurrent reader never observes the mutation as "unchanged".
pub(crate) struct WriteSequence<'a>(&'a SequenceState);

impl<'a> WriteSequence<'a> {
    pub(crate) fn begin(state: &'a SequenceState) -> Self {
        state.begin();
        Self(state)
    }
}

impl Drop for WriteSequence<'_> {
    fn drop(&mut self) {
        self.0.finish();
    }
}

/// Owned child-edge identity captured under the shard guard.
#[derive(Clone)]
pub enum ChildEdge {
    Dir(std::sync::Arc<DirNode>),
    FileId(i64),
}

impl ChildEdge {
    /// Immutable edge identity; safe to read without locking the child.
    pub fn id(&self) -> i64 {
        match self {
            Self::Dir(node) => node.id,
            Self::FileId(id) => *id,
        }
    }

    /// Whether the edge resolves to a live directory node.
    pub fn is_dir(&self) -> bool {
        matches!(self, Self::Dir(_))
    }
}

pub struct DirectoryChildren {
    sequence: SequenceState,
    children: RwLock<InodeChildren>,
    // Promotion changes the representation, not the names that can be
    // reloaded into the original Packed container after a restart.
    packed_names: bool,
    /// Created on first promotion; `ready` only flips inside the single-index
    /// write lock, so readers racing the promotion re-read after their
    /// fallback lock and never observe a half-filled shard table.
    shards: OnceLock<ShardedChildren>,
}

impl std::fmt::Debug for DirectoryChildren {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryChildren")
            .field("len", &self.iter_len())
            .field("sharded", &self.sharded().is_some())
            .finish()
    }
}

impl Default for DirectoryChildren {
    fn default() -> Self {
        Self::with_container(InodeChildren::default())
    }
}

enum MutationGuard<'a> {
    Single(RwLockWriteGuard<'a, InodeChildren>),
    Shard {
        children: RwLockWriteGuard<'a, InodeChildren>,
        index: usize,
        count: usize,
    },
}

enum RenameMutationGuard<'a> {
    Single(RwLockWriteGuard<'a, InodeChildren>),
    Shards {
        first: RwLockWriteGuard<'a, InodeChildren>,
        second: Option<RwLockWriteGuard<'a, InodeChildren>>,
        shard_count: usize,
        first_index: usize,
        second_index: Option<usize>,
    },
}

/// Write window for a same-directory rename. Same-shard renames take one
/// lock; cross-shard renames take both in shard-index ascending order, so
/// two opposite renames can never form an ABBA cycle.
///
/// Write window over one directory's child index: holds the index write lock
/// plus the sequence bump for the whole mutation.
pub struct DirectoryMutation<'a> {
    children: MutationGuard<'a>,
    packed_names: bool,
    _sequence: WriteSequence<'a>,
}

impl DirectoryChildren {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from an existing container (restore / memory benchmarks).
    pub fn with_container(children: InodeChildren) -> Self {
        Self {
            sequence: SequenceState::default(),
            packed_names: matches!(children, InodeChildren::Packed(_)),
            children: RwLock::new(children),
            shards: OnceLock::new(),
        }
    }

    fn sharded(&self) -> Option<&ShardedChildren> {
        self.shards.get().filter(|s| s.is_ready())
    }

    /// Optimistic single-shard read that re-checks promotion after locking.
    fn with_children_for_name<R>(&self, name: &str, f: impl Fn(&InodeChildren, &str) -> R) -> R {
        if let Some(shards) = self.sharded() {
            let shard = shards.shard(name);
            let children = shard.children.read();
            return f(&children, name);
        }
        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.with_children_for_name(name, f);
        }
        f(&children, name)
    }

    fn lock_children_for_mutation(&self) -> (RwLockWriteGuard<'_, InodeChildren>, bool) {
        match self.children.try_write() {
            Some(children) => (children, false),
            None => (self.children.write(), true),
        }
    }

    fn should_promote_to_shards(&self, child_count: usize, contended: bool) -> bool {
        contended && child_count >= min_sharded_children()
    }

    fn promote_to_shards(&self, children: &mut InodeChildren) {
        let sharded = self.shards.get_or_init(ShardedChildren::new);
        sharded.insert_children(children);
        sharded.mark_ready();
    }

    /// Pre-create shard routing for bulk restore. Concurrent or earlier
    /// insertions are migrated under the same lock as runtime promotion.
    pub fn seed_shards_for_bulk_load(&self, expected_children: usize) {
        if expected_children < min_sharded_children() || self.sharded().is_some() {
            return;
        }
        let mut children = self.children.write();
        if self.sharded().is_none() {
            if children.is_empty() {
                self.shards.get_or_init(ShardedChildren::new).mark_ready();
            } else {
                self.promote_to_shards(&mut children);
            }
        }
    }

    /// Begin a dentry mutation. Large contended directories promote to shards
    /// here; the returned guard owns one shard write lock, so independent
    /// dentries in different shards mutate concurrently once promoted.
    pub fn begin_mutation(&self, child_name: &str) -> DirectoryMutation<'_> {
        if let Some(shards) = self.sharded() {
            let index = shards.shard_index(child_name);
            let children = shards.shard_at(index).children.write();
            return DirectoryMutation {
                children: MutationGuard::Shard {
                    children,
                    index,
                    count: shards.shards.len(),
                },
                packed_names: self.packed_names,
                _sequence: WriteSequence::begin(&self.sequence),
            };
        }

        let (mut children, contended) = self.lock_children_for_mutation();
        if self.sharded().is_some() {
            drop(children);
            return self.begin_mutation(child_name);
        }
        if self.should_promote_to_shards(children.len(), contended) {
            self.promote_to_shards(&mut children);
            drop(children);
            return self.begin_mutation(child_name);
        }

        DirectoryMutation {
            children: MutationGuard::Single(children),
            packed_names: self.packed_names,
            _sequence: WriteSequence::begin(&self.sequence),
        }
    }

    /// Begin a same-directory rename of `source_name` to `destination_name`.
    /// Cross-shard locks are taken in shard-index ascending order (Astra's
    /// rename protocol); single-index directories behave like a normal
    /// mutation.
    pub fn begin_rename_mutation(
        &self,
        source_name: &str,
        destination_name: &str,
    ) -> DirectoryRenameMutation<'_> {
        if let Some(shards) = self.sharded() {
            let source_index = shards.shard_index(source_name);
            let destination_index = shards.shard_index(destination_name);
            let (first_index, second_index) = if source_index <= destination_index {
                (source_index, destination_index)
            } else {
                (destination_index, source_index)
            };
            let first = shards.shard_at(first_index).children.write();
            let second = if second_index == first_index {
                None
            } else {
                Some(shards.shard_at(second_index).children.write())
            };
            return DirectoryRenameMutation {
                children: RenameMutationGuard::Shards {
                    first,
                    second,
                    shard_count: shards.shards.len(),
                    first_index,
                    second_index: (second_index != first_index).then_some(second_index),
                },
                packed_names: self.packed_names,
                _sequence: WriteSequence::begin(&self.sequence),
            };
        }

        let (mut children, contended) = self.lock_children_for_mutation();
        if self.sharded().is_some() {
            drop(children);
            return self.begin_rename_mutation(source_name, destination_name);
        }
        if self.should_promote_to_shards(children.len(), contended) {
            self.promote_to_shards(&mut children);
            drop(children);
            return self.begin_rename_mutation(source_name, destination_name);
        }

        DirectoryRenameMutation {
            children: RenameMutationGuard::Single(children),
            packed_names: self.packed_names,
            _sequence: WriteSequence::begin(&self.sequence),
        }
    }

    /// Owned version token for readers that must validate after the walk.
    pub fn shared_version(self: &std::sync::Arc<Self>) -> DirectoryChildrenSharedVersion {
        DirectoryChildrenSharedVersion {
            children: self.clone(),
            version: self.sequence.version(),
        }
    }

    /// Reads whether a child exists with the expected id, plus the version
    /// needed to validate the check later. Avoids cloning the inode body:
    /// identity re-verification only needs the id.
    pub fn child_id_matches_version(
        &self,
        name: &str,
        expected_id: i64,
    ) -> (bool, DirectoryReadVersion<'_>) {
        self.with_children_for_name(name, |children, name| {
            let version = self.sequence.version();
            let matches = children
                .get_child_item(name)
                .is_some_and(|item| item.id() == expected_id);
            (
                matches,
                DirectoryReadVersion {
                    children: self,
                    version,
                },
            )
        })
    }

    /// Reads one child together with the version needed to validate the read
    /// later via [`Self::version_is`].
    pub fn read_child_view(&self, name: &str) -> (Option<InodeView>, DirectoryReadVersion<'_>) {
        let (view, version) = self.with_children_for_name(name, |children, name| {
            let version = self.sequence.version();
            (
                children.get_child_item(name).map(|item| item.into_view()),
                version,
            )
        });
        (
            view,
            DirectoryReadVersion {
                children: self,
                version,
            },
        )
    }

    /// Reads one child with an owned version token that remains valid after
    /// this call returns. Topology readers use it instead of retaining a raw
    /// child pointer beyond the directory read guard.
    pub fn read_child_shared(
        self: &std::sync::Arc<Self>,
        name: &str,
    ) -> (Option<InodeView>, DirectoryChildrenSharedVersion) {
        let (view, version) = self.with_children_for_name(name, |children, name| {
            (
                children.get_child_item(name).map(|item| item.into_view()),
                self.sequence.version(),
            )
        });
        (
            view,
            DirectoryChildrenSharedVersion {
                children: self.clone(),
                version,
            },
        )
    }

    /// The captured generation is valid only when no writer was active and
    /// no mutation has started or finished since it was captured.
    pub fn version_is(&self, version: &DirectoryReadVersion<'_>) -> bool {
        std::ptr::eq(version.children as *const _, self)
            && self.sequence.is_current(version.version)
    }

    pub fn add_child(&self, inode: InodeView) -> CommonResult<ChildAddResult> {
        let name = inode.name().to_owned();
        let mut mutation = self.begin_mutation(&name);
        mutation.add_child(inode)
    }

    pub(crate) fn validate_child_name(&self, name: &str) -> CommonResult<()> {
        Self::validate_name(self.packed_names, name)
    }

    fn validate_name(packed_names: bool, name: &str) -> CommonResult<()> {
        if packed_names && name.len() > super::PackedChildren::NAME_MAX {
            return err_box!("destination name is too long for packed children");
        }
        Ok(())
    }

    pub fn delete_child(&self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let mut mutation = self.begin_mutation(child_name);
        mutation.delete_child(child_id, child_name)
    }

    /// One child edge with its stable identity, captured as owned data:
    /// directory edges carry the `Arc<DirNode>` handle, file edges the
    /// inline name+id record. The guard is released before returning.
    pub fn get_child_edge(&self, name: &str) -> Option<ChildEdge> {
        self.with_children_for_name(name, |children, name| {
            children.get_child_item(name).map(|item| match item {
                ChildItem::DirNode { node, .. } => ChildEdge::Dir(node.clone()),
                ChildItem::FileEntry { id, .. } => ChildEdge::FileId(id),
                ChildItem::View(view) => ChildEdge::FileId(view.id()),
            })
        })
    }

    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodeView> {
        if self.sharded().is_none() {
            let children = self.children.read();
            // The underlying containers iterate in name order. Recheck
            // promotion under the lock before skipping the shard sort.
            if self.sharded().is_none() {
                return children.matching_views(glob_pattern);
            }
        }
        let mut out: Vec<InodeView> = self
            .read_all_guards(|children| children.matching_views(glob_pattern))
            .into_iter()
            .flatten()
            .collect();
        out.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        out
    }

    /// Returns owned entries captured while their shard read locks are held.
    pub fn list_options(&self, opts: &ListOptions) -> Vec<InodeView> {
        if opts.limit == Some(0) {
            return Vec::new();
        }
        if self.sharded().is_none() {
            let children = self.children.read();
            // Each container returns names in order; recheck promotion while
            // holding its read lock before skipping the global shard sort.
            if self.sharded().is_none() {
                return children.list_views(opts);
            }
        }
        let mut out: Vec<InodeView> = self
            .read_all_guards(|children| children.list_views(opts))
            .into_iter()
            .flatten()
            .collect();
        if let Some(limit) = opts.limit {
            if limit < out.len() {
                out.select_nth_unstable_by(limit, |left, right| left.name().cmp(right.name()));
                out.truncate(limit);
            }
        }
        out.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        out
    }

    /// Page membership as `(name, edge)` pairs without building inode
    /// projections: memory-read planning captures identity only, and the
    /// statuses are constructed after the inode read locks are held.
    pub fn child_edges(&self, opts: &ListOptions) -> Vec<(String, ChildEdge)> {
        let to_edge = |item: ChildItem<'_>| {
            let edge = match &item {
                ChildItem::DirNode { node, .. } => ChildEdge::Dir((*node).clone()),
                ChildItem::FileEntry { id, .. } => ChildEdge::FileId(*id),
                ChildItem::View(view) => ChildEdge::FileId(view.id()),
            };
            (item.name().to_owned(), edge)
        };
        if opts.limit == Some(0) {
            return Vec::new();
        }
        if self.sharded().is_none() {
            let children = self.children.read();
            // Each container returns names in order; recheck promotion while
            // holding its read lock before skipping the global shard sort.
            if self.sharded().is_none() {
                return children
                    .list_options(opts)
                    .into_iter()
                    .map(to_edge)
                    .collect();
            }
        }
        let mut out: Vec<(String, ChildEdge)> = self
            .read_all_guards(|children| {
                children
                    .list_options(opts)
                    .into_iter()
                    .map(to_edge)
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .flatten()
            .collect();
        if let Some(limit) = opts.limit {
            if limit < out.len() {
                out.select_nth_unstable_by(limit, |left, right| left.0.cmp(&right.0));
                out.truncate(limit);
            }
        }
        out.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        out
    }

    pub fn iter_len(&self) -> usize {
        if let Some(shards) = self.sharded() {
            {
                let cached = shards.count_cache.read();
                if self.sequence.is_current(cached.0) {
                    return cached.1;
                }
            }
            // The version validates both sparse optimistic counts and cached
            // counts without adding synchronization to the mutation path.
            const MAX_OPTIMISTIC_SHARDS: usize = 8;
            let version = self.sequence.version();
            if version & SequenceState::WRITER_MASK == 0 {
                let initialized = shards.shards.iter().filter_map(|shard| shard.get());
                if initialized.clone().take(MAX_OPTIMISTIC_SHARDS + 1).count()
                    <= MAX_OPTIMISTIC_SHARDS
                {
                    let count = initialized.map(|shard| shard.children.read().len()).sum();
                    if self.sequence.is_current(version) {
                        *shards.count_cache.write() = (version, count);
                        return count;
                    }
                }
            }
            let count = shards
                .read_all()
                .iter()
                .map(|children| children.len())
                .sum();
            if self.sequence.is_current(version) {
                *shards.count_cache.write() = (version, count);
            }
            return count;
        }
        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.iter_len();
        }
        children.len()
    }

    /// Owned snapshot of the children, sorted by name across shards.
    pub(crate) fn read_all(&self) -> Vec<InodeView> {
        if self.sharded().is_none() {
            let children = self.children.read();
            // The single container is already ordered; promotion must not
            // happen between this check and the snapshot.
            if self.sharded().is_none() {
                return children.all_views();
            }
        }
        let mut out: Vec<InodeView> = self
            .read_all_guards(|children| children.all_views())
            .into_iter()
            .flatten()
            .collect();
        out.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        out
    }

    /// Bounded edge enumeration for subtree planning (design §3): reads at
    /// most `max_edges + 1` edges across the whole call — the extra one only
    /// proves the budget was exceeded — with no name sorting and no full
    /// inode DTOs. Returns `Some(edges)` when the complete edge set fits the
    /// budget, `None` when it does not (never a truncated success). A
    /// zero budget succeeds only for an empty directory. Different names
    /// pointing at the same file id stay distinct edges. The caller
    /// captures `shared_version()` before enumerating; this method reads no
    /// DB and does not change the tree.
    pub(crate) fn child_edges_bounded(&self, max_edges: usize) -> Option<Vec<ChildEdge>> {
        let to_edge = |item: ChildItem<'_>| {
            let edge = match &item {
                ChildItem::DirNode { node, .. } => ChildEdge::Dir((*node).clone()),
                ChildItem::FileEntry { id, .. } => ChildEdge::FileId(*id),
                ChildItem::View(view) => ChildEdge::FileId(view.id()),
            };
            edge
        };
        let mut edges: Vec<ChildEdge> = Vec::new();
        let mut remaining = max_edges;
        let mut over = false;
        if let Some(shards) = self.sharded() {
            // One shared budget across all shards: each shard reads at most
            // `remaining + 1`, so the whole call reads at most
            // `max_edges + shard_probe_waste` where the probe waste is one
            // edge on the single shard that proves the overflow.
            for index in 0..shards.shards.len() {
                if over {
                    break;
                }
                let children = shards.shards[index]
                    .get()
                    .map(|shard| shard.children.read());
                let Some(children) = children else {
                    continue;
                };
                let taken: Vec<(String, ChildEdge)> = children
                    .list_options(&ListOptions {
                        limit: Some(remaining + 1),
                        start_after: None,
                    })
                    .into_iter()
                    .map(|item| {
                        let name = item.name().to_owned();
                        (name, to_edge(item))
                    })
                    .collect();
                drop(children);
                if taken.len() > remaining {
                    over = true;
                    break;
                }
                remaining -= taken.len();
                edges.extend(taken.into_iter().map(|(_, edge)| edge));
            }
        } else {
            let children = self.children.read();
            if self.sharded().is_some() {
                drop(children);
                return self.child_edges_bounded(max_edges);
            }
            let taken: Vec<ChildItem<'_>> = children.list_options(&ListOptions {
                limit: Some(remaining + 1),
                start_after: None,
            });
            over = taken.len() > remaining;
            if !over {
                edges.extend(taken.into_iter().map(to_edge));
            }
        }
        if over {
            None
        } else {
            Some(edges)
        }
    }

    fn read_all_guards<R>(&self, f: impl Fn(&InodeChildren) -> R) -> Vec<R> {
        if let Some(shards) = self.sharded() {
            let guards = shards.read_all();
            return guards.iter().map(|children| f(children)).collect();
        }
        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.read_all_guards(f);
        }
        vec![f(&children)]
    }
}

impl DirectoryMutation<'_> {
    fn children_for_name(&mut self, name: &str) -> CommonResult<&mut InodeChildren> {
        match &mut self.children {
            MutationGuard::Single(children) => Ok(children),
            MutationGuard::Shard {
                children,
                index,
                count,
            } => {
                if super::sharded_children::shard_index(name, *count) != *index {
                    return err_box!("mutation touches a shard without its write guard");
                }
                Ok(children)
            }
        }
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<ChildAddResult> {
        DirectoryChildren::validate_name(self.packed_names, inode.name())?;
        self.children_for_name(inode.name())?.add_child(inode)
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        self.children_for_name(child_name)?
            .delete_child(child_id, child_name)
    }
}

/// Write window over one or two shards of the same directory.
pub struct DirectoryRenameMutation<'a> {
    children: RenameMutationGuard<'a>,
    packed_names: bool,
    _sequence: WriteSequence<'a>,
}

impl DirectoryRenameMutation<'_> {
    pub fn child_view(&self, name: &str) -> Option<InodeView> {
        match &self.children {
            RenameMutationGuard::Single(children) => {
                children.get_child_item(name).map(|item| item.into_view())
            }
            RenameMutationGuard::Shards { first, second, .. } => first
                .get_child_item(name)
                .map(|item| item.into_view())
                .or_else(|| {
                    second
                        .as_ref()
                        .and_then(|children| children.get_child_item(name))
                        .map(|item| item.into_view())
                }),
        }
    }

    /// Same-window variant of [`Self::rename_child`] that inserts the fully
    /// prepared `new_view` (name/ctime/xattr already applied by the caller)
    /// instead of re-deriving it from the detached source.
    pub fn rename_to(
        &mut self,
        source_id: i64,
        source_name: &str,
        replaced: Option<(i64, &str)>,
        new_view: InodeView,
    ) -> CommonResult<()> {
        let destination_name = new_view.name().to_owned();
        if source_name == destination_name {
            return err_box!("source and destination names are identical");
        }
        if let RenameMutationGuard::Shards {
            shard_count,
            first_index,
            second_index,
            ..
        } = &self.children
        {
            let locked = |name: &str| {
                let index = super::sharded_children::shard_index(name, *shard_count);
                index == *first_index || Some(index) == *second_index
            };
            if !locked(source_name)
                || !locked(&destination_name)
                || replaced.is_some_and(|(_, name)| !locked(name))
            {
                return err_box!("rename touches a shard without its write guard");
            }
        }
        match self.child_view(source_name) {
            Some(source) if source.id() == source_id => (),
            _ => return err_box!("source edge {} changed before rename", source_name),
        }
        if let Some((replaced_id, replaced_name)) = replaced {
            match self.child_view(replaced_name) {
                Some(target) if target.id() == replaced_id => (),
                _ => return err_box!("replacement edge {} changed before rename", replaced_name),
            }
        }
        DirectoryChildren::validate_name(self.packed_names, &destination_name)?;
        if let Some(target) = self.child_view(&destination_name) {
            if !replaced.is_some_and(|(id, name)| name == destination_name && id == target.id()) {
                return err_box!("destination edge {} already exists", destination_name);
            }
        }
        match &mut self.children {
            RenameMutationGuard::Single(children) => {
                Self::apply_replace(children, source_id, source_name, replaced, new_view)
            }
            RenameMutationGuard::Shards {
                first,
                second,
                shard_count,
                first_index,
                ..
            } => {
                let source_index = super::sharded_children::shard_index(source_name, *shard_count);
                let destination_index =
                    super::sharded_children::shard_index(&destination_name, *shard_count);
                if let Some((replaced_id, replaced_name)) = replaced {
                    let replaced_index =
                        super::sharded_children::shard_index(replaced_name, *shard_count);
                    let replaced_shard: &mut InodeChildren = if replaced_index == *first_index {
                        first
                    } else {
                        second
                            .as_mut()
                            .expect("cross-shard rename must own both shard guards")
                    };
                    let _ = replaced_shard.delete_child(replaced_id, replaced_name)?;
                }
                let source_shard: &mut InodeChildren = if source_index == *first_index {
                    first
                } else {
                    second
                        .as_mut()
                        .expect("cross-shard rename must own both shard guards")
                };
                // The slot already owns the live `Arc<DirNode>`; deleting the
                // source detaches a projection carrying the same node, and
                // `add_child` reuses that backlink, so the directory keeps its
                // identity without re-attaching children.
                let _ = source_shard.delete_child(source_id, source_name)?;
                let destination_shard: &mut InodeChildren = if destination_index == *first_index {
                    first
                } else {
                    second
                        .as_mut()
                        .expect("cross-shard rename must own both shard guards")
                };
                let _ = destination_shard.add_child(new_view)?;
                Ok(())
            }
        }
    }

    fn apply_replace(
        children: &mut InodeChildren,
        source_id: i64,
        source_name: &str,
        replaced: Option<(i64, &str)>,
        new_view: InodeView,
    ) -> CommonResult<()> {
        if let Some((replaced_id, replaced_name)) = replaced {
            let _ = children.delete_child(replaced_id, replaced_name)?;
        }
        // Same identity rule as `rename_to`: the detached projection already
        // carries the slot's `Arc<DirNode>`, so no children re-attachment.
        let _ = children.delete_child(source_id, source_name)?;
        let _ = children.add_child(new_view)?;
        Ok(())
    }

    /// Moves `source_name` to `destination_name` inside this directory.
    /// `replaced` deletes the overwrite target first, mirroring the store
    /// batch order of `apply_rename`. Each touched name routes to its own
    /// shard; the guards exist so concurrent opposite renames serialize in
    /// shard-index order rather than for name lookup.
    pub fn rename_child(
        &mut self,
        source_id: i64,
        source_name: &str,
        replaced: Option<(i64, &str)>,
        destination_name: &str,
    ) -> CommonResult<()> {
        if source_name == destination_name {
            return err_box!("source and destination names are identical");
        }
        if replaced.is_some_and(|(_, name)| name != destination_name) {
            return err_box!("replacement edge does not match destination name");
        }
        if let RenameMutationGuard::Shards {
            shard_count,
            first_index,
            second_index,
            ..
        } = &self.children
        {
            let locked = |name: &str| {
                let index = super::sharded_children::shard_index(name, *shard_count);
                index == *first_index || Some(index) == *second_index
            };
            if !locked(source_name)
                || !locked(destination_name)
                || replaced.is_some_and(|(_, name)| !locked(name))
            {
                return err_box!("rename touches a shard without its write guard");
            }
        }
        // Validate both edge identities before deleting either one. A stale
        // source or replacement must not leave the directory half-mutated.
        match self.child_view(source_name) {
            Some(source) if source.id() == source_id => (),
            _ => return err_box!("source edge {} changed before rename", source_name),
        }
        if let Some((replaced_id, replaced_name)) = replaced {
            match self.child_view(replaced_name) {
                Some(target) if target.id() == replaced_id => (),
                _ => return err_box!("replacement edge {} changed before rename", replaced_name),
            }
        }
        DirectoryChildren::validate_name(self.packed_names, destination_name)?;
        if let Some(target) = self.child_view(destination_name) {
            if !replaced.is_some_and(|(id, name)| name == destination_name && id == target.id()) {
                return err_box!("destination edge {} already exists", destination_name);
            }
        }
        match &mut self.children {
            RenameMutationGuard::Single(children) => {
                Self::apply_rename(children, source_id, source_name, replaced, destination_name)
            }
            RenameMutationGuard::Shards {
                first,
                second,
                shard_count,
                first_index,
                ..
            } => {
                // Route each touched name to the shard guard this mutation
                // already owns; reads use the same hash, so the routing is
                // consistent with them.
                let source_index = super::sharded_children::shard_index(source_name, *shard_count);
                let destination_index =
                    super::sharded_children::shard_index(destination_name, *shard_count);

                // Sequential scoped borrows: each shard guard is borrowed for
                // exactly one operation, so the borrow checker sees disjoint
                // lifetimes.
                if let Some((replaced_id, replaced_name)) = replaced {
                    let replaced_index =
                        super::sharded_children::shard_index(replaced_name, *shard_count);
                    let replaced_shard: &mut InodeChildren = if replaced_index == *first_index {
                        first
                    } else {
                        second
                            .as_mut()
                            .expect("cross-shard rename must own both shard guards")
                    };
                    let _ = replaced_shard.delete_child(replaced_id, replaced_name)?;
                }

                let source_shard: &mut InodeChildren = if source_index == *first_index {
                    first
                } else {
                    second
                        .as_mut()
                        .expect("cross-shard rename must own both shard guards")
                };
                let mut destination = source_shard.delete_child(source_id, source_name)?;
                destination.change_name(destination_name.to_owned());

                let destination_shard: &mut InodeChildren = if destination_index == *first_index {
                    first
                } else {
                    second
                        .as_mut()
                        .expect("cross-shard rename must own both shard guards")
                };
                let _ = destination_shard.add_child(destination)?;
                Ok(())
            }
        }
    }

    fn apply_rename(
        children: &mut InodeChildren,
        source_id: i64,
        source_name: &str,
        replaced: Option<(i64, &str)>,
        destination_name: &str,
    ) -> CommonResult<()> {
        if let Some((replaced_id, replaced_name)) = replaced {
            let _ = children.delete_child(replaced_id, replaced_name)?;
        }
        let mut destination = children.delete_child(source_id, source_name)?;
        destination.change_name(destination_name.to_owned());
        let _ = children.add_child(destination)?;
        Ok(())
    }
}

/// Version token captured by a directory read. Borrows the handle so the
/// reader cannot validate against a different directory's sequence.
pub struct DirectoryReadVersion<'a> {
    children: &'a DirectoryChildren,
    version: u64,
}

impl DirectoryReadVersion<'_> {
    pub fn is_current(&self) -> bool {
        self.children.sequence.is_current(self.version)
    }
}

/// Owned version record for path-walk readers: the handle captured at read
/// time plus its sequence version. `is_current()` re-checks against the same
/// handle, so a walk result can be validated long after the read.
#[derive(Clone)]
pub struct DirectoryChildrenSharedVersion {
    children: std::sync::Arc<DirectoryChildren>,
    version: u64,
}

impl DirectoryChildrenSharedVersion {
    pub fn is_current(&self) -> bool {
        self.children.sequence.is_current(self.version)
    }

    pub fn handle(&self) -> &std::sync::Arc<DirectoryChildren> {
        &self.children
    }
}
