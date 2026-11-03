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
use super::{InodeChildren, InodePtr, InodeView};
use curvine_core_error::CommonResult;
use curvine_model::ListOptions;
use glob::Pattern;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// Odd while a mutation is inside the lock, even when quiescent: a reader
/// that captured `version()` before touching children can detect overlap by
/// re-reading an even value with the same number.
#[derive(Debug, Default)]
pub(crate) struct SequenceState {
    value: AtomicU64,
}

impl SequenceState {
    pub(crate) fn begin(&self) {
        self.value.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn finish(&self) {
        self.value.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn version(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    pub(crate) fn is_current(&self, version: u64) -> bool {
        let current = self.value.load(Ordering::Acquire);
        current.is_multiple_of(2) && current == version
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

pub struct DirectoryChildren {
    sequence: SequenceState,
    children: RwLock<InodeChildren>,
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
        Self {
            sequence: SequenceState::default(),
            children: RwLock::new(InodeChildren::default()),
            shards: OnceLock::new(),
        }
    }
}

enum MutationGuard<'a> {
    Single(RwLockWriteGuard<'a, InodeChildren>),
    Shard(RwLockWriteGuard<'a, InodeChildren>),
}

enum RenameMutationGuard<'a> {
    Single(RwLockWriteGuard<'a, InodeChildren>),
    Shards {
        first: RwLockWriteGuard<'a, InodeChildren>,
        second: Option<RwLockWriteGuard<'a, InodeChildren>>,
        shard_count: usize,
        first_index: usize,
    },
}

/// Write window for a same-directory rename. Same-shard renames take one
/// lock; cross-shard renames take both in shard-index ascending order, so
/// two opposite renames can never form an ABBA cycle.

/// Write window over one directory's child index: holds the index write lock
/// plus the sequence bump for the whole mutation.
pub struct DirectoryMutation<'a> {
    children: MutationGuard<'a>,
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

    /// Begin a dentry mutation. Large contended directories promote to shards
    /// here; the returned guard owns one shard write lock, so independent
    /// dentries in different shards mutate concurrently once promoted.
    pub fn begin_mutation(&self, child_name: &str) -> DirectoryMutation<'_> {
        if let Some(shards) = self.sharded() {
            let shard = shards.shard(child_name);
            let children = shard.children.write();
            return DirectoryMutation {
                children: MutationGuard::Shard(children),
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
                },
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

    /// Read a child together with the version needed to validate the read
    /// later via [`Self::version_is`].
    pub fn read_child_view(&self, name: &str) -> (Option<InodeView>, DirectoryReadVersion<'_>) {
        let (view, version) = self.with_children_for_name(name, |children, name| {
            let version = self.sequence.version();
            (
                children.get_child(name).map(|ptr| ptr.as_ref().clone()),
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

    /// Optimistic read protocol: the read is valid only when the sequence is
    /// still even at the captured version (no mutation overlapping it).
    pub fn version_is(&self, version: &DirectoryReadVersion<'_>) -> bool {
        std::ptr::eq(version.children as *const _, self)
            && self.sequence.is_current(version.version)
    }

    pub fn add_child(&self, inode: InodeView) -> CommonResult<InodePtr> {
        let name = inode.name().to_owned();
        let mut mutation = self.begin_mutation(&name);
        mutation.add_child(inode)
    }

    pub fn delete_child(&self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let mut mutation = self.begin_mutation(child_name);
        mutation.delete_child(child_id, child_name)
    }

    pub fn get_child_ptr(&self, name: &str) -> Option<InodePtr> {
        self.with_children_for_name(name, |children, name| children.get_child(name))
    }

    pub fn get_child_by_glob_pattern(&self, glob_pattern: &Pattern) -> Vec<InodePtr> {
        let mut out: Vec<InodePtr> = self
            .read_all_guards(|children| children.get_child_by_glob_pattern(glob_pattern))
            .into_iter()
            .flatten()
            .collect();
        out.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        out
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<InodePtr> {
        let mut out: Vec<InodePtr> = self
            .read_all_guards(|children| children.list_options(opts))
            .into_iter()
            .flatten()
            .collect();
        out.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        out
    }

    pub fn iter_len(&self) -> usize {
        if let Some(shards) = self.sharded() {
            return shards
                .read_all()
                .iter()
                .map(|children| children.len())
                .sum();
        }
        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.iter_len();
        }
        children.len()
    }

    /// Owned snapshot of the children, sorted by name across shards.
    pub(crate) fn read_all(&self) -> Vec<InodePtr> {
        let mut out: Vec<InodePtr> = self
            .read_all_guards(|children| children.iter().collect::<Vec<_>>())
            .into_iter()
            .flatten()
            .collect();
        out.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        out
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
    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        match &mut self.children {
            MutationGuard::Single(children) | MutationGuard::Shard(children) => {
                children.add_child(inode)
            }
        }
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        match &mut self.children {
            MutationGuard::Single(children) | MutationGuard::Shard(children) => {
                children.delete_child(child_id, child_name)
            }
        }
    }
}

/// Write window over one or two shards of the same directory.
pub struct DirectoryRenameMutation<'a> {
    children: RenameMutationGuard<'a>,
    _sequence: WriteSequence<'a>,
}

impl DirectoryRenameMutation<'_> {
    pub fn child_view(&self, name: &str) -> Option<InodeView> {
        match &self.children {
            RenameMutationGuard::Single(children) => {
                children.get_child(name).map(|ptr| ptr.as_ref().clone())
            }
            RenameMutationGuard::Shards { first, second, .. } => first
                .get_child(name)
                .or_else(|| {
                    second
                        .as_ref()
                        .and_then(|children| children.get_child(name))
                })
                .map(|ptr| ptr.as_ref().clone()),
        }
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
        destination: InodeView,
    ) -> CommonResult<()> {
        match &mut self.children {
            RenameMutationGuard::Single(children) => {
                Self::apply_rename(children, source_id, source_name, replaced, destination)
            }
            RenameMutationGuard::Shards {
                first,
                second,
                shard_count,
                first_index,
            } => {
                // Route each touched name to the shard guard this mutation
                // already owns; reads use the same hash, so the routing is
                // consistent with them.
                let source_index = super::sharded_children::shard_index(source_name, *shard_count);
                let destination_index =
                    super::sharded_children::shard_index(destination.name(), *shard_count);

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
                let _ = source_shard.delete_child(source_id, source_name)?;

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
        destination: InodeView,
    ) -> CommonResult<()> {
        if let Some((replaced_id, replaced_name)) = replaced {
            let _ = children.delete_child(replaced_id, replaced_name)?;
        }
        let _ = children.delete_child(source_id, source_name)?;
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
