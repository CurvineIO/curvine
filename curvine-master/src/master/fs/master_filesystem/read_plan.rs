// Copyright 2026 OPPO
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

//! Pure read planning (C3 layout only).
//!
//! `MemoryReadPlan` is the identity/page-membership plan plus the lock-request
//! projection and the match predicate; `plan_memory_read` is its constructor.
//! Nothing here takes a lock, reads the DB a second time, or touches a
//! snapshot fallback - the caller still owns the original guard window, as the
//! design requires. `MEMORY_READ_ATTEMPTS` stays in the parent because the
//! status readers that retry with it remain there.

use super::*;

/// Identity-only plan for a live-tree hot read: one inode id per path
/// component, the target's kind, and — for a directory target — the page
/// membership as edges. Planning copies no directory attributes and loads no
/// file rows; statuses are built after the inode locks.
pub(super) struct MemoryReadPlan {
    pub(super) ids: Vec<i64>,
    pub(super) full: bool,
    pub(super) target_is_dir: bool,
    pub(super) root_target: bool,
    pub(super) target_name: String,
    pub(super) target_edge: Option<ChildEdge>,
    /// The FileId edge that stopped the walk before all components resolved
    /// (a file edge is never traversed further). `None` when the walk ended
    /// on a directory edge, ran out of live edges, or fully resolved.
    pub(super) trailing_file_edge: Option<i64>,
    pub(super) children: Vec<(String, ChildEdge)>,
}

impl MemoryReadPlan {
    /// Page inode read requests: path ancestors (root stays lock-free as a
    /// pure ancestor) plus the target plus every page child. One
    /// stripe-sorted acquisition covers the whole locked window; no locks
    /// are taken incrementally while a batch is already held.
    pub(super) fn lock_requests(&self) -> SmallVec<[InodeLockRequest; 8]> {
        let mut requests = SmallVec::new();
        for (depth, id) in self.ids.iter().enumerate() {
            let is_target = depth + 1 == self.ids.len();
            if *id == ROOT_INODE_ID && !(is_target && self.root_target) {
                continue;
            }
            requests.push(InodeLockRequest::read(depth, *id));
        }
        let child_depth = self.ids.len();
        for (index, (_, edge)) in self.children.iter().enumerate() {
            requests.push(InodeLockRequest::read(child_depth + index, edge.id()));
        }
        requests
    }

    /// Whether the planned identity still matches the live identity: path
    /// edges and — for directory targets — page membership.
    pub(super) fn matches(&self, other: &MemoryReadPlan) -> bool {
        self.ids == other.ids
            && self.target_is_dir == other.target_is_dir
            && (!self.target_is_dir || Self::membership_eq(&self.children, &other.children))
    }

    fn membership_eq(left: &[(String, ChildEdge)], right: &[(String, ChildEdge)]) -> bool {
        left.len() == right.len()
            && left.iter().zip(right.iter()).all(|((ln, le), (rn, re))| {
                ln == rn && le.id() == re.id() && le.is_dir() == re.is_dir()
            })
    }
}

impl MasterFilesystem {
    /// Plans a hot read under the namespace read guard: an identity walk
    /// plus page membership edges only. The caller releases the guard
    /// before taking inode locks.
    pub(super) fn plan_memory_read(
        fs_dir: &FsDir,
        path: &str,
        page: bool,
        page_opts: &ListOptions,
    ) -> FsResult<MemoryReadPlan> {
        let components = InodeView::path_components(path)?;
        Self::plan_memory_read_with_components(fs_dir, path, &components, page, page_opts)
    }

    /// Same planning, with path components the caller already parsed.
    ///
    /// `exists` validates the immutable path once per call and passes the result
    /// down, so its two walks inside the retry loop do not each re-split it. The
    /// walk, lock requests, epoch and identity checks are unchanged.
    pub(super) fn plan_memory_read_with_components(
        fs_dir: &FsDir,
        path: &str,
        components: &[String],
        page: bool,
        page_opts: &ListOptions,
    ) -> FsResult<MemoryReadPlan> {
        if components.last().is_none_or(String::is_empty) {
            return err_box!("Path {} is invalid", path);
        }
        // A failed restore detaches the store; directory targets never touch
        // file rows, so every memory read re-checks availability up front.
        fs_dir.store.ensure_available()?;
        let mut ids: Vec<i64> = Vec::with_capacity(components.len());
        let mut children: Vec<(String, ChildEdge)> = Vec::new();
        let mut target_is_dir = false;
        let mut target_edge: Option<ChildEdge> = None;
        let mut trailing_file_edge: Option<i64> = None;
        let mut next: Option<ChildEdge> = Some(ChildEdge::Dir(fs_dir.root_node()));
        for (index, _) in components.iter().enumerate() {
            let Some(edge) = next.as_ref() else { break };
            let node = match edge {
                ChildEdge::Dir(node) => Some(node),
                ChildEdge::FileId(_) => None,
            };
            ids.push(edge.id());
            if index + 1 == components.len() {
                target_is_dir = node.is_some();
                target_edge = Some(edge.clone());
                if let Some(node) = node.filter(|_| page) {
                    children = node.children.child_edges(page_opts);
                }
            } else {
                // A file edge mid-path stops the walk exactly like the old
                // resolver: the deeper components cannot resolve, and the
                // inode row behind this edge must be checked — a missing row
                // is the dangling-file error, not a plain FileNotFound.
                if let ChildEdge::FileId(id) = edge {
                    trailing_file_edge = Some(*id);
                    break;
                }
                next = node.and_then(|node| node.children.get_child_edge(&components[index + 1]));
            }
        }
        let root_target = components.len() == 1 && components[0] == PATH_SEPARATOR;
        Ok(MemoryReadPlan {
            // Root is the read target only when the requested path itself is
            // "/". A failed walk of a deeper request also yields just the
            // root id, and that negative lookup must not re-claim the root
            // read lock.
            root_target,
            full: ids.len() == components.len(),
            // The root stat keeps the shared ROOT_INODE_NAME: the component
            // walk spells it "/" but the live tree and store rows do not.
            target_name: if root_target {
                ROOT_INODE_NAME.to_string()
            } else {
                components.last().unwrap().clone()
            },
            ids,
            target_is_dir,
            target_edge,
            trailing_file_edge,
            children,
        })
    }
}
