// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
//
// Bounded recursive-lock planning (C2 phase A, design
// 2026-11-13-recursive-metadata-lock-design.md §10).
//
// Pure in-memory topology planning only: one bounded DFS over live child
// edges collects the inode lock set for a recursive mutation, plus a
// shared-version token per visited directory. No RPC admission, journal,
// confirmation or business mutation happens here; execution entries keep
// their current shape until design §8 closes.

use crate::master::meta::inode::DirectoryChildrenSharedVersion;
use crate::master::meta::inode::{ChildEdge, InodePath, InodeView, ROOT_INODE_ID};
use crate::master::meta::inode_lock_manager::{InodeLockMode, InodeLockRequest};
use curvine_core_error::err_box;
use curvine_error::FsResult;

/// Initial resource budget for one plan attempt: path-chain entries plus
/// every actually visited child edge, counted per edge (two names for the
/// same inode count twice). Not a performance optimum and not a wall-clock
/// holding guarantee; exceeding it means the caller falls back to the cold
/// path. Enumeration itself is bounded: at most one edge beyond the
/// remaining budget is read per directory, and only to prove the budget was
/// exceeded — never a full enumeration followed by a length check.
pub(crate) const SUBTREE_PLAN_LIMIT: usize = 1024;

/// One bounded subtree plan. Keeps only the restore epoch, the path id
/// chain, the target depth, the full inode lock requests, and one
/// shared-version token per visited directory (stable children handles).
/// Deliberately holds no `FsDir` Arc, no store Arc, no namespace guard and
/// no lock.
pub(crate) struct SubtreeWritePlan {
    /// Namespace restore epoch captured before the walk; re-checked under
    /// the inode locks (a restore invalidates id chains).
    restore_epoch: u64,
    /// Resolved chain ids, root..=target.
    chain: Vec<i64>,
    /// The planned path components, for the locked re-walk.
    components: Vec<String>,
    /// Depth of the target in `chain` (`chain.len() - 1`).
    target_depth: usize,
    /// The full inode lock request set (ancestors, parent, subtree).
    requests: Vec<InodeLockRequest>,
    /// One token per directory visited during the walk, captured BEFORE its
    /// edges were enumerated; empty directories get a token too.
    tokens: Vec<DirectoryChildrenSharedVersion>,
}

impl SubtreeWritePlan {
    /// Bounded planning over the live tree. Read-only, no DB point reads:
    /// edges come from the children containers and ids from the edges
    /// themselves. Returns `Ok(None)` when a usable plan cannot be formed
    /// without side effects (unresolvable path, budget exceeded); a real
    /// error (pending restore, invalid path) stays `Err`.
    pub(crate) fn capture(
        fs_dir: &crate::master::meta::FsDir,
        path: &str,
        parent_mode: InodeLockMode,
    ) -> FsResult<Option<Self>> {
        let control = fs_dir.store.scan_control();
        if control.is_restoring() {
            return err_box!("subtree planning rejected: snapshot restore is pending");
        }
        let restore_epoch = control.cancel_epoch();
        let components = InodeView::path_components(path)?;
        if components.last().is_none_or(String::is_empty) {
            return err_box!("Path {} is invalid", path);
        }

        // The budget counts one unit per chain entry and per visited edge.
        let mut budget = SUBTREE_PLAN_LIMIT;
        let charge = |budget: &mut usize, units: usize| -> bool {
            match budget.checked_sub(units) {
                Some(rest) => {
                    *budget = rest;
                    true
                }
                None => false,
            }
        };

        // Walk the chain root..=target. At every directory level: capture
        // the children token first, then enumerate at most
        // `remaining + 1` edges to find the next component.
        let mut chain: Vec<i64> = Vec::with_capacity(components.len());
        let mut tokens: Vec<DirectoryChildrenSharedVersion> = Vec::new();
        let mut walk: Option<ChildEdge> = Some(ChildEdge::Dir(fs_dir.root_node()));
        for component in &components {
            let Some(edge) = walk.take() else {
                // The chain ended before the path did.
                return Ok(None);
            };
            if !charge(&mut budget, 1) {
                return Ok(None);
            }
            chain.push(edge.id());
            let ChildEdge::Dir(node) = edge else {
                // A file component: the path cannot descend past it.
                return Ok(None);
            };
            if chain.len() == components.len() {
                break;
            }
            tokens.push(node.children.shared_version());
            // Point lookup by name: no enumeration, no edge budget.
            walk = node.children.get_child_edge(component);
        }
        if chain.len() != components.len() {
            return Ok(None);
        }
        let target_depth = chain.len() - 1;

        // Collect the target subtree with an explicit Vec stack: every
        // directory and every file id enters the Write set. Each visited
        // directory records a token before enumerating its edges.
        let target_edge = {
            let node = fs_dir.root_node();
            let mut level: Option<ChildEdge> = Some(ChildEdge::Dir(node));
            for component in &components {
                let Some(edge) = level.take() else {
                    return Ok(None);
                };
                level = match edge {
                    ChildEdge::Dir(dir) => dir.children.get_child_edge(component),
                    ChildEdge::FileId(_) => None,
                };
            }
            level
        };
        let Some(target_edge) = target_edge else {
            return Ok(None);
        };

        let mut subtree: Vec<InodeLockRequest> = Vec::new();
        let mut stack: Vec<ChildEdge> = vec![target_edge];
        while let Some(edge) = stack.pop() {
            if !charge(&mut budget, 1) {
                return Ok(None);
            }
            match edge {
                ChildEdge::FileId(id) => {
                    // Files are Write-locked too: a post-plan external hard
                    // link does not always bump the source directory version,
                    // and complete/add_block write the file inode directly.
                    subtree.push(InodeLockRequest::write(0, id));
                }
                ChildEdge::Dir(node) => {
                    subtree.push(InodeLockRequest::write(0, node.id));
                    tokens.push(node.children.shared_version());
                    let Some(edges) = node.children.child_edges_bounded(budget) else {
                        return Ok(None);
                    };
                    if !charge(&mut budget, edges.len()) {
                        return Ok(None);
                    }
                    for edge in edges {
                        stack.push(edge);
                    }
                }
            }
        }

        // Assemble the request set. Ancestors above the parent are Read
        // (root only when it is the target or the parent); the parent takes
        // `parent_mode`; the target and the whole subtree are Write.
        let parent_depth = target_depth.saturating_sub(1);
        let mut requests: Vec<InodeLockRequest> = Vec::with_capacity(chain.len() + subtree.len());
        for (depth, id) in chain.iter().enumerate() {
            let mode = if depth == target_depth {
                InodeLockMode::Write
            } else if depth == parent_depth {
                parent_mode
            } else {
                InodeLockMode::Read
            };
            // Root as a pure ancestor stays lock-free (established filter);
            // a root that is the target or the mutated parent keeps its mode.
            if *id == ROOT_INODE_ID
                && depth != target_depth
                && !(depth == parent_depth && matches!(mode, InodeLockMode::Write))
            {
                continue;
            }
            requests.push(InodeLockRequest {
                depth,
                inode_id: *id,
                mode,
            });
        }
        requests.extend(subtree);

        Ok(Some(Self {
            restore_epoch,
            chain,
            components,
            target_depth,
            requests,
            tokens,
        }))
    }

    /// The complete inode lock request set; the caller acquires it in one
    /// stripe-sorted `lock_many` batch. Taking no locks itself.
    pub(crate) fn inode_requests(&self) -> &[InodeLockRequest] {
        &self.requests
    }

    /// Revalidation under the full inode lock set: restore epoch, every
    /// chain edge and the target identity, the same live directory handles
    /// and every captured version token. Returns the locked resolution for
    /// the body, or `None` when anything moved (the caller then drops the
    /// locks and falls back). Never executes a body.
    pub(crate) fn revalidate(
        &self,
        fs_dir: &crate::master::meta::FsDir,
        path: &str,
    ) -> FsResult<Option<InodePath>> {
        let control = fs_dir.store.scan_control();
        if control.is_restoring() || control.cancel_epoch() != self.restore_epoch {
            return Ok(None);
        }
        for token in &self.tokens {
            if !token.is_current() {
                return Ok(None);
            }
        }
        // Resolve under the locks and compare every edge id with the plan.
        let resolved = InodePath::resolve(fs_dir.root_node(), path, &fs_dir.store)?;
        if resolved.components.len() != self.components.len()
            || resolved.inodes.len() != self.chain.len()
        {
            return Ok(None);
        }
        for (entry, planned) in resolved.inodes.iter().zip(&self.chain) {
            if entry.id() != *planned {
                return Ok(None);
            }
        }
        Ok(Some(resolved))
    }
}
