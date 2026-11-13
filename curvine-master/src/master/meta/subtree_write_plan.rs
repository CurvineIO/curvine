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

/// Independent budget for the location sample (design §6): every block
/// entry and every worker-location entry is charged against this limit —
/// the plan budget does not cover locations, and a total above 1024 must
/// be rejected before any body side effect. One extra item per check is
/// allowed only to prove the overflow; no unbounded block-array copy.
pub(crate) const SUBTREE_LOCATION_LIMIT: usize = 1024;

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

        // Walk the chain root..=target. `components[0]` is the empty root
        // segment (resolve_edge_ids_on convention): the first edge IS the
        // root, and each later component names the next edge. At every
        // directory level the children token is captured before descending.
        let mut chain: Vec<i64> = Vec::with_capacity(components.len());
        let mut tokens: Vec<DirectoryChildrenSharedVersion> = Vec::new();
        let mut walk: Option<ChildEdge> = Some(ChildEdge::Dir(fs_dir.root_node()));
        for (index, _) in components.iter().enumerate() {
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
            if index + 1 == components.len() {
                break;
            }
            tokens.push(node.children.shared_version());
            // Point lookup by name: no enumeration, no edge budget.
            walk = node.children.get_child_edge(&components[index + 1]);
        }
        if chain.len() != components.len() {
            return Ok(None);
        }
        let target_depth = chain.len() - 1;

        // Collect the target subtree with an explicit Vec stack: every
        // directory and every file id enters the Write set. Each visited
        // directory records a token before enumerating its edges. The
        // target edge is re-fetched by the same convention: walk from the
        // root through components[1..].
        let target_edge = {
            let mut level: Option<ChildEdge> = Some(ChildEdge::Dir(fs_dir.root_node()));
            for (index, _) in components.iter().enumerate() {
                let Some(edge) = level.take() else {
                    return Ok(None);
                };
                if index + 1 == components.len() {
                    level = Some(edge);
                    break;
                }
                let ChildEdge::Dir(dir) = edge else {
                    return Ok(None);
                };
                level = dir.children.get_child_edge(&components[index + 1]);
            }
            level
        };
        let Some(target_edge) = target_edge else {
            return Ok(None);
        };

        let mut subtree: Vec<InodeLockRequest> = Vec::new();
        // The target itself entered the budget as a chain entry; every
        // further subtree member is charged exactly once, through the edge
        // it was reached by (enumeration), never again on pop.
        let mut stack: Vec<ChildEdge> = vec![target_edge];
        while let Some(edge) = stack.pop() {
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

        // Assemble the request set. The target and its whole subtree come
        // from the DFS as Write; the chain contributes the ancestors: the
        // parent in `parent_mode`, everything above it Read, with the root
        // kept only when it is the mutated parent.
        let parent_depth = target_depth.saturating_sub(1);
        let mut requests: Vec<InodeLockRequest> = Vec::with_capacity(chain.len() + subtree.len());
        for (depth, id) in chain.iter().enumerate() {
            if depth == target_depth {
                // The subtree section already carries the target as Write.
                continue;
            }
            let mode = if depth == parent_depth {
                parent_mode
            } else {
                InodeLockMode::Read
            };
            // Root as a pure ancestor stays lock-free (established filter);
            // a root that is the mutated parent keeps its mode.
            if *id == ROOT_INODE_ID
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

/// F3: shared location plan for RPC and replay (design §6) — one budget
/// counter over the WHOLE call, charging every actual block entry and
/// worker-location entry (dedup happens at lock-key level, not here). Only
/// owned IDs are kept; no DB Arc, no Guard, no lock acquisition. The
/// bounded location read walks the existing prefix iterator with the
/// remaining budget, returning complete `Some` or over-limit `None` —
/// never a truncated success set.
pub(crate) struct SubtreeLocationPlan {
    block_ids: Vec<i64>,
    worker_ids: Vec<u32>,
}

impl SubtreeLocationPlan {
    /// Bounded location sampling over every Write inode in `requests`.
    /// Charges one entry per block and one per worker-location against
    /// `SUBTREE_LOCATION_LIMIT` (shared counter, dedup-before-counting is
    /// forbidden). Returns `Ok(None)` when the budget is exceeded (static
    /// overflow — no retry); a real store error is still `Err`.
    /// No lock acquisition happens here.
    pub(crate) fn capture(
        fs_dir: &crate::master::meta::FsDir,
        requests: &[InodeLockRequest],
    ) -> FsResult<Option<Self>> {
        let mut block_ids = Vec::new();
        let mut worker_ids = Vec::new();
        let mut entries = 0usize;

        for request in requests {
            if !matches!(request.mode, InodeLockMode::Write) {
                continue;
            }
            let Some(inode) = fs_dir.store.get_inode(request.inode_id, None)? else {
                continue;
            };
            let Ok(file) = inode.as_file_ref() else {
                continue;
            };
            // G4: iterate the inode's real blocks directly (no full Vec
            // clone). The bounded read walks the existing prefix iterator
            // with the remaining budget.
            for block in &file.blocks {
                entries += 1;
                if entries > SUBTREE_LOCATION_LIMIT {
                    return Ok(None);
                }
                block_ids.push(block.id);
                let Some(locations) = fs_dir
                    .store
                    .get_locations_bounded(block.id, SUBTREE_LOCATION_LIMIT - entries)?
                else {
                    return Ok(None);
                };
                for location in locations {
                    entries += 1;
                    if entries > SUBTREE_LOCATION_LIMIT {
                        return Ok(None);
                    }
                    worker_ids.push(location.worker_id);
                }
            }
        }

        Ok(Some(Self {
            block_ids,
            worker_ids,
        }))
    }

    /// Whether the current block/worker set is fully covered by the sampled
    /// set (called under the FINAL view, after location locks are held).
    /// Returns `false` when any live block/worker is missing from the
    /// sampled set (release everything and retry ≤3).
    /// G4: coverage re-validation against the CURRENT full block/worker set
    /// of every Write inode (not just the sampled set). One whole-call
    /// counter charges every block entry and every location row against the
    /// same SUBTREE_LOCATION_LIMIT; bounded reads pass the remaining budget
    /// and only allow one entry past the limit to prove overflow.
    pub(crate) fn covers(
        &self,
        fs_dir: &crate::master::meta::FsDir,
        requests: &[InodeLockRequest],
    ) -> FsResult<bool> {
        let mut coverage_entries = 0usize;
        for request in requests {
            if !matches!(request.mode, InodeLockMode::Write) {
                continue;
            }
            let Some(inode) = fs_dir.store.get_inode(request.inode_id, None)? else {
                continue;
            };
            let Ok(file) = inode.as_file_ref() else {
                continue;
            };
            // G4: iterate the inode's real blocks directly (no full clone).
            for block in &file.blocks {
                coverage_entries += 1;
                if coverage_entries > SUBTREE_LOCATION_LIMIT {
                    return Ok(false);
                }
                if !self.block_ids.contains(&block.id) {
                    return Ok(false);
                }
                let Some(locations) = fs_dir
                    .store
                    .get_locations_bounded(block.id, SUBTREE_LOCATION_LIMIT - coverage_entries)?
                else {
                    return Ok(false);
                };
                for location in locations {
                    coverage_entries += 1;
                    if coverage_entries > SUBTREE_LOCATION_LIMIT {
                        return Ok(false);
                    }
                    if !self.worker_ids.contains(&location.worker_id) {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    pub(crate) fn block_ids(&self) -> &[i64] {
        &self.block_ids
    }

    pub(crate) fn worker_ids(&self) -> &[u32] {
        &self.worker_ids
    }
}
