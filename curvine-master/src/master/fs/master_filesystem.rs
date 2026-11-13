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

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::full_block_report::{
    FullReportBegin, FullReportProgress, FullReportTracker,
};
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::{FsModeReleaser, JournalSystem, JournalWriter};
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::inode::{
    ChildEdge, InodeDir, InodeEntry, InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR,
    ROOT_INODE_ID, ROOT_INODE_NAME,
};
use crate::master::meta::store::InodeStoreSnapshot;
use crate::master::meta::subtree_write_plan::{
    SubtreeLocationPlan, SubtreeWritePlan, SUBTREE_PLAN_LIMIT,
};
use crate::master::meta::{
    trash, BlockLocationLockManager, CacheInvalidationResult, ColdError, ColdFsDirGuard,
    CommitGate, FileSystemStats, FsDir, FsDirHandle, InodeId, InodeLockManager, InodeLockMode,
    InodeLockRequest, InodeLockSet, RestoredTrashEntry, TrashListing, TrashScanPage,
};

use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::ErrorKind;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS;

use curvine_model::*;
use curvine_raft::conf::JournalConfExt;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::ArcRwLock;
use log::{error, info, warn};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// How a delete treats the recycle bin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteMode {
    /// Follow the recycle-bin configuration: soft delete when
    /// `master.trash_enabled` is set, permanent otherwise.
    Default,
    /// Always purge permanently. Used by mounted deletes, cache frees and every
    /// path that already lives under `/.Trash`.
    SkipTrash,
    /// Stage the entry under `/.Trash/.pending` and let the purger reclaim it in
    /// the background, regardless of `master.trash_enabled`. Falls back to a
    /// permanent delete for paths already inside the trash namespace.
    AsyncPurge,
}

pub struct CvMetadataSnapshotEntry {
    pub status: FileStatus,
    pub blocks: Option<FileBlocks>,
}

pub struct CvMetadataSnapshotPage {
    pub entries: Vec<CvMetadataSnapshotEntry>,
    pub next_page_token: Option<String>,
    pub epoch: u64,
}

#[derive(Clone)]
struct CompleteFileOptions {
    only_flush: bool,
    return_file_blocks: bool,
    set_attr_opts: Option<SetAttrOpts>,
}

/// Sentinel for sharded write admission: `Done` carries the operation result.
/// `Fallback` marks a side-effect-free planning point that needs the global
/// lock (unresolved path, identity churn, retries exhausted); it escalates to
/// the single-writer exclusive path. A busy or failed view is an `Err`, not a
/// `Fallback`.
enum ShardedOutcome<R> {
    Done(R),
    Fallback,
}

/// Test-only interleaving gate (see tests/memory_read_gates.rs); production
/// code only reaches it behind #[cfg(test)].
#[cfg(test)]
#[path = "master_filesystem/tests/memory_read_gates.rs"]
pub(crate) mod memory_read_gates;

/// Confirmation-window guard shared by the exclusive path and the C2
/// recursive coordination entry (design §8.1). Armed in the body's lexical
/// scope after the view/lock declarations, disarmed only when the real
/// confirmation reports the apply/cursor ack. On a confirmation error — or
/// if the wait panics while armed — the namespace holder goes terminal
/// before the surrounding resources are released. Borrows only the holder
/// and the writer: no namespace view, object locks, DB Arc, admission slot
/// or exclusive gate. `handle.take` keeps the single terminal/disarm
/// mechanism; there is no second legacy implementation.
pub(crate) struct MetadataConfirmation<'a> {
    handle: Option<&'a FsDirHandle>,
    writer: &'a JournalWriter,
}

impl<'a> MetadataConfirmation<'a> {
    /// Arms a guard over one namespace holder. The caller controls the
    /// declaration point (§8.1: after the view/lock bindings, inside the
    /// body's lexical scope).
    pub(crate) fn arm(handle: &'a FsDirHandle, writer: &'a JournalWriter) -> Self {
        Self {
            handle: Some(handle),
            writer,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.handle = None;
    }

    pub(crate) fn fail_terminal(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.fail_closed(self.writer);
            // F6.5: the observation checkpoint fires ONLY when an actual
            // terminal transition happened (handle was Some and was taken).
            // A successful disarm sets handle=None first, so the guard's
            // Drop calls fail_terminal but never reaches this branch — no
            // fabricated "terminal happened" event from a successful Drop.
            #[cfg(test)]
            if let Some((reached, release)) = self.writer.consume_terminal_hook() {
                let _ = reached.send(());
                let _ = release.recv_timeout(std::time::Duration::from_secs(60));
            }
        }
    }
}

impl Drop for MetadataConfirmation<'_> {
    fn drop(&mut self) {
        self.fail_terminal();
    }
}

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: Arc<FsDirHandle>,
    /// Same counter/stats instances the live FsDir owns, kept for metrics
    /// accessors that must read real values while the view is closed.
    inode_id_counter: Arc<InodeId>,
    fs_stats_counter: Arc<FileSystemStats>,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    /// Serialises namespace mutations across the commit boundary so a snapshot
    /// or role fence can drain in-flight writers before reading metadata.
    metadata_commit_gate: Arc<CommitGate>,
    pub(crate) journal_writer: Arc<JournalWriter>,
    full_report_tracker: FullReportTracker,
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
    pub(crate) block_location_locks: Arc<BlockLocationLockManager>,
    inode_locks: Arc<InodeLockManager>,
    worker_location_locks: Arc<Vec<Mutex<()>>>,
    /// Serialises drains of `/.Trash/.pending`. The blocking empty-trash call
    /// owns the staging directory across staging and draining, and the
    /// background purger skips a cycle while that lock is held, so the two can
    /// never reclaim the same entry and the blocking caller always reports the
    /// totals it actually reclaimed.
    pending_drain: Arc<Mutex<()>>,
    /// Routes fs_mode releases through reconciliation (transfer mode). Unset
    /// keeps the direct release path, which is correct only while the replayer
    /// waits for each export. Shared by every clone so none bypasses the gate.
    fs_mode_releaser: Arc<OnceCell<Arc<dyn FsModeReleaser>>>,
}

/// Identity-only plan for a live-tree hot read: one inode id per path
/// component, the target's kind, and — for a directory target — the page
/// membership as edges. Planning copies no directory attributes and loads no
/// file rows; statuses are built after the inode locks.
struct MemoryReadPlan {
    ids: Vec<i64>,
    full: bool,
    target_is_dir: bool,
    root_target: bool,
    target_name: String,
    target_edge: Option<ChildEdge>,
    children: Vec<(String, ChildEdge)>,
}

impl MemoryReadPlan {
    /// Page inode read requests: path ancestors (root stays lock-free as a
    /// pure ancestor) plus the target plus every page child. One
    /// stripe-sorted acquisition covers the whole locked window; no locks
    /// are taken incrementally while a batch is already held.
    fn lock_requests(&self) -> SmallVec<[InodeLockRequest; 8]> {
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
    fn matches(&self, other: &MemoryReadPlan) -> bool {
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

/// Result of [`MasterFilesystem::free_or_defer`].
pub enum FreeOutcome {
    Freed(FreeResult),
    /// Handed to fs_mode reconciliation; `pending` files still hold data.
    Deferred {
        pending: usize,
    },
}

/// One fs_mode file as Free reconciliation sees it.
pub struct FsModeSnapshot {
    pub path: String,
    pub gen: FileGen,
    pub state: StorageState,
    pub ufs_mtime: i64,
    /// Complete and not being written.
    pub quiescent: bool,
}

pub struct BlockReportResult {
    pub delete_blocks: Vec<i64>,
    pub restart_full_report: bool,
}

#[derive(Default)]
pub struct LostWorkerLocationCleanup {
    pub removed_block_ids: Vec<i64>,
    pub replication_block_ids: Vec<i64>,
}

pub(crate) enum BlockInodeState {
    File,
    Missing,
    NotFile,
}

fn child_snapshot_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn snapshot_token_in_subtree(token: &str, subtree_path: &str) -> bool {
    token == subtree_path
        || (subtree_path != "/"
            && token
                .strip_prefix(subtree_path)
                .map(|rest| rest.starts_with(PATH_SEPARATOR))
                .unwrap_or(false))
}

struct FullBlockReconcileState {
    running: bool,
    generation: u64,
    pending: Option<FullBlockReconcileJob>,
}

struct FullBlockReconcileJob {
    generation: u64,
    reported_blocks: HashSet<i64>,
}

const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;
const WORKER_LOCATION_LOCK_STRIPES: usize = 256;

impl MasterFilesystem {
    // Max block-report location updates applied under one namespace guard.
    const BLOCK_REPORT_WRITE_CHUNK: usize = 4096;
    // Max lost-worker block ids inspected under a single fs_dir write lock.
    const LOST_WORKER_INVALIDATION_CHUNK: usize = Self::BLOCK_REPORT_WRITE_CHUNK;

    fn validate_alloc_capacity(
        current_len: i64,
        replicas: u8,
        opts: &FileAllocOpts,
        available: i64,
    ) -> FsResult<()> {
        if opts.truncate || opts.len <= current_len {
            return Ok(());
        }

        let logical_growth = opts.len - current_len;
        let required = logical_growth.saturating_mul(i64::from(replicas));
        if required > available {
            return err_ext!(FsError::disk_out_of_space(format!(
                "fallocate requires {} bytes for {} replicas, but only {} bytes are available",
                logical_growth, replicas, available
            )));
        }

        Ok(())
    }

    pub fn new(
        conf: &ClusterConf,
        fs_dir: Arc<FsDirHandle>,
        inode_locks: Arc<InodeLockManager>,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> Self {
        let (journal_writer, inode_id_counter, fs_stats_counter) = {
            let dir = fs_dir
                .view()
                .expect("journal_writer: fs_dir must be OPEN during construction");
            (
                dir.journal_writer.clone(),
                dir.inode_id_handle(),
                dir.fs_stats_handle(),
            )
        };
        Self {
            fs_dir,
            inode_id_counter,
            fs_stats_counter,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
            block_location_locks: Arc::new(BlockLocationLockManager::default()),
            inode_locks,
            worker_location_locks: Arc::new(
                (0..WORKER_LOCATION_LOCK_STRIPES)
                    .map(|_| Mutex::new(()))
                    .collect(),
            ),
            pending_drain: Default::default(),
            fs_mode_releaser: Default::default(),
            metadata_commit_gate: Default::default(),
            journal_writer,
        }
    }

    /// Coordinates namespace mutations with snapshot barriers. Role fencing
    /// rejects new admissions separately; admitted writes are not yet drained.
    pub fn metadata_commit_gate(&self) -> Arc<CommitGate> {
        self.metadata_commit_gate.clone()
    }

    /// Test-only access to the shared inode lock manager so journal-replay
    /// regressions can hold a real lock while driving an interleaving.
    #[cfg(test)]
    pub(crate) fn inode_locks_for_test(&self) -> Arc<InodeLockManager> {
        self.inode_locks.clone()
    }

    /// Install the fs_mode release gate. Called once at startup when transfer is
    /// enabled; leaving it unset keeps the legacy direct release path.
    pub fn set_fs_mode_releaser(&self, releaser: Arc<dyn FsModeReleaser>) {
        // First writer wins: a second install would only mean two gates.
        let _ = self.fs_mode_releaser.set(releaser);
    }

    pub fn fs_mode_releaser(&self) -> Option<&Arc<dyn FsModeReleaser>> {
        self.fs_mode_releaser.get()
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        let fs_dir = js.fs().fs_dir.clone();
        let (inode_id_counter, fs_stats_counter) = {
            let dir = fs_dir
                .view()
                .expect("with_js: fs_dir must be OPEN during construction");
            (dir.inode_id_handle(), dir.fs_stats_handle())
        };
        Self {
            fs_dir,
            inode_id_counter,
            fs_stats_counter,
            worker_manager: js.worker_manager(),
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: js.fs().full_report_tracker.clone(),
            full_block_reconciles: js.fs().full_block_reconciles.clone(),
            full_block_reconcile_executor: js.fs().full_block_reconcile_executor.clone(),
            block_location_locks: js.fs().block_location_locks.clone(),
            inode_locks: js.fs().inode_locks.clone(),
            worker_location_locks: js.fs().worker_location_locks.clone(),
            pending_drain: js.fs().pending_drain.clone(),
            fs_mode_releaser: js.fs().fs_mode_releaser.clone(),
            metadata_commit_gate: js.fs().metadata_commit_gate(),
            journal_writer: js.fs().journal_writer.clone(),
        }
    }

    pub fn check_parent(path: &InodePath) -> FsResult<()> {
        // The root directory must exist.All /a does not require verification
        if path.len() > 2 {
            if let Some(v) = path.get_inode(-2) {
                if !v.is_dir() {
                    err_box!(
                        "Parent path is not a directory:: {}",
                        path.get_parent_path()
                    )
                } else {
                    Ok(())
                }
            } else {
                err_box!("Parent directory doesn't exist: {}", path.get_parent_path())
            }
        } else {
            Ok(())
        }
    }

    pub fn print_tree(&self) {
        let mut cold = match self.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("print_tree: namespace unavailable: {e}");
                return;
            }
        };
        cold.view().print_tree();
    }

    /// Holds the commit gate through mutation and journal confirmation so a
    /// gate-closing snapshot can drain the write. The second role check rejects
    /// requests delayed at admission; it does not drain writers already admitted
    /// when a role change installs its fence.
    /// Acquires the cold handle for a global-lock path. A competing cold
    /// owner queues the caller: Busy retries are bounded by the same
    /// DEFAULT_COLD_TIMEOUT budget (the drain-timeout boundary is preserved
    /// per attempt); FAILED is terminal.
    pub(crate) fn cold_acquire_wait(&self) -> FsResult<ColdFsDirGuard<'_>> {
        let deadline = std::time::Instant::now() + DEFAULT_COLD_TIMEOUT;
        loop {
            match self.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT) {
                Ok(guard) => {
                    return Ok(guard);
                }
                Err(ColdError::Busy) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(ColdError::Busy.into());
                    }
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub(crate) fn run_metadata_write<R>(&self, f: impl FnOnce() -> FsResult<R>) -> FsResult<R> {
        let epoch = self.journal_writer.metadata_epoch()?;
        self.run_metadata_write_epoch(epoch, f)
    }

    fn run_metadata_write_epoch<R>(
        &self,
        epoch: u64,
        f: impl FnOnce() -> FsResult<R>,
    ) -> FsResult<R> {
        // Single admission: commit-gate slot, epoch check, commit scope, then
        // the closure runs exactly once. A closed view (cold window) surfaces
        // as view()/cold errors from the closure itself — there is no
        // probe/retry layer here; the typed exclusive protocol lives in
        // run_metadata_write_with_fallback.
        let commit_guard = self.metadata_commit_gate.enter();
        self.journal_writer.ensure_metadata_epoch(epoch)?;
        let scope = self.journal_writer.begin_commit_scope();
        let result = f();
        let commit = scope.wait();
        drop(commit_guard);
        let result = result?;
        commit?;
        self.maybe_create_periodic_snapshot(epoch);
        Ok(result)
    }

    /// Metadata write with an explicit global-lock fallback body, following
    /// the agreed type protocol: `hot` runs once under normal admission and
    /// returns `ShardedOutcome::Fallback` at its side-effect-free global-lock
    /// planning point; the original epoch is captured up front, the normal
    /// slot/view/locks are fully released, and `cold_body` then runs exactly
    /// once against the exclusive owner's `&mut FsDir` (gate closed and held
    /// across the real confirmation, cold covering the body only, original
    /// epoch re-verified with the original ensure_metadata_epoch error,
    /// failure_seq honored, snapshot after gate release).
    pub(crate) fn run_metadata_write_with_fallback<R>(
        &self,
        hot: impl FnOnce(u64) -> FsResult<ShardedOutcome<R>>,
        cold_body: impl FnOnce(&mut FsDir) -> FsResult<R>,
    ) -> FsResult<R> {
        let epoch = self.journal_writer.metadata_epoch()?;
        // Normal admission: hot runs once; a genuine error is a genuine error.
        // F1: the outer original epoch is passed explicitly to the hot
        // coordination closure — recursive entries must use THIS value, not
        // re-sample metadata_epoch() inside the hot body.
        let commit_guard = self.metadata_commit_gate.enter();
        self.journal_writer.ensure_metadata_epoch(epoch)?;
        let outcome = {
            let scope = self.journal_writer.begin_commit_scope();
            let outcome = hot(epoch);
            scope.wait()?;
            outcome?
        };
        drop(commit_guard);
        match outcome {
            ShardedOutcome::Done(value) => {
                self.maybe_create_periodic_snapshot(epoch);
                Ok(value)
            }
            ShardedOutcome::Fallback => {
                // Normal slot, view and object locks are all released here.
                self.run_metadata_write_exclusive(epoch, cold_body)
            }
        }
    }

    /// The single exclusive path: closes the commit gate across the whole
    /// operation; the cold owner covers the body and is released before the
    /// real confirmation wait; publication or confirmation failure fails the
    /// holder closed.
    /// C2 recursive write coordination entry (design §8.1): one ordinary
    /// admission, bounded planning, one stripe-sorted inode-lock batch,
    /// optional location locks (worker stripes before block stripes), the
    /// final revalidation, and the body run under a single
    /// `MetadataConfirmation` declared after the view/lock bindings. Only a
    /// side-effect-free planning/coverage failure yields the typed
    /// `Fallback` — the exclusive cold bridge then runs the original body.
    /// A real body error propagates verbatim and is never retried.
    ///
    /// `parent_mode`: Write for deletes (mutated parent edge), Read for
    /// set_attr/free-style parent use. `need_locations`: recursive free and
    /// a permanent delete that may drop the last link take the full
    /// inode → worker → block lock order (§6).
    fn run_recursive_metadata_write<R>(
        &self,
        epoch: u64,
        path: &str,
        parent_mode: InodeLockMode,
        need_locations: bool,
        body: impl FnOnce(&FsDir) -> FsResult<R>,
    ) -> FsResult<ShardedOutcome<R>> {
        // R5: pre-body churn retries (≤3) must re-run the PLANNING, not the
        // body — the body slot keeps the FnOnce available until an actual
        // execution consumes it.
        self.run_recursive_metadata_write_impl(
            epoch,
            path,
            parent_mode,
            need_locations,
            &mut Some(body),
        )
    }

    fn run_recursive_metadata_write_impl<R>(
        &self,
        epoch: u64,
        path: &str,
        parent_mode: InodeLockMode,
        need_locations: bool,
        body_slot: &mut Option<impl FnOnce(&FsDir) -> FsResult<R>>,
    ) -> FsResult<ShardedOutcome<R>> {
        // R8 epoch boundary: the ORIGINAL epoch comes from the single
        // admission in `run_metadata_write_with_fallback` and is passed
        // explicitly — never re-sampled here. The recursive coordination
        // re-verifies THIS epoch under the locks, so a role fence between
        // admission and the body rejects with the caller's own epoch.
        // §8.3: with the journal enabled and a bounded queue, holding
        // subtree locks across enqueue can self-deadlock against the
        // SenderTask (R→queue→SenderTask→A apply→R). Only a disabled
        // journal or a real unbounded channel (cap == 0, captured at
        // construction) may take the fast path; anything else falls back
        // before any body side effect.
        if !self.journal_writer.queue_never_blocks() {
            return Ok(ShardedOutcome::Fallback);
        }

        const MAX_RETRIES: usize = 3;
        for _ in 0..MAX_RETRIES {
            // Bounded planning over a short view; released before locks.
            let plan = {
                let fs_dir = self.fs_dir.view()?;
                match SubtreeWritePlan::capture(&fs_dir, path, parent_mode) {
                    Ok(Some(plan)) => plan,
                    // Unresolvable path or budget overflow: typed fallback
                    // before any side effect.
                    Ok(None) => return Ok(ShardedOutcome::Fallback),
                    Err(e) => return Err(e),
                }
            };
            let planned_epoch = {
                let fs_dir = self.fs_dir.view()?;
                Self::checked_live_epoch(&fs_dir)?
            };
            let requests = plan.inode_requests();

            // R7 observation checkpoint: capture is complete and the locks are
            // not yet acquired — the driver may mutate the live tree here
            // (move/create/EXCHANGE) to exercise the post-capture revalidation.
            // take() disarms the hook after one pause.
            #[cfg(test)]
            if let Some((reached, release)) = self.journal_writer.consume_plan_pause_hook() {
                let _ = reached.send(());
                let _ = release.recv_timeout(std::time::Duration::from_secs(60));
            }

            // Inode locks: one stripe-sorted batch (§4).
            let inode_locks = self.inode_locks.lock_many(requests);

            // Location sampling with a short view while inode locks are
            // held, then the view is dropped before waiting for location
            // locks (§6: no NamespaceGuard is held while waiting for any
            // object lock). The location budget mirrors the plan budget;
            // exceeding it is a pre-body typed fallback.
            // R5: the location budget is independent of the plan budget
            // and charges every actual entry (block + worker-location)
            // against SUBTREE_LOCATION_LIMIT (design §6).
            // F3: the shared SubtreeLocationPlan replaces the hand-written
            // sampler — one whole-call budget counter (SUBTREE_LOCATION_
            // LIMIT), bounded block iteration and location reads, owned IDs
            // only.
            let location_plan_and_locks = if need_locations {
                {
                    let fs_dir = self.fs_dir.view()?;
                    if Self::checked_live_epoch(&fs_dir)? != planned_epoch {
                        drop(inode_locks);
                        return Ok(ShardedOutcome::Fallback);
                    }
                    // Static budget overflow: immediate typed fallback
                    // (no futile retry).
                    let Some(plan) = SubtreeLocationPlan::capture(&fs_dir, requests)? else {
                        drop(inode_locks);
                        return Ok(ShardedOutcome::Fallback);
                    };
                    // R7 location checkpoint: sampling is complete, the
                    // worker/block stripes are not yet acquired — the driver
                    // may add a location here to prove the post-lock coverage
                    // check re-plans rather than accepting the stale sample.
                    #[cfg(test)]
                    if let Some((reached, release)) =
                        self.journal_writer.consume_location_pause_hook()
                    {
                        let _ = reached.send(());
                        let _ = release.recv_timeout(std::time::Duration::from_secs(60));
                    }
                    let locks = self
                        .block_location_locks
                        .write_workers_blocks(plan.worker_ids(), plan.block_ids());
                    Some((plan, locks))
                }
            } else {
                None
            };
            let (location_plan, location_locks) = match location_plan_and_locks {
                Some((plan, locks)) => (Some(plan), Some(locks)),
                None => (None, None),
            };

            // The commit scope opens before the body so every journal
            // entry the body enqueues joins this thread's scope; the real
            // wait happens after all object locks drop (§8.1 step 5).
            let scope = self.journal_writer.begin_commit_scope();

            // Final view: epoch/identity/token revalidation and then the
            // body under the single confirmation guard declared after the
            // view/lock bindings (§8.1 step 3), so an unwind inside the
            // body goes terminal before the resources drop.
            // (op, guard) leave the protected scope together (§8.1 step 4);
            // a pre-body churn returns Ok(None) with no guard at all.
            let op = (|| -> FsResult<Option<(FsResult<R>, MetadataConfirmation<'_>)>> {
                let fs_dir = self.fs_dir.view()?;
                if Self::checked_live_epoch(&fs_dir)? != planned_epoch {
                    return Ok(None);
                }
                if plan.revalidate(&fs_dir, path)?.is_none() {
                    return Ok(None);
                }

                // R3: location coverage revalidation under the FINAL view.
                // A report/replication writer holds only worker/block
                // stripes (no inode locks), so it can add a location between
                // sampling and lock acquisition. Re-read the current
                // block/worker set of every Write inode: any id not in the
                // sampled set means the acquired location locks do not cover
                // the live state — return None (release view/L/I, retry ≤3,
                // then fallback) rather than running the body against
                // unprotected locations.
                // R3/F3: location coverage revalidation under the FINAL
                // view — SubtreeLocationPlan::covers re-reads the current
                // block/worker set and checks it is fully covered by the
                // sampled set. New ids → churn (release view/L/I, retry
                // ≤3, then fallback).
                if need_locations {
                    let plan = location_plan
                        .as_ref()
                        .expect("need_locations implies a location plan");
                    if !plan.covers(&fs_dir, requests)? {
                        return Ok(None);
                    }
                }

                // F1: the ORIGINAL journal epoch is re-verified under the
                // final view after all revalidation and before the body —
                // a role fence between admission and this point rejects with
                // the caller's own epoch, with no body and no holder
                // retirement.
                self.journal_writer.ensure_metadata_epoch(epoch)?;

                // §8.1 step 3: the guard lives in the body's lexical scope.
                let mut confirmation = MetadataConfirmation {
                    handle: Some(&self.fs_dir),
                    writer: &self.journal_writer,
                };
                let failure_seq_before = self.journal_writer.failure_seq();

                // §8.1 step 4: capture the whole Result; never `?` out of
                // the protected scope. A real op error with a moved failure
                // seq goes terminal while the locks are still held, and the
                // guard leaves the scope together with the result. The body
                // slot is consumed exactly here — a pre-body churn never
                // touched it, so the retry loop can plan again.
                let Some(body) = body_slot.take() else {
                    return err_box!("recursive body already consumed");
                };
                let op = body(&fs_dir);
                if op.is_err() && self.journal_writer.failure_seq() != failure_seq_before {
                    confirmation.fail_terminal();
                }
                Ok(Some((op, confirmation)))
            })();

            // Distinguish "pre-body coverage churn" (inner Ok(None)) from a
            // completed body: churn releases everything and RETRIES (≤3, R5)
            // — only planning-budget overflow exits to the exclusive bridge
            // immediately. The scope is empty (no entry/receipt), so ending
            // it is a no-op (§8.1 step 2).
            let (op, mut confirmation) = match op {
                Ok(Some((op, confirmation))) => (op, confirmation),
                Ok(None) => {
                    drop(location_locks);
                    drop(inode_locks);
                    drop(scope);
                    continue;
                }
                Err(e) => {
                    drop(location_locks);
                    drop(inode_locks);
                    drop(scope);
                    return Err(e);
                }
            };

            // §8.1 step 5: resources drop in order — final view, location
            // locks, inode locks — with the confirmation guard surviving to
            // the confirmation wait. The admission slot is still held;
            // scope.wait runs outside every object lock.
            drop(location_locks);
            drop(inode_locks);

            // §8.1 step 5: the real confirmation. Ok disarms; Err (or an
            // unwind inside the wait) is terminalized by the guard's Drop
            // before the slot ends. The scope must be the one opened by
            // this admission — reuse the same epoch.
            let commit = scope.wait();
            if commit.is_ok() {
                confirmation.disarm();
            }
            drop(confirmation);

            // §8.1 step 6: op Err wins; op Ok + commit Err returns the
            // commit error; both Ok returns the value. A pure validation /
            // uncommitted store error (no seq move, confirmation Ok) keeps
            // the holder usable.
            return match (op, commit) {
                (Err(op_err), _) => Err(op_err),
                (Ok(value), Err(commit_err)) => Err(commit_err),
                (Ok(value), Ok(())) => Ok(ShardedOutcome::Done(value)),
            };
        }
        // Retries exhausted on pre-body churn: the exclusive bridge owns it.
        Ok(ShardedOutcome::Fallback)
    }

    fn run_metadata_write_exclusive<R>(
        &self,
        epoch: u64,
        body: impl FnOnce(&mut FsDir) -> FsResult<R>,
    ) -> FsResult<R> {
        loop {
            if let Some(_gate) = self.metadata_commit_gate.close_and_enter_if_open() {
                let scope = self.journal_writer.begin_commit_scope();
                let failure_seq_before = self.journal_writer.failure_seq();
                // A held owner (a legitimate long cold read) must not turn
                // this into an unbounded close-gate/open-gate spin: the typed
                // Busy error propagates with the original epoch preserved.
                let mut cold = self.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT)?;
                // Re-verify the request's original epoch once the exclusive
                // window is secured: a role fence between normal admission and
                // this point must reject the operation with its own epoch.
                if let Err(e) = self.journal_writer.ensure_metadata_epoch(epoch) {
                    // Keep the original epoch-fence error.
                    drop(cold);
                    drop(_gate);
                    return Err(e);
                }
                // The confirmation guard is armed while the cold owner still
                // holds the unique view: a confirmation error — or a panic in
                // the wait — goes terminal before the gate guard is released.
                let mut confirmation = MetadataConfirmation {
                    handle: Some(&self.fs_dir),
                    writer: &self.journal_writer,
                };
                let result = body(&mut cold);
                // Terminal check while the cold owner still holds the unique
                // view: a publication failure recorded during the body must
                // fence the holder before the half-published view reopens.
                if result.is_err() && self.journal_writer.failure_seq() != failure_seq_before {
                    // The guard's own terminal path retires the taken view,
                    // fences the writer and removes the slot.
                    cold.fail_closed();
                    // The exclusive gate stays closed across the real wait
                    // even for a FAILED holder: snapshot/restore rely on this
                    // static boundary, not on RPC view rejection.
                    let commit = scope.wait();
                    confirmation.disarm();
                    drop(_gate);
                    // The original operation error keeps its variant and
                    // context; op Err takes priority over a commit error.
                    return match (result, commit) {
                        (Err(op_err), _) => Err(op_err),
                        (Ok(_), Err(commit_err)) => Err(commit_err),
                        (Ok(value), Ok(())) => Ok(value),
                    };
                }
                // Body complete (or a pure validation/store error that allows
                // backfill): release the cold owner before the confirmation
                // wait. The exclusive gate stays closed across the wait, so
                // RPC writes, snapshots and restores cannot cross it; the
                // applier can take the view to advance the real ack.
                drop(cold);
                let commit = scope.wait();
                // Only a confirmed wait disarms the guard; a confirmation
                // error stays armed so the Drop below goes terminal before
                // the gate opens.
                if commit.is_ok() {
                    confirmation.disarm();
                }
                drop(confirmation);
                match (result, commit) {
                    (Ok(value), Ok(())) => {
                        // Release the gate first: the snapshot path re-closes
                        // the gate and would deadlock against an in-flight
                        // slot if called earlier.
                        drop(_gate);
                        self.maybe_create_periodic_snapshot(epoch);
                        return Ok(value);
                    }
                    (Ok(_), Err(commit_err)) => {
                        drop(_gate);
                        return Err(commit_err);
                    }
                    (Err(op_err), Ok(())) => {
                        // Pure validation/planning/store error: normal
                        // backfill via journal replay; the handle reopens.
                        drop(_gate);
                        return Err(op_err);
                    }
                    (Err(op_err), Err(_commit_err)) => {
                        // Publication also failed: keep the operation error
                        // as the primary cause.
                        drop(_gate);
                        return Err(op_err);
                    }
                }
            }
            // Gate already closed (snapshot/role fence in progress): wait for
            // it to reopen, then retry the exclusive attempt.
            let _guard = self.metadata_commit_gate.enter();
            drop(_guard);
        }
    }

    fn maybe_create_periodic_snapshot(&self, epoch: u64) {
        // Only include confirmed writes; snapshot failure cannot undo an RPC.
        if self.journal_writer.periodic_snapshot_due() {
            if let Some(_snapshot_guard) = self.metadata_commit_gate.close_and_enter_if_open() {
                if self.journal_writer.ensure_metadata_epoch(epoch).is_ok() {
                    match self.fs_dir.view() {
                        Ok(fs_dir) => {
                            if let Err(error) = self.journal_writer.maybe_emit_snapshot(&fs_dir) {
                                warn!("periodic metadata snapshot failed: {}", error);
                            }
                        }
                        Err(e) => {
                            warn!("periodic metadata snapshot: namespace unavailable: {e}");
                        }
                    }
                }
            }
        }
    }

    // Plan from live child edges rather than reading store rows before taking
    // the inode locks. The IDs are only a plan and are revalidated against the
    // live tree under the locks, so no RocksDB snapshot (which takes the DB
    // mutex) or inode decode is needed here. Files have no edges, so the walk
    // stops at a file component.
    fn resolve_edge_ids(&self, path: &str) -> FsResult<(Vec<i64>, usize, bool, u64)> {
        let fs_dir = self.fs_dir.view()?;
        Self::resolve_edge_ids_on(&fs_dir, path)
    }

    /// Edge-id planning against a caller-supplied namespace borrow: the
    /// admitted closure receives the view (normal) or cold handle (exclusive)
    /// so it never re-enters `self.fs_dir` while the owner holds it.
    fn resolve_edge_ids_on(fs_dir: &FsDir, path: &str) -> FsResult<(Vec<i64>, usize, bool, u64)> {
        let components = InodeView::path_components(path)?;
        if components.last().is_none_or(String::is_empty) {
            return err_box!("Path {} is invalid", path);
        }
        Self::check_not_pending_path(path)?;
        let epoch = Self::checked_live_epoch(fs_dir)?;
        fs_dir.store.ensure_available()?;
        // Plan from live child edges only: the fs_dir guard excludes global
        // restore windows (sharded writers still hold fs_dir.read and do not
        // wait on this walk), each edge is read under DirectoryChildren's own
        // shard lock and kept alive by its Arc, and the walk may straddle
        // topology changes. Correctness comes from the single stripe-ordered
        // lock acquisition afterwards plus the epoch, length and per-edge
        // identity revalidation under those locks — not from this guard.
        let mut ids = Vec::with_capacity(components.len());
        let mut next = Some(ChildEdge::Dir(fs_dir.root_node()));
        for (index, _) in components.iter().enumerate() {
            let Some(edge) = next else { break };
            ids.push(edge.id());
            if index + 1 == components.len() {
                break;
            }
            next = match edge {
                ChildEdge::Dir(node) => node.children.get_child_edge(&components[index + 1]),
                // Files have no edges: the walk stops at a file component.
                ChildEdge::FileId(_) => None,
            };
        }
        let full = ids.len() == components.len();
        Ok((ids, components.len(), full, epoch))
    }

    #[inline]
    fn checked_live_epoch(fs_dir: &FsDir) -> FsResult<u64> {
        let control = fs_dir.store.scan_control();
        if control.is_restoring() {
            return err_box!(
                "metadata scan lease rejected: snapshot restore is pending or in progress"
            );
        }
        Ok(control.cancel_epoch())
    }

    /// Root directory state is internally synchronized, so sharded paths only
    /// retain an explicit root mutation write request.
    fn filter_root_requests(
        requests: &mut SmallVec<[InodeLockRequest; 8]>,
        retain_root_write: bool,
    ) {
        requests.retain(|request| {
            request.inode_id != ROOT_INODE_ID
                || (retain_root_write && matches!(request.mode, InodeLockMode::Write))
        });
    }

    fn lock_sharded_requests(
        &self,
        mut requests: SmallVec<[InodeLockRequest; 8]>,
        retain_root_write: bool,
    ) -> InodeLockSet<'_> {
        Self::filter_root_requests(&mut requests, retain_root_write);
        self.inode_locks.lock_many(&requests)
    }

    // The caller holds every planned inode lock throughout this traversal.
    fn resolve_locked_path(
        fs_dir: &FsDir,
        path: &str,
        ids: &[i64],
        components_len: usize,
    ) -> FsResult<Option<InodePath>> {
        let result = InodePath::resolve_locked(
            fs_dir.root_node(),
            path,
            &fs_dir.store,
            ids,
            components_len,
        )?;
        Self::check_not_pending_path(path)?;
        Ok(result)
    }

    fn mkdir_single_parent_sharded(&self, path: &str, opts: MkdirOpts) -> FsResult<FileStatus> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
            Self::check_not_pending_path(path)?;
            Self::check_not_reserved_path(path)?;
            if components_len <= 1 {
                return err_box!("Not allowed to create existing root path: {}", path);
            }
            let parent_index = ids.len().saturating_sub(1);
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, inode_id)| {
                    if !full && depth == parent_index {
                        InodeLockRequest::write(depth, *inode_id)
                    } else {
                        InodeLockRequest::read(depth, *inode_id)
                    }
                })
                .collect();
            let _locks = self.lock_sharded_requests(requests, !full && parent_index == 0);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            if inp.is_full() {
                if opts.create_parent {
                    let inode = inp
                        .get_last_inode()
                        .ok_or_else(|| FsError::file_not_found(path))?;
                    if inode.is_dir() {
                        return Ok(inode.to_file_status(path)?);
                    }
                }
                return err_ext!(FsError::file_exists(path));
            }
            if !opts.create_parent {
                Self::check_parent(&inp)?;
            }
            let inp = fs_dir.mkdir(inp, opts.clone())?;
            return fs_dir.file_status(&inp);
        }

        err_ext!(FsError::common(format!(
            "namespace changed while creating {}",
            path
        )))
    }

    fn create_single_parent_sharded_with_plan(
        &self,
        path: &str,
        opts: CreateFileOpts,
        flags: OpenFlags,
        initial: (Vec<i64>, usize, bool, u64),
    ) -> FsResult<ShardedOutcome<(FileStatus, Option<DeleteResult>)>> {
        const MAX_RETRIES: usize = 3;
        let mut initial = Some(initial);

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match initial.take() {
                Some(resolved) => resolved,
                None => self.resolve_edge_ids(path)?,
            };
            Self::check_not_reserved_path(path)?;
            if full {
                {
                    let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                        .iter()
                        .enumerate()
                        .map(|(depth, id)| InodeLockRequest::read(depth, *id))
                        .collect();
                    let _locks = self.lock_sharded_requests(requests, false);
                    let fs_dir = self.fs_dir.view()?;
                    if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                        continue;
                    }
                    let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)?
                    else {
                        continue;
                    };
                    let Some(inode) = inp.get_last_inode() else {
                        continue;
                    };
                    if inode.is_dir() {
                        return err_box!("{}  already exists as a dir", inp.path());
                    }
                    if flags.exclusive() || !flags.overwrite() {
                        return err_ext!(FsError::file_exists(inp.path()));
                    }
                }
                if let ShardedOutcome::Done(result) = self.overwrite_file_sharded(
                    path,
                    opts.clone(),
                    Some((ids, components_len, full, restore_epoch, None)),
                )? {
                    return Ok(ShardedOutcome::Done((result.0, Some(result.1))));
                }
                return Ok(ShardedOutcome::Fallback);
            }

            let parent_index = ids.len().saturating_sub(1);
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, inode_id)| {
                    if !full && depth == parent_index {
                        InodeLockRequest::write(depth, *inode_id)
                    } else {
                        InodeLockRequest::read(depth, *inode_id)
                    }
                })
                .collect();
            let _locks = self.lock_sharded_requests(requests, !full && parent_index == 0);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            if !opts.create_parent {
                Self::check_parent(&inp)?;
            }
            let inp = fs_dir.create_file(inp, opts.clone())?;
            return Ok(ShardedOutcome::Done((fs_dir.file_status(&inp)?, None)));
        }

        // Retries exhausted with identity churn: side-effect-free fallback.
        Ok(ShardedOutcome::Fallback)
    }

    fn overwrite_file_sharded(
        &self,
        path: &str,
        opts: CreateFileOpts,
        mut initial: Option<(Vec<i64>, usize, bool, u64, Option<InodeView>)>,
    ) -> FsResult<ShardedOutcome<(FileStatus, DeleteResult)>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch, target) = match initial.take() {
                Some(resolved) => resolved,
                None => {
                    let (ids, components_len, full, epoch) = self.resolve_edge_ids(path)?;
                    (ids, components_len, full, epoch, None)
                }
            };
            if !full || ids.len() < 2 {
                return Ok(ShardedOutcome::Fallback);
            }
            let target_id = *ids.last().expect("full path has a target");
            let target_depth = ids.len() - 1;
            let target = match target {
                Some(target) => Some(target),
                None => {
                    let fs_dir = self.fs_dir.view()?;
                    fs_dir.store.get_inode(target_id, None)?
                }
            };
            let Some(target) = target.filter(|inode| inode.is_file()) else {
                return Ok(ShardedOutcome::Fallback);
            };
            let block_ids = target.as_file_ref()?.block_ids();
            let worker_ids = {
                let fs_dir = self.fs_dir.view()?;
                let mut worker_ids = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => return Ok(ShardedOutcome::Fallback),
                    };
                    worker_ids.extend(locations.into_iter().map(|location| location.worker_id));
                }
                worker_ids.into_iter().collect::<Vec<_>>()
            };
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, inode_id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *inode_id)
                    } else {
                        InodeLockRequest::read(depth, *inode_id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.overwrite_planned",
                    description: "Overwrite path resolved before inode locks",
                context: {"path" => path},
            }
            #[cfg(test)]
            memory_read_gates::park(
                "master.metadata.overwrite_planned",
                std::time::Duration::from_secs(5),
            );
            let _locks = self.lock_sharded_requests(requests, false);
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&worker_ids, &block_ids);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(locked_target) = inp.get_last_inode() else {
                continue;
            };
            if locked_target.id() != target.id()
                || !locked_target.is_file()
                || locked_target.as_file_ref()?.block_ids() != block_ids
            {
                continue;
            }

            let current_workers = {
                let mut current_workers = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => return Ok(ShardedOutcome::Fallback),
                    };
                    current_workers
                        .extend(locations.into_iter().map(|location| location.worker_id));
                }
                current_workers
            };
            if current_workers.len() != worker_ids.len()
                || !worker_ids
                    .iter()
                    .all(|worker_id| current_workers.contains(worker_id))
            {
                continue;
            }

            let deleted = fs_dir.overwrite_file(&inp, opts.clone())?;
            let status = fs_dir.file_status(&inp)?;
            return Ok(ShardedOutcome::Done((status, deleted)));
        }

        Ok(ShardedOutcome::Fallback)
    }

    /// The global-lock create body, run against an owner-provided namespace
    /// (normal: a self-acquired cold handle; exclusive: the owner borrow).
    fn create_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<(FileStatus, Option<DeleteResult>)> {
        let inp = Self::resolve_path(fs_dir, path)?;
        Self::check_not_reserved_path(inp.path())?;
        let last_inode = inp.get_last_inode();
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }
            if flags.exclusive() {
                return err_ext!(FsError::file_exists(inp.path()));
            }
        }
        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }
        let (inp, deleted) = if last_inode.is_some() {
            if !flags.overwrite() {
                return err_ext!(FsError::file_exists(inp.path()));
            }
            let deleted = fs_dir.overwrite_file(&inp, opts)?;
            (inp, Some(deleted))
        } else {
            (fs_dir.create_file(inp, opts)?, None)
        };
        Ok((fs_dir.file_status(&inp)?, deleted))
    }

    /// The caller resolves once in `delete_with_mode`; this function reuses
    /// that result for the lock request set instead of walking the tree again.
    fn delete_single_parent_sharded(
        &self,
        path: &str,
        ids: Vec<i64>,
        components_len: usize,
        restore_epoch: u64,
    ) -> FsResult<ShardedOutcome<DeleteResult>> {
        const MAX_RETRIES: usize = 3;
        let mut ids = Some(ids);

        for _ in 0..MAX_RETRIES {
            let Some(ids) = ids.take() else {
                return Ok(ShardedOutcome::Fallback);
            };
            if ids.len() != components_len || ids.len() < 2 {
                return Ok(ShardedOutcome::Fallback);
            }

            let target_index = ids.len() - 1;
            let parent_index = target_index - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, inode_id)| {
                    if depth == parent_index || depth == target_index {
                        InodeLockRequest::write(depth, *inode_id)
                    } else {
                        InodeLockRequest::read(depth, *inode_id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.delete_planned",
                    description: "Delete path resolved before inode locks",
                context: {"path" => path},
            }
            #[cfg(test)]
            memory_read_gates::park(
                "master.metadata.delete_planned",
                std::time::Duration::from_secs(5),
            );
            let _locks = self.lock_sharded_requests(requests, parent_index == 0);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(target) = inp.get_last_inode() else {
                drop(fs_dir);
                drop(_locks);
                return Ok(ShardedOutcome::Fallback);
            };
            let block_ids = target
                .as_file_ref()
                .ok()
                .map(|file| file.block_ids())
                .unwrap_or_default();
            // An empty block list has no locations to fence: sampling would
            // build two empty sets and take the same no-op locks.
            if block_ids.is_empty() {
                return Ok(ShardedOutcome::Done(fs_dir.delete(&inp, false)?));
            }
            let worker_ids = {
                let mut worker_ids = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => {
                            drop(fs_dir);
                            drop(_locks);
                            return Ok(ShardedOutcome::Fallback);
                        }
                    };
                    worker_ids.extend(locations.into_iter().map(|location| location.worker_id));
                }
                worker_ids.into_iter().collect::<Vec<_>>()
            };
            // Never wait for a location lock while holding the namespace read
            // guard: worker cleanup takes these locks before the write guard.
            drop(fs_dir);
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.locations_sampled",
                description: "Worker locations sampled before inode locks",
                context: {"path" => path},
            }
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&worker_ids, &block_ids);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(target) = inp.get_last_inode() else {
                continue;
            };
            if target
                .as_file_ref()
                .ok()
                .map(|file| file.block_ids())
                .unwrap_or_default()
                != block_ids
            {
                continue;
            }
            let current_workers = {
                let mut current_workers = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => {
                            drop(fs_dir);
                            drop(_location_locks);
                            drop(_locks);
                            return Ok(ShardedOutcome::Fallback);
                        }
                    };
                    current_workers
                        .extend(locations.into_iter().map(|location| location.worker_id));
                }
                current_workers
            };
            if current_workers.len() != worker_ids.len()
                || !worker_ids
                    .iter()
                    .all(|worker_id| current_workers.contains(worker_id))
            {
                continue;
            }
            return Ok(ShardedOutcome::Done(fs_dir.delete(&inp, false)?));
        }

        Ok(ShardedOutcome::Fallback)
    }

    fn rename_sharded(
        &self,
        src: &str,
        dst: &str,
        flags: RenameFlags,
        initial: ((Vec<i64>, usize, bool, u64), (Vec<i64>, usize, bool, u64)),
    ) -> FsResult<ShardedOutcome<bool>> {
        const MAX_RETRIES: usize = 3;
        let mut initial = Some(initial);

        for _ in 0..MAX_RETRIES {
            let (
                (source_ids, source_components, source_full, source_epoch),
                (destination_ids, destination_components, destination_full, destination_epoch),
            ) = match initial.take() {
                Some(resolved) => resolved,
                None => (self.resolve_edge_ids(src)?, self.resolve_edge_ids(dst)?),
            };
            if !source_full
                || destination_full
                || destination_ids.len() + 1 < destination_components
                || source_ids.len() < 2
            {
                // Side-effect-free internal fallback: needs the global lock.
                return Ok(ShardedOutcome::Fallback);
            }

            let source_parent = source_ids.len() - 2;
            let source_target = source_ids.len() - 1;
            let destination_parent = destination_ids.len().saturating_sub(1);
            let mut requests = SmallVec::<[InodeLockRequest; 8]>::with_capacity(
                source_ids.len() + destination_ids.len(),
            );
            requests.extend(source_ids.iter().enumerate().map(|(depth, inode_id)| {
                if depth == source_parent || depth == source_target {
                    InodeLockRequest::write(depth, *inode_id)
                } else {
                    InodeLockRequest::read(depth, *inode_id)
                }
            }));
            requests.extend(destination_ids.iter().enumerate().map(|(depth, inode_id)| {
                if depth == destination_parent {
                    InodeLockRequest::write(depth, *inode_id)
                } else {
                    InodeLockRequest::read(depth, *inode_id)
                }
            }));
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.rename_planned",
                    description: "Rename paths resolved before inode locks",
                context: {"path" => src},
            }
            #[cfg(test)]
            memory_read_gates::park(
                "master.metadata.rename_planned",
                std::time::Duration::from_secs(5),
            );
            let _locks =
                self.lock_sharded_requests(requests, source_parent == 0 || destination_parent == 0);

            let fs_dir = self.fs_dir.view()?;
            let epoch = Self::checked_live_epoch(&fs_dir)?;
            if source_epoch != epoch || destination_epoch != epoch {
                continue;
            }
            let Some(source_path) =
                Self::resolve_locked_path(&fs_dir, src, &source_ids, source_components)?
            else {
                continue;
            };
            let Some(destination_path) =
                Self::resolve_locked_path(&fs_dir, dst, &destination_ids, destination_components)?
            else {
                continue;
            };
            fs_dir.rename(&source_path, &destination_path, flags)?;
            return Ok(ShardedOutcome::Done(true));
        }

        // Retries exhausted with identity churn: side-effect-free, needs the
        // global lock.
        Ok(ShardedOutcome::Fallback)
    }

    fn set_attr_single_path_sharded(
        &self,
        path: &str,
        opts: SetAttrOpts,
        initial: (Vec<i64>, usize, bool, u64),
    ) -> FsResult<ShardedOutcome<FileStatus>> {
        const MAX_RETRIES: usize = 3;
        let mut initial = Some(initial);

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match initial.take() {
                Some(resolved) => resolved,
                None => self.resolve_edge_ids(path)?,
            };
            if !full {
                // Side-effect-free planning point: an unresolved path needs
                // the global lock (exclusive owner body).
                return Ok(ShardedOutcome::Fallback);
            }

            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            let _locks = self.lock_sharded_requests(requests, target_depth == 0);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            return Ok(ShardedOutcome::Done(fs_dir.set_attr(inp, opts)?));
        }

        // Retries exhausted with identity churn: side-effect-free fallback.
        Ok(ShardedOutcome::Fallback)
    }

    fn complete_file_sharded(
        &self,
        path: &str,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: &str,
        options: CompleteFileOptions,
    ) -> FsResult<ShardedOutcome<Option<FileBlocks>>> {
        const MAX_RETRIES: usize = 3;

        let block_ids: Vec<_> = commit_blocks.iter().map(|block| block.block_id).collect();
        let worker_ids: Vec<_> = commit_blocks
            .iter()
            .flat_map(|block| block.locations.iter().map(|location| location.worker_id))
            .collect();

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
            if !full || ids.len() < 2 || inode_id.is_some_and(|id| ids.last() != Some(&id)) {
                // Side-effect-free planning point: needs the global lock.
                return Ok(ShardedOutcome::Fallback);
            }

            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.complete_planned",
                    description: "Complete path resolved before inode locks",
                context: {"path" => path},
            }
            let _inode_locks = self.lock_sharded_requests(requests, false);
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&worker_ids, &block_ids);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let mut inode = inp
                .get_last_inode()
                .ok_or_else(|| FsError::file_not_found(path))?;
            if !inode.is_file() || inode_id.is_some_and(|id| id != inode.id()) {
                // Identity mismatch: side-effect-free fallback (locks drop).
                return Ok(ShardedOutcome::Fallback);
            }

            // Success returns immediately: nothing after this point reuses
            // the arguments, so the single-shot path can move them.
            let completed = fs_dir.complete_file(
                path,
                &mut inode,
                len,
                commit_blocks,
                client_name,
                options.only_flush,
                options.set_attr_opts,
            )?;

            if options.only_flush && options.return_file_blocks {
                let file = inode.as_file_ref()?;
                let locs = self.get_block_locs(path, &fs_dir, file)?;
                let status = inode.to_file_status(path)?;
                let mut blocks = FileBlocks::new(status, locs);
                blocks.status.is_complete = completed;
                return Ok(ShardedOutcome::Done(Some(blocks)));
            }
            return Ok(ShardedOutcome::Done(None));
        }

        // Retries exhausted with identity churn: side-effect-free fallback.
        Ok(ShardedOutcome::Fallback)
    }

    fn complete_file_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: &str,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        // By-id/stale-path contract: the id resolves from the store even when
        // the request path has since been renamed.
        let mut inode = Self::resolve_file_inode(fs_dir, path, inode_id)?;
        let completed = fs_dir.complete_file(
            path,
            &mut inode,
            len,
            commit_blocks,
            client_name,
            options.only_flush,
            options.set_attr_opts,
        )?;
        if options.only_flush && options.return_file_blocks {
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, fs_dir, file)?;
            let status = inode.to_file_status(path)?;
            let mut blocks = FileBlocks::new(status, locs);
            blocks.status.is_complete = completed;
            return Ok(Some(blocks));
        }
        Ok(None)
    }

    fn close_file_sharded(&self, path: &str, client_name: &str) -> FsResult<ShardedOutcome<()>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, _, restore_epoch) = match self.resolve_edge_ids(path) {
                Ok(value) => value,
                Err(error) if matches!(error.kind(), ErrorKind::FileNotFound) => {
                    return Ok(ShardedOutcome::Done(()))
                }
                Err(error) => return Err(error),
            };
            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.close_planned",
                    description: "Close path resolved before inode locks",
                context: {"path" => path},
            }
            let _locks = self.lock_sharded_requests(requests, false);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let inp = match Self::resolve_locked_path(&fs_dir, path, &ids, components_len) {
                Ok(Some(inp)) => inp,
                Ok(None) => continue,
                Err(error) if matches!(error.kind(), ErrorKind::FileNotFound) => {
                    return Ok(ShardedOutcome::Done(()))
                }
                Err(error) => return Err(error),
            };
            fs_dir.close_writer(&inp, client_name)?;
            return Ok(ShardedOutcome::Done(()));
        }

        // Planning point: close needs the global lock.
        Ok(ShardedOutcome::Fallback)
    }

    fn close_file_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        client_name: &str,
    ) -> FsResult<()> {
        let Ok(inp) = Self::resolve_path(fs_dir, path) else {
            return Ok(());
        };
        fs_dir.close_writer(&inp, client_name)
    }

    fn symlink_single_parent_sharded(
        &self,
        target: &str,
        link: &str,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
        initial: (Vec<i64>, usize, bool, u64),
    ) -> FsResult<ShardedOutcome<()>> {
        const MAX_RETRIES: usize = 3;
        let mut initial = Some(initial);
        #[cfg(test)]
        let plan_gate = format!("symlink-plan-{}", link);

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match initial.take() {
                Some(resolved) => resolved,
                None => self.resolve_edge_ids(link)?,
            };
            if full || ids.len() + 1 < components_len {
                return Ok(ShardedOutcome::Fallback);
            }

            let parent_index = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == parent_index {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            // Test-only interleave: parked before any inode lock, so the
            // driver's sharded create of the destination cannot deadlock on
            // us; on resume the is_full check forces the fallback contract.
            #[cfg(test)]
            memory_read_gates::park(&plan_gate, std::time::Duration::from_secs(10));
            let _locks = self.lock_sharded_requests(requests, parent_index == 0);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, link, &ids, components_len)? else {
                continue;
            };
            if inp.is_full() {
                drop(fs_dir);
                drop(_locks);
                return Ok(ShardedOutcome::Fallback);
            }
            fs_dir.symlink(
                target.to_string(),
                inp,
                false,
                mode,
                owner.clone(),
                group.clone(),
            )?;
            return Ok(ShardedOutcome::Done(()));
        }

        // Retries exhausted with identity churn: side-effect-free fallback.
        Ok(ShardedOutcome::Fallback)
    }

    fn link_single_parent_sharded(
        &self,
        src: &str,
        dst: &str,
        initial: ((Vec<i64>, usize, bool, u64), (Vec<i64>, usize, bool, u64)),
    ) -> FsResult<ShardedOutcome<()>> {
        const MAX_RETRIES: usize = 3;
        let mut initial = Some(initial);

        for _ in 0..MAX_RETRIES {
            let (
                (source_ids, source_components, source_full, source_epoch),
                (destination_ids, destination_components, destination_full, destination_epoch),
            ) = match initial.take() {
                Some(resolved) => resolved,
                None => (self.resolve_edge_ids(src)?, self.resolve_edge_ids(dst)?),
            };
            if !source_full
                || destination_full
                || destination_ids.len() + 1 < destination_components
            {
                return Ok(ShardedOutcome::Fallback);
            }

            let mut requests = smallvec::SmallVec::<[InodeLockRequest; 8]>::with_capacity(
                source_ids.len() + destination_ids.len(),
            );
            requests.extend(source_ids.iter().enumerate().map(|(depth, id)| {
                if depth + 1 == source_ids.len() {
                    InodeLockRequest::write(depth, *id)
                } else {
                    InodeLockRequest::read(depth, *id)
                }
            }));
            let destination_parent = destination_ids.len() - 1;
            requests.extend(destination_ids.iter().enumerate().map(|(depth, id)| {
                if depth == destination_parent {
                    InodeLockRequest::write(depth, *id)
                } else {
                    InodeLockRequest::read(depth, *id)
                }
            }));
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.link_planned",
                    description: "Link paths resolved before inode locks",
                context: {"path" => src},
            }
            #[cfg(test)]
            memory_read_gates::park(
                &format!("link-plan-{}", dst),
                std::time::Duration::from_secs(10),
            );
            let _locks = self.lock_sharded_requests(requests, destination_parent == 0);
            let fs_dir = self.fs_dir.view()?;
            let epoch = Self::checked_live_epoch(&fs_dir)?;
            if source_epoch != epoch || destination_epoch != epoch {
                continue;
            }
            let Some(source_path) =
                Self::resolve_locked_path(&fs_dir, src, &source_ids, source_components)?
            else {
                continue;
            };
            let Some(destination_path) =
                Self::resolve_locked_path(&fs_dir, dst, &destination_ids, destination_components)?
            else {
                continue;
            };
            fs_dir.link(source_path, destination_path)?;
            return Ok(ShardedOutcome::Done(()));
        }

        // Retries exhausted with identity churn: side-effect-free fallback.
        Ok(ShardedOutcome::Fallback)
    }

    fn link_with_global_lock_body(&self, fs_dir: &mut FsDir, src: &str, dst: &str) -> FsResult<()> {
        let src_path = Self::resolve_path(fs_dir, src)?;
        let dst_path = Self::resolve_path(fs_dir, dst)?;
        Self::check_not_reserved_path(src_path.path())?;
        Self::check_not_reserved_path(dst_path.path())?;
        fs_dir.link(src_path, dst_path)
    }

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        self.run_metadata_write(|| self.mkdir_single_parent_sharded(path.as_ref(), opts))
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    // Permanently delete a path, bypassing the recycle bin.
    // Internal callers (TTL eviction, quota) count the inode's bytes as
    // reclaimed, so this must never route through the recycle bin.
    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        self.delete_with_mode(path, recursive, DeleteMode::SkipTrash)
    }

    pub fn delete_with_mode<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
        mode: DeleteMode,
    ) -> FsResult<DeleteResult> {
        let mut delete_res = self.run_metadata_write_with_fallback(
            |epoch| {
                let path = path.as_ref();
                let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
                let sharded = matches!(mode, DeleteMode::SkipTrash)
                    && !recursive
                    && full
                    && components_len > 1
                    && path != trash::TRASH_ROOT;
                if sharded {
                    return self.delete_single_parent_sharded(
                        path,
                        ids,
                        components_len,
                        restore_epoch,
                    );
                }
                // R4: only a recursive PERMANENT delete takes the subtree
                // coordination path (§8.4). A trash soft-delete (Default
                // with trash enabled, or AsyncPurge outside the trash) is
                // an internal rename into /.Trash/.pending — the
                // destination directory locks are not in the plan, so it
                // stays on the original cold body. A permanent delete may
                // drop the last link, so it takes the full inode → worker →
                // block order.
                let permanent = matches!(mode, DeleteMode::SkipTrash)
                    || trash::is_trash_path(path)
                    || (matches!(mode, DeleteMode::Default) && !self.conf.trash_enabled);
                if !(recursive && permanent) {
                    return Ok(ShardedOutcome::Fallback);
                }
                // F1: the hot closure receives the outer original epoch
                // explicitly — no re-sampling (G3).
                self.run_recursive_metadata_write(
                    epoch,
                    path,
                    InodeLockMode::Write,
                    true,
                    |locked| self.delete_locked(locked, path, recursive, mode),
                )
            },
            |fs_dir| self.delete_locked(fs_dir, path.as_ref(), recursive, mode),
        )?;
        self.release_deleted_blocks(&mut delete_res);

        Ok(delete_res)
    }

    pub(crate) fn delete_expired_inode(
        &self,
        path: &str,
        inode_id: i64,
        mode: DeleteMode,
    ) -> FsResult<Option<(DeleteResult, InodeView)>> {
        let result = self.run_metadata_write_with_fallback(
            |epoch| {
                if matches!(mode, DeleteMode::SkipTrash) {
                    if let ShardedOutcome::Done(result) =
                        self.delete_expired_inode_sharded(path, inode_id)?
                    {
                        return Ok(ShardedOutcome::Done(Some(result)));
                    }
                }
                // Planning point: expired delete needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| self.delete_expired_inode_with_global_lock_body(fs_dir, path, inode_id, mode),
        )?;
        let Some((mut result, inode)) = result else {
            return Ok(None);
        };
        self.release_deleted_blocks(&mut result);
        Ok(Some((result, inode)))
    }

    fn delete_expired_inode_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        inode_id: i64,
        mode: DeleteMode,
    ) -> FsResult<Option<(DeleteResult, InodeView)>> {
        let inp = Self::resolve_path(fs_dir, path)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(None);
        };
        if inode.id() != inode_id
            || !inode.is_expired()?
            || inode.storage_policy()?.ttl_action != TtlAction::Delete
            || matches!(inode.as_ref(), InodeView::File(file) if file.is_writing())
            || (inode.is_dir() && !inp.is_empty_dir())
        {
            return Ok(None);
        }
        let current = inode.as_ref().clone_metadata();
        self.delete_locked(fs_dir, path, true, mode)
            .map(|result| Some((result, current)))
    }

    fn delete_expired_inode_sharded(
        &self,
        path: &str,
        inode_id: i64,
    ) -> FsResult<ShardedOutcome<(DeleteResult, InodeView)>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
            if !full || ids.len() < 2 || ids.last() != Some(&inode_id) {
                return Ok(ShardedOutcome::Fallback);
            }
            let target = {
                let fs_dir = self.fs_dir.view()?;
                fs_dir.store.get_inode(inode_id, None)?
            };
            let Some(target) = target else {
                return Ok(ShardedOutcome::Fallback);
            };
            if !target.is_expired()?
                || target.storage_policy()?.ttl_action != TtlAction::Delete
                || matches!(&target, InodeView::File(file) if file.is_writing())
            {
                return Ok(ShardedOutcome::Fallback);
            }

            let target_index = ids.len() - 1;
            let parent_index = target_index - 1;
            let block_ids = target
                .as_file_ref()
                .map(|file| file.block_ids())
                .unwrap_or_default();
            let worker_ids = {
                let fs_dir = self.fs_dir.view()?;
                let mut worker_ids = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => return Ok(ShardedOutcome::Fallback),
                    };
                    worker_ids.extend(locations.into_iter().map(|location| location.worker_id));
                }
                worker_ids.into_iter().collect::<Vec<_>>()
            };
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.locations_sampled",
                description: "Worker locations sampled before inode locks",
                context: {"path" => path},
            }
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == parent_index || depth == target_index {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            let _locks = self.lock_sharded_requests(requests, parent_index == 0);
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&worker_ids, &block_ids);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(inode) = inp.get_last_inode() else {
                continue;
            };
            if inode.id() != inode_id
                || inode
                    .as_file_ref()
                    .map(|file| file.block_ids())
                    .unwrap_or_default()
                    != block_ids
            {
                continue;
            }

            let current_workers = {
                let mut current_workers = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => return Ok(ShardedOutcome::Fallback),
                    };
                    current_workers
                        .extend(locations.into_iter().map(|location| location.worker_id));
                }
                current_workers
            };
            if current_workers.len() != worker_ids.len()
                || !worker_ids
                    .iter()
                    .all(|worker_id| current_workers.contains(worker_id))
            {
                continue;
            }
            if inp.path() == trash::TRASH_ROOT {
                return Ok(ShardedOutcome::Fallback);
            }
            let Some(inode) = inp.get_last_inode() else {
                continue;
            };
            if inode.id() != inode_id
                || !inode.is_expired()?
                || inode.storage_policy()?.ttl_action != TtlAction::Delete
                || matches!(inode.as_ref(), InodeView::File(file) if file.is_writing())
                || (inode.is_dir() && !inp.is_empty_dir())
            {
                return Ok(ShardedOutcome::Fallback);
            }
            let current = inode.as_ref().clone_metadata();
            return Ok(ShardedOutcome::Done((fs_dir.delete(&inp, false)?, current)));
        }

        Ok(ShardedOutcome::Fallback)
    }

    /// Variant of `delete_with_mode` that also checks the active-master
    /// monitor before deletion. Role fencing and the commit gate are checked
    /// separately; a demotion can still race an already-admitted operation.
    /// Returns `None` if the monitor is inactive at either check.
    pub fn delete_with_mode_if_active<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
        mode: DeleteMode,
    ) -> FsResult<Option<DeleteResult>> {
        if !self.master_monitor.is_active() {
            return Ok(None);
        }
        let result = self.run_metadata_write_with_fallback(
            |epoch| {
                if !self.master_monitor.is_active() {
                    return Ok(ShardedOutcome::Done(None));
                }
                // Planning point: delete needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                if !self.master_monitor.is_active() {
                    return Ok(None);
                }
                self.delete_locked(fs_dir, path.as_ref(), recursive, mode)
                    .map(Some)
            },
        )?;
        let Some(mut delete_res) = result else {
            return Ok(None);
        };
        self.release_deleted_blocks(&mut delete_res);
        Ok(Some(delete_res))
    }

    fn delete_locked(
        &self,
        fs_dir: &FsDir,
        path: &str,
        recursive: bool,
        mode: DeleteMode,
    ) -> FsResult<DeleteResult> {
        let inp = Self::resolve_path(fs_dir, path)?;

        // Removing the root would orphan every trashed entry. Entries below it
        // stay permanently deletable so the recycle bin can be emptied.
        if inp.path() == trash::TRASH_ROOT {
            return err_ext!(FsError::invalid_argument(
                "the recycle bin root cannot be deleted"
            ));
        }

        // Entries already inside /.Trash are always purged permanently, so the
        // bin cannot be used to hide data from a purge.
        let in_trash = trash::is_trash_path(inp.path());
        let soft_delete = match mode {
            DeleteMode::SkipTrash => false,
            DeleteMode::AsyncPurge => !in_trash,
            DeleteMode::Default => self.conf.trash_enabled && !in_trash,
        };

        // The callee reports how the delete was actually served: a shared inode
        // or an unresolved entry falls back to a permanent unlink even when the
        // trash is enabled, and must not claim to be recoverable or staged.
        let delete_res = match mode {
            DeleteMode::AsyncPurge if soft_delete => fs_dir.trash_pending(&inp, recursive)?,
            _ if soft_delete => fs_dir.trash(&inp, recursive)?,
            _ => fs_dir.delete(&inp, recursive)?,
        };
        Ok(delete_res)
    }

    pub(crate) fn release_deleted_blocks(&self, delete_res: &mut DeleteResult) {
        let blocks = std::mem::take(&mut delete_res.blocks);
        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks,
            trashed: false,
            purge_pending: false,
        });
    }

    /// Applies and journals one bounded trash purge batch.
    ///
    /// The batch is applied before it is journaled, like every other `FsDir`
    /// mutation: `JournalWriter::send` may emit a leader snapshot right after
    /// enqueueing the record, and that checkpoint must already reflect the
    /// purge, otherwise recovery from it would skip the batch and resurrect
    /// inodes whose blocks were already released. `purge_paths` commits the
    /// whole batch atomically, so a failed apply journals nothing. Returns
    /// `None` when this node is no longer the active master.
    pub(crate) fn purge_trash_batch(
        &self,
        entry: &str,
        paths: &[String],
    ) -> FsResult<Option<(u64, u64)>> {
        if !self.master_monitor.is_active() {
            return Ok(None);
        }
        let result = self.run_metadata_write_with_fallback(
            |epoch| {
                if !self.master_monitor.is_active() {
                    return Ok(ShardedOutcome::Done(None));
                }
                // Planning point: a purge batch needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                if !self.master_monitor.is_active() {
                    return Ok(None);
                }
                let mtime = LocalTime::mills() as i64;
                let delete_res = fs_dir.purge_paths(paths, mtime)?;
                let writer = fs_dir.journal_writer.clone();
                writer.log_trash_purge(fs_dir, entry, mtime, paths.to_vec())?;
                Ok(Some(delete_res))
            },
        )?;
        let Some(mut delete_res) = result else {
            return Ok(None);
        };
        self.release_deleted_blocks(&mut delete_res);
        Ok(Some((delete_res.inodes, delete_res.bytes)))
    }

    pub fn list_trash_page(
        &self,
        after: Option<&trash::TrashCursor>,
        limit: usize,
    ) -> FsResult<TrashListing> {
        let mut fs_dir = self.cold_acquire_wait()?;
        fs_dir.list_trash(after, limit)
    }

    pub fn list_trash(&self) -> FsResult<Vec<TrashEntry>> {
        const PAGE_SIZE: usize = 1000;
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_trash_page(cursor.as_ref(), PAGE_SIZE)?;
            entries.extend(page.entries);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(entries),
            }
        }
    }

    pub fn restore_dst<T: AsRef<str>>(&self, path: T, dst: Option<&str>) -> FsResult<String> {
        let mut fs_dir = self.cold_acquire_wait()?;
        fs_dir.restore_dst(path.as_ref(), dst)
    }

    /// Returns one bounded page of expired trash entry paths. The namespace
    /// exclusive lock is held only while scanning that page.
    pub fn scan_expired_trash(
        &self,
        cutoff_ms: i64,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let mut fs_dir = self.cold_acquire_wait()?;
        fs_dir.scan_expired_trash(cutoff_ms, limit, start_after)
    }

    /// Leader-fenced [`FsDir::claim_trash_entry_for_purge`]. Returns
    /// `Ok(None)` when this node is no longer active and `Ok(Some(None))` when
    /// the entry already vanished.
    pub fn claim_trash_entry_for_purge(
        &self,
        trash_path: &str,
    ) -> FsResult<Option<Option<String>>> {
        if !self.master_monitor.is_active() {
            return Ok(None);
        }
        // A journalled rename (trash -> pending staging): route through the
        // trash cold bridge instead of a bare shared view.
        self.run_metadata_write_with_fallback(
            |epoch| {
                if !self.master_monitor.is_active() {
                    return Ok(ShardedOutcome::Done(None));
                }
                // Planning point: the claim rename needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                if !self.master_monitor.is_active() {
                    return Ok(None);
                }
                fs_dir.claim_trash_entry_for_purge(trash_path).map(Some)
            },
        )
    }

    /// One bounded page of `/.Trash/.pending` entries, i.e. entries staged by
    /// `async_delete` that are due for immediate reclamation.
    pub fn scan_pending_trash(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> FsResult<TrashScanPage> {
        let mut fs_dir = self.cold_acquire_wait()?;
        fs_dir.scan_pending_trash(limit, start_after)
    }

    pub fn restore_trash<T: AsRef<str>>(
        &self,
        path: T,
        dst: Option<&str>,
        overwrite: bool,
    ) -> FsResult<RestoredTrashEntry> {
        let restored = self.run_metadata_write_with_fallback(
            |epoch| {
                // Planning point: restore needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| fs_dir.restore_trash(path.as_ref(), dst, overwrite),
        )?;

        // Overwriting an existing destination deletes it; release its blocks
        // just like a regular rename-with-replace.
        if let Some(replaced) = &restored.replaced {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(replaced);
        }

        Ok(restored)
    }

    /// Stages every visible recycle-bin entry for the background purger and
    /// returns how many were accepted. Entries are renamed, not walked, so the
    /// call does not depend on the size of the trashed subtrees.
    ///
    /// Leadership is re-checked under each batch's write lock: a demotion while
    /// the loop is between batches must stop the staging instead of letting a
    /// former leader persist further renames and journal records.
    pub fn stage_trash_for_purge(&self) -> FsResult<u64> {
        const BATCH_SIZE: usize = 1000;

        // Snapshot the names once: entries added after the snapshot stay for the
        // next empty, and each batch releases the write lock between renames.
        let names = self.fs_dir.view()?.trash_entry_names()?;
        let mut staged = 0;
        for batch in names.chunks(BATCH_SIZE) {
            staged += self.run_metadata_write_with_fallback(
                |epoch| {
                    // Planning point: staging renames need the global lock.
                    Ok(ShardedOutcome::Fallback)
                },
                |fs_dir| {
                    if !self.master_monitor.is_active() {
                        return err_ext!(FsError::not_leader(
                            "empty-trash staging lost leadership mid-sweep"
                        ));
                    }
                    fs_dir.stage_trash_entries(batch)
                },
            )?;
        }
        Ok(staged)
    }

    /// Takes ownership of `/.Trash/.pending` for a blocking drain.
    pub(crate) fn lock_pending_drain(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.pending_drain.lock()
    }

    /// Non-blocking variant used by the background purger, which must never
    /// queue behind a blocking empty-trash sweep.
    pub(crate) fn try_lock_pending_drain(&self) -> Option<parking_lot::MutexGuard<'_, ()>> {
        self.pending_drain.try_lock()
    }

    /// Drains `/.Trash/.pending` through the bounded purger until it is empty.
    /// Returns the real reclaimed counts; `blocks` are already handed to the
    /// worker managers by the purge batches themselves.
    pub fn drain_pending_trash(&self) -> FsResult<Option<DeleteResult>> {
        let _guard = self.lock_pending_drain();
        self.drain_pending_trash_owned()
    }

    /// Drain body that assumes the caller already owns the pending directory.
    fn drain_pending_trash_owned(&self) -> FsResult<Option<DeleteResult>> {
        const BATCH_SIZE: usize = 64;
        let purger = crate::master::fs::TrashPurger::new(self.clone(), self.master_monitor.clone());
        let mut total = DeleteResult::new();
        let mut start_after = None;

        loop {
            let page = self.scan_pending_trash(BATCH_SIZE, start_after.as_deref())?;
            for path in page.paths {
                let Some(outcome) = purger.purge_entry(&path)? else {
                    return Ok(None);
                };
                total.inodes += outcome.inodes;
                total.bytes += outcome.bytes;
            }
            match page.next_after {
                Some(next) => start_after = Some(next),
                None => return Ok(Some(total)),
            }
        }
    }

    /// Empties the recycle bin. Returns the number of accepted entries and, in
    /// wait mode, the counts actually reclaimed by the bounded purger.
    ///
    /// Both modes stage the entries first, so there is exactly one purge
    /// implementation and an interrupted sweep resumes from `/.Trash/.pending`
    /// instead of from an in-memory queue.
    pub fn empty_trash0(&self, wait: bool) -> FsResult<(u64, DeleteResult)> {
        if !wait {
            let accepted = self.stage_trash_for_purge()?;
            return Ok((accepted, DeleteResult::new()));
        }

        // Hold the drain lock across staging and draining: the background
        // purger must not reclaim an entry between the rename that stages it and
        // the drain that has to report its totals.
        let _guard = self.lock_pending_drain();
        let accepted = self.stage_trash_for_purge()?;
        match self.drain_pending_trash_owned()? {
            Some(res) => Ok((accepted, res)),
            // The master was demoted mid-sweep. The staged entries stay in the
            // durable pending directory for the next active master, but this
            // call cannot report the blocking totals the caller waits for, so it
            // must fail retryably instead of caching a success with zero counts.
            None => err_ext!(FsError::not_leader(
                "empty-trash lost leadership before the staged entries were reclaimed"
            )),
        }
    }

    /// Empties the recycle bin synchronously, keeping the pre-async contract.
    pub fn empty_trash(&self) -> FsResult<DeleteResult> {
        let (_, res) = self.empty_trash0(true)?;
        Ok(res)
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        match self.free_or_defer(path.as_ref(), recursive)? {
            FreeOutcome::Freed(res) => Ok(res),
            // Reconciliation releases each file once its UFS object is verified.
            FreeOutcome::Deferred { .. } => Ok(FreeResult::default()),
        }
    }

    /// Free `path`, or hand its fs_mode files to reconciliation (transfer mode).
    ///
    /// fs_mode reads a released file from UFS by its current path, so under
    /// transfer mode no fs_mode file is released before its UFS object is
    /// verified. A recursive free of a directory that contains an fs_mode
    /// mount point but is not itself in one is refused: freeing it directly
    /// would release those files unverified.
    pub fn free_or_defer(&self, path: &str, recursive: bool) -> FsResult<FreeOutcome> {
        self.free_or_defer_inner(path, recursive, None)?
            .map(|(outcome, _)| outcome)
            .ok_or_else(|| FsError::file_not_found(path))
    }

    pub(crate) fn free_expired_inode(
        &self,
        path: &str,
        inode_id: i64,
    ) -> FsResult<Option<(FreeOutcome, InodeView)>> {
        self.free_or_defer_inner(path, true, Some(inode_id))
    }

    fn free_or_defer_inner(
        &self,
        path: &str,
        recursive: bool,
        expected_id: Option<i64>,
    ) -> FsResult<Option<(FreeOutcome, InodeView)>> {
        let mut outcome = self.run_metadata_write_with_fallback(
            |epoch| {
            if let Some(releaser) = self.fs_mode_releaser() {
                if releaser.is_fs_mode(path) {
                    let Some((pending, cleanup, inode)) =
                        self.fs_mode_release_candidates(path, recursive, expected_id)?
                    else {
                        return Ok(ShardedOutcome::Done(None));
                    };
                    let count = pending.len();
                    if cleanup {
                        releaser.defer_cleanup(path)?;
                    }
                    for inode_id in pending {
                        releaser.defer_release(inode_id);
                    }
                    return Ok(ShardedOutcome::Done(Some((
                        FreeOutcome::Deferred { pending: count },
                        inode,
                    ))));
                }
                if recursive && releaser.contains_fs_mode_mount(path) {
                    return err_box!(
                        "free {} would release fs_mode files without UFS verification; free each fs_mode mount separately",
                        path
                    );
                }
            }

            if !recursive && expected_id.is_none() {
                if let ShardedOutcome::Done((free_res, inode)) =
                    self.free_single_file_sharded(path)?
                {
                    return Ok(ShardedOutcome::Done(Some((
                        FreeOutcome::Freed(free_res),
                        inode,
                    ))));
                }
            }

            // F7: a NON-recursive directory free stays on the original cold
            // body — the C2 bounded subtree plan only optimises genuine
            // recursive requests. Single-file sharded fallback (above) is
            // separate; reaching here with recursive=false means the target
            // is a directory or a shard-unavailable file — no DFS budget
            // overhead for a path that never needed the subtree plan.
            if !recursive {
                return Ok(ShardedOutcome::Fallback);
            }
            // C2: recursive free runs the bounded subtree-lock path with
            // the full inode → worker → block order (a free always drops
            // the data blocks). Pre-body fallbacks keep the exclusive
            // bridge below; real body errors propagate verbatim.
            // F1: the hot closure receives the outer original epoch
            // explicitly — no re-sampling (G3).
            self.run_recursive_metadata_write(
                epoch,
                path,
                // R4: a free does not mutate the parent edge, so the parent
                // takes Read (§4) — only the target subtree is Write.
                InodeLockMode::Read,
                recursive,
                |locked| {
                    let inp = Self::resolve_path(locked, path)?;
                    Self::check_not_reserved_path(inp.path())?;
                    let Some(inode) = Self::free_target(&inp, expected_id)? else {
                        return Ok(None);
                    };
                    // R4: re-check the fs_mode mount refusal under the
                    // locks — the pre-lock classification is not proof the
                    // condition still holds (a mount could have been added
                    // between planning and locking).
                    if recursive {
                        if let Some(releaser) = self.fs_mode_releaser() {
                            if releaser.contains_fs_mode_mount(path) {
                                return err_box!(
                                    "free {} would release fs_mode files without UFS verification; free each fs_mode mount separately",
                                    path
                                );
                            }
                        }
                    }
                    let free_res = locked.free(&inp, recursive)?;
                    Ok(Some((FreeOutcome::Freed(free_res), inode)))
                },
            )
            },
            |fs_dir| {
            let inp = Self::resolve_path(fs_dir, path)?;
            Self::check_not_reserved_path(inp.path())?;
            let Some(inode) = Self::free_target(&inp, expected_id)? else {
                return Ok(None);
            };

            let free_res = fs_dir.free(&inp, recursive)?;
            Ok(Some((FreeOutcome::Freed(free_res), inode)))
            },
        )?;
        if let Some((FreeOutcome::Freed(ref mut result), _)) = outcome {
            self.remove_freed_blocks(result);
        }
        Ok(outcome)
    }

    fn free_single_file_sharded(
        &self,
        path: &str,
    ) -> FsResult<ShardedOutcome<(FreeResult, InodeView)>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
            if !full || ids.len() < 2 {
                return Ok(ShardedOutcome::Fallback);
            }
            let target_id = *ids.last().expect("full path has a target");
            let target = {
                let fs_dir = self.fs_dir.view()?;
                fs_dir.store.get_inode(target_id, None)?
            };
            let Some(target) = target.filter(|inode| inode.is_file()) else {
                return Ok(ShardedOutcome::Fallback);
            };
            let block_ids = target.as_file_ref()?.block_ids();
            let worker_ids = {
                let fs_dir = self.fs_dir.view()?;
                let mut worker_ids = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => return Ok(ShardedOutcome::Fallback),
                    };
                    worker_ids.extend(locations.into_iter().map(|location| location.worker_id));
                }
                worker_ids.into_iter().collect::<Vec<_>>()
            };
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.locations_sampled",
                description: "Worker locations sampled before inode locks",
                context: {"path" => path},
            }
            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            let _inode_locks = self.lock_sharded_requests(requests, false);
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&worker_ids, &block_ids);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(live_inode) = inp.get_last_inode() else {
                continue;
            };
            let Ok(live_file) = live_inode.as_file_ref() else {
                continue;
            };
            if live_file.block_ids() != block_ids {
                continue;
            }

            let current_workers = {
                let mut current_workers = HashSet::new();
                for block_id in &block_ids {
                    let locations = match fs_dir.get_block_locations(*block_id) {
                        Ok(locations) => locations,
                        Err(_) => return Ok(ShardedOutcome::Fallback),
                    };
                    current_workers
                        .extend(locations.into_iter().map(|location| location.worker_id));
                }
                current_workers
            };
            if current_workers.len() != worker_ids.len()
                || !worker_ids
                    .iter()
                    .all(|worker_id| current_workers.contains(worker_id))
            {
                continue;
            }
            Self::check_not_reserved_path(inp.path())?;
            let original = live_inode.as_ref().clone_metadata();
            let free_res = fs_dir.free_file(&inp, LocalTime::mills() as i64)?;
            return Ok(ShardedOutcome::Done((free_res, original)));
        }

        Ok(ShardedOutcome::Fallback)
    }

    fn free_target(inp: &InodePath, expected_id: Option<i64>) -> FsResult<Option<InodeView>> {
        let Some(inode) = inp.get_last_inode() else {
            return if expected_id.is_some() {
                Ok(None)
            } else {
                err_ext!(FsError::file_not_found(inp.path()))
            };
        };
        if let Some(id) = expected_id {
            if inode.id() != id
                || !inode.is_expired()?
                || inode.storage_policy()?.ttl_action != TtlAction::Free
                || matches!(inode.as_ref(), InodeView::File(file) if file.is_writing())
            {
                return Ok(None);
            }
        }
        Ok(Some(inode.as_ref().clone_metadata()))
    }

    fn remove_freed_blocks(&self, free_res: &mut FreeResult) {
        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut free_res.blocks),
            trashed: false,
            purge_pending: false,
        });
    }

    /// State Free reconciliation needs for one file, read under one lock.
    pub fn fs_mode_snapshot(&self, inode_id: i64) -> FsResult<Option<FsModeSnapshot>> {
        // The returned DTO only needs one consistent read of the inode row and
        // its path identity: a shared-view snapshot covers both without
        // draining the namespace per reconciled file.
        let fs_dir = self.fs_dir.view()?;
        let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
        let path = match FsDir::inode_path_in_snapshot(&snapshot, inode_id) {
            Ok(path) => path,
            Err(FsError::FileNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let resolved = snapshot.resolve_path(&path)?;
        let Some(inode) = resolved.target() else {
            return Ok(None);
        };
        let InodeView::File(file) = &inode else {
            return Ok(None);
        };
        if file.id != inode_id {
            return Ok(None);
        }
        Ok(Some(FsModeSnapshot {
            path,
            gen: file.gen(),
            state: file.storage_policy.state,
            ufs_mtime: file.storage_policy.ufs_mtime,
            quiescent: file.is_complete() && !file.is_writing(),
        }))
    }

    /// Release one fs_mode file after its UFS object was verified. True when
    /// released; false when the file changed since verification.
    pub fn release_verified(
        &self,
        inode_id: i64,
        path: &str,
        gen: FileGen,
        ufs_mtime: i64,
    ) -> FsResult<bool> {
        let result = self.run_metadata_write_with_fallback(
            |_epoch| self.release_verified_sharded(inode_id, path, gen, ufs_mtime),
            |fs_dir| fs_dir.free_verified(inode_id, path, gen, ufs_mtime),
        )?;
        let Some(mut free_res) = result else {
            return Ok(false);
        };
        self.remove_freed_blocks(&mut free_res);
        Ok(true)
    }

    fn release_verified_sharded(
        &self,
        inode_id: i64,
        path: &str,
        gen: FileGen,
        ufs_mtime: i64,
    ) -> FsResult<ShardedOutcome<Option<FreeResult>>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
            if !full || ids.len() < 2 || ids.last() != Some(&inode_id) {
                return Ok(ShardedOutcome::Fallback);
            }
            let target = {
                let fs_dir = self.fs_dir.view()?;
                fs_dir.store.get_inode(inode_id, None)?
            };
            let Some(InodeView::File(file)) = target else {
                return Ok(ShardedOutcome::Done(None));
            };
            if file.gen() != gen
                || file.storage_policy.state != StorageState::Both
                || file.storage_policy.ufs_mtime != ufs_mtime
            {
                // The file changed since verification: nothing to release.
                return Ok(ShardedOutcome::Done(None));
            }

            let block_ids = file.block_ids();
            // Location updates also serialize on worker locks. Snapshot their
            // identities before locking, then retry if that set changed.
            let read_workers = |fs_dir: &FsDir| -> FsResult<HashSet<u32>> {
                let mut workers = HashSet::new();
                for block_id in &block_ids {
                    workers.extend(
                        fs_dir
                            .get_block_locations(*block_id)?
                            .into_iter()
                            .map(|loc| loc.worker_id),
                    );
                }
                Ok(workers)
            };
            // Sample inside its own block: the view guard must end before the
            // fault point and any inode/location lock wait (the guard's debt
            // converts to a strong reference across the cold swap, which a
            // concurrent global-lock delete would otherwise wait on).
            let worker_ids = {
                let view = self.fs_dir.view()?;
                match read_workers(&view) {
                    Ok(workers) => workers,
                    Err(_) => break,
                }
            };
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.locations_sampled",
                description: "Worker locations sampled before inode locks",
                context: {"path" => path},
            }
            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            let _inode_locks = self.lock_sharded_requests(requests, false);
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&worker_ids.iter().copied().collect::<Vec<_>>(), &block_ids);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Ok(current_workers) = read_workers(&fs_dir) else {
                break;
            };
            if current_workers != worker_ids {
                continue;
            }
            let Some(live_inode) = inp.get_last_inode() else {
                continue;
            };
            let live_file = live_inode.as_file_ref()?;
            if live_file.id != inode_id
                || live_file.gen() != gen
                || live_file.storage_policy.state != StorageState::Both
                || live_file.storage_policy.ufs_mtime != ufs_mtime
                || live_file.block_ids() != block_ids
            {
                continue;
            }
            return Ok(ShardedOutcome::Done(Some(
                fs_dir.free_file(&inp, LocalTime::mills() as i64)?,
            )));
        }

        // Retries exhausted with identity churn: side-effect-free fallback.
        Ok(ShardedOutcome::Fallback)
    }

    /// Files under `path` that still hold Curvine data and must be routed
    /// through reconciliation. `Ufs` files are already released. Directories
    /// carry no data of their own, and `recursive=false` inside a directory
    /// frees nothing (same rule as `FsDir::unprotected_free`).
    fn fs_mode_release_candidates(
        &self,
        path: &str,
        recursive: bool,
        expected_id: Option<i64>,
    ) -> FsResult<Option<(Vec<i64>, bool, InodeView)>> {
        let mut fs_dir = self.cold_acquire_wait()?;
        // Recursive collection needs a stable tree and inode content.

        let inp = Self::resolve_path(&fs_dir, path)?;
        Self::check_not_reserved_path(inp.path())?;
        if inp.is_root() {
            return err_box!("The root is not allowed to be free");
        }
        let Some(current) = Self::free_target(&inp, expected_id)? else {
            return Ok(None);
        };
        let root = inp
            .get_last_inode()
            .ok_or_else(|| FsError::file_not_found(inp.path()))?;

        let cleanup = recursive && matches!(root.as_ref(), InodeView::Dir(_));
        let mut ids = Vec::new();
        let mut stack = vec![root];
        while let Some(node) = stack.pop() {
            match node.as_ref() {
                InodeView::File(file) if file.storage_policy.state != StorageState::Ufs => {
                    ids.push(file.id)
                }
                // Directory children are lazily materialised as FileEntry; they
                // must be resolved through the store before they can be treated
                // as files.
                InodeView::FileEntry(entry) => {
                    if let Some(stored) = fs_dir.store.get_inode(entry.id, Some(&entry.name))? {
                        stack.push(InodePtr::from_owned(stored));
                    }
                }
                InodeView::Dir(dir) if recursive => {
                    stack.extend(dir.children_vec().into_iter().map(InodePtr::from_owned));
                }
                _ => {}
            }
        }
        Ok(Some((ids, cleanup, current)))
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed (POSIX EINVAL).
        if let Some(rest) = dst.strip_prefix(src) {
            if rest.starts_with(PATH_SEPARATOR) {
                return err_ext!(FsError::invalid_argument(format!(
                    "cannot rename {} to {}: destination is under source",
                    src, dst
                )));
            }
        }

        // EXCHANGE also rejects src under dst (/a/b <-> /a would make /a its own descendant).
        if flags.exchange_mode() {
            if let Some(rest) = src.strip_prefix(dst) {
                if rest.starts_with(PATH_SEPARATOR) {
                    return err_ext!(FsError::invalid_argument(format!(
                        "cannot exchange {} with {}: source is under destination",
                        src, dst
                    )));
                }
            }
        }

        let (renamed, deleted) = self.run_metadata_write_with_fallback(
            |epoch| {
                // Live-tree pre-check. The store-snapshot read this replaces cost
                // two RocksDB walk rounds per rename and only fed the sharded
                // routing decision, which the sharded loop re-resolves live
                // anyway. A live view can briefly lag a committed store batch
                // (durable-before-live), so a misjudgment here degrades to the
                // global-lock path — never to an incorrect rename.
                let source = self.resolve_edge_ids(src)?;
                let destination = self.resolve_edge_ids(dst)?;

                Self::check_not_reserved_path(src)?;
                Self::check_not_reserved_path(dst)?;

                if source.1 <= 1 {
                    return err_box!("Cannot rename root path");
                }

                if src == dst {
                    // No-op planning check: needs a view for the epoch sanity but
                    // the global body re-resolves everything — explicit fallback.
                    return Ok(ShardedOutcome::Fallback);
                }

                // POSIX rename of two names for the same inode is a no-op.
                // Keep NO_REPLACE's existing destination-exists error and leave
                // journal replay unchanged for entries written by older leaders.
                if !flags.no_replace() && !flags.exchange_mode() {
                    if let (Some(source), Some(target)) = (
                        source.0.last().filter(|_| source.2),
                        destination.0.last().filter(|_| destination.2),
                    ) {
                        if source == target {
                            return Ok(ShardedOutcome::Fallback);
                        }
                    }
                }

                let sharded = !flags.exchange_mode()
                    && !flags.trash()
                    && !flags.restore()
                    && source.2
                    && !destination.2
                    && source.0.len() >= 2
                    && destination.0.len() + 1 >= destination.1;
                if sharded {
                    match self.rename_sharded(src, dst, flags, (source, destination))? {
                        ShardedOutcome::Done(renamed) => {
                            return Ok(ShardedOutcome::Done((renamed, None)));
                        }
                        ShardedOutcome::Fallback => return Ok(ShardedOutcome::Fallback),
                    }
                }

                // Planning point: this rename needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                // The exact same path keeps the original false contract
                // (bff4e7dc5 / b1d6bd568 both returned Ok(false) here); it is
                // not a same-inode rename of two distinct hardlink names.
                if src == dst {
                    return Ok((false, None));
                }
                Self::check_not_reserved_path(src)?;
                Self::check_not_reserved_path(dst)?;
                let src_inp = Self::resolve_path(fs_dir, src)?;
                let dst_inp = Self::resolve_path(fs_dir, dst)?;
                // POSIX same-inode rename of two distinct names is a no-op
                // (kept from the hot path's dropped branch — journal replay
                // unchanged for older leaders). Only a fully-resolved pair
                // names a real target: a missing final component leaves
                // inodes.last pointing at the parent, and comparing those ids
                // would misreport two missing paths under one parent as "same
                // inode". Let FsDir::rename produce the real FileNotFound.
                if src_inp.is_full()
                    && dst_inp.is_full()
                    && !flags.no_replace()
                    && !flags.exchange_mode()
                {
                    if let (Some(source), Some(target)) = (
                        src_inp.inodes.last().map(|inode| inode.id()),
                        dst_inp.inodes.last().map(|inode| inode.id()),
                    ) {
                        if source == target {
                            return Ok((true, None));
                        }
                    }
                }
                let deleted = fs_dir.rename(&src_inp, &dst_inp, flags)?;
                Ok((true, deleted))
            },
        )?;
        if let Some(deleted) = deleted {
            self.worker_manager.write().remove_blocks(&deleted);
        }
        Ok(renamed)
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx, OpenFlags::new_create().set_overwrite(true))
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if !flags.create() {
            return err_box!("create_with_opts requires O_CREAT flag");
        }
        let path = path.as_ref();

        // Check the path length
        self.check_path_length(path)?;

        if opts.replicas < self.conf.min_replication || opts.replicas >= self.conf.max_replication {
            return err_box!(
                "The replica number {} needs to be between {} and {}",
                opts.replicas,
                self.conf.min_replication,
                self.conf.max_replication
            );
        }

        if opts.block_size < self.conf.min_block_size || opts.block_size >= self.conf.max_block_size
        {
            return err_box!(
                "Block size needs to be between {} and {}",
                self.conf.min_block_size,
                self.conf.max_block_size
            );
        }

        let (status, deleted) = self.run_metadata_write_with_fallback(
            |epoch| {
                // Plan first: a missing parent needs the global lock — an
                // explicit side-effect-free Fallback, not a cold re-entry.
                let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
                if ids.len() + 1 < components_len {
                    return Ok(ShardedOutcome::Fallback);
                }
                self.create_single_parent_sharded_with_plan(
                    path,
                    opts.clone(),
                    flags,
                    (ids, components_len, full, restore_epoch),
                )
            },
            |fs_dir| self.create_with_global_lock_body(fs_dir, path, opts.clone(), flags),
        )?;
        if let Some(deleted) = deleted {
            self.worker_manager.write().remove_blocks(&deleted);
        }
        Ok(status)
    }

    pub fn open_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();
        if flags.read_only() {
            if flags.truncate() {
                return err_box!("cannot combine O_RDONLY with O_TRUNC");
            }
            if flags.create() {
                return err_box!("cannot combine O_RDONLY with O_CREAT");
            }
            return self.get_block_locations(path);
        }

        let opened = self.run_metadata_write_with_fallback(
            |epoch| {
                let (ids, components_len, full, epoch) = self.resolve_edge_ids(path)?;
                Self::check_not_reserved_path(path)?;
                if flags.truncate() {
                    let initial = (ids, components_len, full, epoch, None);
                    if let ShardedOutcome::Done((status, deleted)) =
                        self.overwrite_file_sharded(path, opts.clone(), Some(initial))?
                    {
                        return Ok(ShardedOutcome::Done(Ok((
                            FileBlocks::new(status, vec![]),
                            Some(deleted),
                        ))));
                    }
                } else if full {
                    let initial = (ids, components_len, full, epoch);
                    if let ShardedOutcome::Done(blocks) =
                        self.reopen_file_sharded(path, &opts.client_name, initial)?
                    {
                        return Ok(ShardedOutcome::Done(Ok((blocks, None))));
                    }
                }
                #[cfg(test)]
                crate::fault_point! {
                    sync,
                    name: "master.metadata.open_fallback",
                    description: "Open fallback before taking the namespace write lock",
                    context: {"path" => path},
                }

                // Planning point: this open needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                Self::checked_live_epoch(fs_dir)?;
                let inp = Self::resolve_path(fs_dir, path)?;
                Self::check_not_reserved_path(path)?;

                match inp.get_last_inode().filter(|_| inp.is_full()) {
                    None => {
                        return if flags.create() {
                            Ok(Err(opts.clone()))
                        } else {
                            err_ext!(FsError::file_not_found(path))
                        }
                    }
                    Some(inode) if inode.is_dir() => {
                        return err_box!("{} is a directory", path);
                    }
                    Some(_) => {}
                }

                if !flags.truncate() {
                    // Exclusive owner: reopen directly, no sharded re-entry.
                    let status = fs_dir.reopen_file(&inp, &opts.client_name)?;
                    let inode = inp
                        .get_last_inode()
                        .ok_or_else(|| FsError::file_not_found(path))?;
                    let file = inode.as_file_ref()?;
                    let blocks = self.get_block_locs(path, fs_dir, file)?;
                    return Ok(Ok((FileBlocks::new(status, blocks), None)));
                }

                Self::check_not_reserved_path(inp.path())?;
                let deleted = fs_dir.overwrite_file(&inp, opts.clone())?;
                let status = fs_dir.file_status(&inp)?;
                Ok(Ok((FileBlocks::new(status, vec![]), Some(deleted))))
            },
        )?;
        match opened {
            Ok((blocks, deleted)) => {
                if let Some(deleted) = deleted {
                    self.worker_manager.write().remove_blocks(&deleted);
                }
                Ok(blocks)
            }
            Err(opts) => {
                let status = self.create_with_opts(path, opts, flags)?;
                Ok(FileBlocks::new(status, vec![]))
            }
        }
    }

    fn reopen_file_sharded(
        &self,
        path: &str,
        client_name: &str,
        initial: (Vec<i64>, usize, bool, u64),
    ) -> FsResult<ShardedOutcome<FileBlocks>> {
        const MAX_RETRIES: usize = 3;
        let mut initial = Some(initial);

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match initial.take() {
                Some(resolved) => resolved,
                None => self.resolve_edge_ids(path)?,
            };
            if !full {
                break;
            }
            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, inode_id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *inode_id)
                    } else {
                        InodeLockRequest::read(depth, *inode_id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.reopen_planned",
                    description: "Reopen path resolved before inode locks",
                context: {"path" => path},
            }
            let _locks = self.lock_sharded_requests(requests, false);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(inode) = inp.get_last_inode() else {
                continue;
            };
            if inode.is_dir() {
                return err_box!("{} is a directory", path);
            }
            if !inode.is_file() {
                return err_box!("{} is not a regular file", path);
            }
            let status = fs_dir.reopen_file(&inp, client_name)?;
            let file = inode.as_file_ref()?;
            let blocks = self.get_block_locs(path, &fs_dir, file)?;
            return Ok(ShardedOutcome::Done(FileBlocks::new(status, blocks)));
        }

        // Retries exhausted (or the path is not a full identity): the reopen
        // write must run under the exclusive owner.
        Ok(ShardedOutcome::Fallback)
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        self.file_status_by_id(path, None)
    }

    fn file_status_from_snapshot(
        snapshot: &InodeStoreSnapshot<'_>,
        path: &str,
        inode: InodeView,
    ) -> FsResult<FileStatus> {
        if !inode.is_dir() {
            return Ok(inode.to_file_status(path)?);
        }

        let attributes = snapshot.get_directory_attributes(inode.id())?;
        let child_len = match attributes.and_then(|attributes| attributes.children()) {
            Some(children) if children >= 0 => children,
            Some(_) => return err_box!("directory {} has an invalid child count", inode.id()),
            None => i32::try_from(snapshot.count_children(inode.id())?)
                .map_err(|_| FsError::common("directory has too many children"))?,
        };
        let mut status = inode.to_file_status(path)?;
        if let Some(attributes) = attributes {
            if attributes.ctime != inode.ctime() {
                status.x_attr.insert(
                    INTERNAL_CTIME_XATTR.to_string(),
                    attributes.ctime.to_le_bytes().to_vec(),
                );
            }
            status.mtime = attributes.mtime;
            status.nlink = attributes.nlink;
        }
        status.children_num = child_len;
        status.len = i64::from(child_len);
        Ok(status)
    }

    /// Release one writer record registered by OpenFile. Idempotent: missing
    /// files, directories and unknown clients succeed. Only the RocksDB
    /// state is updated (no journal entry): complete carries the
    /// authoritative snapshot, so a crash before complete replays to the
    /// correct unfinished state either way.
    pub fn close_file<T: AsRef<str>>(&self, path: T, client_name: &str) -> FsResult<()> {
        let path = path.as_ref();
        self.run_metadata_write_with_fallback(
            |_epoch| self.close_file_sharded(path, client_name),
            |fs_dir| self.close_file_with_global_lock_body(fs_dir, path, client_name),
        )
    }

    pub fn file_status_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
    ) -> FsResult<FileStatus> {
        let path = path.as_ref();
        if let Some(id) = inode_id.filter(|id| *id > 0) {
            // Directory inode fields and its separate attributes must come
            // from the same generation, even when another writer holds a read guard.
            let fs_dir = self.fs_dir.view()?;
            let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
            let Some(inode) = snapshot.get_inode(id)? else {
                return err_ext!(FsError::file_not_found(path).ctx(format!("inode_id={}", id)));
            };
            return Self::file_status_from_snapshot(&snapshot, path, inode);
        }

        Self::check_not_pending_path(path)?;
        self.file_status_memory(path)
    }

    /// Bounded attempts for a locked live-tree read. Sustained membership
    /// changes between planning and locking keep failing identity
    /// verification, so exhausted attempts fall back to the snapshot read
    /// for progress.
    const MEMORY_READ_ATTEMPTS: usize = 4;

    /// Plans a hot read under the namespace read guard: an identity walk
    /// plus page membership edges only. The caller releases the guard
    /// before taking inode locks.
    fn plan_memory_read(
        fs_dir: &FsDir,
        path: &str,
        page: bool,
        page_opts: &ListOptions,
    ) -> FsResult<MemoryReadPlan> {
        let components = InodeView::path_components(path)?;
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
            children,
        })
    }

    /// Hot-path `file_status` over the live tree. Planning captures only
    /// inode identity; the read locks (path ancestors, target) are taken in
    /// one batch, identity is re-verified under them, and the status is
    /// built inside the same locked window: a writer that mutates any read
    /// inode holds its write lock first, so meta and attributes cannot tear.
    fn file_status_memory(&self, path: &str) -> FsResult<FileStatus> {
        for _ in 0..Self::MEMORY_READ_ATTEMPTS {
            let fs_dir = self.fs_dir.view()?;
            // Reads stay available while the restore flag is set: the
            // snapshot protocol is the pre-S4 safe reader in that window.
            let plan_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => {
                    drop(fs_dir);
                    return self.file_status_snapshot(path);
                }
            };
            let plan = Self::plan_memory_read(&fs_dir, path, false, &ListOptions::default())?;
            drop(fs_dir);
            let _locks = self.inode_locks.lock_many(&plan.lock_requests());
            let fs_dir = self.fs_dir.view()?;
            let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => {
                    drop(_locks);
                    drop(fs_dir);
                    return self.file_status_snapshot(path);
                }
            };
            if locked_epoch != plan_epoch {
                continue;
            }
            let verified = Self::plan_memory_read(&fs_dir, path, false, &ListOptions::default())?;
            if !plan.matches(&verified) {
                continue;
            }
            if !verified.full {
                return err_ext!(FsError::file_not_found(path));
            }
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.file_status.locked_build",
                description: "Inside the locked window before the status is built",
                context: {"path" => path},
                return_error: |fault| Err(fault.message.into()),
            }
            #[cfg(test)]
            memory_read_gates::park(
                "master.metadata.file_status.locked_build",
                std::time::Duration::from_secs(5),
            );
            let status = match verified.target_edge {
                // Directory DTO freshly built under the target's read lock:
                // set_attr holds the write lock, so meta and attributes read
                // one consistent generation.
                Some(ChildEdge::Dir(node)) => {
                    InodeDir::from_dir_node_live(verified.target_name.clone(), node)
                        .to_file_status(path)?
                }
                Some(ChildEdge::FileId(id)) => {
                    let inode = fs_dir
                        .store
                        .get_inode(id, Some(&verified.target_name))?
                        .ok_or_else(|| FsError::file_not_found(path))?;
                    inode.to_file_status(path)?
                }
                None => return err_ext!(FsError::file_not_found(path)),
            };
            return Ok(status);
        }
        // Contention fallback: the pre-S4 snapshot read.
        self.file_status_snapshot(path)
    }

    /// Pre-S4 status read: one store snapshot decides edges, inode rows and
    /// directory attributes together. Also the safe reader while the restore
    /// flag is set, mirroring the old hot-path semantics.
    fn file_status_snapshot(&self, path: &str) -> FsResult<FileStatus> {
        let fs_dir = self.fs_dir.view()?;
        let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
        let resolved = snapshot.resolve_path(path)?;
        let inode = resolved
            .target()
            .ok_or_else(|| FsError::file_not_found(path))?
            .clone();
        Self::file_status_from_snapshot(&snapshot, path, inode)
    }

    /// Hot-path existence check over the live tree with the same locked
    /// identity protocol as [`Self::file_status_memory`].
    fn exists_memory(&self, path: &str) -> FsResult<bool> {
        for _ in 0..Self::MEMORY_READ_ATTEMPTS {
            let fs_dir = self.fs_dir.view()?;
            let plan_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => {
                    drop(fs_dir);
                    return self.exists_snapshot(path);
                }
            };
            let plan = Self::plan_memory_read(&fs_dir, path, false, &ListOptions::default())?;
            drop(fs_dir);
            let _locks = self.inode_locks.lock_many(&plan.lock_requests());
            let fs_dir = self.fs_dir.view()?;
            let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => {
                    drop(_locks);
                    drop(fs_dir);
                    return self.exists_snapshot(path);
                }
            };
            if locked_epoch != plan_epoch {
                continue;
            }
            let verified = Self::plan_memory_read(&fs_dir, path, false, &ListOptions::default())?;
            if !plan.matches(&verified) {
                continue;
            }
            // File targets keep the pre-S4 inode-row existence check: a
            // missing row behind a live edge is the deletion-publication
            // window owned by the caller's drain fallback.
            if let Some(ChildEdge::FileId(id)) = verified.target_edge {
                if fs_dir.store.get_inode(id, None)?.is_none() {
                    return err_box!("Failed to load inode {} from store", id);
                }
            }
            return Ok(verified.full);
        }
        self.exists_snapshot(path)
    }

    /// Pre-S4 existence read: one store snapshot decides both edges and
    /// inode rows, so a row deleted ahead of its journal publication never
    /// reports the live edge alone.
    fn exists_snapshot(&self, path: &str) -> FsResult<bool> {
        let fs_dir = self.fs_dir.view()?;
        let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
        Ok(snapshot.resolve_path(path)?.is_full())
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let path = path.as_ref();
        if trash::is_pending_path(path) {
            return Ok(false);
        }
        let components = InodeView::path_components(path)?;
        if components.last().is_none_or(String::is_empty) {
            return err_box!("Path {} is invalid", path);
        }
        let result = self.exists_memory(path);
        match result {
            Ok(exists) if self.journal_writer.ensure_metadata_current().is_ok() => Ok(exists),
            Ok(_) | Err(_) => {
                // After publication failure, the committed store and live tree
                // may disagree in either direction. Wait for writers to drain.
                let fs_dir = self.cold_acquire_wait()?;
                let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
                if snapshot.resolve_path(path)?.is_full() {
                    return Ok(Self::resolve_path(&fs_dir, path)?.is_full());
                }
                match Self::resolve_path(&fs_dir, path) {
                    Ok(inp) if inp.is_full() => {
                        err_box!("directory edge references missing inode at {}", path)
                    }
                    Ok(_) => Ok(false),
                    Err(error) if error.to_string().contains("Failed to load inode") => {
                        err_box!("directory edge references missing inode: {}", error)
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            // Glob resolution still traverses live inode pointers.
            let fs_dir = self.cold_acquire_wait()?;
            let paths = Self::resolve_path_by_glob_pattern(&fs_dir, path)?;
            let mut all_statuses = Vec::new();
            for path in &paths {
                if trash::is_pending_path(path.path()) {
                    continue;
                }
                let statuses = fs_dir.list_status(path)?;
                all_statuses.extend(Self::hide_pending(path.path(), statuses));
            }
            Ok(all_statuses)
        } else {
            self.list_options(path, ListOptions::default())
        }
    }

    pub fn list_options<T: AsRef<str>>(
        &self,
        path: T,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            return err_box!("list_options does not support glob pattern, path {}", path);
        }
        Self::check_not_pending_path(path)?;
        // The pending trash entry must not consume a client-visible page slot.
        // Over-fetch one entry and hide_pending removes it again.
        let extended_opts;
        let page_opts = if path == trash::TRASH_ROOT && opts.limit.is_some_and(|limit| limit > 0) {
            extended_opts = ListOptions {
                limit: opts.limit.map(|limit| limit.saturating_add(1)),
                start_after: opts.start_after.clone(),
            };
            &extended_opts
        } else {
            &opts
        };
        self.list_options_memory(path, page_opts)
    }

    /// Hot-path listing over the live tree. Planning captures the page
    /// membership as identity edges only; the read locks (path ancestors,
    /// target, every page child) are taken in one batch, identity and
    /// membership are re-verified under them, and every status is built
    /// inside the same locked window. Writers hold the matching write locks
    /// first, so the page equals one point-in-time namespace state: a
    /// grandchild moved between two listed directories can never be counted
    /// in both, and a set_attr cannot tear meta against attributes.
    fn list_options_memory(
        &self,
        path: &str,
        page_opts: &ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        for _ in 0..Self::MEMORY_READ_ATTEMPTS {
            let fs_dir = self.fs_dir.view()?;
            let plan_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => {
                    drop(fs_dir);
                    return self.list_options_snapshot(path, page_opts);
                }
            };
            let plan = Self::plan_memory_read(&fs_dir, path, true, page_opts)?;
            drop(fs_dir);
            let _locks = self.inode_locks.lock_many(&plan.lock_requests());
            let fs_dir = self.fs_dir.view()?;
            let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => {
                    drop(_locks);
                    drop(fs_dir);
                    return self.list_options_snapshot(path, page_opts);
                }
            };
            if locked_epoch != plan_epoch {
                continue;
            }
            let verified = Self::plan_memory_read(&fs_dir, path, true, page_opts)?;
            if !plan.matches(&verified) {
                continue;
            }
            if !verified.full {
                return err_ext!(FsError::file_not_found(path));
            }
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.list_options.locked_build",
                description: "Inside the locked window before the page is built",
                context: {"path" => path},
                return_error: |fault| Err(fault.message.into()),
            }
            #[cfg(test)]
            memory_read_gates::park(
                "master.metadata.list_options.locked_build",
                std::time::Duration::from_secs(5),
            );
            let Some(ChildEdge::Dir(_)) = verified.target_edge else {
                let ChildEdge::FileId(id) = verified
                    .target_edge
                    .expect("non-directory target carries a file edge")
                else {
                    unreachable!("non-directory target carries a file edge")
                };
                let inode = fs_dir
                    .store
                    .get_inode(id, Some(&verified.target_name))?
                    .ok_or_else(|| FsError::file_not_found(path))?;
                let empty = page_opts.limit == Some(0)
                    || page_opts
                        .start_after
                        .as_ref()
                        .is_some_and(|cursor| inode.name() <= cursor.as_str());
                if empty {
                    return Ok(Vec::new());
                }
                return Ok(vec![inode.to_file_status(path)?]);
            };
            let mut views = Vec::with_capacity(verified.children.len());
            for (name, edge) in &verified.children {
                match edge {
                    ChildEdge::Dir(child) => {
                        // Directory DTOs are built fresh under the page's
                        // read locks, never copied during planning.
                        views.push(InodeDir::from_dir_node_live(name.clone(), child.clone()))
                    }
                    ChildEdge::FileId(id) => views.push(InodeView::new_entry(name.clone(), *id)),
                }
            }
            let inp = InodePath::resolve_locked(
                fs_dir.root_node(),
                path,
                fs_dir.store_ref(),
                &verified.ids,
                verified.ids.len(),
            )?
            .ok_or_else(|| FsError::file_not_found(path))?;
            let statuses = fs_dir.store.batched_get_inodes(&inp, views)?;
            return Ok(Self::hide_pending(path, statuses));
        }
        // Contention fallback: the pre-S4 snapshot read.
        self.list_options_snapshot(path, page_opts)
    }

    fn list_options_snapshot(
        &self,
        path: &str,
        page_opts: &ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.view()?;
        let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
        let resolved = snapshot.resolve_path(path)?;
        let inode = resolved
            .target()
            .ok_or_else(|| FsError::file_not_found(path))?;
        if !inode.is_dir() {
            if page_opts.limit == Some(0)
                || page_opts
                    .start_after
                    .as_ref()
                    .is_some_and(|cursor| inode.name() <= cursor.as_str())
            {
                return Ok(Vec::new());
            }
            return Ok(vec![Self::file_status_from_snapshot(
                &snapshot,
                path,
                inode.clone(),
            )?]);
        }
        let children = snapshot.children_page(inode.id(), page_opts)?;
        let mut statuses = Vec::with_capacity(children.len());
        for child in children {
            let child_path = if resolved.components().len() <= 1 {
                format!("/{}", child.name())
            } else {
                format!("{}/{}", path, child.name())
            };
            statuses.push(Self::file_status_from_snapshot(
                &snapshot,
                &child_path,
                child,
            )?);
        }
        Ok(Self::hide_pending(path, statuses))
    }

    /// Resolves a client-supplied path. `/.Trash/.pending` is master-owned
    /// staging for async deletes and reclamation, so it resolves as missing:
    /// clients can neither stat, list, open, rename nor synchronously delete it.
    /// Internal reclamation goes through `FsDir` directly and is not affected.
    fn resolve_path(fs_dir: &FsDir, path: &str) -> FsResult<InodePath> {
        let inp = Self::resolve_path_raw(fs_dir, path)?;
        Self::check_not_pending_path(inp.path())?;
        Ok(inp)
    }

    fn resolve_path_raw(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_node(), path, &fs_dir.store)
    }

    fn check_not_pending_path(path: &str) -> FsResult<()> {
        if trash::is_pending_path(path) {
            return err_ext!(FsError::file_not_found(path));
        }
        Ok(())
    }

    /// Drops the `.pending` staging directory from a listing of `/.Trash`.
    fn hide_pending(dir_path: &str, mut statuses: Vec<FileStatus>) -> Vec<FileStatus> {
        if dir_path == trash::TRASH_ROOT {
            statuses.retain(|status| status.name != trash::TRASH_PENDING_NAME);
        }
        statuses
    }

    fn check_not_reserved_path(path: &str) -> FsResult<()> {
        if trash::is_reserved_path(path) {
            return err_ext!(FsError::invalid_argument(format!(
                "{} is reserved for the recycle bin",
                path
            )));
        }
        Ok(())
    }

    fn resolve_path_by_glob_pattern(fs_dir: &FsDir, path: &str) -> CommonResult<Vec<InodePath>> {
        InodePath::resolve_for_glob_pattern(fs_dir.root_node(), path, &fs_dir.store)
    }

    pub fn check_path_length(&self, path: &str) -> CommonResult<()> {
        if path.len() > self.conf.max_path_len {
            return err_box!(
                "create: Path too long, limit {} characters",
                self.conf.max_path_len
            );
        }

        let depth = path.split(PATH_SEPARATOR).count();
        if depth > self.conf.max_path_depth {
            return err_box!(
                "create: Path too long, limit {} levels",
                self.conf.max_path_depth
            );
        }

        Ok(())
    }

    pub fn validate_add_block(
        file: &InodeFile,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        if let Some(v) = previous {
            if v.block_len != file.block_size as i64 {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas as u16,
            block_size: file.block_size as i64,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn choose_worker(
        &self,
        inp: &InodePath,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;
        self.choose_worker_for_file(file, client_addr, exclude_workers)
    }

    pub fn choose_worker_for_file(
        &self,
        file: &InodeFile,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut wm = self.worker_manager.write();
        let validate_block = Self::validate_add_block(file, &client_addr, None)?;
        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);
        Ok(wm.choose_worker(choose_ctx)?)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        self.worker_manager
            .read()
            .create_locate_block(path, block, locs)
    }

    pub fn resolve_file_inode(
        fs_dir: &FsDir,
        path: &str,
        inode_id: Option<i64>,
    ) -> FsResult<InodePtr> {
        match inode_id {
            Some(v) if v > 0 => match fs_dir.store.get_inode(v, None)? {
                Some(view) => Ok(InodePtr::from_owned(view)),
                None => err_ext!(FsError::file_not_found(path).ctx(format!("inode_id={}", v))),
            },

            _ => {
                // Raw resolution: journal replay addresses `.pending` paths.
                let inp = Self::resolve_path_raw(fs_dir, path)?;
                match inp.task_last() {
                    Some(InodeEntry::View(ptr)) => Ok(ptr),
                    Some(InodeEntry::Dir { .. }) => {
                        err_ext!(FsError::common("pending path resolved to a directory"))
                    }
                    None => err_ext!(FsError::file_not_found(path)),
                }
            }
        }
    }

    /// Document application to allocate a new block.
    #[allow(clippy::too_many_arguments)]
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        self.run_metadata_write_with_fallback(
            |epoch| {
                self.add_block_sharded(
                    path,
                    inode_id,
                    client_addr.clone(),
                    commit_blocks.clone(),
                    exclude_workers.clone(),
                    file_len,
                    last_block.clone(),
                )
            },
            |fs_dir| {
                self.add_block_with_global_lock_body(
                    fs_dir,
                    path,
                    inode_id,
                    client_addr.clone(),
                    commit_blocks.clone(),
                    exclude_workers.clone(),
                    file_len,
                    last_block.clone(),
                )
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_block_sharded(
        &self,
        path: &str,
        expected_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<ShardedOutcome<LocatedBlock>> {
        const MAX_RETRIES: usize = 3;
        let mut commit_block_ids: Vec<i64> =
            commit_blocks.iter().map(|block| block.block_id).collect();
        commit_block_ids.sort_unstable();
        commit_block_ids.dedup();
        let mut commit_worker_ids: Vec<u32> = commit_blocks
            .iter()
            .flat_map(|block| block.locations.iter().map(|location| location.worker_id))
            .collect();
        commit_worker_ids.sort_unstable();
        commit_worker_ids.dedup();

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match self.resolve_edge_ids(path) {
                Ok(value) => value,
                Err(_) if expected_id.is_some() => return Ok(ShardedOutcome::Fallback),
                Err(error) => return Err(error),
            };
            if !full || ids.len() < 2 || expected_id.is_some_and(|id| ids.last() != Some(&id)) {
                return Ok(ShardedOutcome::Fallback);
            }
            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.add_block_planned",
                    description: "Block allocation path resolved before inode locks",
                context: {"path" => path},
            }
            let _inode_locks = self.lock_sharded_requests(requests, false);

            let next_id = {
                let fs_dir = self.fs_dir.view()?;
                let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                    Ok(epoch) => epoch,
                    Err(_) if expected_id.is_some() => return Ok(ShardedOutcome::Fallback),
                    Err(error) => return Err(error),
                };
                if locked_epoch != restore_epoch {
                    continue;
                }
                let inp = match Self::resolve_locked_path(&fs_dir, path, &ids, components_len) {
                    Ok(Some(inp)) => inp,
                    Ok(None) => continue,
                    Err(_) if expected_id.is_some() => return Ok(ShardedOutcome::Fallback),
                    Err(error) => return Err(error),
                };
                let Some(inode) = inp.get_last_inode() else {
                    continue;
                };
                if expected_id.is_some_and(|id| id != inode.id()) || !inode.is_file() {
                    return Ok(ShardedOutcome::Fallback);
                }
                inode
                    .as_file_ref()?
                    .search_next_block(last_block.as_ref().map(|block| block.id))
                    .map(|block| block.id)
            };
            let mut block_ids = commit_block_ids.clone();
            if let Some(next_id) = next_id {
                block_ids.push(next_id);
            }
            let _location_locks = self
                .block_location_locks
                .write_workers_blocks(&commit_worker_ids, &block_ids);

            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            let Some(inode) = inp.get_last_inode() else {
                continue;
            };
            if expected_id.is_some_and(|id| id != inode.id()) || !inode.is_file() {
                continue;
            }
            let file = inode.as_file_ref()?;
            let next = file.search_next_block(last_block.as_ref().map(|block| block.id));
            if next.map(|block| block.id) != next_id {
                continue;
            }

            if let Some(next) = next {
                let locs = fs_dir.get_block_locations(next.id)?;
                let extend_block = ExtendedBlock {
                    id: next.id,
                    len: next.len as i64,
                    storage_type: file.storage_policy.storage_type,
                    file_type: file.file_type,
                    alloc_opts: next.alloc_opts.clone(),
                };
                return Ok(ShardedOutcome::Done(self.create_locate_block(
                    path,
                    extend_block,
                    &locs,
                )?));
            }

            let block_size = file.block_size as i64;
            let choose_workers =
                self.choose_worker_for_file(file, client_addr.clone(), exclude_workers.clone())?;
            let has_spdk = {
                let wm = self.worker_manager.read();
                wm.workers_have_spdk(&choose_workers)
            };
            let block = match fs_dir.acquire_new_block(
                path,
                inode,
                commit_blocks.clone(),
                &choose_workers,
                file_len,
            ) {
                Ok(block) => block,
                Err(error) => {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                    return Err(error);
                }
            };
            return Ok(ShardedOutcome::Done(LocatedBlock {
                block,
                locs: choose_workers,
                has_spdk,
            }));
        }

        Ok(ShardedOutcome::Fallback)
    }

    fn add_block_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let inode = Self::resolve_file_inode(fs_dir, path, inode_id)?;
        let file = inode.as_file_ref()?;

        // File allows concurrent writes, 'previous' is the previous block,
        // need to check if the next block has already been allocated。
        // If it has been allocated, return that block
        if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
            let locs = fs_dir.get_block_locations(next.id)?;
            let extend_block = ExtendedBlock {
                id: next.id,
                len: next.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: next.alloc_opts.clone(),
            };

            return self.create_locate_block(path, extend_block, &locs);
        }

        let block_size = file.block_size as i64;
        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block =
            match fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len) {
                Ok(block) => block,
                Err(e) => {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                    return Err(e);
                }
            };
        let located = LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        };

        Ok(located)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush,
                return_file_blocks: true,
                set_attr_opts,
            },
        )
    }

    /// Flushes file metadata without building the full block-location snapshot.
    pub fn flush_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush: true,
                return_file_blocks: false,
                set_attr_opts: None,
            },
        )
        .map(|_| ())
    }

    fn complete_file0<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let client_name = client_name.as_ref().to_string();
        self.run_metadata_write_with_fallback(
            |epoch| {
                self.complete_file_sharded(
                    path,
                    inode_id,
                    len,
                    commit_blocks.clone(),
                    &client_name,
                    options.clone(),
                )
            },
            |fs_dir| {
                self.complete_file_with_global_lock_body(
                    fs_dir,
                    path,
                    inode_id,
                    len,
                    commit_blocks.clone(),
                    &client_name,
                    options.clone(),
                )
            },
        )
    }

    pub fn get_file_blocks(
        &self,
        path: &str,
        fs_dir: &FsDir,
        inp: &InodePath,
    ) -> FsResult<FileBlocks> {
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        let file = inode.as_file_ref()?;
        let blocks = self.get_block_locs(path, fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let file_locs = fs_dir.get_file_locations(file)?;
        self.build_located_blocks(path, file, file_locs)
    }

    /// Shared LocatedBlock construction for the live and snapshot block-read
    /// paths: the non-tail length check, the lost-block error text and a
    /// single worker_manager read guard per build.
    fn build_located_blocks(
        &self,
        path: &str,
        file: &InodeFile,
        file_locs: HashMap<i64, Vec<BlockLocation>>,
    ) -> FsResult<Vec<LocatedBlock>> {
        // Acquire worker state after the location reads so RocksDB lookups do
        // not delay heartbeats.
        let wm = self.worker_manager.read();
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let lc = try_option!(
                file_locs.get(&meta.id),
                "File {}, block {} Lost (no worker can read)",
                path,
                meta.id
            );
            let lb = wm.create_locate_block(path, extend_block, lc)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        const MAX_RETRIES: usize = 3;
        let path = path.as_ref();
        // Client-facing boundary: the pending staging namespace is invisible
        // to RPC reads, hot path and snapshot fallback alike.
        Self::check_not_pending_path(path)?;
        let read = |fs_dir: &FsDir, inp: &InodePath| -> FsResult<_> {
            let inode = inp
                .get_last_inode()
                .ok_or_else(|| FsError::file_not_found(path))?;
            let needs_touch = inode.is_file() && Self::atime_refresh_due(&inode);
            let inode_id = inode.id();
            let file = inode.as_file_ref()?;
            let block_locs = self.get_block_locs(path, fs_dir, file)?;
            let blocks = FileBlocks {
                status: inode.to_file_status(path)?,
                block_locs,
            };
            let epoch = fs_dir.store_ref().scan_control().cancel_epoch();
            Ok((blocks, needs_touch, inode_id, epoch))
        };

        // Live-tree read under the inode read locks: the locks keep the read
        // mutually exclusive with the sharded writers for this path, while
        // every resolution step stays in memory (no store-snapshot walks).
        let mut result = None;
        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = self.resolve_edge_ids(path)?;
            if !full {
                break;
            }
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| InodeLockRequest::read(depth, *id))
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.locations_read_planned",
                    description: "Block location read path resolved before inode locks",
                context: {"path" => path},
            }
            let _locks = self.lock_sharded_requests(requests, false);
            let fs_dir = self.fs_dir.view()?;
            if restore_epoch != Self::checked_live_epoch(&fs_dir)? {
                continue;
            }
            let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)? else {
                continue;
            };
            result = Some(read(&fs_dir, &inp)?);
            break;
        }
        let (blocks, needs_touch, inode_id, restore_epoch) = match result {
            Some(result) => result,
            None => {
                // Missing path or bounded retries exhausted: fall back to one
                // consistent store snapshot (path, inode payload and block
                // locations from the same read) instead of draining the whole
                // namespace for a plain single-file read.
                let fs_dir = self.fs_dir.view()?;
                let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
                let resolved = snapshot.resolve_path(path)?;
                let inode = resolved
                    .target()
                    .ok_or_else(|| FsError::file_not_found(path))?;
                let needs_touch = inode.is_file() && Self::atime_refresh_due(&inode);
                let inode_id = inode.id();
                let file = inode.as_file_ref()?;
                // Same snapshot for inode payload and locations; the evictor
                // hint and the shared builder keep parity with the live path.
                let file_locs = snapshot.get_file_locations(file)?;
                fs_dir.evictor.on_access(file.id);
                let block_locs = self.build_located_blocks(path, file, file_locs)?;
                let blocks = FileBlocks {
                    status: inode.to_file_status(path)?,
                    block_locs,
                };
                let epoch = fs_dir.store_ref().scan_control().cancel_epoch();
                (blocks, needs_touch, inode_id, epoch)
            }
        };

        // Drop the inode read locks before attempting the best-effort atime write.
        if needs_touch {
            if let Ok(epoch) = self.journal_writer.metadata_epoch() {
                if let Some(commit_guard) = self.metadata_commit_gate.try_enter() {
                    // The epoch check inside the gate keeps the admission
                    // window the same as the blocking write path.
                    let epoch_check = self.journal_writer.ensure_metadata_epoch(epoch);
                    let result = epoch_check.and_then(|()| {
                        let scope = self.journal_writer.begin_commit_scope();
                        let r = self.touch_atime_sharded(path, inode_id, restore_epoch);
                        scope.wait().and(r)
                    });
                    drop(commit_guard);
                    if result.is_ok() {
                        self.maybe_create_periodic_snapshot(epoch);
                    }
                }
            }
        }
        Ok(blocks)
    }

    fn touch_atime_sharded(
        &self,
        path: &str,
        expected_id: i64,
        expected_epoch: u64,
    ) -> FsResult<()> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let Ok((ids, components_len, full, restore_epoch)) = self.resolve_edge_ids(path) else {
                return Ok(());
            };
            if restore_epoch != expected_epoch
                || !full
                || ids.len() < 2
                || ids.last() != Some(&expected_id)
            {
                return Ok(());
            }

            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.atime_planned",
                    description: "Atime path resolved before inode locks",
                context: {"path" => path},
            }
            let _locks = self.lock_sharded_requests(requests, false);

            let fs_dir = self.fs_dir.view()?;
            let Ok(locked_epoch) = Self::checked_live_epoch(&fs_dir) else {
                return Ok(());
            };
            if locked_epoch != expected_epoch {
                continue;
            }
            let inp = match Self::resolve_locked_path(&fs_dir, path, &ids, components_len) {
                Ok(Some(inp)) => inp,
                Ok(None) => continue,
                Err(_) => return Ok(()),
            };
            let Some(inode) = inp.get_last_inode() else {
                return Ok(());
            };
            if inode.id() != expected_id {
                return Ok(());
            }
            fs_dir.touch_atime_for_ttl(&inp, expected_id, expected_epoch)?;
            return Ok(());
        }

        Ok(())
    }

    pub fn get_block_locations_batch<'a>(
        &'a self,
        paths: &'a [String],
    ) -> FsResult<impl Iterator<Item = FsResult<FileBlocks>> + 'a> {
        if paths.len() > GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS {
            return Err(FsError::invalid_argument(format!(
                "GetBlockLocationsBatch accepts at most {} paths",
                GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS
            )));
        }
        Ok(paths.iter().map(|path| self.get_block_locations(path)))
    }

    /// Whether this file's atime should be refreshed on this open: it has an
    /// active TTL policy based on access time and the throttle window
    /// (`ttl_ms / 2`) has elapsed since the last refresh.
    fn atime_refresh_due(inode: &InodeView) -> bool {
        let Ok(sp) = inode.storage_policy() else {
            return false;
        };
        if sp.ttl_ms <= 0 || sp.ttl_action == TtlAction::None {
            return false;
        }

        let ttl_type = match inode.x_attr() {
            Ok(x_attr) => {
                TtlType::from_xattr_value(x_attr.get(INTERNAL_TTL_TYPE_XATTR).map(|v| v.as_slice()))
            }
            Err(_) => return false,
        };
        if ttl_type != TtlType::Atime {
            return false;
        }

        let now = LocalTime::mills() as i64;
        now.saturating_sub(inode.atime()) >= sp.ttl_ms / 2
    }

    pub fn get_file_block_details<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlockDetails> {
        let path = path.as_ref();
        // Same client-facing boundary as get_block_locations.
        Self::check_not_pending_path(path)?;

        let (status, block_snapshots, worker_snapshots) = {
            // One consistent store snapshot covers path, inode and location
            // rows; a low-frequency single-file detail read does not need to
            // drain the namespace. The evictor access hint still fires once.
            let fs_dir = self.fs_dir.view()?;
            let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
            let resolved = snapshot.resolve_path(path)?;
            let inode = match resolved.target() {
                Some(inode) => inode,
                None => return err_ext!(FsError::file_not_found(path)),
            };
            let file = inode.as_file_ref()?;
            let file_locs = {
                let locs = snapshot.get_file_locations(file)?;
                // Keep the original access hint: one notification per read.
                fs_dir.evictor.on_access(file.id);
                locs
            };
            let status = inode.to_file_status(path)?;
            let mut offset = 0;
            let mut block_snapshots = Vec::with_capacity(file.blocks.len());
            let mut worker_ids = HashSet::new();

            for meta in &file.blocks {
                let len = meta.len();
                let replicas = file_locs.get(&meta.id).cloned().unwrap_or_default();
                for replica in &replicas {
                    worker_ids.insert(replica.worker_id);
                }
                block_snapshots.push((meta.id, len, offset, replicas));
                offset += len;
            }

            // Snapshot worker state under the existing fs_dir -> worker_manager lock order.
            let worker_snapshots = {
                let worker_manager = self.worker_manager.read();
                worker_ids
                    .into_iter()
                    .map(|worker_id| {
                        let (address, state) = if let Some(worker) =
                            worker_manager.get_worker(worker_id)
                        {
                            (Some(worker.address.clone()), worker.status)
                        } else if let Some(worker) = worker_manager.get_known_worker(worker_id) {
                            (Some(worker.address.clone()), WorkerStatus::Lost)
                        } else {
                            (None, WorkerStatus::Unknown)
                        };
                        (worker_id, (address, state))
                    })
                    .collect::<HashMap<_, _>>()
            };

            (status, block_snapshots, worker_snapshots)
        };

        let blocks = block_snapshots
            .into_iter()
            .map(|(block_id, len, offset, locations)| {
                let mut replicas = locations
                    .into_iter()
                    .map(|location| BlockReplicaDetail {
                        worker_id: location.worker_id,
                        storage_type: location.storage_type,
                        address: worker_snapshots
                            .get(&location.worker_id)
                            .and_then(|(address, _)| address.clone()),
                        state: worker_snapshots
                            .get(&location.worker_id)
                            .map(|(_, state)| *state)
                            .unwrap_or(WorkerStatus::Unknown),
                    })
                    .collect::<Vec<_>>();
                replicas.sort_by_key(|replica| replica.worker_id);

                FileBlockDetail {
                    block_id,
                    len,
                    offset,
                    replicas,
                }
            })
            .collect();

        Ok(FileBlockDetails { status, blocks })
    }

    pub fn cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataSnapshotPage> {
        let mut fs_dir = self.cold_acquire_wait()?;
        if page_size == 0 {
            return err_box!("cv metadata snapshot page_size must be greater than 0");
        }

        // Traversal reads mutable inode contents as well as directory topology.

        let epoch = fs_dir.op_id.get();
        let start_after = page_token.filter(|token| !token.is_empty());
        let mut entries = Vec::with_capacity(page_size.saturating_add(1));
        self.collect_cv_metadata_snapshot_page(
            &fs_dir,
            &fs_dir.root_view(),
            "/",
            start_after.as_deref(),
            page_size.saturating_add(1),
            &mut entries,
        )?;

        let next_page_token = if entries.len() > page_size {
            let token = entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.status.path.clone());
            entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataSnapshotPage {
            entries,
            next_page_token,
            epoch,
        })
    }

    fn collect_cv_metadata_snapshot_page(
        &self,
        fs_dir: &FsDir,
        inode: &InodeView,
        path: &str,
        start_after: Option<&str>,
        limit: usize,
        entries: &mut Vec<CvMetadataSnapshotEntry>,
    ) -> FsResult<()> {
        if entries.len() >= limit {
            return Ok(());
        }
        if let Some(token) = start_after {
            if path != "/" && token > path && !snapshot_token_in_subtree(token, path) {
                return Ok(());
            }
        }

        let resolved = self.resolve_snapshot_inode(fs_dir, inode)?;
        if start_after.map(|token| path > token).unwrap_or(true) {
            let status = resolved.to_file_status(path)?;
            let blocks = if let Ok(file) = resolved.as_file_ref() {
                Some(FileBlocks::new(
                    status.clone(),
                    self.get_block_locs(path, fs_dir, file)?,
                ))
            } else {
                None
            };
            entries.push(CvMetadataSnapshotEntry { status, blocks });
            if entries.len() >= limit {
                return Ok(());
            }
        }

        if let InodeView::Dir(dir) = resolved.as_ref() {
            for child in dir.children_vec() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_snapshot_page(
                    fs_dir,
                    &child,
                    &child_path,
                    start_after,
                    limit,
                    entries,
                )?;
                if entries.len() >= limit {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Directory children live only in the in-memory tree. Borrow that node.
    /// `FileEntry` stores a name and id, so load the file inode for its blocks
    /// and hard-link name. The caller holds the exclusive namespace lock for the walk.
    fn resolve_snapshot_inode<'a>(
        &self,
        fs_dir: &FsDir,
        inode: &'a InodeView,
    ) -> FsResult<Cow<'a, InodeView>> {
        if let InodeView::FileEntry(entry) = inode {
            let loaded = fs_dir
                .store
                .get_inode(entry.id, Some(&entry.name))?
                .ok_or_else(|| FsError::file_not_found(entry.name.clone()))?;
            return Ok(Cow::Owned(loaded));
        }
        Ok(Cow::Borrowed(inode))
    }

    pub fn filesystem_info(&self) -> FsResult<FilesystemInfo> {
        let metrics = Master::get_metrics()?;
        let mut info = FilesystemInfo {
            inode_dir_num: metrics.inode_dir_num.get(),
            inode_file_num: metrics.inode_file_num.get(),
            ..Default::default()
        };

        let wm = self.worker_manager.read();

        // Requests can only reach active master
        info.active_master = wm.conf.master_addr().to_string();
        info.active_raft_id = if wm.conf.journal.enable {
            wm.conf.journal.node_id().ok()
        } else {
            None
        };
        for peer in &wm.conf.journal.journal_addrs {
            info.journal_nodes.push(peer.to_string())
        }

        for (_, worker) in wm.worker_map.workers() {
            info.capacity += worker.capacity;
            info.available += worker.available;
            info.fs_used += worker.fs_used;
            info.non_fs_used += worker.non_fs_used;
            info.reserved_bytes += worker.reserved_bytes;
            info.block_num += worker.block_num;

            match worker.status {
                WorkerStatus::Live => {
                    info.live_workers.push(worker.clone());
                    // Only Live workers are eligible for new allocations, so the
                    // allocatable view mirrors the allocation policy. Failed
                    // storage dirs are already excluded from worker.capacity.
                    info.allocatable_capacity += worker.capacity;
                    info.allocatable_available += worker.allocatable_available().max(0);
                }
                WorkerStatus::Blacklist => info.blacklist_workers.push(worker.clone()),
                WorkerStatus::Decommission => info.decommission_workers.push(worker.clone()),
                _ => (),
            }
        }

        for (_, worker) in wm.worker_map.lost_workers() {
            info.lost_workers.push(worker.clone());
        }

        Ok(info)
    }

    pub fn fs_dir(&self) -> Arc<FsDirHandle> {
        self.fs_dir.clone()
    }

    pub fn create_leader_snapshot(&self, force: bool) -> FsResult<LeaderSnapshotInfo> {
        // The exclusive gate drains in-flight namespace writes before the
        // checkpoint reads the tree, and blocks new writes until it finishes.
        let Some(_snapshot_guard) = self.metadata_commit_gate.close_and_enter_if_open() else {
            return err_box!("metadata commit gate is already closed");
        };
        let fs_dir = self.fs_dir.view()?;
        fs_dir.create_snapshot(force)
    }

    // Add a test worker and unit tests will use it.
    pub fn add_test_worker(&self, worker: WorkerInfo) {
        let mut wm = self.worker_manager.write();
        wm.add_test_worker(worker);
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let mut fs_dir = self.cold_acquire_wait()?;
        // Hashing traverses and clones live inode contents, not just topology.

        fs_dir.sum_hash()
    }

    pub fn last_inode_id(&self) -> i64 {
        // Same counter instance the live FsDir owns: a closed view (cold
        // window / FAILED) must not fabricate a zero for metrics scrapes.
        self.inode_id_counter.current()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        // Same FileSystemStats instance the live store owns: a closed view
        // must not fabricate zeros for metrics scrapes.
        self.fs_stats_counter.counts()
    }

    // Create a directory number based on rocksdb data for testing.
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        let fs_dir = self.fs_dir.view()?;
        fs_dir.create_tree()
    }

    // Restore in-memory tree from RocksDB (for testing without Raft).
    // In production, Raft automatically restores via apply_snapshot().
    pub fn restore_from_rocksdb(&self) -> CommonResult<()> {
        let mut fs_dir = self.cold_acquire_wait()?;
        fs_dir.restore_from_rocksdb()
    }

    fn block_inode_state(&self, id: i64) -> FsResult<BlockInodeState> {
        let fs_dir = self.fs_dir.view()?;
        fs_dir.block_inode_state(id)
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_report_tracker.reset(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    pub(crate) fn with_worker_location_lock<T>(
        &self,
        worker_id: u32,
        action: impl FnOnce() -> T,
    ) -> T {
        let _worker_lock = self.worker_location_lock(worker_id).lock();
        action()
    }

    #[allow(dead_code)]
    pub(crate) fn expire_worker(
        &self,
        worker_id: u32,
        now_ms: u64,
        blacklist_ms: u64,
        lost_ms: u64,
    ) -> (Option<WorkerInfo>, Option<WorkerInfo>) {
        self.with_worker_location_lock(worker_id, || {
            let mut worker_manager = self.worker_manager.write();
            let Some(last_update) = worker_manager
                .get_worker(worker_id)
                .map(|worker| worker.last_update)
            else {
                return (None, None);
            };

            let blacklisted = (now_ms > last_update + blacklist_ms)
                .then(|| worker_manager.add_blacklist_worker(worker_id))
                .flatten();
            let lost = (now_ms > last_update + lost_ms)
                .then(|| worker_manager.remove_expired_worker(worker_id))
                .flatten();
            (blacklisted, lost)
        })
    }

    /// Report id of the last fully processed full report of the worker.
    #[doc(hidden)]
    pub fn full_report_completed_round(&self, worker_id: u32) -> Option<u64> {
        self.full_report_tracker.completed_round(worker_id)
    }

    /// Number of workers with a partially accumulated full report.
    #[doc(hidden)]
    pub fn full_report_open_sessions(&self) -> usize {
        self.full_report_tracker.open_sessions()
    }

    fn invalidate_full_block_report_session(&self, worker_id: u32) {
        self.full_report_tracker
            .invalidate_session(worker_id, LocalTime::mills());
    }

    fn invalidate_full_block_state(&self, worker_id: u32) {
        self.invalidate_full_block_report_session(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    fn invalidate_full_block_reconcile(&self, worker_id: u32) {
        let mut reconciles = self.full_block_reconciles.lock();
        if let Some(state) = reconciles.get_mut(&worker_id) {
            state.generation = state.generation.saturating_add(1);
            state.pending = None;
            if !state.running {
                reconciles.remove(&worker_id);
            }
        }
    }

    /// Process block reports
    pub fn block_report(
        &self,
        list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        let _worker_lock = self.worker_location_lock(list.worker_id).lock();
        let block_ids: Vec<_> = list.blocks.iter().map(|block| block.id).collect();
        self.run_metadata_write(|| {
            let _location_locks = self
                .block_location_locks
                .write_worker_blocks(list.worker_id, &block_ids);
            self.block_report_locked(&list, replication_handler.clone())
        })
    }

    pub(crate) fn block_report_for_live_worker_session(
        &self,
        list: BlockReportList,
        worker_session_id: &str,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<Option<BlockReportResult>> {
        let _worker_lock = self.worker_location_lock(list.worker_id).lock();
        let block_ids: Vec<_> = list.blocks.iter().map(|block| block.id).collect();
        if !self
            .worker_manager
            .read()
            .is_live_worker_session(list.worker_id, worker_session_id)
        {
            return Ok(None);
        }
        self.run_metadata_write(|| {
            let _location_locks = self
                .block_location_locks
                .write_worker_blocks(list.worker_id, &block_ids);
            self.block_report_locked(&list, replication_handler.clone())
        })
        .map(Some)
    }

    /// Opens a full-report round before the Worker snapshots its local block
    /// store. The worker stripe orders this handshake against replication
    /// completion, which invalidates a started report before it can reconcile
    /// an older snapshot.
    pub(crate) fn begin_full_block_report_for_live_worker_session(
        &self,
        worker_id: u32,
        report_id: u64,
        worker_session_id: &str,
    ) -> Option<bool> {
        let _worker_lock = self.worker_location_lock(worker_id).lock();
        if !self
            .worker_manager
            .read()
            .is_live_worker_session(worker_id, worker_session_id)
        {
            return None;
        }

        Some(matches!(
            self.full_report_tracker
                .begin(worker_id, report_id, LocalTime::mills()),
            FullReportBegin::Accepted
        ))
    }

    pub(crate) fn block_report_for_legacy_live_worker(
        &self,
        list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<Option<BlockReportResult>> {
        let _worker_lock = self.worker_location_lock(list.worker_id).lock();
        let block_ids: Vec<_> = list.blocks.iter().map(|block| block.id).collect();
        if !self
            .worker_manager
            .read()
            .get_worker(list.worker_id)
            .is_some_and(|worker| worker.is_live() && worker.worker_session_id.is_empty())
        {
            return Ok(None);
        }
        self.run_metadata_write(|| {
            let _location_locks = self
                .block_location_locks
                .write_worker_blocks(list.worker_id, &block_ids);
            self.block_report_locked(&list, replication_handler.clone())
        })
        .map(Some)
    }

    fn block_report_locked(
        &self,
        list: &BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        // @todo check cluster.
        let limit_replicas = list.full_report;
        let invalidate_full_reconcile = !list.full_report
            && list.blocks.iter().any(|block| {
                matches!(
                    block.status,
                    BlockReportStatus::Finalized | BlockReportStatus::Writing
                )
            });
        if invalidate_full_reconcile {
            self.invalidate_full_block_state(list.worker_id);
        }

        let full_reported_blocks = match self.full_report_tracker.collect(&list, LocalTime::mills())
        {
            FullReportProgress::Duplicate => {
                // The response to this batch may have been lost, so its delete
                // commands and location updates must still be delivered. Both are
                // idempotent; only the session and the reconcile are skipped.
                info!(
                    "resent batch of completed full block report {} from worker {}: not reconciling again",
                    list.report_id, list.worker_id
                );
                None
            }
            FullReportProgress::InFlight => {
                // The first attempt may still fail, so do not acknowledge the resend.
                // The worker retries IO errors after a short backoff.
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "full block report {} from worker {} is still being processed",
                        list.report_id, list.worker_id
                    ),
                )));
            }
            FullReportProgress::Complete(blocks) => Some(blocks),
            FullReportProgress::Partial => None,
            FullReportProgress::Restart => {
                return Ok(BlockReportResult {
                    delete_blocks: Vec::new(),
                    restart_full_report: true,
                });
            }
        };
        if list.blocks.is_empty() && full_reported_blocks.is_none() {
            return Ok(BlockReportResult {
                delete_blocks: Vec::new(),
                restart_full_report: false,
            });
        }

        //(Whether to increase, block id, block location)
        let mut checked = Vec::with_capacity(list.blocks.len());
        let mut delete_blocks = Vec::new();
        let mut missing_blocks = 0usize;
        let mut not_file_blocks = 0usize;
        for item in &list.blocks {
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let defer_writing_delete =
                        item.status == BlockReportStatus::Writing && !list.full_report;
                    let state = match self.block_inode_state(item.id) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("block_report {item:?}: {e}");
                            continue;
                        }
                    };
                    match state {
                        BlockInodeState::File => checked.push((item, Some(BlockInodeState::File))),
                        BlockInodeState::Missing if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is missing",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::NotFile if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is not a file",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::Missing => {
                            missing_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::Missing)));
                        }
                        BlockInodeState::NotFile => {
                            not_file_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::NotFile)));
                        }
                    }
                }
                BlockReportStatus::Deleted => checked.push((item, None)),
            }
        }
        if missing_blocks > 0 || not_file_blocks > 0 {
            warn!(
                "block_report found {} missing-inode and {} non-file-inode blocks for worker {}; scheduling worker deletion",
                missing_blocks, not_file_blocks, list.worker_id
            );
        }

        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for (item, exists) in checked {
            let loc = BlockLocation::new(list.worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let state = match exists {
                        Some(v) => v,
                        None => {
                            warn!(
                                "block_report invariant violated: missing inode state for block {}",
                                item.id
                            );
                            continue;
                        }
                    };

                    match state {
                        BlockInodeState::File => batch.push((true, item.id, loc)),
                        BlockInodeState::Missing | BlockInodeState::NotFile => {
                            batch.push((false, item.id, loc));
                            wm.remove_block(list.worker_id, item.id);
                        }
                    }
                }
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(list.worker_id, item.id);
                }
            }
        }
        drop(wm);

        // Keep the block ids of a completing batch so a failure can be retried.
        let retry_blocks = full_reported_blocks.clone();
        let applied = (|| -> FsResult<Vec<i64>> {
            if let Some(reported_blocks) = full_reported_blocks {
                self.submit_full_block_reconcile(
                    list.worker_id,
                    reported_blocks,
                    replication_handler,
                )?;
            }
            self.apply_block_report_batch_locked(list.worker_id, batch, limit_replicas)
        })();

        if let Some(blocks) = retry_blocks {
            let now = LocalTime::mills();
            match &applied {
                Ok(_) => self
                    .full_report_tracker
                    .finish(list.worker_id, list.report_id, now),
                Err(_) => self.full_report_tracker.abort(
                    list.worker_id,
                    list.report_id,
                    list.total_len,
                    blocks,
                    now,
                ),
            }
        }

        delete_blocks.extend(applied?);
        delete_blocks.sort_unstable();
        delete_blocks.dedup();

        Ok(BlockReportResult {
            delete_blocks,
            restart_full_report: false,
        })
    }

    fn submit_full_block_reconcile(
        &self,
        worker_id: u32,
        reported_blocks: HashSet<i64>,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<()> {
        let should_spawn = {
            let mut reconciles = self.full_block_reconciles.lock();
            let state = reconciles
                .entry(worker_id)
                .or_insert_with(|| FullBlockReconcileState {
                    running: false,
                    generation: 0,
                    pending: None,
                });
            state.generation = state.generation.saturating_add(1);
            let generation = state.generation;
            state.pending = Some(FullBlockReconcileJob {
                generation,
                reported_blocks,
            });
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if !should_spawn {
            return Ok(());
        }

        let fs = self.clone();
        let res = self
            .full_block_reconcile_executor
            .fixed_spawn(worker_id as i64, move || {
                fs.run_full_block_reconcile(worker_id, replication_handler);
            });
        if let Err(e) = &res {
            self.full_block_reconciles.lock().remove(&worker_id);
            error!("submit full block report reconcile for worker {worker_id} failed: {e}");
        }
        res?;
        Ok(())
    }

    fn run_full_block_reconcile(
        &self,
        worker_id: u32,
        replication_handler: Option<MasterReplicationHandler>,
    ) {
        loop {
            let job = {
                let mut reconciles = self.full_block_reconciles.lock();
                match reconciles.get_mut(&worker_id) {
                    Some(state) => match state.pending.take() {
                        Some(v) => v,
                        None => {
                            reconciles.remove(&worker_id);
                            return;
                        }
                    },
                    None => return,
                }
            };

            if !self.is_full_block_reconcile_current(worker_id, job.generation) {
                info!(
                    "skip stale full block report reconcile for worker {}, generation {}",
                    worker_id, job.generation
                );
                continue;
            }

            match self.reconcile_full_block_report(worker_id, job.generation, job.reported_blocks) {
                Ok(stale_block_ids) => {
                    let stale_block_count = stale_block_ids.len();
                    if stale_block_count > 0 {
                        info!(
                            "full block report reconciled {} stale block locations for worker {}",
                            stale_block_count, worker_id
                        );
                        if let Some(replication_handler) = &replication_handler {
                            if let Err(e) = replication_handler
                                .report_under_replicated_blocks(worker_id, stale_block_ids)
                            {
                                error!(
                                    "Errors on reporting under-replicated {} blocks from full block report reconciliation. err: {:?}",
                                    stale_block_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "full block report reconcile for worker {} failed: {}",
                        worker_id, e
                    );
                }
            }
        }
    }

    fn reconcile_full_block_report(
        &self,
        worker_id: u32,
        generation: u64,
        reported_blocks: HashSet<i64>,
    ) -> FsResult<Vec<i64>> {
        let existing_blocks = {
            let fs_dir = self.fs_dir.view()?;
            fs_dir.get_worker_block_ids(worker_id)?
        };

        let mut stale_block_ids = Vec::new();
        let mut batch = Vec::new();
        for block_id in existing_blocks {
            if !reported_blocks.contains(&block_id) {
                batch.push((false, block_id, BlockLocation::with_id(worker_id)));
                stale_block_ids.push(block_id);
            }
        }

        if !batch.is_empty() {
            let _worker_lock = self.worker_location_lock(worker_id).lock();
            let block_ids: Vec<_> = batch.iter().map(|(_, block_id, _)| *block_id).collect();
            let applied = self.run_metadata_write(|| {
                let _location_locks = self
                    .block_location_locks
                    .write_worker_blocks(worker_id, &block_ids);
                if !self.is_full_block_reconcile_current(worker_id, generation) {
                    info!(
                        "skip stale full block report reconcile apply for worker {}, generation {}",
                        worker_id, generation
                    );
                    return Ok(false);
                }
                self.apply_block_report_batch_locked(worker_id, batch, false)?;
                Ok(true)
            })?;
            if !applied {
                return Ok(Vec::new());
            }
        }

        Ok(stale_block_ids)
    }

    fn apply_block_report_batch_locked(
        &self,
        worker_id: u32,
        batch: Vec<(bool, i64, BlockLocation)>,
        limit_replicas: bool,
    ) -> FsResult<Vec<i64>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let mut delete_blocks = Vec::new();
        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }
            // Inode state is read from the store; the namespace guard excludes
            // restore, while block/worker locks serialize location changes.
            let fs_dir = self.fs_dir.view()?;
            let mut accepted = Vec::with_capacity(chunk.len());
            for (add, block_id, location) in chunk {
                if !add {
                    accepted.push((false, block_id, location));
                    continue;
                }

                let Some(state) = fs_dir.replication_block_state(block_id)? else {
                    delete_blocks.push(block_id);
                    continue;
                };
                if state
                    .locations
                    .iter()
                    .any(|current| current.worker_id == worker_id)
                {
                    continue;
                }
                if limit_replicas && state.locations.len() >= state.replicas as usize {
                    delete_blocks.push(block_id);
                    continue;
                }
                accepted.push((true, block_id, location));
            }
            fs_dir.block_report_locked(accepted)?;
        }
        Ok(delete_blocks)
    }

    fn worker_location_lock(&self, worker_id: u32) -> &Mutex<()> {
        &self.worker_location_locks[worker_id as usize % WORKER_LOCATION_LOCK_STRIPES]
    }

    pub(crate) fn commit_replication_location(
        &self,
        block_id: i64,
        location: BlockLocation,
        worker_session_id: &str,
    ) -> FsResult<bool> {
        let worker_id = location.worker_id;
        let _worker_lock = self.worker_location_lock(worker_id).lock();
        self.run_metadata_write(|| {
            let _location_locks = self
                .block_location_locks
                .write_worker_blocks(worker_id, &[block_id]);
            if !self
                .worker_manager
                .read()
                .get_worker(worker_id)
                .is_some_and(|worker| worker.worker_session_id == worker_session_id)
            {
                return err_ext!(FsError::common(format!(
                    "replication target worker {} session changed",
                    worker_id
                )));
            }
            let view = self.fs_dir.view()?;
            let added = view.add_replication_location_if_needed(block_id, location)?;
            // Completion is newer evidence even if the location was already
            // present, so an older full report cannot safely reconcile it.
            self.invalidate_full_block_state(worker_id);
            Ok(added)
        })
    }

    fn is_full_block_reconcile_current(&self, worker_id: u32, generation: u64) -> bool {
        self.full_block_reconciles
            .lock()
            .get(&worker_id)
            .map(|state| state.generation == generation)
            .unwrap_or(false)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let _worker_lock = self.worker_location_lock(worker_id).lock();
        self.delete_locations_unchecked(worker_id)
    }

    pub(crate) fn delete_lost_worker_locations(
        &self,
        worker_id: u32,
        worker_session_id: &str,
    ) -> FsResult<Option<LostWorkerLocationCleanup>> {
        let _worker_lock = self.worker_location_lock(worker_id).lock();
        if !self
            .worker_manager
            .read()
            .is_lost_worker_session(worker_id, worker_session_id)
        {
            return Ok(None);
        }
        self.delete_locations_unchecked(worker_id).map(Some)
    }

    fn delete_locations_unchecked(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let block_ids = {
            let fs_dir = self.fs_dir.view()?;
            fs_dir.block_ids_for_worker(worker_id)?
        };
        let removed_block_ids = self.run_metadata_write(|| {
            // Shared view + location locks (R4: no cold wait inside object
            // locks); Busy fails the batch and the lost-worker retry re-issues
            // it.
            let _location_locks = self
                .block_location_locks
                .write_worker_blocks(worker_id, &block_ids);
            let fs_dir = self.fs_dir.view()?;
            fs_dir.delete_locations(worker_id)
        })?;
        let mut invalidated = CacheInvalidationResult::default();

        for chunk in removed_block_ids.chunks(Self::LOST_WORKER_INVALIDATION_CHUNK) {
            let result = self.run_metadata_write(|| {
                // Inode-stripes write lock before location locks keeps the
                // inode→location→namespace order; lock_many sorts stripes and
                // dedupes repeated inode ids.
                let inode_requests: smallvec::SmallVec<[InodeLockRequest; 8]> = chunk
                    .iter()
                    .map(|block_id| {
                        InodeLockRequest::write(0, crate::master::meta::InodeId::get_id(*block_id))
                    })
                    .collect();
                #[cfg(test)]
                crate::fault_point! {
                    sync,
                    name: "master.metadata.cache_invalidation_inode_lock_planned",
                    description: "Before requesting cache invalidation inode write locks",
                    context: {
                        "worker_id" => worker_id,
                        "data_dir" => self
                            .fs_dir
                            .view()
                            .map(|v| v.get_rocks_store()
                            .db
                            .conf()
                            .data_dir
                            .clone())
                            .unwrap_or_default(),
                    },
                }
                // R4: plan the candidate files' FULL block sets from a short
                // view, then drop the view and acquire the complete object
                // lock set (every block of every candidate file, plus the
                // inode write locks) before re-opening the view. The cache
                // check inside invalidate_lost_cache_files reads block
                // locations, so it must not race a concurrent location write.
                // R4 order: no view is held while waiting for the inode
                // locks. The full block set is sampled under the inode locks
                // (short view, released before the location locks), and the
                // final verification re-opens the view under both lock sets.
                let _inode_guard = self.inode_locks.lock_many(&inode_requests);
                let full_block_ids: Vec<i64> = {
                    let view = self.fs_dir.view()?;
                    let inode_ids: Vec<i64> = chunk
                        .iter()
                        .map(|block_id| crate::master::meta::InodeId::get_id(*block_id))
                        .collect();
                    let mut ids = Vec::new();
                    for inode_id in inode_ids {
                        if let Some(inode) = view.store.get_inode(inode_id, None)? {
                            if let crate::master::meta::inode::InodeView::File(file) = &inode {
                                ids.extend(file.blocks.iter().map(|b| b.id));
                            }
                        }
                    }
                    ids
                };
                let _location_locks = self.block_location_locks.write_blocks(&full_block_ids);
                let fs_dir = self.fs_dir.view()?;
                fs_dir.invalidate_lost_cache_files(chunk)
            });
            match result {
                Ok(result) => invalidated.extend(result),
                Err(e) => warn!(
                    "failed to invalidate lost cache files for worker {} ({} block ids); \\
                     continuing with normal replica recovery: {}",
                    worker_id,
                    chunk.len(),
                    e
                ),
            }
        }

        let replication_block_ids = removed_block_ids
            .iter()
            .copied()
            .filter(|block_id| !invalidated.invalidated_block_ids.contains(block_id))
            .collect();

        if !invalidated.delete_result.blocks.is_empty() {
            self.worker_manager
                .write()
                .remove_blocks(&invalidated.delete_result);
        }

        Ok(LostWorkerLocationCleanup {
            removed_block_ids,
            replication_block_ids,
        })
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        self.run_metadata_write_with_fallback(
            |epoch| {
                let path = path.as_ref();
                if opts.recursive {
                    // C2: recursive propagation runs the bounded subtree-
                    // lock path. It mutates subtree inodes but not parent
                    // edges, so the parent takes Read and no location locks
                    // are needed (§6). Pre-body fallbacks keep the original
                    // exclusive body below. The hot closure runs inside the
                    // single admission: metadata_epoch() here returns the
                    // outer original epoch, passed explicitly (R8).
                    let epoch = self.journal_writer.metadata_epoch()?;
                    return self.run_recursive_metadata_write(
                        epoch,
                        path,
                        InodeLockMode::Read,
                        false,
                        |locked| {
                            let inp = Self::resolve_path(locked, path)?;
                            Self::check_not_reserved_path(path)?;
                            locked.set_attr(inp, opts.clone())
                        },
                    );
                }
                let resolved = self.resolve_edge_ids(path)?;
                Self::check_not_pending_path(path)?;
                Self::check_not_reserved_path(path)?;
                self.set_attr_single_path_sharded(path, opts.clone(), resolved)
            },
            |fs_dir| {
                // The exclusive owner has drained normal-writer admission and
                // readers, so no extra subtree DFS / object locks are needed:
                // one resolve, then the original recursive body.
                let inp = Self::resolve_path(fs_dir, path.as_ref())?;
                Self::check_not_reserved_path(path.as_ref())?;
                fs_dir.set_attr(inp, opts.clone())
            },
        )
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, mode, None, None)
    }

    pub fn symlink_with_owner_group<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let target = target.as_ref().to_string();
        let link = link.as_ref().to_string();
        self.run_metadata_write_with_fallback(
            |epoch| {
                let resolved = self.resolve_edge_ids(&link)?;
                Self::check_not_pending_path(&link)?;
                Self::check_not_reserved_path(&link)?;
                let single_parent = !force && !resolved.2 && resolved.0.len() + 1 >= resolved.1;
                if single_parent {
                    // The helper's Fallback must reach the exclusive protocol;
                    // discarding it would report success without executing.
                    return self.symlink_single_parent_sharded(
                        &target,
                        &link,
                        mode,
                        owner.clone(),
                        group.clone(),
                        resolved,
                    );
                }
                // Planning point: symlink needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                let inp = Self::resolve_path(fs_dir, &link)?;
                Self::check_not_reserved_path(&link)?;
                fs_dir.symlink(
                    target.clone(),
                    inp,
                    force,
                    mode,
                    owner.clone(),
                    group.clone(),
                )
            },
        )
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let src_path = src_path.as_ref().to_string();
        let dst_path = dst_path.as_ref().to_string();
        self.run_metadata_write_with_fallback(
            |epoch| {
                let src = self.resolve_edge_ids(&src_path)?;
                let dst = self.resolve_edge_ids(&dst_path)?;
                Self::check_not_reserved_path(&src_path)?;
                Self::check_not_reserved_path(&dst_path)?;
                let single_parent = src.2 && !dst.2 && dst.0.len() + 1 >= dst.1;
                if single_parent {
                    // Same contract as symlink: propagate the helper outcome.
                    return self.link_single_parent_sharded(&src_path, &dst_path, (src, dst));
                }
                // Planning point: link needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| self.link_with_global_lock_body(fs_dir, &src_path, &dst_path),
        )
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.resize_by_id(path, None, opts)
    }

    pub fn resize_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        opts: FileAllocOpts,
    ) -> FsResult<FileBlocks> {
        opts.validate()?;

        let path = path.as_ref();
        Self::check_not_reserved_path(path)?;
        // This snapshot only rejects individually impossible requests; it is not a
        // reservation, so concurrent fallocates may observe the same capacity.
        // Worker-side block allocation remains the hard enforcement point.
        let available = if opts.truncate {
            i64::MAX
        } else {
            self.worker_manager.read().available_bytes()
        };

        let (del_res, inode_id) = self.run_metadata_write_with_fallback(
            |epoch| {
                if let Some(result) =
                    self.resize_single_file_sharded(path, inode_id, opts.clone(), available)?
                {
                    return Ok(ShardedOutcome::Done(result));
                }
                // Planning point: resize needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                self.resize_with_global_lock_body(fs_dir, path, inode_id, opts.clone(), available)
            },
        )?;

        if !del_res.blocks.is_empty() {
            self.worker_manager.write().remove_blocks(&del_res);
        }

        let fs_dir = self.fs_dir.view()?;
        let inode = Self::resolve_file_inode(&fs_dir, path, Some(inode_id))?;
        let file = inode.as_file_ref()?;
        let locs = self.get_block_locs(path, &fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, locs))
    }

    fn resize_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        inode_id: Option<i64>,
        opts: FileAllocOpts,
        available: i64,
    ) -> FsResult<(DeleteResult, i64)> {
        let mut inode = Self::resolve_file_inode(fs_dir, path, inode_id)?;
        if inode_id.is_some() {
            // A by-id request may carry a stale path, so also check where
            // the inode actually lives.
            let live_path = fs_dir.get_inode_path(inode.id())?;
            Self::check_not_reserved_path(&live_path)?;
        }
        let file = inode.as_file_ref()?;
        Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
        let inode_id = inode.id();
        let del_res = fs_dir.resize_inode(path, &mut inode, opts)?;
        Ok((del_res, inode_id))
    }

    fn resize_single_file_sharded(
        &self,
        path: &str,
        expected_id: Option<i64>,
        opts: FileAllocOpts,
        available: i64,
    ) -> FsResult<Option<(DeleteResult, i64)>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match self.resolve_edge_ids(path) {
                Ok(value) => value,
                Err(_) if expected_id.is_some() => return Ok(None),
                Err(error) => return Err(error),
            };
            if !full || ids.len() < 2 || expected_id.is_some_and(|id| ids.last() != Some(&id)) {
                return Ok(None);
            }
            let target_id = *ids.last().expect("full path has a target");
            let target = {
                let fs_dir = self.fs_dir.view()?;
                match fs_dir.store.get_inode(target_id, None) {
                    Ok(target) => target,
                    Err(_) if expected_id.is_some() => return Ok(None),
                    Err(error) => return Err(error.into()),
                }
            };
            let Some(InodeView::File(file)) = target else {
                return Ok(None);
            };
            let block_ids = file.block_ids();
            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.resize_planned",
                    description: "Resize path resolved before inode locks",
                context: {"path" => path},
            }
            let _inode_locks = self.lock_sharded_requests(requests, false);
            let _location_locks = self.block_location_locks.write_blocks(&block_ids);
            let fs_dir = self.fs_dir.view()?;
            let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) if expected_id.is_some() => return Ok(None),
                Err(error) => return Err(error),
            };
            if locked_epoch != restore_epoch {
                continue;
            }
            let inp = match Self::resolve_locked_path(&fs_dir, path, &ids, components_len) {
                Ok(Some(inp)) => inp,
                Ok(None) => continue,
                Err(_) if expected_id.is_some() => return Ok(None),
                Err(error) => return Err(error),
            };
            let Some(inode) = inp.get_last_inode() else {
                continue;
            };
            let Ok(file) = inode.as_file_ref() else {
                continue;
            };
            if expected_id.is_some_and(|id| id != inode.id()) || file.block_ids() != block_ids {
                continue;
            }
            Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
            let inode_id = inode.id();
            let mut inode = inode;
            let del_res = fs_dir.resize_inode(path, &mut inode, opts.clone())?;
            return Ok(Some((del_res, inode_id)));
        }

        Ok(None)
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        self.assign_worker_by_id(path, None, block, client_addr, exclude_workers)
    }

    pub fn assign_worker_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        self.run_metadata_write_with_fallback(
            |epoch| {
                if let Some(located) = self.assign_worker_sharded(
                    path,
                    inode_id,
                    block.clone(),
                    client_addr.clone(),
                    exclude_workers.clone(),
                )? {
                    return Ok(ShardedOutcome::Done(located));
                }
                // Planning point: assign needs the global lock.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                self.assign_worker_with_global_lock_body(
                    fs_dir,
                    path,
                    inode_id,
                    block.clone(),
                    client_addr.clone(),
                    exclude_workers.clone(),
                )
            },
        )
    }

    fn assign_worker_with_global_lock_body(
        &self,
        fs_dir: &mut FsDir,
        path: &str,
        inode_id: Option<i64>,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let mut inode = Self::resolve_file_inode(fs_dir, path, inode_id)?;
        let file = inode.as_file_ref()?;
        let block_size = file.block_size as i64;
        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block = match fs_dir.assign_worker_inode(path, &mut inode, block.id, &choose_workers) {
            Ok((block, assigned)) => {
                if !assigned {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                }
                block
            }
            Err(e) => {
                self.worker_manager
                    .write()
                    .unschedule_chosen_workers(&choose_workers, block_size);
                return Err(e);
            }
        };

        Ok(LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        })
    }

    fn assign_worker_sharded(
        &self,
        path: &str,
        expected_id: Option<i64>,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Option<LocatedBlock>> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..MAX_RETRIES {
            let (ids, components_len, full, restore_epoch) = match self.resolve_edge_ids(path) {
                Ok(value) => value,
                Err(_) if expected_id.is_some() => return Ok(None),
                Err(error) => return Err(error),
            };
            if !full || ids.len() < 2 || expected_id.is_some_and(|id| ids.last() != Some(&id)) {
                return Ok(None);
            }

            let target_depth = ids.len() - 1;
            let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                .iter()
                .enumerate()
                .map(|(depth, id)| {
                    if depth == target_depth {
                        InodeLockRequest::write(depth, *id)
                    } else {
                        InodeLockRequest::read(depth, *id)
                    }
                })
                .collect();
            #[cfg(test)]
            crate::fault_point! {
                sync,
                name: "master.metadata.assign_worker_planned",
                    description: "Worker assignment path resolved before inode locks",
                context: {"path" => path},
            }
            let _inode_locks = self.lock_sharded_requests(requests, false);
            let _location_lock = self.block_location_locks.write_blocks(&[block.id]);
            let fs_dir = self.fs_dir.view()?;
            let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) if expected_id.is_some() => return Ok(None),
                Err(error) => return Err(error),
            };
            if locked_epoch != restore_epoch {
                continue;
            }
            let inp = match Self::resolve_locked_path(&fs_dir, path, &ids, components_len) {
                Ok(Some(inp)) => inp,
                Ok(None) => continue,
                Err(_) if expected_id.is_some() => return Ok(None),
                Err(error) => return Err(error),
            };
            let Some(live_inode) = inp.get_last_inode() else {
                continue;
            };
            if expected_id.is_some_and(|id| id != live_inode.id()) || !live_inode.is_file() {
                return Ok(None);
            }

            let file = live_inode.as_file_ref()?;
            let block_size = file.block_size as i64;
            let choose_workers =
                self.choose_worker_for_file(file, client_addr.clone(), exclude_workers.clone())?;
            let has_spdk = {
                let wm = self.worker_manager.read();
                wm.workers_have_spdk(&choose_workers)
            };
            let mut inode = live_inode;
            let result = fs_dir.assign_worker_inode(path, &mut inode, block.id, &choose_workers);
            let (block, assigned) = match result {
                Ok(result) => result,
                Err(error) => {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                    return Err(error);
                }
            };
            if !assigned {
                self.worker_manager
                    .write()
                    .unschedule_chosen_workers(&choose_workers, block_size);
            }
            return Ok(Some(LocatedBlock {
                block,
                locs: choose_workers,
                has_spdk,
            }));
        }

        Ok(None)
    }

    pub fn get_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();
        // The pending staging directory is not client-visible; reject it once
        // at the entry so the locked path and the snapshot fallback share the
        // same boundary (same pattern as the file_status entry).
        Self::check_not_pending_path(path)?;

        // Shared-view read under the target chain's inode read locks, the same
        // plan/lock/re-verify protocol as file_status_memory: the lock metadata
        // lives on the target inode, and the chain locks keep a concurrent
        // rename from moving the target mid-read.
        for _ in 0..Self::MEMORY_READ_ATTEMPTS {
            let fs_dir = self.fs_dir.view()?;
            let plan_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => return self.get_lock_snapshot(path, &lock),
            };
            let plan = Self::plan_memory_read(&fs_dir, path, false, &ListOptions::default())?;
            drop(fs_dir);
            let _locks = self.inode_locks.lock_many(&plan.lock_requests());
            let fs_dir = self.fs_dir.view()?;
            let locked_epoch = match Self::checked_live_epoch(&fs_dir) {
                Ok(epoch) => epoch,
                Err(_) => return self.get_lock_snapshot(path, &lock),
            };
            if locked_epoch != plan_epoch {
                continue;
            }
            let verified = Self::plan_memory_read(&fs_dir, path, false, &ListOptions::default())?;
            if !plan.matches(&verified) {
                continue;
            }
            if !verified.full {
                return err_ext!(FsError::file_not_found(path));
            }
            let Some(target) = verified.target_edge.as_ref().map(ChildEdge::id) else {
                return err_ext!(FsError::file_not_found(path));
            };
            let mut meta = fs_dir.store.get_locks(target)?;
            return Ok(meta.check_conflict(&lock, self.conf.lock_expire_time_ms()));
        }
        // Bounded attempts exhausted: the snapshot read is the safe reader.
        self.get_lock_snapshot(path, &lock)
    }

    /// Pre-S4 lock read: one store snapshot resolves the target and reads its
    /// lock metadata.
    fn get_lock_snapshot(&self, path: &str, lock: &FileLock) -> FsResult<Option<FileLock>> {
        let fs_dir = self.fs_dir.view()?;
        let snapshot = InodeStoreSnapshot::new(fs_dir.store.checked_read_snapshot()?);
        let resolved = snapshot.resolve_path(path)?;
        let inode = resolved
            .target()
            .ok_or_else(|| FsError::file_not_found(path))?;
        // Same-snapshot lock read: the live get_locks could pair the old
        // snapshot's inode with a lock row written after a concurrent rename.
        let mut meta = snapshot.get_locks(inode.id())?;
        Ok(meta.check_conflict(lock, self.conf.lock_expire_time_ms()))
    }

    pub fn set_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();
        self.run_metadata_write_with_fallback(
            |epoch| {
                const MAX_RETRIES: usize = 3;
                for _ in 0..MAX_RETRIES {
                    let (ids, components_len, full, epoch) = self.resolve_edge_ids(path)?;
                    Self::check_not_pending_path(path)?;
                    if !full {
                        // Side-effect-free planning point: unresolved path.
                        return Ok(ShardedOutcome::Fallback);
                    }
                    let target_depth = ids.len() - 1;
                    let requests: smallvec::SmallVec<[InodeLockRequest; 8]> = ids
                        .iter()
                        .enumerate()
                        .map(|(depth, id)| {
                            if depth == target_depth {
                                InodeLockRequest::write(depth, *id)
                            } else {
                                InodeLockRequest::read(depth, *id)
                            }
                        })
                        .collect();
                    #[cfg(test)]
                    crate::fault_point! {
                        sync,
                        name: "master.metadata.set_lock_planned",
                        description: "Lock path resolved before inode locks",
                        context: {"path" => path},
                    }
                    #[cfg(test)]
                    memory_read_gates::park(
                        "master.metadata.set_lock_planned",
                        std::time::Duration::from_secs(5),
                    );
                    let _locks = self.lock_sharded_requests(requests, target_depth == 0);
                    let fs_dir = self.fs_dir.view()?;
                    if epoch != Self::checked_live_epoch(&fs_dir)? {
                        continue;
                    }
                    let Some(inp) = Self::resolve_locked_path(&fs_dir, path, &ids, components_len)?
                    else {
                        continue;
                    };
                    return Ok(ShardedOutcome::Done(fs_dir.set_lock(
                        inp,
                        lock.clone(),
                        self.conf.lock_expire_time_ms(),
                    )?));
                }

                // Retries exhausted with identity churn: side-effect-free fallback.
                Ok(ShardedOutcome::Fallback)
            },
            |fs_dir| {
                let inp = Self::resolve_path(fs_dir, path)?;
                fs_dir.set_lock(inp, lock.clone(), self.conf.lock_expire_time_ms())
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::ErrorExt;
    use curvine_error::ErrorKind;
    use curvine_runtime::common::Utils;

    fn test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    // Seeds an entry under the reserved root through FsDir, since the public
    // API refuses to create anything inside /.Trash.
    fn seed_trash_entry(fs: &MasterFilesystem, name: &str) -> (String, i64) {
        let fs_dir = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let root =
            InodePath::resolve(fs_dir.root_node(), trash::TRASH_ROOT, &fs_dir.store).unwrap();
        if !root.is_full() {
            fs_dir.mkdir(root, MkdirOpts::with_create(true)).unwrap();
        }

        let path = format!("{}/{}", trash::TRASH_ROOT, name);
        let inp = InodePath::resolve(fs_dir.root_node(), &path, &fs_dir.store).unwrap();
        let inp = fs_dir
            .create_file(inp, CreateFileOpts::with_create(false))
            .unwrap();
        let inode_id = inp.get_last_inode().unwrap().id();
        (path, inode_id)
    }

    fn test_fs_with_trash(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.trash_enabled = true;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-trash-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-trash-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn assert_file_not_found_roundtrip(err: &FsError) {
        assert!(
            matches!(err.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound, got {:?}",
            err.kind()
        );
        let decoded = FsError::decode(err.encode());
        assert!(
            matches!(decoded.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound after encode/decode, got {:?}",
            decoded.kind()
        );
        assert!(
            matches!(decoded, FsError::FileNotFound(_)),
            "decoded error collapsed away from FileNotFound: {}",
            decoded
        );
    }

    #[test]
    fn fallocate_rejects_growth_larger_than_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        let err = MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 359).unwrap_err();
        assert!(matches!(err, FsError::DiskOutOfSpace(_)));
    }

    #[test]
    fn fallocate_accepts_exact_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 360).is_ok());
    }

    #[test]
    fn truncate_growth_does_not_require_physical_capacity() {
        let opts = FileAllocOpts::with_truncate(200);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 0).is_ok());
    }

    fn replication_file(fs: &MasterFilesystem, path: &str) -> i64 {
        for worker_id in [100, 200, 300] {
            let mut worker = WorkerInfo::default();
            worker.address.worker_id = worker_id;
            fs.add_test_worker(worker);
        }
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create())
            .unwrap();
        let block = fs
            .add_block(
                path,
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
        let block_id = block.block.id;
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .add_block_location(block_id, BlockLocation::with_id(100))
            .unwrap();
        block_id
    }

    #[test]
    fn multiblock_open_preserves_order_and_filters_current_worker_state() {
        for count in [0, 1, 16, 256] {
            let fs = test_fs(&format!("multiblock-open-{count}"));
            let worker = |id, status| {
                let mut worker = WorkerInfo::default();
                worker.address.worker_id = id;
                worker.status = status;
                worker
            };
            for id in [100, 200] {
                fs.add_test_worker(worker(id, WorkerStatus::Live));
            }
            let path = "/multiblock";
            fs.create_with_opts(
                path,
                CreateFileOptsBuilder::new()
                    .block_size(1_048_576)
                    .replicas(2)
                    .build(),
                OpenFlags::new_create(),
            )
            .unwrap();
            let mut commits = Vec::new();
            let mut previous = None;
            for index in 0..count {
                let located = fs
                    .add_block(
                        path,
                        None,
                        ClientAddress::default(),
                        commits.clone(),
                        vec![],
                        index * 1_048_576,
                        previous,
                    )
                    .unwrap();
                previous = Some(located.block.clone());
                commits.push(CommitBlock {
                    block_id: located.block.id,
                    block_len: 1_048_576,
                    locations: vec![BlockLocation::with_id(100), BlockLocation::with_id(200)],
                });
            }
            fs.complete_file(
                path,
                None,
                count * 1_048_576,
                commits.clone(),
                "",
                false,
                None,
            )
            .unwrap();
            let expected_ids = commits
                .iter()
                .map(|block| block.block_id)
                .collect::<Vec<_>>();
            let check = |blocks: FileBlocks, workers: &[u32]| {
                assert_eq!(blocks.status.len, count * 1_048_576);
                assert_eq!(
                    blocks
                        .block_locs
                        .iter()
                        .map(|block| block.block.id)
                        .collect::<Vec<_>>(),
                    expected_ids
                );
                for block in blocks.block_locs {
                    assert_eq!(block.block.len, 1_048_576);
                    assert_eq!(
                        block
                            .locs
                            .iter()
                            .map(|loc| loc.worker_id)
                            .collect::<Vec<_>>(),
                        workers
                    );
                }
            };
            check(fs.get_block_locations(path).unwrap(), &[100, 200]);
            check(
                fs.open_file(path, CreateFileOpts::with_create(false), OpenFlags::new(0))
                    .unwrap(),
                &[100, 200],
            );
            let details = fs.get_file_block_details(path).unwrap();
            assert_eq!(
                details
                    .blocks
                    .iter()
                    .map(|block| block.block_id)
                    .collect::<Vec<_>>(),
                expected_ids
            );

            fs.add_test_worker(worker(100, WorkerStatus::Blacklist));
            check(fs.get_block_locations(path).unwrap(), &[200]);
            fs.add_test_worker(worker(200, WorkerStatus::Decommission));
            if count == 0 {
                check(fs.get_block_locations(path).unwrap(), &[]);
            } else {
                assert!(fs
                    .get_block_locations(path)
                    .unwrap_err()
                    .to_string()
                    .contains("all replicas has been lost"));
            }
        }
    }

    #[test]
    fn replication_state_rejects_deleted_block() {
        let fs = test_fs("replication-state-deleted");
        let path = "/replication-state-deleted.log";
        let block_id = replication_file(&fs, path);
        assert!(fs
            .fs_dir
            .view()
            .unwrap()
            .replication_block_state(block_id)
            .unwrap()
            .is_some());

        fs.delete(path, false).unwrap();
        let fs_dir = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        assert!(fs_dir.replication_block_state(block_id).unwrap().is_none());
        assert!(!fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
            .unwrap());
    }

    #[test]
    fn replication_state_rejects_overwritten_block() {
        let fs = test_fs("replication-state-overwritten");
        let path = "/replication-state-overwritten.log";
        let old_block_id = replication_file(&fs, path);

        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();

        let fs_dir = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        assert!(fs_dir
            .replication_block_state(old_block_id)
            .unwrap()
            .is_none());
        assert!(!fs_dir
            .add_replication_location_if_needed(old_block_id, BlockLocation::with_id(200))
            .unwrap());
    }

    #[test]
    fn replication_location_update_is_idempotent_and_respects_replica_count() {
        let fs = test_fs("replication-location-guard");
        let block_id = replication_file(&fs, "/replication-location-guard.log");
        let fs_dir = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();

        assert!(fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
            .unwrap());
        assert!(
            !fs_dir
                .add_replication_location_if_needed(
                    block_id,
                    BlockLocation::new(200, StorageType::Ssd),
                )
                .unwrap()
        );
        assert!(!fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(300))
            .unwrap());

        let state = fs_dir.replication_block_state(block_id).unwrap().unwrap();
        assert_eq!(state.replicas, 2);
        assert_eq!(state.locations.len(), 2);
        assert_eq!(
            state
                .locations
                .iter()
                .map(|location| location.worker_id)
                .collect::<Vec<_>>(),
            vec![100, 200]
        );
    }

    #[test]
    fn resolve_file_inode_missing_inode_id_returns_file_not_found() {
        let fs = test_fs("missing-inode-id");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.view().unwrap();
        let missing_id = 9_999_999_i64;
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, "/missing", Some(missing_id))
            .unwrap_err();

        assert_file_not_found_roundtrip(&err);
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            msg
        );
    }

    #[test]
    fn resolve_file_inode_unresolved_path_returns_file_not_found() {
        let fs = test_fs("unresolved-path");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.view().unwrap();
        let path = "/does/not/exist";
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, path, None).unwrap_err();

        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string().contains(path),
            "expected path in error, got: {}",
            err
        );
    }

    #[test]
    fn get_inode_path_follows_parent_links() {
        let fs = test_fs("get-inode-path");
        let status = fs.create("/dir/file.log", true).unwrap();
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.view().unwrap();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/file.log");
    }

    #[test]
    fn get_inode_path_after_rename_returns_new_path() {
        let fs = test_fs("get-inode-path-rename");
        let status = fs.create("/dir/old.log", true).unwrap();
        fs.rename("/dir/old.log", "/dir/new.log", RenameFlags::empty())
            .unwrap();

        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.view().unwrap();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/new.log");
    }

    #[test]
    fn cv_metadata_snapshot_page_walks_live_children_and_loads_file_entries() {
        let fs = test_fs("snapshot-page-borrow");
        let source = fs.create("/a/b.txt", true).unwrap();
        fs.create("/a/c/d.txt", true).unwrap();
        fs.create("/z.txt", true).unwrap();
        fs.link("/a/b.txt", "/a/b-link.txt").unwrap();

        let mut token = None;
        let mut paths = Vec::new();
        let mut pages = 0;
        loop {
            let page = fs.cv_metadata_snapshot_page(token, 2).unwrap();
            assert!(pages < 8, "snapshot pagination did not finish");
            pages += 1;
            assert!(page.entries.len() <= 2);
            paths.extend(page.entries.iter().map(|entry| entry.status.path.clone()));
            match page.next_page_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }

        assert_eq!(
            paths,
            vec![
                "/".to_string(),
                "/a".to_string(),
                "/a/b-link.txt".to_string(),
                "/a/b.txt".to_string(),
                "/a/c".to_string(),
                "/a/c/d.txt".to_string(),
                "/z.txt".to_string(),
            ]
        );

        let full = fs.cv_metadata_snapshot_page(None, 100).unwrap();
        assert!(full.next_page_token.is_none());
        let dir = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a")
            .unwrap();
        assert!(dir.status.is_dir);
        assert!(dir.blocks.is_none());
        assert_eq!(dir.status.children_num, 3);

        let link = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a/b-link.txt")
            .unwrap();
        let file = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a/b.txt")
            .unwrap();
        assert_eq!(link.status.name, "b-link.txt");
        assert_eq!(link.status.id, source.id);
        assert_eq!(file.status.id, source.id);
        assert!(link.blocks.is_some());
        assert!(file.blocks.is_some());
    }

    #[test]
    fn file_status_by_id_after_rename_resolves_inode_keeps_request_path() {
        // By-id status resolves the inode (id/name) but keeps the request path.
        let fs = test_fs("file-status-by-id-stale-path");
        let created = fs.create("/a/old.log", true).unwrap();
        fs.rename("/a/old.log", "/a/new.log", RenameFlags::empty())
            .unwrap();

        let status = fs
            .file_status_by_id("/a/old.log", Some(created.id))
            .unwrap();
        assert_eq!(status.id, created.id);
        assert_eq!(status.path, "/a/old.log");
        assert_eq!(status.name, "new.log");

        // Path-based status still requires the live path.
        let by_path = fs.file_status("/a/new.log").unwrap();
        assert_eq!(by_path.id, created.id);
        assert_eq!(by_path.path, "/a/new.log");

        // Write path still journals/returns the request path (no hot-path rebuild).
        let blocks = fs
            .resize_by_id(
                "/a/old.log",
                Some(created.id),
                FileAllocOpts::with_truncate(128),
            )
            .unwrap();
        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/a/old.log");
        assert_eq!(blocks.status.name, "new.log");
        assert_eq!(blocks.status.len, 128);
    }

    #[test]
    fn file_status_by_id_missing_inode_returns_file_not_found() {
        let fs = test_fs("file-status-missing-inode");
        let missing_id = 9_999_999_i64;
        let err = fs
            .file_status_by_id("/missing.log", Some(missing_id))
            .unwrap_err();
        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string()
                .contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            err
        );
    }

    #[test]
    fn complete_file_by_id_keeps_request_path() {
        let fs = test_fs("complete-by-id-stale-path");
        let created = fs.create("/b/old.log", true).unwrap();
        fs.rename("/b/old.log", "/b/new.log", RenameFlags::empty())
            .unwrap();

        let blocks = fs
            .complete_file(
                "/b/old.log",
                Some(created.id),
                64,
                vec![],
                "test-client",
                true,
                None,
            )
            .unwrap()
            .expect("only_flush should return FileBlocks");

        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/b/old.log");
        assert_eq!(blocks.status.name, "new.log");
    }

    fn worker_with_status(
        id: u32,
        status: WorkerStatus,
        capacity: i64,
        available: i64,
    ) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            1,
        );
        worker.status = status;
        worker.capacity = capacity;
        worker.available = available;
        worker
    }

    #[test]
    fn filesystem_info_allocatable_only_counts_live_workers() {
        // See issue #1460: statfs must report capacity eligible for new writes,
        // i.e. only Live workers. Blacklist/Decommission workers must not
        // contribute to the allocatable view (they still count toward the total
        // physical capacity).
        let fs = test_fs("allocatable-live-only");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.add_test_worker(worker_with_status(2, WorkerStatus::Blacklist, 2000, 1500));
        fs.add_test_worker(worker_with_status(
            3,
            WorkerStatus::Decommission,
            3000,
            2500,
        ));

        let info = fs.filesystem_info().unwrap();

        // Total physical capacity across all non-lost worker states.
        assert_eq!(info.capacity, 6000, "total capacity should sum all workers");
        assert_eq!(
            info.available, 4800,
            "total available should sum all workers"
        );
        assert_eq!(info.live_workers.len(), 1);
        assert_eq!(info.blacklist_workers.len(), 1);
        assert_eq!(info.decommission_workers.len(), 1);

        // Allocatable view mirrors allocation eligibility: Live workers only.
        assert_eq!(
            info.allocatable_capacity, 1000,
            "allocatable capacity must be Live workers only"
        );
        assert_eq!(
            info.allocatable_available, 800,
            "allocatable available must be Live workers only"
        );
    }

    #[test]
    fn filesystem_info_allocatable_excludes_failed_storage_dirs() {
        // A Live worker with a healthy dir plus a failed dir: add_storage already
        // skips failed dirs, so neither total nor allocatable capacity should
        // include the failed dir's bytes.
        let fs = test_fs("allocatable-excludes-failed");
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 10,
                ..Default::default()
            },
            1,
        );
        worker.status = WorkerStatus::Live;
        worker.add_storage(StorageInfo {
            dir_id: 1,
            storage_id: "healthy".into(),
            failed: false,
            capacity: 1000,
            available: 800,
            ..Default::default()
        });
        worker.add_storage(StorageInfo {
            dir_id: 2,
            storage_id: "failed".into(),
            failed: true,
            capacity: 500,
            available: 400,
            ..Default::default()
        });
        fs.add_test_worker(worker);

        let info = fs.filesystem_info().unwrap();

        assert_eq!(
            info.capacity, 1000,
            "failed dir must not count toward total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(info.allocatable_capacity, 1000);
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn filesystem_info_allocatable_excludes_lost_workers() {
        // Lost workers live in a separate map and must not contribute to either
        // the total or the allocatable view. Insert a lost worker directly into
        // the lost map (add_test_worker only touches the live map) and assert its
        // capacity never leaks into aggregation.
        let fs = test_fs("allocatable-excludes-lost");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.worker_manager
            .write()
            .worker_map
            .lost_workers
            .insert(99, worker_with_status(99, WorkerStatus::Lost, 9999, 9999));

        let info = fs.filesystem_info().unwrap();

        // The lost worker is reported but its bytes stay out of both views.
        assert_eq!(info.lost_workers.len(), 1);
        assert_eq!(
            info.capacity, 1000,
            "lost worker capacity must not leak into total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(
            info.allocatable_capacity, 1000,
            "lost worker capacity must not leak into allocatable"
        );
        assert_eq!(info.allocatable_available, 800);
    }

    /// Creates an empty file with an atime-based TTL policy and backdates its
    /// atime so the refresh throttle window (`ttl_ms / 2`) has elapsed.
    fn atime_ttl_file_due(fs: &MasterFilesystem, name: &str) {
        let path = format!("/{}", name);
        let mut opts = CreateFileOpts::with_create(true);
        opts.storage_policy = StoragePolicy {
            ttl_ms: 100, // throttle window = 50 ms
            ttl_action: TtlAction::Free,
            ..Default::default()
        };
        opts.x_attr.insert(
            INTERNAL_TTL_TYPE_XATTR.to_string(),
            TtlType::Atime.to_xattr_value(),
        );
        fs.create_with_opts(&path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();

        // Backdate the atime well past the throttle window.
        fs.set_attr(
            &path,
            SetAttrOpts {
                atime: Some((LocalTime::mills() - 3_600_000) as i64),
                ..Default::default()
            },
        )
        .unwrap();
    }

    fn file_atime(fs: &MasterFilesystem, path: &str) -> i64 {
        fs.file_status(path).unwrap().atime
    }

    fn file_ttl_type(fs: &MasterFilesystem, path: &str) -> TtlType {
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.view().unwrap();
        let inp = MasterFilesystem::resolve_path(&fs_dir, path).unwrap();
        let inode = inp.get_last_inode().unwrap();
        TtlType::from_xattr_value(
            inode
                .x_attr()
                .unwrap()
                .get(INTERNAL_TTL_TYPE_XATTR)
                .map(|v| v.as_slice()),
        )
    }

    #[test]
    fn get_block_locations_refreshes_atime_when_due() {
        let fs = test_fs("atime-refresh-due");
        let path = "/atime-refresh-due-file";
        atime_ttl_file_due(&fs, "atime-refresh-due-file");
        let before = file_atime(&fs, path);

        let blocks = fs.get_block_locations(path).unwrap();
        assert!(blocks.block_locs.is_empty(), "empty file has no blocks");

        let after = file_atime(&fs, path);
        assert!(
            after > before,
            "get_block_locations must refresh a due atime (before={}, after={})",
            before,
            after
        );
    }

    #[test]
    fn get_block_locations_throttles_fresh_atime() {
        let fs = test_fs("atime-refresh-throttled");
        let path = "/atime-fresh-file";
        atime_ttl_file_due(&fs, "atime-fresh-file");

        // First locate refreshes the due atime; the second call inside the
        // throttle window must not move it again.
        fs.get_block_locations(path).unwrap();
        let after_first = file_atime(&fs, path);
        fs.get_block_locations(path).unwrap();
        let after_second = file_atime(&fs, path);

        assert_eq!(
            after_first, after_second,
            "throttle window must suppress redundant atime writes"
        );
    }

    #[test]
    fn get_block_locations_does_not_touch_mtime_policy_files() {
        let fs = test_fs("atime-refresh-mtime-policy");
        let path = "/mtime-policy-file";
        let mut opts = CreateFileOpts::with_create(true);
        opts.storage_policy = StoragePolicy {
            ttl_ms: 100,
            ttl_action: TtlAction::Free,
            ..Default::default()
        };
        // No internal atime marker: the legacy mtime policy must not be
        // touched by reads.
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();
        fs.set_attr(
            path,
            SetAttrOpts {
                atime: Some((LocalTime::mills() - 3_600_000) as i64),
                ..Default::default()
            },
        )
        .unwrap();
        let before = file_atime(&fs, path);

        fs.get_block_locations(path).unwrap();

        assert_eq!(
            file_atime(&fs, path),
            before,
            "mtime-based TTL files must keep their atime on read"
        );
    }

    #[test]
    fn set_attr_cannot_override_internal_ttl_type_marker() {
        let fs = test_fs("set-attr-ttl-type-guard");
        let path = "/guarded-file";

        // Create a file carrying the atime marker (as the mount merge path
        // would), then try to flip it to mtime via a client-side SetAttr RPC:
        // both removal and overwrite of the master-owned key are refused.
        let mut opts = CreateFileOpts::with_create(true);
        opts.storage_policy = StoragePolicy {
            ttl_ms: 100,
            ttl_action: TtlAction::Free,
            ..Default::default()
        };
        opts.x_attr.insert(
            INTERNAL_TTL_TYPE_XATTR.to_string(),
            TtlType::Atime.to_xattr_value(),
        );
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();

        let err = fs
            .set_attr(
                path,
                SetAttrOpts {
                    remove_x_attr: vec![INTERNAL_TTL_TYPE_XATTR.to_string()],
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .set_attr(
                path,
                SetAttrOpts {
                    add_x_attr: HashMap::from([(
                        INTERNAL_TTL_TYPE_XATTR.to_string(),
                        TtlType::Mtime.to_xattr_value(),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let marker = file_ttl_type(&fs, path);
        assert_eq!(
            marker,
            TtlType::Atime,
            "SetAttr must not flip the master-owned TTL-type marker"
        );
    }

    #[test]
    fn overwrite_syncs_ttl_type_marker_both_ways() {
        let fs = test_fs("overwrite-ttl-type-sync");
        let path = "/overwritten-file";

        // Created under an mtime policy (no marker).
        let mut mtime_opts = CreateFileOpts::with_create(true);
        mtime_opts.storage_policy = StoragePolicy {
            ttl_ms: 100,
            ttl_action: TtlAction::Free,
            ..Default::default()
        };
        fs.create_with_opts(
            path,
            mtime_opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        assert_eq!(file_ttl_type(&fs, path), TtlType::Mtime);

        // Let the first generation's atime go stale: if the overwrite kept
        // it, the file would already be past old_atime + ttl_ms here.
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Overwritten under an atime mount: the merged opts carry the marker.
        let mut atime_opts = CreateFileOpts::with_create(true);
        atime_opts.storage_policy = StoragePolicy {
            ttl_ms: 60,
            ttl_action: TtlAction::Free,
            ..Default::default()
        };
        atime_opts.x_attr.insert(
            INTERNAL_TTL_TYPE_XATTR.to_string(),
            TtlType::Atime.to_xattr_value(),
        );
        fs.create_with_opts(
            path,
            atime_opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        assert_eq!(
            file_ttl_type(&fs, path),
            TtlType::Atime,
            "overwrite under an atime policy must flip the marker to atime"
        );

        // The overwrite must start a fresh TTL window: expiration hangs off
        // the new clock (mtime == atime == now), not the pre-overwrite read
        // time, or the checker would Free/Delete the new contents at once.
        // All inode access stays inside the fs_dir guard scope: InodePtr is a
        // non-owning raw handle into the in-memory tree.
        {
            let sync_fs_dir = fs.fs_dir();
            let fs_dir = sync_fs_dir.view().unwrap();
            // Bind the InodePath first: chained temporaries drop the owned
            // backing store before the non-owning InodePtr is used.
            let inp = MasterFilesystem::resolve_path(&fs_dir, path).unwrap();
            let view = inp.get_last_inode().unwrap();
            let expiration = view.expiration_ms().unwrap();
            let mtime = view.as_file_ref().unwrap().mtime;
            assert_eq!(
                expiration.unwrap(),
                mtime + 60,
                "expiration must use the overwrite-time clock, not the old atime"
            );
            let now = curvine_runtime::common::LocalTime::mills() as i64;
            assert!(
                now < expiration.unwrap(),
                "overwritten file must not be expired right after overwrite \
                 (expiration {expiration:?}, now {now})"
            );
            assert!(!view.is_expired().unwrap());
        }

        // Overwritten again under an mtime policy (no marker): it clears.
        let cleared_opts = CreateFileOpts::with_create(true);
        fs.create_with_opts(
            path,
            cleared_opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        assert_eq!(
            file_ttl_type(&fs, path),
            TtlType::Mtime,
            "overwrite under an mtime policy must clear the atime marker"
        );
    }

    #[test]
    fn ttl_skips_in_use_files_and_complete_reopens_the_window() {
        let fs = test_fs("ttl-writing-protection");
        let path = "/in-use-file";

        let mut opts = CreateFileOpts::with_create(true);
        opts.storage_policy = StoragePolicy {
            ttl_ms: 60,
            ttl_action: TtlAction::Delete,
            ..Default::default()
        };
        opts.x_attr.insert(
            INTERNAL_TTL_TYPE_XATTR.to_string(),
            TtlType::Atime.to_xattr_value(),
        );
        fs.create_with_opts(path, opts, OpenFlags::new_create())
            .unwrap();

        // Outlive the ttl while the write is still open.
        std::thread::sleep(std::time::Duration::from_millis(150));

        let executor = crate::master::meta::inode::ttl::InodeTtlExecutor::with_managers(fs.clone());
        let inode_id = {
            let sync_fs_dir = fs.fs_dir();
            let fs_dir = sync_fs_dir.view().unwrap();
            let inp = MasterFilesystem::resolve_path(&fs_dir, path).unwrap();
            inp.get_last_inode().unwrap().id()
        };

        // 1) An expired but still-writing file must not be touched.
        let (executed, _, _) = executor.execute_by_id(inode_id).unwrap();
        assert!(
            !executed,
            "TTL must not touch a file that is still being written"
        );
        assert!(
            fs.file_status(path).is_ok(),
            "writing file must survive its ttl"
        );

        // 2) Completing the write refreshes both clocks, so the file is born
        //    with a fresh window instead of already-expired.
        // client_name must match the one used at create time for the write
        // session to be closed.
        fs.complete_file(path, Some(inode_id), 0, vec![], "", false, None)
            .unwrap();
        let (executed, _, _) = executor.execute_by_id(inode_id).unwrap();
        assert!(
            !executed,
            "a just-completed file must have a fresh TTL window, not be born expired"
        );
        assert!(fs.file_status(path).is_ok());

        // 3) Once that window lapses, the checker deletes it normally.
        std::thread::sleep(std::time::Duration::from_millis(150));
        let (executed, _, _) = executor.execute_by_id(inode_id).unwrap();
        assert!(executed, "expired completed file must be reclaimed");
        assert!(
            fs.file_status(path).is_err(),
            "file must be gone after ttl delete"
        );
    }

    #[test]
    fn worker_info_rest_json_omits_scheduled_fields() {
        let mut worker = worker_with_status(1, WorkerStatus::Live, 1000, 800);
        worker.scheduled_bytes = 128;
        worker.scheduled_since_ms = 99;
        let json = serde_json::to_string(&worker).unwrap();
        assert!(!json.contains("scheduled_bytes"));
        assert!(!json.contains("scheduled_since_ms"));
    }

    #[test]
    fn assign_worker_retry_does_not_leak_scheduled_bytes() {
        let fs = test_fs("assign-worker-retry-unschedule");
        let capacity = 1 << 30;
        fs.add_test_worker(worker_with_status(
            1,
            WorkerStatus::Live,
            capacity,
            capacity,
        ));

        let created = fs.create("/retry.log", true).unwrap();
        let blocks = fs
            .resize(
                "/retry.log",
                FileAllocOpts::with_alloc(created.block_size, FileAllocMode::DEFAULT),
            )
            .unwrap();
        let located = &blocks.block_locs[0];
        assert!(located.block.alloc_opts.is_some());
        assert!(located.locs.is_empty());
        let block = located.block.clone();
        let client = ClientAddress::default();

        fs.assign_worker("/retry.log", block.clone(), client.clone(), vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            created.block_size
        );

        // Idempotent retry must not stack a second reservation on top of the
        // first successful assignment.
        fs.assign_worker("/retry.log", block.clone(), client.clone(), vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            created.block_size
        );

        // After heartbeat reports the write, leftover reservation is gone.
        // Another retry still must not leak scheduled_bytes.
        {
            let mut wm = fs.worker_manager.write();
            let worker = wm.worker_map.workers.get_mut(&1).unwrap();
            let prev = worker.available;
            worker.available = prev - created.block_size;
            worker.reclaim_scheduled_from_heartbeat(prev);
            assert_eq!(worker.scheduled_bytes, 0);
        }

        fs.assign_worker("/retry.log", block, client, vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            0
        );
    }

    #[path = "cache_invalidation_lock_test.rs"]
    mod cache_invalidation_lock_test;
    #[path = "complete_file_sharding_test.rs"]
    mod complete_file_sharding_test;
    #[path = "create_failure_test.rs"]
    mod create_failure_test;
    #[path = "expect_gen_test.rs"]
    mod expect_gen_test;
    #[path = "fallback_contract_test.rs"]
    mod fallback_contract_test;
    #[path = "fs_mode_release_gate_test.rs"]
    mod fs_mode_release_gate_test;
    #[path = "get_block_locations_batch_test.rs"]
    mod get_block_locations_batch_test;
    #[path = "lost_worker_gate_test.rs"]
    mod lost_worker_gate_test;
    #[path = "recursive_confirmation_lifecycle_test.rs"]
    mod recursive_confirmation_lifecycle_test;
    #[path = "recursive_confirmation_support.rs"]
    mod recursive_confirmation_support;
    #[path = "recursive_confirmation_test.rs"]
    mod recursive_confirmation_test;
    #[path = "recursive_location_lock_test.rs"]
    mod recursive_location_lock_test;
    #[path = "recursive_lock_set_test.rs"]
    mod recursive_lock_set_test;
    #[path = "rename_failure_test.rs"]
    mod rename_failure_test;
    #[path = "role_epoch_test.rs"]
    mod role_epoch_test;
    #[path = "set_attr_lock_test.rs"]
    mod set_attr_lock_test;
    #[path = "shared_handles_test.rs"]
    mod shared_handles_test;
    #[path = "snapshot_block_reads_test.rs"]
    mod snapshot_block_reads_test;
    #[path = "worker_recovery_test.rs"]
    mod worker_recovery_test;
    #[path = "write_planning_test.rs"]
    mod write_planning_test;

    #[test]
    fn soft_delete_moves_file_into_trash_with_origin_metadata() {
        let fs = test_fs_with_trash("soft-file");
        fs.create("/data/a.log", true).unwrap();

        fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
            .unwrap();

        assert!(matches!(
            fs.get_block_locations("/data/a.log").unwrap_err(),
            FsError::FileNotFound(_)
        ));

        let mut entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.remove(0);
        assert_eq!(
            entry
                .x_attr
                .get(trash::TRASH_XATTR_ORIGIN)
                .map(Vec::as_slice),
            Some(b"/data/a.log".as_slice())
        );
        let delete_ms = i64::from_le_bytes(
            entry
                .x_attr
                .get(trash::TRASH_XATTR_DELETE_MS)
                .unwrap()
                .as_slice()
                .try_into()
                .unwrap(),
        );
        assert!(delete_ms > 0);
        assert!(entry.name.starts_with(&format!("{}-", delete_ms)));
    }

    #[test]
    fn soft_delete_moves_directory_subtree_into_trash() {
        let fs = test_fs_with_trash("soft-dir");
        fs.create("/dir/child.log", true).unwrap();

        let err = fs
            .delete_with_mode("/dir", false, DeleteMode::Default)
            .unwrap_err();
        assert!(matches!(err, FsError::DirNotEmpty(_)), "got {:?}", err);

        fs.delete_with_mode("/dir", true, DeleteMode::Default)
            .unwrap();

        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        assert_eq!(entries.len(), 1);
        let trashed_child = format!("/.Trash/{}/child.log", entries[0].name);
        assert!(fs.get_block_locations(&trashed_child).is_ok());
        assert!(fs.get_block_locations("/dir/child.log").is_err());
    }

    #[test]
    fn deleting_a_hard_link_keeps_hard_unlink_semantics() {
        let fs = test_fs_with_trash("hardlink");
        fs.create("/a.log", true).unwrap();
        fs.link("/a.log", "/b.log").unwrap();

        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();

        assert!(fs.get_block_locations("/b.log").is_ok());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn skip_trash_deletes_permanently() {
        let fs = test_fs_with_trash("skip-trash");
        fs.create("/keep.log", true).unwrap();

        fs.delete_with_mode("/keep.log", false, DeleteMode::SkipTrash)
            .unwrap();

        assert!(fs.get_block_locations("/keep.log").is_err());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn soft_delete_falls_back_to_hard_delete_when_disabled() {
        let fs = test_fs("trash-disabled");
        fs.create("/a.log", true).unwrap();

        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();

        assert!(fs.get_block_locations("/a.log").is_err());
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());
    }

    #[test]
    fn trash_move_and_purge_transfer_directory_link_counts() {
        let fs = test_fs_with_trash("trash-nlink");
        fs.mkdir("/parent", true).unwrap();
        fs.mkdir("/parent/child", true).unwrap();
        let parent_nlink = fs.file_status("/parent").unwrap().nlink;

        fs.delete_with_mode("/parent/child", true, DeleteMode::Default)
            .unwrap();

        assert_eq!(
            fs.file_status("/parent").unwrap().nlink,
            parent_nlink - 1,
            "the source parent must drop the moved directory link"
        );
        let mut entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let trash_nlink = fs.file_status("/.Trash").unwrap().nlink;
        let entry_path = format!("/.Trash/{}", entries.remove(0).name);

        fs.delete_with_mode(&entry_path, true, DeleteMode::Default)
            .unwrap();

        assert_eq!(
            fs.file_status("/.Trash").unwrap().nlink,
            trash_nlink - 1,
            "purging the entry must drop the trash root link"
        );
    }

    #[test]
    fn client_set_attr_cannot_touch_internal_xattrs() {
        let fs = test_fs("internal-xattr");
        fs.create("/a.log", true).unwrap();

        for key in [trash::TRASH_XATTR_ORIGIN, trash::TRASH_XATTR_DELETE_MS] {
            let err = fs
                .set_attr(
                    "/a.log",
                    SetAttrOptsBuilder::new()
                        .add_x_attr(key, b"x".to_vec())
                        .build(),
                )
                .unwrap_err();
            assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

            let err = fs
                .set_attr(
                    "/a.log",
                    SetAttrOptsBuilder::new().remove_x_attr(key).build(),
                )
                .unwrap_err();
            assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
        }
    }

    #[test]
    fn trash_root_is_protected_and_entries_are_purged_permanently() {
        let fs = test_fs_with_trash("trash-purge");
        fs.create("/a.log", true).unwrap();
        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();

        let err = fs
            .delete_with_mode("/.Trash", true, DeleteMode::Default)
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let entry_path = format!("/.Trash/{}", entries[0].name);
        fs.delete_with_mode(&entry_path, true, DeleteMode::Default)
            .unwrap();
        assert!(fs
            .list_options("/.Trash", ListOptions::default())
            .unwrap()
            .is_empty());
    }

    // Expiry is a permanent removal. Sending it through the recycle bin kept
    // the blocks for another retention window while the executor reported the
    // length as reclaimed, so the freed space never showed up.
    #[test]
    fn ttl_expiry_bypasses_the_recycle_bin() {
        use crate::master::meta::inode::ttl::InodeTtlExecutor;

        let fs = test_fs_with_trash("ttl-permanent");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        fs.create_with_opts(
            "/ttl/expired.log",
            opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        fs.complete_file("/ttl/expired.log", None, 10, vec![], "", false, None)
            .unwrap();
        let inode_id = fs.file_status("/ttl/expired.log").unwrap().id;
        std::thread::sleep(std::time::Duration::from_millis(10));

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let (handled, _, reclaimed) = executor.execute_by_id(inode_id).unwrap();

        assert!(handled);
        assert_eq!(reclaimed, 10, "the expired length is reported as reclaimed");
        assert!(fs.file_status("/ttl/expired.log").is_err());
        assert!(
            fs.list_trash().unwrap().is_empty(),
            "an expired file must not become restorable"
        );
    }

    #[test]
    fn ttl_does_not_expunge_trashed_entries() {
        use crate::master::meta::inode::ttl::InodeTtlExecutor;

        let fs = test_fs_with_trash("ttl-trash");
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        fs.create_with_opts(
            "/ttl/expired.log",
            opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .unwrap();
        fs.complete_file("/ttl/expired.log", None, 0, vec![], "", false, None)
            .unwrap();
        let inode_id = fs.file_status("/ttl/expired.log").unwrap().id;
        std::thread::sleep(std::time::Duration::from_millis(50));

        fs.delete_with_mode("/ttl/expired.log", false, DeleteMode::Default)
            .unwrap();
        let entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        let trashed_path = format!("/.Trash/{}", entries[0].name);

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let (handled, _, reclaimed) = executor.execute_by_id(inode_id).unwrap();

        assert!(handled);
        assert_eq!(
            reclaimed, 0,
            "a skipped trash entry must not report reclaimed bytes"
        );
        assert!(fs.get_block_locations(&trashed_path).is_ok());
    }

    #[test]
    fn list_trash_reports_origin_time_and_status() {
        let fs = test_fs_with_trash("trash-list");
        fs.create("/a.log", true).unwrap();
        fs.create("/b.log", true).unwrap();
        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();
        fs.delete_with_mode("/b.log", false, DeleteMode::Default)
            .unwrap();

        let entries = fs.list_trash().unwrap();
        assert_eq!(entries.len(), 2);

        let mut origins: Vec<&str> = entries.iter().map(|e| e.origin.as_str()).collect();
        origins.sort();
        assert_eq!(origins, vec!["/a.log", "/b.log"]);

        for entry in &entries {
            assert!(entry.path.starts_with("/.Trash/"));
            assert_eq!(entry.status.path, entry.path);
            assert!(entry.delete_ms > 0);
            assert!(entry
                .status
                .name
                .starts_with(&format!("{}-", entry.delete_ms)));
        }
    }

    #[test]
    fn list_trash_page_resumes_without_repeating_entries() {
        let fs = test_fs_with_trash("trash-list-page");
        for i in 0..250 {
            fs.create(format!("/p-{}.log", i), true).unwrap();
        }
        for i in 0..250 {
            fs.delete_with_mode(format!("/p-{}.log", i), false, DeleteMode::Default)
                .unwrap();
        }

        let mut page_sizes = Vec::new();
        let mut paths = Vec::new();
        let mut cursor = None;
        loop {
            let page = fs.list_trash_page(cursor.as_ref(), 100).unwrap();
            page_sizes.push(page.entries.len());
            paths.extend(page.entries.into_iter().map(|entry| entry.path));
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(page_sizes, vec![100, 100, 50]);
        assert_eq!(paths.len(), 250);
        let unique: HashSet<&String> = paths.iter().collect();
        assert_eq!(unique.len(), 250, "a page must not repeat an entry");

        assert_eq!(fs.list_trash().unwrap().len(), 250);
    }

    #[test]
    fn restore_trash_reports_the_effective_destination_and_status() {
        let fs = test_fs_with_trash("trash-restore-outcome");
        fs.create("/a.log", true).unwrap();
        fs.delete_with_mode("/a.log", false, DeleteMode::Default)
            .unwrap();
        let entry = fs.list_trash().unwrap().remove(0);

        assert_eq!(fs.restore_dst(&entry.path, None).unwrap(), "/a.log");
        assert_eq!(
            fs.restore_dst(&entry.path, Some("/b.log")).unwrap(),
            "/b.log"
        );

        let restored = fs.restore_trash(&entry.path, None, false).unwrap();
        assert_eq!(restored.dst, "/a.log");
        assert_eq!(restored.status.path, "/a.log");
        assert!(restored.replaced.is_none());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_without_dst_reports_a_missing_origin_parent() {
        let fs = test_fs_with_trash("trash-restore-missing-parent");
        fs.mkdir("/data", true).unwrap();
        fs.create("/data/a.log", true).unwrap();
        fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
            .unwrap();
        fs.delete_with_mode("/data", true, DeleteMode::Default)
            .unwrap();

        let entry = fs
            .list_trash()
            .unwrap()
            .into_iter()
            .find(|entry| entry.origin == "/data/a.log")
            .unwrap();

        let err = fs.restore_trash(&entry.path, None, false).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {err:?}");
        assert!(
            err.to_string().contains("parent directory"),
            "message should explain the missing parent, got {err}"
        );

        let err = fs
            .restore_trash(&entry.path, Some("/data/a.log"), false)
            .unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {err:?}");
    }

    #[test]
    fn restore_trash_moves_entry_back_to_origin() {
        let fs = test_fs_with_trash("trash-restore");
        fs.create("/data/a.log", true).unwrap();
        fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
            .unwrap();

        let entry = fs.list_trash().unwrap().remove(0);
        assert!(fs.get_block_locations(&entry.path).is_ok());

        fs.restore_trash(&entry.path, None, false).unwrap();

        assert!(fs.get_block_locations("/data/a.log").is_ok());
        assert!(fs.get_block_locations(&entry.path).is_err());
        assert!(fs.list_trash().unwrap().is_empty());
        let status = fs.file_status("/data/a.log").unwrap();
        assert!(!status.x_attr.contains_key(trash::TRASH_XATTR_ORIGIN));
        assert!(!status.x_attr.contains_key(trash::TRASH_XATTR_DELETE_MS));
    }

    #[test]
    fn restore_trash_restores_directory_subtrees() {
        let fs = test_fs_with_trash("trash-restore-dir");
        fs.create("/data/child.log", true).unwrap();
        fs.delete_with_mode("/data", true, DeleteMode::Default)
            .unwrap();

        let entry = fs.list_trash().unwrap().remove(0);
        fs.restore_trash(&entry.path, None, false).unwrap();

        assert!(fs.get_block_locations("/data/child.log").is_ok());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_validates_destination_and_overwrites_on_request() {
        let fs = test_fs_with_trash("trash-restore-dst");
        fs.create("/src.log", true).unwrap();
        fs.delete_with_mode("/src.log", false, DeleteMode::Default)
            .unwrap();
        let entry = fs.list_trash().unwrap().remove(0);

        let err = fs
            .restore_trash(&entry.path, Some("/.Trash/other"), false)
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .restore_trash(&entry.path, Some("/missing/src.log"), false)
            .unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        fs.mkdir("/dst", true).unwrap();
        fs.create("/dst/src.log", true).unwrap();
        let err = fs
            .restore_trash(&entry.path, Some("/dst/src.log"), false)
            .unwrap_err();
        assert!(
            matches!(err, FsError::FileAlreadyExists(_)),
            "got {:?}",
            err
        );
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        fs.restore_trash(&entry.path, Some("/dst/src.log"), true)
            .unwrap();
        assert!(fs.get_block_locations("/dst/src.log").is_ok());
        assert!(fs.list_trash().unwrap().is_empty());
    }

    #[test]
    fn restore_trash_rejects_paths_outside_the_trash() {
        let fs = test_fs_with_trash("trash-restore-invalid");
        fs.create("/plain.log", true).unwrap();

        for path in ["/plain.log", "/.Trash", "/.Trash/", "/.Trash/a/b"] {
            let err = fs.restore_trash(path, None, false).unwrap_err();
            assert!(
                matches!(err, FsError::InvalidArgument(_)),
                "{} -> {:?}",
                path,
                err
            );
        }

        let err = fs
            .restore_trash("/.Trash/missing", None, false)
            .unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
    }

    #[test]
    fn empty_trash_purges_entries_but_keeps_the_trash_root() {
        let fs = test_fs_with_trash("trash-empty");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        let empty = fs.empty_trash().unwrap();
        assert_eq!(empty.inodes, 0);
        assert!(fs.list_options("/.Trash", ListOptions::default()).is_err());

        fs.create("/dir/child.log", true).unwrap();
        fs.create("/keep.log", true).unwrap();
        fs.delete_with_mode("/dir", true, DeleteMode::Default)
            .unwrap();
        fs.delete_with_mode("/keep.log", false, DeleteMode::Default)
            .unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 2);

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 2);
        // Every visible trash entry is gone, and the internal async-staging
        // directory stays behind without being listed to clients.
        assert!(fs.list_trash().unwrap().is_empty());
        let children: Vec<String> = fs
            .list_options("/.Trash", ListOptions::default())
            .unwrap()
            .into_iter()
            .map(|child| child.name)
            .collect();
        assert!(
            children.is_empty(),
            "the staging dir must be hidden: {:?}",
            children
        );
        assert!(fs.get_block_locations("/dir/child.log").is_err());

        fs.create("/next.log", true).unwrap();
        fs.delete_with_mode("/next.log", false, DeleteMode::Default)
            .unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 1);
    }

    #[test]
    fn empty_trash_does_not_purge_foreign_children() {
        let fs = test_fs_with_trash("trash-empty-foreign");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        fs.create("/keep.log", true).unwrap();
        fs.delete_with_mode("/keep.log", false, DeleteMode::Default)
            .unwrap();
        assert_eq!(fs.list_trash().unwrap().len(), 1);

        // A child whose name is not a trash entry name is not shown by
        // ListTrash, so EmptyTrash must leave it alone instead of deleting it
        // silently and without an undo path.
        let (foreign_path, _) = seed_trash_entry(&fs, "not-a-trash-entry");

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 1);
        assert!(fs.get_block_locations(&foreign_path).is_ok());
        let children = fs
            .list_options(trash::TRASH_ROOT, ListOptions::default())
            .unwrap();
        assert!(
            children
                .iter()
                .any(|child| child.name == "not-a-trash-entry"),
            "foreign child must survive EmptyTrash: {:?}",
            children.iter().map(|child| &child.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_trash_purges_healthy_entries_when_one_entry_is_corrupt() {
        let fs = test_fs_with_trash("trash-empty-partial");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        let worker_id = 100;
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);

        fs.create("/good.log", true).unwrap();
        let block = fs
            .add_block(
                "/good.log",
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
        let block_id = block.block.id;
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .add_block_location(block_id, BlockLocation::with_id(worker_id))
            .unwrap();
        fs.complete_file(
            "/good.log",
            None,
            1,
            vec![CommitBlock {
                block_id,
                block_len: 1,
                locations: vec![BlockLocation::with_id(worker_id)],
            }],
            "",
            false,
            None,
        )
        .unwrap();
        fs.delete_with_mode("/good.log", false, DeleteMode::Default)
            .unwrap();

        fs.create("/broken.log", true).unwrap();
        fs.delete_with_mode("/broken.log", false, DeleteMode::Default)
            .unwrap();

        // Drop the store record of one trash entry so it can no longer be
        // resolved. Staging skips it instead of failing the sweep, and the
        // healthy entry must still be purged with its blocks released.
        let entries = fs.list_trash().unwrap();
        let broken_id = entries
            .iter()
            .find(|entry| entry.origin == "/broken.log")
            .unwrap()
            .status
            .id;
        let good_entry = entries
            .iter()
            .find(|entry| entry.origin == "/good.log")
            .unwrap()
            .path
            .clone();
        let fs_dir = fs.fs_dir.view().unwrap();
        let mut batch = fs_dir.store.new_batch();
        batch.delete_inode(broken_id).unwrap();
        batch.commit().unwrap();
        drop(fs_dir);

        let res = fs.empty_trash().unwrap();
        assert_eq!(res.inodes, 1, "only the healthy entry is reclaimed");
        assert!(fs.get_block_locations("/good.log").is_err());
        assert!(
            !fs.exists(&good_entry).unwrap(),
            "the healthy entry must leave the recycle bin"
        );

        let cmds = fs
            .worker_manager
            .write()
            .block_map
            .handle_heartbeat(worker_id);
        let pending: HashSet<i64> = cmds
            .iter()
            .flat_map(|cmd| match cmd {
                WorkerCommand::DeleteBlock(cmd) => cmd.blocks.clone(),
            })
            .collect();
        assert!(
            pending.contains(&block_id),
            "block {block_id} was not queued for removal: {pending:?}"
        );
    }

    #[test]
    fn trash_namespace_is_reserved_for_creates() {
        let fs = test_fs("reserved-create");

        let err = fs.create("/.Trash/entry.log", true).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs.mkdir("/.Trash/entry-dir", true).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        // Names that merely start with the reserved prefix are allowed.
        fs.mkdir("/.Trashy", true).unwrap();
    }

    #[test]
    fn trash_namespace_is_reserved_for_rename() {
        let fs = test_fs("reserved-rename");
        fs.mkdir("/data", true).unwrap();

        let err = fs
            .rename("/data", "/.Trash/moved", RenameFlags::empty())
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .rename("/.Trash", "/moved", RenameFlags::empty())
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn trash_namespace_is_reserved_for_links() {
        let fs = test_fs("reserved-links");
        fs.create("/data.log", true).unwrap();

        let err = fs.link("/data.log", "/.Trash/link.log").unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let err = fs
            .symlink("/data.log", "/.Trash/sym.log", false, 0o777)
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn trash_namespace_rejects_writes_but_allows_reads() {
        let fs = test_fs("reserved-write");
        fs.create("/data.log", true).unwrap();

        let err = fs
            .open_file(
                "/.Trash/new.log",
                CreateFileOpts::with_create(false),
                OpenFlags::new_create(),
            )
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        // Read-only lookups do not touch the reserved namespace guard.
        let err = fs.get_block_locations("/.Trash/absent.log").unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
    }

    #[test]
    fn trash_root_cannot_be_deleted() {
        let fs = test_fs("reserved-delete-root");

        let err = fs.delete(trash::TRASH_ROOT, true).unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn trash_entries_remain_permanently_deletable() {
        let fs = test_fs("reserved-delete-entry");
        let (path, _) = seed_trash_entry(&fs, "entry.log");

        fs.delete(&path, false).unwrap();

        let err = fs.get_block_locations(&path).unwrap_err();
        assert!(matches!(err, FsError::FileNotFound(_)), "got {:?}", err);
    }

    #[test]
    fn trash_namespace_is_reserved_for_free_and_set_attr() {
        let fs = test_fs("reserved-free-attr");
        let (path, _) = seed_trash_entry(&fs, "entry.log");

        for (label, err) in [
            ("free entry", fs.free(&path, false).unwrap_err()),
            ("free root", fs.free(trash::TRASH_ROOT, true).unwrap_err()),
            (
                "set_attr entry",
                fs.set_attr(&path, SetAttrOptsBuilder::new().mode(0o600).build())
                    .unwrap_err(),
            ),
            (
                "set_attr root",
                fs.set_attr(
                    trash::TRASH_ROOT,
                    SetAttrOptsBuilder::new().mode(0o600).build(),
                )
                .unwrap_err(),
            ),
        ] {
            assert!(
                matches!(err, FsError::InvalidArgument(_)),
                "{}: got {:?}",
                label,
                err
            );
        }

        // Regular paths keep their previous behavior.
        fs.create("/data.log", true).unwrap();
        fs.set_attr("/data.log", SetAttrOptsBuilder::new().mode(0o600).build())
            .unwrap();
        fs.free("/data.log", false).unwrap();
    }

    #[test]
    fn trash_namespace_is_reserved_for_resize() {
        let fs = test_fs("reserved-resize");
        let (path, inode_id) = seed_trash_entry(&fs, "entry.log");

        let err = fs
            .resize(&path, FileAllocOpts::with_truncate(0))
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        // By-id requests may carry a stale path, so the live path is checked too.
        let err = fs
            .resize_by_id(
                "/data/stale.log",
                Some(inode_id),
                FileAllocOpts::with_truncate(0),
            )
            .unwrap_err();
        assert!(matches!(err, FsError::InvalidArgument(_)), "got {:?}", err);

        let created = fs.create("/data.log", true).unwrap();
        let blocks = fs
            .resize_by_id(
                "/data.log",
                Some(created.id),
                FileAllocOpts::with_truncate(128),
            )
            .unwrap();
        assert_eq!(blocks.status.len, 128);
    }

    // Journal enabled in testing mode: entries stay on the writer's channel and
    // every journaled mutation emits a leader checkpoint.
    fn test_fs_snapshot_every_entry(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = true;
        conf.journal.leader_create_snapshot = Some(true);
        conf.journal.snapshot_entries = 1;
        conf.journal.snapshot_min_interval = curvine_runtime::common::DurationUnit::new(0);
        conf.master.trash_enabled = true;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-purge-snapshot/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-purge-snapshot/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        fs
    }

    // Regression: the leader checkpoint emitted while journaling a purge batch
    // must already exclude the purged inodes. Journaling before applying left
    // the victims in a checkpoint whose watermark covered the purge, so a
    // recovery from it resurrected metadata for released blocks.
    #[test]
    fn purge_checkpoint_excludes_the_purged_inodes() {
        use crate::master::fs::TrashPurger;
        use crate::master::journal::JournalEntry;
        use crate::master::meta::store::RocksInodeStore;

        let fs = test_fs_snapshot_every_entry("apply-before-log");
        let victim = fs.create("/data/victim.log", true).unwrap();
        fs.delete_with_mode("/data/victim.log", false, DeleteMode::Default)
            .unwrap();
        let entry = fs.list_trash().unwrap().remove(0);
        let writer = fs.fs_dir.view().unwrap().journal_writer.clone();
        writer.take_entries();

        TrashPurger::new(fs.clone(), fs.master_monitor.clone())
            .purge_entry(&entry.path)
            .unwrap()
            .expect("leader");

        let entries = writer.take_entries();
        let purge_at = entries
            .iter()
            .position(|e| matches!(e, JournalEntry::TrashPurge(_)))
            .expect("purge must be journaled");
        let checkpoint = entries[purge_at..]
            .iter()
            .find_map(|e| match e {
                JournalEntry::Snapshot(s) => Some(s.dir.clone()),
                _ => None,
            })
            .expect("snapshot_entries = 1 emits a checkpoint after the purge record");

        // Open the checkpoint itself as a store (a separate scratch dir keeps its
        // own checkpoints away from the live DB).
        let mut db_conf = fs.conf.rocksdb.clone().set_dir(Utils::test_sub_dir(format!(
            "master-fs-purge-snapshot/open-{}",
            Utils::rand_str(6)
        )));
        db_conf.data_dir = checkpoint.clone();
        let store = RocksInodeStore::new(db_conf, false).unwrap();
        assert!(
            store.get_inode(victim.id).unwrap().is_none(),
            "checkpoint {} still holds purged inode {}",
            checkpoint,
            victim.id
        );
    }

    // `delete()` serves TTL eviction and quota, which count the inode's bytes as
    // reclaimed; it must bypass the recycle bin even when trash is enabled.
    #[test]
    fn delete_bypasses_the_recycle_bin_when_trash_is_enabled() {
        let fs = test_fs_with_trash("delete-skips-trash");
        fs.create("/data/ttl.log", true).unwrap();

        let res = fs.delete("/data/ttl.log", false).unwrap();

        assert!(!res.trashed, "delete() must not soft-delete");
        assert!(fs.list_trash().unwrap().is_empty());
        assert!(!fs.exists(trash::TRASH_ROOT).unwrap());
    }

    // `/.Trash/.pending` is master-owned staging: no client RPC may stat, list,
    // open, rename, restore or synchronously delete it.
    #[test]
    fn pending_staging_dir_is_not_addressable_by_clients() {
        let fs = test_fs_with_trash("pending-hidden");
        fs.create("/kept/k.log", true).unwrap();
        fs.delete_with_mode("/kept", true, DeleteMode::Default)
            .unwrap();
        fs.create("/victim/a.log", true).unwrap();
        fs.delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
            .unwrap();
        let staged = fs.scan_pending_trash(16, None).unwrap().paths.remove(0);
        let staged_child = format!("{}/a.log", staged);

        for path in [
            trash::TRASH_PENDING_ROOT,
            staged.as_str(),
            staged_child.as_str(),
        ] {
            assert!(!fs.exists(path).unwrap(), "{} must not exist", path);
            assert_file_not_found_roundtrip(&fs.file_status(path).unwrap_err());
            assert_file_not_found_roundtrip(&fs.list_status(path).unwrap_err());
            assert_file_not_found_roundtrip(
                &fs.list_options(path, ListOptions::default()).unwrap_err(),
            );
            assert_file_not_found_roundtrip(&fs.get_block_locations(path).unwrap_err());
            assert_file_not_found_roundtrip(&fs.delete(path, true).unwrap_err());
            assert_file_not_found_roundtrip(
                &fs.delete_with_mode(path, true, DeleteMode::AsyncPurge)
                    .unwrap_err(),
            );
            assert!(fs.rename(path, "/moved", RenameFlags::empty()).is_err());
            assert!(fs.restore_trash(path, None, false).is_err());
        }

        // Listing the bin shows the restorable entry but never `.pending`.
        let names = |statuses: Vec<FileStatus>| -> Vec<String> {
            statuses.into_iter().map(|s| s.name).collect()
        };
        let listed = names(fs.list_status(trash::TRASH_ROOT).unwrap());
        assert_eq!(listed.len(), 1, "got {:?}", listed);
        assert!(!listed.iter().any(|n| n == trash::TRASH_PENDING_NAME));
        let listed = names(
            fs.list_options(trash::TRASH_ROOT, ListOptions::default())
                .unwrap(),
        );
        assert_eq!(listed.len(), 1, "got {:?}", listed);
        let globbed = names(fs.list_status("/.Trash/*").unwrap());
        assert!(
            globbed.iter().all(|n| n != "a.log"),
            "glob must not descend into .pending: {:?}",
            globbed
        );

        // The staged entry is still drained by the purger.
        assert_eq!(fs.scan_pending_trash(16, None).unwrap().paths.len(), 1);
    }

    // A blocking empty-trash owns `/.Trash/.pending` from the staging rename to
    // the drain that has to report its totals. The background purger must not
    // reclaim a staged entry in that window, otherwise the blocking caller
    // reports zeros for entries it accepted.
    #[test]
    fn background_purger_yields_to_a_blocking_empty_trash() {
        let fs = test_fs_with_trash("trash-drain-owner");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        fs.create("/owned.log", true).unwrap();
        fs.delete_with_mode("/owned.log", false, DeleteMode::Default)
            .unwrap();

        // Stand in for a blocking empty-trash: it holds the pending directory
        // while it stages and drains.
        let guard = fs.lock_pending_drain();
        assert_eq!(fs.stage_trash_for_purge().unwrap(), 1);

        let purger =
            crate::master::fs::PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone());
        assert_eq!(
            purger.drain_pending().unwrap(),
            Some((0, 0, 0)),
            "the background cycle must skip instead of stealing the entry"
        );
        assert_eq!(
            fs.scan_pending_trash(16, None).unwrap().paths.len(),
            1,
            "the staged entry is still there for the owner"
        );

        // Once the owner releases the directory the next cycle reclaims it.
        drop(guard);
        assert_eq!(purger.drain_pending().unwrap(), Some((1, 1, 0)));
        assert!(fs.scan_pending_trash(16, None).unwrap().paths.is_empty());
    }

    // A blocking empty cannot promise reclaimed totals once it is demoted, so it
    // must fail retryably instead of answering success with zero counts while
    // the accepted entries are still staged.
    #[test]
    fn blocking_empty_trash_fails_retryably_when_demoted() {
        let fs = test_fs_with_trash("trash-empty-demoted");
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        fs.create("/lost.log", true).unwrap();
        fs.delete_with_mode("/lost.log", false, DeleteMode::Default)
            .unwrap();

        // Accept the entries while still active, then lose leadership before the
        // drain can run.
        assert_eq!(fs.empty_trash0(false).unwrap().0, 1);
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Follower);

        let err = fs.empty_trash0(true).unwrap_err();
        assert!(
            matches!(err, FsError::NotLeaderMaster(_)),
            "a demoted master must not report a successful empty: {:?}",
            err
        );
        assert_eq!(
            fs.scan_pending_trash(16, None).unwrap().paths.len(),
            1,
            "the accepted entry stays staged for the next active master"
        );

        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        assert_eq!(fs.drain_pending_trash().unwrap().unwrap().inodes, 1);
    }
}
