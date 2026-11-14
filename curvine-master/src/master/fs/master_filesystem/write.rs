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

//! Write-admission coordination (C3 layout only).
//!
//! Holds the ordinary, typed-fallback, recursive and exclusive entry points of
//! `MasterFilesystem` plus their periodic-snapshot step. They stay distinct
//! `impl MasterFilesystem` methods with their own explicit bodies: the
//! design forbids fusing them into one mode machine, and their differences
//! (epoch capture points, fallback boundary, cold window, gate handling) must
//! remain visible at the call site. This module only gathers code that was
//! already adjacent in `master_filesystem.rs`; no protocol behaviour moved.

use super::*;

/// Sentinel for sharded write admission: `Done` carries the operation result.
/// `Fallback` marks a side-effect-free planning point that needs the global
/// lock (unresolved path, identity churn, retries exhausted); it escalates to
/// the single-writer exclusive path. A busy or failed view is an `Err`, not a
/// `Fallback`.
pub(super) enum ShardedOutcome<R> {
    Done(R),
    Fallback,
}

impl MasterFilesystem {
    pub(crate) fn run_metadata_write<R>(&self, f: impl FnOnce() -> FsResult<R>) -> FsResult<R> {
        let epoch = self.journal_writer.metadata_epoch()?;
        self.run_metadata_write_epoch(epoch, f)
    }

    pub(super) fn run_metadata_write_epoch<R>(
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
        op: &'static str,
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
            // G3: pre-hot observation checkpoint — the outer original epoch
            // has been captured and ensured; the driver may fence/unfence
            // here to prove the original-epoch data flow into hot(epoch).
            // take-once, disarms itself.
            #[cfg(test)]
            if let Some((reached, release)) = self.journal_writer.consume_pre_hot_hook() {
                let _ = reached.send(());
                let _ = release.recv_timeout(std::time::Duration::from_secs(60));
            }
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
                // S7 §7: count the final typed Fallback decision once per
                // request (cold event), labelled by the static call-site op.
                // The metrics registry is absent in pure-unit-test
                // environments; a missing registry skips the count instead of
                // constructing one (cold event, not a hot path).
                if let Some(metrics) = crate::master::MASTER_METRICS.get() {
                    metrics
                        .metadata_lock_fallback_total
                        .with_label_values(&[op])
                        .inc();
                }
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
    pub(super) fn run_recursive_metadata_write<R>(
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
                // G5: the short view block ONLY returns the owned location
                // plan — the hook consumption and L acquisition happen AFTER
                // the view is dropped, so a cold drain never waits on a
                // NamespaceGuard held by this same frame.
                let location_plan = {
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
                    plan
                };
                // G5: the view is now dropped. Consume the location hook
                // and acquire L in a separate block (no NamespaceGuard held
                // while waiting for location locks).
                #[cfg(test)]
                if let Some((reached, release)) = self.journal_writer.consume_location_pause_hook()
                {
                    let _ = reached.send(());
                    let _ = release.recv_timeout(std::time::Duration::from_secs(60));
                }
                let locks = self
                    .block_location_locks
                    .write_workers_blocks(location_plan.worker_ids(), location_plan.block_ids());
                Some((location_plan, locks))
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
                let mut confirmation =
                    MetadataConfirmation::arm(&self.fs_dir, &self.journal_writer);
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

    pub(super) fn run_metadata_write_exclusive<R>(
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
                let mut confirmation =
                    MetadataConfirmation::arm(&self.fs_dir, &self.journal_writer);
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

    pub(super) fn maybe_create_periodic_snapshot(&self, epoch: u64) {
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
}
