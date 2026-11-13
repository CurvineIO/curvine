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

#![allow(clippy::result_large_err)]

use crate::master::journal::read_barrier::JournalReadBarrier;
use crate::master::journal::recovery::RecoveryGate;
use crate::master::journal::*;
use crate::master::meta::inode::{InodeDir, InodeFile, InodePath};
use crate::master::meta::FsDir;
use crate::master::{Master, MasterMetrics};
use curvine_config::JournalConf;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_model::{
    CommitBlock, FileLock, LeaderSnapshotInfo, MountInfo, RenameFlags, SetAttrOpts,
};
use curvine_raft::conf::JournalConfExt;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::{FileUtils, LocalTime, TimeSpent};
use curvine_runtime::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use curvine_runtime::sync::{AtomicBool, AtomicCounter};
use log::{debug, info, warn};
use std::cell::RefCell;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

struct CommitScopeEntry {
    writer: *const JournalWriter,
    id: u64,
    waiters: Vec<mpsc::Receiver<()>>,
}

thread_local! {
    static JOURNAL_COMMIT_SCOPES: RefCell<Vec<CommitScopeEntry>> = const { RefCell::new(Vec::new()) };
    static NEXT_COMMIT_SCOPE_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Waits for every journal entry enqueued inside the scope to be committed.
/// A scope that never waits (journal disabled or no waiter recorded) is a
/// no-op. Dropping a scope with pending confirmations fences metadata writes:
/// the mutation may already be durable locally while its Raft outcome is
/// unknown.
pub struct JournalCommitScope<'a> {
    id: Option<u64>,
    writer: Option<&'a JournalWriter>,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl JournalCommitScope<'_> {
    pub fn wait(mut self) -> FsResult<()> {
        let Some(id) = self.id.take() else {
            return Ok(());
        };
        let waiters = Self::remove(id);
        let mut result = Ok(());
        // Test-only instance-isolated unwind injection: this scope has a real
        // ID and at least one pending waiter, so the driver's release makes
        // the wait panic after the body published and the cold owner was
        // dropped — exercising the RAII terminal path on real unwind.
        #[cfg(test)]
        if !waiters.is_empty() {
            if let Some(writer) = self.writer {
                let hook = writer.wait_unwind_hook.lock().unwrap().take();
                if let Some((reached, release)) = hook {
                    // Signal the driver from inside the real pending wait
                    // (after the hook mutex is released by take) so the
                    // driver can observe view/gate before releasing.
                    let _ = reached.send(());
                    if release
                        .recv_timeout(std::time::Duration::from_secs(30))
                        .is_ok()
                    {
                        panic!("injected journal commit wait unwind (instance hook)");
                    }
                }
            }
        }
        for waiter in waiters {
            if let Err(error) = Self::wait_for_commit(waiter) {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        if result.is_err() {
            if let Some(writer) = self.writer {
                writer.fail_metadata_commit();
            }
        }
        result
    }

    fn remove(id: u64) -> Vec<mpsc::Receiver<()>> {
        JOURNAL_COMMIT_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            let index = scopes
                .iter()
                .position(|scope| scope.id == id)
                .expect("commit scope missing");
            scopes.remove(index).waiters
        })
    }

    fn wait_for_commit(waiter: mpsc::Receiver<()>) -> FsResult<()> {
        if waiter.recv().is_err() {
            return err_box!("journal commit failed after metadata commit");
        }
        Ok(())
    }
}

impl Drop for JournalCommitScope<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let waiters = Self::remove(id);
            if !waiters.is_empty() {
                if let Some(writer) = self.writer {
                    writer.fail_metadata_commit();
                }
            }
        }
    }
}

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    /// Testing mode has no SenderTask, so no commit receipt can ever arrive;
    /// enqueue_and_wait must not block in that mode (matches Astra).
    require_raft_commit: bool,
    node_id: u64,
    recovery: RecoveryGate,
    sender: BlockingSender<QueuedJournalEntry>,
    /// Read barrier over Raft metadata catch-up; defaults to current so
    /// single-node and testing writers are never fenced off.
    read_barrier: Arc<JournalReadBarrier>,
    /// Whether the real journal queue is unbounded (cap == 0), captured at
    /// construction from the actual channel mode (design §8.3). Read-only
    /// fact; the C2 recursive fast path requires it (or a disabled journal)
    /// so no admission can block on a full bounded queue while holding
    /// subtree locks.
    journal_unbounded: bool,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<BlockingReceiver<QueuedJournalEntry>>>,

    snapshot_in_flight: AtomicBool,

    leader_create_snapshot: bool,
    snapshot_entries: u64,
    snapshot_min_interval_ms: u64,
    last_snapshot_ms: AtomicCounter,
    entries_since_snapshot: AtomicCounter,

    /// Test-only instance-isolated wait-unwind hook: when armed, the next
    /// real `JournalCommitScope::wait` with pending waiters signals
    /// `reached` (it is inside the real wait with waiters), then blocks on
    /// `release` and panics after the release. Consumed once, so it disarms
    /// itself.
    #[cfg(test)]
    wait_unwind_hook:
        std::sync::Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

impl JournalWriter {
    /// Test-only: switches the writer into real-commit-receipt mode and
    /// binds it to the live recovery gate.
    #[cfg(test)]
    pub(crate) fn enable_testing_receipts(&mut self, recovery: RecoveryGate) {
        self.require_raft_commit = true;
        self.recovery = recovery;
    }

    /// Test-only: runs `f` with the retained receiver when the writer runs
    /// without the sender task (testing builds), so cross-module regression
    /// tests can observe real queued receipts.
    #[cfg(test)]
    pub(crate) fn with_testing_receiver<R>(
        &self,
        f: impl FnOnce(&BlockingReceiver<QueuedJournalEntry>) -> R,
    ) -> Option<R> {
        self.receiver
            .as_ref()
            .map(|receiver| f(&receiver.lock().unwrap()))
    }

    pub fn new(
        testing: bool,
        client: RaftClient,
        conf: &JournalConf,
        recovery: RecoveryGate,
    ) -> FsResult<Self> {
        let node_id = conf.node_id()?;
        let metrics = Master::get_metrics()?;
        let (sender, receiver) = BlockingChannel::new(conf.writer_channel_size).split();

        let receiver = if !testing {
            // Start the send log thread.
            let task = SenderTask::new(client, conf, 0, recovery.clone())?;
            task.spawn(receiver)?;
            None
        } else {
            Some(Mutex::new(receiver))
        };

        Ok(Self {
            enable: conf.enable,
            require_raft_commit: !testing,
            node_id,
            recovery,
            read_barrier: Arc::new(JournalReadBarrier::new()),
            sender,
            metrics,
            receiver,
            snapshot_in_flight: AtomicBool::new(false),
            leader_create_snapshot: conf.leader_creates_snapshot(),
            snapshot_entries: conf.snapshot_entries,
            snapshot_min_interval_ms: conf.snapshot_min_interval.as_millis(),
            last_snapshot_ms: AtomicCounter::new(0),
            journal_unbounded: conf.writer_channel_size == 0,
            entries_since_snapshot: AtomicCounter::new(0),
            #[cfg(test)]
            wait_unwind_hook: std::sync::Mutex::new(None),
        })
    }

    fn send_inner(&self, entry: JournalEntry) -> FsResult<()> {
        if self.recovery.rejects_writes() {
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }
        debug!("send_entry {:?}", entry);
        self.sender.send(QueuedJournalEntry {
            entry,
            completion: None,
        })?;
        self.metrics.journal_queue_len.inc();
        Ok(())
    }

    /// Enqueues the entry and waits for its Raft commit. Inside a
    /// `JournalCommitScope` the waiter is parked and drained together when the
    /// scope waits; journal disabled treats metadata as local-only committed.
    fn enqueue_and_wait(&self, entry: JournalEntry) -> FsResult<()> {
        if !self.enable {
            return Ok(());
        }
        if !self.require_raft_commit {
            // Testing writer: entries stay in the retained receiver for direct
            // assertions and no commit notification is ever produced.
            let result = self.send_inner(entry);
            if result.is_err() {
                self.fail_metadata_commit();
            }
            return result;
        }
        if self.recovery.rejects_writes() {
            self.fail_metadata_commit();
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }
        debug!("send_entry_wait {:?}", entry);
        let scoped = self.has_commit_scope();
        let (tx, rx) = mpsc::sync_channel(1);
        if let Err(error) = self.sender.send(QueuedJournalEntry {
            entry,
            completion: Some(tx),
        }) {
            self.fail_metadata_commit();
            return Err(error.into());
        }
        self.metrics.journal_queue_len.inc();
        if scoped {
            JOURNAL_COMMIT_SCOPES.with(|scopes| {
                scopes
                    .borrow_mut()
                    .iter_mut()
                    .rev()
                    .find(|scope| std::ptr::eq(scope.writer, self))
                    .expect("scope vanished while enqueueing")
                    .waiters
                    .push(rx);
            });
        } else {
            if let Err(error) = JournalCommitScope::wait_for_commit(rx) {
                self.fail_metadata_commit();
                return Err(error);
            }
        }
        Ok(())
    }

    /// Opens a commit scope so every journal entry enqueued inside it shares
    /// one Raft-commit wait. The caller must invoke `wait` after its metadata
    /// work completes.
    /// Test-only instance-isolated hook: arms a channel receiver so the
    /// next `JournalCommitScope::wait` on this writer — entered with at
    /// least one real pending waiter — blocks until the test driver sends,
    /// then panics to exercise the real wait unwind path. The receiver is
    /// consumed once (take), so the hook disarms itself after firing.
    #[cfg(test)]
    pub fn arm_wait_unwind_hook(
        &self,
        reached: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self.wait_unwind_hook.lock().unwrap() = Some((reached, release));
    }

    pub fn begin_commit_scope(&self) -> JournalCommitScope<'_> {
        let id = if self.enable && self.require_raft_commit {
            let id = NEXT_COMMIT_SCOPE_ID.with(|next| {
                let id = next.get();
                next.set(id.checked_add(1).expect("commit scope ID overflow"));
                id
            });
            JOURNAL_COMMIT_SCOPES.with(|scopes| {
                scopes.borrow_mut().push(CommitScopeEntry {
                    writer: self,
                    id,
                    waiters: Vec::new(),
                })
            });
            Some(id)
        } else {
            None
        };
        JournalCommitScope {
            id,
            writer: Some(self),
            _thread_bound: std::marker::PhantomData,
        }
    }

    fn has_commit_scope(&self) -> bool {
        JOURNAL_COMMIT_SCOPES.with(|scopes| {
            scopes
                .borrow()
                .iter()
                .rev()
                .any(|scope| std::ptr::eq(scope.writer, self))
        })
    }

    /// Read-barrier API for the catch-up coordinator (see JournalLoader) and
    /// the namespace write path. Defaults keep single-node/testing current.
    pub fn begin_metadata_catch_up(&self, applied_index: u64, required_index: u64) {
        self.read_barrier
            .begin_catch_up(applied_index, required_index);
    }

    pub fn require_metadata_catch_up(&self, required_index: u64) {
        self.read_barrier.require_catch_up(required_index);
    }

    pub fn fence_metadata(&self) -> u64 {
        self.read_barrier.fence()
    }

    pub(crate) fn fence_metadata_with(&self, publish: impl FnOnce()) -> u64 {
        self.read_barrier.fence_with(publish)
    }

    pub(crate) fn is_metadata_role_epoch(&self, epoch: u64) -> bool {
        self.read_barrier.is_role_epoch(epoch)
    }

    pub(crate) fn unfence_metadata_if_epoch(&self, epoch: u64, publish: impl FnOnce()) -> bool {
        self.read_barrier.unfence_if_epoch(epoch, publish)
    }

    pub fn unfence_metadata(&self) {
        self.read_barrier.unfence();
    }

    pub fn advance_metadata_applied(&self, applied_index: u64) {
        self.read_barrier.advance_applied(applied_index);
    }

    pub fn advance_metadata_catch_up(&self, applied_index: u64) {
        self.read_barrier.advance_catch_up(applied_index);
    }

    pub(crate) fn fail_metadata_commit(&self) {
        self.recovery
            .record_fatal_source("metadata journal commit failed after mutation");
        self.fail_metadata("metadata journal commit failed after mutation");
    }

    pub(crate) fn failure_seq(&self) -> u64 {
        self.recovery.failure_seq()
    }

    pub(crate) fn fail_metadata_publication(&self) {
        self.recovery
            .record_fatal_source("metadata publication failed after durable commit");
        self.fail_metadata("metadata publication failed after durable commit");
    }

    pub(crate) fn fail_metadata_restore(&self) {
        self.recovery
            .record_fatal_source("metadata restore failed after store replacement began");
        self.fail_metadata("metadata restore failed after store replacement began");
    }

    /// Terminal fencing for the namespace holder. The reason reflects the
    /// recorded fatal source (commit/publication/restore failure) when one
    /// exists; an unwind with no recorded fatal source gets a neutral
    /// terminal reason. Ordinary UFS notes are never misattributed here.
    pub(crate) fn fail_metadata_holder_terminal(&self) {
        let reason = self
            .recovery
            .fatal_source()
            .unwrap_or_else(|| "namespace holder terminal (unknown source)".to_string());
        self.fail_metadata(&reason);
    }

    fn fail_metadata(&self, reason: &str) {
        self.fence_metadata();
        self.recovery.enter_safe_mode(reason);
        self.recovery.mark_writer_failed();
        self.recovery.record_failure();
    }

    /// Whether the real journal queue is unbounded (cap == 0) or the writer
    /// has the journal disabled: the C2 recursive path may hold subtree
    /// locks across enqueue only in these cases (design §8.3). The fact is
    /// captured at construction from the actual channel mode, never from a
    /// forked conf or a test identity.
    pub(crate) fn queue_never_blocks(&self) -> bool {
        !self.enable || self.journal_unbounded
    }

    pub fn ensure_metadata_current(&self) -> FsResult<()> {
        if self.recovery.rejects_writes() {
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }
        self.read_barrier.ensure_current()
    }

    pub(crate) fn metadata_epoch(&self) -> FsResult<u64> {
        if self.recovery.rejects_writes() {
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }
        self.read_barrier.current_epoch()
    }

    pub(crate) fn ensure_metadata_epoch(&self, epoch: u64) -> FsResult<()> {
        if self.recovery.rejects_writes() {
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }
        self.read_barrier.ensure_epoch(epoch)
    }

    pub fn is_metadata_current(&self) -> bool {
        !self.recovery.rejects_writes() && self.read_barrier.is_current()
    }

    fn send(&self, _fs_dir: &FsDir, entry: JournalEntry) -> FsResult<()> {
        if self.enable {
            if self.has_commit_scope() {
                self.enqueue_and_wait(entry)?;
            } else {
                if let Err(error) = self.send_inner(entry) {
                    self.fail_metadata_commit();
                    return Err(error);
                }
            }
            self.record_snapshot_entry();
        }
        Ok(())
    }

    /// Sends the entry and waits for Raft to commit the batch carrying it.
    /// Within a commit scope the wait is deferred to the scope. Local metadata
    /// may already be materialized before this wait; crash isolation is not
    /// provided by this method.
    pub(crate) fn send_and_wait(
        &self,
        _fs_dir: &FsDir,
        entry: impl FnOnce() -> JournalEntry,
    ) -> FsResult<()> {
        // Build the entry lazily: with the journal disabled (tests, safe-mode
        // fallback) the caller skips the path/name string clones entirely.
        if !self.enable {
            return Ok(());
        }
        let entry = entry();
        self.enqueue_and_wait(entry)?;
        self.record_snapshot_entry();
        Ok(())
    }

    fn record_snapshot_entry(&self) {
        if self.leader_create_snapshot && self.snapshot_entries != 0 {
            self.entries_since_snapshot.incr();
        }
    }

    pub(crate) fn periodic_snapshot_due(&self) -> bool {
        if !self.leader_create_snapshot
            || self.snapshot_entries == 0
            || self.entries_since_snapshot.get() < self.snapshot_entries
        {
            return false;
        }
        let last = self.last_snapshot_ms.get();
        last == 0 || LocalTime::mills().saturating_sub(last) >= self.snapshot_min_interval_ms
    }

    pub(crate) fn maybe_emit_snapshot(&self, fs_dir: &FsDir) -> FsResult<()> {
        if self.periodic_snapshot_due() {
            self.emit_snapshot(fs_dir, "periodic", self.entries_since_snapshot.get())?;
        }
        Ok(())
    }

    fn emit_snapshot(
        &self,
        fs_dir: &FsDir,
        source: &str,
        entries: u64,
    ) -> FsResult<LeaderSnapshotInfo> {
        let now = LocalTime::mills();
        let checkpoint_id = self.next_checkpoint_id(fs_dir);
        let spend = TimeSpent::new();

        let dir = fs_dir.create_checkpoint(checkpoint_id).map_err(|e| {
            FsError::common(format!(
                "leaderSnapshot[{}]: create_checkpoint failed: {}",
                source, e
            ))
        })?;

        let cost_ms = spend.used_ms();

        info!(
            "create leader snapshot, source {}, dir {}, entries {}, cost {} ms",
            source, dir, entries, cost_ms
        );

        let snapshot_entry = JournalEntry::Snapshot(SnapshotEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            node_id: self.node_id,
            dir: dir.clone(),
        });

        if let Err(send_error) = self.send_inner(snapshot_entry) {
            if let Err(cleanup_error) = FileUtils::delete_path(&dir, true) {
                warn!(
                    "failed to remove checkpoint {} after snapshot entry send failure: {}",
                    dir, cleanup_error
                );
            }
            return Err(send_error);
        }

        self.entries_since_snapshot.set(0);
        self.last_snapshot_ms.set(now);

        Ok(LeaderSnapshotInfo { dir, cost_ms })
    }

    fn next_checkpoint_id(&self, fs_dir: &FsDir) -> u64 {
        let mut id = LocalTime::mills();
        while FileUtils::exists(fs_dir.get_checkpoint_path(id)) {
            id += 1;
        }
        id
    }

    pub fn create_snapshot(&self, fs_dir: &FsDir, force: bool) -> FsResult<LeaderSnapshotInfo> {
        if !self.enable {
            return err_box!("leader snapshot requires journal.enable = true");
        }
        self.ensure_metadata_current()?;

        if !force && self.inside_snapshot_interval() {
            return err_box!(
                "the last leader snapshot was {} ms ago, snapshot_min_interval is {} ms; retry later or pass --force",
                self.last_snapshot_age_ms(),
                self.snapshot_min_interval_ms
            );
        }

        if !self.snapshot_in_flight.compare_and_set(false, true) {
            return err_box!("a leader snapshot is already in progress");
        }

        let info = self.emit_snapshot(fs_dir, "operator", self.entries_since_snapshot.get());
        self.snapshot_in_flight.set(false);
        info
    }

    fn inside_snapshot_interval(&self) -> bool {
        let last = self.last_snapshot_ms.get();
        last > 0 && self.last_snapshot_age_ms() < self.snapshot_min_interval_ms
    }

    fn last_snapshot_age_ms(&self) -> u64 {
        LocalTime::mills().saturating_sub(self.last_snapshot_ms.get())
    }

    pub fn log_mkdir(&self, fs_dir: &FsDir, path: impl AsRef<str>, dir: &InodeDir) -> FsResult<()> {
        let entry = MkdirEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            dir: dir.clone(),
        };
        self.send(fs_dir, JournalEntry::Mkdir(entry))
    }

    pub fn log_create_file(&self, fs_dir: &FsDir, inp: &InodePath) -> FsResult<()> {
        let entry = CreateFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(fs_dir, JournalEntry::CreateFile(entry))
    }

    pub fn log_reopen_file<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
    ) -> FsResult<()> {
        let entry = ReopenFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            file: file.clone(),
        };
        self.send(fs_dir, JournalEntry::ReopenFile(entry))
    }

    pub fn log_add_block<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
        commit_block: Vec<CommitBlock>,
    ) -> FsResult<()> {
        let entry = AddBlockEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            blocks: file.blocks.clone(),
            commit_block,
        };
        self.send(fs_dir, JournalEntry::AddBlock(entry))
    }

    pub fn log_complete_file<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
        commit_blocks: Vec<CommitBlock>,
    ) -> FsResult<()> {
        let entry = CompleteFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            file: file.clone(),
            commit_blocks,
        };
        self.send(fs_dir, JournalEntry::CompleteFile(entry))
    }

    pub fn log_overwrite_file(&self, fs_dir: &FsDir, inp: &InodePath) -> FsResult<()> {
        let entry = OverWriteFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(fs_dir, JournalEntry::OverWriteFile(entry))
    }

    pub fn log_rename<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src: P,
        dst: P,
        mtime: i64,
        flags: RenameFlags,
        exchange_pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<()> {
        let (src_inode_id, dst_inode_id) = exchange_pre_swap_ids.unwrap_or((0, 0));
        let entry = RenameEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            src: src.as_ref().to_string(),
            dst: dst.as_ref().to_string(),
            mtime,
            flags: flags.value(),
            src_inode_id,
            dst_inode_id,
        };
        self.send(fs_dir, JournalEntry::Rename(entry))
    }

    pub fn log_delete<P: AsRef<str>>(&self, fs_dir: &FsDir, path: P, mtime: i64) -> FsResult<()> {
        let entry = DeleteEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            mtime,
        };
        self.send(fs_dir, JournalEntry::Delete(entry))
    }

    /// Journals one bounded purge batch of `entry`. The batch names the paths it
    /// removed, so replay applies exactly the same slice the leader applied.
    pub fn log_trash_purge<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        entry: P,
        mtime: i64,
        paths: Vec<String>,
    ) -> FsResult<()> {
        if paths.is_empty() {
            return Ok(());
        }

        let entry = TrashPurgeEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            entry: entry.as_ref().to_string(),
            mtime,
            paths,
        };
        self.send(fs_dir, JournalEntry::TrashPurge(entry))
    }

    pub fn log_free<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<()> {
        let entry = FreeEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            mtime,
            recursive,
        };
        self.send(fs_dir, JournalEntry::Free(entry))
    }

    pub fn log_cache_invalidations(
        &self,
        fs_dir: &FsDir,
        inodes: Vec<crate::master::meta::inode::InodeView>,
    ) -> FsResult<()> {
        if inodes.is_empty() {
            return Ok(());
        }

        let entry = CacheInvalidationEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            inodes,
        };
        self.send(fs_dir, JournalEntry::CacheInvalidation(entry))
    }

    pub fn log_mount(&self, fs_dir: &FsDir, info: MountInfo) -> FsResult<()> {
        let entry = MountEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            info,
        };
        self.send(fs_dir, JournalEntry::Mount(entry))
    }

    pub fn log_unmount(&self, fs_dir: &FsDir, id: u32) -> FsResult<()> {
        let entry = UnMountEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            id,
        };
        self.send(fs_dir, JournalEntry::UnMount(entry))
    }

    pub fn log_set_attr(&self, fs_dir: &FsDir, inp: &InodePath, opts: SetAttrOpts) -> FsResult<()> {
        let entry = SetAttrEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            opts,
        };
        self.send(fs_dir, JournalEntry::SetAttr(entry))
    }

    pub fn log_symlink<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        link: P,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<()> {
        let entry = SymlinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            link: link.as_ref().to_string(),
            new_inode,
            force,
        };
        self.send(fs_dir, JournalEntry::Symlink(entry))
    }

    pub fn log_link<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src_path: P,
        dst_path: P,
        mtime: i64,
    ) -> FsResult<()> {
        let entry = LinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            mtime,
            src_path: src_path.as_ref().to_string(),
            dst_path: dst_path.as_ref().to_string(),
        };
        self.send(fs_dir, JournalEntry::Link(entry))
    }

    pub fn log_ufs_applied(&self, op_id: u64, term: u64, index: u64) -> FsResult<()> {
        if !self.enable {
            return Ok(());
        }
        if self.recovery.rejects_writes() {
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }

        let entry = UfsAppliedEntry {
            op_id,
            rpc_id: 0,
            term,
            index,
        };
        self.metrics.journal_queue_len.inc();
        self.sender.send(QueuedJournalEntry {
            entry: JournalEntry::UfsApplied(entry),
            completion: None,
        })?;

        Ok(())
    }

    pub fn log_ufs_deferred(&self, entry: UfsDeferredEntry) -> FsResult<()> {
        if !self.enable {
            return Ok(());
        }
        if self.recovery.rejects_writes() {
            return Err(FsError::journal_unavailable(
                "journal writer is not accepting writes",
            ));
        }
        self.metrics.journal_queue_len.inc();
        self.sender.send(QueuedJournalEntry {
            entry: JournalEntry::UfsDeferred(entry),
            completion: None,
        })?;
        Ok(())
    }

    pub fn log_set_locks(&self, fs_dir: &FsDir, ino: i64, locks: Vec<FileLock>) -> FsResult<()> {
        let entry = SetLocksEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            ino,
            locks,
        };
        self.send(fs_dir, JournalEntry::SetLocks(entry))
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        let mut entries = vec![];
        let Some(receiver) = self.receiver.as_ref() else {
            return entries;
        };
        let receiver = match receiver.lock() {
            Ok(receiver) => receiver,
            Err(e) => {
                log::error!("failed to take journal entries: {}", e);
                return entries;
            }
        };
        while let Ok(v) = receiver.try_recv() {
            entries.push(v.entry);
        }
        entries
    }
}

#[cfg(test)]
#[path = "tests/journal_writer_test.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/commit_scope_test.rs"]
mod commit_scope_test;

#[cfg(test)]
#[path = "tests/exclusive_confirmation_test.rs"]
mod exclusive_confirmation_test;

#[cfg(test)]
#[path = "tests/fatal_source_test.rs"]
mod fatal_source_test;
