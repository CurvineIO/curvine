//! In-memory [`KvBackend`] implementation.
//!
//! This backend is for unit tests and MiniCluster runs that must not depend on
//! an external service. It implements the same optimistic-concurrency contract
//! as the future FoundationDB backend so the shared contract tests exercise
//! identical semantics on both.
//!
//! This is not a complete MVCC implementation and is not intended as a
//! production storage backend. It exists primarily for unit tests, transaction
//! semantics validation, MiniCluster runs, and behavioral/performance
//! comparison with the FoundationDB backend.
//!
//! ## Concurrency model
//!
//! A single global monotonically increasing `seq` acts as the version clock.
//! Every committed key carries the `seq` of the commit that last wrote it. A
//! transaction captures the current `seq` at `begin` as its snapshot version
//! and records every key it reads. At `commit`, if any read key has a version
//! greater than the snapshot version, a concurrent transaction touched it and
//! the commit returns [`KvError::Conflict`]. This is what makes CAS and
//! `run_txn` correct under contention.
//!
//! ## Fault injection
//!
//! A [`FaultInjector`] can be attached to force deterministic one-shot errors
//! on specific operations, or a probabilistic commit conflict, so tests can
//! cover the retryable/terminal error paths without racing real threads.

use crate::kv::backend::{KvBackend, KvTransaction};
use crate::kv::error::{KvError, KvResult};
use crate::kv::metrics;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const BACKEND_NAME: &str = "memory";

#[derive(Clone)]
struct Entry {
    /// `Some` = live value, `None` = tombstone. Deletes keep a version-bumped
    /// tombstone (never remove the key) so a concurrent delete of a read key
    /// is detected as a conflict, matching FDB.
    value: Option<Vec<u8>>,
    version: u64,
}

#[derive(Default)]
struct Store {
    /// Behind an `Arc` so `begin` captures a cheap immutable read snapshot;
    /// `commit` copy-on-writes via `Arc::make_mut`.
    data: Arc<BTreeMap<Vec<u8>, Entry>>,
    /// Global version clock; the seq of the most recent commit.
    seq: u64,
}

/// Deterministic fault injection for the memory backend.
///
/// Faults are opt-in and shared (cloneable handle). Use [`FaultInjector::fail_next`]
/// to queue a one-shot error for a named operation (see [`crate::kv::metrics::op`]),
/// or [`FaultInjector::set_commit_conflict_prob`] to make a fraction of commits
/// return [`KvError::Conflict`].
#[derive(Clone, Default)]
pub struct FaultInjector {
    inner: Arc<Mutex<FaultState>>,
}

#[derive(Default)]
struct FaultState {
    /// Per-op FIFO queue of one-shot errors.
    queued: HashMap<&'static str, VecDeque<KvError>>,
    /// Probability in `[0.0, 1.0]` that any given commit returns a conflict.
    commit_conflict_prob: f64,
    /// Number of upcoming commits that must APPLY and THEN return
    /// [`KvError::MaybeCommitted`] (FDB `commit_unknown_result`).
    commit_apply_then_unknown: u32,
}

impl FaultInjector {
    /// Creates an injector with no active faults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a single error to be returned the next time operation `op`
    /// executes. Multiple queued errors for the same op fire in FIFO order.
    pub fn fail_next(&self, op: &'static str, error: KvError) {
        self.inner
            .lock()
            .unwrap()
            .queued
            .entry(op)
            .or_default()
            .push_back(error);
    }

    /// Sets the probability (`0.0..=1.0`) that a commit returns
    /// [`KvError::Conflict`]. Useful for stress-testing retry logic.
    pub fn set_commit_conflict_prob(&self, prob: f64) {
        self.inner.lock().unwrap().commit_conflict_prob = prob.clamp(0.0, 1.0);
    }

    /// Arms the next `count` commits to APPLY their writes and then return
    /// [`KvError::MaybeCommitted`] (FDB `commit_unknown_result`). Unlike
    /// `fail_next(COMMIT, MaybeCommitted)`, which fails before applying, this
    /// exercises the "write landed but result unknown" path.
    pub fn apply_then_unknown_next(&self, count: u32) {
        self.inner.lock().unwrap().commit_apply_then_unknown = count;
    }

    /// Clears all queued faults and resets the conflict probability.
    pub fn reset(&self) {
        let mut state = self.inner.lock().unwrap();
        state.queued.clear();
        state.commit_conflict_prob = 0.0;
        state.commit_apply_then_unknown = 0;
    }

    fn take(&self, op: &'static str) -> Option<KvError> {
        let mut state = self.inner.lock().unwrap();
        state.queued.get_mut(op).and_then(|q| q.pop_front())
    }

    fn maybe_commit_conflict(&self) -> bool {
        let prob = self.inner.lock().unwrap().commit_conflict_prob;
        prob > 0.0 && fastrand::f64() < prob
    }

    /// Consumes one armed apply-then-unknown token.
    fn take_apply_then_unknown(&self) -> bool {
        let mut state = self.inner.lock().unwrap();
        if state.commit_apply_then_unknown > 0 {
            state.commit_apply_then_unknown -= 1;
            true
        } else {
            false
        }
    }
}

/// In-memory KV backend. Cheap to `clone`; all clones share the same store.
#[derive(Clone)]
pub struct MemoryBackend {
    store: Arc<Mutex<Store>>,
    faults: FaultInjector,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    /// Creates an empty backend with no fault injection.
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(Store::default())),
            faults: FaultInjector::new(),
        }
    }

    /// Creates a backend that shares the given [`FaultInjector`], so a test can
    /// hold the handle and arm faults after construction.
    pub fn with_faults(faults: FaultInjector) -> Self {
        Self {
            store: Arc::new(Mutex::new(Store::default())),
            faults,
        }
    }

    /// Returns a handle to this backend's fault injector.
    pub fn faults(&self) -> FaultInjector {
        self.faults.clone()
    }

    /// Number of live keys (test/debug helper); tombstones are not counted.
    pub fn len(&self) -> usize {
        self.store
            .lock()
            .unwrap()
            .data
            .values()
            .filter(|e| e.value.is_some())
            .count()
    }

    /// Returns `true` when the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl KvBackend for MemoryBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    async fn begin(&self) -> KvResult<Box<dyn KvTransaction>> {
        let start = Instant::now();
        if let Some(error) = self.faults.take(metrics::op::BEGIN) {
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::BEGIN,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }
        let (read_version, snapshot) = {
            let store = self.store.lock().unwrap();
            (store.seq, store.data.clone())
        };
        metrics::metrics().observe(BACKEND_NAME, metrics::op::BEGIN, start, &Ok(()));
        metrics::metrics().txn_in_flight.inc();
        Ok(Box::new(MemTxn {
            store: self.store.clone(),
            faults: self.faults.clone(),
            read_version,
            snapshot,
            reads: HashMap::new(),
            writes: BTreeMap::new(),
            finished: false,
        }))
    }
}

/// Buffered write: `Some(value)` = put, `None` = delete.
type PendingWrite = Option<Vec<u8>>;

struct MemTxn {
    store: Arc<Mutex<Store>>,
    faults: FaultInjector,
    /// Snapshot version captured at begin.
    read_version: u64,
    /// Immutable committed-store snapshot captured at begin; all reads observe
    /// this, so concurrent writes after begin are invisible (snapshot isolation).
    snapshot: Arc<BTreeMap<Vec<u8>, Entry>>,
    /// Keys read (point) whose version must not have advanced by commit.
    reads: HashMap<Vec<u8>, ()>,
    /// Buffered writes applied atomically at commit.
    writes: BTreeMap<Vec<u8>, PendingWrite>,
    finished: bool,
}

impl MemTxn {
    /// Reads buffered writes first, then the begin-time snapshot. A tombstone
    /// reads back as absent.
    fn read_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(pending) = self.writes.get(key) {
            return pending.clone();
        }
        self.snapshot.get(key).and_then(|e| e.value.clone())
    }
}

#[async_trait]
impl KvTransaction for MemTxn {
    async fn get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let start = Instant::now();
        if let Some(error) = self.faults.take(metrics::op::GET) {
            metrics::metrics().observe(BACKEND_NAME, metrics::op::GET, start, &Err(error.clone()));
            return Err(error);
        }
        self.reads.insert(key.to_vec(), ());
        let value = self.read_key(key);
        metrics::metrics().observe(BACKEND_NAME, metrics::op::GET, start, &Ok(()));
        Ok(value)
    }

    async fn snapshot_get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let start = Instant::now();
        if let Some(error) = self.faults.take(metrics::op::SNAPSHOT_GET) {
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::SNAPSHOT_GET,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }
        // Snapshot read: same snapshot as `get` but deliberately NOT recorded
        // in `self.reads`, so a concurrent change to this key won't conflict.
        let value = self.read_key(key);
        metrics::metrics().observe(BACKEND_NAME, metrics::op::SNAPSHOT_GET, start, &Ok(()));
        Ok(value)
    }

    async fn multi_get(&mut self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
        let start = Instant::now();
        if let Some(error) = self.faults.take(metrics::op::MULTI_GET) {
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::MULTI_GET,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            self.reads.insert(key.clone(), ());
            out.push(self.read_key(key));
        }
        metrics::metrics().observe(BACKEND_NAME, metrics::op::MULTI_GET, start, &Ok(()));
        Ok(out)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    fn add_read_conflict(&mut self, key: &[u8]) {
        self.reads.insert(key.to_vec(), ());
    }

    async fn commit(&mut self) -> KvResult<()> {
        let start = Instant::now();
        self.finished = true;
        metrics::metrics().txn_in_flight.dec();

        if let Some(error) = self.faults.take(metrics::op::COMMIT) {
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::COMMIT,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }
        if self.faults.maybe_commit_conflict() {
            let error = KvError::Conflict;
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::COMMIT,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }

        let mut store = self.store.lock().unwrap();

        // Conflict check: any read key advanced past our snapshot? Tombstones
        // keep a bumped version, so a concurrent delete is caught like an
        // overwrite.
        for key in self.reads.keys() {
            if let Some(entry) = store.data.get(key) {
                if entry.version > self.read_version {
                    drop(store);
                    let error = KvError::Conflict;
                    metrics::metrics().observe(
                        BACKEND_NAME,
                        metrics::op::COMMIT,
                        start,
                        &Err(error.clone()),
                    );
                    return Err(error);
                }
            }
        }
        // No conflict: bump the version clock and apply the write set. Deletes
        // write a tombstone rather than removing the key.
        if !self.writes.is_empty() {
            store.seq += 1;
            let version = store.seq;
            let writes = std::mem::take(&mut self.writes);
            let data = Arc::make_mut(&mut store.data);
            for (key, pending) in writes {
                data.insert(
                    key,
                    Entry {
                        value: pending,
                        version,
                    },
                );
            }
        }
        drop(store);

        // Apply-then-unknown fault: writes have landed, but report
        // MaybeCommitted (FDB commit_unknown_result). run_txn must NOT retry.
        if self.faults.take_apply_then_unknown() {
            let error = KvError::MaybeCommitted;
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::COMMIT,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }

        metrics::metrics().observe(BACKEND_NAME, metrics::op::COMMIT, start, &Ok(()));
        Ok(())
    }

    fn rollback(&mut self) {
        if !self.finished {
            self.finished = true;
            metrics::metrics().txn_in_flight.dec();
        }
        self.writes.clear();
        self.reads.clear();
    }
}

impl Drop for MemTxn {
    fn drop(&mut self) {
        // A transaction dropped without commit/rollback still releases its
        // in-flight slot so the gauge stays accurate.
        if !self.finished {
            metrics::metrics().txn_in_flight.dec();
        }
    }
}
