//! FoundationDB [`KvBackend`] implementation.
//!
//! This backend maps the generic KV abstraction onto the FoundationDB C
//! client (via the `foundationdb` crate).
//!
//! ## Network lifecycle
//!
//! The FDB C client requires a process-global network event loop that may be
//! started exactly once. [`FdbBackend::open`] boots it on first use through a
//! [`once_cell::sync::OnceCell`] and INTENTIONALLY LEAKS the
//! [`NetworkAutoStop`] handle: dropping it would call `fdb_stop_network`,
//! which blocks on the network thread. Leaking it means a hung or unreachable
//! FDB cluster can never wedge process shutdown on the network stop — the OS
//! reclaims the thread on exit. This satisfies the PR requirement that "FDB
//! unavailable or shut down must not block process exit".
//!
//! ## Fail-fast, not hang
//!
//! Every transaction is created with a `Timeout` (see
//! [`FdbBackend::txn_timeout_ms`]) so an unreachable cluster surfaces a
//! `Timeout`/`Unavailable` [`KvError`] within a bounded window instead of
//! blocking forever.
//!
//! ## Concurrency contract
//!
//! FDB natively provides serializable snapshot isolation, which is exactly the
//! contract [`KvTransaction`] documents: `get`/`multi_get` add keys to the
//! read-conflict set, `snapshot_get` reads the same snapshot without
//! conflict tracking, blind writes don't conflict, and a concurrent delete of
//! a read key conflicts like an overwrite. The same backend-agnostic contract
//! tests that pin the memory backend run against this one unchanged.

use crate::kv::backend::{KvBackend, KvTransaction};
use crate::kv::error::{KvError, KvResult};
use crate::kv::metrics;
use async_trait::async_trait;
use foundationdb::options::{ConflictRangeType, TransactionOption};
use foundationdb::{api::NetworkAutoStop, Database, FdbError, Transaction};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::Instant;

const BACKEND_NAME: &str = "fdb";

/// Process-global FDB network guard. Leaked on purpose (see module docs); the
/// `OnceCell` only guarantees the network is booted exactly once.
static FDB_NETWORK: OnceCell<()> = OnceCell::new();

/// Boots the FDB client network loop once per process. The returned handle is
/// leaked so a stuck cluster can never block `fdb_stop_network` at shutdown.
fn ensure_network_started() {
    FDB_NETWORK.get_or_init(|| {
        // SAFETY: called exactly once (guarded by OnceCell). We deliberately
        // leak the guard instead of storing it: dropping it would stop the
        // network and could block on a hung cluster during exit.
        let network: NetworkAutoStop = unsafe { foundationdb::boot() };
        std::mem::forget(network);
    });
}

/// Maps a native [`FdbError`] to the abstraction's [`KvError`].
///
/// The classification, not the message, is what callers depend on:
/// - `commit_unknown_result` → [`KvError::MaybeCommitted`] (NOT retryable — the
///   write may already have applied).
/// - `not_committed` (1020) → [`KvError::Conflict`], the ONLY code mapped to
///   Conflict so the conflict metric counts real read/write-set clashes.
/// - `transaction_timed_out` (1031) → [`KvError::Timeout`].
/// - any other retryable code (connection loss, coordinator change, stale read
///   version, …) → [`KvError::Unavailable`]: same retry semantics as Conflict
///   but kept distinct so availability problems don't pollute conflict metrics.
/// - everything else → terminal [`KvError::Backend`].
fn map_fdb_error(err: FdbError) -> KvError {
    // FDB error codes from flow/error_definitions.h:
    // https://github.com/apple/foundationdb/blob/main/flow/include/flow/error_definitions.h
    //   1020 not_committed          — optimistic-concurrency conflict
    //   1031 transaction_timed_out  — deadline exceeded
    const NOT_COMMITTED: i32 = 1020;
    const TRANSACTION_TIMED_OUT: i32 = 1031;

    // Timeout first, for semantic precision. 1031 is itself retryable, so this
    // branch is not needed for correctness (it would otherwise fall through to
    // the retryable arm), but surfacing Timeout distinguishes "deadline hit"
    // from conflict/unavailability in errors and metrics.
    if err.code() == TRANSACTION_TIMED_OUT {
        return KvError::Timeout;
    }
    // maybe-committed MUST be classified before the retryable checks below: it
    // is a SUBSET of retryable, but must NOT be auto-retried (the write may
    // already have applied — retrying would double-apply).
    if err.is_maybe_committed() {
        return KvError::MaybeCommitted;
    }
    // Only the genuine optimistic-concurrency conflict (not_committed) maps to
    // Conflict, so `mds_kv_txn_conflicts_total` counts real read/write-set
    // clashes and nothing else. Network/cluster availability errors are ALSO
    // retryable, but lumping them into Conflict would pollute that metric and
    // mislead operators into diagnosing contention when the cluster is simply
    // unreachable.
    if err.code() == NOT_COMMITTED {
        return KvError::Conflict;
    }
    // Every other retryable error — connection_failed (1026), coordinators
    // changed (1027), transaction_too_old (1007), future_version (1009),
    // cluster_version_changed (1039), etc. — is a backend availability /
    // transient condition, not a concurrency conflict. Same retry semantics as
    // Conflict, but classified as Unavailable so metrics stay meaningful.
    if err.is_retryable() {
        return KvError::Unavailable(format!("fdb error_code {}: {}", err.code(), err.message()));
    }
    KvError::Backend(format!("fdb error_code {}: {}", err.code(), err.message()))
}

/// FoundationDB-backed KV store. Cheap to `clone`; all clones share one
/// [`Database`] handle.
#[derive(Clone)]
pub struct FdbBackend {
    db: Arc<Database>,
    txn_timeout_ms: i32,
}

impl FdbBackend {
    /// Opens the backend from a FoundationDB cluster file path (the same file
    /// `fdbcli -C <path>` accepts). Boots the process-global network on first
    /// use.
    pub fn open(cluster_file: &str, txn_timeout_ms: i32) -> KvResult<Self> {
        ensure_network_started();
        let cluster_file = cluster_file.trim();
        if cluster_file.is_empty() {
            return Err(KvError::Backend("fdb cluster file path is empty".into()));
        }
        let db = Database::from_path(cluster_file).map_err(map_fdb_error)?;
        Ok(Self {
            db: Arc::new(db),
            txn_timeout_ms,
        })
    }
}

#[async_trait]
impl KvBackend for FdbBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    async fn begin(&self) -> KvResult<Box<dyn KvTransaction>> {
        let start = Instant::now();
        let result: KvResult<Transaction> = (|| {
            let trx = self.db.create_trx().map_err(map_fdb_error)?;
            // Bound how long any operation on this txn waits on a stuck cluster.
            trx.set_option(TransactionOption::Timeout(self.txn_timeout_ms))
                .map_err(map_fdb_error)?;
            Ok(trx)
        })();
        let trx = match result {
            Ok(trx) => trx,
            Err(error) => {
                metrics::metrics().observe(
                    BACKEND_NAME,
                    metrics::op::BEGIN,
                    start,
                    &Err(error.clone()),
                );
                return Err(error);
            }
        };
        // Eagerly resolve the read version so the snapshot is pinned at begin,
        // not lazily at the first read. Without this, FDB picks the read
        // version on the first get, and a write committed between begin and
        // that get would leak into the snapshot — violating the trait's
        // "reads observe a snapshot taken when the transaction began" contract
        // (and the memory backend's begin-snapshot behavior).
        if let Err(err) = trx.get_read_version().await {
            let error = map_fdb_error(err);
            metrics::metrics().observe(
                BACKEND_NAME,
                metrics::op::BEGIN,
                start,
                &Err(error.clone()),
            );
            return Err(error);
        }
        metrics::metrics().observe(BACKEND_NAME, metrics::op::BEGIN, start, &Ok(()));
        metrics::metrics().txn_in_flight.inc();
        Ok(Box::new(FdbTxn {
            trx: Some(trx),
            finished: false,
        }))
    }
}

/// The end-key of the single-key conflict range `[key, key\0)`; adding this
/// range to the read-conflict set makes an explicit read-conflict cover exactly
/// `key`.
fn key_successor(key: &[u8]) -> Vec<u8> {
    let mut end = Vec::with_capacity(key.len() + 1);
    end.extend_from_slice(key);
    end.push(0x00);
    end
}

struct FdbTxn {
    /// `Some` until commit/rollback/drop; taken out to move into `commit`,
    /// which consumes the `Transaction`.
    trx: Option<Transaction>,
    finished: bool,
}

impl FdbTxn {
    fn trx(&self) -> KvResult<&Transaction> {
        self.trx
            .as_ref()
            .ok_or_else(|| KvError::Backend("transaction already finished".into()))
    }
}

#[async_trait]
impl KvTransaction for FdbTxn {
    async fn get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let start = Instant::now();
        // snapshot = false ⇒ the read is added to the read-conflict set.
        let result = self
            .trx()?
            .get(key, false)
            .await
            .map_err(map_fdb_error)
            .map(|opt| opt.map(|slice| slice.to_vec()));
        observe_read(metrics::op::GET, start, &result);
        result
    }

    async fn snapshot_get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let start = Instant::now();
        // snapshot = true ⇒ same snapshot as `get`, but NOT added to the
        // read-conflict set, so a concurrent change won't conflict at commit.
        let result = self
            .trx()?
            .get(key, true)
            .await
            .map_err(map_fdb_error)
            .map(|opt| opt.map(|slice| slice.to_vec()));
        observe_read(metrics::op::SNAPSHOT_GET, start, &result);
        result
    }

    async fn multi_get(&mut self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
        let start = Instant::now();
        let trx = self.trx()?;
        let mut out = Vec::with_capacity(keys.len());
        let mut error: Option<KvError> = None;
        // FDB futures pipeline: issue all reads, then await. Each `get` tracks
        // the key in the read-conflict set, matching the trait contract.
        let futures: Vec<_> = keys.iter().map(|k| trx.get(k, false)).collect();
        for fut in futures {
            match fut.await {
                Ok(slice) => out.push(slice.map(|s| s.to_vec())),
                Err(err) => {
                    error = Some(map_fdb_error(err));
                    break;
                }
            }
        }
        let result = match error {
            Some(err) => Err(err),
            None => Ok(out),
        };
        observe_read(metrics::op::MULTI_GET, start, &result);
        result
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        if let Some(trx) = self.trx.as_ref() {
            metrics::metrics().observe_kv_size(BACKEND_NAME, key.len(), value.len());
            trx.set(key, value);
        }
    }

    fn delete(&mut self, key: &[u8]) {
        if let Some(trx) = self.trx.as_ref() {
            trx.clear(key);
        }
    }

    fn add_read_conflict(&mut self, key: &[u8]) {
        if let Some(trx) = self.trx.as_ref() {
            let end = key_successor(key);
            // Best-effort: a failure here would also fail commit; ignore so the
            // signature matches the trait (no Result).
            let _ = trx.add_conflict_range(key, &end, ConflictRangeType::Read);
        }
    }

    async fn commit(&mut self) -> KvResult<()> {
        let start = Instant::now();
        self.finished = true;
        // Take the transaction BEFORE touching the in-flight gauge: a repeat
        // commit (trx already None) is a no-op error and must not decrement the
        // gauge, or it would drive txn_in_flight negative.
        let trx = match self.trx.take() {
            Some(trx) => trx,
            None => {
                let error = KvError::Backend("commit on finished transaction".into());
                metrics::metrics().observe(
                    BACKEND_NAME,
                    metrics::op::COMMIT,
                    start,
                    &Err(error.clone()),
                );
                return Err(error);
            }
        };
        metrics::metrics().txn_in_flight.dec();
        let result = match trx.commit().await {
            Ok(_) => Ok(()),
            // `commit` consumes the txn; on error the `TransactionCommitError`
            // derefs to the underlying `FdbError`, which carries the code.
            Err(commit_err) => Err(map_fdb_error(commit_err.into())),
        };
        metrics::metrics().observe(
            BACKEND_NAME,
            metrics::op::COMMIT,
            start,
            &result.as_ref().map(|_| ()).map_err(|e| e.clone()),
        );
        result
    }

    fn rollback(&mut self) {
        if !self.finished {
            self.finished = true;
            metrics::metrics().txn_in_flight.dec();
        }
        // Dropping the Transaction cancels/destroys it.
        self.trx = None;
    }
}

impl Drop for FdbTxn {
    fn drop(&mut self) {
        // A transaction dropped without commit/rollback still releases its
        // in-flight slot so the gauge stays accurate.
        if !self.finished {
            metrics::metrics().txn_in_flight.dec();
        }
    }
}

/// Records the outcome/latency of a read op; reads don't touch the KV-size
/// histogram (that tracks writes).
fn observe_read<T>(op: &'static str, start: Instant, result: &KvResult<T>) {
    metrics::metrics().observe(
        BACKEND_NAME,
        op,
        start,
        &result.as_ref().map(|_| ()).map_err(|e| e.clone()),
    );
}
