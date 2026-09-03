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
//! `DeadlineExceeded`/`TransientUnavailable` [`KvError`] within a bounded window
//! instead of blocking forever.
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

/// FoundationDB error codes the mapper branches on explicitly, from
/// flow/error_definitions.h:
/// https://github.com/apple/foundationdb/blob/main/flow/include/flow/error_definitions.h
///
/// Only codes whose classification the mapper depends on live here; every other
/// code is handled through FDB's `is_retryable` / `is_maybe_committed` /
/// `is_retryable_not_committed` predicates rather than by matching a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
enum FdbErrorCode {
    /// 1020 `not_committed`: transaction rejected due to a read/write-set
    /// conflict. Provably NOT committed — the only code mapped to
    /// [`KvError::Conflict`] so `mds_kv_txn_conflicts_total` counts real clashes.
    NotCommitted = 1020,
    /// 1031 `transaction_timed_out`: per-transaction deadline exceeded. FDB
    /// treats it as terminal (neither predicate-retryable nor maybe-committed).
    /// Neither path retries it (the deadline is the caller's wait budget), but
    /// the mapper keeps the write status distinct: [`KvError::DeadlineExceeded`]
    /// on a read (provably wrote nothing) vs [`KvError::MaybeCommitted`] on a
    /// commit (may already have applied).
    TransactionTimedOut = 1031,
}

impl FdbErrorCode {
    /// The raw FDB error code, for comparison against [`FdbError::code`].
    const fn code(self) -> i32 {
        self as i32
    }
}

/// Maps a native [`FdbError`] from a READ/begin operation (`begin`, `get`,
/// `snapshot_get`, `multi_get`) to the abstraction's [`KvError`].
///
/// `transaction_timed_out` (1031) maps to [`KvError::DeadlineExceeded`] — a read
/// that hit the per-transaction deadline provably wrote nothing, so it is
/// surfaced (NOT auto-retried: the deadline is the caller's wait budget). This
/// is the mirror of the commit path, where the same 1031 becomes
/// [`KvError::MaybeCommitted`] because a commit that timed out may already have
/// applied. Keeping the two distinct preserves that write-status information
/// and keeps errors/metrics precise, even though neither is retried.
fn map_fdb_read_error(err: FdbError) -> KvError {
    if err.code() == FdbErrorCode::TransactionTimedOut.code() {
        return KvError::DeadlineExceeded;
    }
    if err.code() == FdbErrorCode::NotCommitted.code() {
        // Genuine optimistic-concurrency conflict: the ONLY code mapped to
        // Conflict so `mds_kv_txn_conflicts_total` counts real clashes.
        return KvError::Conflict;
    }
    // Every other retryable code — connection_failed, coordinators_changed,
    // transaction_too_old, future_version, throttling, etc. — is a transient
    // condition worth retrying, but NOT a concurrency conflict. Collapsed into
    // one variant (no consumer distinguishes the sub-causes; the raw code stays
    // in the payload for logs) and named TransientUnavailable so it does not
    // imply "cluster unreachable" when the cluster is merely throttling or the
    // txn ran too long.
    if err.is_retryable() {
        return KvError::TransientUnavailable(format!(
            "fdb error_code {}: {}",
            err.code(),
            err.message()
        ));
    }
    KvError::Backend(format!("fdb error_code {}: {}", err.code(), err.message()))
}

/// Maps a native [`FdbError`] from a COMMIT to the abstraction's [`KvError`].
///
/// A commit that fails may already have applied. The invariant `run_txn` relies
/// on: NEVER auto-retry a commit unless FDB can PROVE it did not apply. FDB's
/// `is_retryable_not_committed()` predicate (fdb_c.cpp `RETRYABLE_NOT_COMMITTED`
/// set: not_committed 1020, transaction_too_old, future_version, database_locked,
/// throttled codes, …) is exactly "retryable AND provably not committed". So:
///
/// - provably-not-committed → [`KvError::Conflict`] (1020) / [`KvError::TransientUnavailable`]
///   (the rest) — safe to re-run;
/// - everything else that is not a terminal error — `commit_unknown_result`
///   (1021), `commit_unknown_result_fatal` (1022), `request_maybe_delivered`
///   (1030), `transaction_timed_out` (1031), `cluster_version_changed`, … — is
///   treated as [`KvError::MaybeCommitted`] (NOT retryable; the write may
///   already have applied, so re-running a non-idempotent body would
///   double-apply or flip a successful CAS to `false`).
///
/// This is why the mapper is split by operation: on a read, 1031 is a
/// [`KvError::DeadlineExceeded`]; on a commit, 1031 is [`KvError::MaybeCommitted`].
fn map_fdb_commit_error(err: FdbError) -> KvError {
    // Provably-not-committed (incl. the 1020 conflict) is the ONLY class safe to
    // re-run after a commit.
    if err.is_retryable_not_committed() {
        if err.code() == FdbErrorCode::NotCommitted.code() {
            // Genuine optimistic-concurrency conflict — the only code mapped to
            // Conflict so `mds_kv_txn_conflicts_total` counts real clashes.
            return KvError::Conflict;
        }
        // The rest of the retryable-not-committed set (transaction_too_old,
        // future_version, throttled codes, …): provably not committed and worth
        // retrying, but not a conflict — so TransientUnavailable, not Conflict.
        return KvError::TransientUnavailable(format!(
            "fdb error_code {}: {}",
            err.code(),
            err.message()
        ));
    }
    // Could have committed — the write may already have applied, so this MUST
    // NOT be auto-retried (re-running a non-idempotent body would double-apply
    // or flip a successful CAS to `false`). Two sources, because FDB's own
    // maybe-committed predicate is INCOMPLETE for the commit path:
    //   - `is_maybe_committed()`: commit_unknown_result (1021),
    //     cluster_version_changed — FDB itself labels these maybe-committed.
    //     (Internally FDB folds request_maybe_delivered / never_reply into 1021;
    //     see NativeAPI.actor.cpp tryCommit catch block.)
    //   - transaction_timed_out (1031): NOT covered by is_maybe_committed().
    //     1031 is the *outer* per-txn deadline (a Timeout option timer), not a
    //     commit-RPC status, so it can fire at ANY point — including while a
    //     commit request is in flight. Once the commit RPC is sent, the client
    //     cannot know whether it applied (FDB's own tryCommit comment: the
    //     commit "might even still be in flight"). So a 1031 on the commit path
    //     means the commit status is genuinely unknown → MaybeCommitted. This is
    //     exactly why the mapper is split by operation: on a read 1031 provably
    //     wrote nothing (DeadlineExceeded); on a commit it may have applied.
    if err.is_maybe_committed() || err.code() == FdbErrorCode::TransactionTimedOut.code() {
        return KvError::MaybeCommitted;
    }
    // Everything else is a terminal, permanent error (e.g. internal_error,
    // transaction_too_large): the commit provably did not apply and retrying
    // would not help.
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
        // Enforce the fail-fast contract at the layer that applies the option:
        // FDB TransactionOption::Timeout(0) means "no timeout" (wait forever),
        // which would let begin/commit hang on an unreachable cluster. MdsConf
        // rejects <= 0, but open() is public, so guard here too — a direct
        // caller must not be able to construct a backend that never times out.
        if txn_timeout_ms <= 0 {
            return Err(KvError::Backend(format!(
                "fdb txn_timeout_ms must be greater than zero, got {txn_timeout_ms}"
            )));
        }
        let db = Database::from_path(cluster_file).map_err(map_fdb_read_error)?;
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
            let trx = self.db.create_trx().map_err(map_fdb_read_error)?;
            // Bound how long any operation on this txn waits on a stuck cluster.
            trx.set_option(TransactionOption::Timeout(self.txn_timeout_ms))
                .map_err(map_fdb_read_error)?;
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
            let error = map_fdb_read_error(err);
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
            .map_err(map_fdb_read_error)
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
            .map_err(map_fdb_read_error)
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
                    error = Some(map_fdb_read_error(err));
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
            Err(commit_err) => Err(map_fdb_commit_error(commit_err.into())),
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

#[cfg(test)]
mod tests {
    use super::*;

    // Codes the mapper does NOT branch on individually (handled via FDB
    // predicates); kept local to the test to exercise those predicate paths.
    // The branched-on codes come from `FdbErrorCode` so the test and the mapper
    // cannot drift.
    const COMMIT_UNKNOWN_RESULT: i32 = 1021; // may or may not have committed
    const INTERNAL_ERROR: i32 = 4100; // terminal, non-retryable

    /// The whole point of splitting the mapper by operation: the SAME FDB code
    /// classifies differently on a read (replay-safe) vs a commit (may already
    /// have applied). If someone swaps an arm, one of these rows fails.
    ///
    /// | code | read path            | commit path      |
    /// |------|----------------------|------------------|
    /// | 1020 | Conflict             | Conflict         | provably not committed
    /// | 1021 | TransientUnavailable | MaybeCommitted   | commit_unknown_result
    /// | 1031 | DeadlineExceeded     | MaybeCommitted   | timed out; state unknown
    /// | 4100 | Backend              | Backend          | terminal
    #[test]
    fn error_mapping_is_split_by_operation() {
        // (code, read classification, commit classification)
        // We compare on `kind()` labels to avoid matching the message payload
        // that TransientUnavailable/Backend carry.
        let cases: &[(i32, &str, &str)] = &[
            (FdbErrorCode::NotCommitted.code(), "conflict", "conflict"),
            (
                COMMIT_UNKNOWN_RESULT,
                "transient_unavailable",
                "maybe_committed",
            ),
            (
                FdbErrorCode::TransactionTimedOut.code(),
                "deadline_exceeded",
                "maybe_committed",
            ),
            (INTERNAL_ERROR, "backend", "backend"),
        ];

        for &(code, want_read, want_commit) in cases {
            let read = map_fdb_read_error(FdbError::from_code(code));
            assert_eq!(
                read.kind(),
                want_read,
                "read path: code {code} expected {want_read}, got {read:?}"
            );

            let commit = map_fdb_commit_error(FdbError::from_code(code));
            assert_eq!(
                commit.kind(),
                want_commit,
                "commit path: code {code} expected {want_commit}, got {commit:?}"
            );
        }
    }

    /// A transaction that hit the per-txn deadline (1031) must NOT be
    /// retryable on either path: on a commit `run_txn` could re-run a
    /// possibly-applied write (double-apply); on a read it would defeat
    /// fail-fast by multiplying the caller's wait budget. The two still map to
    /// distinct errors so write status is preserved: MaybeCommitted (commit)
    /// vs DeadlineExceeded (read).
    #[test]
    fn deadline_exceeded_is_not_retryable_on_either_path() {
        let commit = map_fdb_commit_error(FdbError::from_code(
            FdbErrorCode::TransactionTimedOut.code(),
        ));
        assert!(
            !commit.is_retryable(),
            "commit-path 1031 must not be retryable (may already have applied), got {commit:?}"
        );
        assert!(matches!(commit, KvError::MaybeCommitted));

        let read = map_fdb_read_error(FdbError::from_code(
            FdbErrorCode::TransactionTimedOut.code(),
        ));
        assert!(
            !read.is_retryable(),
            "read-path 1031 must not be retryable (deadline budget exhausted), got {read:?}"
        );
        assert!(matches!(read, KvError::DeadlineExceeded));
    }
}
