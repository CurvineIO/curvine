//! Error type for the generic KV abstraction.
//!
//! The single most important property of this type is the distinction between
//! *retryable* and *terminal* failures. Stateless multi-writer correctness on a
//! transactional backend (FoundationDB) depends on the caller being able to
//! tell an optimistic-concurrency conflict (safe to retry the whole
//! transaction) apart from a truly failed operation. `Conflict`,
//! `MaybeCommitted`, `DeadlineExceeded` and `TransientUnavailable` must
//! therefore never be collapsed into an opaque backend string; `run_txn`
//! inspects [`KvError::is_retryable`] to decide whether to re-run the
//! transaction closure.

use thiserror::Error;

/// Errors returned by any [`crate::kv::KvBackend`] / [`crate::kv::KvTransaction`]
/// implementation. Backends map their native failures onto these variants and
/// never leak SDK-specific types to callers.
///
/// Each variant documents whether `run_txn` may retry it and why; the machine
/// version of that answer is [`KvError::is_retryable`].
#[derive(Debug, Clone, Error)]
pub enum KvError {
    /// Optimistic-concurrency conflict: the transaction's read/write set
    /// clashed with a concurrent commit (maps to FDB `not_committed`, 1020).
    ///
    /// RETRYABLE: the commit provably did NOT apply, so re-running the whole
    /// closure is safe. This is the only variant counted as a real conflict in
    /// `mds_kv_txn_conflicts_total`.
    #[error("transaction conflict, retry the transaction")]
    Conflict,

    /// The commit result is unknown: the mutation may or may not have been
    /// applied (maps to FDB `commit_unknown_result` / `cluster_version_changed`,
    /// and to a COMMIT that hit its deadline).
    ///
    /// NOT RETRYABLE: the write may already have applied, so re-running a
    /// non-idempotent closure would double-apply or flip a successful CAS to
    /// `false`. `run_txn` surfaces it to the caller, who owns the idempotency
    /// decision (e.g. a request-id dedup record).
    #[error("commit result unknown, transaction may or may not have applied")]
    MaybeCommitted,

    /// The operation exceeded its per-transaction deadline (maps to FDB
    /// `transaction_timed_out`, 1031, on a READ).
    ///
    /// NOT RETRYABLE: the deadline is the caller's wait budget, so exhausting it
    /// must surface immediately rather than silently re-running and multiplying
    /// the wait (matches FDB's own `is_retryable(1031) == false`). Distinct from
    /// `MaybeCommitted`: a read that hit the deadline provably wrote nothing; a
    /// commit that hit it may have applied (that path maps to `MaybeCommitted`).
    /// Kept distinct from `Backend` so observability can tell "deadline hit"
    /// (tune the timeout / investigate load) from a permanent error.
    ///
    /// Named `DeadlineExceeded`, not `Timeout`, so it does not read as a
    /// retryable transient timeout: exhausting the budget is terminal here.
    #[error("operation deadline exceeded")]
    DeadlineExceeded,

    /// A transient backend condition that is worth another attempt but is NOT a
    /// concurrency conflict. Covers everything FDB marks retryable except the
    /// conflict itself: cluster unreachable (`connection_failed`,
    /// `coordinators_changed`), read version too old (`transaction_too_old`),
    /// storage lagging (`future_version`), throttling (`*_throttled`,
    /// `hot_shard`), and resource pressure (`*_memory_limit_exceeded`).
    ///
    /// RETRYABLE: re-running has a real chance of success. These sub-causes are
    /// deliberately NOT split into separate variants — no consumer distinguishes
    /// them (`run_txn` only asks "retry?"; the raw FDB code is preserved in the
    /// payload string for logs). Named `TransientUnavailable`, not `Unavailable`,
    /// to avoid implying "cluster not reachable" when the cluster is healthy but
    /// throttling or the transaction simply ran too long.
    #[error("backend transiently unavailable: {0}")]
    TransientUnavailable(String),

    /// The transaction was explicitly aborted by the caller (e.g. the
    /// transaction closure returned an application error).
    ///
    /// NOT RETRYABLE: this is the caller's own decision, not a backend failure;
    /// re-running would just abort again.
    #[error("transaction aborted: {0}")]
    Aborted(String),

    /// A terminal backend error (e.g. corrupted value, encoding violation,
    /// permanent I/O error, `internal_error`, `transaction_too_large`).
    ///
    /// NOT RETRYABLE: re-running cannot fix it. This is NOT a catch-all — every
    /// retryable / maybe-committed / deadline condition has its own variant
    /// above; `Backend` is only the failures that are genuinely permanent.
    #[error("kv backend error: {0}")]
    Backend(String),
}

impl KvError {
    /// Classifies whether `run_txn` may transparently re-run the closure.
    ///
    /// Retryable ⇔ the failure provably did not apply AND another attempt could
    /// succeed: `Conflict` (commit did not apply) and `TransientUnavailable`
    /// (transient blip that did not exhaust the deadline). Everything else is
    /// surfaced to the caller:
    /// - `MaybeCommitted`: the mutation may already have applied.
    /// - `DeadlineExceeded`: the caller's wait budget is exhausted.
    /// - `Aborted` / `Backend`: caller decision / permanent failure.
    pub fn is_retryable(&self) -> bool {
        matches!(self, KvError::Conflict | KvError::TransientUnavailable(_))
    }

    /// Short, cardinality-bounded label used for metrics. Never contains keys,
    /// values or request identifiers so it is safe as a Prometheus label.
    pub fn kind(&self) -> &'static str {
        match self {
            KvError::Conflict => "conflict",
            KvError::MaybeCommitted => "maybe_committed",
            KvError::DeadlineExceeded => "deadline_exceeded",
            KvError::TransientUnavailable(_) => "transient_unavailable",
            KvError::Aborted(_) => "aborted",
            KvError::Backend(_) => "backend",
        }
    }
}

/// Convenience result alias for KV operations.
pub type KvResult<T> = Result<T, KvError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        // Auto-retryable: provably did not apply and worth another attempt.
        assert!(KvError::Conflict.is_retryable());
        assert!(KvError::TransientUnavailable("down".into()).is_retryable());

        // Not retryable: the write may already have applied, the deadline
        // budget is exhausted, or the failure is terminal.
        assert!(!KvError::DeadlineExceeded.is_retryable());
        assert!(!KvError::MaybeCommitted.is_retryable());
        assert!(!KvError::Aborted("app".into()).is_retryable());
        assert!(!KvError::Backend("boom".into()).is_retryable());
    }

    #[test]
    fn kind_labels_are_bounded() {
        // Two errors of the same variant produce the same label regardless of
        // their payload, keeping metric cardinality bounded.
        assert_eq!(
            KvError::TransientUnavailable("a".into()).kind(),
            KvError::TransientUnavailable("b".into()).kind()
        );
        assert_eq!(KvError::Backend("x".into()).kind(), "backend");
    }
}
