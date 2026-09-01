//! Error type for the generic KV abstraction.
//!
//! The single most important property of this type is the distinction between
//! *retryable* and *terminal* failures. Stateless multi-writer correctness on a
//! transactional backend (FoundationDB) depends on the caller being able to
//! tell an optimistic-concurrency conflict (safe to retry the whole
//! transaction) apart from a truly failed operation. `Conflict`,
//! `MaybeCommitted`, `Timeout` and `Unavailable` must therefore never be
//! collapsed into an opaque backend string; `run_txn` inspects
//! [`KvError::is_retryable`] to decide whether to re-run the transaction
//! closure.

use thiserror::Error;

/// Errors returned by any [`crate::kv::KvBackend`] / [`crate::kv::KvTransaction`]
/// implementation. Backends map their native failures onto these variants and
/// never leak SDK-specific types to callers.
#[derive(Debug, Clone, Error)]
pub enum KvError {
    /// Optimistic-concurrency conflict: the transaction's read/write set
    /// clashed with a concurrent commit. The commit did NOT apply. Safe to
    /// retry the whole transaction closure (maps to FDB `not_committed`).
    #[error("transaction conflict, retry the transaction")]
    Conflict,

    /// The commit result is unknown: the mutation may or may not have been
    /// applied (maps to FDB `commit_unknown_result`). NOT retryable —
    /// `run_txn` surfaces it to the caller, who owns the idempotency decision.
    #[error("commit result unknown, transaction may or may not have applied")]
    MaybeCommitted,

    /// The operation exceeded its deadline. Retryable.
    #[error("operation timed out")]
    Timeout,

    /// The backend is temporarily unavailable (e.g. FDB cluster not reachable).
    /// Retryable with backoff.
    #[error("backend temporarily unavailable: {0}")]
    Unavailable(String),

    /// The transaction was explicitly aborted by the caller (e.g. the
    /// transaction closure returned an application error). Terminal.
    #[error("transaction aborted: {0}")]
    Aborted(String),

    /// A terminal backend error that must NOT be retried.
    ///
    /// Not a catch-all for "any backend error": the retryable conditions
    /// (conflict, unknown commit, timeout, transient unavailability) have their
    /// own variants above. `Backend` is only the remaining failures re-running
    /// can't fix (corrupted value, encoding violation, permanent I/O error).
    #[error("kv backend error: {0}")]
    Backend(String),
}

impl KvError {
    /// Classifies whether `run_txn` may transparently re-run the closure.
    /// Excludes `MaybeCommitted`: the mutation may already have applied, so
    /// retrying a non-idempotent body would double-apply or flip a successful
    /// CAS to `false`. Only commits that provably did not apply (`Conflict`) or
    /// never decided (`Timeout`, `Unavailable`) are retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            KvError::Conflict | KvError::Timeout | KvError::Unavailable(_)
        )
    }

    /// Short, cardinality-bounded label used for metrics. Never contains keys,
    /// values or request identifiers so it is safe as a Prometheus label.
    pub fn kind(&self) -> &'static str {
        match self {
            KvError::Conflict => "conflict",
            KvError::MaybeCommitted => "maybe_committed",
            KvError::Timeout => "timeout",
            KvError::Unavailable(_) => "unavailable",
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
        // Auto-retryable: the commit provably did not apply or never decided.
        assert!(KvError::Conflict.is_retryable());
        assert!(KvError::Timeout.is_retryable());
        assert!(KvError::Unavailable("down".into()).is_retryable());

        // Not retryable: the write may already have applied, or is terminal.
        assert!(!KvError::MaybeCommitted.is_retryable());
        assert!(!KvError::Aborted("app".into()).is_retryable());
        assert!(!KvError::Backend("boom".into()).is_retryable());
    }

    #[test]
    fn kind_labels_are_bounded() {
        // Two errors of the same variant produce the same label regardless of
        // their payload, keeping metric cardinality bounded.
        assert_eq!(
            KvError::Unavailable("a".into()).kind(),
            KvError::Unavailable("b".into()).kind()
        );
        assert_eq!(KvError::Backend("x".into()).kind(), "backend");
    }
}
