//! Byte-level, business-agnostic KV abstraction.
//!
//! This module defines the stable boundary the stateless MDS repositories and
//! the future FoundationDB backend both build on. It intentionally knows
//! NOTHING about MDS domain types (`MetaKey`, `MetaValue`, mount tables, paths,
//! ...). Keys and values are opaque byte strings; ordering is unsigned
//! lexicographic over the raw bytes, which every intended backend (memory,
//! FDB) preserves.
//!
//! ## Transaction model
//!
//! Transactions are the first-class primitive, not batches. A
//! [`KvTransaction`] buffers writes (`put`/`delete`) and tracks the keys it
//! reads (`get`/`multi_get`, plus explicit
//! [`KvTransaction::add_read_conflict`]) in a read-conflict set. `commit`
//! applies the buffered writes atomically only if no key in the read-conflict
//! set was changed by a concurrent committed transaction; otherwise it returns
//! [`KvError::Conflict`]. This optimistic-concurrency contract is what lets
//! multiple stateless MDS processes write safely without a single owning node,
//! and it mirrors FoundationDB's serializable-snapshot-isolation semantics so
//! the FDB backend (PR 3) can implement the same trait with native behavior.
//!
//! Use [`run_txn`] to execute a transaction closure with automatic retry on
//! retryable errors ([`KvError::is_retryable`]); it re-runs the closure from a
//! fresh snapshot so read-modify-write and compare-and-set become correct
//! under contention.

use crate::kv::error::{KvError, KvResult};
use crate::kv::metrics;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// A single transaction against a [`KvBackend`].
///
/// Reads observe a consistent snapshot taken when the transaction began and add
/// the accessed keys to the read-conflict set. Writes are buffered and applied
/// atomically by [`KvTransaction::commit`]. After `commit` or
/// [`KvTransaction::rollback`] the transaction must not be used again.
#[async_trait]
pub trait KvTransaction: Send {
    /// Reads a single key, adding it to the read-conflict set.
    async fn get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>>;

    /// Reads many keys in one call, adding each to the read-conflict set.
    /// The returned vector has one entry per input key, in order.
    async fn multi_get(&mut self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>>;

    /// Reads a single key from the transaction's snapshot WITHOUT adding it to
    /// the read-conflict set (maps to FDB `snapshotGet`).
    ///
    /// Same snapshot as [`get`], so not a dirty read; the only difference is a
    /// concurrent change to this key won't make the transaction conflict at
    /// commit. Use only when the value read doesn't affect what the transaction
    /// writes (advisory reads, wide scans); otherwise use [`get`].
    ///
    /// [`get`]: KvTransaction::get
    async fn snapshot_get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>>;

    /// Buffers a write of `value` at `key`.
    fn put(&mut self, key: &[u8], value: &[u8]);

    /// Buffers a delete of `key`.
    fn delete(&mut self, key: &[u8]);

    /// Explicitly adds `key` to the read-conflict set without reading it. Use
    /// when a decision depends on a key's absence or when the value was read
    /// through another channel.
    fn add_read_conflict(&mut self, key: &[u8]);

    /// Applies the buffered writes atomically. Returns [`KvError::Conflict`]
    /// when a key in the read-conflict set was changed concurrently; the
    /// transaction is then discarded and the caller should retry from a fresh
    /// transaction (see [`run_txn`]).
    async fn commit(&mut self) -> KvResult<()>;

    /// Discards all buffered writes and releases the transaction.
    fn rollback(&mut self);
}

/// A pluggable byte-level KV store.
///
/// Implementations provide raw transactions via [`KvBackend::begin_raw`]; the
/// shared [`KvBackend::begin`] wrapper owns transaction metrics. The
/// non-transactional convenience methods have default implementations that run
/// a single auto-committed transaction, so a backend only has to implement
/// `begin_raw` and `name` to be fully usable.
#[async_trait]
pub trait KvBackend: Send + Sync {
    /// Short, stable backend identifier used as a metrics label
    /// (e.g. `"memory"`, `"fdb"`). Must be low-cardinality.
    fn name(&self) -> &'static str;

    /// Starts a raw, uninstrumented transaction.
    ///
    /// Backend implementations must only create their transaction here; the
    /// default [`KvBackend::begin`] owns operation metrics and in-flight
    /// accounting for every backend.
    /// Decorating backends must delegate to the inner backend's `begin_raw`,
    /// never its `begin`, to avoid wrapping and counting the transaction twice.
    async fn begin_raw(&self) -> KvResult<Box<dyn KvTransaction>>;

    /// Starts a new instrumented transaction.
    ///
    /// Implementors must not override this method: doing so bypasses the shared
    /// operation metrics and in-flight accounting. Customize [`Self::begin_raw`]
    /// instead.
    async fn begin(&self) -> KvResult<Box<dyn KvTransaction>> {
        let start = Instant::now();
        match self.begin_raw().await {
            Ok(txn) => {
                metrics::metrics().observe(self.name(), metrics::op::BEGIN, start, &Ok(()));
                metrics::metrics().txn_in_flight.inc();
                Ok(Box::new(InstrumentedTransaction::new(self.name(), txn)))
            }
            Err(error) => {
                metrics::metrics().observe(
                    self.name(),
                    metrics::op::BEGIN,
                    start,
                    &Err(error.clone()),
                );
                Err(error)
            }
        }
    }

    /// Point read of a single key. Tracks the key in the read-conflict set and
    /// is wrapped in [`run_txn`], so a concurrent change during the read is
    /// retried; `Conflict` surfaces only if it persists past
    /// `DEFAULT_MAX_RETRIES` (a pathological write hotspot).
    /// Use [`snapshot_get`](KvBackend::snapshot_get) for a conflict-free read.
    async fn get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let key = key.to_vec();
        run_txn(self, DEFAULT_MAX_RETRIES, |txn| {
            let key = key.clone();
            Box::pin(async move { txn.get(&key).await })
        })
        .await
    }

    /// Point snapshot read of a single key: reads the begin snapshot WITHOUT
    /// read-conflict tracking, then rolls back. Never conflicts and never
    /// retries. See [`KvTransaction::snapshot_get`].
    async fn snapshot_get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let mut txn = self.begin().await?;
        let out = txn.snapshot_get(key).await;
        txn.rollback();
        out
    }

    /// Batch read of many keys. Like [`get`](KvBackend::get), tracks the keys
    /// and is wrapped in [`run_txn`] so concurrent changes are retried.
    async fn multi_get(&self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
        let keys = keys.to_vec();
        run_txn(self, DEFAULT_MAX_RETRIES, |txn| {
            let keys = keys.clone();
            Box::pin(async move { txn.multi_get(&keys).await })
        })
        .await
    }

    /// Point write of a single key. Wrapped in [`run_txn`] so a transient
    /// conflict is retried instead of surfaced.
    async fn put(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        let key = key.to_vec();
        let value = value.to_vec();
        let start = std::time::Instant::now();
        let result = run_txn(self, DEFAULT_MAX_RETRIES, |txn| {
            let key = key.clone();
            let value = value.clone();
            Box::pin(async move {
                txn.put(&key, &value);
                Ok(())
            })
        })
        .await;
        metrics::metrics().observe(self.name(), metrics::op::PUT, start, &result);
        result
    }

    /// Point delete of a single key.
    async fn delete(&self, key: &[u8]) -> KvResult<()> {
        let key = key.to_vec();
        let start = std::time::Instant::now();
        let result = run_txn(self, DEFAULT_MAX_RETRIES, |txn| {
            let key = key.clone();
            Box::pin(async move {
                txn.delete(&key);
                Ok(())
            })
        })
        .await;
        metrics::metrics().observe(self.name(), metrics::op::DELETE, start, &result);
        result
    }

    /// Batch delete of many keys in a single transaction.
    async fn batch_delete(&self, keys: &[Vec<u8>]) -> KvResult<()> {
        let keys = keys.to_vec();
        let start = std::time::Instant::now();
        let result = run_txn(self, DEFAULT_MAX_RETRIES, |txn| {
            let keys = keys.clone();
            Box::pin(async move {
                for key in &keys {
                    txn.delete(key);
                }
                Ok(())
            })
        })
        .await;
        metrics::metrics().observe(self.name(), metrics::op::BATCH_DELETE, start, &result);
        result
    }

    /// Atomic compare-and-set. Writes `new` at `key` only if the current value
    /// equals `expected` (`None` means "the key must be absent"; a `new` of
    /// `None` deletes the key). Returns `true` when the write was applied,
    /// `false` when the precondition did not hold.
    ///
    /// Implemented as a retrying read-modify-write so it is correct under
    /// contention on any conforming backend.
    async fn compare_and_set(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> KvResult<bool> {
        let key = key.to_vec();
        let expected = expected.map(|v| v.to_vec());
        let new = new.map(|v| v.to_vec());
        let start = std::time::Instant::now();
        let result = run_txn(self, DEFAULT_MAX_RETRIES, |txn| {
            let key = key.clone();
            let expected = expected.clone();
            let new = new.clone();
            Box::pin(async move {
                let current = txn.get(&key).await?;
                if current.as_deref() != expected.as_deref() {
                    // `txn.get` already added `key` to the read-conflict set,
                    // so a concurrent change still forces a retry, not a stale
                    // `false`.
                    return Ok(false);
                }
                match &new {
                    Some(value) => txn.put(&key, value),
                    None => txn.delete(&key),
                }
                Ok(true)
            })
        })
        .await;
        metrics::metrics().observe(
            self.name(),
            metrics::op::CAS,
            start,
            &result.as_ref().map(|_| ()).map_err(|e| e.clone()),
        );
        result
    }
}

/// Shared metrics wrapper for every transaction returned by [`KvBackend::begin`].
///
/// Keeping this outside individual backends ensures a new backend gets the
/// same operation labels, latency accounting, and in-flight lifecycle without
/// importing Prometheus plumbing.
struct InstrumentedTransaction {
    backend: &'static str,
    inner: Box<dyn KvTransaction>,
    finished: bool,
}

impl InstrumentedTransaction {
    fn new(backend: &'static str, inner: Box<dyn KvTransaction>) -> Self {
        Self {
            backend,
            inner,
            finished: false,
        }
    }

    fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            metrics::metrics().txn_in_flight.dec();
        }
    }
}

#[async_trait]
impl KvTransaction for InstrumentedTransaction {
    async fn get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let start = Instant::now();
        let result = self.inner.get(key).await;
        observe_result(self.backend, metrics::op::GET, start, &result);
        result
    }

    async fn multi_get(&mut self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
        let start = Instant::now();
        let result = self.inner.multi_get(keys).await;
        observe_result(self.backend, metrics::op::MULTI_GET, start, &result);
        result
    }

    async fn snapshot_get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let start = Instant::now();
        let result = self.inner.snapshot_get(key).await;
        observe_result(self.backend, metrics::op::SNAPSHOT_GET, start, &result);
        result
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        metrics::metrics().observe_kv_size(self.backend, key.len(), value.len());
        self.inner.put(key, value);
    }

    fn delete(&mut self, key: &[u8]) {
        self.inner.delete(key);
    }

    fn add_read_conflict(&mut self, key: &[u8]) {
        self.inner.add_read_conflict(key);
    }

    async fn commit(&mut self) -> KvResult<()> {
        if self.finished {
            let result = Err(KvError::Backend("commit on finished transaction".into()));
            metrics::metrics().observe(self.backend, metrics::op::COMMIT, Instant::now(), &result);
            return result;
        }

        let start = Instant::now();
        let result = self.inner.commit().await;
        observe_result(self.backend, metrics::op::COMMIT, start, &result);
        self.finish();
        result
    }

    fn rollback(&mut self) {
        if !self.finished {
            self.inner.rollback();
            self.finish();
        }
    }
}

impl Drop for InstrumentedTransaction {
    fn drop(&mut self) {
        self.finish();
    }
}

fn observe_result<T>(backend: &str, op: &str, start: Instant, result: &KvResult<T>) {
    metrics::metrics().observe(
        backend,
        op,
        start,
        &result.as_ref().map(|_| ()).map_err(|error| error.clone()),
    );
}

/// Default retry budget for [`run_txn`] and the convenience CAS helper.
pub const DEFAULT_MAX_RETRIES: u32 = 16;

/// Runs a transaction closure with automatic retry on retryable errors.
///
/// The closure gets a fresh transaction on each attempt and returns the value
/// to hand back on success. `run_txn` commits it; if `begin`, the closure, or
/// the commit returns an [`KvError::is_retryable`] error it re-runs on a
/// new transaction, up to `max_retries` times with exponential backoff.
///
/// Retry excludes [`KvError::MaybeCommitted`] (the write may already have
/// applied); it and all terminal errors propagate unchanged. The closure is
/// `Fn` because it may run multiple times — keep it free of external mutable
/// state that must not be repeated.
pub async fn run_txn<B, F, T>(backend: &B, max_retries: u32, f: F) -> KvResult<T>
where
    B: KvBackend + ?Sized,
    F: for<'a> Fn(
        &'a mut dyn KvTransaction,
    ) -> Pin<Box<dyn Future<Output = KvResult<T>> + Send + 'a>>,
{
    let mut attempt: u32 = 0;
    loop {
        // begin() failures are folded into the same retry budget.
        let outcome = match backend.begin().await {
            Ok(mut txn) => {
                let result = {
                    let txn_ref = txn.as_mut();
                    f(txn_ref).await
                };
                match result {
                    Ok(value) => txn.commit().await.map(|_| value),
                    Err(error) => {
                        txn.rollback();
                        Err(error)
                    }
                }
            }
            Err(error) => Err(error),
        };

        match outcome {
            Ok(value) => return Ok(value),
            Err(error) => {
                if error.is_retryable() && attempt < max_retries {
                    metrics::metrics().record_retry(backend.name());
                    backoff(attempt).await;
                    attempt += 1;
                } else {
                    return Err(error);
                }
            }
        }
    }
}

/// Exponential backoff capped at 100ms. Attempt 0 does not sleep.
async fn backoff(attempt: u32) {
    if attempt == 0 {
        return;
    }
    // 1<<7 = 128 lets the 100ms cap actually take effect (1<<6 = 64 never would).
    let millis = (1u64 << attempt.min(7)).min(100);
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deliberately has no metrics code: the shared `KvBackend::begin` wrapper
    /// must provide the full operation instrumentation for a new backend.
    struct MetricsFreeBackend(&'static str);

    struct MetricsFreeTransaction;

    #[async_trait]
    impl KvBackend for MetricsFreeBackend {
        fn name(&self) -> &'static str {
            self.0
        }

        async fn begin_raw(&self) -> KvResult<Box<dyn KvTransaction>> {
            Ok(Box::new(MetricsFreeTransaction))
        }
    }

    #[async_trait]
    impl KvTransaction for MetricsFreeTransaction {
        async fn get(&mut self, _key: &[u8]) -> KvResult<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn multi_get(&mut self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
            Ok(vec![None; keys.len()])
        }

        async fn snapshot_get(&mut self, _key: &[u8]) -> KvResult<Option<Vec<u8>>> {
            Ok(None)
        }

        fn put(&mut self, _key: &[u8], _value: &[u8]) {}

        fn delete(&mut self, _key: &[u8]) {}

        fn add_read_conflict(&mut self, _key: &[u8]) {}

        async fn commit(&mut self) -> KvResult<()> {
            Ok(())
        }

        fn rollback(&mut self) {}
    }

    #[tokio::test]
    async fn metrics_are_instrumented_without_backend_metrics_code() {
        let backend = MetricsFreeBackend("metrics-free-test");
        let metrics = metrics::metrics();
        let name = backend.name();

        let begin_before = metrics
            .ops_total
            .with_label_values(&[name, metrics::op::BEGIN])
            .get();
        let get_before = metrics
            .ops_total
            .with_label_values(&[name, metrics::op::GET])
            .get();
        let snapshot_before = metrics
            .ops_total
            .with_label_values(&[name, metrics::op::SNAPSHOT_GET])
            .get();
        let multi_get_before = metrics
            .ops_total
            .with_label_values(&[name, metrics::op::MULTI_GET])
            .get();
        let commit_before = metrics
            .ops_total
            .with_label_values(&[name, metrics::op::COMMIT])
            .get();
        let key_sizes = metrics.kv_key_size_bytes.with_label_values(&[name]);
        let value_sizes = metrics.kv_value_size_bytes.with_label_values(&[name]);
        let key_count_before = key_sizes.get_sample_count();
        let key_sum_before = key_sizes.get_sample_sum();
        let value_count_before = value_sizes.get_sample_count();
        let value_sum_before = value_sizes.get_sample_sum();

        let mut txn = backend.begin().await.unwrap();
        txn.get(b"key").await.unwrap();
        txn.snapshot_get(b"key").await.unwrap();
        txn.multi_get(&[b"key".to_vec()]).await.unwrap();
        txn.put(b"key", b"value");
        txn.commit().await.unwrap();

        assert_eq!(key_sizes.get_sample_count(), key_count_before + 1);
        assert_eq!(key_sizes.get_sample_sum(), key_sum_before + 3.0);
        assert_eq!(value_sizes.get_sample_count(), value_count_before + 1);
        assert_eq!(value_sizes.get_sample_sum(), value_sum_before + 5.0);

        assert_eq!(
            metrics
                .ops_total
                .with_label_values(&[name, metrics::op::BEGIN])
                .get(),
            begin_before + 1
        );
        assert_eq!(
            metrics
                .ops_total
                .with_label_values(&[name, metrics::op::GET])
                .get(),
            get_before + 1
        );
        assert_eq!(
            metrics
                .ops_total
                .with_label_values(&[name, metrics::op::SNAPSHOT_GET])
                .get(),
            snapshot_before + 1
        );
        assert_eq!(
            metrics
                .ops_total
                .with_label_values(&[name, metrics::op::MULTI_GET])
                .get(),
            multi_get_before + 1
        );
        assert_eq!(
            metrics
                .ops_total
                .with_label_values(&[name, metrics::op::COMMIT])
                .get(),
            commit_before + 1
        );
    }

    struct FailingBeginBackend;

    #[async_trait]
    impl KvBackend for FailingBeginBackend {
        fn name(&self) -> &'static str {
            "metrics-failed-begin-test"
        }

        async fn begin_raw(&self) -> KvResult<Box<dyn KvTransaction>> {
            Err(KvError::Backend("begin failed".into()))
        }
    }

    #[tokio::test]
    async fn failed_begin_records_error_without_returning_a_transaction() {
        let backend = FailingBeginBackend;
        let metrics = metrics::metrics();
        let name = backend.name();
        let total = metrics
            .ops_total
            .with_label_values(&[name, metrics::op::BEGIN]);
        let errors =
            metrics
                .ops_err_total
                .with_label_values(&[name, metrics::op::BEGIN, "backend"]);
        let successes = metrics
            .ops_ok_total
            .with_label_values(&[name, metrics::op::BEGIN]);
        let latency = metrics
            .op_latency_us
            .with_label_values(&[name, metrics::op::BEGIN]);
        let total_before = total.get();
        let errors_before = errors.get();
        let successes_before = successes.get();
        let latency_before = latency.get_sample_count();

        assert!(matches!(
            backend.begin().await,
            Err(KvError::Backend(message)) if message == "begin failed"
        ));

        assert_eq!(total.get(), total_before + 1);
        assert_eq!(errors.get(), errors_before + 1);
        assert_eq!(successes.get(), successes_before);
        assert_eq!(latency.get_sample_count(), latency_before + 1);
    }

    #[tokio::test]
    async fn commit_on_finished_transaction_records_error() {
        for (name, rollback) in [
            ("metrics-commit-after-commit-test", false),
            ("metrics-commit-after-rollback-test", true),
        ] {
            let backend = MetricsFreeBackend(name);
            let mut txn = backend.begin().await.unwrap();
            if rollback {
                txn.rollback();
            } else {
                txn.commit().await.unwrap();
            }

            let metrics = metrics::metrics();
            let total = metrics
                .ops_total
                .with_label_values(&[name, metrics::op::COMMIT]);
            let errors =
                metrics
                    .ops_err_total
                    .with_label_values(&[name, metrics::op::COMMIT, "backend"]);
            let successes = metrics
                .ops_ok_total
                .with_label_values(&[name, metrics::op::COMMIT]);
            let total_before = total.get();
            let errors_before = errors.get();
            let successes_before = successes.get();

            assert!(matches!(
                txn.commit().await,
                Err(KvError::Backend(message)) if message == "commit on finished transaction"
            ));

            assert_eq!(total.get(), total_before + 1);
            assert_eq!(errors.get(), errors_before + 1);
            assert_eq!(successes.get(), successes_before);
        }
    }
}
