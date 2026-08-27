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

use crate::kv::error::KvResult;
use crate::kv::metrics;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

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
/// Implementations provide transactions via [`KvBackend::begin`]; the
/// non-transactional convenience methods have default implementations that run
/// a single auto-committed transaction, so a backend only has to implement
/// `begin` and `name` to be fully usable.
#[async_trait]
pub trait KvBackend: Send + Sync {
    /// Short, stable backend identifier used as a metrics label
    /// (e.g. `"memory"`, `"fdb"`). Must be low-cardinality.
    fn name(&self) -> &'static str;

    /// Starts a new transaction.
    async fn begin(&self) -> KvResult<Box<dyn KvTransaction>>;

    /// Point read of a single key.
    async fn get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let mut txn = self.begin().await?;
        let value = txn.get(key).await?;
        txn.commit().await?;
        Ok(value)
    }

    /// Point snapshot read of a single key (no read-conflict tracking).
    ///
    /// See [`KvTransaction::snapshot_get`]. In a standalone transaction the
    /// conflict set is irrelevant so this matches [`get`](KvBackend::get); it
    /// exists so callers can express intent that carries through when the read
    /// later moves inside a larger transaction.
    async fn snapshot_get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let mut txn = self.begin().await?;
        let value = txn.snapshot_get(key).await?;
        txn.commit().await?;
        Ok(value)
    }

    /// Batch read of many keys in a single transaction.
    async fn multi_get(&self, keys: &[Vec<u8>]) -> KvResult<Vec<Option<Vec<u8>>>> {
        let mut txn = self.begin().await?;
        let values = txn.multi_get(keys).await?;
        txn.commit().await?;
        Ok(values)
    }

    /// Point write of a single key.
    async fn put(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        let start = std::time::Instant::now();
        let result = async {
            let mut txn = self.begin().await?;
            txn.put(key, value);
            txn.commit().await
        }
        .await;
        metrics::metrics().observe(self.name(), metrics::op::PUT, start, &result);
        result
    }

    /// Point delete of a single key.
    async fn delete(&self, key: &[u8]) -> KvResult<()> {
        let start = std::time::Instant::now();
        let result = async {
            let mut txn = self.begin().await?;
            txn.delete(key);
            txn.commit().await
        }
        .await;
        metrics::metrics().observe(self.name(), metrics::op::DELETE, start, &result);
        result
    }

    /// Batch delete of many keys in a single transaction.
    async fn batch_delete(&self, keys: &[Vec<u8>]) -> KvResult<()> {
        let start = std::time::Instant::now();
        let result = async {
            let mut txn = self.begin().await?;
            for key in keys {
                txn.delete(key);
            }
            txn.commit().await
        }
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
                    // Precondition failed; nothing to write. Add the key to the
                    // conflict set so a concurrent change still forces a retry
                    // rather than a stale `false`.
                    txn.add_read_conflict(&key);
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

/// Default retry budget for [`run_txn`] and the convenience CAS helper.
pub const DEFAULT_MAX_RETRIES: u32 = 16;

/// Runs a transaction closure with automatic retry on retryable errors.
///
/// The closure receives a fresh, mutable transaction on each attempt and must
/// perform all of its reads and buffered writes on it, returning the value to
/// hand back on success. `run_txn` commits the transaction; if either the
/// closure or the commit returns a retryable error ([`KvError::is_retryable`])
/// it re-runs the closure on a brand-new transaction, up to `max_retries`
/// times, with a short exponential backoff. Terminal errors propagate
/// immediately.
///
/// The closure is `Fn` (not `FnMut`) because it may be invoked multiple times;
/// keep it free of external mutable state that must not be repeated.
pub async fn run_txn<B, F, T>(backend: &B, max_retries: u32, f: F) -> KvResult<T>
where
    B: KvBackend + ?Sized,
    F: for<'a> Fn(
        &'a mut dyn KvTransaction,
    ) -> Pin<Box<dyn Future<Output = KvResult<T>> + Send + 'a>>,
{
    let mut attempt: u32 = 0;
    loop {
        let mut txn: Box<dyn KvTransaction> = backend.begin().await?;
        let result = {
            let txn_ref = txn.as_mut();
            f(txn_ref).await
        };

        let outcome = match result {
            Ok(value) => txn.commit().await.map(|_| value),
            Err(error) => {
                txn.rollback();
                Err(error)
            }
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

/// Exponential backoff capped at ~100ms. Attempt 0 does not sleep.
async fn backoff(attempt: u32) {
    if attempt == 0 {
        return;
    }
    let millis = (1u64 << attempt.min(6)).min(100);
    tokio::time::sleep(Duration::from_millis(millis)).await;
}
