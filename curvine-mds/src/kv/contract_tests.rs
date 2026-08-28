//! Backend-agnostic KV contract tests.
//!
//! Every assertion here is expressed against the [`KvBackend`] trait, never a
//! concrete type, so the FoundationDB backend (a later step) can run the exact
//! same suite by supplying its own factory. The memory backend is wired up at
//! the bottom of this file via [`run_contract`].

use crate::kv::backend::{run_txn, KvBackend, DEFAULT_MAX_RETRIES};
use crate::kv::error::KvError;
use crate::kv::memory::MemoryBackend;
use std::sync::Arc;

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ----- point operations -----

async fn point_get_put_delete(be: Arc<dyn KvBackend>) {
    assert_eq!(be.get(&b("k")).await.unwrap(), None);

    be.put(&b("k"), &b("v")).await.unwrap();
    assert_eq!(be.get(&b("k")).await.unwrap(), Some(b("v")));

    be.put(&b("k"), &b("v2")).await.unwrap();
    assert_eq!(be.get(&b("k")).await.unwrap(), Some(b("v2")));

    be.delete(&b("k")).await.unwrap();
    assert_eq!(be.get(&b("k")).await.unwrap(), None);
    // delete of an absent key is a no-op success.
    be.delete(&b("k")).await.unwrap();
}

// ----- batch operations -----

async fn batch_get_delete(be: Arc<dyn KvBackend>) {
    be.put(&b("a"), &b("1")).await.unwrap();
    be.put(&b("c"), &b("3")).await.unwrap();

    let got = be.multi_get(&[b("a"), b("b"), b("c")]).await.unwrap();
    assert_eq!(got, vec![Some(b("1")), None, Some(b("3"))]);

    be.batch_delete(&[b("a"), b("c"), b("missing")])
        .await
        .unwrap();
    let got = be.multi_get(&[b("a"), b("c")]).await.unwrap();
    assert_eq!(got, vec![None, None]);
}

// ----- arbitrary byte compatibility -----

async fn arbitrary_bytes_round_trip(be: Arc<dyn KvBackend>) {
    let cases = [
        vec![0x80],
        vec![0xFF],
        vec![0xC0, 0xAF],
        vec![0xE2, 0x28, 0xA1],
        vec![b'f', b'o', 0x80, 0xFF],
        vec![b'a', 0x00, b'b'],
    ];

    for (index, key) in cases.into_iter().enumerate() {
        let value = vec![0x00, 0xFF, index as u8, 0x80];
        be.put(&key, &value).await.unwrap();
        assert_eq!(be.get(&key).await.unwrap(), Some(value));
    }
}

async fn distinct_invalid_utf8_keys_remain_distinct(be: Arc<dyn KvBackend>) {
    let key_1 = vec![b'f', b'o', 0x80];
    let key_2 = vec![b'f', b'o', 0x81];

    assert_eq!(
        String::from_utf8_lossy(&key_1),
        String::from_utf8_lossy(&key_2)
    );

    be.put(&key_1, b"value-1").await.unwrap();
    be.put(&key_2, b"value-2").await.unwrap();

    assert_eq!(be.get(&key_1).await.unwrap(), Some(b("value-1")));
    assert_eq!(be.get(&key_2).await.unwrap(), Some(b("value-2")));
}

async fn multilingual_utf8_round_trip(be: Arc<dyn KvBackend>) {
    let cases = [
        ("目录/文件.txt", "中文内容"),
        ("日本語/ファイル", "内容"),
        ("한국어/파일", "데이터"),
        ("emoji/😀/🚀", "✅"),
        ("العربية/ملف", "قيمة"),
    ];

    for (key, value) in cases {
        be.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        assert_eq!(
            be.get(key.as_bytes()).await.unwrap(),
            Some(value.as_bytes().to_vec())
        );
    }
}

async fn unicode_normalization_is_not_implicit(be: Arc<dyn KvBackend>) {
    let composed = "é".as_bytes().to_vec();
    let decomposed = "e\u{301}".as_bytes().to_vec();

    assert_ne!(composed, decomposed);
    be.put(&composed, b"composed").await.unwrap();
    be.put(&decomposed, b"decomposed").await.unwrap();

    assert_eq!(be.get(&composed).await.unwrap(), Some(b("composed")));
    assert_eq!(be.get(&decomposed).await.unwrap(), Some(b("decomposed")));
}

// ----- transactional read-modify-write -----

async fn txn_read_modify_write(be: Arc<dyn KvBackend>) {
    be.put(&b("ctr"), &b("0")).await.unwrap();

    // Increment inside a single transaction closure via run_txn.
    let key = b("ctr");
    let new = run_txn(be.as_ref(), DEFAULT_MAX_RETRIES, move |txn| {
        let key = key.clone();
        Box::pin(async move {
            let cur: i64 = txn
                .get(&key)
                .await?
                .map(|v| String::from_utf8(v).unwrap().parse().unwrap())
                .unwrap_or(0);
            let next = cur + 1;
            txn.put(&key, next.to_string().as_bytes());
            Ok(next)
        })
    })
    .await
    .unwrap();

    assert_eq!(new, 1);
    assert_eq!(be.get(&b("ctr")).await.unwrap(), Some(b("1")));
}

async fn txn_atomicity_on_abort(be: Arc<dyn KvBackend>) {
    // A transaction that returns an application error must apply no writes.
    let key = b("x");
    let result: Result<(), KvError> = run_txn(be.as_ref(), DEFAULT_MAX_RETRIES, move |txn| {
        let key = key.clone();
        Box::pin(async move {
            txn.put(&key, b"should-not-persist".as_ref());
            Err(KvError::Aborted("boom".into()))
        })
    })
    .await;

    assert!(matches!(result, Err(KvError::Aborted(_))));
    assert_eq!(be.get(&b("x")).await.unwrap(), None);
}

// ----- compare-and-set -----

async fn compare_and_set_semantics(be: Arc<dyn KvBackend>) {
    // CAS from absent -> value.
    assert!(be
        .compare_and_set(&b("cas"), None, Some(&b("v1")))
        .await
        .unwrap());
    assert_eq!(be.get(&b("cas")).await.unwrap(), Some(b("v1")));

    // CAS with wrong expected fails and does not write.
    assert!(!be
        .compare_and_set(&b("cas"), Some(&b("WRONG")), Some(&b("v2")))
        .await
        .unwrap());
    assert_eq!(be.get(&b("cas")).await.unwrap(), Some(b("v1")));

    // CAS with correct expected succeeds.
    assert!(be
        .compare_and_set(&b("cas"), Some(&b("v1")), Some(&b("v2")))
        .await
        .unwrap());
    assert_eq!(be.get(&b("cas")).await.unwrap(), Some(b("v2")));

    // CAS delete (new = None).
    assert!(be
        .compare_and_set(&b("cas"), Some(&b("v2")), None)
        .await
        .unwrap());
    assert_eq!(be.get(&b("cas")).await.unwrap(), None);
}

// ----- optimistic-concurrency conflict detection -----

async fn concurrent_write_conflict(be: Arc<dyn KvBackend>) {
    be.put(&b("k"), &b("0")).await.unwrap();

    // Open a transaction, read k, then let another writer bump k before commit.
    let mut txn = be.begin().await.unwrap();
    let _ = txn.get(&b("k")).await.unwrap();

    // Concurrent committed write to the same key.
    be.put(&b("k"), &b("1")).await.unwrap();

    // Our transaction now writes and must fail with Conflict.
    txn.put(&b("k"), &b("stale"));
    let err = txn.commit().await.unwrap_err();
    assert!(matches!(err, KvError::Conflict));
    assert!(err.is_retryable());

    // Store reflects only the concurrent write.
    assert_eq!(be.get(&b("k")).await.unwrap(), Some(b("1")));
}

async fn snapshot_read_does_not_conflict(be: Arc<dyn KvBackend>) {
    be.put(&b("s"), &b("0")).await.unwrap();
    be.put(&b("w"), &b("init")).await.unwrap();

    // A transaction snapshot-reads `s` (no conflict tracking), then a
    // concurrent writer bumps `s` before we commit.
    let mut txn = be.begin().await.unwrap();
    let seen = txn.snapshot_get(&b("s")).await.unwrap();
    assert_eq!(seen, Some(b("0")));

    be.put(&b("s"), &b("1")).await.unwrap();

    // We write to a DIFFERENT key and commit; because `s` was read via
    // snapshot_get it is not in the conflict set, so the commit succeeds even
    // though `s` changed concurrently.
    txn.put(&b("w"), &b("done"));
    txn.commit().await.unwrap();

    assert_eq!(be.get(&b("w")).await.unwrap(), Some(b("done")));
    assert_eq!(be.get(&b("s")).await.unwrap(), Some(b("1")));

    // Contrast: the same access pattern with a plain `get` MUST conflict.
    be.put(&b("s"), &b("0")).await.unwrap();
    let mut txn = be.begin().await.unwrap();
    let _ = txn.get(&b("s")).await.unwrap();
    be.put(&b("s"), &b("2")).await.unwrap();
    txn.put(&b("w"), &b("stale"));
    assert!(matches!(txn.commit().await, Err(KvError::Conflict)));
}

/// A concurrent delete of a read key must conflict, exactly like an overwrite
/// (a delete is a write; the reader depended on the key's existence).
async fn concurrent_delete_of_read_key_conflicts(be: Arc<dyn KvBackend>) {
    be.put(&b("d"), &b("live")).await.unwrap();

    // T1 reads d (adds it to the conflict set) but has not committed yet.
    let mut txn = be.begin().await.unwrap();
    assert_eq!(txn.get(&b("d")).await.unwrap(), Some(b("live")));

    // T2 concurrently deletes d and commits.
    be.delete(&b("d")).await.unwrap();

    // T1 writes an unrelated key and must conflict: d was changed concurrently.
    txn.put(&b("other"), &b("x"));
    let err = txn.commit().await.unwrap_err();
    assert!(matches!(err, KvError::Conflict));

    assert_eq!(be.get(&b("d")).await.unwrap(), None);
    assert_eq!(be.get(&b("other")).await.unwrap(), None);
}

/// Two blind writers (no reads) to the same key BOTH succeed: last-writer-wins.
/// This matches FDB, where a conflict requires a write range to intersect
/// another txn's *read* range; validating write sets here would make the memory
/// backend stricter than FDB.
async fn blind_write_write_does_not_conflict(be: Arc<dyn KvBackend>) {
    let mut t1 = be.begin().await.unwrap();
    let mut t2 = be.begin().await.unwrap();

    t1.put(&b("k"), &b("t1"));
    t2.put(&b("k"), &b("t2"));

    // Both commits succeed (no read-write conflict); the later commit wins.
    t1.commit().await.unwrap();
    t2.commit().await.unwrap();

    assert_eq!(be.get(&b("k")).await.unwrap(), Some(b("t2")));
}

/// Reads observe the begin snapshot: keys written by another txn after begin
/// are invisible to `get` / `snapshot_get` in the older txn.
async fn reads_observe_begin_snapshot(be: Arc<dyn KvBackend>) {
    be.put(&b("v"), &b("v0")).await.unwrap();

    let mut txn = be.begin().await.unwrap();

    // A concurrent writer changes v and creates a new key AFTER begin.
    be.put(&b("v"), &b("v1")).await.unwrap();
    be.put(&b("absent_key"), &b("now_here")).await.unwrap();

    // T1 still sees the begin-time snapshot.
    assert_eq!(txn.snapshot_get(&b("v")).await.unwrap(), Some(b("v0")));
    assert_eq!(txn.snapshot_get(&b("absent_key")).await.unwrap(), None);

    // Snapshot reads carry no conflict, so an unrelated write commits fine.
    txn.put(&b("unrelated"), &b("ok"));
    txn.commit().await.unwrap();
    assert_eq!(be.get(&b("unrelated")).await.unwrap(), Some(b("ok")));
}

// ----- error semantics / classification -----

fn error_classification() {
    assert!(KvError::Conflict.is_retryable());
    assert!(KvError::Timeout.is_retryable());
    // MaybeCommitted is NOT retryable: the write may already have applied.
    assert!(!KvError::MaybeCommitted.is_retryable());
    assert!(!KvError::Aborted("a".into()).is_retryable());
    assert!(!KvError::Backend("b".into()).is_retryable());
}

/// Runs the whole contract against a fresh backend produced by `factory`.
/// PR 3 can call this with an FDB-backed factory to reuse the suite verbatim.
async fn run_contract<F>(factory: F)
where
    F: Fn() -> Arc<dyn KvBackend>,
{
    point_get_put_delete(factory()).await;
    batch_get_delete(factory()).await;
    arbitrary_bytes_round_trip(factory()).await;
    distinct_invalid_utf8_keys_remain_distinct(factory()).await;
    multilingual_utf8_round_trip(factory()).await;
    unicode_normalization_is_not_implicit(factory()).await;
    txn_read_modify_write(factory()).await;
    txn_atomicity_on_abort(factory()).await;
    compare_and_set_semantics(factory()).await;
    concurrent_write_conflict(factory()).await;
    snapshot_read_does_not_conflict(factory()).await;
    concurrent_delete_of_read_key_conflicts(factory()).await;
    blind_write_write_does_not_conflict(factory()).await;
    reads_observe_begin_snapshot(factory()).await;
    error_classification();
}

// ===== memory backend bindings =====

#[tokio::test]
async fn memory_backend_satisfies_contract() {
    run_contract(|| Arc::new(MemoryBackend::new())).await;
}

#[tokio::test]
async fn memory_fault_injection_forces_error_and_retry() {
    let be = MemoryBackend::new();
    let faults = be.faults();

    // A one-shot injected conflict on commit is surfaced to the caller.
    be.put(&b("k"), &b("v")).await.unwrap();
    faults.fail_next(crate::kv::metrics::op::COMMIT, KvError::Conflict);
    let mut txn = be.begin().await.unwrap();
    txn.put(&b("k"), &b("v2"));
    assert!(matches!(txn.commit().await, Err(KvError::Conflict)));

    // run_txn transparently retries past a single injected conflict and then
    // succeeds on the second attempt.
    faults.reset();
    faults.fail_next(crate::kv::metrics::op::COMMIT, KvError::Conflict);
    let key = b("k");
    let out = run_txn(&be, DEFAULT_MAX_RETRIES, move |txn| {
        let key = key.clone();
        Box::pin(async move {
            txn.put(&key, b"final".as_ref());
            Ok(())
        })
    })
    .await;
    assert!(out.is_ok());
    assert_eq!(be.get(&b("k")).await.unwrap(), Some(b("final")));
}

#[tokio::test]
async fn memory_terminal_error_is_not_retried() {
    let be = MemoryBackend::new();
    let faults = be.faults();
    // Queue two terminal backend errors; run_txn must stop after the first.
    faults.fail_next(
        crate::kv::metrics::op::COMMIT,
        KvError::Backend("disk".into()),
    );
    faults.fail_next(
        crate::kv::metrics::op::COMMIT,
        KvError::Backend("disk".into()),
    );

    let out: Result<(), KvError> = run_txn(&be, DEFAULT_MAX_RETRIES, move |txn| {
        Box::pin(async move {
            txn.put(b"k".as_ref(), b"v".as_ref());
            Ok(())
        })
    })
    .await;
    assert!(matches!(out, Err(KvError::Backend(_))));

    // The second terminal error is still queued, so a second run_txn also
    // fails immediately — proving run_txn consumed exactly one, not retried.
    let out2: Result<(), KvError> = run_txn(&be, DEFAULT_MAX_RETRIES, move |txn| {
        Box::pin(async move {
            txn.put(b"k".as_ref(), b"v".as_ref());
            Ok(())
        })
    })
    .await;
    assert!(matches!(out2, Err(KvError::Backend(_))));

    // Both drained; the next commit succeeds.
    be.put(&b("k"), &b("v")).await.unwrap();
    assert_eq!(be.get(&b("k")).await.unwrap(), Some(b("v")));
}

#[tokio::test]
async fn memory_maybe_committed_is_not_auto_retried() {
    // MaybeCommitted must propagate, not be silently retried (the write may
    // already have applied).
    let be = MemoryBackend::new();
    let faults = be.faults();
    faults.fail_next(crate::kv::metrics::op::COMMIT, KvError::MaybeCommitted);

    let out: Result<(), KvError> = run_txn(&be, DEFAULT_MAX_RETRIES, move |txn| {
        Box::pin(async move {
            txn.put(b"k".as_ref(), b"v".as_ref());
            Ok(())
        })
    })
    .await;
    assert!(matches!(out, Err(KvError::MaybeCommitted)));
}

#[tokio::test]
async fn memory_apply_then_unknown_does_not_double_increment() {
    // FDB commit_unknown_result where the write landed: a run_txn increment
    // must not be retried, or the counter advances twice.
    let be = MemoryBackend::new();
    let faults = be.faults();
    be.put(&b("ctr"), &b("0")).await.unwrap();
    faults.apply_then_unknown_next(1);

    let key = b("ctr");
    let out: Result<i64, KvError> = run_txn(&be, DEFAULT_MAX_RETRIES, move |txn| {
        let key = key.clone();
        Box::pin(async move {
            let cur: i64 = txn
                .get(&key)
                .await?
                .map(|v| String::from_utf8(v).unwrap().parse().unwrap())
                .unwrap_or(0);
            txn.put(&key, (cur + 1).to_string().as_bytes());
            Ok(cur + 1)
        })
    })
    .await;

    // Unknown result surfaced, write applied exactly once (ctr == 1, not 2).
    assert!(matches!(out, Err(KvError::MaybeCommitted)));
    assert_eq!(be.get(&b("ctr")).await.unwrap(), Some(b("1")));
}

#[tokio::test]
async fn memory_apply_then_unknown_does_not_flip_successful_cas() {
    // A create-if-absent CAS whose commit lands but returns MaybeCommitted must
    // surface the unknown result, not be retried into a false `Ok(false)`.
    let be = MemoryBackend::new();
    let faults = be.faults();
    faults.apply_then_unknown_next(1);

    let out = be.compare_and_set(&b("cas"), None, Some(&b("v1"))).await;

    // Unknown result surfaced, value written exactly once.
    assert!(matches!(out, Err(KvError::MaybeCommitted)));
    assert_eq!(be.get(&b("cas")).await.unwrap(), Some(b("v1")));
}
