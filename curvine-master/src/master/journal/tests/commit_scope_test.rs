use super::{
    CommitScopeEntry, JournalCommitScope, JournalWriter, RecoveryGate, JOURNAL_COMMIT_SCOPES,
};
use crate::master::journal::JournalSystem;
use crate::master::Master;
use curvine_config::{ClusterConf, JournalConf};
use curvine_raft::raft::RaftClient;
use std::sync::mpsc;
use std::time::Duration;

fn writer_with_commit_receipts() -> JournalWriter {
    Master::init_test_metrics();
    let conf = ClusterConf {
        testing: true,
        format_master: true,
        journal: JournalConf {
            enable: true,
            writer_channel_size: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let client = RaftClient::from_conf(conf.journal.create_runtime(), &conf.journal);
    let mut writer = JournalWriter::new(true, client, &conf.journal, RecoveryGate::new()).unwrap();
    writer.require_raft_commit = true;
    writer
}

#[test]
fn commit_scope_is_thread_bound() {
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}
    let _ = <JournalCommitScope as AmbiguousIfSend<_>>::check;

    trait AmbiguousIfSync<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
    let _ = <JournalCommitScope as AmbiguousIfSync<_>>::check;
}

#[test]
fn failed_commit_waits_for_remaining_entries_before_leaving_scope() {
    let (first, first_waiter) = mpsc::sync_channel(1);
    let (second, second_waiter) = mpsc::sync_channel(1);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    drop(first);
    let worker = std::thread::spawn(move || {
        JOURNAL_COMMIT_SCOPES.with(|scopes| {
            scopes.borrow_mut().push(CommitScopeEntry {
                writer: std::ptr::null(),
                id: 0,
                waiters: vec![first_waiter, second_waiter],
            });
        });
        let scope = JournalCommitScope {
            id: Some(0),
            writer: None,
            _thread_bound: std::marker::PhantomData,
        };
        started_tx.send(()).unwrap();
        done_tx.send(scope.wait().is_err()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let returned_early = done_rx.recv_timeout(Duration::from_millis(200)).ok();
    second.send(()).unwrap();
    let failed =
        returned_early.unwrap_or_else(|| done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
    worker.join().unwrap();
    assert!(
        returned_early.is_none(),
        "scope returned before all entries completed"
    );
    assert!(failed, "scope must retain the first commit error");
}

#[test]
fn dropping_scope_with_unconfirmed_entry_fences_metadata() {
    let writer = writer_with_commit_receipts();
    let scope = writer.begin_commit_scope();
    writer
        .enqueue_and_wait(super::JournalEntry::CacheInvalidation(
            super::CacheInvalidationEntry {
                op_id: 1,
                rpc_id: 0,
                inodes: vec![],
            },
        ))
        .unwrap();
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    drop(scope);
    assert!(writer.ensure_metadata_current().is_err());
    drop(queued);
}

#[test]
fn unscoped_failed_commit_fences_metadata() {
    let writer = std::sync::Arc::new(writer_with_commit_receipts());
    let worker_writer = writer.clone();
    let worker = std::thread::spawn(move || {
        worker_writer.enqueue_and_wait(super::JournalEntry::CacheInvalidation(
            super::CacheInvalidationEntry {
                op_id: 1,
                rpc_id: 0,
                inodes: vec![],
            },
        ))
    });
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    drop(queued);
    assert!(worker.join().unwrap().is_err());
    assert!(writer.recovery.is_safe_mode());
    assert!(writer.ensure_metadata_current().is_err());
}

#[test]
fn unscoped_enqueue_failure_fences_metadata() {
    let mut writer = writer_with_commit_receipts();
    drop(writer.receiver.take());
    let result = writer.enqueue_and_wait(super::JournalEntry::CacheInvalidation(
        super::CacheInvalidationEntry {
            op_id: 1,
            rpc_id: 0,
            inodes: vec![],
        },
    ));
    assert!(result.is_err());
    assert!(writer.recovery.is_safe_mode());
    assert!(writer.ensure_metadata_current().is_err());
}

#[test]
fn rejected_post_mutation_enqueue_enters_safe_mode() {
    let writer = writer_with_commit_receipts();
    writer.recovery.mark_writer_failed();
    let result = writer.enqueue_and_wait(super::JournalEntry::CacheInvalidation(
        super::CacheInvalidationEntry {
            op_id: 1,
            rpc_id: 0,
            inodes: vec![],
        },
    ));
    assert!(result.is_err());
    assert!(writer.recovery.is_safe_mode());
}
