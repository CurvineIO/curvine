use super::*;
use crate::master::journal::{CacheInvalidationEntry, JournalEntry};
use crate::master::Master;
use curvine_config::RaftPeer;

#[test]
#[allow(clippy::field_reassign_with_default)]
fn failed_propose_fences_writer_and_retains_batch_for_discard() {
    Master::init_test_metrics();
    let mut conf = JournalConf::default();
    conf.hostname = "127.0.0.1".to_string();
    conf.rpc_port = 1;
    conf.journal_addrs = vec![RaftPeer::new(1, "127.0.0.1", 1)];
    conf.io_threads = 1;
    conf.worker_threads = 1;
    conf.conn_timeout_ms = 200;
    conf.io_timeout_ms = 200;
    conf.conn_retry_max_duration_ms = 0;
    conf.rpc_retry_max_duration_ms = 0;
    conf.conn_retry_min_sleep_ms = 1;
    conf.conn_retry_max_sleep_ms = 1;
    conf.rpc_retry_min_sleep_ms = 1;
    conf.rpc_retry_max_sleep_ms = 1;
    conf.writer_flush_batch_size = 1;
    conf.writer_flush_batch_ms = 60_000;

    let rt = conf.create_runtime();
    let client = RaftClient::from_conf(rt, &conf);
    let recovery = RecoveryGate::new();
    let mut task = SenderTask::new(client, &conf, 7, recovery).expect("sender");
    let (completion, waiter) = std::sync::mpsc::sync_channel(1);
    let error = task
        .handle(Some(QueuedJournalEntry {
            entry: JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 3,
                rpc_id: 1,
                inodes: vec![],
            }),
            completion: Some(completion),
        }))
        .expect_err("propose to a closed port must fail");

    assert!(
        classify_apply_error(&error.to_string()) == ApplyDisposition::Retry,
        "connection failure must stay retryable: {error}"
    );
    assert_eq!(task.batch.len(), 1, "failed flush must keep the batch");
    assert_eq!(task.batch.seq_id, 7, "failed flush must not roll the batch");
    assert!(task.recovery.rejects_writes());
    let (_sender, receiver) = curvine_runtime::sync::channel::BlockingChannel::new(0).split();
    discard_unproposed(&mut task.batch, &receiver, &mut task.completions);
    assert!(matches!(
        waiter.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Disconnected)
    ));
}

#[test]
fn safe_mode_discards_unproposed_entries() {
    use curvine_runtime::sync::channel::BlockingChannel;

    let (sender, receiver) = BlockingChannel::new(0).split();
    let mut batch = JournalBatch::new(4);
    batch.push(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
        op_id: 1,
        rpc_id: 1,
        inodes: vec![],
    }));
    sender
        .send(QueuedJournalEntry {
            entry: JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 2,
                rpc_id: 2,
                inodes: vec![],
            }),
            completion: None,
        })
        .expect("enqueue");

    let (completion, waiter) = std::sync::mpsc::sync_channel(1);
    let mut completions = vec![completion];
    discard_unproposed(&mut batch, &receiver, &mut completions);

    assert!(
        matches!(
            waiter.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Disconnected)
        ),
        "discard must release the commit waiter with an error"
    );
    assert!(batch.is_empty(), "the unproposed batch must be dropped");
    assert_eq!(batch.seq_id, 5);
    assert!(
        receiver.try_recv().is_err(),
        "queued entries must not remain to be flushed later"
    );
}

#[test]
fn sender_exits_in_safe_mode_and_releases_queued_waiters() {
    use curvine_runtime::sync::channel::BlockingChannel;

    Master::init_test_metrics();
    let conf = JournalConf::default();
    let client = RaftClient::from_conf(conf.create_runtime(), &conf);
    let recovery = RecoveryGate::new();
    recovery.enter_safe_mode("test");
    let task = SenderTask::new(client, &conf, 0, recovery).unwrap();
    let (sender, receiver) = BlockingChannel::new(0).split();
    let (completion, waiter) = std::sync::mpsc::sync_channel(1);
    sender
        .send(QueuedJournalEntry {
            entry: JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 1,
                rpc_id: 1,
                inodes: vec![],
            }),
            completion: Some(completion),
        })
        .unwrap();
    let (finished, done) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        SenderTask::loop0(receiver, Duration::from_millis(10), task);
        finished.send(()).unwrap();
    });
    assert!(matches!(
        waiter.recv_timeout(Duration::from_secs(2)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    ));
    assert!(done.recv_timeout(Duration::from_secs(2)).is_ok());
    assert!(sender
        .send(QueuedJournalEntry {
            entry: JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 2,
                rpc_id: 2,
                inodes: vec![],
            }),
            completion: None,
        })
        .is_err());
}
