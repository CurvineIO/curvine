// Copyright 2026 OPPO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use curvine_config::ClusterConf;
use curvine_runtime::common::Utils;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn with_scan_loader<Fut: Future<Output = ()>>(
    name: &str,
    test: impl FnOnce(JournalLoader, AsyncReceiver<ApplyMsg>) -> Fut,
) {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("scan-worker-{}-{}", name, Utils::rand_str(8)));
    conf.journal.retry_interval_secs = 60;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let mut loader = journal.journal_loader();
    let (sender, receiver) = AsyncChannel::new(4).split();
    loader.sender = sender;
    loader.has_apply_worker = true;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), test(loader, receiver))
                .await
                .expect("scan worker scenario timed out");
        });
    }));
    // Abort the local test worker before shutting down the original system,
    // including when the scenario panics or times out.
    drop(runtime);
    journal.shutdown();
    outcome.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

fn scan(index: u64) -> AppliedIndex {
    AppliedIndex {
        index,
        term: 1,
        ..Default::default()
    }
}

#[test]
fn sync_scan_queues_through_worker_and_waits_for_ack() {
    with_scan_loader("queued", |loader, mut receiver| async move {
        let mut scan_fut = Box::pin(loader.apply(true, ApplyMsg::new_scan(scan(4))));
        assert!(futures::poll!(scan_fut.as_mut()).is_pending());
        let ApplyMsg::ScanWithAck((applied, tx)) = receiver.recv().await.unwrap() else {
            panic!("expected ScanWithAck");
        };
        assert_eq!(applied.index, 4);
        assert!(futures::poll!(scan_fut.as_mut()).is_pending());
        tx.send(Ok(())).unwrap();
        scan_fut.await.unwrap();
    });
}

#[test]
fn scan_error_propagates_to_the_caller() {
    with_scan_loader("error", |loader, mut receiver| async move {
        let mut scan_fut = Box::pin(loader.apply(true, ApplyMsg::new_scan(scan(4))));
        assert!(futures::poll!(scan_fut.as_mut()).is_pending());
        let ApplyMsg::ScanWithAck((_, tx)) = receiver.recv().await.unwrap() else {
            panic!("expected ScanWithAck");
        };
        tx.send(Err("injected scan failure".to_string().into()))
            .unwrap();
        assert!(scan_fut
            .await
            .unwrap_err()
            .to_string()
            .contains("injected scan failure"));
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn scan_with_ack_survives_a_retrying_entry() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    struct RuleCleanup(&'static FaultRuntime, &'static str);
    impl Drop for RuleCleanup {
        fn drop(&mut self) {
            let _ = self.0.remove(self.1);
        }
    }

    with_scan_loader("retry-window", |loader, receiver| async move {
        let child_name = format!("scan-retry-{}", Utils::rand_str(8));
        let faults = FaultRuntime::process();
        let rule_id = "scan-worker-transient-add";
        let rule = FaultRuleBuilder::named("master.metadata.add.before_commit")
            .matches("child_name", child_name.as_str())
            .unwrap()
            .times(1)
            .unwrap()
            .return_error("transient add failure")
            .unwrap();
        faults.configure(rule_id, rule).unwrap();
        let rule_guard = RuleCleanup(faults, rule_id);
        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::Mkdir(MkdirEntry {
            op_id: 5001,
            rpc_id: 0,
            path: format!("/{child_name}"),
            dir: crate::master::meta::inode::InodeDir::new(5001, 0),
        }));
        loader
            .sender
            .send(ApplyMsg::new_entry(Entry {
                index: 1,
                term: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let worker = tokio::spawn(loader.clone().run_apply(receiver));
        // Exclude safe mode, which also rejects snapshots: the remaining
        // cause is a pending retry whose 60s delay exceeds the test timeout.
        let snapshot = loader.create_snapshot().await;
        assert_eq!(rule_guard.0.rule(rule_id).unwrap().unwrap().executions, 1);
        assert!(!loader.recovery.is_safe_mode());
        assert!(matches!(
            snapshot,
            Err(RaftError::Raft(error)) if matches!(
                error.source,
                raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable)
            )
        ));
        assert_eq!(loader.get_fsm_state().applied.index, 0);
        loader
            .apply(true, ApplyMsg::new_scan(scan(0)))
            .await
            .unwrap();
        let (tx, rx) = CallChannel::channel();
        loader.sender.send(ApplyMsg::Shutdown(tx)).await.unwrap();
        rx.receive().await.unwrap();
        worker.await.unwrap();
    });
}

#[test]
fn leader_bookkeeping_does_not_wait_for_namespace_writer() {
    with_scan_loader("bookkeeping", |loader, _receiver| async move {
        let mut batch = JournalBatch::new(1);
        let entry = Entry {
            index: 8192,
            term: 1,
            data: batch.encode().unwrap(),
            context: SerdeUtils::serialize(&7_i64).unwrap().into(),
            ..Default::default()
        };
        let namespace = loader.fs_dir.clone();
        let held = namespace.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(loader.apply0(true, &entry, false));
            drop(runtime);
            let _ = done_tx.send(result);
        });
        let early = done_rx.recv_timeout(Duration::from_secs(2));
        let progressed = matches!(&early, Ok(Ok(())));
        drop(held);
        let result = match early {
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                done_rx.recv_timeout(Duration::from_secs(2))
            }
            completed => completed,
        };
        if result.is_ok() {
            worker.join().unwrap();
        }
        assert!(
            progressed,
            "leader bookkeeping waited for the namespace lock"
        );
        result
            .expect("bookkeeping did not finish after release")
            .unwrap();
        let dir = namespace.view().unwrap();
        let record = dir.store.store.get_applied_req(7).unwrap().unwrap();
        assert_eq!((record.index, record.term), (8192, 1));
    });
}

#[test]
fn no_worker_keeps_the_direct_scan_path() {
    with_scan_loader("direct", |mut loader, _receiver| async move {
        loader.has_apply_worker = false;
        loader
            .apply(true, ApplyMsg::new_scan(scan(0)))
            .await
            .unwrap();
    });
}
