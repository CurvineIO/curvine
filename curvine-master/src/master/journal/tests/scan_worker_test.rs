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

#[test]
fn scan_with_ack_survives_a_retrying_entry() {
    with_scan_loader("retry-window", |loader, receiver| async move {
        let mut batch = JournalBatch::new(1);
        for (path, id, parent) in [("/applied", 5001, 0), ("/missing/child", 5002, 9999)] {
            batch.push(JournalEntry::Mkdir(MkdirEntry {
                op_id: id as u64,
                rpc_id: 0,
                path: path.to_string(),
                dir: crate::master::meta::inode::InodeDir::new(id, parent),
            }));
        }
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
fn no_worker_keeps_the_direct_scan_path() {
    with_scan_loader("direct", |mut loader, _receiver| async move {
        loader.has_apply_worker = false;
        loader
            .apply(true, ApplyMsg::new_scan(scan(0)))
            .await
            .unwrap();
    });
}
