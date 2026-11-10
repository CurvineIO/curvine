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

// Lifecycle tests for the S6-a namespace view holder: hot guards block cold
// exclusivity until released (including cross-thread), cold windows reject
// new views, timeouts and panics never reopen a half-mutated view, and every
// exit path restores access or fails closed.

use crate::master::journal::JournalWriter;
use crate::master::meta::fs_dir_handle::{ColdError, FsDirHandle, NamespaceError};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::FsDir;
use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
use crate::master::quota::eviction::EvictionConf;
use crate::master::Master;
use crate::master::MasterMonitor;
use curvine_config::ClusterConf;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::Utils;
use curvine_runtime::sync::StateCtl;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

/// Builds a real `FsDir` through the same construction path as the journal
/// system's `build_fs_parts`, in testing mode (no journal threads), and hands
/// the caller the journal writer for fence assertions.
fn build(name: &str) -> (FsDirHandle, Arc<JournalWriter>) {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("fs-dir-handle-{}-{}", name, Utils::rand_str(8)));
    let rt = conf.journal.create_runtime();
    let monitor = MasterMonitor::new(StateCtl::new(0), StateCtl::new(0));
    let client = RaftClient::from_conf(rt, &conf.journal);
    let writer =
        Arc::new(JournalWriter::new(true, client, &conf.journal, monitor.recovery()).unwrap());
    let ttl_bucket_list =
        Arc::new(TtlBucketList::new(conf.master.ttl_bucket_interval_ms() as i64).unwrap());
    let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(EvictionConf::from_conf(&conf)));
    let fs_dir = FsDir::new(&conf, writer.clone(), ttl_bucket_list, evictor).unwrap();
    (FsDirHandle::new(fs_dir), writer)
}

#[test]
fn existing_view_blocks_cold_until_released() {
    let (handle, _) = build("held-view");
    let held = handle.view().unwrap();
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(50)),
        Err(ColdError::DrainTimeout)
    ));
    // The timed-out cold window restored access to the untouched view.
    assert!(handle.view().is_ok());
    drop(held);
    let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    cold.view().store.ensure_available().unwrap();
    drop(cold);
    assert!(handle.view().is_ok());
}

#[test]
fn cross_thread_view_blocks_cold_until_release() {
    let (handle, _) = build("cross-thread");
    let handle = Arc::new(handle);
    let reader_handle = Arc::clone(&handle);
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let reader = std::thread::spawn(move || {
        let view = reader_handle.view().unwrap();
        held_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(view);
    });
    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // A live guard on another thread blocks the cold owner.
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(50)),
        Err(ColdError::DrainTimeout)
    ));
    release_tx.send(()).unwrap();
    reader.join().unwrap();
    // Only after the release does exclusivity succeed.
    let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    cold.view().store.ensure_available().unwrap();
    drop(cold);
    assert!(handle.view().is_ok());
}

#[test]
fn cold_window_rejects_new_views_and_second_owner() {
    let (handle, _) = build("cold-window");
    let cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    assert!(matches!(handle.view(), Err(NamespaceError::Busy)));
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(10)),
        Err(ColdError::Busy)
    ));
    drop(cold);
    assert!(handle.view().is_ok());
}

#[test]
fn invalid_timeout_touches_nothing() {
    let (handle, _) = build("invalid-timeout");
    assert!(matches!(
        handle.begin_cold(Duration::MAX),
        Err(ColdError::InvalidTimeout)
    ));
    // The old view is still readable: no slot was emptied by the overflow.
    assert!(handle.view().is_ok());
}

#[test]
fn cold_error_exit_restores_the_same_view() {
    let (handle, _) = build("error-exit");
    let next_id = handle.view().unwrap().last_inode_id() + 1;
    let result: Result<(), ()> = (|| {
        let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
        cold.view().update_last_inode_id(next_id).unwrap();
        Err(())
    })();
    assert!(result.is_err());
    assert_eq!(handle.view().unwrap().last_inode_id(), next_id);
}

#[test]
fn panic_in_cold_window_fails_closed() {
    let (handle, writer) = build("panic-fail-closed");
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
        cold.view();
        panic!("injected cold-window panic");
    }));
    assert!(result.is_err());
    // Fail-closed: reads are rejected and the journal fence engaged, so the
    // possibly half-mutated view is never handed back out.
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
    assert!(matches!(
        handle.begin_cold(Duration::from_secs(1)),
        Err(ColdError::Unavailable)
    ));
    assert!(writer.ensure_metadata_current().is_err());
}

#[test]
fn explicit_failure_is_not_a_busy_cold_window() {
    let (handle, writer) = build("explicit-fail-closed");
    handle
        .begin_cold(Duration::from_secs(1))
        .unwrap()
        .fail_closed();
    assert!(matches!(handle.view(), Err(NamespaceError::Unavailable)));
    assert!(matches!(
        handle.begin_cold(Duration::from_secs(1)),
        Err(ColdError::Unavailable)
    ));
    assert!(writer.ensure_metadata_current().is_err());
}

#[test]
fn cold_guard_leaves_no_stale_strong_reference() {
    let (handle, _) = build("no-stale");
    let mut cold = handle.begin_cold(Duration::from_secs(1)).unwrap();
    cold.view().store.ensure_available().unwrap();
    drop(cold);
    assert!(handle.view().is_ok());
    // A second cold run still proves unique ownership without waiting for
    // any timeout: no dropped guard leaked an `Arc`.
    assert!(matches!(
        handle.begin_cold(Duration::from_millis(20)),
        Ok(_)
    ));
}
