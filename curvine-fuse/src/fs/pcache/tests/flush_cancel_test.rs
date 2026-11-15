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

//! Cancelling a write-back flush must keep the data and let a retry persist it.
//!
//! The release path may abandon an in-flight flush (cancellation, task abort,
//! release failure) at any point. Two invariants must survive that:
//!
//! 1. the bytes are still reachable afterwards - `has_unpersisted()` keeps
//!    reporting them, and the cache still accounts for their usage;
//! 2. a later flush persists *every* byte to the backend, not just the ranges
//!    that happened to be drained before the cancel.
//!
//! Both cases below use the real `FlushGuard` (the same guard production takes
//! via `take_all_guarded`) and a real `LocalWriter` backend, so the retry is
//! checked against actual file contents rather than a mock. They do **not**
//! distinguish "cancelled during write" from "cancelled while waiting for
//! publish": without a test hook inside `FuseWriter` the cancel point cannot be
//! placed that precisely, and pinning a timing guess would make the case flaky.
//! The stage-specific variants need the hook proposed to the fuse writer owner.

use super::*;
use crate::fs::FuseWriter;
use bytes::Bytes;
use curvine_client::unified::UnifiedWriter;
use curvine_config::FuseConf;
use curvine_fs_api::local::LocalWriter;
use curvine_fs_api::Path;
use curvine_model::{CreateFileOpts, OpenFlags};
use curvine_runtime::common::ByteUnit;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use curvine_runtime::sync::channel::AsyncChannel;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const PAGE: u64 = 4096;
const TEST_INO: u64 = 100;
static SEQ: AtomicU64 = AtomicU64::new(0);

fn bytes(b: &[u8]) -> Bytes {
    Bytes::copy_from_slice(b)
}

fn test_conf(page_size: u64) -> FuseConf {
    FuseConf {
        write_back_cache_type: "memory".to_string(),
        write_back_page_size: ByteUnit::new(page_size),
        write_back_cache_size: ByteUnit::new(page_size * 16),
        ..Default::default()
    }
}

/// Cache fixture over a real `CacheManager` (no `CurvineFileSystem` needed:
/// these cases only touch the page map, the guards and the writer).
/// `FuseMetrics` is normally initialised by `CurvineFileSystem::new`; this
/// fixture skips that, so initialise it once per test binary (`get_or_try_init`
/// makes repeated calls cheap and safe under parallel test threads).
fn ensure_metrics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        crate::fuse_metrics::FuseMetrics::ensure_init().unwrap();
    });
}

fn test_cache(conf: &FuseConf) -> Arc<InodeCache> {
    let (sender, receiver) = AsyncChannel::new(0).split();
    Box::leak(Box::new(receiver));
    let cm = Arc::new(CacheManager::new(conf, sender).unwrap());
    Arc::new(InodeCache::new(
        cm.clone(),
        TEST_INO,
        cm.page_size,
        OpenFlags::RDWR,
        CreateFileOpts::with_create(false),
        0,
    ))
}

/// Attach a real local-file writer so `flush()` has somewhere to persist to.
async fn attach_backend(
    cache: &Arc<InodeCache>,
    conf: &FuseConf,
    rt: Arc<Runtime>,
    name: &str,
) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cv_fuse_flush_cancel_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let backend = dir.join(name);
    let path = Path::from_str(backend.to_str().unwrap()).unwrap();
    let page_size = conf.write_back_page_size.as_byte() as usize;
    let writer = Arc::new(FuseWriter::new(
        conf,
        rt,
        UnifiedWriter::Local(LocalWriter::new(&path, page_size).unwrap()),
    ));
    *cache.writer.lock().await = Some(writer);
    backend
}

fn read_backend(path: &PathBuf) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// A guard dropped before commit (the cancelled-flush shape): the drained page
/// must be put back as dirty, usage must not leak, and a retry must persist all
/// bytes.
#[test]
fn cancelled_guard_keeps_the_data_and_a_retry_persists_every_byte() {
    ensure_metrics();
    let conf = test_conf(PAGE);
    let rt = Arc::new(AsyncRuntime::single());
    let rt_run = rt.clone();
    rt.block_on(async move {
        let cache = test_cache(&conf);
        let backend = attach_backend(&cache, &conf, rt_run, "cancelled_guard.bin").await;

        let payload = vec![0xAAu8; PAGE as usize];
        cache.write(0, bytes(&payload)).await.unwrap();
        assert!(
            cache.has_unpersisted(),
            "written data must start unpersisted"
        );
        let used_before = cache.cm.used();

        // Take the page as production does, then abandon it without committing.
        let guard = cache.take_all_guarded().unwrap();
        assert_eq!(
            guard.snapshot().len(),
            1,
            "the guard must really hold the drained page"
        );
        drop(guard);

        // Invariant 1: the bytes are still reachable.
        assert!(
            cache.has_unpersisted(),
            "cancelling a flush must leave the data unpersisted, not silently drop it"
        );
        assert_eq!(
            cache.cm.used(),
            used_before,
            "a cancelled flush must not leak or free page usage"
        );

        // Invariant 2: a retry persists every byte.
        cache.flush().await.unwrap();
        assert!(
            !cache.has_unpersisted(),
            "after a successful retry nothing may stay unpersisted"
        );
        let on_disk = read_backend(&backend);
        assert_eq!(
            on_disk.len(),
            payload.len(),
            "the retry must write the whole page, got {} of {} bytes",
            on_disk.len(),
            payload.len()
        );
        assert!(
            on_disk == payload,
            "the retried bytes must match what was written (first diff at {:?})",
            on_disk.iter().zip(payload.iter()).position(|(a, b)| a != b)
        );
    });
}

/// The same contract when the in-flight flush task is aborted: an aborted
/// publish must not lose the bytes, and the following flush must still be able
/// to persist them.
#[test]
fn aborted_flush_task_keeps_the_data_and_a_retry_persists_every_byte() {
    ensure_metrics();
    let conf = test_conf(PAGE);
    let rt = Arc::new(AsyncRuntime::single());
    let rt_run = rt.clone();
    rt.block_on(async move {
        let cache = test_cache(&conf);
        let backend = attach_backend(&cache, &conf, rt_run, "aborted_flush.bin").await;

        let payload = vec![0xBBu8; PAGE as usize];
        cache.write(0, bytes(&payload)).await.unwrap();
        assert!(cache.has_unpersisted());

        // Start a real flush and drop the future mid-flight. Dropping a future runs
        // the same guard-drop path production takes when a release abandons an
        // in-flight flush; polling it once first makes sure it really started.
        {
            let mut flush_fut = Box::pin(cache.flush());
            let started = std::future::poll_fn(|cx| {
                // One poll to get the flush into the guarded region, then abandon it.
                let _ = flush_fut.as_mut().poll(cx);
                std::task::Poll::Ready(())
            })
            .await;
            let () = started;
            drop(flush_fut);
        }

        assert!(
            cache.has_unpersisted(),
            "an aborted flush must leave the data unpersisted"
        );

        // Retry must persist everything, not only the pre-abort prefix.
        cache.flush().await.unwrap();
        assert!(!cache.has_unpersisted());
        let on_disk = read_backend(&backend);
        assert_eq!(
            on_disk.len(),
            payload.len(),
            "the retry after an abort must write the whole page, got {} of {} bytes",
            on_disk.len(),
            payload.len()
        );
        assert!(
            on_disk == payload,
            "the retried bytes after an abort must match (first diff at {:?})",
            on_disk.iter().zip(payload.iter()).position(|(a, b)| a != b)
        );
    });
}
