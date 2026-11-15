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

//! ABA on the page map: a stale `FlushGuard` must not touch a page that was
//! rebuilt under the same `page_id` after a `truncate`.
//!
//! `FlushGuard::Drop` -> `restore_flushed_dirty(snapshot)` and
//! `FlushGuard::commit` -> `finish_flush_ids(snapshot)` both look pages up by
//! `page_id` (`inode_cache.rs`). `truncate()` frees every page, so a later
//! write recreates the same id with different content. Without an identity or
//! generation check the stale guard's restoration/commit lands on the new page:
//! restoring re-dirties data that was already persisted (a phantom flush), and
//! committing clears the new page's `flushing` flag while its own IO is still
//! in flight.
//!
//! These cases pin both directions with a real cache (no mock backend): the
//! page map, the guards and the dirty accounting are the production ones.

use crate::fs::pcache::{CacheManager, InodeCache};
use bytes::Bytes;
use curvine_config::FuseConf;
use curvine_model::{CreateFileOpts, OpenFlags};
use curvine_runtime::common::ByteUnit;
use curvine_runtime::sync::channel::AsyncChannel;
use std::sync::Arc;

const PAGE: u64 = 4096;
const TEST_INO: u64 = 100;

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

/// Minimal cache fixture: an `InodeCache` over a real `CacheManager`, the same
/// shape the in-module tests in `inode_cache.rs` build. No `CurvineFileSystem`
/// is needed because these cases only exercise the page map and the guards.
fn test_cache(conf: &FuseConf) -> Arc<InodeCache> {
    let (sender, receiver) = AsyncChannel::new(0).split();
    // The cache manager owns the receiver through its channel; leaking it keeps
    // the sender usable for the lifetime of the test process (test-only).
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

/// `FlushGuard::Drop` on a stale guard must not re-dirty a page that was
/// already persisted after being rebuilt under the same page id.
#[tokio::test]
async fn stale_guard_restore_must_not_redirty_the_rebuilt_page() {
    let cache = test_cache(&test_conf(PAGE));

    // Old generation: page 0 must be a FULL page, because `take_dirty_guarded`
    // drains full pages only - a partial page would stay cached and the ABA
    // window would never open.
    let old_page = vec![0xAAu8; PAGE as usize];
    cache.write(0, bytes(&old_page)).await.unwrap();
    assert!(cache.has_unpersisted(), "old page must start dirty");
    let old_guard = cache.take_dirty_guarded().unwrap();
    assert_eq!(
        old_guard.snapshot().len(),
        1,
        "the guard must hold the drained page range (full page required)"
    );

    // The old page is freed; the same page id will be reused below.
    cache.truncate().await.unwrap();
    assert!(
        !cache.has_unpersisted(),
        "truncate must drop every page, including the one the guard drained"
    );

    // New generation: same page id, different payload, fully persisted.
    let new_page = vec![0xBBu8; PAGE as usize];
    cache.write(0, bytes(&new_page)).await.unwrap();
    let fresh_guard = cache.take_dirty_guarded().unwrap();
    fresh_guard.commit();
    assert!(
        !cache.has_unpersisted(),
        "the rebuilt page is persisted, nothing may stay unpersisted"
    );

    // Dropping the stale guard runs `restore_flushed_dirty` on the same
    // `page_id`. It must not resurrect the persisted new page as dirty.
    drop(old_guard);
    assert!(
        !cache.has_unpersisted(),
        "a stale guard's restore must not mark the rebuilt page dirty again \
         (phantom flush of already-persisted data)"
    );
    let got: Vec<u8> = cache
        .read(0, 4)
        .await
        .unwrap()
        .unwrap()
        .iter()
        .flat_map(|slice| slice.as_slice().to_vec())
        .collect();
    assert_eq!(
        &got[..],
        &[0xBBu8; 4],
        "a stale guard's restore must not rewrite the rebuilt page's payload"
    );
}

/// `FlushGuard::commit` on a stale guard must not clear the `flushing` flag of
/// a page that is currently in flight under the same page id.
#[tokio::test]
async fn stale_guard_commit_must_not_clear_the_rebuilt_pages_flushing() {
    let cache = test_cache(&test_conf(PAGE));

    let old_page = vec![0xAAu8; PAGE as usize];
    cache.write(0, bytes(&old_page)).await.unwrap();
    let old_guard = cache.take_dirty_guarded().unwrap();
    assert_eq!(
        old_guard.snapshot().len(),
        1,
        "the stale guard must really hold a drained full page"
    );
    cache.truncate().await.unwrap();

    // The rebuilt page is mid-flush: drained into its own guard, not committed.
    let new_page = vec![0xBBu8; PAGE as usize];
    cache.write(0, bytes(&new_page)).await.unwrap();
    let fresh_guard = cache.take_dirty_guarded().unwrap();
    assert_eq!(
        fresh_guard.snapshot().len(),
        1,
        "the rebuilt page must really be in flight"
    );
    assert!(
        cache.has_unpersisted(),
        "the rebuilt page is in flight and must count as unpersisted"
    );

    // Committing the stale guard runs `finish_flush_ids` on the same page id.
    // If it lands on the new page it clears that page's flushing flag while its
    // own IO is still outstanding.
    old_guard.commit();
    assert!(
        cache.has_unpersisted(),
        "a stale guard's commit must not clear the rebuilt page's flushing flag"
    );

    // Let the real in-flight guard restore its page, then check the payload is
    // still the new generation.
    drop(fresh_guard);
    let got: Vec<u8> = cache
        .read(0, 4)
        .await
        .unwrap()
        .unwrap()
        .iter()
        .flat_map(|slice| slice.as_slice().to_vec())
        .collect();
    assert_eq!(
        &got[..],
        &[0xBBu8; 4],
        "the rebuilt page must keep its own payload throughout the ABA window"
    );
}
