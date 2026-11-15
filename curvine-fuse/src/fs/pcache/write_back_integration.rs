// Copyright 2025 OPPO.
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

use crate::fs::operator::Write;
use crate::fs::pcache::{CacheManager, InodeCache, WriteBackWork};
use crate::fs::state::{FileHandle, NodeState};
use crate::fs::{CurvineFileSystem, FuseWriter};
use crate::raw::fuse_abi::{fuse_in_header, fuse_write_in};
use crate::session::{FuseResponse, FuseTask};
use crate::FUSE_ROOT_ID;
use bytes::Bytes;
use curvine_client::unified::UnifiedWriter;
use curvine_config::{ClusterConf, FuseConf};
use curvine_fs_api::local::LocalWriter;
use curvine_fs_api::Path;
use curvine_model::{CreateFileOpts, OpenFlags};
use curvine_runtime::common::{ByteUnit, DurationUnit};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use curvine_runtime::sync::channel::AsyncChannel;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const PAGE_SIZE: u64 = 4096;
// Must stay below meta_cache_ttl/2 (default 60s -> 30s) after fuse.init().
const LONG_DELAY_MS: u64 = 10_000;

static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

struct WriteBackTestContext {
    rt: Arc<Runtime>,
    state: Arc<NodeState>,
    cm: Arc<CacheManager>,
    conf: FuseConf,
    work_dir: PathBuf,
}

impl WriteBackTestContext {
    fn new(rt: Arc<Runtime>, page_size: u64, cache_size: u64) -> Self {
        Self::new_with(
            rt,
            page_size,
            cache_size,
            "memory",
            LONG_DELAY_MS,
            LONG_DELAY_MS,
        )
    }

    fn new_with(
        rt: Arc<Runtime>,
        page_size: u64,
        cache_size: u64,
        cache_type: &str,
        flush_delay_ms: u64,
        tick_interval_ms: u64,
    ) -> Self {
        let work_dir = std::env::temp_dir().join(format!(
            "cv_fuse_write_back_test_{}_{}",
            std::process::id(),
            TEST_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&work_dir).unwrap();
        let disk_dir = work_dir.join("disk_cache");

        let mut fuse = FuseConf {
            enable_write_back: true,
            enable_meta_cache: true,
            write_back_cache_type: cache_type.to_string(),
            write_back_disk_dirs: vec![disk_dir.to_string_lossy().to_string()],
            write_back_page_size: ByteUnit::new(page_size),
            write_back_cache_size: ByteUnit::new(cache_size),
            write_back_flush_delay: DurationUnit::new(flush_delay_ms),
            write_back_tick_interval: DurationUnit::new(tick_interval_ms),
            write_back_max_concurrent: 4,
            metrics_enabled: false,
            ..Default::default()
        };
        // Populate meta/node cache TTL Duration fields used by CacheManager::check_conf.
        fuse.init().unwrap();

        let cluster = ClusterConf {
            fuse,
            ..Default::default()
        };
        let fs = CurvineFileSystem::new(cluster, rt.clone()).unwrap();
        let state = fs.state().clone();
        let cm = state.cache_manager();
        let conf = state.cluster_conf().fuse.clone();
        Self {
            rt,
            state,
            cm,
            conf,
            work_dir,
        }
    }

    async fn create_file(&self, name: &str) -> Arc<FileHandle> {
        let flags = (OpenFlags::RDWR | OpenFlags::CREAT).value();
        let opts = CreateFileOpts::with_create(false);
        self.state
            .fs_create(FUSE_ROOT_ID, name, flags, opts)
            .await
            .unwrap()
    }

    async fn inject_backend(&self, ino: u64, file_name: &str) -> (Arc<FuseWriter>, PathBuf) {
        let backend_path = self.work_dir.join(file_name);
        let path = Path::from_str(backend_path.to_str().unwrap()).unwrap();
        let cache = self.cm.get_cache(ino).await.unwrap();
        let page_size = self.conf.write_back_page_size.as_byte() as usize;
        let writer = Arc::new(FuseWriter::new(
            &self.conf,
            self.rt.clone(),
            UnifiedWriter::Local(LocalWriter::new(&path, page_size).unwrap()),
        ));
        *cache.writer.lock().await = Some(writer.clone());
        (writer, backend_path)
    }

    async fn write_via_fuse(&self, handle: &Arc<FileHandle>, off: u64, data: &[u8]) {
        let header = fuse_in_header {
            nodeid: handle.ino(),
            pid: 1,
            ..Default::default()
        };
        let arg = fuse_write_in {
            fh: handle.fh(),
            offset: off,
            size: data.len() as u32,
            ..Default::default()
        };
        let op = Write {
            header: &header,
            arg: &arg,
            data: Bytes::copy_from_slice(data),
        };
        let reply = self.new_reply(off);
        handle.write(op, reply).await.unwrap();
    }

    fn new_reply(&self, unique: u64) -> FuseResponse {
        let (tx, mut rx) = AsyncChannel::<FuseTask>::new(16).split();
        self.rt
            .spawn(async move { while rx.recv().await.is_some() {} });
        FuseResponse::new_reply(unique, tx, false, None)
    }

    async fn read_cache(&self, cache: &Arc<InodeCache>, off: u64, len: usize) -> Vec<u8> {
        let slices = cache
            .read(off, len)
            .await
            .unwrap()
            .expect("cache read should hit");
        let mut buf = Vec::with_capacity(len);
        for slice in slices {
            buf.extend_from_slice(slice.as_slice());
        }
        buf
    }
}

impl Drop for WriteBackTestContext {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.work_dir);
    }
}

fn pattern_data(len: usize) -> Vec<u8> {
    let mut data = vec![0u8; len];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    data
}

fn pattern_data_at(off: u64, len: usize) -> Vec<u8> {
    let mut data = vec![0u8; len];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((off as usize + i) % 251) as u8;
    }
    data
}

fn assert_backend_eq(backend: &PathBuf, expected: &[u8]) {
    let actual = std::fs::read(backend).unwrap();
    assert_bytes_eq(&actual, expected, &format!("backend {}", backend.display()));
}

fn assert_bytes_eq(actual: &[u8], expected: &[u8], what: &str) {
    if actual == expected {
        return;
    }
    match actual.iter().zip(expected.iter()).position(|(a, b)| a != b) {
        Some(i) => panic!(
            "{} mismatch at offset {}: got 0x{:02x}, want 0x{:02x} (got len={}, want len={})",
            what,
            i,
            actual[i],
            expected[i],
            actual.len(),
            expected.len()
        ),
        None => panic!(
            "{} mismatch: got len={}, want len={}",
            what,
            actual.len(),
            expected.len()
        ),
    }
}

#[test]
fn write_back_small_file_roundtrip() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("small.bin").await;
        let ino = handle.ino();
        let (_writer, backend) = ctx.inject_backend(ino, "backend_small.bin").await;

        let data = pattern_data(100);
        ctx.write_via_fuse(&handle, 0, &data).await;

        assert_eq!(ctx.cm.used(), PAGE_SIZE);
        assert_eq!(ctx.cm.get_cache(ino).await.unwrap().len(), 100);
        assert_eq!(
            std::fs::metadata(&backend).unwrap().len(),
            0,
            "no backend IO before commit"
        );

        ctx.cm.commit_all(false).await.unwrap();
        assert_eq!(
            std::fs::metadata(&backend).unwrap().len(),
            0,
            "partial page must not be flushed by commit_all"
        );

        let cache = ctx.cm.get_cache(ino).await.unwrap();
        cache.flush().await.unwrap();

        assert_backend_eq(&backend, &data);
        assert_eq!(
            ctx.cm.used(),
            PAGE_SIZE,
            "partial page must be retained after flush"
        );
    });
}

#[test]
fn write_back_small_file_cross_page_with_hole() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("hole.bin").await;
        let (_writer, backend) = ctx.inject_backend(handle.ino(), "backend_hole.bin").await;

        let seg_a = vec![0x5a; 50];
        let seg_b = pattern_data(200);
        ctx.write_via_fuse(&handle, 100, &seg_a).await;
        ctx.write_via_fuse(&handle, 4000, &seg_b).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_eq!(cache.len(), 4200);

        cache.flush().await.unwrap();

        let mut expected = vec![0u8; 4200];
        expected[100..150].copy_from_slice(&seg_a);
        expected[4000..4200].copy_from_slice(&seg_b);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_large_file_sequential() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    const TOTAL: usize = 2 * 1024 * 1024;
    const CHUNK: usize = 128 * 1024;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 8192);
    rt.block_on(async move {
        let handle = ctx.create_file("large_seq.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_large_seq.bin")
            .await;

        let expected = pattern_data(TOTAL);
        let mut off = 0usize;
        while off < TOTAL {
            ctx.write_via_fuse(&handle, off as u64, &expected[off..off + CHUNK])
                .await;
            off += CHUNK;
        }

        assert_eq!(ctx.cm.used(), TOTAL as u64);
        assert_eq!(std::fs::metadata(&backend).unwrap().len(), 0);

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        assert_backend_eq(&backend, &expected);
        assert_eq!(ctx.cm.used(), 0, "full pages must be reclaimed after flush");
    });
}

#[test]
fn write_back_large_file_random_offsets() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    const FILE_LEN: usize = 5 * 1024 * 1024;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 8192);
    rt.block_on(async move {
        let handle = ctx.create_file("large_random.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_large_random.bin")
            .await;

        let mut expected = vec![0u8; FILE_LEN];
        let segs = [
            (0usize, 100usize, 0x11u8),
            (1024 * 1024, 4096, 0x22),
            (2 * 1024 * 1024 + 3072, 1024, 0x33),
            (5 * 1024 * 1024 - 1, 1, 0x44),
        ];
        for (off, len, byte) in segs {
            let data = vec![byte; len];
            expected[off..off + len].copy_from_slice(&data);
            ctx.write_via_fuse(&handle, off as u64, &data).await;
        }

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_eq!(cache.len(), FILE_LEN as u64);

        cache.flush().await.unwrap();

        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_cache_read_before_and_after_flush() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("read.bin").await;
        let (_writer, backend) = ctx.inject_backend(handle.ino(), "backend_read.bin").await;

        let data = pattern_data(1024);
        ctx.write_via_fuse(&handle, 0, &data).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_bytes_eq(&ctx.read_cache(&cache, 0, 1024).await, &data, "cache read");

        cache.flush().await.unwrap();

        assert_backend_eq(&backend, &data);
        assert_bytes_eq(&ctx.read_cache(&cache, 0, 1024).await, &data, "cache read");
    });
}

#[test]
fn write_back_worker_flush_reclaims_clean_pages_and_keeps_reads_correct() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("worker_flush.bin").await;
        let ino = handle.ino();
        let (writer, backend) = ctx.inject_backend(ino, "backend_worker_flush.bin").await;

        let data = pattern_data(PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &data).await;

        let flushed = WriteBackWork::execute(&ctx.cm, ino, 0).await;
        assert_eq!(
            flushed, PAGE_SIZE,
            "worker flush should report the full page"
        );

        let cache = ctx.cm.get_cache(ino).await.unwrap();
        assert_eq!(
            cache.page_len(),
            0,
            "worker flush must reclaim now-clean pages"
        );

        // A later read misses the reclaimed pages and republishes through the
        // writer, which must still surface the flushed bytes from the backend.
        let published = cache
            .publish_read_range(0, PAGE_SIZE as usize, 0)
            .await
            .unwrap();
        assert!(
            published.is_some(),
            "read after reclaim should publish a snapshot"
        );

        writer.flush(None).await.unwrap();
        assert_backend_eq(&backend, &data);
    });
}

#[test]
fn write_back_dirty_read_range_snapshot_tracks_backend_versions() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("flush_range.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_flush_range.bin")
            .await;

        ctx.write_via_fuse(&handle, 0, b"head").await;
        ctx.write_via_fuse(&handle, PAGE_SIZE, b"tail").await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        let first_ver = cache
            .publish_read_range(0, 4, 0)
            .await
            .unwrap()
            .expect("first range flush should publish");

        let unchanged_ver = cache.publish_read_range(0, 4, first_ver).await.unwrap();
        assert_eq!(unchanged_ver, None);

        let second_ver = cache
            .publish_read_range(PAGE_SIZE, 4, first_ver)
            .await
            .unwrap()
            .expect("second range flush should publish");

        cache.flush().await.unwrap();
        let after_global_ver = cache.publish_read_range(0, 4, second_ver).await.unwrap();
        assert_eq!(after_global_ver, None);

        let mut expected = vec![0; PAGE_SIZE as usize + 4];
        expected[..4].copy_from_slice(b"head");
        expected[PAGE_SIZE as usize..].copy_from_slice(b"tail");
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_complete_finalizes_backend() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("complete.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_complete.bin")
            .await;

        let data = pattern_data(2048);
        ctx.write_via_fuse(&handle, 0, &data).await;

        handle.complete(None).await.unwrap();
        ctx.cm.commit_all(false).await.unwrap();

        assert_backend_eq(&backend, &data);
    });
}

#[test]
fn write_back_evicts_on_cache_pressure() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    const CACHE_SIZE: u64 = 512 * 1024;
    const TOTAL: usize = 512 * 1024;
    const CHUNK: usize = 128 * 1024;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, CACHE_SIZE);
    rt.block_on(async move {
        let handle = ctx.create_file("evict.bin").await;
        let (writer, backend) = ctx.inject_backend(handle.ino(), "backend_evict.bin").await;

        let expected = pattern_data(TOTAL);
        let mut off = 0usize;
        while off < TOTAL {
            ctx.write_via_fuse(&handle, off as u64, &expected[off..off + CHUNK])
                .await;
            off += CHUNK;
        }

        assert!(ctx.cm.need_evict());
        assert_eq!(ctx.cm.used(), TOTAL as u64);

        WriteBackWork::evict_inodes(&ctx.cm).await;
        writer.flush(None).await.unwrap();

        assert_backend_eq(&backend, &expected);
        assert_eq!(ctx.cm.used(), 0, "evicted full pages must be reclaimed");
    });
}

#[test]
fn write_back_partial_flush_then_append() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 8192);
    rt.block_on(async move {
        let handle = ctx.create_file("partial.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_partial.bin")
            .await;

        let first = pattern_data_at(0, PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &first).await;

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();
        assert_backend_eq(&backend, &first);
        assert_eq!(
            ctx.cm.used(),
            0,
            "full page must be reclaimed after first commit"
        );

        let second = pattern_data_at(PAGE_SIZE, PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, PAGE_SIZE, &second).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert!(
            cache.read(0, 1).await.unwrap().is_none(),
            "flushed region must miss the page cache"
        );
        assert_bytes_eq(
            &ctx.read_cache(&cache, PAGE_SIZE, PAGE_SIZE as usize).await,
            &second,
            "unflushed region must still hit the page cache",
        );

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        let mut expected = vec![0u8; (PAGE_SIZE * 2) as usize];
        expected[..PAGE_SIZE as usize].copy_from_slice(&first);
        expected[PAGE_SIZE as usize..].copy_from_slice(&second);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_all_flushed_cache_read_miss() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 8192);
    rt.block_on(async move {
        let handle = ctx.create_file("all_backend.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_all_backend.bin")
            .await;

        let data = pattern_data((PAGE_SIZE * 2) as usize);
        ctx.write_via_fuse(&handle, 0, &data).await;

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        assert_backend_eq(&backend, &data);
        assert_eq!(ctx.cm.used(), 0, "all full pages must be reclaimed");
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_eq!(cache.len(), (PAGE_SIZE * 2) as u64);
        assert!(
            cache.read(0, data.len()).await.unwrap().is_none(),
            "data fully on backend must miss the page cache"
        );
    });
}

#[test]
fn write_back_partial_page_resumed_after_flush() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("resume.bin").await;
        let (writer, backend) = ctx.inject_backend(handle.ino(), "backend_resume.bin").await;

        let head = pattern_data_at(0, (PAGE_SIZE / 2) as usize);
        ctx.write_via_fuse(&handle, 0, &head).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        cache.flush().await.unwrap();
        assert_backend_eq(&backend, &head);

        let tail = pattern_data_at(PAGE_SIZE / 2, (PAGE_SIZE / 2) as usize);
        ctx.write_via_fuse(&handle, PAGE_SIZE / 2, &tail).await;

        assert_eq!(cache.len(), PAGE_SIZE);

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        let mut expected = vec![0u8; PAGE_SIZE as usize];
        expected[..PAGE_SIZE as usize / 2].copy_from_slice(&head);
        expected[PAGE_SIZE as usize / 2..].copy_from_slice(&tail);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_overwrite_same_region() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("overwrite.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_overwrite.bin")
            .await;

        let seg_a = vec![0xa5; 100];
        let seg_b = vec![0x5a; 100];
        ctx.write_via_fuse(&handle, 0, &seg_a).await;
        ctx.write_via_fuse(&handle, 50, &seg_b).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_eq!(cache.len(), 150);

        cache.flush().await.unwrap();

        let mut expected = vec![0u8; 150];
        expected[..50].copy_from_slice(&seg_a[..50]);
        expected[50..].copy_from_slice(&seg_b);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_zero_length_write_noop() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("zero.bin").await;
        let (_writer, backend) = ctx.inject_backend(handle.ino(), "backend_zero.bin").await;

        ctx.write_via_fuse(&handle, 0, &[]).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_eq!(cache.len(), 0);
        assert_eq!(ctx.cm.used(), 0);

        ctx.cm.commit_all(false).await.unwrap();

        assert_eq!(std::fs::metadata(&backend).unwrap().len(), 0);
    });
}

async fn wait_backend_len(backend: &PathBuf, expect: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let len = std::fs::metadata(backend).map(|m| m.len()).unwrap_or(0);
        if len == expect {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "backend {} did not reach len {} in time",
            backend.display(),
            expect
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[test]
fn write_back_sync_flush_path() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("sync_flush.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_sync_flush.bin")
            .await;

        let data = pattern_data(1024);
        ctx.write_via_fuse(&handle, 0, &data).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        cache.flush().await.unwrap();

        assert_backend_eq(&backend, &data);
    });
}

#[test]
fn write_back_auto_flush_after_delay() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx =
        WriteBackTestContext::new_with(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096, "memory", 50, 10);
    rt.block_on(async move {
        let handle = ctx.create_file("auto.bin").await;
        let (_writer, backend) = ctx.inject_backend(handle.ino(), "backend_auto.bin").await;

        let data = pattern_data(PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &data).await;

        wait_backend_len(&backend, PAGE_SIZE).await;

        assert_backend_eq(&backend, &data);
    });
}

#[test]
fn write_back_direct_write_when_cache_full() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    const CACHE_SIZE: u64 = 512 * 1024;
    const TOTAL: usize = 512 * 1024;
    const CHUNK: usize = 128 * 1024;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, CACHE_SIZE);
    rt.block_on(async move {
        let handle = ctx.create_file("full_direct.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_full_direct.bin")
            .await;

        let base = pattern_data(TOTAL);
        let mut off = 0usize;
        while off < TOTAL {
            ctx.write_via_fuse(&handle, off as u64, &base[off..off + CHUNK])
                .await;
            off += CHUNK;
        }

        assert!(ctx.cm.need_evict());
        assert!(ctx.cm.is_full());

        let extra = vec![0xee; 1024];
        ctx.write_via_fuse(&handle, TOTAL as u64, &extra).await;

        assert_eq!(
            ctx.cm.used(),
            TOTAL as u64,
            "direct write must not use cache"
        );

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        let mut expected = vec![0u8; TOTAL + 1024];
        expected[..TOTAL].copy_from_slice(&base);
        expected[TOTAL..].copy_from_slice(&extra);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_fallback_direct_write_on_enospc() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Capacity leaves room so one full page is cached with !is_full(), but
    // allocating a second page fails ENOSPC (used + page_size > capacity).
    const CACHE_SIZE: u64 = PAGE_SIZE * 2 - 1;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, CACHE_SIZE);
    rt.block_on(async move {
        let handle = ctx.create_file("enospc.bin").await;
        let (writer, backend) = ctx.inject_backend(handle.ino(), "backend_enospc.bin").await;

        let base = pattern_data(PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &base).await;
        assert!(
            !ctx.cm.is_full(),
            "fixture must exercise ENOSPC path, not is_full short-circuit"
        );
        assert_eq!(ctx.cm.used(), PAGE_SIZE);

        let extra = vec![0xdd; 1024];
        ctx.write_via_fuse(&handle, PAGE_SIZE, &extra).await;

        assert_eq!(
            ctx.cm.used(),
            PAGE_SIZE,
            "ENOSPC fallback must not use cache"
        );

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        let mut expected = vec![0u8; PAGE_SIZE as usize + 1024];
        expected[..PAGE_SIZE as usize].copy_from_slice(&base);
        expected[PAGE_SIZE as usize..].copy_from_slice(&extra);
        assert_backend_eq(&backend, &expected);
    });
}

/// Deterministic interleaving for the flush/publish window: while a flush is in
/// flight (pages taken, `flushing=true`) eviction must NOT reclaim the pages,
/// because publish has not durably taken the data yet. Then, after a publish
/// failure is simulated by restoring the snapshot, the data must still be there
/// and must flush through to a healthy backend.
#[test]
fn eviction_cannot_reclaim_a_page_awaiting_publish() {
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("awaiting_publish.bin").await;
        let page_size = ctx.conf.write_back_page_size.as_byte() as usize;
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        let data = pattern_data(4096);
        ctx.write_via_fuse(&handle, 0, &data).await;
        let used = ctx.cm.used();

        // --- flush in flight: pages drained, flushing=true, publish pending ---
        let pages = cache.take_all_dirty_pages().unwrap();
        let snapshot = InodeCache::dirty_snapshot(&pages);
        assert_eq!(snapshot.len(), 1);

        // Eviction runs in this window and does not hold `flush_gate`.
        assert_eq!(
            cache.free_pages(false),
            0,
            "a page awaiting publish must survive eviction"
        );
        assert_eq!(ctx.cm.used(), used, "usage must not be released early");

        // --- publish "failed": restore, then retry against a healthy writer ---
        cache.restore_flushed_dirty(&snapshot);
        assert_eq!(ctx.cm.used(), used);

        let ok_path = ctx.work_dir.join("awaiting_publish_ok.bin");
        let ok_writer = Arc::new(FuseWriter::new(
            &ctx.conf,
            ctx.rt.clone(),
            UnifiedWriter::Local(
                LocalWriter::new(
                    &Path::from_str(ok_path.to_str().unwrap()).unwrap(),
                    page_size,
                )
                .unwrap(),
            ),
        ));
        let ok_handle = ok_writer.clone();
        *cache.writer.lock().await = Some(ok_writer);
        cache.flush().await.expect("retry must succeed");
        ok_handle.flush(None).await.expect("flush healthy writer");
        assert_eq!(
            std::fs::metadata(&ok_path).map(|m| m.len()).unwrap_or(0),
            4096,
            "the restored data must reach the backend"
        );
    });
}

/// A failed `complete` is not a delete: the dirty data must stay cached.
#[test]
fn complete_failure_keeps_dirty_pages() {
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("complete_fail.bin").await;
        let page_size = ctx.conf.write_back_page_size.as_byte() as usize;
        let full = ctx.work_dir.join("complete_full.bin");
        std::fs::write(&full, b"").unwrap();
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        // /dev/full fails every write and every flush.
        let bad = Arc::new(FuseWriter::new(
            &ctx.conf,
            ctx.rt.clone(),
            UnifiedWriter::Local(
                LocalWriter::new(&Path::from_str("/dev/full").unwrap(), page_size).unwrap(),
            ),
        ));
        *cache.writer.lock().await = Some(bad);
        ctx.write_via_fuse(&handle, 0, &pattern_data(4096)).await;
        let used = ctx.cm.used();

        assert!(cache.complete(None).await.is_err(), "complete must fail");

        assert_eq!(
            ctx.cm.used(),
            used,
            "a failed complete must not silently drop the dirty pages"
        );
    });
}

#[test]
fn write_back_truncate_clears_cache() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("truncate.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_truncate.bin")
            .await;

        let old = pattern_data(2048);
        ctx.write_via_fuse(&handle, 0, &old).await;
        assert_eq!(ctx.cm.used(), PAGE_SIZE);

        let cache = ctx
            .cm
            .get_or_create(
                handle.ino(),
                OpenFlags::RDWR | OpenFlags::TRUNC,
                CreateFileOpts::with_create(false),
                0,
            )
            .await
            .unwrap();
        assert_eq!(cache.len(), 0);
        assert_eq!(ctx.cm.used(), 0, "truncate must release cached pages");

        let fresh = pattern_data(1024);
        ctx.write_via_fuse(&handle, 0, &fresh).await;

        cache.flush().await.unwrap();

        assert_backend_eq(&backend, &fresh);
    });
}

#[test]
fn write_back_dirty_read_range_snapshot_publishes_enospc_write_through() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, 16);
    rt.block_on(async move {
        let handle = ctx.create_file("flush_range_enospc.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_flush_range_enospc.bin")
            .await;

        ctx.write_via_fuse(&handle, 0, b"direct").await;
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_eq!(cache.page_len(), 0);

        let flush_ver = cache
            .publish_read_range(0, 6, 0)
            .await
            .unwrap()
            .expect("write-through should publish");
        assert_backend_eq(&backend, b"direct");

        assert_eq!(
            cache.publish_read_range(0, 6, flush_ver).await.unwrap(),
            None
        );
    });
}

#[test]
fn write_back_large_page_multi_chunk_flush() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    const BIG_PAGE: u64 = 256 * 1024;
    const TOTAL: usize = 512 * 1024;
    const CHUNK: usize = 128 * 1024;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), BIG_PAGE, BIG_PAGE * 4);
    rt.block_on(async move {
        let handle = ctx.create_file("big_page.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_big_page.bin")
            .await;

        let expected = pattern_data(TOTAL);
        let mut off = 0usize;
        while off < TOTAL {
            ctx.write_via_fuse(&handle, off as u64, &expected[off..off + CHUNK])
                .await;
            off += CHUNK;
        }

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_evict_preserves_partial_pages() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    const CACHE_SIZE: u64 = 520 * 1024;
    const TOTAL: usize = 512 * 1024;
    const CHUNK: usize = 128 * 1024;
    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, CACHE_SIZE);
    rt.block_on(async move {
        let handle = ctx.create_file("evict_partial.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_evict_partial.bin")
            .await;

        let base = pattern_data(TOTAL);
        let mut off = 0usize;
        while off < TOTAL {
            ctx.write_via_fuse(&handle, off as u64, &base[off..off + CHUNK])
                .await;
            off += CHUNK;
        }
        let tail = pattern_data_at(TOTAL as u64, 1024);
        ctx.write_via_fuse(&handle, TOTAL as u64, &tail).await;

        assert!(ctx.cm.need_evict());

        WriteBackWork::evict_inodes(&ctx.cm).await;
        writer.flush(None).await.unwrap();

        assert_eq!(
            ctx.cm.used(),
            PAGE_SIZE,
            "partial page must survive eviction"
        );
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_bytes_eq(
            &ctx.read_cache(&cache, TOTAL as u64, 1024).await,
            &tail,
            "partial page must stay readable after eviction",
        );

        assert_backend_eq(&backend, &base);
    });
}

#[test]
fn write_back_commit_all_true_stops_worker() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("commit_stop.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_commit_stop.bin")
            .await;

        let data = pattern_data(PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &data).await;

        ctx.cm.commit_all(true).await.unwrap();
        writer.flush(None).await.unwrap();
        assert_backend_eq(&backend, &data);

        // WriteBackWork must have exited on stop=true; a follow-up CommitAll
        // either fails to enqueue (receiver dropped) or never gets a reply.
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            ctx.cm.commit_all(false),
        )
        .await;
        assert!(
            !matches!(second, Ok(Ok(()))),
            "WriteBackWork must stop after commit_all(true)"
        );
    });
}

#[test]
fn write_back_commit_all_flushes_only_full_pages() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("commit_full.bin").await;
        let (writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_commit_full.bin")
            .await;

        let full = pattern_data_at(0, PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &full).await;
        let tail = pattern_data_at(PAGE_SIZE, 1024);
        ctx.write_via_fuse(&handle, PAGE_SIZE, &tail).await;

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        assert_backend_eq(&backend, &full);
        assert_eq!(ctx.cm.used(), PAGE_SIZE, "partial page must stay cached");

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        assert_bytes_eq(
            &ctx.read_cache(&cache, PAGE_SIZE, 1024).await,
            &tail,
            "partial page must remain readable after commit_all",
        );

        cache.flush().await.unwrap();

        let mut expected = vec![0u8; (PAGE_SIZE + 1024) as usize];
        expected[..PAGE_SIZE as usize].copy_from_slice(&full);
        expected[PAGE_SIZE as usize..].copy_from_slice(&tail);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_multi_complete_handles() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let first_handle = ctx.create_file("multi.bin").await;
        let ino = first_handle.ino();
        let (_writer, backend) = ctx.inject_backend(ino, "backend_multi.bin").await;

        let second_handle = ctx
            .state
            .fs_open(
                ino,
                (OpenFlags::RDWR).value(),
                CreateFileOpts::with_create(false),
            )
            .await
            .unwrap();

        let head = pattern_data_at(0, 1024);
        let tail = pattern_data_at(1024, 1024);
        ctx.write_via_fuse(&first_handle, 0, &head).await;
        ctx.write_via_fuse(&second_handle, 1024, &tail).await;

        first_handle.complete(None).await.unwrap();
        second_handle.complete(None).await.unwrap();

        ctx.cm.commit_all(false).await.unwrap();

        let mut expected = vec![0u8; 2048];
        expected[..1024].copy_from_slice(&head);
        expected[1024..].copy_from_slice(&tail);
        assert_backend_eq(&backend, &expected);
    });
}

#[test]
fn write_back_disk_cache_type() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new_with(
        rt.clone(),
        PAGE_SIZE,
        PAGE_SIZE * 4096,
        "disk",
        LONG_DELAY_MS,
        LONG_DELAY_MS,
    );
    rt.block_on(async move {
        let handle = ctx.create_file("disk.bin").await;
        let (writer, backend) = ctx.inject_backend(handle.ino(), "backend_disk.bin").await;

        let data = pattern_data(PAGE_SIZE as usize);
        ctx.write_via_fuse(&handle, 0, &data).await;
        assert_eq!(ctx.cm.used(), PAGE_SIZE);

        ctx.cm.commit_all(false).await.unwrap();
        writer.flush(None).await.unwrap();

        assert_backend_eq(&backend, &data);
        assert_eq!(ctx.cm.used(), 0, "disk pages must be reclaimed after flush");
    });
}

#[cfg(target_os = "linux")]
#[test]
fn write_back_backend_error_propagates() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("error.bin").await;
        let backend_path = Path::from_str("/dev/full").unwrap();
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        let page_size = ctx.conf.write_back_page_size.as_byte() as usize;
        let writer = Arc::new(FuseWriter::new(
            &ctx.conf,
            ctx.rt.clone(),
            UnifiedWriter::Local(LocalWriter::new(&backend_path, page_size).unwrap()),
        ));
        *cache.writer.lock().await = Some(writer.clone());

        ctx.write_via_fuse(&handle, 0, &pattern_data(1024)).await;

        assert!(cache.flush().await.is_err());
    });
}

#[cfg(target_os = "linux")]
#[test]
fn write_back_flush_failure_keeps_dirty_state_for_retry() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("fail_drop.bin").await;
        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();

        // A writer that always fails: /dev/full.
        let fail_path = Path::from_str("/dev/full").unwrap();
        let page_size = ctx.conf.write_back_page_size.as_byte() as usize;
        let fail_writer = Arc::new(FuseWriter::new(
            &ctx.conf,
            ctx.rt.clone(),
            UnifiedWriter::Local(LocalWriter::new(&fail_path, page_size).unwrap()),
        ));
        *cache.writer.lock().await = Some(fail_writer.clone());

        ctx.write_via_fuse(&handle, 0, &pattern_data(1024)).await;

        let used_before = ctx.cm.used();
        assert!(cache.flush().await.is_err(), "flush must fail on /dev/full");

        // A failed flush must NOT drop the drained ranges: nothing reached the
        // backend, so discarding it would be silent data loss. The pages must
        // stay accounted (not released) and remain retryable — this is what
        // `flush_inner`/`flush_all` restore-on-failure guarantees.
        assert_eq!(
            ctx.cm.used(),
            used_before,
            "failed flush must keep the pages, not release them"
        );

        // Recovery: with a healthy writer the retained data flushes through.
        // `flush_dirty` does not consult the error monitor, so the fenced state
        // left by the failed attempt does not block the retry — which is the
        // point: the data is still there to be re-sent.
        let ok_path = ctx.work_dir.join("backend_flush_retry.bin");
        let ok_writer = Arc::new(FuseWriter::new(
            &ctx.conf,
            ctx.rt.clone(),
            UnifiedWriter::Local(
                LocalWriter::new(
                    &Path::from_str(ok_path.to_str().unwrap()).unwrap(),
                    page_size,
                )
                .unwrap(),
            ),
        ));
        let ok_handle = ok_writer.clone();
        *cache.writer.lock().await = Some(ok_writer);
        cache.flush().await.expect("retry with a healthy writer");
        // `LocalWriter` buffers below the file length until an explicit flush,
        // so drive the underlying writer before checking the backend bytes.
        ok_handle.flush(None).await.expect("flush healthy writer");
        assert_eq!(
            std::fs::metadata(&ok_path).map(|m| m.len()).unwrap_or(0),
            1024,
            "the retained data must reach the healthy backend"
        );
    });
}

#[test]
fn write_back_ensure_flushed_single_flight() {
    // Serialise metric-delta tests: `FuseMetrics` is process-wide and this
    // file writes the `[write, local]` label that other tests assert on.
    let _serial = crate::test_support::METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rt = Arc::new(AsyncRuntime::single());
    let ctx = WriteBackTestContext::new(rt.clone(), PAGE_SIZE, PAGE_SIZE * 4096);
    rt.block_on(async move {
        let handle = ctx.create_file("single_flight.bin").await;
        let (_writer, backend) = ctx
            .inject_backend(handle.ino(), "backend_single_flight.bin")
            .await;

        let data = pattern_data(1024);
        ctx.write_via_fuse(&handle, 0, &data).await;

        let cache = ctx.cm.get_cache(handle.ino()).await.unwrap();
        let first = futures::future::join_all((0..8).map(|_| cache.ensure_flushed())).await;
        for result in first {
            assert_eq!(result.unwrap(), 1);
        }
        assert_backend_eq(&backend, &data);

        let again = futures::future::join_all((0..8).map(|_| cache.ensure_flushed())).await;
        for result in again {
            assert_eq!(result.unwrap(), 1);
        }

        ctx.write_via_fuse(&handle, 0, &data).await;
        assert_eq!(cache.ensure_flushed().await.unwrap(), 2);
        cache.flush().await.unwrap();
        assert_eq!(cache.ensure_flushed().await.unwrap(), 2);
    });
}
