//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use bytes::Bytes;
use curvine_io::DataSlice;
use curvine_model::{CreateFileOpts, FileAllocOpts, FileStatus, OpenFlags, SetAttrOpts};
use curvine_runtime::common::TimeSpent;
use curvine_runtime::sync::{AsyncMutex, AtomicCounter, ErrorMonitor};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::fs::pcache::{CacheManager, DirtyMessage, Page, PageStore};
use crate::fs::FuseWriter;
use crate::{FuseError, FuseMetrics, FuseResult};

pub struct PageState {
    pages: BTreeMap<u64, Page>,
}

impl Default for PageState {
    fn default() -> Self {
        Self::new()
    }
}

impl PageState {
    pub const DEFAULT_PAGE_SIZE: usize = 128 * 1024;

    pub fn new() -> Self {
        Self {
            pages: BTreeMap::new(),
        }
    }

    fn get_or_create_page(&mut self, cache: &InodeCache, page_id: u64) -> FuseResult<&mut Page> {
        if self.pages.get_mut(&page_id).is_none() {
            let page = cache.cm.store.allocate(cache.ino, page_id)?;
            self.pages.insert(page_id, page);
        }
        Ok(self.pages.get_mut(&page_id).unwrap())
    }

    pub fn write(&mut self, cache: &InodeCache, off: u64, data: &Bytes) -> FuseResult<(u64, bool)> {
        if data.is_empty() {
            return Ok((0, false));
        }

        let len = data.len() as u64;
        let mut file_off = off;
        let mut src = 0;
        let mut notify_full_page = false;

        while src < len {
            let page_id = cache.cm.page_id(file_off);
            let page_off = cache.cm.page_off(file_off);
            let take = (len - src).min(cache.cm.page_size - page_off);

            let page = self.get_or_create_page(cache, page_id)?;
            let old_len = page.len();
            page.write(page_off, &data[src as usize..(src + take) as usize])?;
            page.mark_dirty(page_off, page_off + take);

            let new_len = page.len();
            cache.cm.add_usage(new_len - old_len);
            if new_len >= cache.cm.page_size {
                notify_full_page = true;
            }

            src += take;
            file_off += take;
        }

        Ok((len, notify_full_page))
    }

    pub fn read(
        &mut self,
        cache: &InodeCache,
        off: u64,
        len: usize,
    ) -> FuseResult<Option<Vec<DataSlice>>> {
        if len == 0 {
            return Ok(Some(vec![DataSlice::empty()]));
        }
        if off >= cache.len.get() {
            return Ok(Some(vec![DataSlice::empty()]));
        }

        let want = ((cache.len.get() - off) as usize).min(len);
        let end = off + want as u64;

        let mut out: Vec<DataSlice> = Vec::new();
        let mut cur = off;
        while cur < end {
            let page_id = cache.cm.page_id(cur);
            let page_off = cache.cm.page_off(cur);

            // Missing page: hole in this page; contiguous prefix ends here.
            let Some(page) = self.pages.get_mut(&page_id) else {
                break;
            };

            // In prefix or suffix hole of chunk: chunks need not cover the whole page; stop here.
            if page_off < page.start() || page_off >= page.end() {
                break;
            }

            let avail = (page.end() - page_off) as usize;
            let take = avail.min((end - cur) as usize);

            let bytes = page.read(page_off, take)?;
            out.push(DataSlice::bytes(bytes));

            cur += take as u64;
        }

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn cache_len(&self) -> u64 {
        self.pages.values().map(|c| c.len()).sum()
    }
}

pub struct InodeCache {
    pub cm: Arc<CacheManager>,
    pub ino: u64,
    pub page_size: u64,
    pub state: AsyncMutex<PageState>,
    pub flags: OpenFlags,
    pub create_opts: CreateFileOpts,
    pub writer: AsyncMutex<Option<Arc<FuseWriter>>>,
    pub error: ErrorMonitor<FuseError>,
    writer_ver: AtomicCounter,
    len: AtomicCounter,
}

impl InodeCache {
    pub fn new(
        cm: Arc<CacheManager>,
        ino: u64,
        page_size: u64,
        flags: OpenFlags,
        create_opts: CreateFileOpts,
        file_len: u64,
    ) -> Self {
        Self {
            cm,
            ino,
            page_size,
            flags,
            create_opts,
            state: AsyncMutex::new(PageState::new()),
            writer: AsyncMutex::new(None),
            error: ErrorMonitor::new(),
            writer_ver: AtomicCounter::new(0),
            len: AtomicCounter::new(file_len),
        }
    }

    pub fn set_error(&self, e: FuseError) {
        self.error.set_error(e)
    }

    pub fn check_error(self: &Arc<Self>) -> FuseResult<()> {
        self.error.check_error()
    }

    pub async fn get_writer(&self) -> FuseResult<Arc<FuseWriter>> {
        let mut lock = self.writer.lock().await;
        match &*lock {
            Some(writer) => Ok(writer.clone()),
            None => {
                let state = self.cm.state()?;
                let path = state.get_path(self.ino)?;

                let writer = state
                    .new_writer(&path, self.flags, self.create_opts.clone())
                    .await?;
                let _ = lock.insert(writer.clone());
                Ok(writer)
            }
        }
    }

    pub async fn flush_pages(self: &Arc<Self>, sync: bool) -> FuseResult<Arc<FuseWriter>> {
        let writer = self.get_writer().await?;

        // Snapshot dirty pages under the lock (including partial pages for
        // complete/evict). Tick worker still uses take_dirty_pages (full only).
        let mut pages = self.take_all_dirty_pages().await?;
        for page in &mut pages {
            page.flush_to(&writer).await?;
        }

        if sync {
            writer.flush(None).await?;
        }

        Ok(writer)
    }

    pub async fn write(self: &Arc<Self>, off: u64, data: Bytes) -> FuseResult<usize> {
        self.check_error()?;

        let data_len = data.len();
        let spent = TimeSpent::new();

        let label = if self.cm.is_full() {
            let writer = self.get_writer().await?;
            writer.write(off as i64, data, None).await?;
            "through"
        } else {
            let notify = {
                let mut state = self.lock().await;
                match state.write(self, off, &data) {
                    Err(e) if e.errno == libc::ENOSPC => None,
                    Err(e) => {
                        FuseMetrics::with(|m| {
                            m.write_back_error_count.with_label_values(&["write"]).inc();
                        });
                        return Err(e);
                    }
                    Ok((_, notify)) => Some(notify),
                }
            };

            match notify {
                None => {
                    let writer = self.get_writer().await?;
                    writer.write(off as i64, data, None).await?;
                    "enospc"
                }
                Some(true) => {
                    self.cm.send(DirtyMessage::Write(self.ino)).await?;
                    "cache"
                }
                Some(false) => "cache",
            }
        };
        self.writer_ver.incr();
        self.max_len(off + data_len as u64);

        FuseMetrics::with(|m| {
            m.write_back_io_bytes
                .with_label_values(&[label])
                .inc_by(data_len as i64);

            m.write_back_io_us
                .with_label_values(&["write"])
                .observe(spent.used_us() as f64)
        });

        Ok(data_len)
    }

    pub async fn read(
        self: &Arc<Self>,
        off: u64,
        len: usize,
    ) -> FuseResult<Option<Vec<DataSlice>>> {
        self.check_error()?;
        let spent = TimeSpent::new();

        let mut state = self.lock().await;
        let data = state.read(self, off, len)?;

        FuseMetrics::with(|m| {
            m.write_back_io_us
                .with_label_values(&["read"])
                .observe(spent.used_us() as f64)
        });

        Ok(data)
    }

    pub fn len(&self) -> u64 {
        self.len.get()
    }

    pub async fn page_len(&self) -> u64 {
        let state = self.state.lock().await;
        state.pages.values().map(|c| c.len()).sum()
    }

    pub fn reached_eof(&self, off: u64, got: usize) -> bool {
        off + got as u64 >= self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    fn max_len(&self, new: u64) {
        loop {
            let cur = self.len.get();
            if cur >= new {
                return;
            }
            if self.len.compare_and_set(cur, new) {
                return;
            }
        }
    }

    pub async fn resize(self: &Arc<Self>, opts: FileAllocOpts) -> FuseResult<FileStatus> {
        self.check_error()?;

        let writer = self.flush_pages(false).await?;
        let status = writer.resize(opts).await?;

        self.writer_ver.incr();
        self.len.set(status.len as u64);

        Ok(status)
    }

    pub fn queue_flush(self: &Arc<Self>) -> FuseResult<()> {
        self.check_error()?;

        self.cm
            .state()?
            .update_dirty_len(self.ino, self.len.get())?;

        Ok(())
    }

    pub async fn flush(self: &Arc<Self>) -> FuseResult<()> {
        self.flush_pages(true).await?;
        Ok(())
    }

    pub async fn queue_complete(self: &Arc<Self>) -> FuseResult<()> {
        self.cm.send(DirtyMessage::new_complete(self.ino)).await?;
        Ok(())
    }

    pub async fn complete(self: &Arc<Self>, opts: Option<SetAttrOpts>) -> FuseResult<()> {
        let res: FuseResult<()> = async {
            let writer = self.flush_pages(false).await?;
            writer.complete_with_attr(None, opts).await?;
            Ok(())
        }
        .await;

        let _ = self.free_pages(true).await;
        res
    }

    pub fn write_ver(&self) -> u64 {
        self.writer_ver.get()
    }

    pub async fn truncate(self: &Arc<Self>) -> FuseResult<()> {
        let _ = self.free_pages(true).await;
        self.len.set(0);
        self.writer_ver.incr();

        Ok(())
    }

    pub async fn free_pages(self: &Arc<Self>, force: bool) -> u64 {
        let mut state = self.lock().await;
        let mut freed = 0;

        state.pages.retain(|_, page| {
            if !force && page.is_dirty() {
                return true;
            }

            let len = page.clear();
            self.cm.release_usage(len);
            freed += len;

            false
        });
        freed
    }

    pub async fn take_dirty_pages(self: &Arc<Self>) -> FuseResult<Vec<Page>> {
        self.take_dirty_pages_inner(true).await
    }

    async fn take_all_dirty_pages(self: &Arc<Self>) -> FuseResult<Vec<Page>> {
        self.take_dirty_pages_inner(false).await
    }

    async fn take_dirty_pages_inner(self: &Arc<Self>, full_only: bool) -> FuseResult<Vec<Page>> {
        let mut state = self.state.lock().await;
        let mut result = Vec::new();

        for page in state.pages.values_mut() {
            let taken = if full_only {
                page.take_dirty(self.page_size)?
            } else {
                page.take_all_dirty(self.page_size)?
            };
            if let Some(dirty) = taken {
                result.push(dirty);
            }
        }

        Ok(result)
    }

    pub async fn cache_len(&self) -> u64 {
        let state = self.lock().await;
        state.cache_len()
    }
}

impl Deref for InodeCache {
    type Target = AsyncMutex<PageState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl Drop for InodeCache {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_lock() {
            for page in state.pages.values_mut() {
                let len = page.clear();
                self.cm.release_usage(len);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::err_fuse;
    use curvine_config::FuseConf;
    use curvine_model::{CreateFileOpts, OpenFlags};
    use curvine_runtime::common::ByteUnit;
    use curvine_runtime::sync::channel::AsyncChannel;

    const TEST_INO: u64 = 100;

    fn test_conf(page_size: u64) -> FuseConf {
        FuseConf {
            write_back_cache_type: "memory".to_string(),
            write_back_page_size: ByteUnit::new(page_size),
            write_back_cache_size: ByteUnit::new(page_size * 16),
            ..Default::default()
        }
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

    fn bytes(data: &[u8]) -> Bytes {
        Bytes::copy_from_slice(data)
    }

    async fn read_all(cache: &Arc<InodeCache>, off: u64, len: usize) -> Vec<u8> {
        let slices = cache.read(off, len).await.unwrap().unwrap();
        let mut out = Vec::new();
        for s in slices {
            out.extend_from_slice(s.as_slice());
        }
        out
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let cache = test_cache(&test_conf(4096));
        let data = b"hello world".to_vec();
        assert_eq!(cache.write(0, bytes(&data)).await.unwrap(), data.len());
        assert_eq!(cache.len(), data.len() as u64);
        assert_eq!(read_all(&cache, 0, data.len()).await, data);
    }

    #[tokio::test]
    async fn write_empty_is_noop() {
        let cache = test_cache(&test_conf(4096));
        assert_eq!(cache.write(0, Bytes::new()).await.unwrap(), 0);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.cache_len().await, 0);
    }

    #[tokio::test]
    async fn write_read_across_pages() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        cache.write(4096, bytes(&vec![2u8; 4096])).await.unwrap();
        assert_eq!(cache.len(), 8192);
        assert_eq!(cache.cache_len().await, 8192);

        let got = read_all(&cache, 4000, 200).await;
        assert_eq!(&got[..96], &[1u8; 96]);
        assert_eq!(&got[96..], &[2u8; 104]);
    }

    #[tokio::test]
    async fn write_read_partial_page_offset() {
        let cache = test_cache(&test_conf(4096));
        cache.write(100, bytes(b"abc")).await.unwrap();
        assert_eq!(cache.len(), 103);
        assert_eq!(read_all(&cache, 100, 3).await, b"abc");
        assert_eq!(cache.cache_len().await, 3);
    }

    #[tokio::test]
    async fn len_tracks_max_offset() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"1234567890")).await.unwrap();
        assert_eq!(cache.len(), 10);
        cache.write(100, bytes(b"abc")).await.unwrap();
        assert_eq!(cache.len(), 103);
    }

    #[tokio::test]
    async fn read_beyond_len_returns_empty() {
        let cache = test_cache(&test_conf(4096));
        let slices = cache.read(0, 10).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].is_empty());
    }

    #[tokio::test]
    async fn read_missing_page_returns_none() {
        let cache = test_cache(&test_conf(4096));
        cache.write(5000, bytes(b"abc")).await.unwrap();
        assert!(cache.read(0, 100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncate_clears_all() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();
        cache.truncate().await.unwrap();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.cache_len().await, 0);
        assert!(cache.is_empty());
        assert_eq!(cache.cm.store.used(), 0);
    }

    #[tokio::test]
    async fn free_pages_keeps_dirty_and_partial() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();

        assert_eq!(cache.free_pages(false).await, 0);
        assert_eq!(cache.cache_len().await, 4100);

        assert_eq!(cache.take_dirty_pages().await.unwrap().len(), 1);
        assert_eq!(cache.free_pages(false).await, 4096);
        assert_eq!(cache.cache_len().await, 4);
        assert_eq!(cache.cm.store.used(), 4);
    }

    #[tokio::test]
    async fn free_pages_force_clears_all() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();
        assert_eq!(cache.free_pages(true).await, 9);
        assert_eq!(cache.cache_len().await, 0);
        assert_eq!(cache.cm.store.used(), 0);
    }

    #[tokio::test]
    async fn take_dirty_pages_copies_data() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        let pages = cache.take_dirty_pages().await.unwrap();
        assert_eq!(pages.len(), 1);
        let mut page = pages.into_iter().next().unwrap();
        assert_eq!(page.page_id, 0);
        assert_eq!(page.start(), 0);
        assert_eq!(&page.read(0, 4).unwrap()[..], &[1u8; 4]);
    }

    fn injected_error() -> FuseError {
        (err_fuse!(libc::EIO, "injected") as FuseResult<u32>).unwrap_err()
    }

    #[tokio::test]
    async fn error_blocks_io() {
        let cache = test_cache(&test_conf(4096));
        cache.set_error(injected_error());
        assert!(cache.check_error().is_err());

        let cache = test_cache(&test_conf(4096));
        cache.set_error(injected_error());
        assert!(cache.write(0, bytes(b"x")).await.is_err());

        let cache = test_cache(&test_conf(4096));
        cache.set_error(injected_error());
        assert!(cache.read(0, 1).await.is_err());
    }

    #[tokio::test]
    async fn write_ver_increments() {
        let cache = test_cache(&test_conf(4096));
        assert_eq!(cache.write_ver(), 0);
        cache.write(0, bytes(b"a")).await.unwrap();
        assert_eq!(cache.write_ver(), 1);
    }

    #[tokio::test]
    async fn usage_tracks_write_len() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        assert_eq!(cache.cm.store.used(), 5);
        assert_eq!(cache.cache_len().await, 5);
    }

    #[tokio::test]
    async fn read_random_offset_across_pages() {
        let cache = test_cache(&test_conf(4096));
        let data: Vec<u8> = (0..12288u64).map(|i| (i % 251) as u8).collect();
        cache.write(0, bytes(&data)).await.unwrap();

        let got = read_all(&cache, 3000, 5000).await;
        assert_eq!(got.len(), 5000);
        assert_eq!(got, data[3000..8000]);
    }

    #[tokio::test]
    async fn read_exact_page_boundary() {
        let cache = test_cache(&test_conf(4096));
        let data = vec![0x5Au8; 8192];
        cache.write(0, bytes(&data)).await.unwrap();
        let got = read_all(&cache, 4096, 4096).await;
        assert_eq!(got, data[4096..]);
    }

    #[tokio::test]
    async fn write_crosses_three_pages() {
        let cache = test_cache(&test_conf(4096));
        let data: Vec<u8> = (0..5000u64).map(|i| (i % 251) as u8).collect();
        assert_eq!(cache.write(3000, bytes(&data)).await.unwrap(), 5000);
        assert_eq!(cache.len(), 8000);
        assert_eq!(cache.cache_len().await, 5000);
        let got = read_all(&cache, 3000, 5000).await;
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn read_zero_len_returns_empty() {
        let cache = test_cache(&test_conf(4096));
        let slices = cache.read(0, 0).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].is_empty());
    }

    #[tokio::test]
    async fn read_at_file_len_returns_empty() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        let slices = cache.read(5, 10).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].is_empty());
    }

    #[tokio::test]
    async fn sparse_write_read_prefix_then_stops_at_hole() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();
        let got = read_all(&cache, 0, 4096).await;
        assert_eq!(got, b"hello");
    }

    #[tokio::test]
    async fn read_in_page_prefix_hole_returns_none() {
        let cache = test_cache(&test_conf(4096));
        cache.write(5000, bytes(b"tail")).await.unwrap();
        assert!(cache.read(4096, 100).await.unwrap().is_none());
    }
}
